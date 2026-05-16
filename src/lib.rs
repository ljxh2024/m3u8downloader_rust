pub mod downloader;

use downloader::{ChannelMessage, DownloadManager, DownloadTask};
use reqwest::Client;
use std::{error::Error, rc::Rc};
use tokio::{process::Command, sync::mpsc, time::Duration};

// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";
// :todo USER-AGENT，后期引入请求头后改为自定义
const APP_USER_AGENT: &str = "Chrome/147";

slint::include_modules!();

/// 应用入口
pub fn run() -> Result<(), slint::PlatformError> {
    let window = AppWindow::new()?;

    // UI界面默认语言，注释掉则自动根据系统区域设置，当前支持：中文/英文
    // let _ = slint::select_bundled_translation("en");
    // 初始化信道
    let (tx, mut rx) = mpsc::channel(20);
    // 下载任务
    let download_task = Rc::new(DownloadTask::default());

    // 启动异步任务处理信道消息并维护UI
    let ui_weak_clone_for_channel = window.as_weak();
    let download_task_clone = Rc::clone(&download_task);
    slint::spawn_local(async move {
        consume_channel_message(ui_weak_clone_for_channel, download_task_clone, &mut rx).await;
    })
    .unwrap();

    // 启动下载
    window.on_start_download({
        let ui_weak = window.as_weak();
        let download_task_clone = Rc::clone(&download_task);
        let tx_clone = tx.clone();

        move || {
            let ui = ui_weak.unwrap();
            let download_task_clone = Rc::clone(&download_task_clone);
            let tx_clone = tx_clone.clone();

            slint::spawn_local(async_compat::Compat::new(async move {
                // 处理下载期间的错误
                if let Err(e) = parse_download(&ui, Rc::clone(&download_task_clone), tx_clone).await
                {
                    ui.invoke_show_message(e.to_string().into(), true);
                    ui.set_enable_start_btn(true);
                    ui.set_download_state(download_task_clone.state.get() as i32);
                }
            }))
            .unwrap();
        }
    });

    // 暂停
    window.on_pause_download({
        let ui_weak = window.as_weak();
        // let tx_clone = tx.clone();
        let download_task_clone = Rc::clone(&download_task);

        move || {
            let ui = ui_weak.unwrap();

            ui.set_enable_pause_btn(false);
            ui.invoke_show_message("Pausing...".into(), false);

            download_task_clone.state.set(2);
        }
    });

    // 取消
    window.on_cancel_download({
        let ui_weak = window.as_weak();
        let download_task_clone = Rc::clone(&download_task);

        move || {
            let ui = ui_weak.unwrap();

            ui.set_enable_start_btn(false);
            ui.set_enable_pause_btn(false);
            ui.set_enable_cancel_btn(false);
            ui.invoke_show_message("Canceling...".into(), false);

            let old_state = download_task_clone.state.replace(0);
            if old_state == 2 {
                // 重置任务
                download_task_clone.clear();
                ui.invoke_task_finished("You canceled the download.".into(), true);
            }
        }
    });

    // 选择目录
    window.on_select_dir({
        let ui_weak = window.as_weak();
        move || {
            ui_weak.upgrade().unwrap().set_work_dir(
                rfd::FileDialog::new()
                    .pick_folder()
                    .map(|path| path.to_string_lossy().to_string().into())
                    .unwrap_or_default(),
            );
        }
    });

    // 打开下载失败的文件
    window.on_open_failed_file({
        let download_task_clone = Rc::clone(&download_task);
        move || {
            let download_task_clone = Rc::clone(&download_task_clone);
            slint::spawn_local(async move {
                let file_path = download_task_clone.save_path.borrow().join(FAILED_FILENAME);
                if file_path.is_file() {
                    #[cfg(windows)]
                    {
                        Command::new("explorer")
                            .arg(format!("/select,{}", file_path.to_string_lossy()))
                            .spawn()
                            .unwrap()
                            .wait()
                            .await
                            .unwrap();
                    }
                }
            })
            .unwrap();
        }
    });

    window.run()
}

/// 处理信道消息并维护UI
async fn consume_channel_message(
    ui_weak: slint::Weak<AppWindow>,
    download_task: Rc<DownloadTask>,
    rx: &mut mpsc::Receiver<ChannelMessage>,
) {
    while let Some(item) = rx.recv().await {
        match item {
            ChannelMessage::Start {
                total_nums,
                is_master_playlist,
            } => {
                let msg = format!(
                    "Downloading{}...",
                    if is_master_playlist {
                        "(master playlist)"
                    } else {
                        ""
                    }
                );
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message(msg.into(), false);
                        ui.set_enable_pause_btn(true);
                        ui.set_enable_cancel_btn(true);
                        ui.set_total_nums(total_nums as i32);
                    })
                    .unwrap();
            }
            // 实时更新下载进度
            ChannelMessage::Progress { segment_size } => {
                // downloaded_segment_sizes += segment_size;
                // downloaded_segment_nums += 1;
                download_task.downloaded_sizes.update(|v| v + segment_size);
                download_task.downloaded_nums.update(|v| v + 1);

                let downloaded_sizes = download_task.downloaded_sizes.get();
                let downloaded_nums = download_task.downloaded_nums.get() as i32;

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.set_downloaded_sizes(format_size(downloaded_sizes).into());
                        ui.set_downloaded_nums(downloaded_nums);
                    })
                    .unwrap();
            }
            // 任务暂停成功
            ChannelMessage::Paused => {
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.set_enable_start_btn(true);
                        ui.set_enable_cancel_btn(true);
                        ui.set_download_state(2);
                        ui.invoke_show_message("You paused the download.".into(), false);
                    })
                    .unwrap();
            }
            // 任务取消成功（非暂停状态下的取消）
            ChannelMessage::Canceled => {
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_task_finished("You canceled the download.".into(), true);
                    })
                    .unwrap();
            }
            // 下载完毕
            ChannelMessage::Downloaded {
                message,
                have_failed_segment,
            } => {
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_task_finished(message.into(), false);
                        ui.set_have_failed_segment(have_failed_segment);
                    })
                    .unwrap();
            }
            // 合并分片
            ChannelMessage::Merging => {
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message("Merging segments into MP4...".into(), false);
                    })
                    .unwrap();
            }
        }
    }
}

/// 解析、下载
async fn parse_download(
    ui: &AppWindow,
    download_task: Rc<DownloadTask>,
    tx: mpsc::Sender<ChannelMessage>,
) -> Result<(), Box<dyn Error>> {
    // 选择状态 0,1,2
    let download_state = download_task.state.get();
    // 初始化下载管理
    let download_manager = DownloadManager::new(ui, download_state).await?;
    let client = Rc::new(
        Client::builder()
            .connect_timeout(Duration::from_secs(download_manager.connect_timeout))
            .user_agent(APP_USER_AGENT)
            .build()?,
    );

    // 新下载，重新解析内容
    if download_state == 0 {
        download_manager
            .load_task(Rc::clone(&download_task), Rc::clone(&client))
            .await?;
    }

    download_manager
        .download(download_task, tx, Rc::clone(&client))
        .await
}

/// 格式化大小显示
fn format_size(size: usize) -> String {
    if size <= 1024 {
        return String::from("1 KB");
    }

    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut size = size as f64;
    let mut unit_index = 0;

    while size > 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}
