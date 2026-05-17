pub mod downloader;

use downloader::{ChannelMessage, DownloadConfig, DownloadManager, DownloadState};
use std::{error::Error, sync::Arc};
use tokio::sync::mpsc;
use slint::PhysicalPosition;
use winsafe::{GetSystemMetrics, co::SM};

slint::include_modules!();

// 信道缓冲区容量
const CHANNEL_BUFFER_CAPACITY: usize = 100;
// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";

/// 应用入口
pub fn run() -> Result<(), slint::PlatformError> {
    let window = AppWindow::new()?;

    // 控制窗口位置
    let x = (GetSystemMetrics(SM::CXSCREEN) - 380) / 2;
    let y = (GetSystemMetrics(SM::CYSCREEN) - 600) / 2; // 尽量偏高
    window.window().set_position(slint::WindowPosition::Physical(PhysicalPosition { x, y }));

    // UI界面默认语言，注释掉则自动根据系统区域设置，当前支持：中文/英文
    // let _ = slint::select_bundled_translation("en");
    // 全局下载管理
    let download_manager = Arc::new(DownloadManager::new());
    // 使用信道通信
    let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER_CAPACITY);

    // 启动异步任务处理信道消息并维护UI
    let ui_weak_channel = window.as_weak();
    slint::spawn_local(async move {
        consume_channel_message(ui_weak_channel, &mut rx).await;
    })
    .unwrap();

    // 启动下载
    window.on_start_download({
        let ui_weak = window.as_weak();
        let download_manager_clone = Arc::clone(&download_manager);
        let tx_clone = tx.clone();

        move || {
            let ui = ui_weak.unwrap();
            let download_manager_clone = Arc::clone(&download_manager_clone);
            let tx_clone = tx_clone.clone();

            slint::spawn_local(async_compat::Compat::new(async move {
                // 处理下载期间的错误
                if let Err(e) =
                    parse_download(&ui, Arc::clone(&download_manager_clone), tx_clone).await
                {
                    ui.invoke_show_message(e.to_string().into(), true);
                    ui.set_enable_start_btn(true);
                    // 使用全局的下载状态重新赋值
                    ui.set_download_state(download_manager_clone.get_download_state().await as i32);
                }
            }))
            .unwrap();
        }
    });

    // 暂停
    window.on_pause_download({
        let ui_weak = window.as_weak();
        let download_manager_clone = Arc::clone(&download_manager);

        move || {
            let ui = ui_weak.unwrap();

            ui.set_enable_pause_btn(false);
            ui.invoke_show_message("Pausing...".into(), false);

            let download_manager_clone = Arc::clone(&download_manager_clone);
            slint::spawn_local(async move {
                download_manager_clone
                    .set_download_state(DownloadState::Paused)
                    .await;
            })
            .unwrap();
        }
    });

    // 取消
    window.on_cancel_download({
        let ui_weak = window.as_weak();
        let download_manager_clone = Arc::clone(&download_manager);

        move || {
            let ui = ui_weak.unwrap();

            ui.set_enable_start_btn(false);
            ui.set_enable_pause_btn(false);
            ui.set_enable_cancel_btn(false);
            ui.invoke_show_message("Canceling...".into(), false);

            let download_manager_clone = Arc::clone(&download_manager_clone);
            slint::spawn_local(async move {
                let old_state = download_manager_clone
                    .update_download_state(DownloadState::Canceled)
                    .await;
                if old_state == DownloadState::Paused {
                    ui.invoke_task_finished("You canceled the download.".into(), true);

                    // 重置任务
                    download_manager_clone.clear().await;
                }
            })
            .unwrap();
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
        let download_manager_clone = Arc::clone(&download_manager);
        move || {
            let download_manager_clone = Arc::clone(&download_manager_clone);
            slint::spawn_local(async move {
                let file_path = download_manager_clone
                    .save_path
                    .lock()
                    .await
                    .join(FAILED_FILENAME);
                if file_path.exists() {
                    let _ = open::that(file_path);
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
    rx: &mut mpsc::Receiver<ChannelMessage>,
) {
    while let Some(item) = rx.recv().await {
        match item {
            ChannelMessage::Start {
                total_nums,
                is_new_download,
            } => {
                update_ui(&ui_weak, move |ui| {
                    ui.invoke_show_message("Downloading...".into(), false);
                    ui.set_enable_pause_btn(true);
                    ui.set_enable_cancel_btn(true);

                    if is_new_download {
                        ui.set_total_nums(total_nums as i32);
                    }
                });
            }
            // 实时更新下载进度
            ChannelMessage::Progress {
                downloaded_nums,
                downloaded_sizes,
            } => {
                update_ui(&ui_weak, move |ui| {
                    ui.set_downloaded_sizes(format_size(downloaded_sizes).into());
                    ui.set_downloaded_nums(downloaded_nums as i32);
                });
            }
            // 任务暂停成功
            ChannelMessage::Paused => {
                update_ui(&ui_weak, move |ui| {
                    ui.set_enable_start_btn(true);
                    ui.set_enable_cancel_btn(true);
                    ui.set_download_state(DownloadState::Paused as i32);
                    ui.invoke_show_message("You paused the download.".into(), false);
                });
            }
            // 任务取消成功（非暂停状态下的取消）
            ChannelMessage::Canceled => {
                update_ui(&ui_weak, |ui| {
                    ui.invoke_task_finished("You canceled the download.".into(), true);
                });
            }
            // 下载完毕
            ChannelMessage::Downloaded {
                message,
                have_failed_segment,
            } => {
                update_ui(&ui_weak, move |ui| {
                    ui.invoke_task_finished(message.into(), false);
                    ui.set_have_failed_segment(have_failed_segment);
                });
            }
            // 合并分片
            ChannelMessage::Merging => {
                update_ui(&ui_weak, |ui| {
                    ui.invoke_show_message("Merging segments into MP4...".into(), false);
                });
            }
        }
    }
}

/// 简化UI更新代码量
fn update_ui<F>(ui_weak: &slint::Weak<AppWindow>, f: F)
where
    F: FnOnce(AppWindow) + Send + 'static,
{
    ui_weak.upgrade_in_event_loop(f).unwrap();
}

/// 解析M3U8,下载分片
async fn parse_download(
    ui: &AppWindow,
    download_manager: Arc<DownloadManager>,
    tx: mpsc::Sender<ChannelMessage>,
) -> Result<(), Box<dyn Error>> {
    download_manager
        .download(DownloadConfig::new(ui, &download_manager).await?, tx)
        .await
}

/// 格式化大小显示
fn format_size(size: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit_index = 0;

    for _ in 0..UNITS.len() - 1 {
        if size < 1024.0 {
            break;
        }
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}
