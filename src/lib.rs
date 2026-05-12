use regex::Regex;
use reqwest::Client;
use slint::{PhysicalPosition, SharedString};
use std::{
    error::Error,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncWriteExt, BufWriter},
    process::Command,
    sync::{Mutex, Semaphore, mpsc, Notify},
    task::JoinHandle,
    time::{Duration, sleep},
};
use url::Url;
use winsafe::{GetSystemMetrics, co::SM};
use futures::stream::{FuturesUnordered, StreamExt};

slint::include_modules!();

// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";
// M3U8文件名
const M3U8_FILENAME: &str = "index.m3u8";
// user agent
const APP_USER_AGENT: &str = "Chrome/147";

/// 应用入口
pub fn run() -> Result<(), slint::PlatformError> {
    let window = AppWindow::new()?;

    // 控制窗口位置
    let x = (GetSystemMetrics(SM::CXSCREEN) - 370) / 2;
    let y = (GetSystemMetrics(SM::CYSCREEN) - 600) / 2; // 尽量偏高
    window
        .window()
        .set_position(slint::WindowPosition::Physical(PhysicalPosition { x, y }));

    // 使用信道处理UI界面事件
    let (tx, mut rx) = mpsc::channel(20);

    // 启动下载
    window.on_start_download({
        let ui_weak = window.as_weak();
        let tx_start = tx.clone();

        move |is_pause| {
            let ui = ui_weak.unwrap();

            ui.invoke_parse_state();

            let tx_start = tx_start.clone();
            slint::spawn_local(async_compat::Compat::new(async move {
                if is_pause {
                    // 继续下载
                    let _ = tx_start.send(ChannelMessage::Continue).await;
                } else {
                    // 新的下载，从解析M3U8开始
                    let (save_path, video_name) =
                        create_safe_save_path(&ui.get_work_dir(), &ui.get_video_name())
                            .await
                            .unwrap();
                    let _ = tx_start
                        .send(ChannelMessage::ParseDownload(RequestData {
                            video_name,
                            save_path,
                            m3u8_url: ui.get_m3u8_url().into(),
                            concurrency: ui.get_concurrency().parse::<usize>().unwrap_or(4),
                            retry: ui.get_retry().parse::<u32>().unwrap_or(3),
                            timeout: ui.get_timeout().parse::<u64>().unwrap_or(3),
                            is_merge: ui.get_is_merge(),
                            is_delete_segment: ui.get_is_delete_segment(),
                        }))
                        .await;
                }
            }))
            .unwrap();
        }
    });

    // 暂停下载
    window.on_pause_download({
        let ui_weak = window.as_weak();
        let tx_pause = tx.clone();

        move || {
            ui_weak.unwrap().invoke_paused_state();

            let tx_pause = tx_pause.clone();
            slint::spawn_local(async_compat::Compat::new(async move {
                let _ = tx_pause.send(ChannelMessage::Pause).await;
            }))
            .unwrap();
        }
    });

    // 取消下载
    window.on_cancel_download({
        let ui_weak = window.as_weak();
        let tx_cancel = tx.clone();

        move || {
            let ui = ui_weak.unwrap();
            
            ui.invoke_default_state();

            let tx_cancel = tx_cancel.clone();
            slint::spawn_local(async_compat::Compat::new(async move {
                let _ = tx_cancel.send(ChannelMessage::Cancel).await;
            }))
            .unwrap();
        }
    });

    // 选择工作目录
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

    // 打开目录
    window.on_open_failed_file({
        let ui_weak = window.as_weak();
        move || {
            let ui = ui_weak.unwrap();

            slint::spawn_local(async_compat::Compat::new(async move {
                Command::new("explorer")
                    .arg(
                        Path::new("/select,")
                            .join(ui.get_work_dir())
                            .join(ui.get_video_name())
                            .join(FAILED_FILENAME),
                    )
                    .output()
                    .await
                    .unwrap();
            }))
            .unwrap();
        }
    });

    // 异步监控信道，处理下载、暂停、取消操作
    let ui_weak = window.as_weak();
    slint::spawn_local(async_compat::Compat::new(async move {
        loop_receive_message(tx.clone(), &mut rx, ui_weak).await;
    }))
    .unwrap();

    window.run()
}

// 信道消息类型
enum ChannelMessage {
    ParseDownload(RequestData),
    PrepareDownloadSegment {
        total_segment_nums: u32,
        has_master_playlist: bool,
    },
    Pause,
    Continue,
    Cancel,
    // ReleaseTask,
    SegmentDownloaded {
        content_length: u64,
    },
}

// 下载参数
struct RequestData {
    video_name: String,
    save_path: Arc<Path>,
    m3u8_url: String,
    concurrency: usize,
    retry: u32,
    timeout: u64,
    is_merge: bool,
    is_delete_segment: bool,
}

// 分片信息
#[derive(Clone)]
struct Segment {
    segment_name: String,
    save_path: Arc<Path>,
    download_url: String,
}

// 下载任务配置
struct DownloadTask {
    total_segment_nums: u32,
    failed_segment_nums: u32,
    // downloaded_segments: Vec<String>,
    // segments: Mutex<Vec<Segment>>,
    // downloaded_segments: Mutex<Vec<String>>,
    // failed_segments: Mutex<Vec<String>>,
    // is_new_download: AtomicBool,
    // is_pause: AtomicBool,
    // is_cancel: AtomicBool,
    // is_parse_fail: AtomicBool,
}

impl DownloadTask {
    fn new() -> Self {
        Self {
            total_segment_nums: 0,
            failed_segment_nums: 0,
            // downloaded_segments: Vec::new(),
            // segments: Mutex::new(Vec::new()),
            // downloaded_segments: Mutex::new(Vec::new()),
            // failed_segments: Mutex::new(Vec::new()),
            // is_new_download: AtomicBool::new(true),
            // is_pause: AtomicBool::new(false),
            // is_cancel: AtomicBool::new(false),
            // is_parse_fail: AtomicBool::new(false),
        }
    }
}

// 合并配置
struct MergeConfig {
    args: Vec<String>,
    delete_segment: bool,
    save_path: Arc<Path>,
}

// 监控信道，处理下载、暂停、取消
async fn loop_receive_message(
    tx: mpsc::Sender<ChannelMessage>,
    rx: &mut mpsc::Receiver<ChannelMessage>,
    ui_weak: slint::Weak<AppWindow>,
) {
    // 初始化任务配置
    let download_task = Arc::new(Mutex::new(DownloadTask::new()));
    // 当前已下载的文件大小
    let mut current_content_length: u64 = 0;
    // 已下载的分片数
    let mut downloaded_nums: u32 = 0;
    // 下载任务句柄
    // let master_task: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    // 暂停通知
    let notify = Arc::new(Notify::new());
    // 下载状态 1下载中，2暂停，3取消
    let download_state = Arc::new(AtomicU8::new(1));

    while let Some(channel_message) = rx.recv().await {
        match channel_message {
            // 解析
            ChannelMessage::ParseDownload(request_data) => {
                ui_weak.upgrade_in_event_loop(move |ui| {
                    ui.invoke_show_message("正在解析...".into(), false);
                }).unwrap();
                
                let ui_weak_clone = ui_weak.clone();
                let tx_clone = tx.clone();
                let notify_clone = Arc::clone(&notify);
                let download_task_clone = Arc::clone(&download_task);
                let download_state_clone =  Arc::clone(&download_state);

                current_content_length = 0;
                downloaded_nums = 0;

                tokio::spawn(async move {
                    match start_parse_download(download_task_clone, request_data, tx_clone, notify_clone, download_state_clone).await {
                        Ok(_) => {
                            println!("111");
                        }
                        Err(e) => {
                            let err_msg = e.to_string();
                            ui_weak_clone.upgrade_in_event_loop(move |ui| {
                                ui.invoke_default_state();
                                ui.invoke_show_message(err_msg.into(), true);
                            }).unwrap();
                        }
                    }
                });

                // match 
                // let download_task1 = Arc::clone(&download_task);
                // let notify_clone = Arc::clone(&notify);
                // let tx1 = tx.clone();
                // let ui_weak = ui_weak.clone();

                // download_task1.is_parse_fail.swap(false, Ordering::Relaxed);
                // download_task1.is_pause.swap(false, Ordering::Relaxed);
                // download_task1.is_cancel.swap(false, Ordering::Relaxed);
                // download_task1.failed_segments.lock().await.clear();

                // if download_task1.is_new_download.load(Ordering::Relaxed) {
                //     current_content_length = 0;
                // }

                // // 构建合并MP4参数
                // let merge_config = if request_data.is_merge {
                //     let m3u8_path = request_data.save_path.join(M3U8_FILENAME);
                //     let mp4_path = request_data
                //         .save_path
                //         .join(format!("{}.mp4", request_data.video_name));

                //     let args = vec![
                //         "-allowed_extensions".to_string(),
                //         "ALL".to_string(),
                //         "-i".to_string(),
                //         m3u8_path.to_str().unwrap().to_string(),
                //         "-c".to_string(),
                //         "copy".to_string(),
                //         "-y".to_string(),
                //         mp4_path.to_str().unwrap().to_string(),
                //     ];

                //     Some(MergeConfig {
                //         args,
                //         delete_segment: request_data.is_delete_segment,
                //         save_path: request_data.save_path.clone(),
                //     })
                // } else {
                //     None
                // };

                // let task = tokio::spawn(async move {
                //     let download_task2 = Arc::clone(&download_task1);

                //     // 解析下载
                //     if let Err(e) =
                //         parse_and_download(Arc::clone(&download_task1), request_data, tx1.clone(), Arc::clone(&notify_clone))
                //             .await
                //     {
                //         download_task2.is_parse_fail.swap(true, Ordering::Relaxed);
                //         let err_msg = e.to_string();
                //         ui_weak
                //             .upgrade_in_event_loop(move |ui| {
                //                 ui.invoke_show_message(err_msg.into(), true);
                //                 ui.set_is_downloading(false);
                //                 ui.set_enable_start_btn(true);
                //             })
                //             .unwrap();
                //     }

                //     // 下载任务完成，等待释放
                //     tx1.send(ChannelMessage::ReleaseTask).await.unwrap();

                //     // 解析失败
                //     if download_task1.is_parse_fail.load(Ordering::Relaxed) {
                //         return;
                //     }

                //     // 暂停处理
                //     // if download_task1.is_pause.load(Ordering::Relaxed) {
                //     //     ui_weak
                //     //         .upgrade_in_event_loop(move |ui| {
                //     //             ui.invoke_show_message("已暂停".into(), false);
                //     //             ui.set_is_pause(true);
                //     //             ui.set_enable_start_btn(true);
                //     //         })
                //     //         .unwrap();
                //     //     return;
                //     // }

                //     // 取消处理
                //     if download_task1.is_cancel.load(Ordering::Relaxed) {
                //         reset_download_status(
                //             &ui_weak,
                //             &download_task1,
                //             SharedString::from("已取消"),
                //             true,
                //         )
                //         .await;
                //         return;
                //     }

                //     // 正常下载结束
                //     let (downloaded_segments, segment_nums) = {
                //         let guard = download_task1.downloaded_segments.lock().await;
                //         (guard.clone(), download_task1.segments.lock().await.len())
                //     };
                //     if downloaded_segments.len() == segment_nums {
                //         let mut message = String::from("所有分片已下载完毕");
                //         // 合并为MP4
                //         if let Some(config) = merge_config {
                //             ui_weak
                //                 .upgrade_in_event_loop(move |ui| {
                //                     ui.invoke_show_message("正在合并为MP4...".into(), false);
                //                 })
                //                 .unwrap();
                //             // 使用ffmpeg合并为MP4
                //             match merge_and_delete(
                //                 config.args,
                //                 config.delete_segment,
                //                 downloaded_segments,
                //                 config.save_path,
                //             )
                //             .await
                //             {
                //                 Ok(msg) => {
                //                     message = msg;
                //                 }
                //                 Err(e) => match e.kind() {
                //                     io::ErrorKind::NotFound => {
                //                         message = String::from("合并失败：未找到FFmpeg命令");
                //                     }
                //                     _ => {
                //                         message = e.to_string();
                //                     }
                //                 },
                //             }
                //         }
                //         reset_download_status(
                //             &ui_weak,
                //             &download_task1,
                //             SharedString::from(message),
                //             false,
                //         )
                //         .await;
                //     } else {
                //         // 有分片下载失败了
                //         reset_download_status(
                //             &ui_weak,
                //             &download_task1,
                //             SharedString::new(),
                //             false,
                //         )
                //         .await;
                //     }
                // });
                // *master_task.lock().await = Some(task);
            }
            // 解析完毕，准备下载
            ChannelMessage::PrepareDownloadSegment {
                total_segment_nums,
                has_master_playlist,
            } => {
                let msg = format!("正在下载分片{}...", if has_master_playlist { "（已选择最高分辨率）" } else { "" });

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message(msg.into(), false);
                        ui.set_total_nums(total_segment_nums as i32);
                        ui.invoke_downloading_state();
                    })
                    .unwrap();
            }
            // 某个分片下载完成
            ChannelMessage::SegmentDownloaded { content_length} => {
                current_content_length += content_length;
                downloaded_nums += 1;

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.set_downloaded_nums(downloaded_nums as i32);
                        ui.set_content_length(byte_convert(current_content_length).into());
                    })
                    .unwrap();
            }
            // 暂停
            ChannelMessage::Pause => {
                // download_task.is_pause.swap(true, Ordering::Relaxed);
                download_state.store(2, Ordering::Relaxed);
                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message("你已暂停下载".into(), false);
                    })
                    .unwrap();
            }
            // 继续
            ChannelMessage::Continue => {
                // download_task.is_pause.swap(false, Ordering::Relaxed);
                download_state.store(1, Ordering::Relaxed);
                notify.notify_waiters();

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message("继续下载...".into(), false);
                        ui.invoke_downloading_state();
                    })
                    .unwrap();
            }
            // 取消
            ChannelMessage::Cancel => {
                // download_task.is_cancel.swap(true, Ordering::Relaxed);
                download_state.store(3, Ordering::Relaxed);

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_show_message("你已取消下载".into(), false);
                        ui.invoke_default_state();
                    })
                    .unwrap();
            }
            // 释放任务
            // ChannelMessage::ReleaseTask => {
            //     // if let Some(task) = master_task.lock().await.take() {
            //     //     task.await.unwrap();
            //     //     println!("ReleaseTask");
            //     // }
            // }
        }
    }
}

// 解析+下载
async fn start_parse_download(
    download_task: Arc<Mutex<DownloadTask>>,
    request_data: RequestData,
    tx: mpsc::Sender<ChannelMessage>,
    notify: Arc<Notify>,
    download_state: Arc<AtomicU8>,
) -> Result<(), Box<dyn Error>> {
    // 总分片（含KEY）、是否存在大师列表
    let (segments, has_master_playlist) = parse_m3u8(&request_data.m3u8_url, &request_data.save_path, request_data.timeout).await?;
    let total_segment_nums = segments.len() as u32;

    download_task.lock().await.total_segment_nums = total_segment_nums;

    let client = Arc::new(Client::builder().connect_timeout(Duration::from_secs(request_data.timeout)).user_agent(APP_USER_AGENT).build()?);

    let failed_filepath = request_data.save_path.join(FAILED_FILENAME);
    if failed_filepath.is_file() {
        fs::remove_file(failed_filepath).await?;
    }

    tx.send(ChannelMessage::PrepareDownloadSegment { total_segment_nums, has_master_playlist }).await?;

    let mut futures = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(request_data.concurrency));

    for segment in segments {
        let client_clone = Arc::clone(&client);
        let download_task_clone = Arc::clone(&download_task);
        let semaphore_clone = Arc::clone(&semaphore);
        let notify_clone = Arc::clone(&notify);
        let download_state_clone = Arc::clone(&download_state);
        let tx_clone = tx.clone();

        let future = async move {
            loop {
                // 获取一个信号量许可
                let _permit = semaphore_clone.acquire().await.unwrap();

                // 暂停
                if download_state_clone.load(Ordering::Relaxed) == 2 {
                    notify_clone.notified().await;
                }

                // 取消
                if download_state_clone.load(Ordering::Relaxed) == 3 {
                    break;
                }

                download_single_segment(segment, client_clone, download_task_clone, request_data.retry, tx_clone).await;
                break;
            }
        };

        futures.push(future);
    }

    while let Some(_) = futures.next().await {}

    Ok(())
    // let mut exist_max_resolution = false;

    // // 初始化待下载的分片和总分片数
    // let (segments, total_nums) = if download_task.is_new_download.load(Ordering::Relaxed) {
    //     // 获取并解析M3U8文件
    //     let (segments, is_exist_max_resolution) = parse_m3u8(
    //         &request_data.m3u8_url,
    //         &request_data.save_path,
    //         request_data.timeout,
    //     )
    //     .await?;
    //     exist_max_resolution = is_exist_max_resolution;
    //     let len = segments.len();
    //     *download_task.segments.lock().await = segments.clone();
    //     (segments, len)
    // } else {
    //     let total_segments = download_task.segments.lock().await;
    //     let len = total_segments.len();
    //     let downloaded_segments = download_task.downloaded_segments.lock().await;
    //     (
    //         total_segments
    //             .iter()
    //             .filter(|&item| !downloaded_segments.contains(&item.segment_name))
    //             .cloned()
    //             .collect(),
    //         len,
    //     )
    // };

    // let failed_filepath = request_data.save_path.join(FAILED_FILENAME);
    // if failed_filepath.is_file() {
    //     fs::remove_file(failed_filepath).await.unwrap();
    // }

    // let client = Arc::new(
    //     Client::builder()
    //         .connect_timeout(Duration::from_secs(request_data.timeout))
    //         .user_agent(APP_USER_AGENT)
    //         .build()?,
    // );

    // // 通知UI，正在下载分片
    // let _ = tx
    //     .send(ChannelMessage::PrepareDownloadSegment {
    //         total_nums,
    //         exist_max_resolution,
    //     })
    //     .await;

    // let mut futures = FuturesUnordered::new();
    // // 创建信号量，限制最大并发数
    // let semaphore = Arc::new(Semaphore::new(request_data.concurrency));

    // for segment in segments {
        // let client = Arc::clone(&client);
        // let tx = tx.clone();
        // let download_task1 = Arc::clone(&download_task);
        // let semaphore = Arc::clone(&semaphore);
        // let notify_clone = Arc::clone(&notify);

    //     futures.push(async move {
    //         loop {
    //             // 获取一个信号量许可
    //             let _permit = semaphore.acquire().await.unwrap();

    //             if download_task1.is_pause.load(Ordering::Relaxed) {
    //                 notify_clone.notified().await;
    //             }
                
    //             download_single_segment(segment, client, download_task1, request_data.retry, tx).await;
    //             break;
    //         }
    //     });
    // }
    // while let Some(_) = futures.next().await {}
    
    // 任务构造器
    // let create_task = |segment: Segment| {
    //     let client = Arc::clone(&client);
    //     let tx = tx.clone();
    //     let download_task = Arc::clone(&download_task);

    //     async move {
    //         download_single_segment(segment, client, download_task, request_data.retry, tx).await
    //     };

    //     // async move {
    //     //     // let res = timeout(Duration::from_millis(30), download_single_segment(segment, client, download_task, request_data.retry, tx)).await?;
    //     //     // match res {
    //     //     //     Ok(r) => r,
    //     //     //     Err(_) => Err("下载超时".into()),
    //     //     // }
    //     //     res
    //     // }
    // };

    // 初始填充并发槽位
    // for _ in 0..request_data.concurrency {
    //     if let Some(segment) = segments.pop() {
    //         futures.push(create_task(segment));
    //         // futures.push(create_task(segment));
    //     } else {
    //         break;
    //     }
    // }

    // // 动态调度
    // while let Some(result) = futures.next().await {
    //     if let Some(segment) = segments.pop() {
    //         // futures.push(create_task(segment));
    //     }
    // }

    // let mut tasks = Vec::with_capacity(segments.len());
    /*
    for item in segments {
        let semaphore = Arc::clone(&semaphore);
        let client = Arc::clone(&client);
        let tx = tx.clone();
        let download_task = Arc::clone(&download_task);

        tasks.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            if download_task.is_pause.load(Ordering::Relaxed)
                || download_task.is_cancel.load(Ordering::Relaxed)
            {
                return;
            }

            // 带重试的下载
            let mut is_finish = false;
            for attempt in 0..request_data.retry {
                if let Ok(resp) = client.get(&item.download_url).send().await
                    && resp.status().is_success()
                {
                    let content_length = resp.content_length().unwrap_or(0);
                    if let Ok(bytes) = resp.bytes().await {
                        fs::write(item.save_path.join(&item.segment_name), bytes)
                            .await
                            .unwrap();
                        let downloaded_nums = {
                            let mut downloaded = download_task.downloaded_segments.lock().await;
                            downloaded.push(item.segment_name.to_string());
                            downloaded.len()
                        } as i32;
                        let _ = tx
                            .send(ChannelMessage::SegmentDownloaded {
                                downloaded_nums,
                                content_length,
                            })
                            .await;
                        is_finish = true;
                        break;
                    }
                }

                // 延迟请求
                if attempt < request_data.retry - 1 {
                    sleep(Duration::from_millis(200)).await;
                }
            }

            if !is_finish {
                record_failed_file(&download_task, &item).await;
            }
        }));
    }

    // 等待任务完成
    for task in tasks {
        let _ = task.await;
    }
    */

    // Ok(())
}

async fn download_single_segment(
    segment: Segment,
    client: Arc<Client>,
    download_task: Arc<Mutex<DownloadTask>>,
    retry: u32,
    tx: mpsc::Sender<ChannelMessage>,
) {
    let mut is_finish = false;

    for attempt in 0..retry {
        if let Ok(resp) = client.get(&segment.download_url).send().await && resp.status().is_success() {
            let content_length = resp.content_length().unwrap_or(0);
            if let Ok(bytes) = resp.bytes().await && fs::write(segment.save_path.join(&segment.segment_name), bytes).await.is_ok() {
                let _ = tx.send(ChannelMessage::SegmentDownloaded { content_length }).await;
                is_finish = true;
                break;
                // println!("延迟500ms");
                // sleep(Duration::from_millis(500)).await;
                // let downloaded_nums = {
                //     let mut downloaded = download_task.downloaded_segments.lock().await;
                //     downloaded.push(segment.segment_name.to_string());
                //     downloaded.len()
                // } as i32;
                // let _ = tx
                //     .send(ChannelMessage::SegmentDownloaded {
                //         downloaded_nums,
                //         content_length,
                //     })
                //     .await;
                // is_finish = true;
                // break;
            }
        }

        if attempt < retry - 1 {
            sleep(Duration::from_millis(200)).await;
        }
    }

    if !is_finish {
        record_failed_file(&download_task, &segment).await;
    }
}

// 记录下载失败的分片
async fn record_failed_file(
    download_task: &Arc<Mutex<DownloadTask>>,
    segment: &Segment,
) {
    {
        let mut dt = download_task.lock().await;
        dt.failed_segment_nums += 1;
    }

    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(segment.save_path.join(FAILED_FILENAME)).await {
        let _ = file.write_all(format!("{},{}\n", segment.segment_name, segment.download_url).as_bytes()).await;
    }
}

// 格式化大小
fn byte_convert(size: u64) -> String {
    if size < 1024 {
        return format!("{size} B");
    }

    if size == 1024 {
        return String::from("1 KB");
    }

    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit_index = 0;

    while size > 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

// 获取M3U8内容，验证content-type
async fn fetch_m3u8_content(client: &Client, url: &str) -> Result<String, Box<dyn Error>> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("请求失败：状态码 {}", resp.status()).into());
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let is_valid_mime = content_type == "application/vnd.apple.mpegurl"
        || content_type == "application/x-mpegURL"
        || content_type == "audio/mpegurl";

    if !is_valid_mime {
        return Err("非 M3U8 格式".into());
    }

    Ok(resp.text().await?)
}

// 构建绝对URL
fn build_abs_url(base: &Url, uri: &str) -> Result<Url, Box<dyn Error>> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        Ok(Url::parse(uri)?)
    } else {
        Ok(base.join(uri)?)
    }
}

// 解析M3U8
async fn parse_m3u8(
    m3u8_url: &str,
    save_path: &Arc<Path>,
    timeout: u64,
) -> Result<(Vec<Segment>, bool), Box<dyn Error>> {
    let mut base_url = Url::parse(m3u8_url)?.join(".")?;

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(timeout))
        .user_agent(APP_USER_AGENT)
        .build()?;

    let mut content = fetch_m3u8_content(&client, m3u8_url).await?;
    // 默认不是大师列表
    let mut is_master_playlist = false;

    // 处理多个分辨率
    if content.contains("#EXT-X-STREAM-INF") {
        let best_url = parse_m3u8_master(&content);

        if best_url.is_empty() {
            return Err("无效的多分辨率播放列表".into());
        }

        let final_url = build_abs_url(&base_url, &best_url)?;

        // 重新构建base url
        base_url = Url::parse(final_url.as_str())?.join(".")?;
        // 获取高分辨率内容
        content = fetch_m3u8_content(&client, final_url.as_str()).await?;
        is_master_playlist = true;
    }

    let mut writer = BufWriter::new(File::create(save_path.join(M3U8_FILENAME)).await?);
    let mut segments: Vec<Segment> = Vec::with_capacity(content.lines().count() / 3);
    let mut index = 0;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        // key
        if line.starts_with("#EXT-X-KEY") {
            let key = line
                .split("URI=\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap_or("");
            let download_url = build_abs_url(&base_url, key)?.to_string();
            segments.push(Segment {
                segment_name: "key.key".to_string(),
                save_path: save_path.clone(),
                download_url,
            });
            writer
                .write_all(format!("{}\n", line.replace(key, "key.key")).as_bytes())
                .await?;
        } else if !line.starts_with("#") {
            let download_url = build_abs_url(&base_url, line)?.to_string();
            let segment_name = format!("index{}.ts", index);
            segments.push(Segment {
                segment_name: segment_name.to_owned(),
                save_path: save_path.clone(),
                download_url,
            });
            writer
                .write_all(format!("{}\n", segment_name).as_bytes())
                .await?;
            index += 1;
        } else {
            writer.write_all(format!("{}\n", line).as_bytes()).await?;
        }
    }

    writer.flush().await.unwrap();
    drop(writer);

    if segments.is_empty() {
        return Err("无分片可下载".into());
    }

    Ok((segments, is_master_playlist))
}

// 提取最高分辨率的url
fn parse_m3u8_master(content: &str) -> String {
    let re = Regex::new(r"BANDWIDTH=(\d+),RESOLUTION=(\d+)x(\d+)").unwrap();

    let mut best_url = String::default();
    let mut best_bandwidth = 0;
    let mut best_resolution_sum = 0;

    let mut lines = content.lines().filter(|line| !line.is_empty());
    while let Some(line) = lines.next() {
        if line.starts_with("#EXT-X-STREAM-INF")
            && let Some(caps) = re.captures(line)
        {
            let bandwidth: u32 = caps[1].parse().unwrap_or(0);
            let resolution_sum: u32 = caps[2].parse().unwrap_or(0) + caps[3].parse().unwrap_or(0);

            if let Some(url_line) = lines.next()
                && (resolution_sum > best_resolution_sum
                    || (resolution_sum == best_resolution_sum && bandwidth > best_bandwidth))
            {
                best_resolution_sum = resolution_sum;
                best_bandwidth = bandwidth;
                best_url = url_line.to_string();
            }
        }
    }

    best_url
}

// 安全的创建保存目录
async fn create_safe_save_path(
    work_dir: &SharedString,
    video_name: &SharedString,
) -> Option<(Arc<Path>, String)> {
    if video_name.is_empty() {
        return None;
    }

    // 只取文件名部分（去除路径）
    let safe_name = Path::new(video_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(video_name);

    // 移除非法字符
    let re = Regex::new(r#"[<>:"/\\|?*]"#).unwrap();
    let clean_name = re.replace_all(safe_name, "_");
    // 中文下最多50个字符
    let clean_name = if clean_name.len() > 150 {
        &clean_name[..150]
    } else {
        &clean_name
    };

    // 创建保存目录
    let save_path = Path::new(work_dir).join(clean_name);
    if !save_path.is_dir() {
        fs::create_dir_all(&save_path).await.unwrap();
    }

    Some((save_path.into(), clean_name.to_string()))
}

// 取消或正常结束下载时（含下载失败的文件）重置UI
async fn reset_download_status(
    ui_weak: &slint::Weak<AppWindow>,
    download_task: &Arc<DownloadTask>,
    message: SharedString,
    is_cancel_reset: bool,
) {
    // 非取消下载时计算失败文件数
    // let failed_file_nums = if !is_cancel_reset {
    //     download_task.failed_segments.lock().await.len()
    // } else {
    //     0
    // };

    // 构建最终消息
    // let final_message = if failed_file_nums > 0 {
        // format!("有{}个分片无法下载", failed_file_nums,).into()
    // } else {
        // message
    // };

    // download_task.downloaded_segments.lock().await.clear();
    // download_task.is_new_download.swap(true, Ordering::Relaxed);

    ui_weak
        .upgrade_in_event_loop(move |ui| {
            // ui.invoke_show_message(final_message, failed_file_nums > 0);
            ui.set_enable_start_btn(true);
            ui.set_enable_pause_btn(false);
            ui.set_enable_cancel_btn(false);
            // ui.set_is_downloading(false);
            ui.set_is_pause(false);
            // ui.set_has_failed_file(failed_file_nums > 0);

            if is_cancel_reset {
                ui.set_total_nums(0);
                ui.set_downloaded_nums(0);
            }
        })
        .unwrap();
}

// 合并为MP4并删除分片
async fn merge_and_delete(
    args: Vec<String>,
    is_delete_segment: bool,
    downloaded_segments: Vec<String>,
    save_path: Arc<Path>,
) -> Result<String, io::Error> {
    let mut cmd = Command::new("ffmpeg");
    cmd.args(args);

    #[cfg(windows)]
    cmd.creation_flags(0x08000000);

    let status = cmd.spawn()?.wait().await?;

    // 合并失败
    if !status.success() {
        return Err(io::Error::other("合并失败"));
    }

    let mut msg = String::from("合并完成");

    // 勾选了要删除分片
    if is_delete_segment {
        let mut tasks = FuturesUnordered::new();
        let mut deleted = 0;

        // 删除m3u8
        let _ = fs::remove_file(save_path.join(M3U8_FILENAME)).await;

        // 删除分片（含key，若有）
        for segment_name in downloaded_segments {
            let segment_filepath = save_path.join(segment_name);
            if segment_filepath.is_file() {
                tasks.push(async move { fs::remove_file(&segment_filepath).await.ok() });
                if tasks.len() >= 20 && tasks.next().await.is_some() {
                    deleted += 1;
                }
            }
        }

        while let Some(success) = tasks.next().await {
            if success.is_some() {
                deleted += 1;
            }
        }

        msg.push_str(&format!("，已删除 {} 个分片", deleted));
    }

    Ok(msg)
}
