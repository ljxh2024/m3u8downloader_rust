use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use slint::{PhysicalPosition, SharedString};
use std::{
    error::Error,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
    },
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncWriteExt, BufWriter},
    process::Command,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{Duration, sleep},
};
use url::Url;
use winsafe::{GetSystemMetrics, co::SM};

// 匹配无效文件名/路径
static INVALID_FILENAME_RE: Lazy<regex::Regex> =
    Lazy::new(|| Regex::new(r#"[<>:"/\\|?*]"#).unwrap());
// 匹配大师列表
static MASTER_RE: Lazy<regex::Regex> =
    Lazy::new(|| Regex::new(r"BANDWIDTH=(\d+),RESOLUTION=(\d+)x(\d+)").unwrap());

slint::include_modules!();

// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";
// M3U8文件名
const M3U8_FILENAME: &str = "index.m3u8";
// :todo USER-AGENT，后期引入请求头后改为自定义
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

        move || {
            let ui = ui_weak.unwrap();

            let tx_start = tx_start.clone();
            slint::spawn_local(async_compat::Compat::new(async move {
                // 保存目录、视频名称
                let (save_path, video_name) =
                    match create_safe_save_path(&ui.get_work_dir(), &ui.get_video_name()).await {
                        Ok((save_path, video_name)) => (save_path, video_name),
                        Err(e) => {
                            ui.invoke_show_message(e.to_string().into(), true);
                            ui.set_enable_start_btn(true);
                            ui.set_in_progress(false);
                            return;
                        }
                    };

                let _ = tx_start
                    .send(ChannelMessage::ParseDownload(RequestData {
                        video_name,
                        save_path,
                        m3u8_url: ui.get_m3u8_url().into(),
                        concurrency: ui.get_concurrency().parse::<usize>().unwrap_or(4),
                        retry: ui.get_retry().parse().unwrap_or(3) + 1, // 加1确保至少执行一次
                        connect_timeout: ui.get_connect_timeout().parse::<u64>().unwrap_or(3),
                        is_merge: ui.get_is_merge(),
                        is_delete_segment: ui.get_is_delete_segment(),
                    }))
                    .await;
            }))
            .unwrap();
        }
    });

    // 暂停下载
    window.on_pause_download({
        let ui_weak = window.as_weak();
        let tx_pause = tx.clone();

        move || {
            let ui = ui_weak.unwrap();

            ui.set_enable_pause_btn(false);
            ui.set_enable_cancel_btn(false);
            ui.invoke_show_message("正在暂停...".into(), false);

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

            ui.set_enable_start_btn(false);
            ui.set_enable_pause_btn(false);
            ui.set_enable_cancel_btn(false);
            ui.invoke_show_message("正在取消...".into(), false);

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

    // 打开下载失败的文件
    window.on_open_failed_file({
        let ui_weak = window.as_weak();
        move || {
            let ui = ui_weak.unwrap();

            let file_path = Path::new(&ui.get_work_dir())
                .join(ui.get_video_name())
                .join(FAILED_FILENAME);
            if file_path.exists() {
                #[cfg(windows)]
                {
                    let path_str = file_path.to_string_lossy();
                    Command::new("explorer")
                        .arg(format!("/select,{}", path_str))
                        .spawn()
                        .ok();
                }
            }
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

/// 信道消息类型
enum ChannelMessage {
    ParseDownload(RequestData),
    Parsed {
        // 总分片数
        total_segment_nums: usize,
        // 是否大师列表
        is_master_playlist: bool,
    },
    Pause,
    Cancel,
    ReleaseTask,
    Downloaded(u64),
}

/// 下载参数
struct RequestData {
    video_name: String,
    save_path: Arc<Path>,
    m3u8_url: String,
    concurrency: usize,
    retry: u32,
    connect_timeout: u64,
    is_merge: bool,
    is_delete_segment: bool,
}

/// 分片信息
#[derive(Clone)]
struct Segment {
    name: String,
    save_path: Arc<Path>,
    download_url: String,
}

/// 下载任务配置
struct DownloadTask {
    // 待下载的分片
    segments: Mutex<Vec<Segment>>,
    // 已下载的分片，存储分片名，用以删除分片
    downloaded_segments: Mutex<Vec<String>>,
    // 已下载分片数
    downloaded_nums: AtomicU32,
    // 0未下载或取消下载，1下载中，2暂停
    state: AtomicU8,
    // 是否大师列表
    is_master_playlist: AtomicBool,
}

impl DownloadTask {
    fn new() -> Self {
        Self {
            segments: Mutex::new(Vec::new()),
            downloaded_segments: Mutex::new(Vec::new()),
            downloaded_nums: AtomicU32::new(0),
            state: AtomicU8::new(0),
            is_master_playlist: AtomicBool::new(false),
        }
    }
}

/// 监控信道，处理下载、暂停、取消
async fn loop_receive_message(
    tx: mpsc::Sender<ChannelMessage>,
    rx: &mut mpsc::Receiver<ChannelMessage>,
    ui_weak: slint::Weak<AppWindow>,
) {
    // 初始化任务配置
    let download_task = Arc::new(DownloadTask::new());
    // 当前已下载的文件大小
    let mut current_content_length = 0u64;
    // 下载任务句柄
    let master_task: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

    while let Some(channel_message) = rx.recv().await {
        match channel_message {
            // 解析下载
            ChannelMessage::ParseDownload(request_data) => {
                let ui_weak_clone = ui_weak.clone();
                let tx_clone = tx.clone();
                let download_task_clone = Arc::clone(&download_task);

                if download_task_clone.state.load(Ordering::Relaxed) == 0 {
                    download_task_clone.downloaded_segments.lock().await.clear();
                    download_task_clone
                        .downloaded_nums
                        .store(0, Ordering::Relaxed);
                    current_content_length = 0;
                }

                // 开启解析下载任务
                let task = tokio::spawn(async move {
                    let mut err_flag = false;

                    if let Err(e) =
                        start_parse_download(&download_task_clone, &request_data, tx_clone.clone())
                            .await
                    {
                        err_flag = true;
                        let err_msg = e.to_string();
                        let state = download_task_clone.state.load(Ordering::Relaxed);

                        ui_weak_clone
                            .upgrade_in_event_loop(move |ui| {
                                ui.invoke_show_message(err_msg.into(), true);
                                ui.set_enable_start_btn(true);
                                ui.set_in_progress(false);
                                ui.set_is_pause(state == 2);
                            })
                            .unwrap();
                    }

                    tx_clone.send(ChannelMessage::ReleaseTask).await.unwrap();

                    if err_flag {
                        return;
                    }

                    // 暂停下载
                    if download_task_clone.state.load(Ordering::Relaxed) == 2 {
                        ui_weak_clone
                            .upgrade_in_event_loop(move |ui| {
                                ui.invoke_paused_state();
                            })
                            .unwrap();
                        return;
                    }

                    // 取消下载
                    if download_task_clone.state.load(Ordering::Relaxed) == 0 {
                        ui_weak_clone
                            .upgrade_in_event_loop(move |ui| {
                                ui.invoke_canceled_state();
                            })
                            .unwrap();
                        return;
                    }

                    // 任务正常结束，重置下载状态
                    download_task_clone.state.store(0, Ordering::Relaxed);

                    // 构建最终消息
                    let mut final_msg = String::from("所有分片已下载完毕");

                    // 已下载的分片、失败分片数
                    let (downloaded_segments, failed_nums) = {
                        let downloaded = download_task_clone.downloaded_segments.lock().await;
                        (
                            downloaded.clone(),
                            download_task_clone.segments.lock().await.len() - downloaded.len(),
                        )
                    };

                    if failed_nums > 0 {
                        final_msg = format!("有 {} 个分片下载失败", failed_nums);
                    } else {
                        // 合并为MP4
                        if request_data.is_merge {
                            ui_weak_clone
                                .upgrade_in_event_loop(move |ui| {
                                    ui.invoke_show_message("正在合并分片...".into(), false);
                                })
                                .unwrap();

                            let m3u8_path = request_data
                                .save_path
                                .join(M3U8_FILENAME)
                                .to_string_lossy()
                                .to_string();
                            let mp4_path = request_data
                                .save_path
                                .join(format!("{}.mp4", request_data.video_name))
                                .to_string_lossy()
                                .to_string();
                            // 构建合并参数
                            let args = vec![
                                "-allowed_extensions",
                                "ALL",
                                "-i",
                                &m3u8_path,
                                "-c",
                                "copy",
                                "-y",
                                &mp4_path,
                            ];

                            match merge_and_delete(
                                args,
                                request_data.is_delete_segment,
                                &downloaded_segments,
                                &request_data.save_path,
                            )
                            .await
                            {
                                Ok(msg) => {
                                    final_msg = msg;
                                }
                                Err(e) => match e.kind() {
                                    io::ErrorKind::NotFound => {
                                        final_msg = String::from("未找到FFmpeg命令");
                                    }
                                    _ => final_msg = e.to_string(),
                                },
                            }
                        }
                    }

                    // 更新UI
                    ui_weak_clone
                        .upgrade_in_event_loop(move |ui| {
                            ui.invoke_downloaded_state(final_msg.into(), failed_nums > 0);
                        })
                        .unwrap();
                });
                *master_task.lock().await = Some(task);
            }
            // 解析完毕，准备下载
            ChannelMessage::Parsed {
                total_segment_nums,
                is_master_playlist,
            } => {
                let msg = format!(
                    "正在下载分片{}...",
                    if is_master_playlist {
                        "（已选择最高分辨率）"
                    } else {
                        ""
                    }
                );

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.invoke_parsed_state(msg.into(), total_segment_nums as i32);
                    })
                    .unwrap();
            }
            // 某个分片下载完成
            ChannelMessage::Downloaded(content_length) => {
                current_content_length += content_length;
                let downloaded_nums = download_task
                    .downloaded_nums
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;

                ui_weak
                    .upgrade_in_event_loop(move |ui| {
                        ui.set_downloaded_nums(downloaded_nums as i32);
                        ui.set_content_length(format_size(current_content_length).into());
                    })
                    .unwrap();
            }
            // 暂停
            ChannelMessage::Pause => {
                download_task.state.store(2, Ordering::Release);
            }
            // 取消
            ChannelMessage::Cancel => {
                let old_state = download_task.state.swap(0, Ordering::AcqRel);
                // 暂停时，下载任务已释放，需显式更新UI
                if old_state == 2 {
                    ui_weak
                        .upgrade_in_event_loop(move |ui| {
                            ui.invoke_canceled_state();
                        })
                        .unwrap();
                }
            }
            // 等待下载任务完成
            ChannelMessage::ReleaseTask => {
                if let Some(task) = master_task.lock().await.take() {
                    task.await.unwrap();
                }
            }
        }
    }
}

/// 解析+下载
async fn start_parse_download(
    download_task: &Arc<DownloadTask>,
    request_data: &RequestData,
    tx: mpsc::Sender<ChannelMessage>,
) -> Result<(), Box<dyn Error>> {
    // 在这里验证并发数和连接超时
    if request_data.concurrency < 1 {
        return Err("并发数不能小于1".into());
    }
    if request_data.connect_timeout < 1 {
        return Err("连接超时不能小于1秒".into());
    }

    // 待下载的分片、总分片数
    let (wait_download_segments, total_segment_nums, is_master_playlist) =
        if download_task.state.load(Ordering::Relaxed) == 2 {
            // 当前是暂停状态，任务类型是恢复下载
            let segments = download_task.segments.lock().await;
            let downloaded_segments = download_task.downloaded_segments.lock().await;

            (
                segments
                    .iter()
                    .filter(|&item| !downloaded_segments.contains(&item.name))
                    .cloned()
                    .collect(),
                segments.len(),
                download_task.is_master_playlist.load(Ordering::Relaxed),
            )
        } else {
            // 新下载任务
            let (segments, is_master_playlist) = parse_m3u8(
                &request_data.m3u8_url,
                &request_data.save_path,
                request_data.connect_timeout,
            )
            .await?;
            let len = segments.len();

            *download_task.segments.lock().await = segments.clone();
            download_task
                .is_master_playlist
                .store(is_master_playlist, Ordering::Relaxed);

            (segments, len, is_master_playlist)
        };

    let client = Arc::new(
        Client::builder()
            .connect_timeout(Duration::from_secs(request_data.connect_timeout))
            .user_agent(APP_USER_AGENT)
            .build()?,
    );

    let failed_filepath = request_data.save_path.join(FAILED_FILENAME);
    if failed_filepath.is_file() {
        fs::remove_file(failed_filepath).await?;
    }

    tx.send(ChannelMessage::Parsed {
        total_segment_nums,
        is_master_playlist,
    })
    .await?;

    // 真正的并发下载开始
    download_task.state.store(1, Ordering::Relaxed);
    // 并发数
    let concurrency = request_data.concurrency.min(wait_download_segments.len());
    let segments_iter = wait_download_segments.into_iter();

    // 使用for_each_concurrent控制并发
    futures::stream::iter(segments_iter)
        .for_each_concurrent(concurrency, move |segment| {
            let client = Arc::clone(&client);
            let tx = tx.clone();

            async move {
                if download_task.state.load(Ordering::Acquire) != 1 {
                    return;
                }

                download_single_segment(
                    segment,
                    &client,
                    download_task,
                    request_data.retry,
                    tx.clone(),
                )
                .await;
            }
        })
        .await;

    Ok(())
}

/// 下载单个分片
async fn download_single_segment(
    segment: Segment,
    client: &Client,
    download_task: &Arc<DownloadTask>,
    retry: u32,
    tx: mpsc::Sender<ChannelMessage>,
) {
    let mut is_finish = false;

    for attempt in 0..retry {
        if let Ok(resp) = client.get(&segment.download_url).send().await
            && resp.status().is_success()
        {
            let content_length = resp.content_length().unwrap_or(0);

            // 使用流式写入
            match File::create(segment.save_path.join(&segment.name)).await {
                Ok(file) => {
                    let mut ok = true;
                    let mut writer = BufWriter::new(file);
                    let mut stream = resp.bytes_stream();

                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(chunk) => {
                                if writer.write_all(&chunk).await.is_err() {
                                    ok = false;
                                    break;
                                }
                            }
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }

                    let _ = writer.flush().await;

                    if ok {
                        // 记录已下载分片名
                        download_task
                            .downloaded_segments
                            .lock()
                            .await
                            .push(segment.name.clone());
                        // 更新进度
                        let _ = tx.send(ChannelMessage::Downloaded(content_length)).await;
                        is_finish = true;
                        break;
                    }
                }
                Err(_) => {
                    // 创建文件失败，视为一次失败尝试，继续重试
                }
            }
        }

        if attempt < retry - 1 {
            sleep(Duration::from_millis(500)).await;
        }
    }

    // 记录下载失败的分片
    if !is_finish
        && let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment.save_path.join(FAILED_FILENAME))
            .await
    {
        let _ = file
            .write_all(format!("{},{}\n", segment.name, segment.download_url).as_bytes())
            .await;
    }
}

/// 格式化大小显示
fn format_size(size: u64) -> String {
    if size <= 1024 {
        return String::from("1 KB");
    }

    const UNITS: [&str; 3] = ["KB", "MB", "GB"];
    let mut size = size as f64;
    let mut unit_index = 0;

    while size > 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

/// 获取M3U8内容，并做基础验证
async fn fetch_m3u8_content(client: &Client, url: &str) -> Result<String, Box<dyn Error>> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("{}", resp.status()).into());
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();
    let text = resp.text().await?;

    if !content_type.contains("mpegurl") && !text.contains("#EXTM3U") && !text.contains("#EXTINF") {
        return Err("非M3U8格式".into());
    }

    Ok(text)
}

/// 构建绝对URL
fn build_abs_url(base: &Url, uri: &str) -> Result<Url, Box<dyn Error>> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        Ok(Url::parse(uri)?)
    } else {
        Ok(base.join(uri)?)
    }
}

/// 解析M3U8，返回总分片和是否大师列表
async fn parse_m3u8(
    m3u8_url: &str,
    save_path: &Arc<Path>,
    connect_timeout: u64,
) -> Result<(Vec<Segment>, bool), Box<dyn Error>> {
    let mut base_url = Url::parse(m3u8_url)?.join(".")?;

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(connect_timeout))
        .user_agent(APP_USER_AGENT)
        .build()?;

    let mut content = fetch_m3u8_content(&client, m3u8_url).await?;
    // 默认不是大师列表
    let mut is_master_playlist = false;

    // 处理多个分辨率
    if content.contains("#EXT-X-STREAM-INF") {
        let best_url = parse_m3u8_master(&content);

        if best_url.is_empty() {
            return Err("无效的大师列表".into());
        }

        let final_url = build_abs_url(&base_url, &best_url)?;

        // 重新构建base url
        base_url = Url::parse(final_url.as_str())?.join(".")?;
        // 获取高分辨率内容
        content = fetch_m3u8_content(&client, final_url.as_str()).await?;
        is_master_playlist = true;
    }

    let mut writer = BufWriter::new(File::create(save_path.join(M3U8_FILENAME)).await?);
    let segment_count = content
        .lines()
        .filter(|line| !line.starts_with('#'))
        .count();
    let mut segments: Vec<Segment> = Vec::with_capacity(segment_count);
    let mut key_index = 1u32;
    let mut segment_index = 0u32;

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
            let new_key_name = format!("key_{}.key", key_index);

            segments.push(Segment {
                name: new_key_name.clone(),
                save_path: Arc::clone(save_path),
                download_url,
            });

            writer
                .write_all(format!("{}\n", line.replace(key, &new_key_name)).as_bytes())
                .await?;

            key_index += 1;
        } else if !line.starts_with("#") {
            let download_url = build_abs_url(&base_url, line)?.to_string();
            let segment_name = format!("segment_{}.ts", segment_index);

            segments.push(Segment {
                name: segment_name.clone(),
                save_path: Arc::clone(save_path),
                download_url,
            });

            writer
                .write_all(format!("{}\n", segment_name).as_bytes())
                .await?;

            segment_index += 1;
        } else {
            writer.write_all(format!("{}\n", line).as_bytes()).await?;
        }
    }

    writer.flush().await.unwrap();

    if segments.is_empty() {
        return Err("无分片可下载".into());
    }

    Ok((segments, is_master_playlist))
}

/// 提取大师列表最高分辨率的url
fn parse_m3u8_master(content: &str) -> String {
    let mut best_url = String::new();
    let mut best_bandwidth = 0;
    let mut best_resolution_sum = 0;

    let mut lines = content.lines().filter(|line| !line.is_empty());
    while let Some(line) = lines.next() {
        if line.starts_with("#EXT-X-STREAM-INF")
            && let Some(caps) = MASTER_RE.captures(line)
        {
            let bandwidth: u32 = caps
                .get(1)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let w: u32 = caps
                .get(2)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let h: u32 = caps
                .get(3)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let resolution_sum = w.saturating_add(h);

            if let Some(url_line) = lines.next()
                && (resolution_sum > best_resolution_sum
                    || (resolution_sum == best_resolution_sum && bandwidth > best_bandwidth))
            {
                best_resolution_sum = resolution_sum;
                best_bandwidth = bandwidth;
                best_url = url_line.to_owned();
            }
        }
    }

    best_url
}

/// 安全的创建保存目录
///
/// work_dir是手动选择的工作目录，不需要做校验
async fn create_safe_save_path(
    work_dir: &SharedString,
    video_name: &SharedString,
) -> Result<(Arc<Path>, String), Box<dyn Error>> {
    let video_name = video_name.trim();

    if video_name.is_empty() {
        return Err("视频名称不能为空".into());
    }

    // 只取文件名部分（去除路径）
    let safe_name = Path::new(video_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(video_name);

    // 移除非法字符
    let cleaned = INVALID_FILENAME_RE.replace_all(safe_name, "_");
    // 对 UTF-8 做字符级截断，避免截断到半个字符
    let mut clean_name: String = cleaned.chars().collect();

    if clean_name.chars().count() > 150 {
        clean_name = clean_name.chars().take(150).collect();
    }

    // 创建保存目录
    let save_path = Path::new(work_dir).join(&clean_name);
    if !save_path.is_dir() && fs::create_dir_all(&save_path).await.is_err() {
        return Err("无法创建保存目录".into());
    }

    Ok((save_path.into(), clean_name))
}

/// 合并为MP4、删除分片
async fn merge_and_delete(
    args: Vec<&str>,
    is_delete_segment: bool,
    downloaded_segments: &[String],
    save_path: &Arc<Path>,
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

    // 删除所有分片，包括key
    if is_delete_segment {
        // 删除 m3u8 文件
        let _ = fs::remove_file(save_path.join(M3U8_FILENAME)).await;
        // 并发限制
        let delete_concurrency = 20usize;
        // 已删除文件数
        let deleted_counter = Arc::new(AtomicU32::new(0));

        futures::stream::iter(downloaded_segments.iter())
            .for_each_concurrent(delete_concurrency, |item| {
                let deleted_counter = Arc::clone(&deleted_counter);
                async move {
                    let filepath = save_path.join(item);
                    if filepath.is_file() && fs::remove_file(&filepath).await.is_ok() {
                        deleted_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .await;
        msg.push_str(&format!(
            "，已删除 {} 个分片",
            deleted_counter.load(Ordering::Relaxed)
        ));
    }

    Ok(msg)
}
