//! # 下载管理
//!
//! `downloader` 主要管理M3U8的解析和下载

use crate::AppWindow;
use dashmap::DashSet;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::{Client, header::CONTENT_TYPE};
use slint::SharedString;
use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
};
use tokio::{
    fs::{self, File},
    io::{self, AsyncWriteExt, BufWriter},
    process::Command,
    sync::{Mutex, RwLock, mpsc},
    time::{Duration, sleep},
};
use url::Url;

// M3U8文件名
const M3U8_FILENAME: &str = "index.m3u8";
// 删除分片最大并发数
const DELETE_CONCURRENCY: usize = 20;
// 视频名称最大长度
const MAX_VIDEO_NAME_LEN: usize = 50;
// :todo USER-AGENT，后期引入请求头后改为自定义
const APP_USER_AGENT: &str = "Chrome/147";
// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";

// 匹配大师列表
static MASTER_RE: Lazy<regex::Regex> = Lazy::new(|| Regex::new(r"RESOLUTION=(\d+)x(\d+)").unwrap());
// 过滤视频名称
static VIDEO_NAME_RE: Lazy<regex::Regex> = Lazy::new(|| Regex::new(r#"[<>:"/\\|?*]"#).unwrap());

/// 下载状态
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum DownloadState {
    Idle,
    Downloading,
    Paused,
    Canceled,
}

/// 信道消息
#[derive(Debug)]
pub enum ChannelMessage {
    Start {
        total_nums: usize, // 总分片数
        is_new_download: bool,
    },
    Paused,
    Canceled,
    Progress {
        downloaded_nums: u32,    // 已下载的分片数
        downloaded_sizes: usize, // 已下载的大小
    },
    Retry,
    Downloaded {
        message: String,           // 消息
        have_failed_segment: bool, // 是否有下载失败的分片
    },
    Merging,
}

/// 分片信息
#[derive(Debug, Clone)]
pub struct Segment {
    name: String,
    download_url: String,
}

/// 下载管理
#[derive(Debug)]
pub struct DownloadManager {
    download_state: RwLock<DownloadState>,
    segments: Mutex<Vec<Segment>>,
    // 已下载的分片，仅存储分片名称，高并发频繁写入，使用 DashSet 无锁结构
    downloaded_segments: DashSet<String>,
    // 下载失败的分片，任务完成后再写入文件，一般下载失败的概率很小，使用 Mutex 即可
    failed_segments: Mutex<Vec<Segment>>,
    pub save_path: Mutex<PathBuf>, // 保存路径
    downloaded_nums: AtomicU32,
    downloaded_sizes: AtomicUsize,
}

impl DownloadManager {
    /// 创建新的下载任务
    pub fn new() -> Self {
        Self {
            download_state: RwLock::new(DownloadState::Idle),
            segments: Mutex::new(Vec::new()),
            downloaded_segments: DashSet::new(),
            failed_segments: Mutex::new(Vec::new()),
            save_path: Mutex::new(PathBuf::new()),
            downloaded_nums: AtomicU32::new(0),
            downloaded_sizes: AtomicUsize::new(0),
        }
    }

    /// 清空/恢复默认值
    pub async fn clear(&self) {
        // 没有下载失败的分片时才清除 save_path，否则不能打开失败的文件
        if self.failed_segments.lock().await.is_empty() {
            self.save_path.lock().await.clear();
        }

        *self.download_state.write().await = DownloadState::Idle;
        self.segments.lock().await.clear();
        self.downloaded_segments.clear();
        self.failed_segments.lock().await.clear();
        self.downloaded_nums.store(0, Ordering::Relaxed);
        self.downloaded_sizes.store(0, Ordering::Relaxed);
    }

    /// 获取下载状态
    pub async fn get_download_state(&self) -> DownloadState {
        *self.download_state.read().await
    }

    /// 是否空闲
    pub async fn is_idle(&self) -> bool {
        *self.download_state.read().await == DownloadState::Idle
    }

    /// 是否下载中
    pub async fn is_downloading(&self) -> bool {
        *self.download_state.read().await == DownloadState::Downloading
    }

    /// 是否已暂停
    pub async fn is_paused(&self) -> bool {
        *self.download_state.read().await == DownloadState::Paused
    }

    /// 是否已取消
    pub async fn is_canceled(&self) -> bool {
        *self.download_state.read().await == DownloadState::Canceled
    }

    /// 设置下载状态
    pub async fn set_download_state(&self, state: DownloadState) {
        *self.download_state.write().await = state;
    }

    /// 更新下载状态，返回旧值
    pub async fn update_download_state(&self, state: DownloadState) -> DownloadState {
        let old = *self.download_state.read().await;
        *self.download_state.write().await = state;
        old
    }

    /// 执行下载解析和下载任务
    pub async fn download(
        &self,
        config: DownloadConfig,
        tx: mpsc::Sender<ChannelMessage>,
    ) -> Result<(), Box<dyn Error>> {
        let client = Arc::new(
            Client::builder()
                .connect_timeout(Duration::from_secs(config.connect_timeout))
                .user_agent(APP_USER_AGENT)
                .build()?,
        );

        // 新下载任务
        // 待下载的分片、保存路径
        let (segments, path_buf) = if self.is_idle().await {
            let save_path = Path::new(&config.save_path);
            let segments = self
                .parse_m3u8(&config.m3u8_url, Arc::clone(&client), save_path)
                .await?;

            self.segments.lock().await.clone_from(&segments);
            self.save_path.lock().await.clone_from(&config.save_path);

            (segments, config.save_path)
        } else {
            // 继续下载
            (
                self.segments.lock().await.clone(),
                self.save_path.lock().await.clone(),
            )
        };

        let segments_len = segments.len();
        let save_path = Path::new(&path_buf);

        tx.send(ChannelMessage::Start {
            total_nums: segments_len,
            is_new_download: self.is_idle().await,
        })
        .await?;

        // 将下载状态置为下载中
        self.set_download_state(DownloadState::Downloading).await;

        // 并发数
        let concurrency = config.concurrency.min(segments_len);

        // 并发下载，第一次不重试，全部下载后若有下载失败的，再决定是否再集中重试
        self.future_download(
            segments,
            save_path,
            Arc::clone(&client),
            concurrency,
            1,
            tx.clone(),
        )
        .await;

        // 任务因暂停而提前结束，但可能还继续下载
        if self.is_paused().await {
            // 排除已下载的分片
            let new_segments = self
                .segments
                .lock()
                .await
                .iter()
                .filter(|s| !self.downloaded_segments.contains(&s.name))
                .cloned()
                .collect::<Vec<Segment>>();
            // 更新待下载分片
            self.segments.lock().await.clone_from(&new_segments);
            // 清除下载失败的分片，重试时重新下载
            self.failed_segments.lock().await.clear();

            let _ = tx.send(ChannelMessage::Paused).await;
            return Ok(());
        }

        // 任务提前取消
        if self.is_canceled().await {
            self.clear().await;
            let _ = tx.send(ChannelMessage::Canceled).await;
            return Ok(());
        }

        // 任务正常结束

        // 重试
        let failed_segments = {
            let mut guard = self.failed_segments.lock().await;
            let clone = guard.clone();
            guard.clear();
            clone
        };
        if !failed_segments.is_empty() && config.retry > 0 {
            let _ = tx.send(ChannelMessage::Retry).await;
            self.future_download(
                failed_segments,
                save_path,
                client,
                concurrency,
                config.retry,
                tx.clone(),
            )
            .await;
        }

        // 构建最终消息
        let mut final_msg = String::from("Successfully downloaded!");

        let failed_nums = self.failed_segments.lock().await.len();
        if failed_nums > 0 {
            final_msg = format!("{} failed to download.", failed_nums);

            // 记录下载失败的分片
            if let Ok(mut file) = File::create(save_path.join(FAILED_FILENAME)).await {
                let failed_segments = self.failed_segments.lock().await;
                let mut buffer = String::new();
                for segment in failed_segments.iter() {
                    buffer.push_str(&format!("{} - {}\n", segment.name, segment.download_url));
                }
                file.write_all(buffer.as_bytes()).await?;
            }
        } else {
            // 合并分片
            if config.is_merge {
                let _ = tx.send(ChannelMessage::Merging).await;
                let m3u8_path = save_path.join(M3U8_FILENAME).to_string_lossy().to_string();
                let mp4_path = save_path
                    .join(format!("{}.mp4", config.video_name))
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
                let downloaded_segments = self
                    .downloaded_segments
                    .iter()
                    .map(|s| s.key().clone())
                    .collect::<Vec<String>>();

                match merge_and_delete(
                    args,
                    config.is_delete_segment,
                    downloaded_segments,
                    save_path,
                )
                .await
                {
                    Ok(msg) => {
                        final_msg = msg;
                    }
                    Err(e) => match e.kind() {
                        io::ErrorKind::NotFound => {
                            final_msg = String::from("Not found ffmpeg Command.");
                        }
                        _ => final_msg = e.to_string(),
                    },
                }
            }
        }

        // 更新UI
        let _ = tx
            .send(ChannelMessage::Downloaded {
                message: final_msg,
                have_failed_segment: failed_nums > 0,
            })
            .await;

        self.clear().await; // 重置

        Ok(())
    }

    /// 使用 futures::stream 并发下载
    async fn future_download(
        &self,
        segments: Vec<Segment>,
        save_path: &Path,
        client: Arc<Client>,
        concurrency: usize,
        retry: u32, // 重试次数
        tx: mpsc::Sender<ChannelMessage>,
    ) {
        futures::stream::iter(segments)
            .for_each_concurrent(concurrency, move |segment| {
                let client = Arc::clone(&client);
                let tx_clone = tx.clone();

                async move {
                    if !self.is_downloading().await {
                        return;
                    }

                    self.download_single_segment(client, save_path, segment, tx_clone, retry)
                        .await;
                }
            })
            .await;
    }

    /// 下载单个分片
    async fn download_single_segment(
        &self,
        client: Arc<Client>,
        save_path: &Path,
        segment: Segment,
        tx: mpsc::Sender<ChannelMessage>,
        retry: u32, // 重试次数
    ) {
        let mut is_finish = false;

        for attempt in 0..retry {
            if let Ok(resp) = client.get(&segment.download_url).send().await
                && resp.status().is_success()
                && let Ok(file) = File::create(save_path.join(&segment.name)).await
            {
                let mut ok = true;
                let mut writer = BufWriter::new(file);
                let mut stream = resp.bytes_stream();
                let mut segment_size = 0usize;

                // 使用流式写入
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(chunk) => {
                            if writer.write_all(&chunk).await.is_err() {
                                ok = false;
                                break;
                            }
                            segment_size += chunk.len();
                        }
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    }
                }

                // 记录下载成功的分片
                if writer.flush().await.is_ok() && ok {
                    self.downloaded_segments.insert(segment.name.clone());
                    let downloaded_nums = self.downloaded_nums.fetch_add(1, Ordering::Relaxed) + 1;
                    let downloaded_sizes = self
                        .downloaded_sizes
                        .fetch_add(segment_size, Ordering::Relaxed)
                        + segment_size;
                    let _ = tx
                        .send(ChannelMessage::Progress {
                            downloaded_nums,
                            downloaded_sizes,
                        })
                        .await;
                    is_finish = true;
                    break;
                }
            }

            // 重试，使用指数退避策略
            if attempt < retry {
                let delay = 1000 * 2u64.pow(attempt);
                sleep(Duration::from_millis(delay)).await;
            }
        }

        // 记录下载失败的切片，任务结束后再写入文件
        if !is_finish {
            self.failed_segments.lock().await.push(segment);
        }
    }

    async fn parse_m3u8(
        &self,
        m3u8_url: &str,
        client: Arc<Client>,
        save_path: &Path,
    ) -> Result<Vec<Segment>, Box<dyn Error>> {
        let mut base_url = Url::parse(m3u8_url)?.join(".")?;
        let mut content = fetch_m3u8_content(&client, m3u8_url).await?;

        // 处理大师列表多个分辨率
        if content.contains("#EXT-X-STREAM-INF") {
            let best_url = parse_m3u8_master(&content)?;

            if best_url.is_empty() {
                return Err("Invalid master playlist.".into());
            }

            let final_url = build_abs_url(&base_url, &best_url)?;
            // 获取高分辨率的M3U8内容
            content = fetch_m3u8_content(&client, final_url.as_str()).await?;
            // 重新构建base url
            base_url = Url::parse(final_url.as_str())?.join(".")?;
        }

        // 提取第一个分片，确定视频的 MIME 类型
        let suffix = extract_suffix_from_m3u8_content(&client, &base_url, &content).await?;
        let segments = extract_segments(&content, &base_url, save_path, &suffix).await?;

        if segments.is_empty() {
            return Err("No segments found.".into());
        }

        Ok(segments)
    }
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 用户下载配置
#[derive(Debug)]
pub struct DownloadConfig {
    //pub save_path: PathBuf, // 保存目录将保存至 DownloadTask
    //pub connect_timeout: u64,
    save_path: PathBuf,
    video_name: String,
    m3u8_url: String,
    concurrency: usize,
    retry: u32,
    connect_timeout: u64,
    is_merge: bool,
    is_delete_segment: bool,
}

impl DownloadConfig {
    /// 创建新的下载任务配置
    pub async fn new(
        ui: &AppWindow,
        download_manager: &Arc<DownloadManager>,
    ) -> Result<Self, Box<dyn Error>> {
        let video_name = ui.get_video_name();

        let (save_path, video_name, m3u8_url) = if download_manager.is_idle().await {
            let (save_path, video_name) =
                create_safe_save_path(&ui.get_work_dir(), &video_name).await?;
            let m3u8_url = ui.get_m3u8_url().to_string();

            if Url::parse(&ui.get_m3u8_url()).is_err() {
                return Err("Invalid M3U8 URL".into());
            };

            (save_path, video_name, m3u8_url)
        } else {
            (PathBuf::default(), video_name.into(), String::default())
        };

        let concurrency = ui.get_concurrency().parse::<usize>().unwrap_or(4);
        if concurrency < 1 {
            return Err("Concurrency cannot be less than 1".into());
        }

        let connect_timeout = ui.get_connect_timeout().parse::<u64>().unwrap_or(3);
        if connect_timeout < 1 {
            return Err("Connect timeout cannot be less than 1 second".into());
        }

        Ok(Self {
            save_path,
            video_name,
            m3u8_url,
            concurrency,
            connect_timeout,
            retry: ui.get_retry().parse().unwrap_or(3),
            is_merge: ui.get_is_merge(),
            is_delete_segment: ui.get_is_delete_segment(),
        })
    }
}

/// 安全的创建保存目录
async fn create_safe_save_path(
    work_dir: &SharedString,
    video_name: &SharedString,
) -> Result<(PathBuf, String), Box<dyn Error>> {
    if video_name.trim().is_empty() {
        return Err("The video name cannot be empty.".into());
    }

    // 只取文件名部分（去除路径）
    let safe_name = Path::new(video_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(video_name);

    // 移除非法字符
    let cleaned = VIDEO_NAME_RE.replace_all(safe_name, "_");
    // 对 UTF-8 做字符级截断，避免截断到半个字符
    let mut clean_name: String = cleaned.chars().collect();

    // 限制视频名称长度
    if clean_name.chars().count() > MAX_VIDEO_NAME_LEN {
        clean_name = clean_name.chars().take(MAX_VIDEO_NAME_LEN).collect();
    }

    // 创建保存目录
    let save_path = Path::new(work_dir).join(&clean_name);
    if !save_path.is_dir() && fs::create_dir_all(&save_path).await.is_err() {
        return Err("Failed to create the save directory.".into());
    }

    Ok((save_path, clean_name))
}

/// 获取M3U8内容，验证非空、content-type
async fn fetch_m3u8_content(client: &Arc<Client>, url: &str) -> Result<String, Box<dyn Error>> {
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

    if text.trim().is_empty() {
        return Err("M3U8 Content is empty.".into());
    }

    if !content_type.contains("mpegurl") && !text.contains("#EXTM3U") && !text.contains("#EXTINF") {
        return Err("Not valid M3U8 playlist.".into());
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

/// 解析大师列表
///
/// # Returns
/// * `Result<(String)` - 最高分辨率的M3U8地址
fn parse_m3u8_master(content: &str) -> Result<String, Box<dyn Error>> {
    let mut best_url = String::default();
    let mut best_resolution_sum = 0u32;

    let mut lines = content.lines().filter(|line| !line.is_empty());
    while let Some(line) = lines.next() {
        if line.starts_with("#EXT-X-STREAM-INF")
            && let Some(caps) = MASTER_RE.captures(line)
        {
            let w: u32 = caps
                .get(1)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let h: u32 = caps
                .get(2)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let resolution_sum = w.saturating_add(h);

            if let Some(url_line) = lines.next()
                && resolution_sum > best_resolution_sum
            {
                best_resolution_sum = resolution_sum;
                best_url = url_line.to_string();
            }
        }
    }

    Ok(best_url)
}

/// 提取视频内容 mime 类型，返回文件后缀名，目前暂支持以 image/ 开头的
async fn extract_suffix_from_m3u8_content(
    client: &Arc<Client>,
    base_url: &Url,
    content: &str,
) -> Result<String, Box<dyn Error>> {
    for line in content.lines() {
        if !line.starts_with("#") {
            let download_url = build_abs_url(base_url, line)?.to_string();
            let resp = client.head(download_url).send().await?;
            if resp.status().is_success()
                && let Some(v) = resp
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                && let Some(suffix) = v.split("/").collect::<Vec<&str>>().last()
                && *suffix != "mp2t"
            {
                return Ok(format!(".{}", suffix));
            }
            break;
        }
    }

    // 默认后缀 ts
    Ok(String::from(".ts"))
}

/// 提取M3U8分片并写入文件
async fn extract_segments(
    content: &str,
    base_url: &Url,
    save_path: &Path,
    suffix: &str,
) -> Result<Vec<Segment>, Box<dyn Error>> {
    let segment_count = content
        .lines()
        .filter(|line| !line.starts_with('#'))
        .count();
    let mut segments: Vec<Segment> = Vec::with_capacity(segment_count);
    let mut writer = BufWriter::new(File::create(save_path.join(M3U8_FILENAME)).await?);
    let mut key_index = 1u32;
    let mut segment_index = 0u32;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        if line.starts_with("#EXT-X-KEY") {
            let key = line
                .split("URI=\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap_or("");
            let download_url = build_abs_url(base_url, key)?.to_string();
            let new_key_name = format!("key_{}.key", key_index);

            writer
                .write_all(format!("{}\n", line.replace(key, &new_key_name)).as_bytes())
                .await?;

            segments.push(Segment {
                name: new_key_name,
                download_url,
            });

            key_index += 1;
        } else if !line.starts_with("#") {
            let download_url = build_abs_url(base_url, line)?.to_string();
            let segment_name = format!("segment_{}{suffix}", segment_index);

            writer
                .write_all(format!("{}\n", segment_name).as_bytes())
                .await?;

            segments.push(Segment {
                name: segment_name.clone(),
                download_url,
            });

            segment_index += 1;
        } else {
            writer.write_all(format!("{}\n", line).as_bytes()).await?;
        }
    }

    writer.flush().await?;

    Ok(segments)
}

/// 合并为MP4、删除分片
async fn merge_and_delete(
    args: Vec<&str>,
    is_delete_segment: bool,
    downloaded_segments: Vec<String>,
    save_path: &Path,
) -> Result<String, io::Error> {
    let mut cmd = Command::new("ffmpeg");
    cmd.args(args);

    #[cfg(windows)]
    cmd.creation_flags(0x08000000);

    let status = cmd.spawn()?.wait().await?;

    // 合并失败
    if !status.success() {
        return Err(io::Error::other("Failed to merge the segments."));
    }

    let mut msg = String::from("Successfully merged!");

    // 删除所有分片，包括key
    if is_delete_segment {
        // 删除 m3u8 文件
        let _ = fs::remove_file(save_path.join(M3U8_FILENAME)).await;
        // 已删除文件数
        let deleted_counter = Arc::new(AtomicU32::new(0));

        futures::stream::iter(downloaded_segments)
            .for_each_concurrent(DELETE_CONCURRENCY, |item| {
                let deleted_counter = Arc::clone(&deleted_counter);
                async move {
                    let filepath = save_path.join(item);
                    if filepath.is_file() && fs::remove_file(&filepath).await.is_ok() {
                        deleted_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .await;

        let deleted = deleted_counter.load(Ordering::Relaxed);
        msg.push_str(&format!(
            " {} segment{} have been deleted.",
            deleted,
            if deleted > 1 { "s" } else { "" }
        ));
    }

    Ok(msg)
}
