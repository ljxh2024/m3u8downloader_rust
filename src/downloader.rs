//! # 异步单线程并发下载版本

use crate::AppWindow;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::{Client, header::CONTENT_TYPE};
use slint::SharedString;
use smol::{
    Timer, channel,
    fs::{self, File},
    io::{self, AsyncWriteExt, BufWriter},
    process::{Command, windows::CommandExt},
};
use std::{
    cell::{Cell, Ref, RefCell},
    error::Error,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};
use url::Url;

// 过滤视频名称
static VIDEO_NAME_RE: Lazy<regex::Regex> = Lazy::new(|| Regex::new(r#"[<>:"/\\|?*]"#).unwrap());
// 匹配大师列表
static MASTER_RE: Lazy<regex::Regex> = Lazy::new(|| Regex::new(r"RESOLUTION=(\d+)x(\d+)").unwrap());
// 匹配 image MIME
static IMAGE_MIME_RE: Lazy<regex::Regex> =
    Lazy::new(|| Regex::new(r#"image/(jpg|jpeg|png|gif|webp|bmp|svg)"#).unwrap());

// 视频名称最大长度
const MAX_VIDEO_NAME_LEN: usize = 50;
// :todo USER-AGENT，后期引入请求头后改为自定义
const APP_USER_AGENT: &str = "Chrome/147";
// M3U8文件名
const M3U8_FILENAME: &str = "index.m3u8";
// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";
// 删除分片最大并发数
const DELETE_CONCURRENCY: usize = 20;

/// 下载状态
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum DownloadState {
    /// 空闲
    Idle,

    /// 下载中
    Downloading,

    /// 已暂停
    Paused,

    /// 已取消
    Canceled,
}

/// 信道消息
#[derive(Debug)]
pub enum ChannelMessage {
    /// 解析完毕，开始下载
    /// is_new_download == true => 更新 total_nums
    Start {
        total_nums: usize,
        is_new_download: bool,
    },

    /// 暂停
    Paused,

    /// 取消
    Canceled,

    /// 下载进度
    Progress {
        downloaded_nums: u32,
        downloaded_sizes: usize,
    },

    /// 下载完毕
    Downloaded {
        message: String,
        have_failed_segment: bool,
    },

    /// 合并中
    Merging,
}

/// 分片信息
#[derive(Debug, Clone)]
pub struct Segment {
    /// 分片名称
    name: String,

    /// 下载绝对地址
    download_url: String,
}

/// 用户下载配置
#[derive(Debug)]
pub struct UserConfig {
    /// 保存目录 下载目录+视频名称
    save_path: PathBuf,

    /// 视频名称
    video_name: String,

    /// M3U8地址
    m3u8_url: String,

    /// 并发数
    concurrency: usize,

    /// 重试次数
    retry_count: u32,

    /// 连接超时
    connect_timeout: u64,

    /// 是否合并分片
    is_merge: bool,

    /// 是否删除分片，前提是成功合并分片
    is_delete_segment: bool,
}

impl UserConfig {
    /// 创建下载配置
    pub async fn new(
        ui: &AppWindow,
        download_manager: &Rc<DownloadManager>,
    ) -> Result<Self, Box<dyn Error>> {
        let video_name = ui.get_video_name();

        let (save_path, video_name, m3u8_url) = if download_manager.is_idle() {
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
            // 重试次数， + 1 确保至少执行一次
            retry_count: ui.get_retry_count().parse().unwrap_or(3) + 1,
            is_merge: ui.get_is_merge(),
            is_delete_segment: ui.get_is_delete_segment(),
        })
    }
}

/// 任务下载管理
#[derive(Debug)]
pub struct DownloadManager {
    /// 下载状态
    download_state: Cell<DownloadState>,

    /// 总分片
    all_segments: RefCell<Vec<Segment>>,

    /// 已下载的分片，仅存储分片名称，可用以删除分片
    downloaded_segments: RefCell<Vec<String>>,

    /// 下载失败的分片
    ///
    /// 写入文件格式形如：{segment_name}.ts - {download_url}
    failed_segments: RefCell<Vec<Segment>>,

    /// 保存路径
    save_path: RefCell<PathBuf>,

    /// 已下载分片数
    downloaded_nums: Cell<u32>,

    /// 已下载的大小
    downloaded_sizes: Cell<usize>,
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadManager {
    /// 创建一个 DownloadManager
    pub fn new() -> Self {
        Self {
            download_state: Cell::new(DownloadState::Idle),
            all_segments: RefCell::new(Vec::new()),
            downloaded_segments: RefCell::new(Vec::new()),
            failed_segments: RefCell::new(Vec::new()),
            save_path: RefCell::new(PathBuf::new()),
            downloaded_nums: Cell::new(0),
            downloaded_sizes: Cell::new(0),
        }
    }

    /// 清除 DownloadManager 所有字段的值，恢复为默认
    pub fn clear(&self) {
        // 有下载失败时才保留 save_path 用以打开查看
        if self.failed_segments.borrow().is_empty() {
            self.save_path.borrow_mut().clear();
        }

        self.download_state.set(DownloadState::Idle);
        self.all_segments.borrow_mut().clear();
        self.downloaded_segments.borrow_mut().clear();
        self.failed_segments.borrow_mut().clear();
        self.downloaded_nums.set(0);
        self.downloaded_sizes.set(0);
    }

    /// 获取 save_path
    pub fn get_save_path(&self) -> Ref<'_, PathBuf> {
        self.save_path.borrow()
    }

    /// 返回当前下载状态
    pub fn get_download_state(&self) -> DownloadState {
        self.download_state.get()
    }

    /// 更新下载状态，返回旧值
    pub fn set_download_state(&self, state: DownloadState) {
        self.download_state.set(state);
    }

    /// 更新下载状态，返回旧值
    pub fn update_download_state(&self, state: DownloadState) -> DownloadState {
        let old = self.download_state.get();
        self.download_state.set(state);
        old
    }

    /// 当前下载状态是否空闲
    fn is_idle(&self) -> bool {
        self.download_state.get() == DownloadState::Idle
    }

    /// 当前是否正在下载
    fn is_downloading(&self) -> bool {
        self.download_state.get() == DownloadState::Downloading
    }

    /// 任务是否暂停状态
    fn is_paused(&self) -> bool {
        self.download_state.get() == DownloadState::Paused
    }

    /// 任务是否情取消
    fn is_canceled(&self) -> bool {
        self.download_state.get() == DownloadState::Canceled
    }

    /// 下载实现
    pub async fn download(
        &self,
        user_config: &UserConfig,
        tx: channel::Sender<ChannelMessage>,
    ) -> Result<(), Box<dyn Error>> {
        let client = Rc::new(
            Client::builder()
                .connect_timeout(Duration::from_secs(user_config.connect_timeout))
                .user_agent(APP_USER_AGENT)
                .build()?,
        );

        let (segments, save_path, segments_len) = if self.is_idle() {
            let all_segments = self
                .parse_m3u8(&client, &user_config.m3u8_url, &user_config.save_path)
                .await?;
            let segments_len = all_segments.len();

            // 删除下载失败的文件（若存在）
            let failed_file_path = user_config.save_path.join(FAILED_FILENAME);
            if failed_file_path.is_file() {
                fs::remove_file(failed_file_path).await?;
            }

            (all_segments, user_config.save_path.clone(), segments_len)
        } else {
            // 过滤掉已下载的
            let all_segments = self.all_segments.borrow();
            let downloaded = self.downloaded_segments.borrow();
            let wait_download_segments = all_segments
                .iter()
                .filter(|&item| !downloaded.contains(&item.name))
                .cloned()
                .collect();

            (wait_download_segments, self.save_path.borrow().clone(), 0)
        };

        let _ = tx
            .send(ChannelMessage::Start {
                total_nums: segments_len,
                is_new_download: self.is_idle(),
            })
            .await;

        self.set_download_state(DownloadState::Downloading);

        // 并发下载
        let concurrency = user_config.concurrency.min(segments_len);
        self.future_download(
            segments,
            &save_path,
            &client,
            concurrency,
            user_config.retry_count,
            tx.clone(),
        )
        .await;

        // 任务并发下载结束，但可能是由于暂停或取消导致的

        // 暂停
        if self.is_paused() {
            let _ = tx.send(ChannelMessage::Paused).await;
            return Ok(());
        }

        // 取消
        if self.is_canceled() {
            self.clear();
            let _ = tx.send(ChannelMessage::Canceled).await;
            return Ok(());
        }

        // 正常下载结束

        let failed_nums = self.failed_segments.borrow().len();
        let mut final_msg = String::from("Successfully downloaded!");

        if failed_nums > 0 {
            final_msg = format!("{} failed to download.", failed_nums);
        } else {
            // 合并分片
            if user_config.is_merge {
                let _ = tx.send(ChannelMessage::Merging).await;
                match self
                    .merge_segments(
                        &save_path,
                        user_config.is_delete_segment,
                        &user_config.video_name,
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

        let _ = tx
            .send(ChannelMessage::Downloaded {
                message: final_msg,
                have_failed_segment: failed_nums > 0,
            })
            .await;

        self.clear();

        Ok(())
    }

    /// 合并分片
    async fn merge_segments(
        &self,
        save_path: &Path,
        is_delete_segment: bool,
        video_name: &str,
    ) -> Result<String, io::Error> {
        let m3u8_path = save_path.join(M3U8_FILENAME).to_string_lossy().to_string();
        let mp4_path = save_path
            .join(format!("{}.mp4", video_name))
            .to_string_lossy()
            .to_string();
        let args = ["-i", &m3u8_path, "-c", "copy", "-y", &mp4_path];

        let mut cmd = Command::new("ffmpeg");
        cmd.args(args);

        #[cfg(windows)]
        cmd.creation_flags(0x08000000);

        let status = cmd.spawn()?.status().await?;

        if !status.success() {
            return Err(io::Error::other("Failed to merge the segments."));
        }

        let mut msg = String::from("Successfully merged!");

        if is_delete_segment {
            // 删除 m3u8 文件
            let _ = fs::remove_file(save_path.join(M3U8_FILENAME)).await;

            let deleted_counter = Rc::new(Cell::new(0));
            let downloaded_segments = self.downloaded_segments.borrow().clone();
            let save_path = Rc::new(save_path.to_path_buf());
            // 并发删除分片
            futures::stream::iter(downloaded_segments.iter())
                .for_each_concurrent(DELETE_CONCURRENCY, |item| {
                    let save_path_clone = Rc::clone(&save_path);
                    let deleted_counter = Rc::clone(&deleted_counter);

                    async move {
                        let filepath = save_path_clone.join(item);
                        if filepath.is_file() && fs::remove_file(&filepath).await.is_ok() {
                            deleted_counter.update(|v| v + 1);
                        }
                    }
                })
                .await;

            let deleted = deleted_counter.get();
            msg.push_str(&format!(
                " {} segment{} have been deleted.",
                deleted,
                if deleted > 1 { "s" } else { "" }
            ));
        }

        Ok(msg)
    }

    /// 解析M3U8，自动选择大师列表最高分辨率，自动选择文件扩展名
    ///
    /// 将总分片和保存路径写入 DownloadManager
    async fn parse_m3u8(
        &self,
        client: &Rc<Client>,
        m3u8_url: &str,
        save_path: &Path,
    ) -> Result<Vec<Segment>, Box<dyn Error>> {
        let mut base_url = Url::parse(m3u8_url)?.join(".")?;
        let mut content = fetch_m3u8_content(client, m3u8_url).await?;

        // 处理大师列表多个分辨率
        if content.contains("#EXT-X-STREAM-INF") {
            let best_url = parse_m3u8_master(&content)?;

            if best_url.is_empty() {
                return Err("Invalid master playlist.".into());
            }

            let final_url = build_abs_url(&base_url, &best_url)?;
            // 获取高分辨率的M3U8内容
            content = fetch_m3u8_content(client, final_url.as_str()).await?;
            // 重新构建base url
            base_url = Url::parse(final_url.as_str())?.join(".")?;
        }

        // 提取第一个分片，确定视频后缀
        let suffix = extract_suffix_from_m3u8_content(client, &base_url, &content).await?;
        let segments = extract_segments(&content, &base_url, save_path, &suffix).await?;

        if segments.is_empty() {
            return Err("No segments found.".into());
        }

        self.all_segments.borrow_mut().clone_from(&segments);
        self.save_path
            .borrow_mut()
            .clone_from(&save_path.to_path_buf());

        Ok(segments)
    }

    /// 使用 futures::stream 并发下载
    async fn future_download(
        &self,
        segments: Vec<Segment>,
        save_path: &Path,
        client: &Rc<Client>,
        concurrency: usize,
        retry_count: u32,
        tx: channel::Sender<ChannelMessage>,
    ) {
        futures::stream::iter(segments)
            .for_each_concurrent(concurrency, move |segment| {
                let client = Rc::clone(client);
                let tx_clone = tx.clone();

                async move {
                    if !self.is_downloading() {
                        return;
                    }

                    self.download_single_segment(client, save_path, segment, tx_clone, retry_count)
                        .await;
                }
            })
            .await;
    }

    /// 下载单个分片
    async fn download_single_segment(
        &self,
        client: Rc<Client>,
        save_path: &Path,
        segment: Segment,
        tx: channel::Sender<ChannelMessage>,
        retry_count: u32,
    ) {
        for attempt in 0..retry_count {
            match self
                .try_download_segment(&client, save_path, &segment)
                .await
            {
                Ok(segment_size) => {
                    // 记录下载成功的分片
                    self.downloaded_segments.borrow_mut().push(segment.name);
                    // 更新下载数
                    self.downloaded_nums.update(|v| v + 1);
                    // 更新文件总大小
                    self.downloaded_sizes.update(|v| v + segment_size);

                    // UI进度
                    let _ = tx
                        .send(ChannelMessage::Progress {
                            downloaded_nums: self.downloaded_nums.get(),
                            downloaded_sizes: self.downloaded_sizes.get(),
                        })
                        .await;

                    return;
                }
                Err(_) if attempt < retry_count - 1 => {
                    let delay = Duration::from_secs(2 * 2u64.pow(attempt));
                    Timer::after(delay).await;
                }
                Err(_) => break,
            }
        }

        // 记录下载失败的切片
        let failed_path = save_path.join(FAILED_FILENAME);
        if let Ok(mut file) = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(failed_path)
            .await
        {
            let _ = file
                .write_all(format!("{} - {}\n", segment.name, segment.download_url).as_bytes())
                .await;
            let _ = file.flush().await;
        }
        self.failed_segments.borrow_mut().push(segment);

        // self.failed_segments.lock().await.push(segment);
    }

    /// 下载分片+流式写入+记录成功 or 失败
    ///
    /// # Returns
    /// * `Result<usize, Box<dyn Error>>` 文件大小
    async fn try_download_segment(
        &self,
        client: &Client,
        save_path: &Path,
        segment: &Segment,
    ) -> Result<usize, Box<dyn Error>> {
        let resp = client
            .get(&segment.download_url)
            .send()
            .await?
            .error_for_status()?;

        let file = File::create(save_path.join(&segment.name)).await?;
        let mut writer = BufWriter::new(file);
        let mut stream = resp.bytes_stream();
        let mut total_size = 0usize;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            writer.write_all(&chunk).await?;
            total_size += chunk.len();
        }

        writer.flush().await?;
        Ok(total_size)
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
async fn fetch_m3u8_content(client: &Rc<Client>, url: &str) -> Result<String, Box<dyn Error>> {
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

/// 构建绝对URL
fn build_abs_url(base: &Url, uri: &str) -> Result<Url, Box<dyn Error>> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        Ok(Url::parse(uri)?)
    } else {
        Ok(base.join(uri)?)
    }
}

/// 提取视频内容 mime 类型，返回文件后缀名，目前暂支持以 image/ 开头的
async fn extract_suffix_from_m3u8_content(
    client: &Rc<Client>,
    base_url: &Url,
    content: &str,
) -> Result<String, Box<dyn Error>> {
    for line in content.lines() {
        if !line.starts_with("#") {
            let download_url = build_abs_url(base_url, line)?.to_string();
            let resp = client.head(download_url).send().await?.error_for_status()?;
            let content_type = resp
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            if let Some(caps) = IMAGE_MIME_RE.captures(content_type) {
                let suffix = caps.get(1).map(|m| m.as_str()).unwrap_or("ts");
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
