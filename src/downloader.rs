//! # 下载管理
//!
//! `downloader` 主要管理M3U8的解析和下载

use crate::AppWindow;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::{Client, header::CONTENT_TYPE};
use slint::SharedString;
use std::{
    cell::{Cell, RefCell},
    error::Error,
    path::{Path, PathBuf},
    rc::Rc,
};
use tokio::{
    fs,
    io::{self, AsyncWriteExt, BufWriter},
    process::Command,
    sync::mpsc,
    time::{Duration, sleep},
};
use url::Url;

/// 匹配无效文件名/路径
static INVALID_FILENAME_RE: Lazy<regex::Regex> =
    Lazy::new(|| Regex::new(r#"[<>:"/\\|?*]"#).unwrap());
// 匹配大师列表
static MASTER_RE: Lazy<regex::Regex> = Lazy::new(|| Regex::new(r"RESOLUTION=(\d+)x(\d+)").unwrap());

// M3U8文件名
const M3U8_FILENAME: &str = "index.m3u8";
// 下载失败的文件名
const FAILED_FILENAME: &str = "failed.txt";

/// 信道消息类型
pub enum ChannelMessage {
    Start {
        total_nums: usize,        // 总分片数
        is_master_playlist: bool, // 是否大师列表
    },
    Paused,
    Canceled,
    Progress {
        segment_size: usize, // 单个分片的大小
    },
    Downloaded {
        message: String,
        have_failed_segment: bool,
    },
    Merging,
}

/// 分片信息
#[derive(Debug, Clone)]
pub struct Segment {
    pub name: String,
    pub download_url: String,
}

/// 下载任务信息
#[derive(Debug, Default)]
pub struct DownloadTask {
    pub total_nums: Cell<usize>,                       // 总分片数
    pub downloaded_nums: Cell<u32>,                    // 已下载分片数，成功一个就加 1
    pub downloaded_sizes: Cell<usize>,                 // 已下载的文件大小
    pub state: Cell<u8>,                               // 0未下载或取消下载，1下载中，2暂停
    pub wait_download_segments: RefCell<Vec<Segment>>, // 待下载的分片
    pub downloaded_segments: RefCell<Vec<String>>,     // 已下载的分片，存储分片名，用以删除分片
    pub failed_segments: RefCell<Vec<Segment>>,        // 下载失败的分片
    pub save_path: RefCell<PathBuf>,                   // 保存目录
    pub is_master_playlist: Cell<bool>,                // 是否大师列表
}

impl DownloadTask {
    pub fn clear(&self) {
        self.total_nums.set(0);
        self.state.set(0);

        self.wait_download_segments.take();
        self.downloaded_segments.take();
        self.failed_segments.take();

        self.is_master_playlist.set(false);
    }
}

/// 负责解析和下载
#[derive(Debug)]
pub struct DownloadManager {
    pub save_path: PathBuf, // 保存目录将保存至 DownloadTask
    pub connect_timeout: u64,
    video_name: String,
    m3u8_url: String,
    concurrency: usize,
    retry: u32,
    is_merge: bool,
    is_delete_segment: bool,
}

impl DownloadManager {
    /// 提取用户输入，创建一个新的下载任务配置
    ///
    /// 若是新下载任务，必须对M3U8地址、并发数和连接超时做验证
    pub async fn new(ui: &AppWindow, download_state: u8) -> Result<Self, String> {
        let video_name = ui.get_video_name();
        let (save_path, video_name, m3u8_url) = if download_state == 0 {
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
            retry: ui.get_retry().parse().unwrap_or(3) + 1, // 重试次数加 1 确保执行一次
            is_merge: ui.get_is_merge(),
            is_delete_segment: ui.get_is_delete_segment(),
        })
    }

    /// 装载下载任务：解析M3U8，将需要的内容更新至 DownloadTask
    pub async fn load_task(
        &self,
        download_task: Rc<DownloadTask>,
        client: Rc<Client>,
    ) -> Result<(), Box<dyn Error>> {
        let save_path = Path::new(&self.save_path);

        // 1、若存在失败文件，则删除
        let failed_filepath = save_path.join(FAILED_FILENAME);
        if failed_filepath.is_file() {
            fs::remove_file(failed_filepath).await?;
        }

        // 2、解析M3U8
        let (segments, is_master_playlist) = self.parse_m3u8(Rc::clone(&client), save_path).await?;

        // 3、初始化 DownloadTask
        let segments_len = segments.len();
        *download_task.wait_download_segments.borrow_mut() = segments.clone();
        download_task.is_master_playlist.set(is_master_playlist);
        download_task.total_nums.set(segments_len);
        *download_task.save_path.borrow_mut() = self.save_path.clone();
        download_task.downloaded_nums.set(0);
        download_task.downloaded_sizes.set(0);

        Ok(())
    }

    /// 解析M3U8
    pub async fn parse_m3u8(
        &self,
        client: Rc<Client>,
        save_path: &Path,
    ) -> Result<(Vec<Segment>, bool), Box<dyn Error>> {
        let mut base_url = Url::parse(&self.m3u8_url)?.join(".")?;
        let mut content = fetch_m3u8_content(&client, &self.m3u8_url).await?;
        // 默认不是大师列表
        let mut is_master_playlist = false;

        // 处理大师列表多个分辨率
        if content.contains("#EXT-X-STREAM-INF") {
            let best_url = parse_m3u8_master(&content);

            if best_url.is_empty() {
                return Err("Invalid master playlist.".into());
            }

            let final_url = build_abs_url(&base_url, &best_url)?;
            // 获取高分辨率的M3U8内容
            content = fetch_m3u8_content(&client, final_url.as_str()).await?;
            // 重新构建base url
            base_url = Url::parse(final_url.as_str())?.join(".")?;
            is_master_playlist = true;
        }

        // 提取第一个分片，确定视频的 MIME 类型
        let suffix = extra_suffix_from_m3u8_content(&client, &base_url, &content).await?;

        let segment_count = content
            .lines()
            .filter(|line| !line.starts_with('#'))
            .count();
        let mut segments: Vec<Segment> = Vec::with_capacity(segment_count);
        let mut writer = BufWriter::new(fs::File::create(save_path.join(M3U8_FILENAME)).await?);
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
                let download_url = build_abs_url(&base_url, key)?.to_string();
                let new_key_name = format!("key_{}.key", key_index);

                segments.push(Segment {
                    name: new_key_name.clone(),
                    download_url,
                });
                writer
                    .write_all(format!("{}\n", line.replace(key, &new_key_name)).as_bytes())
                    .await?;
                key_index += 1;
            } else if !line.starts_with("#") {
                let download_url = build_abs_url(&base_url, line)?.to_string();
                let segment_name = format!("segment_{}{suffix}", segment_index);

                segments.push(Segment {
                    name: segment_name.clone(),
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
            return Err("No segments available for download.".into());
        }

        Ok((segments, is_master_playlist))
    }

    /// 并发下载分片
    pub async fn download(
        &self,
        download_task: Rc<DownloadTask>,
        tx: mpsc::Sender<ChannelMessage>,
        client: Rc<Client>,
    ) -> Result<(), Box<dyn Error>> {
        // 通知UI，准备下载
        tx.send(ChannelMessage::Start {
            total_nums: download_task.total_nums.get(),
            is_master_playlist: download_task.is_master_playlist.get(),
        })
        .await?;

        download_task.state.set(1);

        let segments = download_task.wait_download_segments.borrow().clone();
        let concurrency = self.concurrency.min(segments.len()); // 并发数
        let segments_iter = segments.into_iter();
        let download_task_clone = Rc::clone(&download_task);
        let tx_clone_for_download = tx.clone();

        let save_path = download_task.save_path.borrow().clone();
        let save_path = Path::new(&save_path);

        futures::stream::iter(segments_iter)
            .for_each_concurrent(concurrency, move |segment| {
                let client = Rc::clone(&client);
                let download_task_clone = Rc::clone(&download_task_clone);
                let tx_clone_for_download = tx_clone_for_download.clone();

                async move {
                    if download_task_clone.state.get() != 1 {
                        return;
                    }
                    download_single_segment(
                        segment,
                        client,
                        download_task_clone,
                        self.retry,
                        save_path,
                        tx_clone_for_download,
                    )
                    .await;
                }
            })
            .await;

        // 并发下载结束

        if download_task.state.get() == 2 {
            let downloaded_segments = download_task.downloaded_segments.borrow().clone();
            // 过滤已下载的分片，重新赋值
            let new = download_task
                .wait_download_segments
                .borrow()
                .iter()
                .filter(|&item| !downloaded_segments.contains(&item.name))
                .cloned()
                .collect();
            *download_task.wait_download_segments.borrow_mut() = new;
            // 清除，否则继续下载又有失败时会累加
            download_task.failed_segments.take();
            let _ = tx.send(ChannelMessage::Paused).await;
            return Ok(());
        }

        if download_task.state.get() == 0 {
            let _ = tx.send(ChannelMessage::Canceled).await;
            // 重置 download_task
            download_task.clear();
            return Ok(());
        }

        // 任务正常结束，重置下载状态
        download_task.state.set(0);

        // 构建最终消息
        let mut final_msg = String::from("Successfully downloaded all segments.");
        // 下载失败的分片数
        let failed_segments = download_task.failed_segments.borrow().clone();
        let failed_nums = failed_segments.len() as u32;

        if failed_nums > 0 {
            final_msg = format!(
                "{} segment{} failed to download.",
                failed_nums,
                format_complex(failed_nums)
            );
            // 记录下载失败的分片
            if let Ok(mut file) = fs::File::create(save_path.join(FAILED_FILENAME)).await {
                let mut failed_str = String::default();
                for segment in failed_segments {
                    failed_str.push_str(&format!("{},{}\n", segment.name, segment.download_url));
                }
                let _ = file.write_all(failed_str.as_bytes()).await;
                let _ = file.flush().await;
            }
        } else {
            // 合并为MP4
            if self.is_merge {
                let _ = tx.send(ChannelMessage::Merging).await;
                let m3u8_path = save_path.join(M3U8_FILENAME).to_string_lossy().to_string();
                let mp4_path = save_path
                    .join(format!("{}.mp4", self.video_name))
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
                let downloaded_segments = download_task.downloaded_segments.borrow().clone();

                match merge_and_delete(
                    args,
                    self.is_delete_segment,
                    &downloaded_segments,
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

        // 重置 download_task
        download_task.clear();

        Ok(())
    }
}

/// 获取M3U8内容，验证非空、content-type
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

    if text.trim().is_empty() {
        return Err("M3U8 Content is empty.".into());
    }

    if !content_type.contains("mpegurl") && !text.contains("#EXTM3U") && !text.contains("#EXTINF") {
        return Err("Not valid M3U8 playlist.".into());
    }

    Ok(text)
}

/// 提取大师列表最高分辨率的url
///
/// 优先resolution,暂不考虑bandwidth
fn parse_m3u8_master(content: &str) -> String {
    let mut best_url = String::default();
    let mut best_resolution_sum = 0;

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

    best_url
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
async fn extra_suffix_from_m3u8_content(
    client: &Client,
    base_url: &Url,
    content: &str,
) -> Result<String, Box<dyn Error>> {
    for line in content.lines() {
        if !line.starts_with("#") {
            let download_url = build_abs_url(base_url, line)?.to_string();
            let resp = client.get(download_url).send().await?;
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

/// 安全的创建保存目录,返回保存目录和视频名称
///
/// work_dir是手动选择的工作目录，不需要做校验
async fn create_safe_save_path(
    work_dir: &SharedString,
    video_name: &SharedString,
) -> Result<(PathBuf, String), String> {
    if video_name.trim().is_empty() {
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

    // 限制视频名称长度
    if clean_name.chars().count() > 50 {
        clean_name = clean_name.chars().take(50).collect();
    }

    // 创建保存目录
    let save_path = Path::new(work_dir).join(&clean_name);
    if !save_path.is_dir() && fs::create_dir_all(&save_path).await.is_err() {
        return Err("无法创建保存目录".into());
    }

    Ok((save_path, clean_name))
}

/// 下载单个分片，带重试
async fn download_single_segment(
    segment: Segment,
    client: Rc<Client>,
    download_task: Rc<DownloadTask>,
    retry: u32,
    save_path: &Path,
    tx: mpsc::Sender<ChannelMessage>,
) {
    let mut is_finish = false;

    for attempt in 0..retry {
        if let Ok(resp) = client.get(&segment.download_url).send().await
            && resp.status().is_success()
        {
            // 使用流式写入
            if let Ok(file) = fs::File::create(save_path.join(&segment.name)).await {
                let mut ok = true;
                let mut writer = BufWriter::new(file);
                let mut stream = resp.bytes_stream();
                let mut segment_size = 0usize;

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

                let _ = writer.flush().await;

                if ok {
                    download_task
                        .downloaded_segments
                        .borrow_mut()
                        .push(segment.name.clone());
                    let _ = tx.send(ChannelMessage::Progress { segment_size }).await;
                    is_finish = true;
                    break;
                }
            }
        }

        if attempt < retry - 1 {
            let delay = 200 * 2u64.pow(retry - 1); // 使用指数退避策略
            sleep(Duration::from_millis(delay)).await;
        }
    }

    if !is_finish {
        download_task
            .failed_segments
            .borrow_mut()
            .push(segment.clone());
    }
}

/// 合并为MP4、删除分片
async fn merge_and_delete(
    args: Vec<&str>,
    is_delete_segment: bool,
    downloaded_segments: &[String],
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

    let mut msg = String::from("Successfully merged");

    // 删除所有分片，包括key
    if is_delete_segment {
        // 删除 m3u8 文件
        let _ = fs::remove_file(save_path.join(M3U8_FILENAME)).await;
        // 并发限制
        let delete_concurrency = 20usize;
        // 已删除文件数
        let deleted_counter = Rc::new(Cell::new(0u32));

        futures::stream::iter(downloaded_segments.iter())
            .for_each_concurrent(delete_concurrency, |item| {
                let deleted_counter = Rc::clone(&deleted_counter);
                async move {
                    let filepath = save_path.join(item);
                    if filepath.is_file() && fs::remove_file(&filepath).await.is_ok() {
                        deleted_counter.update(|v| v + 1);
                    }
                }
            })
            .await;
        msg.push_str(&format!(
            " and {} segment{} have been deleted.",
            deleted_counter.get(),
            format_complex(deleted_counter.get())
        ));
    }

    Ok(msg)
}

/// 处理英文复数
fn format_complex(n: u32) -> String {
    if n > 1 {
        String::from("s")
    } else {
        String::default()
    }
}
