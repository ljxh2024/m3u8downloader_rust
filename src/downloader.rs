//! # 下载管理
//!
//! `downloader` 主要管理M3U8的解析和下载

use crate::AppWindow;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use slint::SharedString;
use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
    },
};
use tokio::{
    fs,
    sync::{Mutex, mpsc},
};
use url::Url;

/// 匹配无效文件名/路径
static INVALID_FILENAME_RE: Lazy<regex::Regex> =
    Lazy::new(|| Regex::new(r#"[<>:"/\\|?*]"#).unwrap());

/// 信道消息类型
pub enum ChannelMessage {
    Pause,
    Cancel,
}

/// 分片信息
#[derive(Debug)]
pub struct Segment {
    pub name: String,
    pub download_url: String,
}

/// 下载任务信息
#[derive(Debug, Default)]
pub struct DownloadTask {
    // 总分片数
    pub total_segment_nums: AtomicU32,
    // 等待下载的分片
    pub wait_download_segments: Mutex<Vec<Segment>>,
    // 已下载的分片，存储分片名，用以删除分片
    pub downloaded_segments: Mutex<Vec<String>>,
    // 已下载分片数
    pub downloaded_nums: AtomicU32,
    // 下载失败的分片
    pub failed_segments: Mutex<Vec<Segment>>,
    // 0未下载或取消下载，1下载中，2暂停
    pub state: AtomicU8,
    // 是否大师列表
    pub is_master_playlist: AtomicBool,
    // 保存目录
    pub save_path: PathBuf,
}

/// 负责解析和下载
#[derive(Debug)]
pub struct DownloadManager {
    save_path: PathBuf,
    video_name: String,
    m3u8_url: SharedString,
    concurrency: usize,
    retry: u32,
    connect_timeout: u64,
    is_merge: bool,
    is_delete_segment: bool,
}

impl DownloadManager {
    /// 提取用户输入，创建一个新的下载任务配置
    ///
    /// 若是新下载任务，必须对M3U8地址、并发数和连接超时做验证
    pub async fn new(ui: &AppWindow, download_state: u8) -> Result<Self, String> {
        let (save_path, video_name, m3u8_url) = if download_state == 0 {
            let (save_path, video_name) =
                Self::create_safe_save_path(&ui.get_work_dir(), &ui.get_video_name()).await?;
            let m3u8_url = ui.get_m3u8_url();

            if Url::parse(&ui.get_m3u8_url()).is_err() {
                return Err("Invalid M3U8 URL".into());
            };

            (save_path, video_name, m3u8_url)
        } else {
            (PathBuf::new(), String::new(), SharedString::new())
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

    /// 安全的创建保存目录,返回保存目录和视频名称
    ///
    /// work_dir是手动选择的工作目录，不需要做校验
    async fn create_safe_save_path(
        work_dir: &SharedString,
        video_name: &SharedString,
    ) -> Result<(PathBuf, String), String> {
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

        Ok((save_path, clean_name))
    }

    /// 执行下载实现
    pub async fn download(
        &self,
        download_task: Arc<DownloadTask>,
        tx: mpsc::Sender<ChannelMessage>,
    ) -> Result<(), Box<dyn Error>> {
        //
        Ok(())
    }
}
