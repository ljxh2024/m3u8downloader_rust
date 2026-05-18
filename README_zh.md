# M3U8Downloader_rust

使用 `Rust` 语言开发的 `Windows` 桌面端**M3U8**视频下载器。

感谢强大的 `Rust` 生态：`slint`、`tokio`、`futures`、`reqwest`等！

## 主要功能

- [x] 异步并发下载
- [x] 自动选择最高分辨率
- [x] 自定义并发数、重试次数和连接超时
- [x] 实时显示下载进度（总分片数、已下载分片数和大小）
- [x] 支持暂停和取消
- [x] 可合并为MP4（需要安装 `FFmpeg`）
- [x] 合并后可删除分片
- [x] 支持中文/英文（根据系统区域自适应）
- [x] 浅色/暗黑主题自适应
- [ ] 支持默认设置，如请求头、代理模式等。

## 截图

<div style="display: flex; gap: 10px;">
  <img src="screenshots/light.png" alt="light" style="width: 300px;">
  <img src="screenshots/dark.png" alt="dark" style="width: 300px;">
</div>