# M3U8Downloader
基于异步单线程模型实现的 `Windows` 桌面端**M3U8**视频下载器。界面美观、易于使用，使用 `Rust` 语言开发，性能强大、内存占用率极低。

感谢强大的 `Rust` 生态：`slint`、`tokio`、`futures`、`reqwest`等！

## 主要功能

- [x] 异步单线程并发下载
- [x] 自动选择最高分辨率
- [x] 自定义并发数、重试次数和连接超时
- [x] 实时显示下载进度（总分片数、已下载分片数和大小）
- [x] 支持暂停和取消
- [x] 支持合并为MP4（需要安装 `FFmpeg`）
- [x] 合并后可删除分片
- [x] 支持中文/英文（根据系统区域自适应）
- [x] 浅色/暗黑主题自适应
- [ ] 支持默认设置，如请求头、代理模式等。

## 使用

二选一

- 克隆本项目手动构建
- 下载 `exe` 文件：[m3u8downloader-x86_64-v0.1.1.exe](http://124.71.107.97/res/m3u8downloader-x86_64-v0.1.1.exe)

## 截图

<div style="display: flex; gap: 10px;">
  <img src="screenshots/light.png" alt="light" style="width: 300px;">
  <img src="screenshots/dark.png" alt="dark" style="width: 300px;">
</div>