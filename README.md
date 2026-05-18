[English](./README.md) | [简体中文](./README_zh.md)

# M3U8Downloader_rust

A windows desktop M3U8 downloader developed using `Rust` language.

Thank the powerful `Rust` ecosystem.: `slint`, `tokio`, `futures`, `reqwest`, and more!

## Main functions

- [x] Asynchronous concurrent download
- [x] Automatically select the highest resolution
- [x] Customize concurrency, retry count, and connection timeout
- [x] Display download progress in real-time
- [x] Support for pause and cancel
- [x] Can be merged into MP4 (requires installation of `FFmpeg`)
- [x] After merging, the segments can be deleted
- [x] Support Chinese/English (adaptive according to system locale)
- [x] Light/Dark Theme Adaptation
- [ ] Support default settings, such as request headers, proxy mode, etc.

## Screenshots

<div style="display: flex; gap: 10px;">
  <img src="screenshots/light.png" alt="light" style="width: 300px;">
  <img src="screenshots/dark.png" alt="dark" style="width: 300px;">
</div>