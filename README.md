# Rust 语音 IM 服务端（统一服务端降噪）

这是一个非点对点（非 P2P）的语音即时通讯示例：
- 浏览器采集麦克风音频；
- 客户端把每个 10ms 帧（48kHz、单声道、480 采样点）发给服务端；
- 服务端使用 `nnnoiseless` 做降噪；
- 再把降噪后的帧转发给同房间其他成员。

## 依赖

- Rust stable（建议 1.76+）
- 浏览器需支持 `AudioWorklet`（现代 Chrome/Edge/Firefox 均可）

## 运行

```bash
cargo run
```

默认行为：
- 从当前目录读取 `config.json` 的 `listenAddresses`。
- 如果没有 `config.json` 或字段为空，默认监听 `[::]:3000`。
- 当监听地址是 IPv6 wildcard（`[::]:port`）时，服务端会关闭 `v6-only`，保证 IPv4 设备可访问。

`config.json` 示例：

```json
{
  "listenAddresses": [
    "[::]:3000"
  ]
}
```

多地址监听示例：

```json
{
  "listenAddresses": [
    "0.0.0.0:3000",
    "[::1]:3000"
  ]
}
```

## 协议说明

- WebSocket 地址：`/ws?room=<room>&name=<name>`
- 客户端上行二进制帧：`960` 字节（`480 * i16` 小端）
- 服务端下行二进制帧：同上（已降噪）

## 已实现能力

- 非 P2P：所有音频都走服务端
- 每个连接独立 `DenoiseState`
- 房间隔离广播（不回传给发送者）
- 网页端实时采集 + 播放

## 后续建议

- 增加身份认证和鉴权（JWT / session）
- 接入 `opus` 编码降低带宽
- 增加 jitter buffer 和丢包补偿
- 增加房间人数、在线列表、静音状态广播等信令
