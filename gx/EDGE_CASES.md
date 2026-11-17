# gx 层边界用例

接口见 `gx.h`,实现 `gx.c`,功能测试 `test_gx.c`(14 项),基准 `bench.c`。

## 背压(可靠通道)
- 队列硬上限:`GX_MAX_RELIABLE_MSGS=256` 条 **或** `GX_MAX_RELIABLE_BYTES=1MB`,先到先触发。
- 触发时 `gx_send` 返回 `GX_WOULD_BLOCK`,消息**不入队**;调用方应降速/缓存/踢人。
- 上限统计包含「待发送 + 已发未确认(unacked)」:对端不回 ACK,容量就不释放,
  这正是背压的语义——网络慢会传导为发送方阻塞。
- 恢复:对端 ACK 到达后队列裁剪,`gx_send` 自然恢复 `GX_OK`,无需额外回调。
- 断线期间可靠消息**允许入队**(占用硬上限),重连后自动补发。

## 不可靠通道
- TTL:入队超过 `GX_UNRELIABLE_TTL_MS=100ms` 的消息在下一次 `gx_flush` 丢弃,永不上线。
- 队列满(`GX_MAX_UNRELIABLE_MSGS=64`):挤掉**最旧**一条,返回 `GX_EVICTED_OLD`
  (位置/朝向语义:新的状态使旧的过期作废)。
- 断线:`gx_unbind` 立即清空不可靠队列;断线期间 `gx_send` 返回 `GX_ERR_NO_PEER`;
  重连后**不补发**。
- 单包上限 `GX_MAX_PAYLOAD=1400B`:不超 MTU,避免 ENet 分片(分片会放大丢失率,
  一个分片丢则整包丢)。

## 断线重连(可靠:不重不漏)
- 会话(`GXSession` + token)跨连接存活;`ENetPeer` 只是当前绑定。
- 每条可靠消息带会话级 u32 序号;接收侧按 `recvSeq` 去重(重复计入
  `reliableDupDropped`),发送侧保留 unacked 直到对端 ACK。
- 重连流程:新 peer → `gx_bind` → 互发 HELLO(token + 各自 recvSeq)→
  发送侧先按对端 recvSeq 裁剪,再把剩余 unacked 全部重发。
  接收侧序号去重兜底 ⇒ exactly-once。
- 服务器侧用 `gx_packet_session_token()` 在首包上取 token 找回旧会话。
- 序号回绕:差值比较(`gx_seq_before`),u32 回绕安全。
- 乱序兜底缓冲 `GX_OOO_MAX=64` 条(正常路径不触发:ENet 可靠通道有序,
  补发也按序号升序)。

## 时序与线程
- **必须每帧调用 `gx_flush`**:过期丢弃、ACK 捎带、HELLO 首发都在 flush 里;
  不调用则 ACK 不发,对端可靠队列会被 unacked 占满。
- `gx_bind` 时连接可能尚未建立(`enet_peer_send` 在非 CONNECTED 状态静默失败),
  HELLO 延迟到 `ENET_PEER_STATE_CONNECTED` 后的第一次 flush 发出。
- 单线程:所有 gx 调用必须与 `enet_host_service` 同线程。

## 容错
- token 不匹配的 HELLO:忽略(防串会话)。
- 未知包类型:静默丢弃(前向兼容,便于以后加消息类型)。
- 参数非法(空指针/零长/未知通道):`GX_ERR_ARG`;超长:`GX_ERR_TOO_BIG`。
- `enet_peer_send` 失败:packet 立即销毁,可靠消息保持未入 ENet 状态,下帧重试。
