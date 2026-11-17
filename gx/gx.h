/* gx.h — 窄接口:ENet 之上的双通道发送层
 *
 * 一条物理连接(ENet peer)上同时跑:
 *   - 可靠通道   : 角色 / 背包 / 战斗事件。有序、不重不漏,支持背压。
 *   - 不可靠通道 : 位置 / 朝向。过期即弃,断线不补发。
 *
 * 线程模型: 单线程。所有 API 必须在调用 enet_host_service 的同一线程调用。
 */
#ifndef GX_H
#define GX_H

#include <enet/enet.h>
#include <stdint.h>

typedef uint64_t gx_uint64;
typedef int32_t  gx_int32;

#ifdef __cplusplus
extern "C" {
#endif

#define GX_CHANNEL_RELIABLE   0   /* ENet channel id,可靠有序 */
#define GX_CHANNEL_UNRELIABLE 1   /* ENet channel id,不可靠无序 */
#define GX_CHANNEL_COUNT      2

/* 硬上限(编译期常量,见"边界用例"文档) */
#define GX_MAX_RELIABLE_MSGS    256            /* 可靠队列最大消息数(含待重发) */
#define GX_MAX_RELIABLE_BYTES   (1u << 20)     /* 可靠队列最大字节数 */
#define GX_MAX_UNRELIABLE_MSGS  64             /* 不可靠队列最大消息数 */
#define GX_UNRELIABLE_TTL_MS    100            /* 不可靠消息存活期,超时丢弃 */
#define GX_MAX_PAYLOAD          1400           /* 单条消息载荷上限(不超出 MTU,避免分片) */

typedef enum {
    GX_OK = 0,          /* 已入队 */
    GX_WOULD_BLOCK,     /* 可靠队列达到硬上限 -> 背压,调用方应降速或稍候重试 */
    GX_EVICTED_OLD,     /* 已入队,但挤掉了最旧的一条不可靠消息(计数见 gx_stats) */
    GX_ERR_TOO_BIG,     /* 载荷超过 GX_MAX_PAYLOAD */
    GX_ERR_NO_PEER,     /* 会话当前未绑定连接(断线中),仅可靠消息允许入队 */
    GX_ERR_ARG          /* 参数非法 */
} GXResult;

typedef struct {
    enet_uint32 reliableQueued;    /* 可靠队列当前消息数(含未确认待重发) */
    enet_uint32 reliableBytes;     /* 可靠队列当前字节数 */
    enet_uint32 reliableUnacked;   /* 已发出但未收到对端确认的消息数 */
    enet_uint32 unreliableQueued;  /* 不可靠队列当前消息数 */
    gx_uint64 unreliableDropped; /* 累计丢弃(过期 + 被挤掉 + 断线清空) */
    gx_uint64 reliableResent;    /* 累计重发(重连后补发) */
    gx_uint64 reliableDupDropped;/* 接收侧累计丢弃的重复可靠消息 */
} GXStats;

typedef struct GXSession GXSession;

/* 会话生命周期:session 跨断线重连存活,peer 只是当前绑定的连接。 */
GXSession *gx_session_create (gx_uint64 sessionToken);
void       gx_session_destroy (GXSession *);

/* 绑定/解绑 ENet peer。重连后用同一个 token 拿到原 session 再 gx_bind。
 * 绑定后自动从对端汇报的接收进度恢复:补发未确认的可靠消息,清空不可靠队列。 */
void gx_bind   (GXSession *, ENetPeer *peer);
void gx_unbind (GXSession *);   /* 断线时调用;可靠消息保留,不可靠队列清空 */

/* 发送。nowMs 用 enet_time_get()。可靠消息在断线期间也允许入队(GX_OK),
 * 不可靠消息在断线期间返回 GX_ERR_NO_PEER 并丢弃。 */
GXResult gx_send (GXSession *, int channel, const void *data, size_t len, enet_uint32 nowMs);

/* 每个逻辑帧调用一次:按优先级(可靠 > 不可靠)把队列灌进 ENet,
 * 丢弃过期不可靠消息。maxBytesPerFlush 为本帧发送预算(0 = 不限)。 */
void gx_flush (GXSession *, enet_uint32 nowMs, enet_uint32 maxBytesPerFlush);

/* 接收:把从该 peer 两个通道上收到的 ENetPacket 交还给本层。
 * 可靠数据经过去重/排序后通过 onMessage 回调上交,内部自动回 ACK。 */
typedef void (*GXMessageFn)(GXSession *, int channel, const enet_uint8 *data, size_t len, void *user);
void gx_set_message_handler (GXSession *, GXMessageFn, void *user);
void gx_receive (GXSession *, ENetPacket *);

const GXStats *gx_stats (const GXSession *);
int            gx_is_bound (const GXSession *);

#ifdef __cplusplus
}
#endif
#endif

/* 服务器侧重连辅助:新 peer 连上后收到的第一个包通常是 HELLO,
 * 用它取出 token 找到旧 session,再 gx_bind + gx_receive。 */
int gx_packet_session_token (const ENetPacket *, gx_uint64 *outToken);
