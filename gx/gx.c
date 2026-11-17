/* gx.c — 见 gx.h。单线程,与 enet_host_service 同线程调用。 */
#include "gx.h"
#include <stdlib.h>
#include <string.h>

/* ---- 线格式(所有多字节字段网络序) ----
 * [0x00][u32 seq][payload]        可靠数据,channel 0
 * [0x01][u32 ackSeq]              可靠确认(下一期望 seq),channel 0
 * [0x02][u64 token][u32 recvSeq]  HELLO,bind 后立即发送,channel 0
 * [0x03][payload]                 不可靠数据,channel 1
 */
#define GX_T_DATA  0x00
#define GX_T_ACK   0x01
#define GX_T_HELLO 0x02
#define GX_T_UDATA 0x03

#define GX_OOO_MAX 64   /* 接收侧乱序缓冲上限(正常路径不会用到,纯防御) */

/* u32 序列号回绕安全比较:a 在 b 之前(严格)为真 */
static int gx_seq_before (enet_uint32 a, enet_uint32 b) { return (gx_int32)(a - b) < 0; }

typedef struct GXReliableMsg {
    ENetListNode node;
    enet_uint32  seq;
    enet_uint32  len;
    int          inEnet;   /* 0=待灌入 1=已交给 ENet 等待确认 */
    enet_uint8   data [1];
} GXReliableMsg;

typedef struct GXUnrelMsg {
    ENetListNode node;
    enet_uint32  enqueuedMs;
    enet_uint32  len;
    enet_uint8   data [1];
} GXUnrelMsg;

typedef struct GXOooMsg {
    ENetListNode node;
    enet_uint32  seq;
    enet_uint32  len;
    enet_uint8   data [1];
} GXOooMsg;

struct GXSession {
    gx_uint64 token;
    ENetPeer   *peer;

    /* 发送侧 */
    ENetList    relQueue;      /* GXReliableMsg,按 seq 升序;未确认前不摘除 */
    enet_uint32 sendSeq;       /* 下一个要分配的 seq */
    enet_uint32 relBytes;
    enet_uint32 relUnacked;
    ENetList    unrelQueue;    /* GXUnrelMsg,按入队时间升序 */

    /* 接收侧 */
    enet_uint32 recvSeq;       /* 下一个期望收到的 seq */
    ENetList    oooQueue;      /* GXOooMsg,按 seq 升序 */
    int         ackPending;

    GXMessageFn onMessage;
    void       *user;

    int helloPending;            /* bind 后等待连接建立再发 HELLO */
    GXStats stats;
};

/* ---------- 小工具 ---------- */

static enet_uint32 gx_get32 (const enet_uint8 *p) {
    enet_uint32 v; memcpy (& v, p, 4); return ENET_NET_TO_HOST_32 (v);
}
static gx_uint64 gx_get64 (const enet_uint8 *p) {
    gx_uint64 v; memcpy (& v, p, 8);
    return ((gx_uint64) ENET_NET_TO_HOST_32 ((enet_uint32) v) << 32)
         | ENET_NET_TO_HOST_32 ((enet_uint32) (v >> 32));
}
static void gx_put32 (enet_uint8 *p, enet_uint32 v) {
    v = ENET_HOST_TO_NET_32 (v); memcpy (p, & v, 4);
}
static void gx_put64 (enet_uint8 *p, gx_uint64 v) {
    enet_uint32 hi = ENET_HOST_TO_NET_32 ((enet_uint32) (v >> 32));
    enet_uint32 lo = ENET_HOST_TO_NET_32 ((enet_uint32) v);
    memcpy (p, & hi, 4); memcpy (p + 4, & lo, 4);
}

static int gx_send_raw (GXSession *s, int channel, enet_uint32 flags,
                        const void *data, size_t len) {
    ENetPacket *p;
    if (! s-> peer || s-> peer -> state != ENET_PEER_STATE_CONNECTED) return 0;
    p = enet_packet_create (data, len, flags);
    if (! p) return 0;
    if (enet_peer_send (s-> peer, (enet_uint8) channel, p) != 0) {
        enet_packet_destroy (p);
        return 0;
    }
    return 1;
}

/* ---------- 生命周期 ---------- */

GXSession *gx_session_create (gx_uint64 token) {
    GXSession *s = (GXSession *) calloc (1, sizeof (GXSession));
    if (! s) return NULL;
    s-> token = token;
    enet_list_clear (& s-> relQueue);
    enet_list_clear (& s-> unrelQueue);
    enet_list_clear (& s-> oooQueue);
    return s;
}

static void gx_clear_rel (GXSession *s) {
    while (! enet_list_empty (& s-> relQueue))
        free (enet_list_remove (enet_list_begin (& s-> relQueue)));
    s-> relBytes = 0;
    s-> relUnacked = 0;
}

static void gx_clear_unrel (GXSession *s) {
    while (! enet_list_empty (& s-> unrelQueue)) {
        free (enet_list_remove (enet_list_begin (& s-> unrelQueue)));
        s-> stats.unreliableDropped ++;
    }
}

void gx_session_destroy (GXSession *s) {
    if (! s) return;
    gx_clear_rel (s);
    gx_clear_unrel (s);
    while (! enet_list_empty (& s-> oooQueue))
        free (enet_list_remove (enet_list_begin (& s-> oooQueue)));
    free (s);
}

/* ---------- 绑定 / 断线 ---------- */

void gx_bind (GXSession *s, ENetPeer *peer) {
    s-> peer = peer;
    s-> helloPending = 1;   /* peer 可能尚未 CONNECTED,推迟到 gx_flush */
}

static void gx_send_hello (GXSession *s) {
    enet_uint8 hello [1 + 8 + 4];
    hello [0] = GX_T_HELLO;
    gx_put64 (hello + 1, s-> token);
    gx_put32 (hello + 9, s-> recvSeq);
    if (gx_send_raw (s, GX_CHANNEL_RELIABLE, ENET_PACKET_FLAG_RELIABLE, hello, sizeof hello))
        s-> helloPending = 0;
}

void gx_unbind (GXSession *s) {
    s-> peer = NULL;
    gx_clear_unrel (s);   /* 不可靠消息断线即弃,不补发 */
}

int gx_is_bound (const GXSession *s) { return s && s-> peer != NULL; }

/* ---------- 发送 ---------- */

GXResult gx_send (GXSession *s, int channel, const void *data, size_t len, enet_uint32 nowMs) {
    if (! s || ! data || len == 0) return GX_ERR_ARG;
    if (len > GX_MAX_PAYLOAD) return GX_ERR_TOO_BIG;

    if (channel == GX_CHANNEL_RELIABLE) {
        GXReliableMsg *m;
        if (enet_list_size ((ENetList *) & s-> relQueue) >= GX_MAX_RELIABLE_MSGS ||
            s-> relBytes + len > GX_MAX_RELIABLE_BYTES)
            return GX_WOULD_BLOCK;                 /* 背压:调用方降速 */
        m = (GXReliableMsg *) malloc (sizeof (GXReliableMsg) + len - 1);
        if (! m) return GX_WOULD_BLOCK;
        m-> seq    = s-> sendSeq ++;
        m-> len    = (enet_uint32) len;
        m-> inEnet = 0;
        memcpy (m-> data, data, len);
        enet_list_insert (enet_list_end (& s-> relQueue), m);
        s-> relBytes += (enet_uint32) len;
        return GX_OK;
    }

    if (channel == GX_CHANNEL_UNRELIABLE) {
        GXUnrelMsg *m;
        GXResult rc = GX_OK;
        if (! s-> peer) {                          /* 断线中:直接弃 */
            s-> stats.unreliableDropped ++;
            return GX_ERR_NO_PEER;
        }
        if (enet_list_size ((ENetList *) & s-> unrelQueue) >= GX_MAX_UNRELIABLE_MSGS) {
            free (enet_list_remove (enet_list_begin (& s-> unrelQueue))); /* 挤掉最旧 */
            s-> stats.unreliableDropped ++;
            rc = GX_EVICTED_OLD;
        }
        m = (GXUnrelMsg *) malloc (sizeof (GXUnrelMsg) + len - 1);
        if (! m) return GX_ERR_ARG;
        m-> enqueuedMs = nowMs;
        m-> len = (enet_uint32) len;
        memcpy (m-> data, data, len);
        enet_list_insert (enet_list_end (& s-> unrelQueue), m);
        return rc;
    }

    return GX_ERR_ARG;
}

void gx_flush (GXSession *s, enet_uint32 nowMs, enet_uint32 maxBytesPerFlush) {
    enet_uint32 spent = 0;
    ENetListIterator it;

    if (! s || ! s-> peer) return;

    /* 0. HELLO(连接建立后首发)与捎带 ACK(每帧至多一个) */
    if (s-> helloPending) gx_send_hello (s);
    if (s-> ackPending) {
        enet_uint8 ack [1 + 4];
        ack [0] = GX_T_ACK;
        gx_put32 (ack + 1, s-> recvSeq);
        gx_send_raw (s, GX_CHANNEL_RELIABLE, ENET_PACKET_FLAG_RELIABLE, ack, sizeof ack);
        s-> ackPending = 0;
    }

    /* 1. 可靠优先:把未灌入 ENet 的消息按序灌入 */
    for (it = enet_list_begin (& s-> relQueue); it != enet_list_end (& s-> relQueue);
         it = enet_list_next (it)) {
        GXReliableMsg *m = (GXReliableMsg *) it;
        enet_uint8 stackBuf [1 + 4 + GX_MAX_PAYLOAD];
        size_t wireLen;
        if (m-> inEnet) continue;
        wireLen = 1 + 4 + m-> len;
        if (maxBytesPerFlush && spent + wireLen > maxBytesPerFlush) break;
        stackBuf [0] = GX_T_DATA;
        gx_put32 (stackBuf + 1, m-> seq);
        memcpy (stackBuf + 5, m-> data, m-> len);
        gx_send_raw (s, GX_CHANNEL_RELIABLE, ENET_PACKET_FLAG_RELIABLE, stackBuf, wireLen);
        m-> inEnet = 1;
        s-> relUnacked ++;
        spent += (enet_uint32) wireLen;
    }

    /* 2. 不可靠:先丢过期,再在剩余预算内发送 */
    while (! enet_list_empty (& s-> unrelQueue)) {
        GXUnrelMsg *m = (GXUnrelMsg *) enet_list_begin (& s-> unrelQueue);
        enet_uint8 stackBuf [1 + GX_MAX_PAYLOAD];
        size_t wireLen;
        if ((enet_uint32) (nowMs - m-> enqueuedMs) > GX_UNRELIABLE_TTL_MS) {
            free (enet_list_remove (enet_list_begin (& s-> unrelQueue)));
            s-> stats.unreliableDropped ++;
            continue;
        }
        wireLen = 1 + m-> len;
        if (maxBytesPerFlush && spent + wireLen > maxBytesPerFlush) break;
        enet_list_remove (enet_list_begin (& s-> unrelQueue));
        stackBuf [0] = GX_T_UDATA;
        memcpy (stackBuf + 1, m-> data, m-> len);
        gx_send_raw (s, GX_CHANNEL_UNRELIABLE, 0, stackBuf, wireLen);
        spent += (enet_uint32) wireLen;
        free (m);
    }
}

/* ---------- 接收 ---------- */

static void gx_deliver (GXSession *s, int channel, const enet_uint8 *data, size_t len) {
    if (s-> onMessage) s-> onMessage (s, channel, data, len, s-> user);
}

/* 收到 HELLO/ACK 后,砍掉对方已确认的可靠消息 */
static void gx_trim_acked (GXSession *s, enet_uint32 ackSeq) {
    while (! enet_list_empty (& s-> relQueue)) {
        GXReliableMsg *m = (GXReliableMsg *) enet_list_begin (& s-> relQueue);
        if (! gx_seq_before (m-> seq, ackSeq)) break;
        if (m-> inEnet && s-> relUnacked) s-> relUnacked --;
        s-> relBytes -= m-> len;
        free (enet_list_remove (enet_list_begin (& s-> relQueue)));
    }
}

static void gx_on_hello (GXSession *s, const enet_uint8 *p, size_t len) {
    gx_uint64 token;
    enet_uint32 theirRecvSeq;
    ENetListIterator it;
    if (len < 1 + 8 + 4) return;
    token = gx_get64 (p + 1);
    if (token != s-> token) return;   /* 不属于本会话,忽略 */
    theirRecvSeq = gx_get32 (p + 9);
    gx_trim_acked (s, theirRecvSeq);
    /* 旧连接上已发出但未确认的一律重发;接收侧按 seq 去重,保证不重不漏 */
    for (it = enet_list_begin (& s-> relQueue); it != enet_list_end (& s-> relQueue);
         it = enet_list_next (it)) {
        GXReliableMsg *m = (GXReliableMsg *) it;
        if (m-> inEnet) {
            m-> inEnet = 0;
            if (s-> relUnacked) s-> relUnacked --;
            s-> stats.reliableResent ++;
        }
    }
}

static void gx_on_data (GXSession *s, const enet_uint8 *p, size_t len) {
    enet_uint32 seq;
    if (len < 1 + 4) return;
    seq = gx_get32 (p + 1);
    if (gx_seq_before (seq, s-> recvSeq)) {   /* 重复(重连补发),丢弃 */
        s-> stats.reliableDupDropped ++;
        s-> ackPending = 1;
        return;
    }
    if (seq == s-> recvSeq) {
        gx_deliver (s, GX_CHANNEL_RELIABLE, p + 5, len - 5);
        s-> recvSeq ++;
        /* 顺排消费乱序缓冲 */
        while (! enet_list_empty (& s-> oooQueue)) {
            GXOooMsg *o = (GXOooMsg *) enet_list_begin (& s-> oooQueue);
            if (o-> seq != s-> recvSeq) break;
            gx_deliver (s, GX_CHANNEL_RELIABLE, o-> data, o-> len);
            s-> recvSeq ++;
            free (enet_list_remove (enet_list_begin (& s-> oooQueue)));
        }
    } else if (enet_list_size (& s-> oooQueue) < GX_OOO_MAX) {
        /* 乱序到达(理论兜底):按 seq 升序插入 */
        ENetListIterator it = enet_list_begin (& s-> oooQueue);
        GXOooMsg *o;
        while (it != enet_list_end (& s-> oooQueue) &&
               gx_seq_before (((GXOooMsg *) it)-> seq, seq))
            it = enet_list_next (it);
        if (it != enet_list_end (& s-> oooQueue) && ((GXOooMsg *) it)-> seq == seq) {
            s-> stats.reliableDupDropped ++;
            s-> ackPending = 1;
            return;
        }
        o = (GXOooMsg *) malloc (sizeof (GXOooMsg) + (len - 5) - 1);
        if (! o) return;
        o-> seq = seq;
        o-> len = (enet_uint32) (len - 5);
        memcpy (o-> data, p + 5, o-> len);
        enet_list_insert (it, o);
    }
    s-> ackPending = 1;
}

void gx_receive (GXSession *s, ENetPacket *packet) {
    const enet_uint8 *p;
    if (! s || ! packet || packet -> dataLength < 1) return;
    p = packet -> data;
    switch (p [0]) {
    case GX_T_DATA:  gx_on_data (s, p, packet -> dataLength); break;
    case GX_T_ACK:
        if (packet -> dataLength >= 5) gx_trim_acked (s, gx_get32 (p + 1));
        break;
    case GX_T_HELLO: gx_on_hello (s, p, packet -> dataLength); break;
    case GX_T_UDATA:
        gx_deliver (s, GX_CHANNEL_UNRELIABLE, p + 1, packet -> dataLength - 1);
        break;
    default: break;   /* 未知类型:静默丢弃,前向兼容 */
    }
}

int gx_packet_session_token (const ENetPacket *packet, gx_uint64 *outToken) {
    if (! packet || packet -> dataLength < 1 + 8 + 4 || packet -> data [0] != GX_T_HELLO)
        return 0;
    if (outToken) * outToken = gx_get64 (packet -> data + 1);
    return 1;
}

void gx_set_message_handler (GXSession *s, GXMessageFn fn, void *user) {
    s-> onMessage = fn;
    s-> user = user;
}

const GXStats *gx_stats (const GXSession *s) {
    static GXStats empty;
    if (! s) return & empty;
    ((GXSession *) s)-> stats.reliableQueued   = (enet_uint32) enet_list_size ((ENetList *) & s-> relQueue);
    ((GXSession *) s)-> stats.reliableBytes    = s-> relBytes;
    ((GXSession *) s)-> stats.reliableUnacked  = s-> relUnacked;
    ((GXSession *) s)-> stats.unreliableQueued = (enet_uint32) enet_list_size ((ENetList *) & s-> unrelQueue);
    return & s-> stats;
}
