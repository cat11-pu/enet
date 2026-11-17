/* bench.c — N 连接 × 三档负载,gx 发送路径耗时对照
 *
 * 拓扑: 1 个 server host,N 个 client host(各 1 peer),loopback UDP。
 * 计时: 只统计发送侧 gx_send + gx_flush + enet_host_flush 的 CPU 时间,
 *       每 tick(60Hz)汇总,按连接数取平均,报 mean / p99。
 * 档位(每连接每 tick):
 *   轻: 1x64B 不可靠 + 每12tick 1x128B 可靠   (~30Hz 位置 + 5Hz 事件)
 *   中: 1x256B 不可靠 + 每3tick 1x512B 可靠   (~60Hz 位置 + 20Hz 事件)
 *   重: 2x512B 不可靠 + 每tick 1x1024B 可靠   (~120Hz 位置 + 60Hz 战斗)
 */
#include "gx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TICK_MS 16
#define RUN_TICKS 300   /* ~5s */

typedef struct { int unrelN, unrelSz, relEvery, relSz; const char *name; } Tier;
static const Tier TIERS [] = {
    { 1,  64, 12,  128, "light" },
    { 1, 256,  3,  512, "mid"   },
    { 2, 512,  1, 1024, "heavy" },
};

typedef struct {
    ENetHost  *host;
    ENetPeer  *peer;
    GXSession *sess;
    int        connected;
} Conn;

static ENetHost *server;
static GXSession **serverSess;
static int nConn;

static gx_uint64 nowNs (void) {
    struct timespec ts; clock_gettime (CLOCK_MONOTONIC, & ts);
    return (gx_uint64) ts.tv_sec * 1000000000ull + (gx_uint64) ts.tv_nsec;
}

static void server_pump (void) {
    ENetEvent ev;
    while (enet_host_service (server, & ev, 0) > 0) {
        if (ev.type == ENET_EVENT_TYPE_RECEIVE) {
            gx_uint64 token;
            int idx = ev.peer -> data ? (int) (long) ev.peer -> data - 1 : -1;
            if (idx < 0 && gx_packet_session_token (ev.packet, & token) &&
                token >= 5000 && token < (gx_uint64) (5000 + nConn)) {
                idx = (int) (token - 5000);
                ev.peer -> data = (void *) (long) (idx + 1);
                gx_bind (serverSess [idx], ev.peer);
            }
            if (idx >= 0) gx_receive (serverSess [idx], ev.packet);
            enet_packet_destroy (ev.packet);
        }
    }
    for (int i = 0; i < nConn; i ++)
        gx_flush (serverSess [i], enet_time_get (), 0);
}

static int cmpU64 (const void *a, const void *b) {
    gx_uint64 x = * (const gx_uint64 *) a, y = * (const gx_uint64 *) b;
    return x < y ? -1 : x > y;
}

static void runTier (Conn *conns, const Tier *t) {
    static enet_uint8 buf [1024];
    gx_uint64 *tickCost = malloc (sizeof (gx_uint64) * RUN_TICKS);
    gx_uint64 wouldBlock = 0, dropBase = 0;
    memset (buf, 0xAB, sizeof buf);
    for (int i = 0; i < nConn; i ++)
        dropBase += gx_stats (conns [i].sess) -> unreliableDropped;

    for (int tick = 0; tick < RUN_TICKS; tick ++) {
        gx_uint64 t0 = nowNs ();
        for (int i = 0; i < nConn; i ++) {
            Conn *c = & conns [i];
            enet_uint32 now = enet_time_get ();
            for (int k = 0; k < t-> unrelN; k ++)
                gx_send (c-> sess, GX_CHANNEL_UNRELIABLE, buf, t-> unrelSz, now);
            if (tick % t-> relEvery == 0)
                if (gx_send (c-> sess, GX_CHANNEL_RELIABLE, buf, t-> relSz, now) == GX_WOULD_BLOCK)
                    wouldBlock ++;
            gx_flush (c-> sess, now, 0);
            enet_host_flush (c-> host);
        }
        tickCost [tick] = nowNs () - t0;

        /* 收 + 服务器侧泵,直到下一个 tick */
        {
            gx_uint64 tickEnd = t0 + (gx_uint64) TICK_MS * 1000000ull;
            while (nowNs () < tickEnd) {
                ENetEvent ev;
                for (int i = 0; i < nConn; i ++)
                    while (enet_host_service (conns [i].host, & ev, 0) > 0)
                        if (ev.type == ENET_EVENT_TYPE_RECEIVE) {
                            gx_receive (conns [i].sess, ev.packet);
                            enet_packet_destroy (ev.packet);
                        }
                server_pump ();
            }
        }
    }

    qsort (tickCost, RUN_TICKS, sizeof (gx_uint64), cmpU64);
    {
        gx_uint64 sum = 0;
        for (int i = 0; i < RUN_TICKS; i ++) sum += tickCost [i];
        double meanUs = (double) sum / RUN_TICKS / 1000.0 / nConn;
        double p99Us  = (double) tickCost [(int) (RUN_TICKS * 0.99)] / 1000.0 / nConn;
        double maxUs  = (double) tickCost [RUN_TICKS - 1] / 1000.0 / nConn;
        gx_uint64 drops = 0;
        for (int i = 0; i < nConn; i ++)
            drops += gx_stats (conns [i].sess) -> unreliableDropped;
        printf ("| %4d | %-5s | %8.2f | %8.2f | %8.2f | %8llu | %10llu |\n",
                nConn, t-> name, meanUs, p99Us, maxUs,
                (unsigned long long) wouldBlock,
                (unsigned long long) (drops - dropBase));
    }
    free (tickCost);
}

int main (int argc, char **argv) {
    enet_initialize ();
    printf ("发送路径耗时 (us/连接/tick, 60Hz tick, loopback)\n");
    printf ("| 连接数 | 档位  | mean(us) | p99(us)  | max(us)  | 背压次数 | 不可靠丢弃 |\n");
    printf ("|-------:|-------|---------:|---------:|---------:|---------:|-----------:|\n");

    int sizes [] = { 32, 128 };
    for (int s = 0; s < 2; s ++) {
        nConn = sizes [s];
        ENetAddress addr;
        addr.host = ENET_HOST_ANY; addr.port = 46000 + (enet_uint16) s;
        server = enet_host_create (& addr, nConn, GX_CHANNEL_COUNT, 0, 0);
        serverSess = calloc (nConn, sizeof (GXSession *));
        Conn *conns = calloc (nConn, sizeof (Conn));

        for (int i = 0; i < nConn; i ++) {
            serverSess [i] = gx_session_create ((gx_uint64) (5000 + i));
            conns [i].host = enet_host_create (NULL, 1, GX_CHANNEL_COUNT, 0, 0);
            conns [i].sess = gx_session_create ((gx_uint64) (5000 + i));
        }
        /* 连接建立(分批避免握手风暴) */
        for (int i = 0; i < nConn; i ++) {
            ENetAddress to; to.port = addr.port;
            enet_address_set_host (& to, "127.0.0.1");
            conns [i].peer = enet_host_connect (conns [i].host, & to, GX_CHANNEL_COUNT, 0);
            gx_bind (conns [i].sess, conns [i].peer);
            if (i % 16 == 15) server_pump ();
        }
        /* 等全部 HELLO 绑定完成 */
        for (int w = 0; w < 2000; w ++) {
            int bound = 0;
            server_pump ();
            for (int i = 0; i < nConn; i ++) {
                ENetEvent ev;
                while (enet_host_service (conns [i].host, & ev, 0) > 0)
                    if (ev.type == ENET_EVENT_TYPE_RECEIVE) {
                        gx_receive (conns [i].sess, ev.packet);
                        enet_packet_destroy (ev.packet);
                    }
                if (gx_is_bound (serverSess [i])) bound ++;
            }
            if (bound == nConn) break;
        }

        for (int t = 0; t < 3; t ++) runTier (conns, & TIERS [t]);

        /* 清理 */
        for (int i = 0; i < nConn; i ++) {
            gx_session_destroy (conns [i].sess);
            enet_host_destroy (conns [i].host);
            gx_session_destroy (serverSess [i]);
        }
        enet_host_destroy (server);
        free (serverSess); free (conns);
    }
    enet_deinitialize ();
    return 0;
}
