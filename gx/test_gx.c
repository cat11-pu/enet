/* test_gx.c — gx 层边界用例功能测试(loopback 双 host) */
#include "gx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PORT 45678
#define TOKEN 1001

static int failures = 0;
#define CHECK(cond, name) do { \
    if (cond) printf ("  PASS %s\n", name); \
    else { printf ("  FAIL %s\n", name); failures ++; } \
} while (0)

/* ---- 接收记录 ---- */
#define MAX_RECV 4096
static enet_uint32 recvSeqs [MAX_RECV];
static int recvChans [MAX_RECV];
static int recvCount = 0;
static int recvBase = 0;

static void on_msg (GXSession *s, int ch, const enet_uint8 *data, size_t len, void *user) {
    (void) s; (void) user;
    if (recvCount < MAX_RECV) {
        recvChans [recvCount] = ch;
        recvSeqs [recvCount] = (ch == GX_CHANNEL_RELIABLE && len >= 4)
            ? (enet_uint32) data [0] | ((enet_uint32) data [1] << 8)
              | ((enet_uint32) data [2] << 16) | ((enet_uint32) data [3] << 24)
            : 0xFFFFFFFFu;
        recvCount ++;
    }
}

/* ---- 双端环境 ---- */
typedef struct {
    ENetHost  *host;
    ENetPeer  *peer;
    GXSession *sess;
    int        connected;
} End;

static End server, client;

static void end_handle_event (End *e, ENetEvent *ev, int isServer) {
    switch (ev-> type) {
    case ENET_EVENT_TYPE_CONNECT:
        e-> connected = 1;
        if (isServer) {
            /* 等 HELLO 到达后再绑定(见包处理) */
            e-> peer = ev-> peer;
        } else {
            e-> peer = ev-> peer;
            gx_bind (e-> sess, ev-> peer);
        }
        break;
    case ENET_EVENT_TYPE_RECEIVE: {
        if (isServer && ! gx_is_bound (e-> sess)) {
            gx_uint64 token;
            if (gx_packet_session_token (ev-> packet, & token) && token == TOKEN)
                gx_bind (e-> sess, e-> peer);
        }
        gx_receive (e-> sess, ev-> packet);
        enet_packet_destroy (ev-> packet);
        break;
    }
    case ENET_EVENT_TYPE_DISCONNECT:
        e-> connected = 0;
        gx_unbind (e-> sess);
        e-> peer = NULL;
        break;
    default: break;
    }
}

static void pump (int ms) {
    enet_uint32 end = enet_time_get () + (enet_uint32) ms;
    while ((gx_int32) (enet_time_get () - end) < 0) {
        ENetEvent ev;
        while (enet_host_service (server.host, & ev, 1) > 0)
            end_handle_event (& server, & ev, 1);
        while (enet_host_service (client.host, & ev, 1) > 0)
            end_handle_event (& client, & ev, 0);
        gx_flush (server.sess, enet_time_get (), 0);
        gx_flush (client.sess, enet_time_get (), 0);
    }
}

static void send_rel (End *e, enet_uint32 n) {
    enet_uint8 buf [4] = { n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF, (n >> 24) & 0xFF };
    gx_send (e-> sess, GX_CHANNEL_RELIABLE, buf, sizeof buf, enet_time_get ());
}

int main (void) {
    ENetAddress addr;
    enet_initialize ();

    memset (& server, 0, sizeof server);
    memset (& client, 0, sizeof client);
    addr.host = ENET_HOST_ANY; addr.port = PORT;
    server.host = enet_host_create (& addr, 8, GX_CHANNEL_COUNT, 0, 0);
    client.host = enet_host_create (NULL, 1, GX_CHANNEL_COUNT, 0, 0);
    server.sess = gx_session_create (TOKEN);
    client.sess = gx_session_create (TOKEN);
    gx_set_message_handler (server.sess, on_msg, NULL);

    addr.host = ENET_HOST_ANY; addr.port = PORT;
    enet_address_set_host (& addr, "127.0.0.1");
    enet_host_connect (client.host, & addr, GX_CHANNEL_COUNT, 0);
    pump (300);
    CHECK (server.connected && client.connected && gx_is_bound (server.sess), "connect + hello bind");

    /* 1. 可靠有序送达 */
    recvCount = 0;
    for (enet_uint32 i = 0; i < 10; i ++) send_rel (& client, i);
    pump (300);
    CHECK (recvCount == 10, "reliable: all 10 delivered");
    { int ordered = 1; for (int i = 0; i < 10 && i < recvCount; i ++)
          if (recvSeqs [i] != (enet_uint32) i || recvChans [i] != GX_CHANNEL_RELIABLE) ordered = 0;
      CHECK (ordered, "reliable: in-order"); }

    /* 2. 背压:可靠队列硬上限 */
    { int blocked = 0; enet_uint8 b [16] = {0};
      for (int i = 0; i < GX_MAX_RELIABLE_MSGS + 8; i ++)
          if (gx_send (client.sess, GX_CHANNEL_RELIABLE, b, sizeof b, enet_time_get ()) == GX_WOULD_BLOCK)
              { blocked = 1; break; }
      CHECK (blocked, "backpressure: GX_WOULD_BLOCK at cap");
      pump (500); }   /* 排空,等 ACK 释放容量 */

    /* 3. 不可靠 TTL:超过 100ms 丢弃 */
    { enet_uint8 b [32] = {0}; gx_uint64 before;
      recvCount = 0;
      gx_send (client.sess, GX_CHANNEL_UNRELIABLE, b, sizeof b, enet_time_get ());
      usleep (150 * 1000);
      before = gx_stats (client.sess) -> unreliableDropped;
      gx_flush (client.sess, enet_time_get (), 0);
      pump (100);
      CHECK (gx_stats (client.sess) -> unreliableDropped > before, "unreliable: expired >100ms dropped");
      CHECK (recvCount == 0, "unreliable: expired not delivered"); }

    /* 4. 不可靠队列满:挤掉最旧 */
    { enet_uint8 b [32] = {0}; int evicted = 0;
      for (int i = 0; i < GX_MAX_UNRELIABLE_MSGS + 4; i ++)
          if (gx_send (client.sess, GX_CHANNEL_UNRELIABLE, b, sizeof b, enet_time_get ()) == GX_EVICTED_OLD)
              evicted = 1;
      CHECK (evicted, "unreliable: GX_EVICTED_OLD at cap");
      pump (200); }

    /* 5. 断线重连:可靠不重不漏,不可靠不补发 */
    recvCount = 0;
    for (enet_uint32 i = 100; i < 120; i ++) send_rel (& client, i);
    pump (50);   /* 只给 50ms,部分可能未送达 */
    { enet_uint8 b [32] = {0};
      gx_send (client.sess, GX_CHANNEL_UNRELIABLE, b, sizeof b, enet_time_get ()); }
    /* 断线:发起 disconnect,双方 gx_unbind,不可靠队列被清空 */
    enet_peer_disconnect (client.peer, 0);
    pump (500);
    CHECK (! gx_is_bound (server.sess), "disconnect: server unbound");
    /* 断线期间可靠消息仍可入队 */
    for (enet_uint32 i = 120; i < 130; i ++) send_rel (& client, i);
    { GXResult r = gx_send (client.sess, GX_CHANNEL_UNRELIABLE, "x", 1, enet_time_get ());
      CHECK (r == GX_ERR_NO_PEER, "unreliable while down: GX_ERR_NO_PEER (dropped)"); }
    /* 重连 */
    recvBase = recvCount;   /* 不可靠检查只统计重连之后;可靠检查覆盖全程 */
    enet_address_set_host (& addr, "127.0.0.1"); addr.port = PORT;
    enet_host_connect (client.host, & addr, GX_CHANNEL_COUNT, 0);
    pump (800);
    CHECK (gx_is_bound (client.sess) && gx_is_bound (server.sess), "reconnect: rebound via token");
    pump (500);
    { int seen [130]; int ok = 1, cnt = 0;
      memset (seen, 0, sizeof seen);
      for (int i = 0; i < recvCount; i ++) {
          enet_uint32 v = recvSeqs [i];
          if (recvChans [i] != GX_CHANNEL_RELIABLE) continue;
          if (v < 100 || v >= 130) continue;   /* 前面用例的消息不参与 */
          if (seen [v]) { ok = 0; break; }
          seen [v] = 1; cnt ++;
      }
      for (enet_uint32 v = 100; v < 130; v ++) if (! seen [v]) ok = 0;
      if (! ok) {
          printf ("    recvCount=%d got:", recvCount);
          for (int i = 0; i < recvCount; i ++)
              if (recvChans [i] == GX_CHANNEL_RELIABLE) printf (" %u", recvSeqs [i]);
          printf ("\n    client: resent=%llu unacked=%u queued=%u | server: dup=%llu recvSeq?\n",
              (unsigned long long) gx_stats (client.sess) -> reliableResent,
              gx_stats (client.sess) -> reliableUnacked,
              gx_stats (client.sess) -> reliableQueued,
              (unsigned long long) gx_stats (server.sess) -> reliableDupDropped);
      }
      CHECK (ok && cnt == 30, "reconnect: reliable 100..129 exactly-once"); }
    { int unrelAfter = 0;
      for (int i = recvBase; i < recvCount; i ++)
          if (recvChans [i] == GX_CHANNEL_UNRELIABLE) unrelAfter ++;
      CHECK (unrelAfter == 0, "reconnect: stale unreliable not resent"); }

    /* 6. 参数边界 */
    CHECK (gx_send (client.sess, GX_CHANNEL_RELIABLE, "x", GX_MAX_PAYLOAD + 1, 0) == GX_ERR_TOO_BIG,
           "oversize payload rejected");
    CHECK (gx_send (client.sess, 7, "x", 1, 0) == GX_ERR_ARG, "bad channel rejected");

    printf (failures ? "RESULT: %d FAILURES\n" : "RESULT: ALL PASS\n", failures);
    return failures ? 1 : 0;
}
