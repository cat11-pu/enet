#define _GNU_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "enet/enet.h"

#define CLIENT_COUNT              128
#define RELIABLE_EVENTS_PER_CLIENT 16
#define RELIABLE_PACKET_SIZE      512
#define NOISE_COMMANDS            32
#define NOISE_PACKET_SIZE         128
#define POSITION_PACKET_SIZE      64
#define POSITION_INTERVAL_MS      16
#define POSITION_PHASE_MS         2000
#define MAX_POSITIONS_PER_CLIENT  256
#define CONNECT_TIMEOUT_MS        10000
#define RUN_TIMEOUT_MS            60000

enum
{
   BENCH_MAGIC = 0x454E4554,
   BENCH_TYPE_RELIABLE = 1,
   BENCH_TYPE_POSITION = 2,
   BENCH_TYPE_NOISE = 3,
   BENCH_CHANNEL_RELIABLE = 0,
   BENCH_CHANNEL_POSITION = 1,
   BENCH_CHANNEL_NOISE = 2
};

typedef struct _BenchHeader
{
   uint32_t magic;
   uint16_t client;
   uint8_t type;
   uint8_t reserved;
   uint32_t sequence;
   uint32_t sendTime;
   uint32_t state;
} BenchHeader;

typedef struct _Client
{
   ENetHost * host;
   ENetPeer * peer;
   int connected;
   ENetPacket * pendingReliable [RELIABLE_EVENTS_PER_CLIENT];
   int pendingReliableCount;
   uint32_t nextPosition;
   uint32_t nextNoise;
   uint64_t lastPositionTime;
} Client;

typedef struct _RunStats
{
   uint64_t peakReliableQueuedBytes;
   uint64_t peakReliableInTransitBytes;
   uint64_t peakUnreliableQueuedBytes;
   uint32_t peakUnreliableQueuedCommands;
   uint64_t reliableBackpressure;
   uint64_t unreliableDropped;
   uint64_t unreliableDroppedBytes;
   uint64_t unreliableThrottleDrops;
} RunStats;

static int sendLossPercent = 0;
static uint32_t sendRandomState = 0x12345678u;
static uint64_t sendAttempts = 0;
static uint64_t sendDrops = 0;
static uint64_t epochMs = 0;

static int reliableSeen [CLIENT_COUNT][RELIABLE_EVENTS_PER_CLIENT];
static uint8_t positionSeen [CLIENT_COUNT][MAX_POSITIONS_PER_CLIENT];
static uint32_t positionLatencies [CLIENT_COUNT * MAX_POSITIONS_PER_CLIENT];
static uint32_t positionQueueLatencies [CLIENT_COUNT * MAX_POSITIONS_PER_CLIENT];
static size_t positionLatencyCount = 0;
static size_t positionQueueLatencyCount = 0;
static size_t reliableReceived = 0;
static size_t duplicateReliable = 0;
static size_t disconnectCount = 0;

ssize_t __real_sendmsg (int, const struct msghdr *, int);

static size_t
messageLength (const struct msghdr * message)
{
   size_t length = 0;
   size_t index;

   for (index = 0; index < message -> msg_iovlen; ++ index)
     length += message -> msg_iov [index].iov_len;

   return length;
}

ssize_t
__wrap_sendmsg (int socket, const struct msghdr * message, int flags)
{
   if (sendLossPercent > 0)
   {
      sendRandomState ^= sendRandomState << 13;
      sendRandomState ^= sendRandomState >> 17;
      sendRandomState ^= sendRandomState << 5;

      ++ sendAttempts;
      if (sendRandomState % 100 < (uint32_t) sendLossPercent)
      {
         ++ sendDrops;
         return (ssize_t) messageLength (message);
      }
   }

   return __real_sendmsg (socket, message, flags);
}

static uint64_t
nowMilliseconds (void)
{
   struct timespec timeValue;

   clock_gettime (CLOCK_MONOTONIC, & timeValue);

   return (uint64_t) timeValue.tv_sec * 1000 + (uint64_t) timeValue.tv_nsec / 1000000;
}

static void
recordPositionQueueLatency (ENetPacket * packet)
{
   BenchHeader header;

   if (packet -> dataLength < sizeof (header))
     return;

   memcpy (& header, packet -> data, sizeof (header));
   if (header.magic != BENCH_MAGIC || header.type != BENCH_TYPE_POSITION)
     return;

   if (positionQueueLatencyCount < sizeof (positionQueueLatencies) / sizeof (positionQueueLatencies [0]))
     positionQueueLatencies [positionQueueLatencyCount ++] =
       (uint32_t) (nowMilliseconds () - epochMs) - header.sendTime;
}

static void
fillPacket (ENetPacket * packet, uint16_t client, uint8_t type, uint32_t sequence, uint32_t state)
{
   BenchHeader header;

   memset (& header, 0, sizeof (header));
   header.magic = BENCH_MAGIC;
   header.client = client;
   header.type = type;
   header.sequence = sequence;
   header.sendTime = (uint32_t) (nowMilliseconds () - epochMs);
   header.state = state;

   memcpy (packet -> data, & header, sizeof (header));
}

static ENetPacket *
createBenchPacket (size_t size, uint32_t flags, uint16_t client, uint8_t type, uint32_t sequence, uint32_t state)
{
   ENetPacket * packet = enet_packet_create (NULL, size, flags);

   if (packet == NULL)
     return NULL;

   fillPacket (packet, client, type, sequence, state);
   return packet;
}

static void
queueReliableEvent (Client * client, uint16_t clientID, uint32_t sequence)
{
   ENetPacket * packet = createBenchPacket (RELIABLE_PACKET_SIZE, ENET_PACKET_FLAG_RELIABLE,
                                            clientID, BENCH_TYPE_RELIABLE, sequence, 0);
   int result;

   if (packet == NULL)
     return;

   result = enet_peer_send_ex (client -> peer, BENCH_CHANNEL_RELIABLE, packet, ENET_SEND_PRIORITY_NORMAL);
   if (result == ENET_SEND_RESULT_OK)
     return;

   if (result == ENET_SEND_RESULT_BACKPRESSURE &&
       client -> pendingReliableCount < RELIABLE_EVENTS_PER_CLIENT)
   {
      client -> pendingReliable [client -> pendingReliableCount ++] = packet;
      return;
   }

   enet_packet_destroy (packet);
}

static void
retryReliableEvents (Client * client)
{
   while (client -> pendingReliableCount > 0)
   {
      ENetPacket * packet = client -> pendingReliable [0];
      int result = enet_peer_send_ex (client -> peer, BENCH_CHANNEL_RELIABLE, packet, ENET_SEND_PRIORITY_NORMAL);

      if (result == ENET_SEND_RESULT_BACKPRESSURE)
        return;

      if (result != ENET_SEND_RESULT_OK)
        enet_packet_destroy (packet);

      -- client -> pendingReliableCount;
      memmove (& client -> pendingReliable [0], & client -> pendingReliable [1],
               (size_t) client -> pendingReliableCount * sizeof (client -> pendingReliable [0]));
   }
}

static void
queuePosition (Client * client, uint16_t clientID, uint32_t sequence)
{
   ENetPacket * packet = createBenchPacket (POSITION_PACKET_SIZE, 0,
                                            clientID, BENCH_TYPE_POSITION, sequence, sequence * 37);

   if (packet == NULL)
     return;

   if (enet_peer_send_ex (client -> peer, BENCH_CHANNEL_POSITION, packet, ENET_SEND_PRIORITY_HIGH) != ENET_SEND_RESULT_OK)
     enet_packet_destroy (packet);
   else
     packet -> freeCallback = recordPositionQueueLatency;
}

static void
queueNoise (Client * client, uint16_t clientID, uint32_t sequence)
{
   ENetPacket * packet = createBenchPacket (NOISE_PACKET_SIZE, 0,
                                            clientID, BENCH_TYPE_NOISE, sequence, 0);

   if (packet == NULL)
     return;

   if (enet_peer_send_ex (client -> peer, BENCH_CHANNEL_NOISE, packet, ENET_SEND_PRIORITY_LOW) != ENET_SEND_RESULT_OK)
     enet_packet_destroy (packet);
}

static void
handleServerEvent (ENetEvent * event)
{
   BenchHeader header;
   uint64_t now;

   if (event -> type == ENET_EVENT_TYPE_CONNECT)
     enet_peer_timeout (event -> peer, 1024, 30000, 120000);
   else
   if (event -> type == ENET_EVENT_TYPE_RECEIVE)
   {
      if (event -> packet -> dataLength >= sizeof (header))
      {
         memcpy (& header, event -> packet -> data, sizeof (header));

         if (header.magic == BENCH_MAGIC && header.client < CLIENT_COUNT)
         {
            if (header.type == BENCH_TYPE_RELIABLE &&
                header.sequence < RELIABLE_EVENTS_PER_CLIENT)
            {
               if (reliableSeen [header.client][header.sequence])
                 ++ duplicateReliable;
               else
               {
                  reliableSeen [header.client][header.sequence] = 1;
                  ++ reliableReceived;
               }
            }
            else
            if (header.type == BENCH_TYPE_POSITION &&
                header.sequence < MAX_POSITIONS_PER_CLIENT)
            {
               if (! positionSeen [header.client][header.sequence])
               {
                  now = nowMilliseconds ();
                  positionSeen [header.client][header.sequence] = 1;

                  if (positionLatencyCount < sizeof (positionLatencies) / sizeof (positionLatencies [0]))
                    positionLatencies [positionLatencyCount ++] =
                      (uint32_t) (now - epochMs) - header.sendTime;
               }
            }
         }
      }

      enet_packet_destroy (event -> packet);
   }
   else
   if (event -> type == ENET_EVENT_TYPE_DISCONNECT)
     ++ disconnectCount;
}

static void
serviceServer (ENetHost * server)
{
   ENetEvent event;
   int result;

   do
   {
      result = enet_host_service (server, & event, 0);
      if (result > 0)
        handleServerEvent (& event);
   } while (result > 0);
}

static int
compareLatencies (const void * left, const void * right)
{
   uint32_t leftValue = * (const uint32_t *) left,
            rightValue = * (const uint32_t *) right;

   return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

static uint32_t
percentile (size_t percentileValue)
{
   size_t index;

   if (positionLatencyCount == 0)
     return 0;

   index = (positionLatencyCount - 1) * percentileValue / 100;
   return positionLatencies [index];
}

static void
sampleStats (Client * clients, RunStats * stats, uint64_t * currentReliableBytes,
             uint64_t * currentReliableInTransit, uint64_t * currentUnreliableBytes,
             uint64_t * currentUnreliableCommands)
{
   ENetSendSchedulerStats peerStats;
   int clientIndex;

   * currentReliableBytes = 0;
   * currentReliableInTransit = 0;
   * currentUnreliableBytes = 0;
   * currentUnreliableCommands = 0;

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
   {
      enet_peer_send_scheduler_get_stats (clients [clientIndex].peer, & peerStats);

      * currentReliableBytes += peerStats.reliableQueuedBytes;
      * currentReliableInTransit += peerStats.reliableInTransitBytes;
      * currentUnreliableBytes += peerStats.unreliableQueuedBytes;
      * currentUnreliableCommands += peerStats.unreliableQueuedCommands;
   }

   if (* currentReliableBytes > stats -> peakReliableQueuedBytes)
     stats -> peakReliableQueuedBytes = * currentReliableBytes;
   if (* currentReliableInTransit > stats -> peakReliableInTransitBytes)
     stats -> peakReliableInTransitBytes = * currentReliableInTransit;
   if (* currentUnreliableBytes > stats -> peakUnreliableQueuedBytes)
     stats -> peakUnreliableQueuedBytes = * currentUnreliableBytes;
   if (* currentUnreliableCommands > stats -> peakUnreliableQueuedCommands)
     stats -> peakUnreliableQueuedCommands = * currentUnreliableCommands;
}

static void
collectFinalStats (Client * clients, RunStats * stats)
{
   ENetSendSchedulerStats peerStats;
   uint64_t peakReliableQueuedBytes = 0,
            peakUnreliableQueuedBytes = 0;
   uint32_t peakUnreliableQueuedCommands = 0;
   int clientIndex;

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
   {
      enet_peer_send_scheduler_get_stats (clients [clientIndex].peer, & peerStats);
      stats -> reliableBackpressure += peerStats.reliableBackpressure;
      stats -> unreliableDropped += peerStats.unreliableDropped;
      stats -> unreliableDroppedBytes += peerStats.unreliableDroppedBytes;
      stats -> unreliableThrottleDrops += peerStats.unreliableThrottleDrops;
      peakReliableQueuedBytes += peerStats.peakReliableQueuedBytes;
      peakUnreliableQueuedBytes += peerStats.peakUnreliableQueuedBytes;
      peakUnreliableQueuedCommands += peerStats.peakUnreliableQueuedCommands;
   }

   if (peakReliableQueuedBytes > stats -> peakReliableQueuedBytes)
     stats -> peakReliableQueuedBytes = peakReliableQueuedBytes;
   if (peakUnreliableQueuedBytes > stats -> peakUnreliableQueuedBytes)
     stats -> peakUnreliableQueuedBytes = peakUnreliableQueuedBytes;
   if (peakUnreliableQueuedCommands > stats -> peakUnreliableQueuedCommands)
     stats -> peakUnreliableQueuedCommands = peakUnreliableQueuedCommands;
}

static int
runBenchmark (int lossPercent)
{
   ENetAddress serverAddress;
   ENetHost * server;
   Client clients [CLIENT_COUNT];
   RunStats stats;
   ENetSendSchedulerConfig schedulerConfig;
   uint64_t connectDeadline, phaseStart, now, currentReliableBytes, currentReliableInTransit,
            currentUnreliableBytes, currentUnreliableCommands, loopStart, maxLoopMs = 0;
   uint32_t expectedPositions = 0;
   int clientIndex, eventIndex;

   memset (clients, 0, sizeof (clients));
   memset (& stats, 0, sizeof (stats));
   memset (reliableSeen, 0, sizeof (reliableSeen));
   memset (positionSeen, 0, sizeof (positionSeen));
   positionLatencyCount = 0;
   positionQueueLatencyCount = 0;
   reliableReceived = 0;
   duplicateReliable = 0;
   disconnectCount = 0;
   sendAttempts = 0;
   sendDrops = 0;
   sendLossPercent = 0;

   enet_initialize ();
   epochMs = nowMilliseconds ();

   serverAddress.host = ENET_HOST_ANY;
   serverAddress.port = 0;
   enet_address_set_host_ip (& serverAddress, "127.0.0.1");
   server = enet_host_create (& serverAddress, CLIENT_COUNT, 3, 0, 0);
   if (server == NULL)
   {
      fprintf (stderr, "failed to create server\n");
      return 1;
   }

   enet_socket_get_address (server -> socket, & serverAddress);
   enet_socket_set_option (server -> socket, ENET_SOCKOPT_RCVBUF, 8 * 1024 * 1024);

   schedulerConfig.reliableWeight = 1;
   schedulerConfig.unreliableWeight = 4;
   schedulerConfig.maxReliableBytes = 6 * 1024;
   schedulerConfig.maxUnreliableBytes = 4 * 1024;
   schedulerConfig.maxReliableCommands = 32;
   schedulerConfig.maxUnreliableCommands = 32;

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
   {
      ENetAddress clientAddress;

      clientAddress.host = ENET_HOST_ANY;
      clientAddress.port = 0;

      clients [clientIndex].host = enet_host_create (& clientAddress, 1, 3, 0, 0);
      if (clients [clientIndex].host == NULL)
      {
         fprintf (stderr, "failed to create client %d\n", clientIndex);
         return 1;
      }

      clients [clientIndex].peer = enet_host_connect (clients [clientIndex].host, & serverAddress, 3, 0);
      if (clients [clientIndex].peer == NULL)
      {
         fprintf (stderr, "failed to connect client %d\n", clientIndex);
         return 1;
      }

      enet_peer_send_scheduler_configure (clients [clientIndex].peer, & schedulerConfig);
      enet_peer_timeout (clients [clientIndex].peer, 1024, 30000, 120000);
      enet_socket_set_option (clients [clientIndex].host -> socket, ENET_SOCKOPT_SNDBUF, 1024 * 1024);
   }

   connectDeadline = nowMilliseconds () + CONNECT_TIMEOUT_MS;
   for (;;)
   {
      int connectedCount = 0;

      serviceServer (server);

      for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
      {
         ENetEvent event;
         int result;

         do
         {
            result = enet_host_service (clients [clientIndex].host, & event, 0);
            if (result > 0 && event.type == ENET_EVENT_TYPE_CONNECT)
              clients [clientIndex].connected = 1;
            else
            if (result > 0 && event.type == ENET_EVENT_TYPE_DISCONNECT)
              ++ disconnectCount;
         } while (result > 0);

         if (clients [clientIndex].connected)
           ++ connectedCount;
      }

      if (connectedCount == CLIENT_COUNT && server -> connectedPeers == CLIENT_COUNT)
        break;

      now = nowMilliseconds ();
      if (now >= connectDeadline)
      {
         fprintf (stderr, "connection timeout: %d/%zu\n", connectedCount, server -> connectedPeers);
         return 1;
      }

      usleep (1000);
   }

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
   {
      for (eventIndex = 0; eventIndex < RELIABLE_EVENTS_PER_CLIENT; ++ eventIndex)
      {
         queueReliableEvent (& clients [clientIndex], (uint16_t) clientIndex, (uint32_t) eventIndex);
      }

      for (eventIndex = 0; eventIndex < NOISE_COMMANDS; ++ eventIndex)
        queueNoise (& clients [clientIndex], (uint16_t) clientIndex, (uint32_t) eventIndex);
   }

   sendLossPercent = lossPercent;
   phaseStart = nowMilliseconds ();

   for (;;)
   {
      loopStart = nowMilliseconds ();
      serviceServer (server);

      for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
      {
         Client * client = & clients [clientIndex];

         retryReliableEvents (client);

         now = nowMilliseconds ();
         if (now - phaseStart < POSITION_PHASE_MS &&
             now - client -> lastPositionTime >= POSITION_INTERVAL_MS)
         {
            queuePosition (client, (uint16_t) clientIndex, client -> nextPosition ++);
            client -> lastPositionTime = now;
         }

         enet_host_service (client -> host, NULL, 0);

         if ((clientIndex & 7) == 7)
           serviceServer (server);
      }

      serviceServer (server);
      sampleStats (clients, & stats, & currentReliableBytes, & currentReliableInTransit,
                   & currentUnreliableBytes, & currentUnreliableCommands);

      now = nowMilliseconds ();
      if (now - loopStart > maxLoopMs)
        maxLoopMs = now - loopStart;

      if (now - phaseStart >= POSITION_PHASE_MS &&
          reliableReceived == CLIENT_COUNT * RELIABLE_EVENTS_PER_CLIENT)
        break;

      if (now - phaseStart >= RUN_TIMEOUT_MS)
      {
         fprintf (stderr, "run timeout: reliable %zu/%d, positions %zu\n",
                  reliableReceived, CLIENT_COUNT * RELIABLE_EVENTS_PER_CLIENT,
                  positionLatencyCount);
         break;
      }

      usleep (1000);
   }

   collectFinalStats (clients, & stats);

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
     expectedPositions += clients [clientIndex].nextPosition;

   qsort (positionLatencies, positionLatencyCount, sizeof (positionLatencies [0]), compareLatencies);
   qsort (positionQueueLatencies, positionQueueLatencyCount, sizeof (positionQueueLatencies [0]), compareLatencies);

   printf ("loss_percent=%d\n", lossPercent);
   printf ("clients=%d\n", CLIENT_COUNT);
   printf ("reliable_expected=%d\n", CLIENT_COUNT * RELIABLE_EVENTS_PER_CLIENT);
   printf ("reliable_received=%zu\n", reliableReceived);
   printf ("reliable_duplicates=%zu\n", duplicateReliable);
   printf ("positions_expected=%u\n", expectedPositions);
   printf ("positions_received=%zu\n", positionLatencyCount);
   printf ("position_loss_percent=%.2f\n",
           expectedPositions > 0
             ? 100.0 * (double) (expectedPositions - positionLatencyCount) / (double) expectedPositions
             : 0.0);
   printf ("position_latency_p50_ms=%u\n", percentile (50));
   printf ("position_latency_p95_ms=%u\n", percentile (95));
   printf ("position_latency_p99_ms=%u\n", percentile (99));
   printf ("position_latency_max_ms=%u\n",
           positionLatencyCount > 0 ? positionLatencies [positionLatencyCount - 1] : 0);
   printf ("position_scheduler_latency_p50_ms=%u\n",
           positionQueueLatencyCount > 0 ? positionQueueLatencies [(positionQueueLatencyCount - 1) * 50 / 100] : 0);
   printf ("position_scheduler_latency_p95_ms=%u\n",
           positionQueueLatencyCount > 0 ? positionQueueLatencies [(positionQueueLatencyCount - 1) * 95 / 100] : 0);
   printf ("position_scheduler_latency_p99_ms=%u\n",
           positionQueueLatencyCount > 0 ? positionQueueLatencies [(positionQueueLatencyCount - 1) * 99 / 100] : 0);
   printf ("position_scheduler_latency_max_ms=%u\n",
           positionQueueLatencyCount > 0 ? positionQueueLatencies [positionQueueLatencyCount - 1] : 0);
   printf ("peak_reliable_queued_bytes=%llu\n", (unsigned long long) stats.peakReliableQueuedBytes);
   printf ("peak_reliable_in_transit_bytes=%llu\n", (unsigned long long) stats.peakReliableInTransitBytes);
   printf ("peak_unreliable_queued_bytes=%llu\n", (unsigned long long) stats.peakUnreliableQueuedBytes);
   printf ("peak_unreliable_queued_commands=%u\n", stats.peakUnreliableQueuedCommands);
   printf ("reliable_backpressure=%llu\n", (unsigned long long) stats.reliableBackpressure);
   printf ("unreliable_priority_drops=%llu\n", (unsigned long long) stats.unreliableDropped);
   printf ("unreliable_priority_drop_bytes=%llu\n", (unsigned long long) stats.unreliableDroppedBytes);
   printf ("unreliable_throttle_drops=%llu\n", (unsigned long long) stats.unreliableThrottleDrops);
   printf ("udp_send_attempts=%llu\n", (unsigned long long) sendAttempts);
   printf ("udp_send_drops=%llu\n", (unsigned long long) sendDrops);
   printf ("max_event_loop_ms=%llu\n", (unsigned long long) maxLoopMs);
   printf ("disconnects=%zu\n", disconnectCount);
   printf ("connected_peers_final=%zu\n", server -> connectedPeers);

   for (clientIndex = 0; clientIndex < CLIENT_COUNT; ++ clientIndex)
   {
      while (clients [clientIndex].pendingReliableCount > 0)
        enet_packet_destroy (clients [clientIndex].pendingReliable [-- clients [clientIndex].pendingReliableCount]);

      enet_host_destroy (clients [clientIndex].host);
   }

   enet_host_destroy (server);
   enet_deinitialize ();

   return reliableReceived == CLIENT_COUNT * RELIABLE_EVENTS_PER_CLIENT ? 0 : 2;
}

int
main (int argc, char ** argv)
{
   int lossPercent = 0;

   if (argc > 1)
     lossPercent = atoi (argv [1]);

   if (lossPercent < 0 || lossPercent > 100)
   {
      fprintf (stderr, "usage: %s [loss-percent]\n", argv [0]);
      return 1;
   }

   return runBenchmark (lossPercent);
}
