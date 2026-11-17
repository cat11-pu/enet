#define _POSIX_C_SOURCE 200809L

#include <enet/enet.h>

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum
{
    BENCH_CHANNEL = 0,
    BENCH_PAYLOAD_SIZE = 8,
    BENCH_SETUP_TIMEOUT_MS = 30000
};

typedef struct
{
    size_t connections;
    size_t packetsPerConnection;
    size_t iterations;
    size_t warmupIterations;
    const char * tracePath;
} BenchOptions;

typedef struct
{
    uint64_t count;
    uint64_t digest;
    uint64_t hashes[ENET_PROTOCOL_MAXIMUM_PEER_ID];
    int lastSequence[ENET_PROTOCOL_MAXIMUM_PEER_ID];
    uint64_t expectedCount;
    FILE * trace;
} ReceiveState;

typedef struct
{
    ENetHost * host;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    ReceiveState state;
    uint64_t generation;
    uint64_t activeGeneration;
    uint64_t completedGeneration;
    uint64_t traceGeneration;
    size_t connections;
    size_t packetsPerConnection;
    FILE * trace;
    int stop;
    int error;
} Receiver;

static double
nowSeconds (void)
{
    struct timespec value;

    clock_gettime (CLOCK_MONOTONIC, & value);

    return (double) value.tv_sec + (double) value.tv_nsec / 1000000000.0;
}

static double
nowThreadCpuSeconds (void)
{
    struct timespec value;

    clock_gettime (CLOCK_THREAD_CPUTIME_ID, & value);

    return (double) value.tv_sec + (double) value.tv_nsec / 1000000000.0;
}

static uint64_t
fnv1a (uint64_t hash, const void * data, size_t dataLength)
{
    const unsigned char * bytes = (const unsigned char *) data;
    size_t index;

    for (index = 0; index < dataLength; ++ index)
    {
        hash ^= bytes [index];
        hash *= UINT64_C (1099511628211);
    }

    return hash;
}

static void
resetReceiveState (ReceiveState * state, size_t connections, size_t packetsPerConnection, FILE * trace);

static void
makePayload (unsigned char * payload, size_t connection, size_t sequence)
{
    uint32_t connectionValue = (uint32_t) connection,
             sequenceValue = (uint32_t) sequence;
    size_t index;

    memcpy (payload, & connectionValue, sizeof (connectionValue));
    memcpy (payload + sizeof (connectionValue), & sequenceValue, sizeof (sequenceValue));

    for (index = 2 * sizeof (uint32_t); index < BENCH_PAYLOAD_SIZE; ++ index)
      payload [index] = (unsigned char) (connection * 31 + sequence * 17 + index);
}

static int
validateReceiveEvent (ReceiveState * state, ENetEvent * event)
{
    unsigned char expected [BENCH_PAYLOAD_SIZE];
    uint32_t connection,
             sequence;

    if (event -> type == ENET_EVENT_TYPE_DISCONNECT)
      return -1;

    if (event -> type != ENET_EVENT_TYPE_RECEIVE)
      return 0;

    if (event -> channelID != BENCH_CHANNEL ||
        event -> packet -> dataLength != BENCH_PAYLOAD_SIZE)
      return -1;

    memcpy (& connection, event -> packet -> data, sizeof (connection));
    memcpy (& sequence, event -> packet -> data + sizeof (connection), sizeof (sequence));

    if (connection >= ENET_PROTOCOL_MAXIMUM_PEER_ID ||
        sequence != (uint32_t) (state -> lastSequence [connection] + 1))
      return -1;

    makePayload (expected, connection, sequence);

    if (memcmp (expected, event -> packet -> data, sizeof (expected)) != 0)
      return -1;

    state -> lastSequence [connection] = (int) sequence;
    state -> hashes [connection] = fnv1a (state -> hashes [connection],
                                         event -> packet -> data,
                                         event -> packet -> dataLength);
    ++ state -> count;

    if (state -> trace != NULL)
    {
        fprintf (state -> trace, "%" PRIu32 ",%" PRIu32 ",%zu,%016" PRIx64 "\n",
                 connection, sequence, event -> packet -> dataLength,
                 fnv1a (UINT64_C (1469598103934665603),
                        event -> packet -> data, event -> packet -> dataLength));
    }

    return 0;
}

static void *
receiverThread (void * argument)
{
    Receiver * receiver = (Receiver *) argument;
    uint64_t processedGeneration = 0;

    for (;;)
    {
        ENetEvent event;
        int result;

        pthread_mutex_lock (& receiver -> mutex);

        while (! receiver -> stop &&
               receiver -> generation == processedGeneration)
          pthread_cond_wait (& receiver -> condition, & receiver -> mutex);

        if (receiver -> stop)
        {
            pthread_mutex_unlock (& receiver -> mutex);

            break;
        }

        processedGeneration = receiver -> generation;
        resetReceiveState (& receiver -> state,
                           receiver -> connections,
                           receiver -> packetsPerConnection,
                           processedGeneration == receiver -> traceGeneration ?
                             receiver -> trace : NULL);
        receiver -> activeGeneration = processedGeneration;
        pthread_cond_broadcast (& receiver -> condition);

        pthread_mutex_unlock (& receiver -> mutex);

        for (;;)
        {
            int frameComplete = 0;

            result = enet_host_service (receiver -> host, & event, 10);
            if (result < 0)
              break;

            if (result == 0)
              continue;

            pthread_mutex_lock (& receiver -> mutex);

            result = validateReceiveEvent (& receiver -> state, & event);

            if (result >= 0 && event.type == ENET_EVENT_TYPE_RECEIVE)
              enet_packet_destroy (event.packet);

            if (result < 0)
            {
                receiver -> error = 1;
                pthread_cond_broadcast (& receiver -> condition);
                frameComplete = 1;
            }
            else
            if (receiver -> state.count == receiver -> state.expectedCount)
            {
                receiver -> completedGeneration = processedGeneration;
                pthread_cond_broadcast (& receiver -> condition);
                frameComplete = 1;
            }

            pthread_mutex_unlock (& receiver -> mutex);

            if (frameComplete)
              break;
        }

        if (result < 0)
        {
            pthread_mutex_lock (& receiver -> mutex);
            receiver -> error = 1;
            pthread_cond_broadcast (& receiver -> condition);
            pthread_mutex_unlock (& receiver -> mutex);

          break;
        }
    }

    return NULL;
}

static int
waitForConnections (ENetHost * server, ENetHost * client, size_t connections)
{
    ENetEvent event;
    double deadline = nowSeconds () + BENCH_SETUP_TIMEOUT_MS / 1000.0;

    while (server -> connectedPeers < connections)
    {
        while (enet_host_service (client, & event, 0) > 0)
        {
            if (event.type == ENET_EVENT_TYPE_DISCONNECT)
              return -1;
        }

        while (enet_host_service (server, & event, 0) > 0)
        {
            if (event.type == ENET_EVENT_TYPE_DISCONNECT)
              return -1;
        }

        if (nowSeconds () >= deadline)
          return -1;

        enet_host_flush (client);
        enet_host_flush (server);
    }

    return 0;
}

static void
resetReceiveState (ReceiveState * state, size_t connections, size_t packetsPerConnection, FILE * trace)
{
    size_t connection;

    memset (state, 0, sizeof (* state));

    for (connection = 0; connection < connections; ++ connection)
    {
        state -> hashes [connection] = UINT64_C (1469598103934665603);
        state -> lastSequence [connection] = -1;
    }

    state -> expectedCount = connections * packetsPerConnection;
    state -> trace = trace;
}

static uint64_t
finishReceiveDigest (ReceiveState * state, size_t connections)
{
    uint64_t digest = UINT64_C (1469598103934665603);
    size_t connection;

    for (connection = 0; connection < connections; ++ connection)
    {
        uint64_t values [2] = { (uint64_t) connection, state -> hashes [connection] };

        digest = fnv1a (digest, values, sizeof (values));
    }

    return digest;
}

static int
parseOptions (int argc, char ** argv, BenchOptions * options)
{
    int index;

    memset (options, 0, sizeof (* options));

    for (index = 1; index < argc; ++ index)
    {
        if (strcmp (argv [index], "--connections") == 0 && index + 1 < argc)
          options -> connections = (size_t) strtoul (argv [++ index], NULL, 10);
        else
        if (strcmp (argv [index], "--packets") == 0 && index + 1 < argc)
          options -> packetsPerConnection = (size_t) strtoul (argv [++ index], NULL, 10);
        else
        if (strcmp (argv [index], "--iterations") == 0 && index + 1 < argc)
          options -> iterations = (size_t) strtoul (argv [++ index], NULL, 10);
        else
        if (strcmp (argv [index], "--warmup") == 0 && index + 1 < argc)
          options -> warmupIterations = (size_t) strtoul (argv [++ index], NULL, 10);
        else
        if (strcmp (argv [index], "--trace") == 0 && index + 1 < argc)
          options -> tracePath = argv [++ index];
        else
          return -1;
    }

    return options -> connections > 0 &&
           options -> connections <= ENET_PROTOCOL_MAXIMUM_PEER_ID &&
           options -> packetsPerConnection > 0 &&
           options -> iterations > 0 ? 0 : -1;
}

int
main (int argc, char ** argv)
{
    BenchOptions options;
    ENetAddress address;
    ENetHost * server = NULL,
             * client = NULL;
    Receiver receiver;
    pthread_t receiverThreadId;
    FILE * trace = NULL;
    size_t connection;
    size_t iteration;
    double sendSeconds = 0.0;
    double sendCpuSeconds = 0.0;
    uint64_t sentData;
    uint64_t sentPackets;
    uint64_t measuredPackets;
    uint64_t digest = 0;
    int receiverInitialized = 0;
    int receiverStarted = 0;
    int result = EXIT_FAILURE;

    if (parseOptions (argc, argv, & options) < 0)
    {
        fprintf (stderr,
                 "usage: %s --connections N --packets N --iterations N "
                 "[--warmup N] [--trace FILE]\n",
                 argv [0]);

        return EXIT_FAILURE;
    }

    if (enet_initialize () < 0)
      return EXIT_FAILURE;

    memset (& address, 0, sizeof (address));
    address.host = ENET_HOST_ANY;
    address.port = 0;

    server = enet_host_create (& address, options.connections, 1, 0, 0);
    client = enet_host_create (NULL, options.connections, 1, 0, 0);
    if (server == NULL || client == NULL)
      goto cleanup;

    if (enet_address_set_host (& server -> address, "127.0.0.1") < 0)
      goto cleanup;

    enet_socket_set_option (server -> socket, ENET_SOCKOPT_SNDBUF, 4 * 1024 * 1024);
    enet_socket_set_option (client -> socket, ENET_SOCKOPT_RCVBUF, 4 * 1024 * 1024);

    for (connection = 0; connection < options.connections; ++ connection)
    {
        if (enet_host_connect (client, & server -> address, 1, (enet_uint32) connection) == NULL)
          goto cleanup;
    }

    if (waitForConnections (server, client, options.connections) < 0)
    {
        fprintf (stderr, "failed to establish %zu connections\n", options.connections);

        goto cleanup;
    }

    for (connection = 0; connection < options.connections; ++ connection)
      server -> peers [connection].pingInterval = 0xFFFFFFFFU;

    if (options.tracePath != NULL)
    {
        trace = fopen (options.tracePath, "w");
        if (trace == NULL)
          goto cleanup;

        fprintf (trace, "connection,sequence,length,hash\n");
    }

    memset (& receiver, 0, sizeof (receiver));
    receiver.host = client;
    receiver.connections = options.connections;
    receiver.packetsPerConnection = options.packetsPerConnection;
    receiver.trace = trace;

    if (pthread_mutex_init (& receiver.mutex, NULL) != 0)
      goto cleanup;

    if (pthread_cond_init (& receiver.condition, NULL) != 0)
    {
      pthread_mutex_destroy (& receiver.mutex);

      goto cleanup;
    }

    receiverInitialized = 1;

    if (pthread_create (& receiverThreadId, NULL, receiverThread, & receiver) != 0)
      goto cleanup;

    receiverStarted = 1;

    sentData = 0;
    sentPackets = 0;

    for (iteration = 0;
         iteration < options.warmupIterations + options.iterations;
         ++ iteration)
    {
        double start;
        double cpuStart;
        double cpuElapsed;
        size_t sequence;

        if (iteration == options.warmupIterations)
        {
            sentData = server -> totalSentData;
            sentPackets = server -> totalSentPackets;
        }

        for (connection = 0; connection < options.connections; ++ connection)
        {
            for (sequence = 0; sequence < options.packetsPerConnection; ++ sequence)
            {
                unsigned char payload [BENCH_PAYLOAD_SIZE];
                ENetPacket * packet;

                makePayload (payload, connection, sequence);
                packet = enet_packet_create (payload, sizeof (payload), 0);
                if (packet == NULL)
                  goto cleanup;

                if (enet_peer_send (& server -> peers [connection], BENCH_CHANNEL, packet) < 0)
                {
                    enet_packet_destroy (packet);

                    goto cleanup;
                }
            }
        }

        pthread_mutex_lock (& receiver.mutex);

        ++ receiver.generation;
        if (iteration == options.warmupIterations)
          receiver.traceGeneration = receiver.generation;

        pthread_cond_broadcast (& receiver.condition);

        while (! receiver.error &&
               receiver.activeGeneration != receiver.generation)
          pthread_cond_wait (& receiver.condition, & receiver.mutex);

        if (receiver.error)
        {
            pthread_mutex_unlock (& receiver.mutex);

            goto cleanup;
        }

        pthread_mutex_unlock (& receiver.mutex);

        start = nowSeconds ();
        cpuStart = nowThreadCpuSeconds ();
        enet_host_flush (server);
        cpuElapsed = nowThreadCpuSeconds () - cpuStart;
        if (iteration >= options.warmupIterations)
        {
          sendSeconds += nowSeconds () - start;
          sendCpuSeconds += cpuElapsed;
        }

        pthread_mutex_lock (& receiver.mutex);

        while (! receiver.error &&
               receiver.completedGeneration != receiver.generation)
          pthread_cond_wait (& receiver.condition, & receiver.mutex);

        if (receiver.error ||
            receiver.state.count != receiver.state.expectedCount)
        {
            fprintf (stderr,
                     "receive validation failed: connection=%zu iteration=%zu "
                     "received=%" PRIu64 " expected=%" PRIu64 "\n",
                     connection, iteration, receiver.state.count,
                     receiver.state.expectedCount);

            pthread_mutex_unlock (& receiver.mutex);

            goto cleanup;
        }

        if (iteration >= options.warmupIterations)
        {
            uint64_t frameDigest = finishReceiveDigest (& receiver.state,
                                                         options.connections);

            digest = fnv1a (digest, & frameDigest, sizeof (frameDigest));
        }

        pthread_mutex_unlock (& receiver.mutex);
    }

    measuredPackets = server -> totalSentPackets - sentPackets;

    printf ("connections=%zu packets_per_connection=%zu iterations=%zu "
            "datagrams_per_frame=%zu packets_per_frame=%zu "
            "total_send_ms=%.3f send_ms_per_frame=%.3f "
            "total_send_cpu_ms=%.3f send_cpu_ms_per_frame=%.3f "
            "us_per_datagram=%.3f us_per_packet=%.3f "
            "sent_bytes=%" PRIu64 " digest=%016" PRIx64 "\n",
            options.connections,
            options.packetsPerConnection,
            options.iterations,
            (size_t) (measuredPackets / options.iterations),
            options.connections * options.packetsPerConnection,
            sendSeconds * 1000.0,
            sendSeconds * 1000.0 / options.iterations,
            sendCpuSeconds * 1000.0,
            sendCpuSeconds * 1000.0 / options.iterations,
            sendSeconds * 1000000.0 / measuredPackets,
            sendSeconds * 1000000.0 / options.iterations /
              (options.connections * options.packetsPerConnection),
            (uint64_t) server -> totalSentData - sentData,
            digest);

    result = EXIT_SUCCESS;

cleanup:
    if (receiverStarted)
    {
        pthread_mutex_lock (& receiver.mutex);
        receiver.stop = 1;
        pthread_cond_broadcast (& receiver.condition);
        pthread_mutex_unlock (& receiver.mutex);

        pthread_join (receiverThreadId, NULL);
    }

    if (receiverInitialized)
    {
        pthread_cond_destroy (& receiver.condition);
        pthread_mutex_destroy (& receiver.mutex);
    }

    if (trace != NULL)
      fclose (trace);

    if (server != NULL)
      enet_host_destroy (server);

    if (client != NULL)
      enet_host_destroy (client);

    enet_deinitialize ();

    return result;
}
