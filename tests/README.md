## Scheduler benchmark

`scheduler_benchmark.c` exercises the per-peer outgoing scheduler with 128
real loopback ENet connections.

It uses a `sendmsg` link wrapper:

- after all connections are established, the wrapper drops the requested
  percentage of UDP datagrams in both directions;
- reliable events use one channel, position updates use another, and
  low-priority noise uses a third;
- every peer starts with a full reliable queue and a full unreliable queue;
- position updates are sent at 60 Hz and their scheduler residence and
  end-to-end receive latency are reported.

Build and run:

```sh
cmake -S . -B build -DENET_BUILD_BENCHMARKS=ON
cmake --build build
./build/enet_scheduler_benchmark 0
./build/enet_scheduler_benchmark 30
```

`0` is the no-loss control group. `30` drops 30% of UDP datagrams. The process
returns non-zero if any reliable event is missing.
