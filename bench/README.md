# Send batching benchmark

The benchmark connects N loopback ENet peers, queues 64 eight-byte
unreliable packets per peer, and times `enet_host_flush`.

Build and run:

```sh
cmake -S . -B build -DENET_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
build/enet-send-batch-bench --connections 256 --packets 64 \
  --iterations 150 --warmup 20 --trace packets.csv
```

A receiver thread drains the client host concurrently, like a separate
game client. The trace records every received packet as
`connection,sequence,length,hash`. Sort traces by connection and sequence
before comparing baseline and optimized runs, because UDP does not define
ordering between different connections.

The checked-in measurement summary is in
`artifacts/send-batch/final/summary.csv`.
