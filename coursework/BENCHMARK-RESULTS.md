# Replicated broker measurement, 22 October 2026

Run [39747710892](https://github.com/STEVVVU/distributed-message-broker/actions/runs/39747710892), clean source `380590c72342e909888a0ad0b708109c45c08273`, completed 46 trials. All 40 matrix cells, both lower sweeps, 120-second stability, backlog drain and leader loss passed. The overall verdict remains **false** because the 500 messages/s sweep declined offers. No check or negative cell was removed.

The runner exposed four Linux x86_64 cores, kernel 6.17.0-1022-azure/glibc 2.39, Python 3.11.17 and Go 1.26.0; `GOMAXPROCS=2`, `GOMEMLIMIT=512MiB`. CPU model/total RAM were not captured. Three real NATS processes and one HTTP adapter used fresh stores, authenticated loopback traffic and exact payload/stream-sequence/ACK proof. Results describe this adapter/workload, not native NATS maximum capacity or an original consensus implementation.

## Five-repeat matrix

Each fresh trial measured 64 messages after an untimed readiness journey. Values below are median (full minimum–maximum) over all five runs. Throughput uses actual load duration including drain; scheduled-to-delivery+ACK tails include client queueing. These short batches are distinct from sustained offered-rate capacity.

| Payload bytes | Producers | Consumers | Finished messages/s | Delivery+ACK p99, ms |
| --- | --- | --- | --- | --- |
| 1,024 | 1 | 1 | 311.544 (305.565–354.485) | 203.210 (178.474–205.299) |
| 64 | 4 | 4 | 426.065 (423.770–428.557) | 145.051 (143.786–145.556) |
| 1,024 | 4 | 4 | 423.863 (419.094–427.768) | 144.894 (141.928–146.545) |
| 16,384 | 4 | 4 | 375.068 (363.720–381.202) | 164.987 (161.844–169.045) |
| 1,024 | 1 | 4 | 339.305 (328.951–354.291) | 185.530 (177.125–190.714) |
| 1,024 | 16 | 4 | 385.589 (380.131–395.196) | 138.563 (131.658–147.655) |
| 1,024 | 4 | 1 | 305.277 (300.153–307.182) | 205.477 (204.333–209.007) |
| 1,024 | 4 | 16 | 427.657 (405.728–440.655) | 142.109 (138.329–150.194) |

## Offered load and recovery

The following are single observations at 1,024 bytes, four producers and four consumers. All accepted messages were received and ACK-confirmed with matching sequences. There were zero final failed/rejected/timed-out or unresolved logical messages. Fault-transient HTTP uncertainties are separately retained below.

| Offered load | Offers / finished / generator declines | End-to-end p50 / p95 / p99, ms |
| --- | --- | --- |
| 10/s, 10 s | 100 / 100 / 0 | 2.921 / 3.222 / 3.454 |
| 100/s, 10 s | 1,000 / 1,000 / 0 | 2.548 / 2.774 / 3.082 |
| 500/s, 10 s | 5,000 / 2,874 / 2,126 | 225.786 / 236.435 / 240.987 |
| 100/s, 120 s | 12,000 / 12,000 / 0 | 2.713 / 3.067 / 3.374 |
| Backlog/drain | 64 / 64 / 0 | 93.207 / 139.538 / 140.344 |
| Leader loss | 64 / 64 / 0 | 77.859 / 6,812.379 / 6,817.481 |

All 20,788 measured offers balance as 18,662 exact accepted/received/ACK-confirmed messages plus 2,126 generator declines. Each trial also retained one untimed readiness message at sequence 1, excluded from measured offers, useful bytes and latency. Forty-five probes published once; trial 17 retained an initial HTTP 503 `no_stream_response` and retried the identical readiness ID before succeeding. All 46 probes received and ACKed exactly once. Every native final stream contains measured accepted messages plus that one probe. Stability retained 12,001 messages / 13,297,058 bytes, below the unchanged 32 MiB cap; this run does not test admission at that cap.

Leader loss retained 16 publish-uncertain attempts (15 `no_stream_response`, one timeout), 92 ACK-uncertain attempts (90 `no_responders`, two timeouts), and five transient pull HTTP 503 `no_responders`. Same-ID/receipt retries resolved all publish/ACK uncertainty; no duplicates/redeliveries or missing delivery sequences remained. All five pull errors remain visible and satisfy the narrowly versioned post-kill recovery classification. New post-kill identities first confirmed publish at 5,931.525 ms and ACK at 6,676.289 ms. The separate any-ACK observation was 0.385 ms and includes pre-kill in-flight work; it is not failover recovery time. All consumer threads/processes/samplers stopped, with no cleanup errors or trace overflow.

At 500/s the Python generator averaged 86.269% of one core with 43.393 MB peak RSS; accepted dispatch p99 was 8.043 ms. The 32-outstanding-action bound declined 2,126 offers. At sustained 100/s the generator averaged 15.653% / 64.881 MB, adapter 20.808% / 16.519 MB, and NATS servers 21.635%, 12.143%, 20.369% with peaks 61.972, 66.773, 67.568 MB. These 100 ms `/proc` observations include setup/shutdown and use each process's observed lifecycle. They do not establish the service's maximum throughput or a profiled bottleneck. Align separate native/adapter profiling with the actual overload before any product optimization; current native profile mode's small closed-loop cell is not that proof.

Reproduce with the builds and pinned environment in `.github/workflows/benchmark.yml`, then `python coursework/benchmark.py smoke` and `python coursework/benchmark.py benchmark` on isolated Linux. No cross-run speedup is claimed. The baseline uses workload version 3 / outcome accounting version 1, separate from earlier warmup/accounting versions.

Raw evidence remains under ignored `.local/` in the hosted artifact and local audit directory `C:/Users/steve/.codex/orchestrate/metrics30-20261022/evidence/P06-39747710892`. Summary `benchmark-benchmark-1792657634538179552/summary.json` SHA256: `81eb3ba4df5dcc0c9b2eecee997b350c7ce14991679f1bebe1216a57163fa173`. The original 8,840,459-byte ZIP, full central directory/source/hash receipt and 48,700,016-byte expansion are preserved. Artifact selection excludes configs, capabilities, signed receipts and native stores. Build gates, 17 pure controls and real smoke passed; scoped Go unit/vet checks are not separate steps in this benchmark workflow. No local build, service or timed load ran for curation.
