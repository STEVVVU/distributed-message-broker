---
name: verify-broker
description: Launch and drive the real three-process NATS broker and topic/channel HTTP adapter, including crash recovery.
---

## Launch

Build `.local/nats-server.exe` with `go build -o .local/nats-server.exe .` and the adapter with `go build -o .local/topic-channel.exe ./coursework/cmd/topic-channel`. Run `python coursework/verify.py`. The driver allocates loopback ports and a unique evidence directory, owns all child processes, and starts three servers with distinct persistent stores.

## Doctor

The driver checks each owned PID, `/healthz?js-enabled-only=true` and `/jsz` on its assigned monitoring port. It waits for the stream to report a leader and two current replicas before fault injection. `python coursework/verify.py --baseline` drives the inherited broker before adapter implementation.

## Drive

`python coursework/verify.py` exercises HTTP topic and channel creation, publish, pull, ACK, duplicate IDs, consumer loss, leader loss, loss of quorum, store restart and complete cluster restart. It uses the portable Python client. Assertions fail the process and retain failure evidence.

## Evidence

`.local/evidence/<run>/` retains configurations, process logs, raw HTTP actions and replies, accepted-message ledger, restart observations and `result.json`. Authentication tokens are redacted in HTTP evidence; configuration tokens are local generated secrets. This directory is ignored by Git.

## Cleanup

The driver's `finally` block terminates only PIDs it started and waits for exit. Stores and proof remain under the unique run directory. Evidence is checked after process cleanup. No process-name killing, global ports or external services. `python coursework/verify.py --inject-failure` deliberately exits nonzero after launch; require `result.json` to say `passed: false` and `events.json` to show cleanup with `all_exited: true`.

## Helpers

`coursework/benchmark.py` and `coursework/benchmark_support.py` add the [frozen benchmark path](features/benchmark.md). Start with `python coursework/benchmark.py smoke`, then use isolated Linux `benchmark` or separate `profile` mode. The existing correctness driver remains the full fault/semantic baseline.

The maintained driver is `coursework/verify.py`; its invocation above needs Python 3.11 or newer and built executables. See `features/README.md` for feature journeys. Keep the map current with the PStack maintain-verification-skill workflow when behavior changes.
