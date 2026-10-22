# Distributed message broker

The complete NATS server is inherited upstream code. Keep its public APIs, authors and notices intact. The course extension is in `coursework/`; no upstream consensus changes are needed for this adapter.

Run `go test ./coursework/...`, `go vet ./coursework/...`, build both executables as documented in README, then run `python coursework/verify.py`. The real-product driver and feature map live at `.agents/skills/verify-broker/`. Keep generated evidence and credentials under ignored `.local/`.

Upstream automation is preserved outside the active workflow directory. The only active workflow exercises the adaptation without deployments or external services. Do not enable upstream release automation in this clone.
