# Failure and recovery

## Sub-features

Stream-leader loss, majority operation, quorum loss, durable channel state and whole-cluster store recovery.

## How to get to it (user POV)

Use the driver's owned synthetic cluster. Never point the destructive fault journey at a user cluster.

## Driving it with Python

Run `python coursework/verify.py`. It discovers the actual stream leader through monitoring, kills that PID, proves another accepted write, kills another node and requires bounded write failure. Restart uses the same stores. The oracle compares every accepted sequence against each channel and requires three healthy replicas after restart.

## Gotchas

A timed-out publish can still commit. The ledger tracks acknowledged writes; uncertain writes are identified separately. Loopback process crashes do not prove datacenter, disk corruption or partition tolerance.
