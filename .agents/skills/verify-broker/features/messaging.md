# Durable topic/channel messaging

## Sub-features

Replicated topics, independent channel fanout, shared work, publisher deduplication, explicit ACK and redelivery.

## How to get to it (user POV)

Start the adapter against a three-server JetStream cluster. Use the Python `Broker` client with its bearer capability.

## Driving it with Python

Run `python coursework/verify.py`. The journey records accepted publish sequences, pulls both channels, shares work across client instances and ACKs each receipt. An unacknowledged delivery must reappear with an increased delivery count.

## Gotchas

Delivery is at least once. ACK timeout permits duplicates. Deduplication is bounded by the stream duplicate window. Pull order does not imply concurrent processing order.
