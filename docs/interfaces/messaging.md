# Messaging & broker interface

epistemic-graph carries a **RabbitMQ-class message broker** as a native modality (feature `broker`),
built on the KG-2.303 in-engine claim/ack work-queue: exchanges, queues, bindings, DLQ, TTL, priority,
delayed delivery, consumer groups, replayable streams, and publisher confirms — all as nodes in the
isolated `__control__` graph, Raft/WAL-safe. Three protocol wires front it: **AMQP 0.9.1**, **MQTT**,
and **STOMP**. All three share the **one** broker, so a message published over AMQP can be consumed over
MQTT/STOMP by topic.

> Status snapshot: the broker (exchanges/routing, DLQ, TTL, priority, delayed delivery, consumer groups +
> QoS/prefetch, replayable streams, publisher confirms + manual acks) is shipped (EG-275..284), as are the
> AMQP (EG-275), MQTT (EG-281), and STOMP (EG-282) wires. Program B adds **effectively-exactly-once**
> delivery (idempotent producer) + AMQP `confirm.select` / MQTT 5 frame exposure (EG-314). See the
> [capability matrix](../capabilities.md).

## The broker model

| Capability | What it is | Concept |
|------------|-----------|---------|
| **Exchanges + routing** | Durable exchanges (`direct`/`topic`/`fanout`) + bindings/routing-keys; queues are `__control__` graph nodes; publish/consume/ack are additive `Method::*` ops on the KG-2.303 claim path | EG-275 |
| **Dead-letter queues** | Per-queue DLQ: a message that exceeds max-delivery-attempts or is rejected routes to a configured dead-letter exchange/queue, original routing metadata preserved | EG-276 |
| **Message TTL + queue expiry** | Per-message and per-queue TTL; expired messages are dead-lettered (or dropped) on a lazy sweep at claim time + a periodic reaper | EG-277 |
| **Priority queues** | Priority-ordered delivery (higher priority claimed first, FIFO within a band) on the claim path | EG-278 |
| **Delayed / scheduled delivery** | Deliver-after (delay) + deliver-at (scheduled): messages held invisible until their eta, then made claimable, via a due-time index | EG-279 |
| **Consumer groups + QoS/prefetch** | Named consumer groups sharing a queue, per-consumer prefetch (unacked-in-flight limit), fair round-robin dispatch, per-consumer visibility leases | EG-280 |
| **Replayable streams** | Kafka/RabbitMQ-Streams-style persistent append-only logs: messages retained (not deleted on consume) in an ordered offset-indexed log; consumers read from an explicit offset (earliest/latest/N) and replay; retention by size/age | EG-283 |
| **Publisher confirms + consumer acks** | Per-message confirm/nack once durably enqueued (monotonic delivery-tag ⇒ at-least-once) + consumer manual-ack / nack-with-requeue over the claim path | EG-284 |
| **Exactly-once (idempotent producer)** | A producer-id + monotonic sequence lets the broker **drop duplicate publishes** (a retried publish after a lost confirm is de-duplicated) for effectively-exactly-once on top of at-least-once; the stream/confirm/ack ops are exposed over the AMQP `confirm.select` + MQTT 5 wire frames (previously engine-Method-only) | EG-314 |

Work-queue (claim/delete) semantics and stream (retain/replay) semantics coexist: the queue is the
task/RPC pattern, the stream is the event-log pattern.

## AMQP 0.9.1 wire (EG-275, feature `amqp-wire`)

A hand-rolled AMQP 0.9.1 server (no heavy AMQP crate) maps connection/channel/exchange/queue/`basic.*`
frames onto the broker primitives.

```bash
EPISTEMIC_GRAPH_AMQP_ADDR=127.0.0.1:5672 \
  epistemic-graph-server --persist-dir /var/lib/eg   # --features "amqp-wire server"
```

```python
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 5672))
ch = conn.channel()
ch.exchange_declare(exchange="events", exchange_type="topic")
ch.queue_declare(queue="tasks")
ch.queue_bind(queue="tasks", exchange="events", routing_key="task.*")
ch.confirm_delivery()                                   # publisher confirms (EG-284)
ch.basic_publish(exchange="events", routing_key="task.new", body="hello")
```

## MQTT 3.1.1 / 5.0 wire (EG-281, feature `mqtt-wire`)

Maps CONNECT/PUBLISH/SUBSCRIBE/PINGREQ/DISCONNECT onto the broker's topic exchange + bindings, QoS 0/1.

```bash
EPISTEMIC_GRAPH_MQTT_ADDR=127.0.0.1:1883 \
  epistemic-graph-server --persist-dir /var/lib/eg   # --features "mqtt-wire server"

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/#' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/room1' -m '21.5'
```

## STOMP 1.2 wire (EG-282, feature `stomp-wire`)

A text-frame listener mapping CONNECT/SEND/SUBSCRIBE/ACK/DISCONNECT onto the broker.

```bash
EPISTEMIC_GRAPH_STOMP_ADDR=127.0.0.1:61613 \
  epistemic-graph-server --persist-dir /var/lib/eg   # --features "stomp-wire server"
```

## Wire ↔ broker graph

Each wire selects the broker graph via its `*_GRAPH` env var (default `__commons__`):
`EPISTEMIC_GRAPH_AMQP_GRAPH`, `EPISTEMIC_GRAPH_MQTT_GRAPH`, `EPISTEMIC_GRAPH_STOMP_GRAPH`. Because all
three front the same broker, cross-protocol pub/sub works by topic. See
[connecting](connecting.md#rabbitmq--amqp-091-client-amqp-wire--broker) for the per-wire connect recipes.

---
*The broker is the Phase-Y modality over the KG-2.303 native engine task queue — messages are durable,
replicated engine state, not a bolt-on daemon.*

---

**See also:** [Capabilities matrix](../capabilities.md) · [Key-value & Blob](kv_blob.md) · [Observability](observability.md) · [Time-series](timeseries.md) · [Connecting (per-wire guide)](connecting.md).
