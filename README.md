# Kafka Streaming

This application is a real-time fraud detection pipeline built with Spring Boot and Apache Kafka Streams. Payment transactions are published as events, examined continuously as they arrive, and suspicious ones are written out as fraud alerts. The service listens on port `8085`.

## What Kafka streaming is

Apache Kafka is a distributed log. Producers append events to named topics, and those events stay available for consumers to read in order, at their own pace. A topic is split into partitions so many producers and consumers can work in parallel. Each event has a key, a value, and a position in the log called an offset.

Kafka Streams is the library that turns that log into a continuous processing application. It reads a topic as a stream, applies filters and transformations to each event, and writes the result to another topic. The application does not poll and batch on a schedule. It stays running and reacts as soon as a new event is committed.

A few ideas define how a stream works:

- **Event, not request.** A transaction is a fact that already happened. The stream observes it. It does not wait for a caller to ask for a result.
- **Ordered processing inside a partition.** Events that share the same key stay in the same partition and are handled in the order they were written. In this application the key is the transaction id.
- **Stateless steps.** A filter looks at one event and decides whether it continues. This pipeline only filters. It does not join topics or keep a running total.
- **Exactly the same topology on every instance.** The stream job is identified as `fraud-detection-streams`. Kafka assigns partitions across instances of that application id, so adding another instance splits the work instead of duplicating it.
- **Durable handoff.** The input topic and the output topic are both stored by the broker. If the stream application restarts, it resumes from the last committed offset and continues from the events it has not yet processed.

Spring Boot starts the Kafka Streams runtime with `@EnableKafkaStreams`. The broker address is `localhost:9092`. Topics are created at startup with three partitions and a replication factor of one, which matches a single local broker.

## Purpose

The purpose of this pipeline is to separate ordinary payment traffic from transactions that look fraudulent, while the payments are still in flight.

A batch job would wait until a file or a database window closed, then scan what had already settled. By then the suspicious payment may already have been accepted. Streaming checks each transaction at the moment it is published, so an alert exists as soon as the rule matches.

The rule in this application is deliberately simple. A transaction is suspicious when its amount is greater than 10,000. Matching events are logged as fraud alerts and published to a dedicated topic so another system can notify, block, or review them. Events that fail to parse as a transaction are ignored and do not become alerts.

## How the flow works

1. A client calls `GET /api/v1/transaction/process`.
2. The service creates 50 sample transactions. Each one has a transaction id, a user id (`USER_0` through `USER_49`), a random amount between 8,000 and 11,000, and the current time.
3. Every transaction is published as JSON to the `transactions` topic. The message key is the transaction id.
4. The stream reads `transactions` continuously.
5. Each message is parsed. If the amount is above 10,000, the event is treated as fraud, a warning is logged, and the same message is written to the `fraud-alerts` topic.
6. Transactions at or below 10,000 stay on the input topic and are not copied to alerts.

Because amounts are chosen at random inside a range that straddles the threshold, a single call produces a mix of normal payments and fraud alerts. Downstream consumers can subscribe only to `fraud-alerts` and never see the ordinary traffic.

## Topics

| Topic | Role |
| --- | --- |
| `transactions` | Incoming payments. Three partitions, one replica. |
| `fraud-alerts` | Payments whose amount is above 10,000. Three partitions, one replica. |

## Transaction

Each event carries:

- **transactionId** — unique id, also used as the Kafka message key
- **userId** — the user the sample generator assigns
- **amount** — payment value compared with the 10,000 threshold
- **timestamp** — time the transaction was created
