# Kafka Streaming

This application is a real-time fraud detection pipeline built with Spring Boot and Apache Kafka Streams. Payment transactions are published as events, examined continuously as they arrive, and suspicious ones are written out as fraud alerts. The service listens on port `8085`.

## What Kafka streaming is

Apache Kafka is a distributed log. Producers append events to named topics, and those events stay available for consumers to read in order, at their own pace. A topic is split into partitions so many producers and consumers can work in parallel. Each event has a key, a value, and a position in the log called an offset.

Kafka stores every value as bytes. The application works with a `Transaction` object, so a serializer and a deserializer sit on either side of the topic. Together they are a serde.

- The producer hands Kafka a `Transaction`. `TransactionSerializer` converts that object to JSON bytes before the broker stores them. The message key stays a plain string, the transaction id.
- The stream reads those bytes with `TransactionSerde`. `TransactionDeserializer` rebuilds the `Transaction` before any fraud rule runs. The filter then sees the amount, the user, and the timestamp as fields of that object.
- The same serde writes a matching transaction onto the alert topic, so a downstream consumer receives the same kind of object.

`TransactionSerde` is also the default value serde for the stream job. The default key serde remains a string serde. The stream still names both explicitly when it reads `transactions` and when it writes `fraud-alerts`, so this topology does not depend on a later change to the defaults.

Kafka Streams is the library that turns the log into a continuous processing application. It reads a topic as a stream of `Transaction` values, applies a filter to each one, and writes the matches to another topic. The application does not poll and batch on a schedule. It stays running and reacts as soon as a new event is committed.

A few ideas define how a stream works:

- **Event, not request.** A transaction is a fact that already happened. The stream observes it. It does not wait for a caller to ask for a result.
- **Ordered processing inside a partition.** Events that share the same key stay in the same partition and are handled in the order they were written. In this application the key is the transaction id.
- **Stateless steps.** A filter looks at one transaction and decides whether it continues. This pipeline only filters. It does not join topics or keep a running total.
- **Exactly the same topology on every instance.** The stream job is identified as `fraud-detection-streams`. Kafka assigns partitions across instances of that application id, so adding another instance splits the work instead of duplicating it.
- **Durable handoff.** The input topic and the output topic are both stored by the broker. If the stream application restarts, it resumes from the last committed offset and continues from the events it has not yet processed.

Spring Boot starts the Kafka Streams runtime when the application starts. The broker address is `localhost:9092`. Topics are created at startup with three partitions and a replication factor of one, which matches a single local broker.

## Purpose

The purpose of this pipeline is to separate ordinary payment traffic from transactions that look fraudulent, while the payments are still in flight.

A batch job would wait until a file or a database window closed, then scan what had already settled. By then the suspicious payment may already have been accepted. Streaming checks each transaction at the moment it is published, so an alert exists as soon as the rule matches.

The rule in this application is deliberately simple. A transaction is suspicious when its amount is greater than 10,000. Matching events are logged as fraud alerts and published to a dedicated topic so another system can notify, block, or review them.

The producer and the stream agree on one value type. The service publishes a `Transaction`, and the stream filters a `Transaction`. JSON exists only on the wire, inside the serde, so the fraud rule never handles a raw string.

## How the flow works

1. A client calls `GET /api/v1/transaction/process`.
2. The service creates 50 sample transactions. Each one has a transaction id, a user id (`USER_0` through `USER_49`), a random amount between 8,000 and 11,000, and the current time.
3. Each `Transaction` is published to the `transactions` topic. The message key is the transaction id. `TransactionSerializer` stores the value as JSON bytes.
4. The stream reads `transactions` continuously and rebuilds each value into a `Transaction`.
5. If the amount is above 10,000, the event is treated as fraud, a warning is logged, and the transaction is written to the `fraud-alerts` topic with the same serde.
6. Transactions at or below 10,000 stay on the input topic and are not copied to alerts.

Because amounts are chosen at random inside a range that straddles the threshold, a single call produces a mix of normal payments and fraud alerts. Downstream consumers can subscribe only to `fraud-alerts` and never see the ordinary traffic.

A payload that cannot be read back into a `Transaction` fails in the deserializer. It does not pass the fraud rule and it does not become an alert.

## Topics

| Topic | Role |
| --- | --- |
| `transactions` | Incoming payments. Key is a string. Value is a `Transaction` stored as JSON. Three partitions, one replica. |
| `fraud-alerts` | Payments whose amount is above 10,000, written with the same key and value types. Three partitions, one replica. |

## Transaction

Each event carries:

- **transactionId** — unique id, also used as the Kafka message key
- **userId** — the user the sample generator assigns
- **amount** — payment value compared with the 10,000 threshold
- **timestamp** — time the transaction was created

## Kafka Streams methods

These are the Kafka Streams DSL methods available with Kafka 4.2, which is the version this project uses. Each example is written against a `Transaction` stream. This application itself uses only four of them: `stream`, `filter`, `peek`, and `to`.

Older tutorials call three of these methods by earlier names. Kafka 4 uses `split` (previously `branch`), `repartition` (previously `through`), and `process` / `processValues` (previously `transform` / `transformValues`).

The snippets below assume a builder and the transaction stream this application already creates:

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, Transaction> transactions = builder.stream(
        "transactions",
        Consumed.with(Serdes.String(), new TransactionSerde()));
```

When a step changes the key or value type, pass `Grouped`, `Produced`, or `Materialized` with the new serdes. The default value serde in this application is `TransactionSerde`, so a step that still carries a `Transaction` can omit it.

### Reading topics

`stream` reads an event log. Every record is kept, including later records for a key that already appeared.

```java
KStream<String, Transaction> transactions = builder.stream(
        "transactions",
        Consumed.with(Serdes.String(), new TransactionSerde()));
```

`stream` also accepts several topic names, or a regular expression:

```java
KStream<String, Transaction> payments = builder.stream(
        List.of("card-transactions", "wallet-transactions"),
        Consumed.with(Serdes.String(), new TransactionSerde()));

KStream<String, String> allAlerts = builder.stream(Pattern.compile(".*-alerts"));
```

`table` reads a topic as a changelog. One key keeps only its latest value.

```java
KTable<String, String> userRegion = builder.table(
        "user-regions",
        Consumed.with(Serdes.String(), Serdes.String()));
```

`globalTable` copies the whole table onto every instance. A later join can look up any key, including keys that live on another partition.

```java
GlobalKTable<String, Double> userLimit = builder.globalTable(
        "user-limits",
        Consumed.with(Serdes.String(), Serdes.Double()));
```

### Stateless record methods

`filter` keeps records for which the predicate returns true. This is the fraud rule in this application.

```java
KStream<String, Transaction> fraud = transactions.filter(
        (key, tx) -> tx.amount() > 10000);
```

`filterNot` keeps records for which the predicate returns false.

```java
KStream<String, Transaction> normal = transactions.filterNot(
        (key, tx) -> tx.amount() > 10000);
```

`mapValues` replaces the value and leaves the key unchanged, so Kafka does not have to reshuffle the stream before a later count or join.

```java
KStream<String, Double> amounts = transactions.mapValues(Transaction::amount);

KStream<String, String> labeled = transactions.mapValues(
        (key, tx) -> key + " amount=" + tx.amount());
```

`map` replaces both the key and the value. A later `groupByKey` or join repartitions, because the key changed.

```java
KStream<String, Double> spendByUser = transactions.map(
        (key, tx) -> KeyValue.pair(tx.userId(), tx.amount()));
```

`flatMapValues` turns one record into zero or more records and keeps the original key.

```java
KStream<String, Transaction> largeOnly = transactions.flatMapValues(tx ->
        tx.amount() > 10000 ? List.of(tx) : List.of());
```

`flatMap` turns one record into zero or more records and may change the key.

```java
KStream<String, Double> userAndTotal = transactions.flatMap((key, tx) -> List.of(
        KeyValue.pair(tx.userId(), tx.amount()),
        KeyValue.pair("ALL", tx.amount())));
```

`selectKey` sets a new key and keeps the value. It is the usual step before grouping by a field that is not already the message key.

```java
KStream<String, Transaction> byUser = transactions.selectKey(
        (key, tx) -> tx.userId());
```

`peek` runs a side effect and passes the same record downstream. This application logs the fraud warning here.

```java
fraud.peek((key, tx) -> log.warn("FRAUD ALERT for {}", tx));
```

`foreach` runs a side effect and ends the branch. Nothing can be chained after it.

```java
fraud.foreach((key, tx) -> log.warn("FRAUD ALERT for {}", tx));
```

`print` writes each record to the console or a file. Use it while developing a topology.

```java
transactions.print(Printed.<String, Transaction>toSysOut().withLabel("transactions"));
```

`split` sends each record to the first matching branch.

```java
Map<String, KStream<String, Transaction>> branches = transactions
        .split(Named.as("amount-"))
        .branch((key, tx) -> tx.amount() > 10000, Branched.as("fraud"))
        .defaultBranch(Branched.as("normal"));

KStream<String, Transaction> fraudBranch = branches.get("amount-fraud");
KStream<String, Transaction> normalBranch = branches.get("amount-normal");
```

Applying two operators to the same stream sends every record down both paths:

```java
transactions.filter((key, tx) -> tx.amount() > 10000).to("fraud-alerts");
transactions.filter((key, tx) -> tx.amount() > 50000).to("high-value-alerts");
```

`merge` combines two streams of the same key and value type into one. Order is preserved inside each input. Records from the two inputs are not sorted against each other.

```java
KStream<String, Transaction> allPayments = cardPayments.merge(walletPayments);
```

`repartition` writes the stream to an internal topic and reads it back, with a chosen partition count. Use it when a new key must be distributed before a processor or a join.

```java
KStream<String, Transaction> spread = byUser.repartition(
        Repartitioned.<String, Transaction>numberOfPartitions(6)
                .withName("by-user")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TransactionSerde()));
```

### Writing results

`to` is a terminal write to a topic. This application writes fraud matches to `fraud-alerts`.

```java
fraud.to("fraud-alerts", Produced.with(Serdes.String(), new TransactionSerde()));
```

A topic-name extractor picks the destination from the record:

```java
transactions.to(
        (key, tx, context) -> tx.amount() > 10000 ? "fraud-alerts" : "cleared-transactions",
        Produced.with(Serdes.String(), new TransactionSerde()));
```

`toTable` turns the stream into a table of the latest value per key.

```java
KTable<String, Transaction> latest = transactions.toTable(
        Materialized.<String, Transaction, KeyValueStore<Bytes, byte[]>>as("latest-transactions")
                .withKeySerde(Serdes.String())
                .withValueSerde(new TransactionSerde()));
```

`toStream` turns a table back into a stream of updates. The optional mapper chooses the output key.

```java
KStream<String, Long> countStream = counts.toStream();

KStream<String, Long> byUserId = windowedCounts.toStream(
        (windowedUserId, count) -> windowedUserId.key());
```

### Grouping and aggregation

`groupByKey` groups by the current key. It repartitions only when an earlier `map`, `flatMap`, or `selectKey` changed that key.

```java
KGroupedStream<String, Transaction> grouped = byUser.groupByKey(
        Grouped.with(Serdes.String(), new TransactionSerde()));
```

`groupBy` chooses a new key, then groups. It always repartitions.

```java
KGroupedStream<String, Transaction> byUserId = transactions.groupBy(
        (key, tx) -> tx.userId(),
        Grouped.with(Serdes.String(), new TransactionSerde()));
```

`count` returns how many records have been seen for each key.

```java
KTable<String, Long> paymentsPerUser = byUserId.count(
        Materialized.with(Serdes.String(), Serdes.Long()));
```

`reduce` combines values of the same type into one running value.

```java
KTable<String, Double> spendPerUser = transactions
        .selectKey((key, tx) -> tx.userId())
        .mapValues(Transaction::amount)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::sum);
```

`aggregate` does the same job when the result type differs from the input. The initializer creates the first value for a key. The adder folds each new record in.

```java
KTable<String, Double> spend = byUserId.aggregate(
        () -> 0.0,
        (userId, tx, total) -> total + tx.amount(),
        Materialized.with(Serdes.String(), Serdes.Double()));
```

A grouped table needs a subtractor as well, because an update must remove the previous contribution:

```java
KGroupedTable<String, Double> spendByBand = spend.groupBy(
        (userId, total) -> KeyValue.pair(total > 10000 ? "high" : "normal", total),
        Grouped.with(Serdes.String(), Serdes.Double()));

KTable<String, Double> bandTotals = spendByBand.aggregate(
        () -> 0.0,
        (band, newTotal, sum) -> sum + newTotal,
        (band, oldTotal, sum) -> sum - oldTotal,
        Materialized.with(Serdes.String(), Serdes.Double()));
```

`cogroup` aggregates several grouped streams in one step. Each input supplies its own adder.

```java
KGroupedStream<String, Double> refundsByUser = refunds
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

KTable<String, Double> netSpend = byUserId
        .cogroup((userId, tx, total) -> total + tx.amount())
        .cogroup(refundsByUser, (userId, refund, total) -> total - refund)
        .aggregate(() -> 0.0, Materialized.with(Serdes.String(), Serdes.Double()));
```

On a `KTable`, `groupBy` rebuilds both the key and the value before a table aggregation:

```java
KGroupedTable<String, Integer> nameLengthByRegion = userRegion.groupBy(
        (userId, region) -> KeyValue.pair(region, userId.length()),
        Grouped.with(Serdes.String(), Serdes.Integer()));
```

### Windowing

`windowedBy` buckets the grouped records before `count`, `reduce`, or `aggregate`. The result key is `Windowed<K>`, which carries the original key plus the window start and end.

Tumbling windows are fixed and do not overlap. This counts payments per user in successive 5-minute blocks:

```java
KTable<Windowed<String>, Long> tumbling = byUserId
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count();
```

Hopping windows overlap. A 5-minute window that advances every minute produces a new result each minute:

```java
KTable<Windowed<String>, Long> hopping = byUserId
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))
                .advanceBy(Duration.ofMinutes(1)))
        .count();
```

A grace period keeps the window open for late events. `ofSizeAndGrace` sets the size and how long after the window end a late record is still accepted.

```java
KTable<Windowed<String>, Double> withGrace = byUserId
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
        .aggregate(
                () -> 0.0,
                (userId, tx, total) -> total + tx.amount(),
                Materialized.with(Serdes.String(), Serdes.Double()));
```

Sliding windows update a result whenever two events for the same key fall inside the time difference.

```java
KTable<Windowed<String>, Long> sliding = byUserId
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
        .count();
```

Session windows group events that arrive close together. A new session starts after the inactivity gap.

```java
KTable<Windowed<String>, Double> sessions = byUserId
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
        .aggregate(
                () -> 0.0,
                (userId, tx, total) -> total + tx.amount(),
                (userId, left, right) -> left + right,
                Materialized.with(Serdes.String(), Serdes.Double()));
```

`emitStrategy` chooses when a windowed aggregation is forwarded. `onWindowUpdate` emits on each input. `onWindowClose` emits once, after the window and its grace period have closed.

```java
KTable<Windowed<String>, Long> finalCounts = byUserId
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)))
        .emitStrategy(EmitStrategy.onWindowClose())
        .count();
```

`suppress` holds intermediate table updates and forwards them on a schedule. `untilTimeLimit` flushes on a time limit. `untilWindowCloses` waits until a window is closed.

```java
KTable<String, Long> quietCounts = paymentsPerUser.suppress(
        Suppressed.untilTimeLimit(Duration.ofSeconds(30), Suppressed.BufferConfig.unbounded()));

KTable<Windowed<String>, Long> closedWindows = finalCounts.suppress(
        Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
```

### Joins

A stream-stream join matches records that share a key and fall inside a time window. Both sides must use the same key and the same number of partitions.

`join` emits only when both sides have a match.

```java
KStream<String, String> withChargeback = byUser.join(
        chargebacksByUser,
        (tx, reason) -> tx.transactionId() + ":" + reason,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        StreamJoined.with(Serdes.String(), new TransactionSerde(), Serdes.String()));
```

`leftJoin` still emits the left record when the right side has no match. The right value is then null.

```java
KStream<String, String> maybeChargeback = byUser.leftJoin(
        chargebacksByUser,
        (tx, reason) -> reason == null ? tx.transactionId() : tx.transactionId() + ":" + reason,
        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)),
        StreamJoined.with(Serdes.String(), new TransactionSerde(), Serdes.String()));
```

`outerJoin` emits a record when either side has an event in the window. The missing side is null.

```java
KStream<String, String> eitherSide = byUser.outerJoin(
        chargebacksByUser,
        (tx, reason) -> (tx == null ? "none" : tx.transactionId()) + ":" + reason,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
        StreamJoined.with(Serdes.String(), new TransactionSerde(), Serdes.String()));
```

A stream-table join looks up the latest table value for the record key. It is not windowed.

```java
KStream<String, String> withRegion = byUser.join(
        userRegion,
        (tx, region) -> tx.transactionId() + " in " + region,
        Joined.with(Serdes.String(), new TransactionSerde(), Serdes.String()));

KStream<String, String> regionOrUnknown = byUser.leftJoin(
        userRegion,
        (tx, region) -> region == null ? "unknown" : region);
```

A stream-globalTable join can use a key taken from the value, because every instance holds the full table.

```java
KStream<String, Transaction> checked = transactions.leftJoin(
        userLimit,
        (key, tx) -> tx.userId(),
        (tx, limit) -> tx);
```

A table-table join combines the latest values of two tables on the same key.

```java
KTable<String, String> profile = userRegion.join(userName, (region, name) -> name + "@" + region);
KTable<String, String> profileLeft = userRegion.leftJoin(userName, (region, name) -> name + "@" + region);
KTable<String, String> profileOuter = userRegion.outerJoin(userName, (region, name) -> name + "@" + region);
```

A foreign-key join uses a field inside the left value as the lookup key. `outerJoin` is not available for this form.

```java
KTable<String, String> txUser = latest.join(
        userName,
        Transaction::userId,
        (tx, name) -> tx.transactionId() + ":" + name);

KTable<String, String> txUserLeft = latest.leftJoin(
        userName,
        Transaction::userId,
        (tx, name) -> name == null ? tx.transactionId() : tx.transactionId() + ":" + name);
```

### KTable record methods

`filter` and `filterNot` on a table keep or drop keys. A dropped key is forwarded as a delete so downstream tables remove it too.

```java
KTable<String, Transaction> largeLatest = latest.filter((key, tx) -> tx.amount() > 10000);
KTable<String, Transaction> smallLatest = latest.filterNot((key, tx) -> tx.amount() > 10000);
```

`mapValues` on a table changes the value for the same key.

```java
KTable<String, Double> latestAmount = latest.mapValues(Transaction::amount);
```

### Custom processing

`process` runs a Processor API instance for each record and may emit zero or more records, with a new key or value. The supplier must return a new processor each time it is called.

```java
transactions.process(() -> new ContextualProcessor<String, Transaction, String, String>() {
    @Override
    public void process(Record<String, Transaction> record) {
        if (record.value().amount() > 10000) {
            context().forward(record.withValue("fraud:" + record.key()));
        }
    }
});
```

`processValues` is the same idea with the key fixed, so the step does not force a repartition.

```java
transactions.processValues(() -> new ContextualFixedKeyProcessor<String, Transaction, Double>() {
    @Override
    public void process(FixedKeyRecord<String, Transaction> record) {
        context().forward(record.withValue(record.value().amount()));
    }
});
```

Pass store names when the processor reads or writes state that was added to the builder:

```java
builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("seen-users"),
        Serdes.String(),
        Serdes.Long()));

transactions.process(() -> new ContextualProcessor<String, Transaction, String, Transaction>() {
    @Override
    public void process(Record<String, Transaction> record) {
        context().forward(record);
    }
}, "seen-users");
```

### What this application calls

| Method | Where it is used |
| --- | --- |
| `stream` | Read `transactions` with a string key and `TransactionSerde`. |
| `filter` | Keep a transaction when `amount` is greater than 10,000. |
| `peek` | Log the fraud warning. |
| `to` | Write the match to `fraud-alerts` with the same serde. |
