package com.kafka.streaming.streams;

import com.kafka.streaming.events.Transaction;
import com.kafka.streaming.serdes.TransactionSerde;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Log4j2
public class TransactionWindowStream {

    private static final String SOURCE_TOPIC = "transactions";
    private static final String COUNT_TOPIC = "user-txn-counts";
    private static final String FRAUD_TOPIC = "txn-fraud-alert";

    private static final long FRAUD_THRESHOLD = 3L;

    @Bean
    public KStream<String, Transaction> windowedTransactionSystem(
            StreamsBuilder builder) {

        KStream<String, Transaction> stream = builder.stream(
                SOURCE_TOPIC,
                Consumed.with(
                        Serdes.String(),
                        new TransactionSerde()
                )
        );

        KTable<Windowed<String>, Long> counts = stream
                .filter((key, transaction) ->
                        transaction != null
                                && transaction.userId() != null)
                .groupBy(
                        (key, transaction) -> transaction.userId(),
                        Grouped.with(
                                Serdes.String(),
                                new TransactionSerde()
                        )
                )
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(
                                Duration.ofSeconds(10)
                        )
                )
                .count(
                        Materialized.as(
                                "user-txn-count-window-store"
                        )
                );

        KStream<Windowed<String>, Long> countStream =
                counts.toStream();

        // Log each updated count.
        countStream.peek((windowedKey, count) ->
                log.info(
                        "User={} | Count={} | Window=[{} - {}]",
                        windowedKey.key(),
                        count,
                        windowedKey.window().startTime(),
                        windowedKey.window().endTime()
                )
        );

        // Publish windowed counts.
        countStream.to(
                COUNT_TOPIC,
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(
                                String.class, 3
                        ),
                        Serdes.Long()
                )
        );

        // Publish an alert only when the count first crosses 3.
        countStream
                .filter((windowedKey, count) ->
                        count == FRAUD_THRESHOLD + 1)
                .map((windowedKey, count) ->
                        KeyValue.pair(
                                windowedKey.key(),
                                "FRAUD ALERT: User "
                                        + windowedKey.key()
                                        + " reached "
                                        + count
                                        + " transactions in a 10-second window. "
                                        + "Window start: "
                                        + windowedKey.window().startTime()
                        )
                )
                .peek((userId, message) ->
                        log.warn("{}", message)
                )
                .to(
                        FRAUD_TOPIC,
                        Produced.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        return stream;
    }
}