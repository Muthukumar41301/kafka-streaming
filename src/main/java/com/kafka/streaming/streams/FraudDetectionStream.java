package com.kafka.streaming.streams;

import com.kafka.streaming.events.Transaction;
import com.kafka.streaming.serdes.TransactionSerde;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@Log4j2
public class FraudDetectionStream {

    @Bean
    KStream<String, Transaction> fraudDetectionStreams(StreamsBuilder streamsBuilder) {

        KStream<String, Transaction> transactionsStream = streamsBuilder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        transactionsStream
                .filter((key, tx) -> tx.amount() > 10000)
                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx))
                .to("fraud-alerts", Produced.with(Serdes.String(), new TransactionSerde()));

        return transactionsStream;
    }
}
