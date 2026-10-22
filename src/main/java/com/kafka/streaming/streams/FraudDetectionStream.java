package com.kafka.streaming.streams;

import com.kafka.streaming.events.Transaction;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
@Log4j2
public class FraudDetectionStream {

    @Bean
    KStream<String, String> fraudDetectionStreams(StreamsBuilder streamsBuilder) {

        // Step 1: Read messages from the input topic.
        KStream<String, String> transactionsStream = streamsBuilder.stream("transactions");

        // Step 2: Process the stream to detect fraudulent transactions.
        KStream<String, String> fraudTransactionStream = transactionsStream.filter(((key, value) -> isSuspicious(value)))
                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT - transactionId={} , value={}", key, value));

        // Step 3: write detected fraudulent transactions to an output topic.
        fraudTransactionStream.to("fraud-alerts");

        return transactionsStream;
    }

    private boolean isSuspicious(String value) {
        try {
            Transaction transaction = new ObjectMapper()
                    .readValue(value, Transaction.class); // validate JSON
            return transaction.amount() > 10000; // simple fraud rule
        } catch (Exception _) {
            return false;
        }
    }
}
