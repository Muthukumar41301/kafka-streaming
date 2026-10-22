package com.kafka.streaming.service;

import com.kafka.streaming.events.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class TransactionService {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;

    public TransactionService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public String processTransactions() {

        for (int i = 0; i < 50; i++) {

            String transactionId =
                    "txn-" + System.currentTimeMillis() + "-" + i;

            double amount = ThreadLocalRandom.current()
                    .nextDouble(8000, 11000);

            Transaction txn = new Transaction(
                    transactionId,
                    "USER_" + i,
                    amount,
                    LocalDateTime.now(clock)
            );

            String txnJson = objectMapper.writeValueAsString(txn);

            kafkaTemplate.send("transactions", transactionId, txnJson);
        }

        return "Transaction sent to Kafka!";
    }
}
