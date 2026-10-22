package com.kafka.streaming.service;

import com.kafka.streaming.events.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class TransactionService {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final Clock clock;

    public TransactionService(
            KafkaTemplate<String, Transaction> kafkaTemplate,
            Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
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

            kafkaTemplate.send("transactions", transactionId, txn);
        }

        return "Transaction sent to Kafka!";
    }
}
