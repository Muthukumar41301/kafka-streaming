package com.kafka.streaming.service;

import com.kafka.streaming.events.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class TransactionService {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final Clock clock;
    private final ObjectMapper mapper;

    public TransactionService(
            KafkaTemplate<String, Transaction> kafkaTemplate,
            Clock clock, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.mapper = mapper;
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

    public String publishTransaction() {
        List<Transaction> transactions = readTransactionsFromResource();

        for (Transaction txn : transactions) {
            kafkaTemplate.send("transactions", txn.transactionId(), txn);
        }
        return "✅ Published " + transactions.size() + " transactions to Kafka!";
    }

    private List<Transaction> readTransactionsFromResource() {
        try (InputStream is = getClass().getResourceAsStream("/transaction.json")) {
            return mapper.readValue(is, new TypeReference<List<Transaction>>() {
            });
        } catch (Exception e) {
            // wrap any parsing errors and rethrow as a runtime exception
            throw new RuntimeException("Failed to parse transactions.json", e);
        }
    }
}
