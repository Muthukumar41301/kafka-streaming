package com.kafka.streaming.events;

import java.time.LocalDateTime;

public record Transaction(String transactionId, String userId, double amount, LocalDateTime timestamp) {
}
