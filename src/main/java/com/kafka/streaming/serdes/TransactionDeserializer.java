package com.kafka.streaming.serdes;

import com.kafka.streaming.events.Transaction;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, Transaction.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Transaction", e);
        }
    }
}
