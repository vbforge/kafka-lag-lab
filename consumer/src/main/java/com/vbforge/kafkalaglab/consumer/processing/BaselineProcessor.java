package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/** Sprint 1 baseline: no-op, just counts. Establishes the theoretical ceiling. */

public class BaselineProcessor implements MessageProcessor {

    @Override
    public void process(ConsumerRecords<String, String> records) {
        // intentionally empty
    }

}