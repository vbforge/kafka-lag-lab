package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/** Sprint 3 PROBLEM: one message at a time, fully sequential. */

public class SlowSyncProcessor implements MessageProcessor {

    private final long slowDownstreamMs;

    public SlowSyncProcessor(long slowDownstreamMs) {
        this.slowDownstreamMs = slowDownstreamMs;
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            Sleep.quietly(slowDownstreamMs);
        }
    }
}