package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.ThreadLocalRandom;

/** Sprint 4 PROBLEM: retries forever, fixed short delay, no cap. */
public class RetryStormProcessor implements MessageProcessor {

    private final double failureRate;

    public RetryStormProcessor(double failureRate) {
        this.failureRate = failureRate;
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            boolean success = false;
            while (!success) {
                success = ThreadLocalRandom.current().nextDouble() >= failureRate;
                if (!success) {
                    Sleep.quietly(50);
                }
            }
        }
    }
}