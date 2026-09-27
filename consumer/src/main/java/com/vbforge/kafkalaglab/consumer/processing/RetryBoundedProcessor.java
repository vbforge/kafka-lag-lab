package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.ThreadLocalRandom;

/** Sprint 4 FIX: capped attempts + exponential backoff, then simulated DLQ. */
public class RetryBoundedProcessor implements MessageProcessor {

    private final double failureRate;
    private final int maxAttempts;
    private final long baseBackoffMs;

    public RetryBoundedProcessor(double failureRate, int maxAttempts, long baseBackoffMs) {
        this.failureRate = failureRate;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            int attempt = 0;
            boolean success = false;
            while (!success && attempt < maxAttempts) {
                success = ThreadLocalRandom.current().nextDouble() >= failureRate;
                if (!success) {
                    attempt++;
                    Sleep.quietly(baseBackoffMs * (1L << (attempt - 1)));
                }
            }
            if (!success) {
                System.out.printf("DLQ: giving up after %d attempts (partition=%d offset=%d)%n",
                        maxAttempts, record.partition(), record.offset());
            }
        }
    }
}