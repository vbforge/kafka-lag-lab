package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Sprint 3 FIX: same slow call, fanned out across a bounded pool so several
 * overlap instead of queueing. Owns its executor -> must be closed.
 */
public class AsyncBatchProcessor implements MessageProcessor {

    private final long slowDownstreamMs;
    private final ExecutorService executor;

    public AsyncBatchProcessor(long slowDownstreamMs, int threadPoolSize) {
        this.slowDownstreamMs = slowDownstreamMs;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        List<Future<?>> futures = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            futures.add(executor.submit(() -> Sleep.quietly(slowDownstreamMs)));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("processing failed", e);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}