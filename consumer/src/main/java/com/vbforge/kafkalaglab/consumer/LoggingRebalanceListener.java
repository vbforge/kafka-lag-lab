package com.vbforge.kafkalaglab.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JUNIOR NOTE: fires whenever the group coordinator moves partitions
 * around — including when this consumer gets kicked out for exceeding
 * max.poll.interval.ms and has to rejoin. Lives alongside ConsumerApp
 * rather than under processing/ since it's orchestration, not a scenario.
 */
public class LoggingRebalanceListener implements ConsumerRebalanceListener {

    private final AtomicInteger rebalanceCount;

    public LoggingRebalanceListener(AtomicInteger rebalanceCount) {
        this.rebalanceCount = rebalanceCount;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        rebalanceCount.incrementAndGet();
        System.out.printf("[REBALANCE] partitions revoked: %s (total: %d)%n", partitions, rebalanceCount.get());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("[REBALANCE] partitions assigned: %s%n", partitions);
    }
}