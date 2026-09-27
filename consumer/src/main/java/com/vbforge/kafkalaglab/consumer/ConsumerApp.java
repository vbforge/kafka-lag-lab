package com.vbforge.kafkalaglab.consumer;

import com.vbforge.kafkalaglab.consumer.processing.MessageProcessor;
import com.vbforge.kafkalaglab.consumer.processing.MessageProcessorFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * kafka-lag-lab :: ConsumerApp
 *
 * JUNIOR NOTE: this class no longer contains any scenario-specific logic —
 * PROCESSING_MODE just selects which MessageProcessor implementation runs.
 * Adding a new scenario means adding a new class + one line in the
 * factory, never touching this file again.
 */
public class ConsumerApp {

    public static void main(String[] args) {
        String bootstrapServers = envOr("BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = envOr("TOPIC", "lag-lab-events");
        String groupId = envOr("GROUP_ID", "lag-lab-group");
        String processingMode = envOr("PROCESSING_MODE", "baseline");
        long slowDownstreamMs = Long.parseLong(envOr("SLOW_DOWNSTREAM_MS", "20"));
        int threadPoolSize = Integer.parseInt(envOr("THREAD_POOL_SIZE", "16"));
        double retryFailureRate = Double.parseDouble(envOr("RETRY_FAILURE_RATE", "0.3")); //to check with 0.6-0.7
        int retryMaxAttempts = Integer.parseInt(envOr("RETRY_MAX_ATTEMPTS", "3"));
        long retryBackoffMs = Long.parseLong(envOr("RETRY_BACKOFF_MS", "200"));
        int maxPollRecords = Integer.parseInt(envOr("MAX_POLL_RECORDS", "500"));
        int maxPollIntervalMs = Integer.parseInt(envOr("MAX_POLL_INTERVAL_MS", "300000"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        AtomicInteger rebalanceCount = new AtomicInteger(0);

        try (MessageProcessor processor = MessageProcessorFactory.from(
                processingMode, slowDownstreamMs, threadPoolSize,
                retryFailureRate, retryMaxAttempts, retryBackoffMs)) {

            System.out.printf(
                    "Consumer started -> topic=%s group=%s mode=%s (%s) maxPollRecords=%d maxPollIntervalMs=%d%n",
                    topic, groupId, processingMode, processor.describe(), maxPollRecords, maxPollIntervalMs);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of(topic), new LoggingRebalanceListener(rebalanceCount));

                long processedInWindow = 0;
                long windowStart = System.currentTimeMillis();

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                    if (!records.isEmpty()) {
                        processor.process(records);
                        processedInWindow += records.count();
                        consumer.commitSync();
                    }

                    long now = System.currentTimeMillis();
                    if (now - windowStart >= 1000) {
                        System.out.printf("processed=%d msgs in last window (rebalances so far: %d)%n",
                                processedInWindow, rebalanceCount.get());
                        processedInWindow = 0;
                        windowStart = now;
                    }
                }
            }
        }
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}