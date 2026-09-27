package com.vbforge.kafkalaglab.producer;

import com.vbforge.kafkalaglab.producer.strategy.KeyStrategy;
import com.vbforge.kafkalaglab.producer.strategy.KeyStrategyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * kafka-lag-lab :: ProducerApp
 *
 * JUNIOR NOTE: this class no longer contains any scenario-specific logic.
 * KEY_STRATEGY just selects which KeyStrategy implementation runs — adding
 * a new scenario means adding a new class + one line in the factory, never
 * touching this file again.
 */
public class ProducerApp {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = envOr("BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = envOr("TOPIC", "lag-lab-events");
        int ratePerSecond = Integer.parseInt(envOr("RATE_MSGS_PER_SEC", "1000"));
        String keyStrategyName = envOr("KEY_STRATEGY", "uniform");
        int saltBuckets = Integer.parseInt(envOr("KEY_SALT_BUCKETS", "6"));

        KeyStrategy keyStrategy = KeyStrategyFactory.from(keyStrategyName, saltBuckets);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        System.out.printf("Starting producer -> topic=%s rate=%d/s keyStrategy=%s (%s) bootstrap=%s%n",
                topic, ratePerSecond, keyStrategyName, keyStrategy.describe(), bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long intervalNanos = 1_000_000_000L / ratePerSecond;
            long sent = 0;
            long windowStart = System.currentTimeMillis();

            while (true) {
                long loopStart = System.nanoTime();

                String key = keyStrategy.nextKey();
                String entityId = keyStrategy.entityId(key);
                String value = "{\"eventId\":\"" + UUID.randomUUID() + "\","
                        + "\"entityId\":\"" + entityId + "\","
                        + "\"ts\":" + System.currentTimeMillis() + "}";

                producer.send(new ProducerRecord<>(topic, key, value));
                sent++;

                long now = System.currentTimeMillis();
                if (now - windowStart >= 1000) {
                    System.out.printf("sent=%d msgs in last window%n", sent);
                    sent = 0;
                    windowStart = now;
                }

                long elapsedNanos = System.nanoTime() - loopStart;
                long sleepNanos = intervalNanos - elapsedNanos;
                if (sleepNanos > 0) {
                    Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                }
            }
        }
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}