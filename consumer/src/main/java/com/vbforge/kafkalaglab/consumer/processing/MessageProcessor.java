package com.vbforge.kafkalaglab.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * JUNIOR NOTE: extends AutoCloseable because AsyncBatchProcessor owns a
 * thread pool that needs shutting down — every processor gets a close()
 * for free via the default, so ConsumerApp can treat all of them uniformly
 * without knowing which ones actually hold resources.
 */
public interface MessageProcessor extends AutoCloseable {

    void process(ConsumerRecords<String, String> records);

    @Override
    default void close() {
        // no-op by default; only processors holding resources override this
    }

    default String describe() {
        return getClass().getSimpleName();
    }
}