package com.vbforge.kafkalaglab.producer.strategy;

/**
 * JUNIOR NOTE: nextKey() decides which partition a message lands on.
 * entityId() is separate on purpose — for salted strategies, the Kafka KEY
 * and the logical business entity are different things (that's the whole
 * trick behind the fix), so we don't want callers assuming key == entity.
 */
public interface KeyStrategy {

    String nextKey();

    /** The logical entity this generated key represents. Defaults to the key itself. */
    default String entityId(String generatedKey) {
        return generatedKey;
    }

    default String describe() {
        return getClass().getSimpleName();
    }
}