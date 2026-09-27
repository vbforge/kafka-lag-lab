package com.vbforge.kafkalaglab.producer.strategy;

/** Sprint 2 PROBLEM: always the same key -> pinned to one partition. */
public class HotKeyStrategy implements KeyStrategy {

    public static final String HOT_ENTITY_ID = "hot-key-1";

    @Override
    public String nextKey() {
        return HOT_ENTITY_ID;
    }

    @Override
    public String entityId(String generatedKey) {
        return HOT_ENTITY_ID;
    }

}