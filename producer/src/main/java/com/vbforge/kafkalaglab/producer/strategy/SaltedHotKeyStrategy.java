package com.vbforge.kafkalaglab.producer.strategy;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Sprint 2 FIX: same logical entity as HotKeyStrategy, but salted across
 * N buckets so it spreads across partitions instead of pinning to one.
 * entityId() strips the salt back off so downstream consumers can still
 * regroup by the real entity if they need to.
 */
public class SaltedHotKeyStrategy implements KeyStrategy {

    private final int saltBuckets;

    public SaltedHotKeyStrategy(int saltBuckets) {
        this.saltBuckets = saltBuckets;
    }

    @Override
    public String nextKey() {
        int salt = ThreadLocalRandom.current().nextInt(0, saltBuckets);
        return HotKeyStrategy.HOT_ENTITY_ID + "-salt-" + salt;
    }

    @Override
    public String entityId(String generatedKey) {
        return HotKeyStrategy.HOT_ENTITY_ID;
    }

}