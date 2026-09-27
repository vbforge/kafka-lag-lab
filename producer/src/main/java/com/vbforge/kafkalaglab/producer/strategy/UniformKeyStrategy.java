package com.vbforge.kafkalaglab.producer.strategy;

import java.util.concurrent.ThreadLocalRandom;

/** Sprint 1 baseline: random key from a wide pool -> even partition spread. */
public class UniformKeyStrategy implements KeyStrategy {

    @Override
    public String nextKey() {

        return "key-" + ThreadLocalRandom.current().nextInt(0, 10_000);
    }

}