package com.vbforge.kafkalaglab.producer.strategy;

/**
 * This is the "registry" piece — the only place that maps an env var string to a class:
 * */

public final class KeyStrategyFactory {

    private KeyStrategyFactory() {}

    public static KeyStrategy from(String strategyName, int saltBuckets) {
        return switch (strategyName.toLowerCase()) {
            case "hotkey" -> new HotKeyStrategy();
            case "hotkey-salted" -> new SaltedHotKeyStrategy(saltBuckets);
            default -> new UniformKeyStrategy();
        };
    }

}