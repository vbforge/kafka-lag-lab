package com.vbforge.kafkalaglab.consumer.processing;

public final class MessageProcessorFactory {

    private MessageProcessorFactory() {}

    public static MessageProcessor from(String mode, long slowDownstreamMs, int threadPoolSize,
                                         double retryFailureRate, int retryMaxAttempts, long retryBackoffMs) {
        return switch (mode.toLowerCase()) {
            case "slow-sync" -> new SlowSyncProcessor(slowDownstreamMs);
            case "async-batch" -> new AsyncBatchProcessor(slowDownstreamMs, threadPoolSize);
            case "retry-storm" -> new RetryStormProcessor(retryFailureRate);
            case "retry-bounded" -> new RetryBoundedProcessor(retryFailureRate, retryMaxAttempts, retryBackoffMs);
            default -> new BaselineProcessor();
        };
    }

}