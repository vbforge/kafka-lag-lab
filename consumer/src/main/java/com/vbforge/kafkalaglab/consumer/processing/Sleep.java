package com.vbforge.kafkalaglab.consumer.processing;

final class Sleep {

    private Sleep() {}

    static void quietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}