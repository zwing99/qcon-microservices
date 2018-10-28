package io.confluent.qcon.orders.utils;

import io.confluent.qcon.orders.Service;

public class MicroserviceShutdown {

    public static void addShutdownHookAndBlock(Service service) throws InterruptedException {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> service.stop());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
        Thread.currentThread().join();
    }
}
