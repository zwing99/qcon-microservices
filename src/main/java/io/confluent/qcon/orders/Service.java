package io.confluent.qcon.orders;

public interface Service {
    void start(String bootstrapServers, String stateDir);

    void stop();

}
