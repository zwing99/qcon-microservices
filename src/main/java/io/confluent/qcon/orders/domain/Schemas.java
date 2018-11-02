package io.confluent.qcon.orders.domain;

import io.confluent.qcon.orders.serde.JsonDeserializer;
import io.confluent.qcon.orders.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class that represents Topics and their various Serializers/Deserializers in a
 * convenient form.
 */
public class Schemas {

    public static class Topic<K, V> {

        private String name;
        private Serde<K> keySerde;
        private Serde<V> valueSerde;

        Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            Topics.ALL.put(name, this);
        }

        public Serde<K> keySerde() {
            return keySerde;
        }

        public Serde<V> valueSerde() {
            return valueSerde;
        }

        public String name() {
            return name;
        }

        public String toString() {
            return name;
        }
    }

    public static class Topics {

        public static final Map<String, Topic> ALL = new HashMap<>();

        static public final class OrderSerde extends Serdes.WrapperSerde<Order> {
            public OrderSerde() {
                super(new JsonSerializer<Order>(), new JsonDeserializer<Order>(Order.class));
            }
        }

        public static Topic<String, Order> ORDERS =
                new Topic<String, Order>("orders", Serdes.String(), new OrderSerde());

    }
}
