package io.confluent.qcon.orders.utils;

import io.confluent.qcon.orders.domain.Order;
import io.confluent.qcon.orders.domain.OrderState;
import io.confluent.qcon.orders.domain.Product;
import io.confluent.qcon.orders.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static io.confluent.qcon.orders.domain.OrderState.CREATED;

public class ProduceOrders {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException, ExecutionException {

        final JsonSerializer<Order> mySerializer = new JsonSerializer<>();

        final Properties props = LoadConfigs.loadConfig(LoadConfigs.parseArgsAndConfigure(new String[] {}));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        try (final KafkaProducer<String, Order> producer = new KafkaProducer<>(props, new StringSerializer(), mySerializer)) {
            int orderId = 0;
            while (true) {
                Order order = maybeInvalidateOrder(new Order(Integer.toString(orderId), "15", OrderState.CREATED, Product.AMPS,
                        3, 5.00d));
                ProducerRecord<String, Order> record = new ProducerRecord<>("orders", order.getId(), order);
                producer.send(record).get();
                orderId = orderId + 1;
                Thread.sleep(1000L);
            }
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    static Order maybeInvalidateOrder(Order order) {
        if (Integer.valueOf(order.getId()) % 10 == 0) {
            switch (ThreadLocalRandom.current().nextInt(0, 3)) {
                case 0:
                    order.setPrice(-5.00);
                    return order;
                case 1:
                    order.setQuantity(-3);
                    return order;
                case 2:
                    order.setProduct(null);
                    return order;
            }
        }
        return order;
    }

}