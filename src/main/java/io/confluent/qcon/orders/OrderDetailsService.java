package io.confluent.qcon.orders;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.confluent.qcon.orders.domain.Order;
import io.confluent.qcon.orders.domain.OrderState;
import io.confluent.qcon.orders.domain.OrderValidationResult;
import io.confluent.qcon.orders.domain.Schemas;
import io.confluent.qcon.orders.utils.LoadConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.qcon.orders.utils.MicroserviceShutdown.addShutdownHookAndBlock;
import static java.util.Collections.singletonList;

/**
 * Validates the details of each order.
 * - Is the quantity positive?
 * - Is there a customerId
 * - etc...
 * <p>
 * This service could be built with Kafka Streams but we've used a Producer/Consumer pair to demonstrate
 * this other style of building event driven services.
 */

public class OrderDetailsService implements Service {

    private static final Logger log = LoggerFactory.getLogger(OrderDetailsService.class);
    private final String CONSUMER_GROUP_ID = getClass().getSimpleName();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private KafkaConsumer<String, Order> consumer;
    private KafkaProducer<String, Order> producer;
    private boolean running;

    public void start(String configFile, String stateDir) {
        executorService.execute(() -> {
            try {
                startService(configFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        running = true;
        log.info("Started Service " + getClass().getSimpleName());
    }

    private void startService(String configFile) throws IOException {
        startConsumer(configFile);
        startProducer(configFile);

        try {

            consumer.subscribe(singletonList(Schemas.Topics.ORDERS.name()));

            while (running) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Order> record : records) {
                    Order order = record.value();
                    if (order.getState() == OrderState.CREATED) {
                        // TODO: Validate the order (using validate())
                        // TODO: create a ProducerRecord from the order and result (see record())
                        // TODO: then produce the result to Kafka using the existing producer
                    }
                }
            }
        } finally {
            close();
        }

    }

    private void close() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    private OrderValidationResult validate(Order order) {
        if ((order.getCustomerId() == null) ||
                (order.getQuantity() < 0) ||
                (order.getPrice() < 0) ||
                (order.getProduct() == null)) {
            return OrderValidationResult.FAIL;
        }

        return  OrderValidationResult.PASS;
    }

    private ProducerRecord<String, Order> record(Order order,
                                                           OrderValidationResult result) {

        if (result.equals(OrderValidationResult.PASS))
            order.setState(OrderState.VALIDATED);
        else
            order.setState(OrderState.FAILED);

        return new ProducerRecord<String, Order>(
                Schemas.Topics.ORDERS.name(),
                order.getId(),
                order);
    }

    private void startProducer(String configFile) throws IOException {
        Properties producerConfig = LoadConfigs.loadConfig(configFile);
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-details-service-producer");

        producer = new KafkaProducer<>(producerConfig,
                Schemas.Topics.ORDERS.keySerde().serializer(),
                Schemas.Topics.ORDERS.valueSerde().serializer());
    }

    private void startConsumer(String configFile) throws IOException {
        Properties consumerConfig = LoadConfigs.loadConfig(configFile);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "order-details-service-consumer");

        consumer = new KafkaConsumer<>(consumerConfig,
                Schemas.Topics.ORDERS.keySerde().deserializer(),
                Schemas.Topics.ORDERS.valueSerde().deserializer());
    }

    public void stop() {

        running = false;
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("Failed to stop " + getClass().getSimpleName() + " in 1000ms");
        }
        log.info(getClass().getSimpleName() + " was stopped");

    }

    public static void main(String[] args) throws Exception {
        OrderDetailsService service = new OrderDetailsService();
        service.start(LoadConfigs.parseArgsAndConfigure(args), "/tmp/kafka-streams");
        addShutdownHookAndBlock(service);
    }
}