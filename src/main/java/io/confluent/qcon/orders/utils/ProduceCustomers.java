package io.confluent.qcon.orders.utils;

import io.confluent.qcon.orders.domain.Customer;
import io.confluent.qcon.orders.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProduceCustomers {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException, ExecutionException {

        final JsonSerializer<Customer> mySerializer = new JsonSerializer<>();

        final Properties props = LoadConfigs.loadConfig(LoadConfigs.parseArgsAndConfigure(new String[] {}));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        try (final KafkaProducer<String, Customer> producer = new KafkaProducer<>(props, new StringSerializer(), mySerializer)) {
            // generating just one customer for now...
            final String customerId = "15";
            final Customer customer = new Customer(customerId, "Moshe", "Rabinobvich", "moshe@bigcompany.foo", "5467 Blurb St. San Francisco, CA");
            final ProducerRecord<String, Customer> record = new ProducerRecord<>("customers", customer.getCustomerId(), customer);
            producer.send(record).get();
            Thread.sleep(1000L);

        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

}