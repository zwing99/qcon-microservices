package io.confluent.qcon.orders.utils;

import io.confluent.qcon.orders.domain.Order;
import io.confluent.qcon.orders.domain.OrderValidation;
import io.confluent.qcon.orders.domain.Schemas;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumeValidatedOrders {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {
        final Properties props = LoadConfigs.loadConfig(LoadConfigs.parseArgsAndConfigure(new String[] {}));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-orders");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final KafkaConsumer<String, OrderValidation> consumer = new KafkaConsumer<>(props,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().deserializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().deserializer() )) {
            consumer.subscribe(Collections.singletonList(Schemas.Topics.ORDER_VALIDATIONS.name()));

            while (true) {
                // poll returns right away when there is data available.
                // the timeout is basically configuring "long poll" behavior, how long to keep checking when there
                // is no data available, so it's better to poll for a longer period of time
                final ConsumerRecords<String, OrderValidation> records = consumer.poll(Duration.ofSeconds(30));
                for (final ConsumerRecord<String, OrderValidation> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
