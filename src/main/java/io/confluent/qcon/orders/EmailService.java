package io.confluent.qcon.orders;


import io.confluent.qcon.orders.domain.Customer;
import io.confluent.qcon.orders.domain.Order;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.confluent.qcon.orders.domain.Schemas.Topics.CUSTOMERS;
import static io.confluent.qcon.orders.domain.Schemas.Topics.ORDERS;
import static io.confluent.qcon.orders.utils.LoadConfigs.configStreams;
import static io.confluent.qcon.orders.utils.LoadConfigs.parseArgsAndConfigure;
import static io.confluent.qcon.orders.utils.MicroserviceShutdown.addShutdownHookAndBlock;
import static sun.nio.cs.Surrogate.MIN;

/**
 * A very simple service which sends emails. Order is joined to a lookup table of Customers.
 * An email is sent for each resulting tuple.
 */
public class EmailService implements Service {

    private static final Logger log = LoggerFactory.getLogger(EmailService.class);
    private final String SERVICE_APP_ID = "email-service-1";

    private KafkaStreams streams;
    private Emailer emailer;

    public EmailService(Emailer emailer) {
        this.emailer = emailer;
    }

    @Override
    public void start(String configFile, String stateDir) {
        try {
            streams = processStreams(configFile, stateDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        streams.start();
        log.info("Started Service " + SERVICE_APP_ID);
    }

    private KafkaStreams processStreams(final String bootstrapServers, final String stateDir) throws IOException {

        final StreamsBuilder builder = new StreamsBuilder();

        //Create the streams/tables for the join
        final KStream<String, Order> orders = builder.stream(ORDERS.name(),
                Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()));
        final GlobalKTable<String, Customer> customers = builder.globalTable(CUSTOMERS.name(),
                Consumed.with(CUSTOMERS.keySerde(), CUSTOMERS.valueSerde()));
//
        // Join a stream and a table then send an email for each
        // GlobalKTable to stream join takes three arguments: Table, mapping of stream (key,value) to table key for join
        // And the join function - takes values from stream and table and returns result
        orders.join(customers,
                        //TODO: Function that returns Customer key (customerId) from (orderId, order) tuple
                        ,
                        //TODO: Function that returns an email tuple that we can use to send email notifications
        )
                //Now for each tuple send an email.
                .peek((key, emailTuple)
                        -> emailer.sendEmail(emailTuple)
                );

        return new KafkaStreams(builder.build(), configStreams(bootstrapServers, stateDir, SERVICE_APP_ID));
    }

    interface Emailer {
        void sendEmail(EmailTuple details);
    }

    private static class LoggingEmailer implements Emailer {

        @Override
        public void sendEmail(EmailTuple details) {
            //In a real implementation we would do something a little more useful
            log.warn("Sending an email to: \nCustomer: {} \n Order: {}", details.customer, details.order);
        }
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }


    public class EmailTuple {

        public Order order;
        public Customer customer;

        public EmailTuple(Order order, Customer customer) {
            this.order = order;
            this.customer = customer;
        }

        @Override
        public String toString() {
            return "EmailTuple{" +
                    "order=" + order +
                    ", customer=" + customer +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        EmailService service = new EmailService(new LoggingEmailer());
        service.start(parseArgsAndConfigure(args), "/tmp/kafka-streams");
        addShutdownHookAndBlock(service);
    }
}
