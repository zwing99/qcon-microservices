package io.confluent.qcon.orders;

import io.confluent.qcon.orders.domain.Order;
import io.confluent.qcon.orders.domain.OrderState;
import io.confluent.qcon.orders.domain.Schemas;
import io.confluent.qcon.orders.utils.LoadConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.confluent.qcon.orders.utils.MicroserviceShutdown.addShutdownHookAndBlock;

/**
 * This class provides a REST interface to write and read orders using a CQRS pattern
 * (https://martinfowler.com/bliki/CQRS.html). Three methods are exposed over HTTP:
 * <p>
 * - POST(Order) -> Writes and order and returns location of the resource.
 * <p>
 * - GET(OrderId) (Optional timeout) -> Returns requested order, blocking for timeout if no id present.
 * <p>
 * - GET(OrderId)/Validated (Optional timeout)
 * <p>
 * POST does what you might expect: it adds an Order to the system returning when Kafka sends the appropriate
 * acknowledgement.
 * <p>
 * GET accesses an inbuilt Materialized View, of Orders, which are kept in a
 * State Store inside the service. This CQRS-styled view is updated asynchronously wrt the HTTP
 * POST.
 * <p>
 * Calling GET(id) when the ID is not present will block the caller until either the order
 * is added to the view, or the passed TIMEOUT period elapses. This allows the caller to
 * read-their-own-writes.
 * <p>
 * In addition HTTP POST returns the location of the order submitted in the response.
 * <p>
 * Calling GET/id/validated will block until the FAILED/VALIDATED order is available in
 * the View.
 * <p>
 * The View can also be scaled out linearly simply by adding more instances of the
 * view service, and requests to any of the REST endpoints will be automatically forwarded to the
 * correct instance for the key requested orderId via Kafka's Queryable State feature.
 * <p>
 * Non-blocking IO is used for all operations other than the intialization of state stores on
 * startup or rebalance which will block calling Jetty thread.
 *<p>
 * NB This demo code only includes a partial implementation of the holding of outstanding requests
 * and as such would lead timeouts if used in a production use case.
 */

@Path("v1")
public class OrderService implements Service {

    private static final String ORDERS_STORE_NAME = "orders-store";
    // Renaming the service is the easiest way to reset and start from scratch
    private final String SERVICE_APP_ID = "order-service-1";
    private static final String CALL_TIMEOUT = "10000";
    private static final int PORT = 9099;
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private Server jettyServer;
    private KafkaProducer<String, Order> producer;
    private KafkaStreams streams = null;


    //In a real implementation we would need to (a) support outstanding requests for the same Id/filter from
    // different users and (b) periodically purge old entries from this map.
    private Map<String, FilteredResponse<String, Order>> outstandingRequests = new ConcurrentHashMap<>();


    @Override
    public void start(String configFile, String stateDir) {
        jettyServer = startJetty(PORT, this);
        try {
            producer = startProducer(configFile);
            streams = startKStreams(configFile, stateDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("Started Service " + getClass().getSimpleName());

    }

    Server startJetty(int port, Object binding) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(binding);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
        return jettyServer;
    }

    public static <T> KafkaProducer startProducer(String configFile) throws IOException {
        Properties producerConfig = LoadConfigs.loadConfig(configFile);

        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-sender");

        return new KafkaProducer<String, Order>(producerConfig,
                Schemas.Topics.ORDERS.keySerde().serializer(),
                Schemas.Topics.ORDERS.valueSerde().serializer());
    }

    private KafkaStreams startKStreams(String configFile, String stateDir) throws IOException {
        KafkaStreams streams = new KafkaStreams(
                createOrdersMaterializedView().build(),
                configStreams(configFile, stateDir));
        streams.start();
        return streams;
    }

    private Properties configStreams(String configFile, String stateDir) throws IOException {
        Properties props = LoadConfigs.loadConfig(configFile);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, SERVICE_APP_ID);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Recommended cloud configuration for Streams (basically, wait for longer before exiting if brokers disconnect)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
        props.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);

        return props;
    }

    /**
     * Create a table of orders which we can query. When the table is updated
     * we check to see if there is an outstanding HTTP GET request waiting to be
     * fulfilled.
     */
    private StreamsBuilder createOrdersMaterializedView() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(Schemas.Topics.ORDERS.name(),
                Consumed.with(Schemas.Topics.ORDERS.keySerde(), Schemas.Topics.ORDERS.valueSerde()),
                    Materialized.as(ORDERS_STORE_NAME))
                .toStream().foreach(this::maybeCompleteLongPollGet);
        return builder;
    }

    private void maybeCompleteLongPollGet(String id, Order order) {
        FilteredResponse callback = outstandingRequests.get(id);

        // We just got a new update to our local state, which means an order was created or validated
        // If there is an outstanding request for an update regarding that order
        // AND the update satisfies the request (i.e. if the request is just for a "validated" response, don't return new orders)
        // then unblock the caller

        if (callback != null && callback.predicate.test(id, order)) {
            callback.asyncResponse.resume(order);
        }
    }

    /**
     * Perform a "Long-Poll" styled get. This method will attempt to get the value for the passed key
     * blocking until the key is available or passed timeout is reached. Non-blocking IO is used to
     * implement this, but the API will block the calling thread if no data is available
     *
     * @param id - the key of the value to retrieve
     * @param timeout - the timeout for the long-poll
     * @param asyncResponse - async response used to trigger the poll early should the appropriate
     * value become available
     */
    @GET
    @ManagedAsync
    @Path("/orders/{id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void getWithTimeout(@PathParam("id") final String id,
                               @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) Long timeout,
                               @Suspended final AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);

        log.info("running GET on this node");
        try {
            Order order = ordersStore().get(id);
            if (order == null) {
                log.info("Delaying get as order not present for id " + id);
                outstandingRequests.put(id, new FilteredResponse<>(asyncResponse, (k, v) -> true));
            } else {
                asyncResponse.resume(order);
            }
        } catch (InvalidStateStoreException e) {
            //Store not ready so delay
            log.info("Delaying request for " + id + " because state store is not ready.");
            outstandingRequests.put(id, new FilteredResponse<>(asyncResponse, (k, v) -> true));
        }
    }

    /**
     * Perform a "Long-Poll" styled get. This method will attempt to get the order for the ID
     * blocking until the order has been validated or passed timeout is reached. Non-blocking IO is used to
     * implement this, but the API will block the calling thread if no data is available
     *
     * @param id - the key of the value to retrieve
     * @param timeout - the timeout for the long-poll
     * @param asyncResponse - async response used to trigger the poll early should the appropriate
     * value become available
     */
    @GET
    @ManagedAsync
    @Path("orders/{id}/validated")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void getPostValidationWithTimeout(@PathParam("id") final String id,
                                             @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) Long timeout,
                                             @Suspended final AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);

        log.info("running GET on this node");
        try {
            Order order = ordersStore().get(id);
            if (order == null || (order.getState() != OrderState.VALIDATED && order.getState() != OrderState.FAILED)) {
                log.info("Delaying get as a validated order not present for id " + id);
                outstandingRequests.put(id, new FilteredResponse<>(asyncResponse,
                        (k, v) -> (v.getState() == OrderState.VALIDATED || v.getState() == OrderState.FAILED)));
            } else {
                asyncResponse.resume(order);
            }
        } catch (InvalidStateStoreException e) {
            //Store not ready so delay
            log.info("Delaying request for " + id + " because state store is not ready.");
            outstandingRequests.put(id, new FilteredResponse<>(asyncResponse,
                    (k, v) -> (v.getState() == OrderState.VALIDATED || v.getState() == OrderState.FAILED)));
        }
    }

    /**
     * Persist an Order to Kafka. Returns once the order is successfully written to R nodes where
     * R is the replication factor configured in Kafka.
     *
     * @param order the order to add
     * @param timeout the max time to wait for the response from Kafka before timing out the POST
     */
    @POST
    @ManagedAsync
    @Path("/orders")
    @Consumes(MediaType.APPLICATION_JSON)
    public void submitOrder(final Order order,
                            @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) final Long timeout,
                            @Suspended final AsyncResponse response) {
        setTimeout(timeout, response);

        Order bean = order;
        producer.send(new ProducerRecord<>(Schemas.Topics.ORDERS.name(), bean.getId(), bean),
                callback(response, bean.getId()));
    }

    private Callback callback(final AsyncResponse response, final String orderId) {
        return (recordMetadata, e) -> {
            if (e != null) {
                response.resume(e);
            } else {
                try {
                    //Return the location of the newly created resource
                    Response uri = Response.created(new URI("/v1/orders/" + orderId)).build();
                    response.resume(uri);
                } catch (URISyntaxException e2) {
                    e2.printStackTrace();
                }
            }
        };
    }

    private ReadOnlyKeyValueStore<String, Order> ordersStore() {
        return streams.store(ORDERS_STORE_NAME, QueryableStoreTypes.keyValueStore());
    }

    public static void setTimeout(long timeout, AsyncResponse asyncResponse) {
        asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(resp -> resp.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("HTTP GET timed out after " + timeout + " ms\n")
                        .build()));
    }


    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        OrderService service = new OrderService();
        service.start(LoadConfigs.parseArgsAndConfigure(args), "/tmp/kafka-streams");
        addShutdownHookAndBlock(service);
    }

    class FilteredResponse<K, V> {
        private AsyncResponse asyncResponse;
        private Predicate<K, V> predicate;

        FilteredResponse(AsyncResponse asyncResponse, Predicate<K, V> predicate) {
            this.asyncResponse = asyncResponse;
            this.predicate = predicate;
        }
    }
}
