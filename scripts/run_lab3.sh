#!/usr/bin/env bash

trap 'kill $(jobs -p)' EXIT

echo "NOTE: Creating a single new customer using ProduceCustomers class"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.utils.ProduceCustomers &

echo "NOTE: starting a utility program that generates not-very-random orders for our service to validate and notify customers..."
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.utils.ProduceOrders &

echo "NOTE: starting our validation service"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.OrderDetailsService &

echo "NOTE: 'sending emails' to customers with order status"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.EmailService