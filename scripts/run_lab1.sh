#!/usr/bin/env bash

trap 'kill $(jobs -p)' EXIT

echo "NOTE: starting a utility program that generates not-very-random orders for our service to validate..."
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.utils.ProduceOrders &

echo "NOTE: starting our validation service"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.OrderDetailsService &

echo "NOTE: starting to consume results - both new and validated orders"
ccloud consume -t orders