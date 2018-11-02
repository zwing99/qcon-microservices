#!/usr/bin/env bash

# this kills the services when the script exits. Comment out to keep experimenting with the REST API
trap 'kill $(jobs -p)' EXIT

echo "NOTE: Starting validation service"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.OrderDetailsService &

echo "Note: Starting order service"
java -cp target/uber-orders-service-1.0.0-SNAPSHOT.jar io.confluent.qcon.orders.OrderService &

read  -n 1 -p "Services started. Press any key to continue running few tests"

echo "Creating a valid order"
curl -XPOST --header "Content-Type: application/json" -d '{"id":"0","customerId":"15", "state":"CREATED", "product":"AMPS", "quantity":3, "price":5.0}' http://127.0.0.1:9099/v1/orders

echo "Checking status of order"
curl http://127.0.0.1:9099/v1/orders/0

# This will work after lab2 is done and the new endpoint is implemented
# echo "Checking that order is validated:"
# curl http://127.0.0.1:9099/v1/orders/0/validated

echo "Creating a invalid order"
curl -X POST --header "Content-Type: application/json" -d '{"id":"1","customerId":"15", "state":"CREATED", "product":"AMPS", "quantity":-13, "price":5.0}' http://127.0.0.1:9099/v1/orders

echo "Checking status of order"
curl http://127.0.0.1:9099/v1/orders/1

# This will work after lab2 is done and the new endpoint is implemented
# echo "Checking that order is validated:"
# curl http://127.0.0.1:9099/v1/orders/1/validated

read  -n 1 -p "Tests ran. Press any key to exit"


