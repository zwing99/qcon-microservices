# qcon-microservices
Example online orders app composed of event-driven microservices. Built for QCon workshop.

This workshop will include:

- Create a Kafka cluster on Confluent Cloud
- Create topics, produce and consume events from commandline
- Using Kafka consumer and producer in a microservice to perform simple order validation.
- Implement a webservice that submits orders and uses "read-own-writes" pattern to show order status.
- Use Streams-to-Table join to send status updates by email.

## Outline

1. Introduction to Kafka, topics and events:
    - What is Stream Processing?
    - Key concepts of Kafka Brokers, Connectors and Streams
    - Create a cluster in Confluent Cloud
    - Install the Confluent Cloud CLI
    - Create topic
    - Produce events to topic
    - Consume events from topic
2. Simple event validation:
    - Overview of our Architecture
    - "Hipster Stream Processing"
    - Producer and Consumer APIs
    - Configuring Kafka Clients to Connect to Cloud
3. 


## Requirements
- OSX or Linux
- Java 8
- Java development environment (Intellij recommended)
- Maven
- Maven dependencies (Fetch with "mvn dependency:resolve")
- Step 1 of outline (Create cluster and test connectivity)

