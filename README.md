# qcon-microservices
Example online orders app composed of event-driven microservices. Built for QCon workshop.

This workshop will include:

- Create a Kafka cluster on Confluent Cloud
- Create topics, produce and consume events from commandline
- Using Kafka consumer and producer in a microservice to perform simple order validation.
- Implement a webservice that submits orders and uses "read-own-writes" pattern to show order status.
- Use Streams-to-Table join to send status updates by email.


## Preparation!

## Make sure you have the following:
- OSX or Linux
- Java 8
- Java development environment (Intellij recommended)
- Maven

### Please create cloud clusters for the workshop!

1. Login to https://confluent.cloud with username and password you received by email from QCon.
2. Create a new cluster! call it “lastname-firstname”, make sure you
use the smallest options, create it on Google Cloud Platform and use
us-centeral1 region. You'll be shown the cost for the cluster. Don't
worry about it - Confluent has been kind enough to sponsor the
clusters and you will not be charged.
3. Follow the on-screen instructions closely to install Confluent
Cloud CLI and initialize the CLI.
4. If you ran into issues, try to refer to the documentation here:
https://docs.confluent.io/current/quickstart/cloud-quickstart.html.
5. If you managed to create a cluster, follow Step 3 of the Quickstart
(https://docs.confluent.io/current/quickstart/cloud-quickstart.html)
to create a topic and produce/consume some events.

### In addition:
1. Please clone this repository
2. Run `mvn dependency:resolve` to get dependencies in advance. This will help with the network in the class.


## Outline

1. **Lesson 0:** Introduction to Kafka, topics and events:
    - What is Stream Processing?
    - Key concepts of Kafka Brokers, Connectors and Streams
2. **Lab 0:** 
    - Create a cluster in Confluent Cloud
    - Install the Confluent Cloud CLI
    - Create topic, produce and consume events from Cloud CLI
3. **Lesson 1:** Simple event validation:
    - Overview of our Architecture
    - "Hipster Stream Processing"
    - Producer and Consumer APIs
4. **Lab 1:** 
    - `git checkout lab1`
    - Create topic `ORDERS` with 3 partitions
    - Validate events and produce results to Kafka: Implement the TODO part of the OrderValidationService. Note that `validate()` method implements the specific validation rules.
    - Build the service :) using `mvn clean install`
    - Test the simple validation service: Either using `scripts/run_lab1.sh`, writing a unit test, or a different method.
    - **Extra credit:** How would you add multiple validators?
5. **Lesson 2:** CQRS-based web-service with Kafka Streams
    - Introduction to Kafka Streams
    - Introducing local materialized views
    - Interactive queries in Kafka Streams
    - Code review of web-service
6. **Lab 2:**
    - `git checkout lab2`
    - Run and experiment with web service:
        - Build the service :) using `mvn clean install`        
        - Use `scripts/run_lab2.sh` to experiment with the service. Running the script will start the Order service and validation service. Pressing any key will run few pre-canned `curl` commands that use the service. Another click will cause the services to exit.
    - Add "check for validation" endpoint: Implement the TODO part of the OrderService. Note that we already have a simple GET endpoint that gets any order, we need a new endpoint that only returns validated orders.
    - Use the same script from earlier to experiment with the new endpoint.
    - **Extra Credit:** How would you scale to multiple instances?
7. **Lesson 3** (if there is time): Simple email service
    - Stream-table join enrichment pattern
    - Global KTables
8. **Lab 3** (if there is time):
    - `git checkout lab3`
    - Implement Stream-table join: Implement the two functions required for the join (marked as TODO)
    - Test email service using the CustomerProducer and OrderProducer clases



