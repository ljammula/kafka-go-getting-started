# A Simple Confluent Kafka Pub/Sub

## Build

`go build -o out .`

## Run

### Create Kafka topic with 3 partitions

`./out/kafka-go-getting-started create-topic getting-started.properties`

### Publishing to Kafka

`./out/kafka-go-getting-started write getting-started.properties`

### Consuming from Kafka using 3 consumers with same consumer-group-id

`./out/kafka-go-getting-started read getting-started.properties consumer-group-1`
`./out/kafka-go-getting-started read getting-started.properties consumer-group-1`
`./out/kafka-go-getting-started read getting-started.properties consumer-group-1`
.
.
.
