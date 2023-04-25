# A Simple Kafka Pub/Sub using confluent-kafka-go

## Development Environment

The first step in getting your local development environment running is to spin up local versions of external dependencies. Use `docker-compose` to manage these, and you can bring them up by running the following command:

```
$ docker-compose up -d
```

We are using provectus/kafka-ui which is a free, open-source web UI to monitor and manage Apache Kafka clusters.

```
http://localhost:8080/
```

## Build

```
go build -o out .
```

## Run

### Create Kafka topic with 3 partitions

```
./out/kafka-go-getting-started create-topic getting-started.properties
```

### Publishing to Kafka

```
./out/kafka-go-getting-started write getting-started.properties
```

### Consuming from Kafka using 3 consumers with same consumer-group-id

```
./out/kafka-go-getting-started read getting-started.properties consumer-group-1
```

```
./out/kafka-go-getting-started read getting-started.properties consumer-group-1
```

```
./out/kafka-go-getting-started read getting-started.properties consumer-group-1
```

.
.
.
