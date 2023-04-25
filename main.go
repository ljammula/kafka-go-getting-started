package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
)

func main() {

	if len(os.Args) == 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}

	consumerGroupId := "kafka-go-getting-started"

	//properties file name
	configFile := os.Args[2]

	//rootCmd
	var rootCmd = &cobra.Command{Use: "app"}

	var cmdCreateTopic = &cobra.Command{
		Use:   "create-topic [properties-file-name]",
		Short: "create kafka topic",
		Run: func(cmd *cobra.Command, args []string) {
			createTopic(configFile)
		},
	}

	var cmdWrite = &cobra.Command{
		Use:   "write [properties-file-name]",
		Short: "write to kafka topic",
		Run: func(cmd *cobra.Command, args []string) {
			writeToTopic(configFile)
		},
	}

	//consumer group-id
	if len(os.Args) == 4 {
		consumerGroupId = os.Args[3]
	}

	var cmdRead = &cobra.Command{
		Use:   "read [properties-file-name]",
		Short: "read from kafka topic",
		Run: func(cmd *cobra.Command, args []string) {
			readFromTopic(configFile, consumerGroupId)
		},
	}
	rootCmd.AddCommand(cmdRead)
	rootCmd.AddCommand(cmdWrite)
	//without this, we will create a topic with single partition
	rootCmd.AddCommand(cmdCreateTopic)
	rootCmd.Execute()
}

func createTopic(configFile string) {
	//create topic with partitions
	conf := ReadConfig(configFile)
	admin, err := kafka.NewAdminClient(&conf)
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n ", err.Error())
		os.Exit(1)
	}
	defer admin.Close()

	//define topic configuration with multiple partitions
	topic := "purchases"
	config := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	//send a CreateTopics request to the kafka broker
	results, err := admin.CreateTopics(context.Background(),
		[]kafka.TopicSpecification{config},
		kafka.SetAdminOperationTimeout(5000),
	)
	if err != nil {
		fmt.Printf("Failed to create topic: %s\n", err.Error())
		os.Exit(1)
	}

	//Print results of the CreateTopics request
	for _, result := range results {
		fmt.Printf("Topics: %s, Errors: %v\n", result.Topic, result.Error)
	}
}

func writeToTopic(configFile string) {
	conf := ReadConfig(configFile)
	topic := "purchases"
	p, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic = %s : key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := [...]string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 10000; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(data),
		}, nil)
	}

	// Wait for all messages to be delivered
	p.Flush(60 * 1000)
	p.Close()
}

func readFromTopic(configFile string, consumerGroupId string) {
	conf := ReadConfig(configFile)
	conf["group.id"] = consumerGroupId
	conf["auto.offset.reset"] = "earliest"

	c, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	topic := "purchases"
	_ = c.SubscribeTopics([]string{topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}

	c.Close()
}
