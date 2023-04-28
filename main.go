package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
)

type Event struct {
	User  string `json:"user"`
	Items string `json:"items"`
}

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

	var cmdResetOffset = &cobra.Command{
		Use:   "reset-offset [properties-file-name]",
		Short: "reset-offset kafka topic",
		Run: func(cmd *cobra.Command, args []string) {
			resetOffset(configFile, args)
		},
	}
	rootCmd.AddCommand(cmdRead)
	rootCmd.AddCommand(cmdWrite)
	//without this, we will create a topic with single partition
	rootCmd.AddCommand(cmdCreateTopic)
	//reset offset
	rootCmd.AddCommand(cmdResetOffset)
	rootCmd.Execute()
}

func resetOffset(configFile string, args []string) {
	consumerGroupId := "consumer-group-1"
	var partition int32
	var offset int64

	// Parse flags into variables
	for _, f := range args {
		strs := strings.Split(f, "=")
		if len(strs) == 2 {
			switch strs[0] {
			case "consumer-group":
				consumerGroupId = strs[1]
			case "partition":
				i64, _ := strconv.ParseInt(strs[1], 10, 32)
				partition = int32(i64)
			case "offset":
				i64, _ := strconv.ParseInt(strs[1], 10, 64)
				offset = i64
			}
		}
	}

	// Create a new Kafka consumer
	conf := ReadConfig(configFile)
	conf["group.id"] = consumerGroupId
	conf["auto.offset.reset"] = "earliest"
	// Whether or not we store offsets automatically.
	conf["enable.auto.offset.store"] = false
	conf["session.timeout.ms"] = 600000
	conf["max.poll.interval.ms"] = 900000
	conf["heartbeat.interval.ms"] = 1000

	c, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	topic := "purchases"
	_ = c.SubscribeTopics([]string{topic}, nil)

	// Assign the topic partition to the consumer
	tp := kafka.TopicPartition{Topic: &topic, Partition: partition, Offset: kafka.Offset(offset)}
	err = c.Assign([]kafka.TopicPartition{tp})
	if err != nil {
		fmt.Printf("Failed to assign the topic partition to the consumer: %s\n", err)
		os.Exit(1)
	}

	// Seek to the specified offset
	err = c.Seek(tp, 100)
	if err != nil {
		fmt.Printf("Failed to seek from the offset: %s\n", err)
	}

	// Consume message at specified offset so the offset advances to next
	fmt.Printf("\n ***** Skipping offset on consumer-group-id = \"%s\", partition = %v, offset = %v\n", consumerGroupId, partition, offset)
	msg, err := c.ReadMessage(1000 * time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading message from offset %v\n", offset)
		os.Exit(1)
	}
	fmt.Printf("Received message: %v\n", string(msg.Value))

	// Manually commit the offset for the consumed message
	_, err = c.CommitMessage(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing message %s \n %s\n",
			msg.TopicPartition, err)
	}

	defer c.Close()
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
	items := [...]string{"books", "alarm clocks", "t-shirts", "gift cards", "batteries"}

	event := Event{
		User:  "default",
		Items: "none",
	}

	for n := 0; n < 10; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		//populate event data
		event.User = key
		event.Items = data

		//json marshal
		b, err := json.Marshal(event)
		if err != nil {
			fmt.Println(err)
			return
		}

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(string(b)),
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
			var event Event
			//unmarshal topic message
			err = json.Unmarshal(ev.Value, &event)
			//if message is not in expected format, exit the process
			if err != nil {
				fmt.Printf("Error in unmarshalling event %v: terminating. partition: %v, offset: %v\n", string(ev.Value), ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				return
			}

			fmt.Printf("Consumed event from topic = \"%s\", partition = %v, offset: %v: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, string(ev.Key), string(ev.Value))
		}
	}

	defer c.Close()
}
