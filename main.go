// main.go
package main

import (
	"fmt"
	"log"
	"logPush/kafkautils"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LogEntry struct {
	Msg       string `json:"msg"`
	Timestamp string `json:"timestamp"` // Epoch/Unix timestamp
	OtherKeys string `json:"other keys"`
}

func main() {
	// Step 1: Create Kafka Admin Client
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092", // Replace with the first broker address
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka admin client: %s\n", err)
	}
	defer adminClient.Close()

	// Step 2: Initialize Kafka with partitions and get the config from init.go
	kafkaConfig := kafkautils.InitKafka(adminClient)

	// Step 3: Create Kafka producer and consumer clients (in kafka.go)
	producer, err := kafkautils.NewKafkaProducer(kafkaConfig.Brokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %s\n", err)
	}
	defer producer.Close()

	consumer, err := kafkautils.NewKafkaConsumer(kafkaConfig.Brokers, "go-consumer-group")
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %s\n", err)
	}
	defer consumer.Close()

	// Initialize Kafka client struct
	kafkaClient := kafkautils.KafkaClient{
		Producer: producer,
		Consumer: consumer,
	}

	// Step 4: Produce a message to a specific partition (e.g., partition 1)

	logEntries := []LogEntry{
		{
			Msg:       "log message 1",
			Timestamp: "UTC Timestamp/epoch", // Current timestamp in epoch
			OtherKeys: "customvalues",
		},
		{
			Msg:       "log message 2",
			Timestamp: "UTC Timestamp/epoch",
			OtherKeys: "customvalues",
		},
	}

	for _, logEntry := range logEntries {
		message := fmt.Sprintf("%v", logEntry)
		kafkaClient.ProduceMessage(kafkaConfig.Topic, 1, message)
	}

	// message := "Hello Kafka with Partitions!"
	// kafkaClient.ProduceMessage(kafkaConfig.Topic, 1, message)

	// Step 5: Consume messages from the topic
	kafkaClient.ConsumeMessages(kafkaConfig.Topic)
}
