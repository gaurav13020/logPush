// main.go
package main

import (
	"fmt"
	"log"
	"logPush/kafkautils"

	// "os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofiber/fiber/v2"
)

type LogEntry struct {
	Msg       string `json:"msg"`
	Timestamp string `json:"timestamp"`
	OtherKeys string `json:"otherKeys"`
}

func main() {
	// Step 1: Create Kafka Admin Client
	// kafka_host := os.Getenv("KAFKA_HOST")
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092", // Replace with the first broker address
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka admin client: %s\n", err)
	}
	defer adminClient.Close()

	// Step 2: Initialize Kafka with partitions and get the config from init.go
	kafkaConfig := kafkautils.InitKafka(adminClient)

	// Step 3: Create Kafka producer and consumer clients
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

	// Step 4: Set up Fiber server to receive logs from Fluentd
	app := fiber.New()

	// Define a route for log ingestion
	app.Post("/api", func(c *fiber.Ctx) error {
		// Get the raw JSON body from the request
		jsonMessage := c.Body()
		fmt.Print(jsonMessage)

		// Produce the raw JSON to Kafka
		kafkaClient.ProduceMessage(kafkaConfig.Topic, 1, string(jsonMessage))

		return c.SendString("Log received and sent to Kafka")
	})

	// Start Fiber server to listen for logs from Fluentd
	go func() {
		log.Fatal(app.Listen("0.0.0.0:9000"))
	}()

	// Step 5: Consume messages from Kafka topic
	kafkaClient.ConsumeMessages(kafkaConfig.Topic)
}
