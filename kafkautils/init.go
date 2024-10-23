package kafkautils

import (
	"context"
	"fmt"
	"log"

	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaConfig holds the configuration for Kafka initialization
type KafkaConfig struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	Brokers           []string
}

// InitKafka initializes the Kafka topic with partitions and replication
func InitKafka(adminClient *kafka.AdminClient) KafkaConfig {
	// Define topic and partition settings
	kafka_host := os.Getenv("KAFKA_HOST")
	kafkaConfig := KafkaConfig{
		Topic:             "partitioned-topic",
		NumPartitions:     1,                             // Adjust the partition count as needed
		ReplicationFactor: 1,                             // Adjust replication factor as per requirements
		Brokers:           []string{kafka_host + "9092"}, // Replace with the first broker address
	}

	// Use Metadata to check if the topic exists
	topicName := kafkaConfig.Topic
	metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
	if err != nil {
		// Handle the error gracefully
		log.Printf("Error occurred while getting Kafka metadata: %s\n", err)
		return kafkaConfig // or handle it as needed
	}

	// Check if the topic exists in the metadata
	topicExists := false
	if _, ok := metadata.Topics[topicName]; ok {
		fmt.Printf("Topic '%s' already exists.\n", topicName)
		topicExists = true
	}

	// Create the topic only if it does not exist
	if !topicExists {
		// Define the topic creation request
		topicConfig := kafka.TopicSpecification{
			Topic:             kafkaConfig.Topic,
			NumPartitions:     kafkaConfig.NumPartitions,
			ReplicationFactor: kafkaConfig.ReplicationFactor,
		}

		// Create the topic
		results, err := adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{topicConfig})
		if err != nil {
			log.Fatalf("Failed to create topic: %s\n", err)
		}

		// Handle the results of topic creation
		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				fmt.Printf("Failed to create topic: %s (%s)\n", result.Topic, result.Error.String())
			} else {
				fmt.Printf("Topic '%s' created successfully with %d partitions\n", result.Topic, kafkaConfig.NumPartitions)
			}
		}
	}

	// Return Kafka configuration for further use
	return kafkaConfig
}
