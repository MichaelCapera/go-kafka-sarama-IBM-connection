package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {
	// Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// Print a message to confirm that the connection has been established successfully
	log.Println("Connected to Kafka!")
}
