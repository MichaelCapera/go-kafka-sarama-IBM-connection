package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
	"gopkg.in/gomail.v2"
)

func main() {
	// Configure SMTP client for sending email
	d := gomail.NewDialer("smtp.example.com", 587, "user@example.com", "xxxxpasswordxxx")

	// Connect to Kafka and process messages
	log.Println("Connecting to Kafka...")
	err := connectToKafka(d)
	if err != nil {
		log.Fatalf("Error connecting to Kafka: %v", err)
	}
}

func connectToKafka(d *gomail.Dialer) error {
	// Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Handle interrupt signals (Ctrl+C)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Define topic and partition
	topic := "topic_name"
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	log.Println("Connected to Kafka! Waiting for messages...")

	// Process Kafka messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Received message from Kafka
			log.Printf("Received message: %s\n", msg.Value)

			// Filter message if necessary
			// Send email
			go processAndSendEmail(d, string(msg.Value))

		case err := <-partitionConsumer.Errors():
			// Error consuming message from Kafka
			log.Printf("Error consuming message from Kafka: %v\n", err)

		case <-signals:
			// Received interrupt signal (Ctrl+C), stop consumer
			log.Println("Interrupt signal received, shutting down consumer...")
			return nil
		}
	}
}

func processAndSendEmail(d *gomail.Dialer, message string) {
	// Filter message if necessary
	// Send email
	err := sendEmail(d, message)
	if err != nil {
		log.Printf("Error sending email: %v\n", err)
	} else {
		log.Println("Email sent successfully!")
	}
}

func sendEmail(d *gomail.Dialer, message string) error {
	// Create email message
	m := gomail.NewMessage()
	m.SetHeader("From", "andrescapera.realestate@gmail.com")
	m.SetHeader("To", "to@gmail.com")
	m.SetHeader("Subject", "Hello Kafka")
	m.SetBody("text/plain", message)

	// Send email
	if err := d.DialAndSend(m); err != nil {
		return err
	}
	return nil
}
