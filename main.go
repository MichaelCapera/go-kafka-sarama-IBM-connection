package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {
	// Configuración del consumidor de Kafka
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Crear un consumidor de Kafka
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// Imprimir un mensaje para confirmar que la conexión se ha establecido con éxito
	log.Println("Connected to Kafka!")
}
