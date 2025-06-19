package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func startKafkaConsumer() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	fmt.Println("kafkaBrokers", kafkaBrokers)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		Topic:   "messages",
		GroupID: "websocket-delivery",
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka read error: %v", err)
			continue
		}

		// Extract recipient from headers
		var to string
		for _, header := range msg.Headers {
			if header.Key == "To" {
				to = string(header.Value)
				break
			}
		}

		if to == "" {
			continue
		}

		// Find recipient's connection
		clientsMu.Lock()
		conn, ok := clients[to]
		clientsMu.Unlock()

		if ok {
			// Send message to recipient
			err := conn.WriteJSON(map[string]string{
				"from":    string(msg.Key),
				"content": string(msg.Value),
			})
			if err != nil {
				log.Printf("Write error to %s: %v", to, err)
			}
		}
	}
}
