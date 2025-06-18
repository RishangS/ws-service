package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	auth "github.com/RishangS/auth-service/gen/proto"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	authClient     auth.AuthServiceClient
	messagesWriter *kafka.Writer
	persistWriter  *kafka.Writer
	clients        = make(map[string]*websocket.Conn)
	clientsMu      sync.Mutex
)

func main() {
	// Connect to Auth Service
	authAddr := os.Getenv("AUTH_SERVICE_ADDR")
	if authAddr == "" {
		authAddr = "localhost:50051" // default
	}

	authConn, err := grpc.Dial(
		authAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect to auth service: %v", err)
	}
	defer authConn.Close()
	authClient = auth.NewAuthServiceClient(authConn)

	// Initialize Kafka writers
	initKafkaWriters()
	defer func() {
		if err := messagesWriter.Close(); err != nil {
			log.Printf("Error closing messages writer: %v", err)
		}
		if err := persistWriter.Close(); err != nil {
			log.Printf("Error closing persist writer: %v", err)
		}
	}()

	http.HandleFunc("/ws", handleWebSocket)
	go ensureTopicExists()
	go startKafkaConsumer()
	log.Println("WebSocket service started on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func initKafkaWriters() {
	// Writer for real-time messages
	messagesWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "messages",
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
	})

	// Writer for persistence
	persistWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "persist",
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: -1, // Ensure message is persisted
	})
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")

	if token == "" {
		log.Println("Rejected: No token provided")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Verify token with Auth service
	resp, err := authClient.VerifyToken(context.Background(), &auth.VerifyRequest{
		Token: token,
	})
	if err != nil || !resp.Valid {
		fmt.Println("ERROR", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	log.Printf("Incoming message %v", resp.Username)

	username := resp.Username

	// Upgrade to WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	// Register client
	registerClient(username, conn)
	defer unregisterClient(username)

	// Message handling loop
	for {
		var msg Message
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("Read error for %s: %v", username, err)
			break
		}

		// Validate message
		if msg.To == "" || msg.Content == "" {
			continue
		}

		// Publish to both topics
		if err := publishMessage(username, msg); err != nil {
			log.Printf("Error publishing message: %v", err)
		}
	}
}

func registerClient(username string, conn *websocket.Conn) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	clients[username] = conn
}

func unregisterClient(username string) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	delete(clients, username)
}

func publishMessage(sender string, msg Message) error {
	// Common headers for both messages
	headers := []kafka.Header{
		{Key: "From", Value: []byte(sender)},
		{Key: "To", Value: []byte(msg.To)},
		{Key: "Timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
	}

	// Publish to real-time topic
	if err := messagesWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:     []byte(sender),
			Value:   []byte(msg.Content),
			Headers: headers,
		},
	); err != nil {
		return fmt.Errorf("messages topic write error: %w", err)
	}

	// Publish to persistence topic
	if err := persistWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:     []byte(sender),
			Value:   []byte(msg.Content),
			Headers: headers,
		},
	); err != nil {
		return fmt.Errorf("persist topic write error: %w", err)
	}

	return nil
}

// Message represents the WebSocket message structure
type Message struct {
	To      string `json:"to"`
	Content string `json:"content"`
}

func ensureTopicExists() {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatalf("Failed to get controller: %v", err)
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             "messages",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		{
			Topic:             "persist",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("Error creating topics: %v", err)
	}
}
