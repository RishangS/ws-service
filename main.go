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
	authClient  auth.AuthServiceClient
	kafkaWriter *kafka.Writer
	clients     = make(map[string]*websocket.Conn)
	clientsMu   sync.Mutex
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

	// Kafka writer for outgoing messages
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "messages",
	})
	defer kafkaWriter.Close()
	http.HandleFunc("/ws", handleWebSocket)
	go ensureTopicExists()
	go startKafkaConsumer()
	log.Println("WebSocket service started on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	log.Printf("Incoming connection with token: %s", token) // Add this

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

	username := resp.Username

	// Upgrade to WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	// Register client
	clientsMu.Lock()
	clients[username] = conn
	clientsMu.Unlock()

	// Remove client when they disconnect
	defer func() {
		clientsMu.Lock()
		delete(clients, username)
		clientsMu.Unlock()
	}()

	// Message handling loop
	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Read error for %s: %v", username, err)
			break
		}

		// Validate message
		if msg.To == "" || msg.Content == "" {
			continue
		}

		// Publish to Kafka
		err = kafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(username),
				Value: []byte(msg.Content),
				Headers: []kafka.Header{
					{Key: "To", Value: []byte(msg.To)},
				},
			},
		)
		if err != nil {
			log.Printf("Kafka write error: %v", err)
		}
	}
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
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("Error creating topics: %v", err)
	}
}
