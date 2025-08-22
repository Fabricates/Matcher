package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/worthies/matcher"
)

func main() {
	ctx := context.Background()

	// Example 1: Redis Event Broker
	fmt.Println("=== Redis Event Broker Example ===")
	redisConfig := matcher.RedisEventBrokerConfig{
		RedisAddr:     "localhost:6379",
		Password:      "",
		DB:            0,
		StreamName:    "matcher-events",
		ConsumerGroup: "matcher-group",
		ConsumerName:  "consumer-1",
		NodeID:        "node-redis-demo",
	}

	redisBroker, err := matcher.NewRedisEventBroker(redisConfig)
	if err != nil {
		log.Printf("Failed to create Redis broker: %v", err)
	} else {
		defer redisBroker.Close()

		// Test health
		if err := redisBroker.Health(ctx); err != nil {
			log.Printf("Redis broker health check failed: %v", err)
		} else {
			fmt.Println("Redis broker is healthy")

			// Create event channel
			events := make(chan *matcher.Event, 10)

			// Subscribe to events
			go func() {
				if err := redisBroker.Subscribe(ctx, events); err != nil {
					log.Printf("Failed to subscribe: %v", err)
				}
			}()

			// Publish a test event
			testEvent := &matcher.Event{
				Type:      matcher.EventTypeRuleAdded,
				Timestamp: time.Now(),
				NodeID:    "node-redis-demo",
				Data:      "This is a test event from Redis broker",
			}

			if err := redisBroker.Publish(ctx, testEvent); err != nil {
				log.Printf("Failed to publish event: %v", err)
			} else {
				fmt.Println("Published test event to Redis")
			}

			// Wait for event (timeout after 2 seconds)
			select {
			case receivedEvent := <-events:
				fmt.Printf("Received event from Redis: %+v\n", receivedEvent)
			case <-time.After(2 * time.Second):
				fmt.Println("No event received from Redis (this is expected if Redis server is not running)")
			}
		}
	}

	fmt.Println()

	// Example 2: Kafka Event Broker
	fmt.Println("=== Kafka Event Broker Example ===")
	kafkaConfig := matcher.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "matcher-events",
		ConsumerGroup: "matcher-group",
		NodeID:        "node-kafka-demo",
	}

	kafkaBroker, err := matcher.NewKafkaEventBroker(kafkaConfig)
	if err != nil {
		log.Printf("Failed to create Kafka broker: %v", err)
	} else {
		defer kafkaBroker.Close()

		// Test health
		if err := kafkaBroker.Health(ctx); err != nil {
			log.Printf("Kafka broker health check failed: %v", err)
		} else {
			fmt.Println("Kafka broker is healthy")

			// Create event channel
			events := make(chan *matcher.Event, 10)

			// Subscribe to events
			go func() {
				if err := kafkaBroker.Subscribe(ctx, events); err != nil {
					log.Printf("Failed to subscribe: %v", err)
				}
			}()

			// Publish a test event
			testEvent := &matcher.Event{
				Type:      matcher.EventTypeRuleDeleted,
				Timestamp: time.Now(),
				NodeID:    "node-kafka-demo",
				Data:      "This is a test event from Kafka broker",
			}

			if err := kafkaBroker.Publish(ctx, testEvent); err != nil {
				log.Printf("Failed to publish event: %v", err)
			} else {
				fmt.Println("Published test event to Kafka")
			}

			// Wait for event (timeout after 2 seconds)
			select {
			case receivedEvent := <-events:
				fmt.Printf("Received event from Kafka: %+v\n", receivedEvent)
			case <-time.After(2 * time.Second):
				fmt.Println("No event received from Kafka (this is expected if Kafka server is not running)")
			}
		}
	}

	fmt.Println()

	// Example 3: In-Memory Event Broker (always works)
	fmt.Println("=== In-Memory Event Broker Example ===")
	memoryBroker := matcher.NewInMemoryEventBroker("node-memory-demo")
	defer memoryBroker.Close()

	// Create event channel
	events := make(chan *matcher.Event, 10)

	// Subscribe to events
	if err := memoryBroker.Subscribe(ctx, events); err != nil {
		log.Printf("Failed to subscribe: %v", err)
	} else {
		// Publish a test event
		testEvent := &matcher.Event{
			Type:      matcher.EventTypeDimensionAdded,
			Timestamp: time.Now(),
			NodeID:    "node-memory-demo",
			Data:      "This is a test event from in-memory broker",
		}

		if err := memoryBroker.Publish(ctx, testEvent); err != nil {
			log.Printf("Failed to publish event: %v", err)
		} else {
			fmt.Println("Published test event to in-memory broker")
		}

		// Wait for event
		select {
		case receivedEvent := <-events:
			fmt.Printf("Received event from memory: %+v\n", receivedEvent)
		case <-time.After(1 * time.Second):
			fmt.Println("No event received from memory broker")
		}
	}

	fmt.Println("\nDemo completed!")
}
