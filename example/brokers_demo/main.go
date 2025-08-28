package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Fabricates/Matcher"
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
		slog.Error("Failed to create Redis broker", "error", err)
	} else {
		defer redisBroker.Close()

		// Test health
		if err := redisBroker.Health(ctx); err != nil {
			slog.Error("Redis broker health check failed", "error", err)
		} else {
			fmt.Println("Redis broker is healthy")

			// Create event channel
			events := make(chan *matcher.Event, 10)

			// Subscribe to events
			go func() {
				if err := redisBroker.Subscribe(ctx, events); err != nil {
					slog.Error("Failed to subscribe", "error", err)
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
				slog.Error("Failed to publish event", "error", err)
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

	// Example 2: Redis CAS Event Broker
	fmt.Println("=== Redis CAS Event Broker Example ===")
	redisCASConfig := matcher.RedisCASConfig{
		RedisAddr:    "localhost:6379",
		Password:     "",
		DB:           0,
		NodeID:       "node-redis-cas-demo",
		Namespace:    "matcher-cas-demo",
		PollInterval: 1 * time.Second,
	}

	redisCASBroker, err := matcher.NewRedisCASBroker(redisCASConfig)
	if err != nil {
		slog.Error("Failed to create Redis CAS broker: %v", err)
	} else {
		defer redisCASBroker.Close()

		// Test health
		if err := redisCASBroker.Health(ctx); err != nil {
			slog.Error("Redis CAS broker health check failed", "error", err)
		} else {
			fmt.Println("Redis CAS broker is healthy")

			// Create event channel
			events := make(chan *matcher.Event, 10)

			// Subscribe to events
			go func() {
				if err := redisCASBroker.Subscribe(ctx, events); err != nil {
					slog.Error("Failed to subscribe: %v", err)
				}
			}()

			// Publish a test event
			testEvent := &matcher.Event{
				Type:      matcher.EventTypeRuleUpdated,
				Timestamp: time.Now(),
				NodeID:    "node-redis-cas-demo",
				Data:      "This is a test event from Redis CAS broker",
			}

			if err := redisCASBroker.Publish(ctx, testEvent); err != nil {
				slog.Error("Failed to publish event: %v", err)
			} else {
				fmt.Println("Published test event to Redis CAS")
			}

			// Wait for event (timeout after 3 seconds to allow for polling)
			select {
			case receivedEvent := <-events:
				fmt.Printf("Received event from Redis CAS: %+v\n", receivedEvent)
			case <-time.After(3 * time.Second):
				fmt.Println("No event received from Redis CAS (this is expected if Redis server is not running)")
			}
		}
	}

	fmt.Println()

	// Example 3: Kafka Event Broker
	fmt.Println("=== Kafka Event Broker Example ===")
	kafkaConfig := matcher.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "matcher-events",
		ConsumerGroup: "matcher-group",
		NodeID:        "node-kafka-demo",
	}

	kafkaBroker, err := matcher.NewKafkaEventBroker(kafkaConfig)
	if err != nil {
		slog.Error("Failed to create Kafka broker: %v", err)
	} else {
		defer kafkaBroker.Close()

		// Test health
		if err := kafkaBroker.Health(ctx); err != nil {
			slog.Error("Kafka broker health check failed: %v", err)
		} else {
			fmt.Println("Kafka broker is healthy")

			// Create event channel
			events := make(chan *matcher.Event, 10)

			// Subscribe to events
			go func() {
				if err := kafkaBroker.Subscribe(ctx, events); err != nil {
					slog.Error("Failed to subscribe: %v", err)
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
				slog.Error("Failed to publish event: %v", err)
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

	// Example 4: In-Memory Event Broker (always works)
	fmt.Println("=== In-Memory Event Broker Example ===")
	memoryBroker := matcher.NewInMemoryEventBroker("node-memory-demo")
	defer memoryBroker.Close()

	// Create event channel
	events := make(chan *matcher.Event, 10)

	// Subscribe to events
	if err := memoryBroker.Subscribe(ctx, events); err != nil {
		slog.Error("Failed to subscribe: %v", err)
	} else {
		// Publish a test event
		testEvent := &matcher.Event{
			Type:      matcher.EventTypeDimensionAdded,
			Timestamp: time.Now(),
			NodeID:    "node-memory-demo",
			Data:      "This is a test event from in-memory broker",
		}

		if err := memoryBroker.Publish(ctx, testEvent); err != nil {
			slog.Error("Failed to publish event: %v", err)
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
