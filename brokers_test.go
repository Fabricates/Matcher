package matcher

import (
	"context"
	"testing"
	"time"
)

func TestInMemoryEventBroker(t *testing.T) {
	broker := NewInMemoryEventBroker("test-node")
	defer broker.Close()

	// Test health check
	if err := broker.Health(context.Background()); err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	// Test subscriber count
	if count := broker.GetSubscriberCount(); count != 0 {
		t.Errorf("Expected 0 subscribers, got %d", count)
	}

	// Test event count
	if count := broker.GetEventCount(); count != 0 {
		t.Errorf("Expected 0 events, got %d", count)
	}

	// Test subscription
	events := make(chan *Event, 10)
	ctx := context.Background()

	if err := broker.Subscribe(ctx, events); err != nil {
		t.Errorf("Failed to subscribe: %v", err)
	}

	// Verify subscriber count
	if count := broker.GetSubscriberCount(); count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}

	// Test publishing
	testEvent := &Event{
		Type:      EventTypeRuleAdded,
		Timestamp: time.Now(),
		NodeID:    "test-node",
		Data:      "test data",
	}

	if err := broker.Publish(ctx, testEvent); err != nil {
		t.Errorf("Failed to publish event: %v", err)
	}

	// Verify event count
	if count := broker.GetEventCount(); count != 1 {
		t.Errorf("Expected 1 event, got %d", count)
	}

	// Verify event was received
	select {
	case receivedEvent := <-events:
		if receivedEvent.Type != testEvent.Type {
			t.Errorf("Expected event type %s, got %s", testEvent.Type, receivedEvent.Type)
		}
		if receivedEvent.NodeID != testEvent.NodeID {
			t.Errorf("Expected node ID %s, got %s", testEvent.NodeID, receivedEvent.NodeID)
		}
		if receivedEvent.Data != testEvent.Data {
			t.Errorf("Expected data %v, got %v", testEvent.Data, receivedEvent.Data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Did not receive event within timeout")
	}

	// Test getting stored events
	storedEvents := broker.GetStoredEvents()
	if len(storedEvents) != 1 {
		t.Errorf("Expected 1 stored event, got %d", len(storedEvents))
	}

	// Test multiple events
	for i := 0; i < 5; i++ {
		event := &Event{
			Type:      EventTypeDimensionAdded,
			Timestamp: time.Now(),
			NodeID:    "test-node",
			Data:      i,
		}
		if err := broker.Publish(ctx, event); err != nil {
			t.Errorf("Failed to publish event %d: %v", i, err)
		}
	}

	// Verify total event count
	if count := broker.GetEventCount(); count != 6 {
		t.Errorf("Expected 6 events, got %d", count)
	}

	// Test close
	if err := broker.Close(); err != nil {
		t.Errorf("Failed to close broker: %v", err)
	}

	// Test operations after close
	if err := broker.Health(ctx); err == nil {
		t.Error("Expected health check to fail after close")
	}

	if err := broker.Subscribe(ctx, events); err == nil {
		t.Error("Expected subscribe to fail after close")
	}

	if err := broker.Publish(ctx, testEvent); err == nil {
		t.Error("Expected publish to fail after close")
	}
}

func TestInMemoryEventBrokerConcurrency(t *testing.T) {
	broker := NewInMemoryEventBroker("test-node")
	defer broker.Close()

	ctx := context.Background()
	numSubscribers := 3
	numEvents := 10

	// Create multiple subscribers
	subscribers := make([]chan *Event, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subscribers[i] = make(chan *Event, numEvents)
		if err := broker.Subscribe(ctx, subscribers[i]); err != nil {
			t.Errorf("Failed to subscribe %d: %v", i, err)
		}
	}

	// Verify subscriber count
	if count := broker.GetSubscriberCount(); count != numSubscribers {
		t.Errorf("Expected %d subscribers, got %d", numSubscribers, count)
	}

	// Publish events concurrently
	done := make(chan bool)
	go func() {
		for i := 0; i < numEvents; i++ {
			event := &Event{
				Type:      EventTypeRuleUpdated,
				Timestamp: time.Now(),
				NodeID:    "test-node",
				Data:      i,
			}
			if err := broker.Publish(ctx, event); err != nil {
				t.Errorf("Failed to publish event %d: %v", i, err)
			}
		}
		done <- true
	}()

	// Wait for publishing to complete
	<-done

	// Verify each subscriber received all events
	for i, subscriber := range subscribers {
		eventCount := 0
		timeout := time.After(500 * time.Millisecond)

		for eventCount < numEvents {
			select {
			case <-subscriber:
				eventCount++
			case <-timeout:
				t.Errorf("Subscriber %d only received %d/%d events", i, eventCount, numEvents)
			}
		}
	}
}

func TestKafkaEventBrokerCreation(t *testing.T) {
	// Test invalid configurations
	testCases := []struct {
		name        string
		config      KafkaConfig
		expectError bool
	}{
		{
			name: "Empty brokers",
			config: KafkaConfig{
				Brokers:       []string{},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty topic",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "",
				ConsumerGroup: "test-group",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty consumer group",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty node ID",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				NodeID:        "",
			},
			expectError: true,
		},
		{
			name: "Valid config",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				NodeID:        "test-node",
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			broker, err := NewKafkaEventBroker(tc.config)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if broker != nil {
				broker.Close()
			}
		})
	}
}

func TestRedisEventBrokerCreation(t *testing.T) {
	// Test invalid configurations
	testCases := []struct {
		name        string
		config      RedisEventBrokerConfig
		expectError bool
	}{
		{
			name: "Empty redis address",
			config: RedisEventBrokerConfig{
				RedisAddr:     "",
				StreamName:    "test-stream",
				ConsumerGroup: "test-group",
				ConsumerName:  "test-consumer",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty stream name",
			config: RedisEventBrokerConfig{
				RedisAddr:     "localhost:6379",
				StreamName:    "",
				ConsumerGroup: "test-group",
				ConsumerName:  "test-consumer",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty consumer group",
			config: RedisEventBrokerConfig{
				RedisAddr:     "localhost:6379",
				StreamName:    "test-stream",
				ConsumerGroup: "",
				ConsumerName:  "test-consumer",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty consumer name",
			config: RedisEventBrokerConfig{
				RedisAddr:     "localhost:6379",
				StreamName:    "test-stream",
				ConsumerGroup: "test-group",
				ConsumerName:  "",
				NodeID:        "test-node",
			},
			expectError: true,
		},
		{
			name: "Empty node ID",
			config: RedisEventBrokerConfig{
				RedisAddr:     "localhost:6379",
				StreamName:    "test-stream",
				ConsumerGroup: "test-group",
				ConsumerName:  "test-consumer",
				NodeID:        "",
			},
			expectError: true,
		},
		{
			name: "Valid config but no Redis server",
			config: RedisEventBrokerConfig{
				RedisAddr:     "localhost:6379",
				StreamName:    "test-stream",
				ConsumerGroup: "test-group",
				ConsumerName:  "test-consumer",
				NodeID:        "test-node",
			},
			expectError: true, // Will fail to connect
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			broker, err := NewRedisEventBroker(tc.config)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}

			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if broker != nil {
				broker.Close()
			}
		})
	}
}

// Test event broker interface compliance
func TestBroker(t *testing.T) {
	// Test that our brokers implement the interface
	var _ Broker = NewInMemoryEventBroker("test")

	// Test with valid configs (will fail to connect but that's expected)
	kafkaConfig := KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		ConsumerGroup: "test-group",
		NodeID:        "test-node",
	}

	if kafkaBroker, err := NewKafkaEventBroker(kafkaConfig); err == nil {
		var _ Broker = kafkaBroker
		kafkaBroker.Close()
	}

	redisConfig := RedisEventBrokerConfig{
		RedisAddr:     "localhost:9999", // Use invalid port to test creation
		StreamName:    "test-stream",
		ConsumerGroup: "test-group",
		ConsumerName:  "test-consumer",
		NodeID:        "test-node",
	}

	// Redis broker creation should fail due to invalid address
	if redisBroker, err := NewRedisEventBroker(redisConfig); err == nil {
		var _ Broker = redisBroker
		redisBroker.Close()
	}

	// Test Redis CAS broker interface compliance
	redisCASConfig := RedisCASConfig{
		RedisAddr:    "localhost:9999", // Use invalid port to test creation
		NodeID:       "test-node",
		Namespace:    "test",
		PollInterval: 2 * time.Second,
	}

	// Redis CAS broker creation should fail due to invalid address
	if redisCASBroker, err := NewRedisCASBroker(redisCASConfig); err == nil {
		var _ Broker = redisCASBroker
		redisCASBroker.Close()
	}
}
