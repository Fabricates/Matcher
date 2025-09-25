package matcher

import (
	"context"
	"testing"
)

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

func TestMockEventSubscriberBranches(t *testing.T) {
	subscriber := NewMockEventSubscriber()
	defer subscriber.Close()

	// Test health check
	ctx := context.Background()
	if err := subscriber.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	// Test publishing to trigger different branches in Publish method
	events := []*Event{
		{Type: EventTypeRuleAdded, NodeID: "test-node"},
		{Type: EventTypeRuleUpdated, NodeID: "test-node"},
		{Type: EventTypeRuleDeleted, NodeID: "test-node"},
		{Type: EventTypeDimensionAdded, NodeID: "test-node"},
		{Type: EventTypeDimensionUpdated, NodeID: "test-node"},
		{Type: EventTypeDimensionDeleted, NodeID: "test-node"},
	}

	for _, event := range events {
		err := subscriber.Publish(ctx, event)
		if err != nil {
			t.Errorf("Failed to publish event %s: %v", event.Type, err)
		}
	}
}
