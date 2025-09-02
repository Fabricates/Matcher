package matcher

import (
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
