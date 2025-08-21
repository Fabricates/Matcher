package matcher

import (
	"context"
	"testing"
	"time"
)

// TestRedisEventBrokerCreation tests basic Redis broker creation and configuration
func TestRedisEventBrokerCreation(t *testing.T) {
	broker := NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	
	if broker == nil {
		t.Fatal("Failed to create Redis broker")
	}
	
	if broker.streamKey != "test-stream" {
		t.Errorf("Expected stream key 'test-stream', got '%s'", broker.streamKey)
	}
	
	if broker.consumerGroup != "test-group" {
		t.Errorf("Expected consumer group 'test-group', got '%s'", broker.consumerGroup)
	}
	
	if broker.nodeID != "test-node" {
		t.Errorf("Expected node ID 'test-node', got '%s'", broker.nodeID)
	}
}

// TestEventIdGeneration tests that event IDs are generated correctly
func TestEventIdGeneration(t *testing.T) {
	id1 := generateEventID()
	id2 := generateEventID()
	
	if id1 == "" {
		t.Error("Generated event ID should not be empty")
	}
	
	if id1 == id2 {
		t.Error("Generated event IDs should be unique")
	}
	
	// Event ID should have timestamp-random format
	if len(id1) < 10 {
		t.Error("Event ID should have sufficient length")
	}
}

// TestRedisEventBrokerInterface tests that Redis broker implements EventBrokerInterface
func TestRedisEventBrokerInterface(t *testing.T) {
	var broker EventBrokerInterface = NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	
	// Test Health method (will fail if Redis is not available, but interface should work)
	ctx := context.Background()
	_ = broker.Health(ctx) // Just test the interface
	
	// Test Close method
	err := broker.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}

// TestEventWithID tests that events can be created with IDs
func TestEventWithID(t *testing.T) {
	rule := NewRule("test_rule").
		Dimension("product", "TestProduct", MatchTypeEqual, 10.0).
		Build()

	event := &Event{
		ID:        generateEventID(),
		Type:      EventTypeRuleAdded,
		Timestamp: time.Now(),
		NodeID:    "test-node",
		Data: &RuleEvent{
			Rule: rule,
		},
	}

	if event.ID == "" {
		t.Error("Event should have an ID")
	}

	if event.Type != EventTypeRuleAdded {
		t.Errorf("Expected event type %s, got %s", EventTypeRuleAdded, event.Type)
	}

	if event.NodeID != "test-node" {
		t.Errorf("Expected node ID 'test-node', got '%s'", event.NodeID)
	}
}

// TestRedisEventBrokerHealthClosed tests health check on closed broker
func TestRedisEventBrokerHealthClosed(t *testing.T) {
	broker := NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	
	// Close the broker
	err := broker.Close()
	if err != nil {
		t.Fatalf("Failed to close broker: %v", err)
	}
	
	// Health check should fail on closed broker
	ctx := context.Background()
	err = broker.Health(ctx)
	if err == nil {
		t.Error("Health check should fail on closed broker")
	}
	
	if err.Error() != "broker is closed" {
		t.Errorf("Expected 'broker is closed' error, got: %v", err)
	}
}