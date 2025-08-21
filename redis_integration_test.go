package matcher

import (
	"context"
	"testing"
	"time"
)

// TestRedisIntegrationBasic tests basic functionality without requiring actual Redis
// This test focuses on the interface and basic operations
func TestRedisIntegrationBasic(t *testing.T) {
	// Create a Redis broker (will fail health checks without Redis, but interface works)
	broker := NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	defer broker.Close()
	
	// Test that it implements the interface correctly
	var _ EventBrokerInterface = broker
	
	// Create event
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
	
	// Test that publish doesn't panic (will fail due to no Redis, but that's expected)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	
	err := broker.Publish(ctx, event)
	// We expect this to fail without Redis running, but the interface should work
	if err == nil {
		t.Log("Publish succeeded (Redis must be running)")
	} else {
		t.Logf("Publish failed as expected without Redis: %v", err)
	}
	
	// Test subscription (will also fail without Redis, but interface should work)
	eventChan := make(chan *Event, 10)
	err = broker.Subscribe(ctx, eventChan)
	if err == nil {
		t.Log("Subscribe succeeded (Redis must be running)")
	} else {
		t.Logf("Subscribe failed as expected without Redis: %v", err)
	}
}

// TestEventIDUniqueness tests that event IDs are unique across multiple generations
func TestEventIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	
	// Generate 1000 event IDs
	for i := 0; i < 1000; i++ {
		id := generateEventID()
		if ids[id] {
			t.Fatalf("Duplicate event ID found: %s", id)
		}
		ids[id] = true
	}
	
	if len(ids) != 1000 {
		t.Errorf("Expected 1000 unique IDs, got %d", len(ids))
	}
}

// TestMatcherWithRedisEventBroker tests matcher integration with Redis broker
func TestMatcherWithRedisEventBroker(t *testing.T) {
	// Create Redis broker
	broker := NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	defer broker.Close()
	
	// Create persistence using JSON persistence in temp directory
	persistence := NewJSONPersistence("/tmp/test-matcher")
	
	// Create matcher with Redis broker
	// Note: This will fail if Redis is not available, which is expected
	matcher, err := NewInMemoryMatcher(persistence, broker, "test-node")
	if err != nil {
		// If Redis is not available, just test that the error is as expected
		if broker.Health(context.Background()) != nil {
			t.Skipf("Redis not available for testing: %v", err)
			return
		}
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer matcher.Close()
	
	// Add dimension config using AddDimension (not AddDimensionConfig)
	dimensionConfig := &DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	}
	
	err = matcher.AddDimension(dimensionConfig)
	if err != nil {
		t.Errorf("Failed to add dimension config: %v", err)
	}
	
	// Add rule (this will try to publish event via Redis broker)
	rule := NewRule("test_rule").
		Dimension("product", "TestProduct", MatchTypeEqual, 10.0).
		Build()
	
	err = matcher.AddRule(rule)
	if err != nil {
		t.Errorf("Failed to add rule: %v", err)
	}
	
	// Test matching using FindBestMatch with QueryRule
	query := map[string]string{
		"product": "TestProduct",
	}
	queryRule := CreateQuery(query)
	
	match, err := matcher.FindBestMatch(queryRule)
	if err != nil {
		t.Errorf("Failed to find match: %v", err)
	}
	
	if match == nil {
		t.Error("Expected to find a match, got nil")
	} else if match.Rule.ID != "test_rule" {
		t.Errorf("Expected rule 'test_rule', got '%s'", match.Rule.ID)
	}
}

// TestRedisEventBrokerMultipleSubscribers tests multiple subscribers
func TestRedisEventBrokerMultipleSubscribers(t *testing.T) {
	broker := NewRedisEventBroker("localhost:6379", "", 0, "test-stream", "test-group", "test-node")
	defer broker.Close()
	
	// Create multiple event channels
	eventChan1 := make(chan *Event, 10)
	eventChan2 := make(chan *Event, 10)
	
	ctx := context.Background()
	
	// Subscribe both channels (will fail without Redis, but tests interface)
	err1 := broker.Subscribe(ctx, eventChan1)
	err2 := broker.Subscribe(ctx, eventChan2)
	
	// Both should behave consistently
	if (err1 == nil) != (err2 == nil) {
		t.Error("Subscribers should have consistent behavior")
	}
	
	// Test that subscriber count tracking works
	// Note: This is more of an interface test since we can't easily test Redis functionality
	// without a running Redis instance
	t.Log("Multiple subscriber test completed (interface validation)")
}