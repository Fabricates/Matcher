package matcher

import (
	"context"
	"testing"
)

func TestInMemoryBrokerPublishVariations(t *testing.T) {
	broker := NewInMemoryEventBroker("test-node")
	defer broker.Close()

	// Test publishing different event types to cover more branches
	events := []*Event{
		{Type: EventTypeRuleAdded, NodeID: "test-node"},
		{Type: EventTypeRuleUpdated, NodeID: "test-node"},
		{Type: EventTypeRuleDeleted, NodeID: "test-node"},
		{Type: EventTypeDimensionAdded, NodeID: "test-node"},
		{Type: EventTypeDimensionUpdated, NodeID: "test-node"},
		{Type: EventTypeDimensionDeleted, NodeID: "test-node"},
	}

	// Add a subscriber channel
	eventChan := make(chan *Event, 10)
	broker.Subscribe(context.Background(), eventChan)

	// Publish various events
	for _, event := range events {
		err := broker.Publish(context.Background(), event)
		if err != nil {
			t.Errorf("Failed to publish event %s: %v", event.Type, err)
		}
	}

	// Consume events from channel
	eventsReceived := 0
	for eventsReceived < len(events) {
		select {
		case <-eventChan:
			// Event received
			eventsReceived++
		default:
			// No more events available
		}
	}
	close(eventChan)
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
