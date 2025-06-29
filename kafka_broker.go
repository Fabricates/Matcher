package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// KafkaEventBroker implements EventBrokerInterface using Kafka
// This is a simplified example - in production you would use actual Kafka clients
type KafkaEventBroker struct {
	brokers       []string
	topic         string
	consumerGroup string
	nodeID        string

	// Simplified mock implementation
	eventChan   chan *Event
	subscribers []chan<- *Event
	mu          sync.RWMutex
	closed      bool
}

// NewKafkaEventBroker creates a new Kafka-based event broker
func NewKafkaEventBroker(brokers []string, topic string, consumerGroup string, nodeID string) *KafkaEventBroker {
	return &KafkaEventBroker{
		brokers:       brokers,
		topic:         topic,
		consumerGroup: consumerGroup,
		nodeID:        nodeID,
		eventChan:     make(chan *Event, 1000),
		subscribers:   make([]chan<- *Event, 0),
	}
}

// Publish publishes an event to the Kafka topic
func (kb *KafkaEventBroker) Publish(ctx context.Context, event *Event) error {
	kb.mu.RLock()
	defer kb.mu.RUnlock()

	if kb.closed {
		return fmt.Errorf("broker is closed")
	}

	// In a real implementation, you would serialize and send to Kafka
	// For this example, we'll just forward to local subscribers

	// Serialize event for logging/debugging
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	fmt.Printf("[KAFKA] Publishing event to topic %s: %s\n", kb.topic, string(eventData))

	// Simulate publishing to all subscribers
	select {
	case kb.eventChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("event channel is full")
	}
}

// Subscribe starts listening for events from the Kafka topic
func (kb *KafkaEventBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	if kb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Add this channel to our subscribers list
	kb.subscribers = append(kb.subscribers, events)

	// Start forwarding events from our internal channel to subscribers
	go kb.forwardEvents(ctx)

	fmt.Printf("[KAFKA] Subscribed to topic %s with consumer group %s\n", kb.topic, kb.consumerGroup)

	return nil
}

// forwardEvents forwards events from the internal channel to all subscribers
func (kb *KafkaEventBroker) forwardEvents(ctx context.Context) {
	for {
		select {
		case event := <-kb.eventChan:
			kb.mu.RLock()
			for _, subscriber := range kb.subscribers {
				select {
				case subscriber <- event:
					// Event forwarded successfully
				default:
					// Subscriber channel is full, skip
					fmt.Printf("[KAFKA] Warning: subscriber channel full, dropping event\n")
				}
			}
			kb.mu.RUnlock()

		case <-ctx.Done():
			return
		}
	}
}

// Health checks if the Kafka broker connection is healthy
func (kb *KafkaEventBroker) Health(ctx context.Context) error {
	kb.mu.RLock()
	defer kb.mu.RUnlock()

	if kb.closed {
		return fmt.Errorf("broker is closed")
	}

	// In a real implementation, you would check Kafka broker connectivity
	// For this example, we'll just return nil
	return nil
}

// Close closes the Kafka broker connections
func (kb *KafkaEventBroker) Close() error {
	kb.mu.Lock()
	defer kb.mu.Unlock()

	if kb.closed {
		return nil
	}

	kb.closed = true
	close(kb.eventChan)

	// Clear subscribers
	kb.subscribers = nil

	fmt.Printf("[KAFKA] Closed broker connections\n")

	return nil
}

// InMemoryEventBroker implements EventBrokerInterface for testing and development
type InMemoryEventBroker struct {
	events      []Event
	subscribers []chan<- *Event
	mu          sync.RWMutex
	nodeID      string
	closed      bool
}

// NewInMemoryEventBroker creates a new in-memory event broker for testing
func NewInMemoryEventBroker(nodeID string) *InMemoryEventBroker {
	return &InMemoryEventBroker{
		events:      make([]Event, 0),
		subscribers: make([]chan<- *Event, 0),
		nodeID:      nodeID,
	}
}

// Publish publishes an event to in-memory storage
func (mb *InMemoryEventBroker) Publish(ctx context.Context, event *Event) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Store event
	mb.events = append(mb.events, *event)

	// Forward to all subscribers
	for _, subscriber := range mb.subscribers {
		select {
		case subscriber <- event:
			// Event forwarded successfully
		default:
			// Subscriber channel is full, skip
			fmt.Printf("[MEMORY] Warning: subscriber channel full, dropping event\n")
		}
	}

	fmt.Printf("[MEMORY] Published event: %s from node %s\n", event.Type, event.NodeID)

	return nil
}

// Subscribe starts listening for events
func (mb *InMemoryEventBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Add subscriber
	mb.subscribers = append(mb.subscribers, events)

	fmt.Printf("[MEMORY] Added subscriber\n")

	return nil
}

// Health checks if the in-memory broker is healthy
func (mb *InMemoryEventBroker) Health(ctx context.Context) error {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	if mb.closed {
		return fmt.Errorf("broker is closed")
	}

	return nil
}

// Close closes the in-memory broker
func (mb *InMemoryEventBroker) Close() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.closed {
		return nil
	}

	mb.closed = true
	mb.subscribers = nil

	fmt.Printf("[MEMORY] Closed in-memory broker\n")

	return nil
}

// GetStoredEvents returns all stored events (for testing)
func (mb *InMemoryEventBroker) GetStoredEvents() []Event {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	events := make([]Event, len(mb.events))
	copy(events, mb.events)
	return events
}

// GetSubscriberCount returns the number of active subscribers
func (mb *InMemoryEventBroker) GetSubscriberCount() int {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	return len(mb.subscribers)
}

// GetEventCount returns the total number of events processed
func (mb *InMemoryEventBroker) GetEventCount() int {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	return len(mb.events)
}
