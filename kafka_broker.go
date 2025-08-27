package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaBroker implements Broker using Apache Kafka
type KafkaBroker struct {
	brokers       []string
	topic         string
	consumerGroup string
	nodeID        string
	writer        *kafka.Writer
	reader        *kafka.Reader
	subscription  chan<- *Event
	stopChan      chan struct{}
	subscribed    bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
}

// KafkaConfig holds configuration for the Kafka event broker
type KafkaConfig struct {
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
	ConsumerGroup string   `json:"consumer_group"`
	NodeID        string   `json:"node_id"`
}

// NewKafkaEventBroker creates a new Kafka-based event broker
func NewKafkaEventBroker(config KafkaConfig) (*KafkaBroker, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker must be specified")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic must be specified")
	}
	if config.ConsumerGroup == "" {
		return nil, fmt.Errorf("consumer group must be specified")
	}
	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID must be specified")
	}

	// Create Kafka writer
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(config.Brokers...),
		Topic:                  config.Topic,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}

	// Create Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.ConsumerGroup,
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})

	broker := &KafkaBroker{
		brokers:       config.Brokers,
		topic:         config.Topic,
		consumerGroup: config.ConsumerGroup,
		nodeID:        config.NodeID,
		writer:        writer,
		reader:        reader,
		stopChan:      make(chan struct{}),
	}

	return broker, nil
}

// Publish publishes an event to the Kafka topic
func (k *KafkaBroker) Publish(ctx context.Context, event *Event) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	// Generate a unique message key based on event type and timestamp
	messageKey := fmt.Sprintf("%s-%d", event.Type, event.Timestamp.UnixNano())

	// Serialize event to JSON
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(messageKey),
		Value: eventData,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(event.Type))},
			{Key: "source_node_id", Value: []byte(k.nodeID)},
			{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
		},
	}

	// Publish message
	err = k.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Subscribe starts listening for events and sends them to the provided channel
func (k *KafkaBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.subscribed {
		return fmt.Errorf("already subscribed")
	}

	k.subscribed = true
	k.subscription = events

	k.wg.Add(1)
	go k.eventLoop(ctx)

	return nil
}

// eventLoop runs the main event consumption loop
func (k *KafkaBroker) eventLoop(ctx context.Context) {
	defer k.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-k.stopChan:
			return
		default:
			// Read message from Kafka
			message, err := k.reader.ReadMessage(ctx)
			if err != nil {
				// Log error and continue
				fmt.Printf("Error reading Kafka message: %v\n", err)
				time.Sleep(time.Second)
				continue
			}

			// Process message
			if err := k.processMessage(ctx, message); err != nil {
				fmt.Printf("Error processing message: %v\n", err)
			}

			// Commit message
			if err := k.reader.CommitMessages(ctx, message); err != nil {
				fmt.Printf("Error committing message: %v\n", err)
			}
		}
	}
}

// processMessage processes a Kafka message and converts it to an Event
func (k *KafkaBroker) processMessage(ctx context.Context, message kafka.Message) error {
	// Deserialize event
	var event Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Filter out events from this node to avoid loops
	for _, header := range message.Headers {
		if header.Key == "source_node_id" && string(header.Value) == k.nodeID {
			// Skip events from this node
			return nil
		}
	}

	// Send event to subscription channel
	select {
	case k.subscription <- &event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-k.stopChan:
		return nil
	}
}

// Health checks the health of the Kafka connection
func (k *KafkaBroker) Health(ctx context.Context) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	// Create a test connection to check Kafka health
	conn, err := kafka.DialContext(ctx, "tcp", k.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	// Check if topic exists by fetching metadata
	_, err = conn.ReadPartitions(k.topic)
	if err != nil {
		return fmt.Errorf("failed to read topic partitions: %w", err)
	}

	return nil
}

// Close closes the Kafka event broker and cleans up resources
func (k *KafkaBroker) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.subscribed {
		close(k.stopChan)
		k.wg.Wait()
		k.subscribed = false
	}

	// Close writer
	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
	}

	// Close reader
	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			return fmt.Errorf("failed to close reader: %w", err)
		}
	}

	return nil
}

// InMemoryEventBroker implements Broker for testing and development
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
