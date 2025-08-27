package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBroker implements Broker using Redis Streams
type RedisBroker struct {
	client        *redis.Client
	consumerGroup string
	consumerName  string
	nodeID        string
	streamName    string
	subscription  chan<- *Event
	stopChan      chan struct{}
	subscribed    bool
	wg            sync.WaitGroup
	mu            sync.RWMutex
}

// RedisEventBrokerConfig holds configuration for Redis event broker
type RedisEventBrokerConfig struct {
	RedisAddr     string // Redis server address (e.g., "localhost:6379")
	Password      string // Redis password (empty if no password)
	DB            int    // Redis database number
	StreamName    string // Redis stream name for events
	ConsumerGroup string // Consumer group name
	ConsumerName  string // Consumer name within the group
	NodeID        string // Node identifier
}

// NewRedisEventBroker creates a new Redis-based event broker
func NewRedisEventBroker(config RedisEventBrokerConfig) (*RedisBroker, error) {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.Password,
		DB:       config.DB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Set defaults
	if config.StreamName == "" {
		config.StreamName = "matcher:events"
	}
	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "matcher-nodes"
	}
	if config.ConsumerName == "" {
		config.ConsumerName = fmt.Sprintf("node-%s", config.NodeID)
	}

	broker := &RedisBroker{
		client:        rdb,
		streamName:    config.StreamName,
		consumerGroup: config.ConsumerGroup,
		consumerName:  config.ConsumerName,
		nodeID:        config.NodeID,
		stopChan:      make(chan struct{}),
	}

	// Create consumer group if it doesn't exist
	if err := broker.ensureConsumerGroup(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return broker, nil
}

// ensureConsumerGroup creates the consumer group if it doesn't exist
func (r *RedisBroker) ensureConsumerGroup(ctx context.Context) error {
	// Try to create the consumer group
	err := r.client.XGroupCreate(ctx, r.streamName, r.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Publish publishes an event to the Redis stream
func (r *RedisBroker) Publish(ctx context.Context, event *Event) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Serialize event data
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Add to Redis stream
	_, err = r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.streamName,
		Values: map[string]interface{}{
			"event":      eventData,
			"node_id":    event.NodeID,
			"event_type": string(event.Type),
			"timestamp":  event.Timestamp.Unix(),
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to publish event to Redis stream: %w", err)
	}

	return nil
}

// Subscribe starts listening for events from the Redis stream
func (r *RedisBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		return fmt.Errorf("already subscribed")
	}

	r.subscribed = true
	r.subscription = events

	// Start consumer goroutine
	r.wg.Add(1)
	go r.consumeMessages(ctx)

	return nil
}

// consumeMessages runs the message consumption loop
func (r *RedisBroker) consumeMessages(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		default:
			// Read messages from the consumer group
			streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    r.consumerGroup,
				Consumer: r.consumerName,
				Streams:  []string{r.streamName, ">"},
				Count:    10,
				Block:    1 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// No messages available
					continue
				}
				// Log error but continue
				fmt.Printf("Error reading from Redis stream: %v\n", err)
				time.Sleep(time.Second)
				continue
			}

			// Process messages
			for _, stream := range streams {
				for _, message := range stream.Messages {
					if err := r.processMessage(ctx, message); err != nil {
						fmt.Printf("Error processing message %s: %v\n", message.ID, err)
					}
				}
			}
		}
	}
}

// processMessage processes a single Redis stream message
func (r *RedisBroker) processMessage(ctx context.Context, message redis.XMessage) error {
	eventData, ok := message.Values["event"].(string)
	if !ok {
		return fmt.Errorf("invalid event data in message")
	}

	// Deserialize event
	var event Event
	if err := json.Unmarshal([]byte(eventData), &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Filter out events from this node
	if event.NodeID == r.nodeID {
		// Acknowledge the message but don't process it
		r.client.XAck(ctx, r.streamName, r.consumerGroup, message.ID)
		return nil
	}

	// Send event to subscriber
	select {
	case r.subscription <- &event:
		// Message sent successfully
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopChan:
		return nil
	}

	// Acknowledge the message
	if err := r.client.XAck(ctx, r.streamName, r.consumerGroup, message.ID).Err(); err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	return nil
}

// Health checks the health of the Redis connection
func (r *RedisBroker) Health(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the Redis event broker
func (r *RedisBroker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		close(r.stopChan)
		r.wg.Wait()
		r.subscribed = false
	}

	return r.client.Close()
}
