package matcher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisEventBroker implements EventBrokerInterface using Redis Streams
type RedisEventBroker struct {
	client        *redis.Client
	streamKey     string
	consumerGroup string
	consumerName  string
	nodeID        string

	// Internal state
	subscribers []chan<- *Event
	mu          sync.RWMutex
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewRedisEventBroker creates a new Redis-based event broker
func NewRedisEventBroker(redisAddr, password string, db int, streamKey, consumerGroup, nodeID string) *RedisEventBroker {
	ctx, cancel := context.WithCancel(context.Background())

	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: password,
		DB:       db,
	})

	return &RedisEventBroker{
		client:        client,
		streamKey:     streamKey,
		consumerGroup: consumerGroup,
		consumerName:  fmt.Sprintf("%s-%s", nodeID, generateShortID()),
		nodeID:        nodeID,
		subscribers:   make([]chan<- *Event, 0),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// generateShortID generates a short unique ID for idempotence
func generateShortID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// Publish publishes an event to the Redis stream
func (rb *RedisEventBroker) Publish(ctx context.Context, event *Event) error {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Ensure event has a unique ID for idempotence
	if event.ID == "" {
		event.ID = generateEventID()
	}

	// Serialize event data
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// Publish to Redis stream using XADD
	args := &redis.XAddArgs{
		Stream: rb.streamKey,
		Values: map[string]interface{}{
			"event_id":   event.ID,
			"event_type": string(event.Type),
			"node_id":    event.NodeID,
			"timestamp":  event.Timestamp.Unix(),
			"data":       string(eventData),
		},
	}

	streamID, err := rb.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish event to Redis stream: %w", err)
	}

	fmt.Printf("[REDIS] Published event %s to stream %s with ID %s\n", event.ID, rb.streamKey, streamID)

	return nil
}

// Subscribe starts listening for events from the Redis stream
func (rb *RedisEventBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Add subscriber
	rb.subscribers = append(rb.subscribers, events)

	// Create consumer group if it doesn't exist
	err := rb.client.XGroupCreateMkStream(ctx, rb.streamKey, rb.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Start consuming events in a goroutine
	go rb.consumeEvents(ctx)

	fmt.Printf("[REDIS] Subscribed to stream %s with consumer group %s\n", rb.streamKey, rb.consumerGroup)

	return nil
}

// consumeEvents continuously reads events from the Redis stream
func (rb *RedisEventBroker) consumeEvents(ctx context.Context) {
	processedEvents := make(map[string]bool) // For idempotence tracking

	for {
		select {
		case <-ctx.Done():
			return
		case <-rb.ctx.Done():
			return
		default:
			// Read from consumer group
			streams, err := rb.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    rb.consumerGroup,
				Consumer: rb.consumerName,
				Streams:  []string{rb.streamKey, ">"},
				Count:    10,
				Block:    1 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					fmt.Printf("[REDIS] Error reading from stream: %v\n", err)
				}
				continue
			}

			// Process messages
			for _, stream := range streams {
				for _, message := range stream.Messages {
					if err := rb.processMessage(ctx, message, processedEvents); err != nil {
						fmt.Printf("[REDIS] Error processing message %s: %v\n", message.ID, err)
						continue
					}

					// Acknowledge the message
					if err := rb.client.XAck(ctx, rb.streamKey, rb.consumerGroup, message.ID).Err(); err != nil {
						fmt.Printf("[REDIS] Error acknowledging message %s: %v\n", message.ID, err)
					}
				}
			}
		}
	}
}

// processMessage processes a single message from the stream
func (rb *RedisEventBroker) processMessage(ctx context.Context, message redis.XMessage, processedEvents map[string]bool) error {
	// Extract event data
	eventDataStr, ok := message.Values["data"].(string)
	if !ok {
		return fmt.Errorf("invalid event data format")
	}

	eventID, ok := message.Values["event_id"].(string)
	if !ok {
		return fmt.Errorf("missing event ID")
	}

	// Check for idempotence - skip if already processed
	if processedEvents[eventID] {
		fmt.Printf("[REDIS] Skipping already processed event %s\n", eventID)
		return nil
	}

	// Deserialize event
	var event Event
	if err := json.Unmarshal([]byte(eventDataStr), &event); err != nil {
		return fmt.Errorf("failed to deserialize event: %w", err)
	}

	// Skip events from the same node to avoid processing our own events
	if event.NodeID == rb.nodeID {
		processedEvents[eventID] = true
		return nil
	}

	// Forward to all subscribers
	rb.mu.RLock()
	for _, subscriber := range rb.subscribers {
		select {
		case subscriber <- &event:
			// Event forwarded successfully
		default:
			// Subscriber channel is full, skip
			fmt.Printf("[REDIS] Warning: subscriber channel full, dropping event %s\n", eventID)
		}
	}
	rb.mu.RUnlock()

	// Mark as processed for idempotence
	processedEvents[eventID] = true

	fmt.Printf("[REDIS] Processed event %s from node %s\n", eventID, event.NodeID)

	return nil
}

// Health checks if the Redis broker connection is healthy
func (rb *RedisEventBroker) Health(ctx context.Context) error {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.closed {
		return fmt.Errorf("broker is closed")
	}

	// Ping Redis to check connectivity
	if err := rb.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis connection unhealthy: %w", err)
	}

	return nil
}

// Close closes the Redis broker connections
func (rb *RedisEventBroker) Close() error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return nil
	}

	rb.closed = true
	
	// Cancel context to stop consumers
	rb.cancel()

	// Clear subscribers
	rb.subscribers = nil

	// Close Redis client
	if err := rb.client.Close(); err != nil {
		return fmt.Errorf("failed to close Redis client: %w", err)
	}

	fmt.Printf("[REDIS] Closed broker connections\n")

	return nil
}

// GetStreamInfo returns information about the Redis stream (for debugging/monitoring)
func (rb *RedisEventBroker) GetStreamInfo(ctx context.Context) (map[string]interface{}, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.closed {
		return nil, fmt.Errorf("broker is closed")
	}

	info, err := rb.client.XInfoStream(ctx, rb.streamKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	return map[string]interface{}{
		"stream_key":     rb.streamKey,
		"consumer_group": rb.consumerGroup,
		"consumer_name":  rb.consumerName,
		"length":         info.Length,
		"groups":         info.Groups,
		"last_entry_id": info.LastGeneratedID,
	}, nil
}