package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCASBroker implements Broker using Redis with Compare-And-Swap operations
// It uses a fixed event key for CAS operations - no initialization, only latest event
type RedisCASBroker struct {
	client       *redis.Client
	nodeID       string
	eventKey     string // Fixed Redis key for CAS operations
	subscription chan<- *Event
	stopChan     chan struct{}
	subscribed   bool
	wg           sync.WaitGroup
	mu           sync.RWMutex

	// CAS polling configuration
	pollInterval  time.Duration // How often to poll for changes (1-5s)
	lastTimestamp int64         // Last known event timestamp
	timestampMu   sync.RWMutex
}

// RedisCASConfig holds configuration for Redis CAS broker
type RedisCASConfig struct {
	RedisAddr    string        // Redis server address (e.g., "localhost:6379")
	Password     string        // Redis password (empty if no password)
	DB           int           // Redis database number
	NodeID       string        // Node identifier
	Namespace    string        // Namespace for keys (optional, defaults to "matcher")
	PollInterval time.Duration // How often to poll for changes (defaults to 2s, should be 1-5s)
}

// LatestEvent represents the latest event stored in Redis (single event only)
type LatestEvent struct {
	Timestamp int64  `json:"timestamp"` // Unix timestamp in nanoseconds for ordering
	NodeID    string `json:"node_id"`   // Node that published this event
	Event     *Event `json:"event"`     // The actual event
}

// NewRedisCASBroker creates a new Redis CAS-based broker
// Does NOT initialize any default values in Redis
func NewRedisCASBroker(config RedisCASConfig) (*RedisCASBroker, error) {
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
	namespace := config.Namespace
	if namespace == "" {
		namespace = "matcher"
	}

	pollInterval := config.PollInterval
	if pollInterval == 0 {
		pollInterval = 2 * time.Second // Default to 2 seconds
	}
	if pollInterval < 1*time.Second || pollInterval > 5*time.Second {
		return nil, fmt.Errorf("poll interval must be between 1-5 seconds, got %v", pollInterval)
	}

	broker := &RedisCASBroker{
		client:       rdb,
		nodeID:       config.NodeID,
		eventKey:     fmt.Sprintf("%s:events", namespace),
		stopChan:     make(chan struct{}),
		pollInterval: pollInterval,
	}

	// Load current state if key exists (don't create if it doesn't exist)
	_ = broker.loadCurrentState(context.Background())

	return broker, nil
}

// loadCurrentState loads the current event state if it exists
func (r *RedisCASBroker) loadCurrentState(ctx context.Context) error {
	r.timestampMu.Lock()
	defer r.timestampMu.Unlock()

	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Key doesn't exist, that's fine - no previous events
			r.lastTimestamp = 0
			return nil
		}
		return err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		// If we can't unmarshal, treat as no previous events
		r.lastTimestamp = 0
		return nil
	}

	r.lastTimestamp = latestEvent.Timestamp
	return nil
}

// Publish publishes an event using CAS operation
func (r *RedisCASBroker) Publish(ctx context.Context, event *Event) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Retry CAS operation up to 3 times
	for retries := 0; retries < 3; retries++ {
		success, err := r.publishWithCAS(ctx, event)
		if err != nil {
			return err
		}
		if success {
			return nil
		}

		// Brief backoff before retry
		time.Sleep(time.Duration(retries+1) * 10 * time.Millisecond)
	}

	return fmt.Errorf("failed to publish event after retries")
}

// publishWithCAS performs a single CAS publish operation
func (r *RedisCASBroker) publishWithCAS(ctx context.Context, event *Event) (bool, error) {
	// Create new event entry
	now := time.Now().UnixNano()
	newEvent := &LatestEvent{
		Timestamp: now,
		NodeID:    r.nodeID,
		Event:     event,
	}

	newEventData, err := json.Marshal(newEvent)
	if err != nil {
		return false, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Use CAS operation to update the key
	// This will set the key regardless of previous value, but we use WATCH for conflict detection
	err = r.client.Watch(ctx, func(tx *redis.Tx) error {
		// Get current value to detect changes (but don't fail if key doesn't exist)
		currentData, err := tx.Get(ctx, r.eventKey).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		// If key exists, check if it changed during our operation
		if err != redis.Nil {
			var currentEvent LatestEvent
			if json.Unmarshal([]byte(currentData), &currentEvent) == nil {
				// Key exists and is valid - this is normal for CAS
			}
		}

		// Set the new event atomically
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, r.eventKey, newEventData, 0)
			return nil
		})

		return err
	}, r.eventKey)

	if err != nil {
		// Check if it's a watch error (concurrent modification)
		if err.Error() == "redis: transaction failed" {
			return false, nil // Indicate retry needed
		}
		return false, err
	}

	return true, nil
}

// Subscribe starts listening for events by polling the Redis key
func (r *RedisCASBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		return fmt.Errorf("already subscribed")
	}

	r.subscribed = true
	r.subscription = events

	// Start polling goroutine
	r.wg.Add(1)
	go r.pollForEvents(ctx)

	return nil
}

// pollForEvents runs the event polling loop
func (r *RedisCASBroker) pollForEvents(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			if err := r.checkForNewEvents(ctx); err != nil {
				// Log error but continue
				fmt.Printf("Error checking for new events: %v\n", err)
			}
		}
	}
}

// checkForNewEvents checks for new events since last known timestamp
func (r *RedisCASBroker) checkForNewEvents(ctx context.Context) error {
	// Get current event from Redis
	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil // No events yet
		}
		return err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		return fmt.Errorf("failed to unmarshal latest event: %w", err)
	}

	r.timestampMu.Lock()
	defer r.timestampMu.Unlock()

	// Check if this is a new event
	if latestEvent.Timestamp <= r.lastTimestamp {
		return nil // No new events
	}

	// Skip events from this node
	if latestEvent.NodeID == r.nodeID {
		// Update timestamp but don't process the event
		r.lastTimestamp = latestEvent.Timestamp
		return nil
	}

	// Send event to subscriber
	select {
	case r.subscription <- latestEvent.Event:
		// Event sent successfully
		r.lastTimestamp = latestEvent.Timestamp
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopChan:
		return nil
	default:
		// Channel is full, skip this event but update timestamp
		r.lastTimestamp = latestEvent.Timestamp
	}

	return nil
}

// Health checks the health of the Redis connection
func (r *RedisCASBroker) Health(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the Redis CAS broker
func (r *RedisCASBroker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		close(r.stopChan)
		r.wg.Wait()
		r.subscribed = false
	}

	return r.client.Close()
}

// GetLastTimestamp returns the last known event timestamp
func (r *RedisCASBroker) GetLastTimestamp() int64 {
	r.timestampMu.RLock()
	defer r.timestampMu.RUnlock()
	return r.lastTimestamp
}

// WaitForTimestamp waits for events to reach at least the specified timestamp
func (r *RedisCASBroker) WaitForTimestamp(ctx context.Context, targetTimestamp int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		r.timestampMu.RLock()
		currentTs := r.lastTimestamp
		r.timestampMu.RUnlock()

		if currentTs >= targetTimestamp {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue polling - check for new events manually
			if err := r.checkForNewEvents(ctx); err != nil {
				return fmt.Errorf("error checking for events: %w", err)
			}
		}
	}

	return fmt.Errorf("timeout waiting for timestamp %d", targetTimestamp)
}

// GetLatestEvent returns the current latest event from Redis (for debugging/testing)
func (r *RedisCASBroker) GetLatestEvent(ctx context.Context) (*LatestEvent, error) {
	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No event exists
		}
		return nil, err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		return nil, fmt.Errorf("failed to unmarshal latest event: %w", err)
	}

	return &latestEvent, nil
}
