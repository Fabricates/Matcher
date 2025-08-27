package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions

func getRedisAddr() string {
	// Can be overridden with environment variable for CI
	if addr := os.Getenv("REDIS_TEST_ADDR"); addr != "" {
		return addr
	}
	addr := "localhost:6379"
	return addr
}

func isRedisAvailable(addr string) bool {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return client.Ping(ctx).Err() == nil
}

func cleanupRedisKey(t *testing.T, redisAddr, namespace string) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	defer client.Close()

	ctx := context.Background()

	ns := namespace
	if ns == "" {
		ns = "matcher"
	}

	key := fmt.Sprintf("%s:events", ns)
	client.Del(ctx, key)
}

// TestRedisCASBroker_NewRedisCASBroker tests broker creation
func TestRedisCASBroker_NewRedisCASBroker(t *testing.T) {
	// Skip if Redis is not available
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	tests := []struct {
		name        string
		config      RedisCASConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config",
			config: RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "test-node",
				PollInterval: 2 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "invalid poll interval - too short",
			config: RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "test-node",
				PollInterval: 500 * time.Millisecond,
			},
			wantErr:     true,
			errContains: "poll interval must be between 1-5 seconds",
		},
		{
			name: "invalid poll interval - too long",
			config: RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "test-node",
				PollInterval: 6 * time.Second,
			},
			wantErr:     true,
			errContains: "poll interval must be between 1-5 seconds",
		},
		{
			name: "default values",
			config: RedisCASConfig{
				RedisAddr: redisAddr,
				NodeID:    "test-node",
				// PollInterval and Namespace not set - should use defaults
			},
			wantErr: false,
		},
		{
			name: "custom namespace",
			config: RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "test-node",
				Namespace:    "custom",
				PollInterval: 3 * time.Second,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing key
			cleanupRedisKey(t, redisAddr, tt.config.Namespace)

			broker, err := NewRedisCASBroker(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, broker)

			// Verify broker properties
			assert.Equal(t, tt.config.NodeID, broker.nodeID)

			// Check default namespace
			expectedNamespace := tt.config.Namespace
			if expectedNamespace == "" {
				expectedNamespace = "matcher"
			}
			assert.Equal(t, expectedNamespace+":events", broker.eventKey)

			// Check default poll interval
			expectedInterval := tt.config.PollInterval
			if expectedInterval == 0 {
				expectedInterval = 2 * time.Second
			}
			assert.Equal(t, expectedInterval, broker.pollInterval)

			// Test health check
			assert.NoError(t, broker.Health(context.Background()))

			// Cleanup
			assert.NoError(t, broker.Close())
		})
	}
}

// TestRedisCASBroker_PublishAndSubscribe tests basic publish/subscribe functionality
func TestRedisCASBroker_PublishAndSubscribe(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_pub_sub"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create two brokers to simulate different nodes
	publisherConfig := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "publisher-node",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	subscriberConfig := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "subscriber-node",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	publisher, err := NewRedisCASBroker(publisherConfig)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewRedisCASBroker(subscriberConfig)
	require.NoError(t, err)
	defer subscriber.Close()

	// Set up subscription
	eventChan := make(chan *Event, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = subscriber.Subscribe(ctx, eventChan)
	require.NoError(t, err)

	// Give subscriber time to start polling
	time.Sleep(100 * time.Millisecond)

	// Publish an event
	testEvent := &Event{
		Type: EventTypeRuleAdded,
		Data: []byte(`{"test": "data"}`),
	}

	err = publisher.Publish(ctx, testEvent)
	require.NoError(t, err)

	// Wait for event to be received
	select {
	case receivedEvent := <-eventChan:
		assert.Equal(t, testEvent.Type, receivedEvent.Type)
		assert.Equal(t, testEvent.Data, receivedEvent.Data)
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for event")
	}
}

// TestRedisCASBroker_TimestampOrdering tests that events are properly ordered by timestamp
func TestRedisCASBroker_TimestampOrdering(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_ordering"
	cleanupRedisKey(t, redisAddr, namespace)

	config := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "test-node",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	broker, err := NewRedisCASBroker(config)
	require.NoError(t, err)
	defer broker.Close()

	ctx := context.Background()

	// Publish first event
	event1 := &Event{Type: EventTypeRuleAdded, Data: []byte("event1")}
	err = broker.Publish(ctx, event1)
	require.NoError(t, err)

	// Get timestamp after first event
	time.Sleep(10 * time.Millisecond) // Ensure different timestamp
	firstTimestamp := broker.GetLastTimestamp()

	// Publish second event
	event2 := &Event{Type: EventTypeDimensionAdded, Data: []byte("event2")}
	err = broker.Publish(ctx, event2)
	require.NoError(t, err)

	// Verify timestamp increased
	secondTimestamp := broker.GetLastTimestamp()
	assert.Greater(t, secondTimestamp, firstTimestamp)

	// Verify latest event is event2
	latestEvent, err := broker.GetLatestEvent(ctx)
	require.NoError(t, err)
	require.NotNil(t, latestEvent)
	assert.Equal(t, event2.Type, latestEvent.Event.Type)
	assert.Equal(t, event2.Data, latestEvent.Event.Data)
	assert.Equal(t, secondTimestamp, latestEvent.Timestamp)
}

// TestRedisCASBroker_IgnoreOwnEvents tests that nodes don't receive their own events
func TestRedisCASBroker_IgnoreOwnEvents(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_ignore_own"
	cleanupRedisKey(t, redisAddr, namespace)

	config := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "test-node",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	broker, err := NewRedisCASBroker(config)
	require.NoError(t, err)
	defer broker.Close()

	// Set up subscription
	eventChan := make(chan *Event, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = broker.Subscribe(ctx, eventChan)
	require.NoError(t, err)

	// Give subscriber time to start polling
	time.Sleep(100 * time.Millisecond)

	// Publish an event from the same node
	testEvent := &Event{
		Type: EventTypeRuleAdded,
		Data: []byte(`{"test": "own-event"}`),
	}

	err = broker.Publish(ctx, testEvent)
	require.NoError(t, err)

	// Wait to see if we receive our own event (we shouldn't)
	select {
	case receivedEvent := <-eventChan:
		t.Fatalf("Received own event, should have been ignored: %+v", receivedEvent)
	case <-time.After(2 * time.Second):
		// This is expected - we should not receive our own event
	}

	// Verify the event was published but timestamp was updated
	latestEvent, err := broker.GetLatestEvent(ctx)
	require.NoError(t, err)
	require.NotNil(t, latestEvent)
	assert.Equal(t, testEvent.Type, latestEvent.Event.Type)
	assert.Equal(t, config.NodeID, latestEvent.NodeID)
}

// TestRedisCASBroker_ConcurrentPublish tests concurrent publishing
func TestRedisCASBroker_ConcurrentPublish(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_concurrent"
	cleanupRedisKey(t, redisAddr, namespace)

	numPublishers := 5
	numEventsPerPublisher := 10

	// Create multiple publisher brokers
	var publishers []*RedisCASBroker
	for i := 0; i < numPublishers; i++ {
		config := RedisCASConfig{
			RedisAddr:    redisAddr,
			NodeID:       fmt.Sprintf("publisher-%d", i),
			Namespace:    namespace,
			PollInterval: 1 * time.Second,
		}

		broker, err := NewRedisCASBroker(config)
		require.NoError(t, err)
		publishers = append(publishers, broker)
	}

	defer func() {
		for _, p := range publishers {
			p.Close()
		}
	}()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Publish events concurrently
	for i, publisher := range publishers {
		wg.Add(1)
		go func(publisherID int, p *RedisCASBroker) {
			defer wg.Done()

			for j := 0; j < numEventsPerPublisher; j++ {
				event := &Event{
					Type: EventTypeRuleAdded,
					Data: []byte(fmt.Sprintf(`{"publisher": %d, "event": %d}`, publisherID, j)),
				}

				err := p.Publish(ctx, event)
				assert.NoError(t, err)

				// Small delay to reduce contention
				time.Sleep(time.Duration(j+1) * time.Millisecond)
			}
		}(i, publisher)
	}

	wg.Wait()

	// Verify only one event exists (latest one)
	latestEvent, err := publishers[0].GetLatestEvent(ctx)
	require.NoError(t, err)
	require.NotNil(t, latestEvent)

	// Verify the event is valid JSON
	var eventData map[string]interface{}
	err = json.Unmarshal(latestEvent.Event.Data.([]byte), &eventData)
	require.NoError(t, err)

	// Should have publisher and event fields
	assert.Contains(t, eventData, "publisher")
	assert.Contains(t, eventData, "event")
}

// TestRedisCASBroker_WaitForTimestamp tests the timestamp waiting functionality
func TestRedisCASBroker_WaitForTimestamp(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_wait_timestamp"
	cleanupRedisKey(t, redisAddr, namespace)

	config := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "test-node",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	broker, err := NewRedisCASBroker(config)
	require.NoError(t, err)
	defer broker.Close()

	ctx := context.Background()

	// Test waiting for a timestamp that doesn't exist yet (should timeout)
	futureTimestamp := time.Now().UnixNano() + int64(time.Hour)
	err = broker.WaitForTimestamp(ctx, futureTimestamp, 1*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")

	// Publish an event
	testEvent := &Event{Type: EventTypeRuleAdded, Data: []byte("test")}
	err = broker.Publish(ctx, testEvent)
	require.NoError(t, err)

	currentTimestamp := broker.GetLastTimestamp()

	// Test waiting for current timestamp (should return immediately)
	err = broker.WaitForTimestamp(ctx, currentTimestamp, 1*time.Second)
	assert.NoError(t, err)

	// Test waiting for past timestamp (should return immediately)
	pastTimestamp := currentTimestamp - 1000
	err = broker.WaitForTimestamp(ctx, pastTimestamp, 1*time.Second)
	assert.NoError(t, err)
}

// TestRedisCASBroker_MultipleSubscribers tests multiple subscribers
func TestRedisCASBroker_MultipleSubscribers(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping test")
	}

	namespace := "test_multi_sub"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create publisher
	publisherConfig := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "publisher",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	}

	publisher, err := NewRedisCASBroker(publisherConfig)
	require.NoError(t, err)
	defer publisher.Close()

	// Create multiple subscribers
	numSubscribers := 3
	var subscribers []*RedisCASBroker
	var eventChans []chan *Event

	for i := 0; i < numSubscribers; i++ {
		config := RedisCASConfig{
			RedisAddr:    redisAddr,
			NodeID:       fmt.Sprintf("subscriber-%d", i),
			Namespace:    namespace,
			PollInterval: 1 * time.Second,
		}

		subscriber, err := NewRedisCASBroker(config)
		require.NoError(t, err)
		subscribers = append(subscribers, subscriber)

		eventChan := make(chan *Event, 10)
		eventChans = append(eventChans, eventChan)

		ctx := context.Background()
		err = subscriber.Subscribe(ctx, eventChan)
		require.NoError(t, err)
	}

	defer func() {
		for _, s := range subscribers {
			s.Close()
		}
	}()

	// Give subscribers time to start polling
	time.Sleep(200 * time.Millisecond)

	// Publish an event
	testEvent := &Event{
		Type: EventTypeDimensionAdded,
		Data: []byte(`{"multi": "subscriber", "test": true}`),
	}

	ctx := context.Background()
	err = publisher.Publish(ctx, testEvent)
	require.NoError(t, err)

	// All subscribers should receive the event
	for i, eventChan := range eventChans {
		select {
		case receivedEvent := <-eventChan:
			assert.Equal(t, testEvent.Type, receivedEvent.Type, "Subscriber %d", i)
			assert.Equal(t, testEvent.Data, receivedEvent.Data, "Subscriber %d", i)
		case <-time.After(3 * time.Second):
			t.Fatalf("Subscriber %d did not receive event", i)
		}
	}
}
