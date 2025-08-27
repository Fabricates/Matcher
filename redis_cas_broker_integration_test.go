package matcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedisCASBroker_Integration_EndToEnd tests the complete workflow
func TestRedisCASBroker_Integration_EndToEnd(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "integration_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create Redis CAS brokers
	broker1, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "matcher-node-1",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker1.Close()

	broker2, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "matcher-node-2",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker2.Close()

	// Create persistence (using JSON persistence for testing)
	persistence := NewJSONPersistence("./test_data")

	// Create matchers with Redis CAS brokers
	matcher1, err := NewInMemoryMatcher(persistence, broker1, "matcher-node-1")
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewInMemoryMatcher(persistence, broker2, "matcher-node-2")
	require.NoError(t, err)
	defer matcher2.Close()

	// Add dimensions to both matchers
	regionDim := &DimensionConfig{
		Name:   "region",
		Index:  0,
		Weight: 1.0,
	}
	serviceDim := &DimensionConfig{
		Name:   "service",
		Index:  1,
		Weight: 2.0,
	}

	err = matcher1.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher1.AddDimension(serviceDim)
	require.NoError(t, err)

	err = matcher2.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher2.AddDimension(serviceDim)
	require.NoError(t, err)

	// Give time for brokers to initialize and start polling
	time.Sleep(200 * time.Millisecond)

	// Add a rule to matcher1
	rule := &Rule{
		ID:     "test-rule-1",
		Status: RuleStatusWorking,
		Dimensions: []*DimensionValue{
			{
				DimensionName: "region",
				Value:         "us-east-1",
				MatchType:     MatchTypeEqual,
				Weight:        2.0,
			},
			{
				DimensionName: "service",
				Value:         "api",
				MatchType:     MatchTypeEqual,
				Weight:        3.0,
			},
		},
	}

	err = matcher1.AddRule(rule)
	require.NoError(t, err)

	// Wait for the rule to propagate to matcher2
	time.Sleep(2 * time.Second)

	// Test that both matchers can find the rule
	query := &QueryRule{
		Values: map[string]string{
			"region":  "us-east-1",
			"service": "api",
		},
	}

	result1, err := matcher1.FindBestMatch(query)
	require.NoError(t, err)
	require.NotNil(t, result1)
	assert.Equal(t, "test-rule-1", result1.Rule.ID)

	result2, err := matcher2.FindBestMatch(query)
	require.NoError(t, err)
	require.NotNil(t, result2)
	assert.Equal(t, "test-rule-1", result2.Rule.ID)
}

// TestRedisCASBroker_Integration_MultipleRules tests handling multiple rules
func TestRedisCASBroker_Integration_MultipleRules(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "multi_rules_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create Redis CAS brokers
	broker1, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-1",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker1.Close()

	broker2, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-2",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker2.Close()

	// Create persistence
	persistence := NewJSONPersistence("./test_data_multi")

	// Create matchers
	matcher1, err := NewInMemoryMatcher(persistence, broker1, "node-1")
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewInMemoryMatcher(persistence, broker2, "node-2")
	require.NoError(t, err)
	defer matcher2.Close()

	// Add dimensions
	regionDim := &DimensionConfig{Name: "region", Index: 0, Weight: 1.0}
	envDim := &DimensionConfig{Name: "env", Index: 1, Weight: 1.0}

	err = matcher1.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher1.AddDimension(envDim)
	require.NoError(t, err)

	err = matcher2.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher2.AddDimension(envDim)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Add multiple rules from different matchers
	rules := []*Rule{
		{
			ID:     "rule-prod",
			Status: RuleStatusWorking,
			Dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "us-east-1", MatchType: MatchTypeEqual, Weight: 1.0},
				{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual, Weight: 2.0},
			},
		},
		{
			ID:     "rule-dev",
			Status: RuleStatusWorking,
			Dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "us-west-2", MatchType: MatchTypeEqual, Weight: 1.0},
				{DimensionName: "env", Value: "dev", MatchType: MatchTypeEqual, Weight: 1.0},
			},
		},
		{
			ID:     "rule-staging",
			Status: RuleStatusWorking,
			Dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "eu-west-1", MatchType: MatchTypeEqual, Weight: 1.0},
				{DimensionName: "env", Value: "staging", MatchType: MatchTypeEqual, Weight: 1.5},
			},
		},
	}

	// Add first rule from matcher1
	err = matcher1.AddRule(rules[0])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Add second rule from matcher2
	err = matcher2.AddRule(rules[1])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Add third rule from matcher1
	err = matcher1.AddRule(rules[2])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Test that both matchers can see all rules
	testCases := []struct {
		values     map[string]string
		expectedID string
	}{
		{
			values: map[string]string{
				"region": "us-east-1",
				"env":    "prod",
			},
			expectedID: "rule-prod",
		},
		{
			values: map[string]string{
				"region": "us-west-2",
				"env":    "dev",
			},
			expectedID: "rule-dev",
		},
		{
			values: map[string]string{
				"region": "eu-west-1",
				"env":    "staging",
			},
			expectedID: "rule-staging",
		},
	}

	for _, tc := range testCases {
		query := &QueryRule{Values: tc.values}

		// Test matcher1
		result1, err := matcher1.FindBestMatch(query)
		require.NoError(t, err)
		require.NotNil(t, result1, "Matcher1 should find rule for %v", tc.values)
		assert.Equal(t, tc.expectedID, result1.Rule.ID)

		// Test matcher2
		result2, err := matcher2.FindBestMatch(query)
		require.NoError(t, err)
		require.NotNil(t, result2, "Matcher2 should find rule for %v", tc.values)
		assert.Equal(t, tc.expectedID, result2.Rule.ID)
	}
}

// TestRedisCASBroker_Integration_DimensionChanges tests dimension changes
func TestRedisCASBroker_Integration_DimensionChanges(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "dimension_changes_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create Redis CAS brokers
	broker1, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-1",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker1.Close()

	broker2, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-2",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker2.Close()

	// Create persistence
	persistence := NewJSONPersistence("./test_data_dims")

	// Create matchers
	matcher1, err := NewInMemoryMatcher(persistence, broker1, "node-1")
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewInMemoryMatcher(persistence, broker2, "node-2")
	require.NoError(t, err)
	defer matcher2.Close()

	// Add initial dimension
	regionDim := &DimensionConfig{Name: "region", Index: 0, Weight: 1.0}
	err = matcher1.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher2.AddDimension(regionDim)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Add a new dimension from matcher1
	newDimension := &DimensionConfig{
		Name:   "service",
		Index:  1,
		Weight: 2.0,
	}

	err = matcher1.AddDimension(newDimension)
	require.NoError(t, err)

	// Wait for propagation
	time.Sleep(2 * time.Second)

	// Verify both matchers have the new dimension
	dims1, err := matcher1.ListDimensions()
	require.NoError(t, err)
	dims2, err := matcher2.ListDimensions()
	require.NoError(t, err)

	assert.Len(t, dims1, 2)
	assert.Len(t, dims2, 2)

	// Find the service dimension in both matchers
	var serviceDim1, serviceDim2 *DimensionConfig
	for _, dim := range dims1 {
		if dim.Name == "service" {
			serviceDim1 = dim
			break
		}
	}
	for _, dim := range dims2 {
		if dim.Name == "service" {
			serviceDim2 = dim
			break
		}
	}

	require.NotNil(t, serviceDim1)
	require.NotNil(t, serviceDim2)
	assert.Equal(t, 2.0, serviceDim1.Weight)
	assert.Equal(t, 2.0, serviceDim2.Weight)
}

// TestRedisCASBroker_Integration_HighConcurrency tests high concurrency scenarios
func TestRedisCASBroker_Integration_HighConcurrency(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "high_concurrency_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create multiple matcher instances
	numMatchers := 5
	var matchers []*InMemoryMatcher
	var brokers []*RedisCASBroker

	for i := 0; i < numMatchers; i++ {
		broker, err := NewRedisCASBroker(RedisCASConfig{
			RedisAddr:    redisAddr,
			NodeID:       fmt.Sprintf("node-%d", i),
			Namespace:    namespace,
			PollInterval: 1 * time.Second,
		})
		require.NoError(t, err)
		brokers = append(brokers, broker)

		persistence := NewJSONPersistence(fmt.Sprintf("./test_data_concurrency_%d", i))
		matcher, err := NewInMemoryMatcher(persistence, broker, fmt.Sprintf("node-%d", i))
		require.NoError(t, err)
		matchers = append(matchers, matcher)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_ = ctx // Prevent unused variable warning

	defer func() {
		for _, matcher := range matchers {
			matcher.Close()
		}
		for _, broker := range brokers {
			broker.Close()
		}
	}()

	// Add dimensions to all matchers
	regionDim := &DimensionConfig{Name: "region", Index: 0, Weight: 1.0}
	serviceDim := &DimensionConfig{Name: "service", Index: 1, Weight: 1.0}

	for _, matcher := range matchers {
		err := matcher.AddDimension(regionDim)
		require.NoError(t, err)
		err = matcher.AddDimension(serviceDim)
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	// Concurrently add rules from different matchers
	var wg sync.WaitGroup
	rulesPerMatcher := 10
	totalRules := numMatchers * rulesPerMatcher

	for i, matcher := range matchers {
		wg.Add(1)
		go func(matcherIndex int, m *InMemoryMatcher) {
			defer wg.Done()

			for j := 0; j < rulesPerMatcher; j++ {
				rule := &Rule{
					ID:     fmt.Sprintf("rule-%d-%d", matcherIndex, j),
					Status: RuleStatusWorking,
					Dimensions: []*DimensionValue{
						{
							DimensionName: "region",
							Value:         fmt.Sprintf("region-%d", matcherIndex),
							MatchType:     MatchTypeEqual,
							Weight:        1.0,
						},
						{
							DimensionName: "service",
							Value:         fmt.Sprintf("service-%d", j),
							MatchType:     MatchTypeEqual,
							Weight:        2.0,
						},
					},
				}

				err := m.AddRule(rule)
				assert.NoError(t, err)

				// Small delay to reduce contention
				time.Sleep(50 * time.Millisecond)
			}
		}(i, matcher)
	}

	wg.Wait()

	// Wait for all events to propagate
	time.Sleep(5 * time.Second)

	// Verify all matchers see all the rules
	for i, matcher := range matchers {
		rules, err := matcher.ListRules(0, totalRules)
		require.NoError(t, err)
		assert.Len(t, rules, totalRules, "Matcher %d should see all rules", i)
	}

	// Test that all matchers can match rules correctly
	for matcherIndex := 0; matcherIndex < numMatchers; matcherIndex++ {
		for ruleIndex := 0; ruleIndex < rulesPerMatcher; ruleIndex++ {
			query := &QueryRule{
				Values: map[string]string{
					"region":  fmt.Sprintf("region-%d", matcherIndex),
					"service": fmt.Sprintf("service-%d", ruleIndex),
				},
			}

			expectedRuleID := fmt.Sprintf("rule-%d-%d", matcherIndex, ruleIndex)

			// Test with a random matcher
			testMatcherIndex := (matcherIndex + ruleIndex) % numMatchers
			result, err := matchers[testMatcherIndex].FindBestMatch(query)
			require.NoError(t, err)
			require.NotNil(t, result, "Should find exactly one rule")
			assert.Equal(t, expectedRuleID, result.Rule.ID)
		}
	}
}

// TestRedisCASBroker_Integration_NetworkPartition simulates network partition scenarios
func TestRedisCASBroker_Integration_NetworkPartition(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "network_partition_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create Redis CAS brokers
	broker1, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-1",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker1.Close()

	broker2, err := NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-2",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)

	// Create persistence
	persistence1 := NewJSONPersistence("./test_data_partition1")
	persistence2 := NewJSONPersistence("./test_data_partition2")

	// Create matchers
	matcher1, err := NewInMemoryMatcher(persistence1, broker1, "node-1")
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewInMemoryMatcher(persistence2, broker2, "node-2")
	require.NoError(t, err)
	defer matcher2.Close()

	// Add dimensions
	regionDim := &DimensionConfig{Name: "region", Index: 0, Weight: 1.0}
	err = matcher1.AddDimension(regionDim)
	require.NoError(t, err)
	err = matcher2.AddDimension(regionDim)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Add initial rule
	rule1 := &Rule{
		ID:     "initial-rule",
		Status: RuleStatusWorking,
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-east-1", MatchType: MatchTypeEqual, Weight: 1.0},
		},
	}

	err = matcher1.AddRule(rule1)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Verify both matchers see the rule
	query1 := &QueryRule{Values: map[string]string{"region": "us-east-1"}}
	result1, err := matcher1.FindBestMatch(query1)
	require.NoError(t, err)
	require.NotNil(t, result1)

	result2, err := matcher2.FindBestMatch(query1)
	require.NoError(t, err)
	require.NotNil(t, result2)

	// Simulate "network partition" by closing one matcher's broker
	matcher2.Close()

	// Add a rule from the still-connected matcher
	rule2 := &Rule{
		ID:     "during-partition",
		Status: RuleStatusWorking,
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west-2", MatchType: MatchTypeEqual, Weight: 1.0},
		},
	}

	err = matcher1.AddRule(rule2)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Matcher1 should see both rules
	rules1, err := matcher1.ListRules(0, 10)
	require.NoError(t, err)
	assert.Len(t, rules1, 2)

	// "Reconnect" matcher2 by creating a new instance
	broker2, err = NewRedisCASBroker(RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "node-2",
		Namespace:    namespace,
		PollInterval: 1 * time.Second,
	})
	require.NoError(t, err)
	defer broker2.Close()

	matcher2, err = NewInMemoryMatcher(persistence2, broker2, "node-2")
	require.NoError(t, err)
	defer matcher2.Close()

	// Add the region dimension to the new matcher2
	err = matcher2.AddDimension(regionDim)
	require.NoError(t, err)

	// Wait for the new matcher to catch up
	time.Sleep(3 * time.Second)

	// Both matchers should now see both rules
	rules1, err = matcher1.ListRules(0, 10)
	require.NoError(t, err)
	rules2, err := matcher2.ListRules(0, 10)
	require.NoError(t, err)

	assert.Len(t, rules1, 2)
	assert.Len(t, rules2, 2)

	// Verify both rules can be matched by both matchers
	query2 := &QueryRule{Values: map[string]string{"region": "us-west-2"}}
	result1, err = matcher1.FindBestMatch(query2)
	require.NoError(t, err)
	require.NotNil(t, result1)
	assert.Equal(t, "during-partition", result1.Rule.ID)

	result2, err = matcher2.FindBestMatch(query2)
	require.NoError(t, err)
	require.NotNil(t, result2)
	assert.Equal(t, "during-partition", result2.Rule.ID)
}

// TestRedisCASBroker_Integration_EventOrdering tests that events are processed in order
func TestRedisCASBroker_Integration_EventOrdering(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "event_ordering_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create brokers directly for more control
	publisherConfig := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "publisher",
		Namespace:    namespace,
		PollInterval: 500 * time.Millisecond, // Faster polling
	}

	subscriberConfig := RedisCASConfig{
		RedisAddr:    redisAddr,
		NodeID:       "subscriber",
		Namespace:    namespace,
		PollInterval: 500 * time.Millisecond,
	}

	publisher, err := NewRedisCASBroker(publisherConfig)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewRedisCASBroker(subscriberConfig)
	require.NoError(t, err)
	defer subscriber.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Set up subscription to collect events
	eventChan := make(chan *Event, 100)
	err = subscriber.Subscribe(ctx, eventChan)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Publish events rapidly
	numEvents := 10
	publishedEvents := make([]*Event, numEvents)

	for i := 0; i < numEvents; i++ {
		event := &Event{
			Type: EventTypeRuleAdded,
			Data: []byte(fmt.Sprintf(`{"sequence": %d, "data": "event-%d"}`, i, i)),
		}
		publishedEvents[i] = event

		err = publisher.Publish(ctx, event)
		require.NoError(t, err)

		// Small delay to ensure different timestamps
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for all events to be received
	var receivedEvents []*Event
	timeout := time.After(10 * time.Second)

collectLoop:
	for len(receivedEvents) < numEvents {
		select {
		case event := <-eventChan:
			receivedEvents = append(receivedEvents, event)
		case <-timeout:
			break collectLoop
		}
	}

	// Since we only store the latest event, we should only receive the last one
	require.Len(t, receivedEvents, 1, "Should only receive the latest event")

	// The received event should be the last published event
	lastEvent := publishedEvents[numEvents-1]
	assert.Equal(t, lastEvent.Type, receivedEvents[0].Type)
	
	// Convert interface{} to []byte for comparison
	lastEventData, ok := lastEvent.Data.([]byte)
	require.True(t, ok, "Expected lastEvent.Data to be []byte")
	receivedEventData, ok := receivedEvents[0].Data.([]byte)
	require.True(t, ok, "Expected receivedEvents[0].Data to be []byte")
	assert.Equal(t, string(lastEventData), string(receivedEventData))

	// Verify the latest event in Redis is indeed the last one
	latestEvent, err := subscriber.GetLatestEvent(ctx)
	require.NoError(t, err)
	require.NotNil(t, latestEvent)

	assert.Equal(t, lastEvent.Type, latestEvent.Event.Type)
	latestEventData, ok := latestEvent.Event.Data.([]byte)
	require.True(t, ok, "Expected latestEvent.Event.Data to be []byte")
	assert.Equal(t, string(lastEventData), string(latestEventData))
}
