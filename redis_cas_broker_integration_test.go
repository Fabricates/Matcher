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

	// Create matcher instances with Redis CAS brokers
	matcherConfig1 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
			{Name: "service", DefaultWeight: 2.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "matcher-node-1",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	matcherConfig2 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
			{Name: "service", DefaultWeight: 2.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "matcher-node-2",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create matchers
	matcher1, err := NewMatcherWithConfig(ctx, matcherConfig1)
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewMatcherWithConfig(ctx, matcherConfig2)
	require.NoError(t, err)
	defer matcher2.Close()

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

	err = matcher1.AddRule(ctx, rule)
	require.NoError(t, err)

	// Wait for the rule to propagate to matcher2
	time.Sleep(2 * time.Second)

	// Test that both matchers can find the rule
	testDimensions := []*DimensionValue{
		{DimensionName: "region", Value: "us-east-1"},
		{DimensionName: "service", Value: "api"},
	}

	results1, err := matcher1.MatchRule(ctx, testDimensions)
	require.NoError(t, err)
	require.Len(t, results1, 1)
	assert.Equal(t, "test-rule-1", results1[0].Rule.ID)

	results2, err := matcher2.MatchRule(ctx, testDimensions)
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.Equal(t, "test-rule-1", results2[0].Rule.ID)

	// Update the rule from matcher2
	rule.Dimensions[0].Weight = 5.0 // Change weight
	err = matcher2.UpdateRule(ctx, rule)
	require.NoError(t, err)

	// Wait for the update to propagate
	time.Sleep(2 * time.Second)

	// Verify both matchers see the updated rule
	results1, err = matcher1.MatchRule(ctx, testDimensions)
	require.NoError(t, err)
	require.Len(t, results1, 1)
	assert.Equal(t, 5.0, results1[0].Rule.Dimensions[0].Weight)

	results2, err = matcher2.MatchRule(ctx, testDimensions)
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.Equal(t, 5.0, results2[0].Rule.Dimensions[0].Weight)
}

// TestRedisCASBroker_Integration_MultipleRules tests handling multiple rules
func TestRedisCASBroker_Integration_MultipleRules(t *testing.T) {
	redisAddr := getRedisAddr()
	if !isRedisAvailable(redisAddr) {
		t.Skip("Redis not available, skipping integration test")
	}

	namespace := "multi_rules_test"
	cleanupRedisKey(t, redisAddr, namespace)

	// Create two matcher instances
	matcherConfig1 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
			{Name: "env", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-1",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	matcherConfig2 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
			{Name: "env", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-2",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	matcher1, err := NewMatcherWithConfig(ctx, matcherConfig1)
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewMatcherWithConfig(ctx, matcherConfig2)
	require.NoError(t, err)
	defer matcher2.Close()

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
	err = matcher1.AddRule(ctx, rules[0])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Add second rule from matcher2
	err = matcher2.AddRule(ctx, rules[1])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Add third rule from matcher1
	err = matcher1.AddRule(ctx, rules[2])
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Test that both matchers can see all rules
	testCases := []struct {
		dimensions []*DimensionValue
		expectedID string
	}{
		{
			dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "us-east-1"},
				{DimensionName: "env", Value: "prod"},
			},
			expectedID: "rule-prod",
		},
		{
			dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "us-west-2"},
				{DimensionName: "env", Value: "dev"},
			},
			expectedID: "rule-dev",
		},
		{
			dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "eu-west-1"},
				{DimensionName: "env", Value: "staging"},
			},
			expectedID: "rule-staging",
		},
	}

	for _, tc := range testCases {
		// Test matcher1
		results1, err := matcher1.MatchRule(ctx, tc.dimensions)
		require.NoError(t, err)
		require.Len(t, results1, 1, "Matcher1 should find rule for %v", tc.dimensions)
		assert.Equal(t, tc.expectedID, results1[0].Rule.ID)

		// Test matcher2
		results2, err := matcher2.MatchRule(ctx, tc.dimensions)
		require.NoError(t, err)
		require.Len(t, results2, 1, "Matcher2 should find rule for %v", tc.dimensions)
		assert.Equal(t, tc.expectedID, results2[0].Rule.ID)
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

	matcherConfig1 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-1",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	matcherConfig2 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-2",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	matcher1, err := NewMatcherWithConfig(ctx, matcherConfig1)
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewMatcherWithConfig(ctx, matcherConfig2)
	require.NoError(t, err)
	defer matcher2.Close()

	time.Sleep(200 * time.Millisecond)

	// Add a new dimension from matcher1
	newDimension := &DimensionConfig{
		Name:          "service",
		DefaultWeight: 2.0,
	}

	err = matcher1.AddDimension(ctx, newDimension)
	require.NoError(t, err)

	// Wait for propagation
	time.Sleep(2 * time.Second)

	// Verify both matchers have the new dimension
	dims1 := matcher1.GetDimensions()
	dims2 := matcher2.GetDimensions()

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
	assert.Equal(t, 2.0, serviceDim1.DefaultWeight)
	assert.Equal(t, 2.0, serviceDim2.DefaultWeight)

	// Update dimension weight from matcher2
	serviceDim2.DefaultWeight = 3.0
	err = matcher2.UpdateDimension(ctx, serviceDim2)
	require.NoError(t, err)

	// Wait for propagation
	time.Sleep(2 * time.Second)

	// Verify both matchers see the updated weight
	dims1 = matcher1.GetDimensions()
	dims2 = matcher2.GetDimensions()

	for _, dim := range dims1 {
		if dim.Name == "service" {
			assert.Equal(t, 3.0, dim.DefaultWeight)
		}
	}
	for _, dim := range dims2 {
		if dim.Name == "service" {
			assert.Equal(t, 3.0, dim.DefaultWeight)
		}
	}
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
	var configs []*MatcherConfig

	for i := 0; i < numMatchers; i++ {
		config := &MatcherConfig{
			Dimensions: []*DimensionConfig{
				{Name: "region", DefaultWeight: 1.0},
				{Name: "service", DefaultWeight: 1.0},
			},
			BrokerConfig: &BrokerConfig{
				Type: "redis-cas",
				Redis: &RedisCASConfig{
					RedisAddr:    redisAddr,
					NodeID:       fmt.Sprintf("node-%d", i),
					Namespace:    namespace,
					PollInterval: 1 * time.Second,
				},
			},
		}
		configs = append(configs, config)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create all matchers
	for _, config := range configs {
		matcher, err := NewMatcherWithConfig(ctx, config)
		require.NoError(t, err)
		matchers = append(matchers, matcher)
	}

	defer func() {
		for _, matcher := range matchers {
			matcher.Close()
		}
	}()

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

				err := m.AddRule(ctx, rule)
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
		rules := matcher.GetRules()
		assert.Len(t, rules, totalRules, "Matcher %d should see all rules", i)
	}

	// Test that all matchers can match rules correctly
	for matcherIndex := 0; matcherIndex < numMatchers; matcherIndex++ {
		for ruleIndex := 0; ruleIndex < rulesPerMatcher; ruleIndex++ {
			testDimensions := []*DimensionValue{
				{DimensionName: "region", Value: fmt.Sprintf("region-%d", matcherIndex)},
				{DimensionName: "service", Value: fmt.Sprintf("service-%d", ruleIndex)},
			}

			expectedRuleID := fmt.Sprintf("rule-%d-%d", matcherIndex, ruleIndex)

			// Test with a random matcher
			testMatcherIndex := (matcherIndex + ruleIndex) % numMatchers
			results, err := matchers[testMatcherIndex].MatchRule(ctx, testDimensions)
			require.NoError(t, err)
			require.Len(t, results, 1, "Should find exactly one rule")
			assert.Equal(t, expectedRuleID, results[0].Rule.ID)
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

	matcherConfig1 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-1",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	matcherConfig2 := &MatcherConfig{
		Dimensions: []*DimensionConfig{
			{Name: "region", DefaultWeight: 1.0},
		},
		BrokerConfig: &BrokerConfig{
			Type: "redis-cas",
			Redis: &RedisCASConfig{
				RedisAddr:    redisAddr,
				NodeID:       "node-2",
				Namespace:    namespace,
				PollInterval: 1 * time.Second,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	matcher1, err := NewMatcherWithConfig(ctx, matcherConfig1)
	require.NoError(t, err)
	defer matcher1.Close()

	matcher2, err := NewMatcherWithConfig(ctx, matcherConfig2)
	require.NoError(t, err)
	defer matcher2.Close()

	time.Sleep(200 * time.Millisecond)

	// Add initial rule
	rule1 := &Rule{
		ID:     "initial-rule",
		Status: RuleStatusWorking,
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-east-1", MatchType: MatchTypeEqual, Weight: 1.0},
		},
	}

	err = matcher1.AddRule(ctx, rule1)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Verify both matchers see the rule
	results1, err := matcher1.MatchRule(ctx, []*DimensionValue{{DimensionName: "region", Value: "us-east-1"}})
	require.NoError(t, err)
	require.Len(t, results1, 1)

	results2, err := matcher2.MatchRule(ctx, []*DimensionValue{{DimensionName: "region", Value: "us-east-1"}})
	require.NoError(t, err)
	require.Len(t, results2, 1)

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

	err = matcher1.AddRule(ctx, rule2)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Matcher1 should see both rules
	rules1 := matcher1.GetRules()
	assert.Len(t, rules1, 2)

	// "Reconnect" matcher2 by creating a new instance
	matcher2, err = NewMatcherWithConfig(ctx, matcherConfig2)
	require.NoError(t, err)
	defer matcher2.Close()

	// Wait for the new matcher to catch up
	time.Sleep(3 * time.Second)

	// Both matchers should now see both rules
	rules1 = matcher1.GetRules()
	rules2 := matcher2.GetRules()

	assert.Len(t, rules1, 2)
	assert.Len(t, rules2, 2)

	// Verify both rules can be matched by both matchers
	results1, err = matcher1.MatchRule(ctx, []*DimensionValue{{DimensionName: "region", Value: "us-west-2"}})
	require.NoError(t, err)
	require.Len(t, results1, 1)
	assert.Equal(t, "during-partition", results1[0].Rule.ID)

	results2, err = matcher2.MatchRule(ctx, []*DimensionValue{{DimensionName: "region", Value: "us-west-2"}})
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.Equal(t, "during-partition", results2[0].Rule.ID)
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
			Type: RuleChange,
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
	assert.Equal(t, string(lastEvent.Data), string(receivedEvents[0].Data))

	// Verify the latest event in Redis is indeed the last one
	latestEvent, err := subscriber.GetLatestEvent(ctx)
	require.NoError(t, err)
	require.NotNil(t, latestEvent)

	assert.Equal(t, lastEvent.Type, latestEvent.Event.Type)
	assert.Equal(t, string(lastEvent.Data), string(latestEvent.Event.Data))
}
