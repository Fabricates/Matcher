package matcher

import (
	"fmt"
	"testing"
	"time"
)

// Helper function to add test dimensions for backward compatibility
func addTestDimensions(engine *InMemoryMatcher) error {
	dimensions := []*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
		NewDimensionConfig("tool_id", 3, false),
		NewDimensionConfig("recipe", 4, false),
	}

	for _, dim := range dimensions {
		if err := engine.AddDimension(dim); err != nil {
			return err
		}
	}
	return nil
}

func TestBasicMatching(t *testing.T) {
	// Create engine with mock persistence
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-1")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add a test rule
	rule := NewRule("test_rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRoute", MatchTypeEqual).
		Dimension("tool", "TestTool", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test exact match
	query := CreateQuery(map[string]string{
		"product": "TestProduct",
		"route":   "TestRoute",
		"tool":    "TestTool",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none")
	}

	if result.Rule.ID != "test_rule" {
		t.Errorf("Expected rule 'test_rule', got '%s'", result.Rule.ID)
	}

	if result.TotalWeight != 0.0 { // No explicit weights set
		t.Errorf("Expected weight 0.0, got %.1f", result.TotalWeight)
	}
}

func TestPrefixMatching(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-2")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add prefix rule
	rule := NewRule("prefix_rule").
		Dimension("product", "Test", MatchTypePrefix).
		ManualWeight(10.0).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test prefix match
	query := CreateQuery(map[string]string{
		"product": "TestProduct123",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected prefix match but got none")
	}

	if result.Rule.ID != "prefix_rule" {
		t.Errorf("Expected rule 'prefix_rule', got '%s'", result.Rule.ID)
	}
}

func TestSuffixMatching(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-3")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add suffix rule
	rule := NewRule("suffix_rule").
		Dimension("product", "Product", MatchTypeSuffix).
		ManualWeight(10.0).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test suffix match
	query := CreateQuery(map[string]string{
		"product": "TestProduct",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected suffix match but got none")
	}

	if result.Rule.ID != "suffix_rule" {
		t.Errorf("Expected rule 'suffix_rule', got '%s'", result.Rule.ID)
	}
}

func TestAnyMatching(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-4")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add any rule (fallback)
	rule := NewRule("any_rule").
		Dimension("product", "", MatchTypeAny).
		ManualWeight(5.0).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test any match
	query := CreateQuery(map[string]string{
		"product": "AnythingWorks",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected any match but got none")
	}

	if result.Rule.ID != "any_rule" {
		t.Errorf("Expected rule 'any_rule', got '%s'", result.Rule.ID)
	}

	if result.TotalWeight != 5.0 { // Manual weight
		t.Errorf("Expected weight 5.0, got %.1f", result.TotalWeight)
	}
}

func TestWeightPriority(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-5")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add high weight rule
	highRule := NewRule("high_weight").
		Dimension("product", "Test", MatchTypeEqual).
		ManualWeight(100.0).
		Build()

	// Add low weight rule
	lowRule := NewRule("low_weight").
		Dimension("product", "Test", MatchTypeEqual).
		ManualWeight(1.0).
		Build()

	err = engine.AddRule(highRule)
	if err != nil {
		t.Fatalf("Failed to add high rule: %v", err)
	}

	err = engine.AddRule(lowRule)
	if err != nil {
		t.Fatalf("Failed to add low rule: %v", err)
	}

	// Query should match the high weight rule
	query := CreateQuery(map[string]string{
		"product": "Test",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none")
	}

	if result.Rule.ID != "high_weight" {
		t.Errorf("Expected rule 'high_weight', got '%s'", result.Rule.ID)
	}
}

func TestManualWeightOverride(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-6")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add rule with high calculated weight
	highCalcRule := NewRule("high_calc").
		Dimension("product", "Test", MatchTypeEqual).
		ManualWeight(100.0).
		Build()

	// Add rule with low calculated weight but high manual weight
	highManualRule := NewRule("high_manual").
		Dimension("product", "Test", MatchTypeEqual).
		ManualWeight(200.0).
		Build()

	err = engine.AddRule(highCalcRule)
	if err != nil {
		t.Fatalf("Failed to add high calc rule: %v", err)
	}

	err = engine.AddRule(highManualRule)
	if err != nil {
		t.Fatalf("Failed to add high manual rule: %v", err)
	}

	// Query should match the manual weight rule
	query := CreateQuery(map[string]string{
		"product": "Test",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none")
	}

	if result.Rule.ID != "high_manual" {
		t.Errorf("Expected rule 'high_manual', got '%s'", result.Rule.ID)
	}

	if result.TotalWeight != 200.0 {
		t.Errorf("Expected weight 200.0, got %.1f", result.TotalWeight)
	}
}

func TestCaching(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-7")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add test rule
	rule := NewRule("cache_test").
		Dimension("product", "CacheTest", MatchTypeEqual).
		ManualWeight(10.0).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	query := CreateQuery(map[string]string{
		"product": "CacheTest",
	})

	// First query (cache miss)
	start := time.Now()
	result1, err := engine.FindBestMatch(query)
	firstTime := time.Since(start)
	if err != nil {
		t.Fatalf("First query failed: %v", err)
	}

	// Second query (cache hit)
	start = time.Now()
	result2, err := engine.FindBestMatch(query)
	secondTime := time.Since(start)
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}

	// Results should be the same
	if result1.Rule.ID != result2.Rule.ID {
		t.Errorf("Cache returned different result")
	}

	// Second query should be faster (though this might be flaky in tests)
	if secondTime > firstTime {
		t.Logf("Warning: Cached query was slower than original (%.2fms vs %.2fms)",
			secondTime.Seconds()*1000, firstTime.Seconds()*1000)
	}
}

func TestPerformance(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-8")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for performance testing to avoid weight conflicts
	engine.allowDuplicateWeights = true

	// Add multiple rules for performance testing
	for i := 0; i < 100; i++ {
		rule := NewRule(fmt.Sprintf("perf_rule_%d", i)).
			Dimension("product", fmt.Sprintf("Product%d", i), MatchTypeEqual).
			Dimension("route", fmt.Sprintf("Route%d", i), MatchTypeEqual).
			ManualWeight(15.0).
			Build()

		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %d: %v", i, err)
		}
	}

	// Test query performance
	query := CreateQuery(map[string]string{
		"product": "Product50",
		"route":   "Route50",
	})

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		_, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Performance test query %d failed: %v", i, err)
		}
	}

	totalTime := time.Since(start)
	avgTime := totalTime / time.Duration(iterations)
	qps := float64(iterations) / totalTime.Seconds()

	t.Logf("Performance test results:")
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Average query time: %v", avgTime)
	t.Logf("  Queries per second: %.2f", qps)

	// Expect at least 200 QPS as per requirements
	if qps < 200 {
		t.Errorf("Performance requirement not met: got %.2f QPS, expected >= 200", qps)
	}
}

func TestDynamicDimensions(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-9")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add test dimensions including required ones
	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize test dimensions: %v", err)
	}

	// Add custom dimension
	customDim := NewDimensionConfig("custom_dimension", 5, false)

	err = engine.AddDimension(customDim)
	if err != nil {
		t.Fatalf("Failed to add custom dimension: %v", err)
	}

	// Add rule using custom dimension and required dimensions
	rule := NewRule("custom_rule").
		Dimension("product", "CustomProduct", MatchTypeEqual). // Required dimension
		Dimension("custom_dimension", "custom_value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule with custom dimension: %v", err)
	}

	// Test query with custom dimension
	query := CreateQuery(map[string]string{
		"product":          "CustomProduct",
		"custom_dimension": "custom_value",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query with custom dimension failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match with custom dimension but got none")
	}

	if result.Rule.ID != "custom_rule" {
		t.Errorf("Expected rule 'custom_rule', got '%s'", result.Rule.ID)
	}
}

func TestEventSubscription(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	eventSub := NewMockEventSubscriber()

	engine, err := NewInMemoryMatcher(persistence, eventSub, "test-node-10")
	if err != nil {
		t.Fatalf("Failed to create engine with event subscriber: %v", err)
	}
	defer engine.Close()
	defer eventSub.Close()

	// Publish a rule addition event
	rule := NewRule("event_rule").
		Dimension("product", "EventProduct", MatchTypeEqual).
		Build()

	event := &Event{
		Type:      EventTypeRuleAdded,
		Timestamp: time.Now(),
		Data:      &RuleEvent{Rule: rule},
	}

	eventSub.PublishEvent(event)

	// Give some time for event processing
	time.Sleep(100 * time.Millisecond)

	// Test if rule was added via event
	query := CreateQuery(map[string]string{
		"product": "EventProduct",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query after event failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match after event but got none")
	}

	if result.Rule.ID != "event_rule" {
		t.Errorf("Expected rule 'event_rule', got '%s'", result.Rule.ID)
	}
}

func TestDimensionConsistencyValidation(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-consistency")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test since we're testing dimension consistency, not weight conflicts
	engine.allowDuplicateWeights = true

	// Test 1: Without configured dimensions, any rule should be allowed
	rule1 := NewRule("flexible_rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRoute", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule without configured dimensions: %v", err)
	}

	// Test 2: Configure dimensions
	err = engine.AddDimension(NewDimensionConfig("product", 0, true))
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("route", 1, false))
	if err != nil {
		t.Fatalf("Failed to add route dimension: %v", err)
	}

	// Test 3: Valid rule that matches configured dimensions
	rule2 := NewRule("valid_rule").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		Dimension("route", "TestRoute2", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add valid rule: %v", err)
	}

	// Test 4: Rule missing required dimension should fail
	rule3 := NewRule("missing_required").
		Dimension("route", "TestRoute3", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule3)
	if err == nil {
		t.Fatalf("Expected error for rule missing required dimension, but got none")
	}
	if err.Error() != "invalid rule: rule missing required dimension 'product'" {
		t.Errorf("Expected specific error for missing required dimension, got: %v", err)
	}

	// Test 5: Rule with extra dimension not in configuration should fail
	rule4 := NewRule("extra_dimension").
		Dimension("product", "TestProduct4", MatchTypeEqual).
		Dimension("route", "TestRoute4", MatchTypeEqual).
		Dimension("unknown_dim", "unknown_value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule4)
	if err == nil {
		t.Fatalf("Expected error for rule with extra dimension, but got none")
	}
	if err.Error() != "invalid rule: rule contains dimensions not in configuration: [unknown_dim]" {
		t.Errorf("Expected specific error for extra dimension, got: %v", err)
	}

	// Test 6: Rule with only required dimensions should be valid
	rule5 := NewRule("only_required").
		Dimension("product", "TestProduct5", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule5)
	if err != nil {
		t.Fatalf("Failed to add rule with only required dimensions: %v", err)
	}
}

func TestRebuild(t *testing.T) {
	// Create engine with mock persistence
	persistence := NewJSONPersistence("./test_data_rebuild")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-rebuild")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add initial test dimensions
	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add some rules with different weights to avoid conflicts
	rule1 := NewRule("rebuild_test_1").
		Dimension("product", "Product1", MatchTypeEqual).
		Dimension("route", "Route1", MatchTypeEqual).
		ManualWeight(1.0).
		Build()

	rule2 := NewRule("rebuild_test_2").
		Dimension("product", "Product2", MatchTypeEqual).
		Dimension("tool", "Tool2", MatchTypeEqual).
		ManualWeight(2.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule2: %v", err)
	}

	// Verify rules are loaded
	stats := engine.GetStats()
	if stats.TotalRules != 2 {
		t.Fatalf("Expected 2 rules before rebuild, got %d", stats.TotalRules)
	}

	// Perform a query to populate cache
	query := CreateQuery(map[string]string{
		"product": "Product1",
		"route":   "Route1",
	})
	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Failed to query before rebuild: %v", err)
	}
	if result == nil {
		t.Fatalf("Expected 1 result before rebuild, got none")
	}

	// Save current state to persistence
	err = engine.SaveToPersistence()
	if err != nil {
		t.Fatalf("Failed to save before rebuild: %v", err)
	}

	// Now manually add a rule to memory (not persisted) to test rebuild clears it
	tempRule := NewRule("temp_rule_not_persisted").
		Dimension("product", "TempProduct", MatchTypeEqual).
		Build()

	// Add directly to internal structures without persistence (simulate corrupted state)
	engine.rules["temp_rule_not_persisted"] = tempRule
	// Add to appropriate forest (default tenant/app since tempRule has empty tenant context)
	forestIndex := engine.getOrCreateForestIndex(tempRule.TenantID, tempRule.ApplicationID)
	forestIndex.AddRule(tempRule)
	engine.stats.TotalRules = len(engine.rules)

	// Verify temp rule exists in memory
	stats = engine.GetStats()
	if stats.TotalRules != 3 {
		t.Fatalf("Expected 3 rules after adding temp rule, got %d", stats.TotalRules)
	}

	// Perform rebuild - this should clear everything and reload from persistence
	err = engine.Rebuild()
	if err != nil {
		t.Fatalf("Failed to rebuild: %v", err)
	}

	// Verify rebuild restored only persisted data
	stats = engine.GetStats()
	if stats.TotalRules != 2 {
		t.Fatalf("Expected 2 rules after rebuild (temp rule should be gone), got %d", stats.TotalRules)
	}

	// Verify the temp rule is gone
	if _, exists := engine.rules["temp_rule_not_persisted"]; exists {
		t.Fatalf("Temp rule should not exist after rebuild")
	}

	// Verify cache was cleared (check by looking at cache stats)
	// The cache should be empty after rebuild
	cacheStats := engine.cache.Stats()
	totalEntries, ok := cacheStats["total_entries"].(int)
	if !ok {
		t.Fatalf("Expected total_entries to be an int in cache stats")
	}
	if totalEntries != 0 {
		t.Fatalf("Expected cache to be empty after rebuild, got %d entries", totalEntries)
	}

	// Verify original rules still work
	result, err = engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Failed to query after rebuild: %v", err)
	}
	if result == nil {
		t.Fatalf("Expected result after rebuild, got none")
	}
	if result.Rule.ID != "rebuild_test_1" {
		t.Fatalf("Expected rule 'rebuild_test_1' after rebuild, got %s", result.Rule.ID)
	}
}

func TestExcludeRules(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-exclude")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add multiple test rules
	rules := []*Rule{
		NewRule("rule1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).
			Build(),
		NewRule("rule2").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(8.0).
			Build(),
		NewRule("rule3").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(6.0).
			Build(),
		NewRule("rule4").
			Dimension("product", "ProductB", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(5.0).
			Build(),
	}

	// Allow duplicate weights for this test
	engine.allowDuplicateWeights = true

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	t.Run("NoExclusions", func(t *testing.T) {
		// Test without exclusions - should find the highest weight rule
		query := CreateQuery(map[string]string{
			"product": "ProductA",
			"route":   "main",
		})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "rule1" {
			t.Errorf("Expected rule 'rule1' (highest weight), got '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeHighestWeight", func(t *testing.T) {
		// Exclude the highest weight rule - should find the second highest
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"rule1"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "rule2" {
			t.Errorf("Expected rule 'rule2' (second highest weight), got '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeMultipleRules", func(t *testing.T) {
		// Exclude multiple rules
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"rule1", "rule2"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "rule3" {
			t.Errorf("Expected rule 'rule3' (third highest weight), got '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeAllMatchingRules", func(t *testing.T) {
		// Exclude all matching rules for ProductA
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"rule1", "rule2", "rule3"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result != nil {
			t.Errorf("Expected no match but got rule '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeNonExistentRule", func(t *testing.T) {
		// Exclude a rule that doesn't exist - should not affect results
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"nonexistent_rule"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "rule1" {
			t.Errorf("Expected rule 'rule1' (highest weight), got '%s'", result.Rule.ID)
		}
	})

	t.Run("EmptyExcludeList", func(t *testing.T) {
		// Empty exclude list - should behave normally
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "rule1" {
			t.Errorf("Expected rule 'rule1' (highest weight), got '%s'", result.Rule.ID)
		}
	})
}

func TestExcludeRulesWithFindAllMatches(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-exclude-all")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add multiple test rules
	rules := []*Rule{
		NewRule("all_rule1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).
			Build(),
		NewRule("all_rule2").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(8.0).
			Build(),
		NewRule("all_rule3").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(6.0).
			Build(),
	}

	// Allow duplicate weights for this test
	engine.allowDuplicateWeights = true

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	t.Run("FindAllWithoutExclusions", func(t *testing.T) {
		query := CreateQuery(map[string]string{
			"product": "ProductA",
			"route":   "main",
		})

		results, err := engine.FindAllMatches(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(results) != 3 {
			t.Fatalf("Expected 3 matches, got %d", len(results))
		}

		// Results should be ordered by weight (descending)
		expectedOrder := []string{"all_rule1", "all_rule2", "all_rule3"}
		for i, result := range results {
			if result.Rule.ID != expectedOrder[i] {
				t.Errorf("Expected rule '%s' at position %d, got '%s'", expectedOrder[i], i, result.Rule.ID)
			}
		}
	})

	t.Run("FindAllWithExclusions", func(t *testing.T) {
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"all_rule2"})

		results, err := engine.FindAllMatches(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(results) != 2 {
			t.Fatalf("Expected 2 matches, got %d", len(results))
		}

		// Should exclude all_rule2
		expectedRules := []string{"all_rule1", "all_rule3"}
		for i, result := range results {
			if result.Rule.ID != expectedRules[i] {
				t.Errorf("Expected rule '%s' at position %d, got '%s'", expectedRules[i], i, result.Rule.ID)
			}
		}

		// Verify all_rule2 is not in results
		for _, result := range results {
			if result.Rule.ID == "all_rule2" {
				t.Error("Rule 'all_rule2' should have been excluded")
			}
		}
	})

	t.Run("FindAllExcludeAllRules", func(t *testing.T) {
		query := CreateQueryWithExcludedRules(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"all_rule1", "all_rule2", "all_rule3"})

		results, err := engine.FindAllMatches(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(results) != 0 {
			t.Fatalf("Expected 0 matches, got %d", len(results))
		}
	})
}

func TestExcludeRulesWithTenants(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-exclude-tenants")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add rules for different tenants
	rules := []*Rule{
		NewRuleWithTenant("tenant1_rule1", "tenant1", "app1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).
			Build(),
		NewRuleWithTenant("tenant1_rule2", "tenant1", "app1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(8.0).
			Build(),
		NewRuleWithTenant("tenant2_rule1", "tenant2", "app1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(12.0).
			Build(),
	}

	// Allow duplicate weights for this test
	engine.allowDuplicateWeights = true

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	t.Run("ExcludeRulesWithTenant", func(t *testing.T) {
		// Exclude tenant1_rule1, should find tenant1_rule2
		query := CreateQueryWithTenantAndExcluded("tenant1", "app1", map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"tenant1_rule1"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "tenant1_rule2" {
			t.Errorf("Expected rule 'tenant1_rule2', got '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeOtherTenantRule", func(t *testing.T) {
		// Try to exclude tenant2 rule from tenant1 query - should have no effect
		query := CreateQueryWithTenantAndExcluded("tenant1", "app1", map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"tenant2_rule1"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		// Should still find tenant1_rule1 (highest weight for tenant1)
		if result.Rule.ID != "tenant1_rule1" {
			t.Errorf("Expected rule 'tenant1_rule1', got '%s'", result.Rule.ID)
		}
	})
}

func TestExcludeRulesWithDraftStatus(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-exclude-draft")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add rules with different statuses
	workingRule := NewRule("working_rule").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("route", "main", MatchTypeEqual).
		Status(RuleStatusWorking).
		ManualWeight(10.0).
		Build()

	draftRule := NewRule("draft_rule").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("route", "main", MatchTypeEqual).
		Status(RuleStatusDraft).
		ManualWeight(15.0). // Higher weight but draft
		Build()

	// Allow duplicate weights for this test
	engine.allowDuplicateWeights = true

	err = engine.AddRule(workingRule)
	if err != nil {
		t.Fatalf("Failed to add working rule: %v", err)
	}

	err = engine.AddRule(draftRule)
	if err != nil {
		t.Fatalf("Failed to add draft rule: %v", err)
	}

	t.Run("ExcludeWorkingRuleFromAllRulesQuery", func(t *testing.T) {
		// Query all rules but exclude the working rule - should find only draft
		query := CreateQueryWithAllRulesAndExcluded(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"working_rule"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "draft_rule" {
			t.Errorf("Expected rule 'draft_rule', got '%s'", result.Rule.ID)
		}
	})

	t.Run("ExcludeDraftRuleFromAllRulesQuery", func(t *testing.T) {
		// Query all rules but exclude the draft rule - should find working rule
		query := CreateQueryWithAllRulesAndExcluded(map[string]string{
			"product": "ProductA",
			"route":   "main",
		}, []string{"draft_rule"})

		result, err := engine.FindBestMatch(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result == nil {
			t.Fatal("Expected match but got none")
		}

		if result.Rule.ID != "working_rule" {
			t.Errorf("Expected rule 'working_rule', got '%s'", result.Rule.ID)
		}
	})
}
