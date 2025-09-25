package matcher

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func getDimensions() []*DimensionConfig {
	return []*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
		NewDimensionConfig("tool_id", 3, false),
		NewDimensionConfig("recipe", 4, false),
	}
}

// Helper function to add test dimensions for backward compatibility
func addTestDimensions(engine *InMemoryMatcher) error {
	for _, dim := range getDimensions() {
		if err := engine.AddDimension(dim); err != nil {
			return err
		}
	}
	return nil
}

// Helper function to add only the required test dimensions
func addTestDimensionsMinimal(engine *InMemoryMatcher) error {
	dimensions := []*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
	}

	engine.dimensionConfigs.LoadBulk(dimensions)
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

	err = addTestDimensionsMinimal(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Debug: Check if dimensions were added correctly
	t.Logf("Dimension count: %d", engine.dimensionConfigs.Count())
	if engine.dimensionConfigs.Count() == 0 {
		t.Fatal("No dimensions were configured")
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

	// Debug: Check if rule was added correctly
	t.Logf("Rule count: %d", len(engine.rules))

	// Debug: Check the rule details
	for id, rule := range engine.rules {
		t.Logf("Rule ID: %s, TenantID: '%s', AppID: '%s', Status: %s", id, rule.TenantID, rule.ApplicationID, rule.Status)
		for dimName, dimValue := range rule.Dimensions {
			t.Logf("  Dimension %s: %s (%s)", dimName, dimValue.Value, dimValue.MatchType)
		}
	}

	// Debug: Get forest stats
	forestIndex := engine.getOrCreateForestIndex("", "")
	stats := forestIndex.GetStats()
	t.Logf("Forest stats: %v", stats)

	// Test exact match with all rules (bypass status filtering)
	query := CreateQueryWithAllRules(map[string]string{
		"product": "TestProduct",
		"route":   "TestRoute",
		"tool":    "TestTool",
	})

	// Debug: Let's see what the query looks like
	t.Logf("Query values: %v", query.Values)
	t.Logf("Query include all rules: %v", query.IncludeAllRules)

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	addTestDimensions(engine)

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

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

	addTestDimensions(engine)

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

func TestNewInMemoryMatcherWithContext(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	ctx := context.Background()

	engine, err := NewInMemoryMatcherWithContext(ctx, persistence, nil, "test-node-context")
	if err != nil {
		t.Fatalf("Failed to create engine with context: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add a test rule
	rule := NewRule("context_test_rule").
		Dimension("product", "ContextProduct", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test basic matching
	query := CreateQuery(map[string]string{
		"product": "ContextProduct",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none")
	}

	if result.Rule.ID != "context_test_rule" {
		t.Errorf("Expected rule 'context_test_rule', got '%s'", result.Rule.ID)
	}
}

func TestNewInMemoryMatcherWithContextTimeout(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	engine, err := NewInMemoryMatcherWithContext(ctx, persistence, nil, "test-node-timeout")
	if err != nil {
		t.Fatalf("Failed to create engine with timeout context: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add a test rule
	rule := NewRule("timeout_test_rule").
		Dimension("product", "TimeoutProduct", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Wait for context to timeout
	time.Sleep(100 * time.Millisecond)

	// Test that operations still work (matcher should handle cancelled context gracefully)
	query := CreateQuery(map[string]string{
		"product": "TimeoutProduct",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed after context timeout: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none after context timeout")
	}

	if result.Rule.ID != "timeout_test_rule" {
		t.Errorf("Expected rule 'timeout_test_rule', got '%s'", result.Rule.ID)
	}
}

func TestNewInMemoryMatcherWithContextCancellation(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	ctx, cancel := context.WithCancel(context.Background())

	engine, err := NewInMemoryMatcherWithContext(ctx, persistence, nil, "test-node-cancel")
	if err != nil {
		t.Fatalf("Failed to create engine with cancellable context: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Add a test rule
	rule := NewRule("cancel_test_rule").
		Dimension("product", "CancelProduct", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Cancel the context
	cancel()

	// Give a moment for cancellation to propagate
	time.Sleep(10 * time.Millisecond)

	// Test that operations still work (matcher should handle cancelled context gracefully)
	query := CreateQuery(map[string]string{
		"product": "CancelProduct",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed after context cancellation: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match but got none after context cancellation")
	}

	if result.Rule.ID != "cancel_test_rule" {
		t.Errorf("Expected rule 'cancel_test_rule', got '%s'", result.Rule.ID)
	}
}

func TestNewInMemoryMatcherWithContextEquivalentToDefault(t *testing.T) {
	// Test that NewInMemoryMatcherWithContext with background context behaves the same as NewInMemoryMatcher
	persistence1 := NewJSONPersistence("./test_data_default")
	persistence2 := NewJSONPersistence("./test_data_context")

	ctx := context.Background()

	engine1, err := NewInMemoryMatcher(persistence1, nil, "test-node-default")
	if err != nil {
		t.Fatalf("Failed to create default engine: %v", err)
	}
	defer engine1.Close()

	engine2, err := NewInMemoryMatcherWithContext(ctx, persistence2, nil, "test-node-context")
	if err != nil {
		t.Fatalf("Failed to create context engine: %v", err)
	}
	defer engine2.Close()

	// Add same dimensions to both
	err = addTestDimensions(engine1)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions for default engine: %v", err)
	}

	err = addTestDimensions(engine2)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions for context engine: %v", err)
	}

	// Add same rule to both
	rule := NewRule("equivalent_test_rule").
		Dimension("product", "EquivalentProduct", MatchTypeEqual).
		ManualWeight(15.0).
		Build()

	err = engine1.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule to default engine: %v", err)
	}

	rule2 := NewRule("equivalent_test_rule").
		Dimension("product", "EquivalentProduct", MatchTypeEqual).
		ManualWeight(15.0).
		Build()

	err = engine2.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule to context engine: %v", err)
	}

	// Test same query on both
	query := CreateQuery(map[string]string{
		"product": "EquivalentProduct",
	})

	result1, err := engine1.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed on default engine: %v", err)
	}

	result2, err := engine2.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Query failed on context engine: %v", err)
	}

	// Results should be equivalent
	if result1 == nil || result2 == nil {
		t.Fatal("Both engines should return matches")
	}

	if result1.Rule.ID != result2.Rule.ID {
		t.Errorf("Expected same rule ID, got '%s' vs '%s'", result1.Rule.ID, result2.Rule.ID)
	}

	if result1.TotalWeight != result2.TotalWeight {
		t.Errorf("Expected same weight %.2f, got %.2f vs %.2f", result1.TotalWeight, result1.TotalWeight, result2.TotalWeight)
	}
}

func TestMatchTypeBasedWeights(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Create dimension config with different weights per match type
	regionConfig := NewDimensionConfig("region", 0, false) // Default weight
	regionConfig.SetWeight(MatchTypeEqual, 10.0)           // Higher weight for exact matches
	regionConfig.SetWeight(MatchTypePrefix, 7.0)           // Medium weight for prefix matches
	regionConfig.SetWeight(MatchTypeSuffix, 6.0)           // Lower weight for suffix matches
	regionConfig.SetWeight(MatchTypeAny, 3.0)              // Lowest weight for any matches

	envConfig := NewDimensionConfig("env", 1, false) // Default weight
	envConfig.SetWeight(MatchTypeEqual, 8.0)         // High weight for exact env matches
	envConfig.SetWeight(MatchTypeAny, 1.0)           // Low weight for any env matches

	// Add dimension configs
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Create rules with different match types
	rule1 := NewRule("exact-match").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	rule2 := NewRule("prefix-match").
		Dimension("region", "us-", MatchTypePrefix).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	rule3 := NewRule("suffix-match").
		Dimension("region", "-west", MatchTypeSuffix).
		Dimension("env", "any", MatchTypeAny).
		Build()

	rule4 := NewRule("any-match").
		Dimension("region", "any", MatchTypeAny).
		Dimension("env", "any", MatchTypeAny).
		Build()

	// Add rules
	rules := []*Rule{rule1, rule2, rule3, rule4}
	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test query that matches all rules
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 4 {
		t.Fatalf("Expected 4 matches, got %d", len(matches))
	}

	// Verify weights are calculated correctly based on match types
	expectedWeights := map[string]float64{
		"exact-match":  18.0, // region: 10.0 (Equal) + env: 8.0 (Equal)
		"prefix-match": 15.0, // region: 7.0 (Prefix) + env: 8.0 (Equal)
		"suffix-match": 7.0,  // region: 6.0 (Suffix) + env: 1.0 (Any)
		"any-match":    4.0,  // region: 3.0 (Any) + env: 1.0 (Any)
	}

	// Verify matches are sorted by weight (highest first)
	if matches[0].Rule.ID != "exact-match" {
		t.Errorf("Expected highest weight rule 'exact-match' first, got '%s'", matches[0].Rule.ID)
	}

	for _, match := range matches {
		expectedWeight := expectedWeights[match.Rule.ID]
		if match.TotalWeight != expectedWeight {
			t.Errorf("Rule %s: expected weight %.1f, got %.1f", match.Rule.ID, expectedWeight, match.TotalWeight)
		}
	}

	t.Logf("Match type-based weights working correctly:")
	for _, match := range matches {
		t.Logf("  Rule %s: weight %.1f", match.Rule.ID, match.TotalWeight)
	}
}

func TestDynamicConfigsWithMatchTypeWeights(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add a basic dimension config with default weights
	categoryConfig := NewDimensionConfig("category", 0, false)
	categoryConfig.SetWeight(MatchTypeEqual, 10.0)
	categoryConfig.SetWeight(MatchTypePrefix, 7.0)

	err = engine.AddDimension(categoryConfig)
	if err != nil {
		t.Fatalf("Failed to add category dimension: %v", err)
	}

	// Add a rule
	rule := NewRule("test-rule").
		Dimension("category", "urgent", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test 1: Query with default dimension configs
	query1 := &QueryRule{
		Values: map[string]string{
			"category": "urgent",
		},
	}

	matches1, err := engine.FindAllMatches(query1)
	if err != nil {
		t.Fatalf("FindAllMatches with default configs failed: %v", err)
	}

	expectedWeight1 := 10.0 // category: 10.0 (Equal match type)
	if matches1[0].TotalWeight != expectedWeight1 {
		t.Errorf("Expected weight %.1f with default configs, got %.1f", expectedWeight1, matches1[0].TotalWeight)
	}

	// Test 2: Query with dynamic dimension configs that override weights per match type
	dynamicCategoryConfig := NewDimensionConfig("category", 0, false)
	dynamicCategoryConfig.SetWeight(MatchTypeEqual, 50.0)  // Much higher weight for exact matches
	dynamicCategoryConfig.SetWeight(MatchTypePrefix, 30.0) // High weight for prefix matches

	dynamicConfigs := NewDimensionConfigs()
	dynamicConfigs.Add(dynamicCategoryConfig)

	query2 := &QueryRule{
		Values: map[string]string{
			"category": "urgent",
		},
		DynamicDimensionConfigs: dynamicConfigs,
	}

	matches2, err := engine.FindAllMatches(query2)
	if err != nil {
		t.Fatalf("FindAllMatches with dynamic configs failed: %v", err)
	}

	expectedWeight2 := 50.0 // category: 50.0 (Equal match type from dynamic config)
	if matches2[0].TotalWeight != expectedWeight2 {
		t.Errorf("Expected weight %.1f with dynamic configs, got %.1f", expectedWeight2, matches2[0].TotalWeight)
	}

	t.Logf("Dynamic match type weights working correctly:")
	t.Logf("  Default config weight: %.1f", matches1[0].TotalWeight)
	t.Logf("  Dynamic config weight: %.1f", matches2[0].TotalWeight)
}

func TestMixedMatchTypesInSingleRule(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Create dimension configs with specific weights per match type
	userConfig := NewDimensionConfig("user_id", 0, false)
	userConfig.SetWeight(MatchTypePrefix, 20.0)

	actionConfig := NewDimensionConfig("action", 1, false)
	actionConfig.SetWeight(MatchTypeEqual, 15.0)
	actionConfig.SetWeight(MatchTypeSuffix, 8.0)

	serviceConfig := NewDimensionConfig("service", 2, false)
	serviceConfig.SetWeight(MatchTypeAny, 5.0)

	// Add dimension configs
	configs := []*DimensionConfig{userConfig, actionConfig, serviceConfig}
	for _, config := range configs {
		err = engine.AddDimension(config)
		if err != nil {
			t.Fatalf("Failed to add dimension %s: %v", config.Name, err)
		}
	}

	// Create a rule that uses different match types for each dimension
	rule := NewRule("mixed-match-types").
		Dimension("user_id", "admin_", MatchTypePrefix). // user_id starts with "admin_"
		Dimension("action", "_delete", MatchTypeSuffix). // action ends with "_delete"
		Dimension("service", "any", MatchTypeAny).       // service can be anything
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test query that matches the rule
	query := &QueryRule{
		Values: map[string]string{
			"user_id": "admin_john",
			"action":  "user_delete",
			"service": "user_management",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("Expected 1 match, got %d", len(matches))
	}

	// Expected weight: user_id (20.0 prefix) + action (8.0 suffix) + service (5.0 any) = 33.0
	expectedWeight := 33.0
	if matches[0].TotalWeight != expectedWeight {
		t.Errorf("Expected weight %.1f, got %.1f", expectedWeight, matches[0].TotalWeight)
	}

	t.Logf("Mixed match types rule weight: %.1f", matches[0].TotalWeight)
	t.Logf("  user_id (prefix): 20.0")
	t.Logf("  action (suffix): 8.0")
	t.Logf("  service (any): 5.0")
	t.Logf("  Total: %.1f", matches[0].TotalWeight)
}

func TestFallbackToZeroWeight(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Create dimension config with only some match type weights defined
	statusConfig := NewDimensionConfig("status", 0, false) // No default weight anymore
	statusConfig.SetWeight(MatchTypeEqual, 25.0)           // Only define weight for Equal match type
	// MatchTypePrefix, MatchTypeSuffix, MatchTypeAny will use 0.0 weight

	err = engine.AddDimension(statusConfig)
	if err != nil {
		t.Fatalf("Failed to add status dimension: %v", err)
	}

	// Create rules with different match types
	equalRule := NewRule("equal-rule").
		Dimension("status", "active", MatchTypeEqual).
		Build()

	prefixRule := NewRule("prefix-rule").
		Dimension("status", "act", MatchTypePrefix).
		Build()

	// Add rules
	rules := []*Rule{equalRule, prefixRule}
	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test query
	query := &QueryRule{
		Values: map[string]string{
			"status": "active",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 2 {
		t.Fatalf("Expected 2 matches, got %d", len(matches))
	}

	// Find matches by rule ID
	var equalMatch, prefixMatch *MatchResult
	for _, match := range matches {
		switch match.Rule.ID {
		case "equal-rule":
			equalMatch = match
		case "prefix-rule":
			prefixMatch = match
		}
	}

	// Verify weights
	expectedEqualWeight := 25.0 // Uses specific weight for MatchTypeEqual
	expectedPrefixWeight := 0.0 // Falls back to 0.0 weight (no DefaultWeight anymore)

	if equalMatch.TotalWeight != expectedEqualWeight {
		t.Errorf("Equal rule: expected weight %.1f, got %.1f", expectedEqualWeight, equalMatch.TotalWeight)
	}

	if prefixMatch.TotalWeight != expectedPrefixWeight {
		t.Errorf("Prefix rule: expected weight %.1f, got %.1f", expectedPrefixWeight, prefixMatch.TotalWeight)
	}

	t.Logf("Fallback to default weight working correctly:")
	t.Logf("  Equal match (configured): %.1f", equalMatch.TotalWeight)
	t.Logf("  Prefix match (fallback): %.1f", prefixMatch.TotalWeight)
}

func TestDAGStructureWithSharedNodes(t *testing.T) {
	// Set up dimension configs for the test
	dimensionConfigs := NewDimensionConfigs()
	dimensionConfigs.Add(NewDimensionConfig("product", 0, true))
	dimensionConfigs.Add(NewDimensionConfig("route", 1, false))
	dimensionConfigs.Add(NewDimensionConfig("tool", 2, false))

	forest := CreateRuleForest(dimensionConfigs)

	// Create rules that will demonstrate DAG-like sharing
	// These rules share some dimensions but have different match types
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypePrefix}, // Prefix match
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual}, // Exact match - different from rule1
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeSuffix}, // Suffix match
		},
	}
	// Add all rules
	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Test query that should match rule1 (prefix: "laser" prefix of "laser_v2")
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser_v2",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	t.Logf("Query1 candidates: %d", len(candidates1))

	// Should find rule1 (prefix match) but not rule2 (exact match) or rule3 (suffix match)
	foundRule1 := false
	for _, candidate := range candidates1 {
		if candidate.ID == "rule1" {
			foundRule1 = true
		}
		t.Logf("Found candidate: %s", candidate.ID)
	}
	if !foundRule1 {
		t.Error("Should find rule1 with prefix match")
	}

	// Test query that should match rule2 (exact: "laser" equals "laser")
	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates2 := forest.FindCandidateRules(query2)
	t.Logf("Query2 candidates: %d", len(candidates2))

	// Should find all rules because:
	// - rule1: "laser" starts with "laser" (prefix match)
	// - rule2: "laser" equals "laser" (exact match)
	// - rule3: "laser" ends with "laser" (suffix match)
	if len(candidates2) < 3 {
		t.Error("Should find all three rules for exact 'laser' match")
	}

	// Test query that should match rule3 (suffix: "tool_laser" ends with "laser")
	query3 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "tool_laser",
		},
	}

	candidates3 := forest.FindCandidateRules(query3)
	t.Logf("Query3 candidates: %d", len(candidates3))

	foundRule3 := false
	for _, candidate := range candidates3 {
		if candidate.ID == "rule3" {
			foundRule3 = true
		}
	}
	if !foundRule3 {
		t.Error("Should find rule3 with suffix match")
	}

	// Test removal of shared nodes
	forest.RemoveRule(rule2)

	// Query2 should now have fewer results
	candidates2After := forest.FindCandidateRules(query2)
	if len(candidates2After) >= len(candidates2) {
		t.Error("Should have fewer candidates after removing rule2")
	}

	// Query1 and Query3 should still work (rule1 and rule3 should remain)
	candidates1After := forest.FindCandidateRules(query1)
	candidates3After := forest.FindCandidateRules(query3)

	foundRule1After := false
	foundRule3After := false

	for _, candidate := range candidates1After {
		if candidate.ID == "rule1" {
			foundRule1After = true
		}
	}

	for _, candidate := range candidates3After {
		if candidate.ID == "rule3" {
			foundRule3After = true
		}
	}

	if !foundRule1After {
		t.Error("Rule1 should still be found after removing rule2")
	}

	if !foundRule3After {
		t.Error("Rule3 should still be found after removing rule2")
	}
}

func TestDAGStatistics(t *testing.T) {
	// Set up dimension configs for the test
	dimensionConfigs := NewDimensionConfigs()
	dimensionConfigs.Add(NewDimensionConfig("product", 0, true))
	dimensionConfigs.Add(NewDimensionConfig("tool", 1, false))

	forest := CreateRuleForest(dimensionConfigs)

	// Create rules that demonstrate node sharing in DAG structure
	for i := 0; i < 10; i++ {
		for _, matchType := range []MatchType{MatchTypeEqual, MatchTypePrefix, MatchTypeSuffix} {
			rule := &Rule{
				ID: fmt.Sprintf("rule_%d_%s", i, matchType.String()),
				Dimensions: map[string]*DimensionValue{
					"product": {DimensionName: "product", Value: "CommonProduct", MatchType: MatchTypeEqual},
					"tool":    {DimensionName: "tool", Value: "shared_tool", MatchType: matchType},
				},
			}
			forest.AddRule(rule)
		}
	}

	stats := forest.GetStats()
	t.Logf("DAG Forest stats: %+v", stats)

	// Should have significant node sharing
	if totalRules, exists := stats["total_rules"]; exists {
		t.Logf("Total rules: %d", totalRules.(int))
		if totalRules.(int) != 30 { // 10 * 3 match types
			t.Errorf("Expected 30 rules, got %d", totalRules.(int))
		}
	}

	// Should have some shared nodes
	if sharedCount, exists := stats["shared_nodes_count"]; exists && sharedCount.(int) > 0 {
		t.Logf("Shared nodes: %d", sharedCount.(int))
	} else {
		t.Log("No shared nodes detected - this might be expected with current implementation")
	}
}

func TestMultiTenantFunctionality(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "multitenant-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test NewRuleWithTenant
	rule := NewRuleWithTenant("test-rule", "tenant1", "app1").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Build()

	if rule.TenantID != "tenant1" {
		t.Errorf("Expected tenant 'tenant1', got '%s'", rule.TenantID)
	}

	if rule.ApplicationID != "app1" {
		t.Errorf("Expected application 'app1', got '%s'", rule.ApplicationID)
	}

	// Test Tenant method
	rule2 := NewRule("test-rule-2").
		Tenant("tenant2").
		Application("app2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		Build()

	if rule2.TenantID != "tenant2" {
		t.Errorf("Expected tenant 'tenant2', got '%s'", rule2.TenantID)
	}

	if rule2.ApplicationID != "app2" {
		t.Errorf("Expected application 'app2', got '%s'", rule2.ApplicationID)
	}

	// Test CreateQueryWithTenant
	query := CreateQueryWithTenant("tenant1", "app1", map[string]string{
		"product": "TestProduct",
	})

	if query.TenantID != "tenant1" {
		t.Errorf("Expected query tenant 'tenant1', got '%s'", query.TenantID)
	}

	if query.ApplicationID != "app1" {
		t.Errorf("Expected query application 'app1', got '%s'", query.ApplicationID)
	}

	// Test CreateQueryWithAllRules
	queryAll := CreateQueryWithAllRules(map[string]string{
		"product": "TestProduct",
	})

	if !queryAll.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	// Test CreateQueryWithAllRulesAndTenant
	queryAllTenant := CreateQueryWithAllRulesAndTenant("tenant3", "app3", map[string]string{
		"product": "TestProduct",
	})

	if queryAllTenant.TenantID != "tenant3" {
		t.Errorf("Expected tenant 'tenant3', got '%s'", queryAllTenant.TenantID)
	}

	if queryAllTenant.ApplicationID != "app3" {
		t.Errorf("Expected application 'app3', got '%s'", queryAllTenant.ApplicationID)
	}

	if !queryAllTenant.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	// Test GetTenantContext
	tenantID, appID := rule.GetTenantContext()
	if tenantID != "tenant1" || appID != "app1" {
		t.Errorf("Expected tenant context 'tenant1:app1', got '%s:%s'", tenantID, appID)
	}

	queryTenantID, queryAppID := query.GetTenantContext()
	if queryTenantID != "tenant1" || queryAppID != "app1" {
		t.Errorf("Expected query context 'tenant1:app1', got '%s:%s'", queryTenantID, queryAppID)
	}

	// Test MatchesTenantContext
	if !rule.MatchesTenantContext("tenant1", "app1") {
		t.Error("Expected rule to match query tenant context")
	}

	// Test with different tenant context
	if rule.MatchesTenantContext("other-tenant", "other-app") {
		t.Error("Expected rule not to match different tenant context")
	}
}

func TestRuleStatus(t *testing.T) {
	// Test Status method in RuleBuilder
	rule := NewRule("status-test").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Status(RuleStatusDraft).
		Build()

	if rule.Status != RuleStatusDraft {
		t.Errorf("Expected status %v, got %v", RuleStatusDraft, rule.Status)
	}

	// Test with working status
	rule2 := NewRule("status-test-2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		Status(RuleStatusWorking).
		Build()

	if rule2.Status != RuleStatusWorking {
		t.Errorf("Expected status %v, got %v", RuleStatusWorking, rule2.Status)
	}
}

func TestSetAllowDuplicateWeights(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewMatcherEngine(persistence, nil, "duplicate-weights-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension config first
	dimConfig := NewDimensionConfig("product", 0, true)
	err = engine.AddDimension(dimConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension config: %v", err)
	}

	// Test SetAllowDuplicateWeights
	engine.SetAllowDuplicateWeights(true)

	// Add two rules with the same weight
	rule1 := NewRule("rule1").
		Dimension("product", "TestProduct", MatchTypeEqual).
		ManualWeight(10.0).
		Build()

	rule2 := NewRule("rule2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		ManualWeight(10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	// This should succeed because duplicate weights are allowed
	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule2 with duplicate weight: %v", err)
	}

	// Test disabling duplicate weights
	engine.SetAllowDuplicateWeights(false)

	// Add a rule that will intersect with the next one
	rule3_intersect := NewRule("rule3_intersect").
		Dimension("product", "Test", MatchTypePrefix). // This will intersect with prefix "Test"
		ManualWeight(15.0).
		Build()

	err = engine.AddRule(rule3_intersect)
	if err != nil {
		t.Fatalf("Failed to add intersecting rule: %v", err)
	}

	// This rule should conflict because it intersects and has the same weight
	rule3_conflict := NewRule("rule3_conflict").
		Dimension("product", "TestProduct", MatchTypeEqual). // "TestProduct" matches prefix "Test"
		ManualWeight(15.0).                                  // Same weight as rule3_intersect
		Build()

	// This should fail because duplicate weights are not allowed for intersecting rules
	err = engine.AddRule(rule3_conflict)
	if err == nil {
		t.Error("Expected error when adding rule with duplicate weight that intersects")
	}
}

func TestInitializeDimension(t *testing.T) {
	// Create forest with dimension configs
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
	}, nil)

	forest := CreateRuleForest(dimensionConfigs)

	// Test InitializeDimension
	forest.InitializeDimension("region")
	// InitializeDimension doesn't return error, so we just test it doesn't panic

	// Test initializing existing dimension (should not cause error)
	forest.InitializeDimension("product")
	// Again, just test it doesn't panic
}

func TestMatchTypeString(t *testing.T) {
	// Test String method for MatchType
	tests := []struct {
		matchType MatchType
		expected  string
	}{
		{MatchTypeEqual, "equal"},
		{MatchTypePrefix, "prefix"},
		{MatchTypeSuffix, "suffix"},
		{MatchTypeAny, "any"},
		{MatchType(99), "unknown"}, // Test unknown match type
	}

	for _, test := range tests {
		result := test.matchType.String()
		if result != test.expected {
			t.Errorf("Expected %s, got %s for match type %v", test.expected, result, test.matchType)
		}
	}
}
