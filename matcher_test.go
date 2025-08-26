package matcher

import (
	"fmt"
	"testing"
	"time"
)

// Helper function to add test dimensions for backward compatibility
func addTestDimensions(engine *InMemoryMatcher) error {
	dimensions := []*DimensionConfig{
		{Name: "product", Index: 0, Required: true, Weight: 10.0},
		{Name: "route", Index: 1, Required: false, Weight: 5.0},
		{Name: "tool", Index: 2, Required: false, Weight: 8.0},
		{Name: "tool_id", Index: 3, Required: false, Weight: 3.0},
		{Name: "recipe", Index: 4, Required: false, Weight: 12.0},
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
		Dimension("product", "TestProduct", MatchTypeEqual, 10.0).
		Dimension("route", "TestRoute", MatchTypeEqual, 5.0).
		Dimension("tool", "TestTool", MatchTypeEqual, 8.0).
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

	if result.TotalWeight != 23.0 { // 10 + 5 + 8
		t.Errorf("Expected weight 23.0, got %.1f", result.TotalWeight)
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
		Dimension("product", "Test", MatchTypePrefix, 10.0).
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
		Dimension("product", "Product", MatchTypeSuffix, 10.0).
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
		Dimension("product", "", MatchTypeAny, 1.0).
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
		Dimension("product", "Test", MatchTypeEqual, 100.0).
		Build()

	// Add low weight rule
	lowRule := NewRule("low_weight").
		Dimension("product", "Test", MatchTypeEqual, 1.0).
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
		Dimension("product", "Test", MatchTypeEqual, 100.0).
		Build()

	// Add rule with low calculated weight but high manual weight
	highManualRule := NewRule("high_manual").
		Dimension("product", "Test", MatchTypeEqual, 1.0).
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
		Dimension("product", "CacheTest", MatchTypeEqual, 10.0).
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
			Dimension("product", fmt.Sprintf("Product%d", i), MatchTypeEqual, 10.0).
			Dimension("route", fmt.Sprintf("Route%d", i), MatchTypeEqual, 5.0).
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
	customDim := &DimensionConfig{
		Name:     "custom_dimension",
		Index:    5,
		Required: false,
		Weight:   20.0,
	}

	err = engine.AddDimension(customDim)
	if err != nil {
		t.Fatalf("Failed to add custom dimension: %v", err)
	}

	// Add rule using custom dimension and required dimensions
	rule := NewRule("custom_rule").
		Dimension("product", "CustomProduct", MatchTypeEqual, 10.0). // Required dimension
		Dimension("custom_dimension", "custom_value", MatchTypeEqual, 20.0).
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
		Dimension("product", "EventProduct", MatchTypeEqual, 10.0).
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
		Dimension("product", "TestProduct", MatchTypeEqual, 10.0).
		Dimension("route", "TestRoute", MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule without configured dimensions: %v", err)
	}

	// Test 2: Configure dimensions
	err = engine.AddDimension(&DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	})
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(&DimensionConfig{
		Name:     "route",
		Index:    1,
		Required: false,
		Weight:   5.0,
	})
	if err != nil {
		t.Fatalf("Failed to add route dimension: %v", err)
	}

	// Test 3: Valid rule that matches configured dimensions
	rule2 := NewRule("valid_rule").
		Dimension("product", "TestProduct2", MatchTypeEqual, 10.0).
		Dimension("route", "TestRoute2", MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add valid rule: %v", err)
	}

	// Test 4: Rule missing required dimension should fail
	rule3 := NewRule("missing_required").
		Dimension("route", "TestRoute3", MatchTypeEqual, 5.0).
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
		Dimension("product", "TestProduct4", MatchTypeEqual, 10.0).
		Dimension("route", "TestRoute4", MatchTypeEqual, 5.0).
		Dimension("unknown_dim", "unknown_value", MatchTypeEqual, 8.0).
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
		Dimension("product", "TestProduct5", MatchTypeEqual, 10.0).
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

	// Add some rules
	rule1 := NewRule("rebuild_test_1").
		Dimension("product", "Product1", MatchTypeEqual, 10.0).
		Dimension("route", "Route1", MatchTypeEqual, 5.0).
		Build()

	rule2 := NewRule("rebuild_test_2").
		Dimension("product", "Product2", MatchTypeEqual, 10.0).
		Dimension("tool", "Tool2", MatchTypeEqual, 8.0).
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
		Dimension("product", "TempProduct", MatchTypeEqual, 10.0).
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
