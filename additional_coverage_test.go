package matcher

import (
	"testing"
	"time"
)

func TestDimensionConfig_GetWeight(t *testing.T) {
	// Test case 1: GetWeight with defined match type weights
	config := NewDimensionConfig("test", 0, false, 5.0)
	config.SetWeight(MatchTypeEqual, 10.0)
	config.SetWeight(MatchTypePrefix, 7.0)
	config.SetWeight(MatchTypeAny, 2.0)

	// Test getting defined weights
	if weight := config.GetWeight(MatchTypeEqual); weight != 10.0 {
		t.Errorf("Expected weight 10.0 for Equal match type, got %.1f", weight)
	}

	if weight := config.GetWeight(MatchTypePrefix); weight != 7.0 {
		t.Errorf("Expected weight 7.0 for Prefix match type, got %.1f", weight)
	}

	if weight := config.GetWeight(MatchTypeAny); weight != 2.0 {
		t.Errorf("Expected weight 2.0 for Any match type, got %.1f", weight)
	}

	// Test getting undefined weight (should return default)
	if weight := config.GetWeight(MatchTypeSuffix); weight != 5.0 {
		t.Errorf("Expected default weight 5.0 for undefined Suffix match type, got %.1f", weight)
	}

	// Test case 2: GetWeight with no defined weights (all should return default)
	emptyConfig := NewDimensionConfig("empty", 1, true, 15.0)

	matchTypes := []MatchType{MatchTypeEqual, MatchTypePrefix, MatchTypeSuffix, MatchTypeAny}
	for _, mt := range matchTypes {
		if weight := emptyConfig.GetWeight(mt); weight != 15.0 {
			t.Errorf("Expected default weight 15.0 for %s match type, got %.1f", mt, weight)
		}
	}

	// Test case 3: GetWeight after setting and overriding weights
	overrideConfig := NewDimensionConfig("override", 2, false, 3.0)

	// Set initial weight
	overrideConfig.SetWeight(MatchTypeEqual, 8.0)
	if weight := overrideConfig.GetWeight(MatchTypeEqual); weight != 8.0 {
		t.Errorf("Expected weight 8.0 after initial set, got %.1f", weight)
	}

	// Override the weight
	overrideConfig.SetWeight(MatchTypeEqual, 12.0)
	if weight := overrideConfig.GetWeight(MatchTypeEqual); weight != 12.0 {
		t.Errorf("Expected weight 12.0 after override, got %.1f", weight)
	}

	// Other match types should still return default
	if weight := overrideConfig.GetWeight(MatchTypePrefix); weight != 3.0 {
		t.Errorf("Expected default weight 3.0 for Prefix after override, got %.1f", weight)
	}
}

func TestDimensionConfig_SetWeightFunction(t *testing.T) {
	config := NewDimensionConfig("test", 0, false, 1.0)

	// Test setting weights for all match types
	weights := map[MatchType]float64{
		MatchTypeEqual:  10.0,
		MatchTypePrefix: 7.5,
		MatchTypeSuffix: 5.0,
		MatchTypeAny:    2.5,
	}

	for matchType, weight := range weights {
		config.SetWeight(matchType, weight)
	}

	// Verify all weights were set correctly
	for matchType, expectedWeight := range weights {
		actualWeight := config.GetWeight(matchType)
		if actualWeight != expectedWeight {
			t.Errorf("Match type %s: expected weight %.1f, got %.1f", matchType, expectedWeight, actualWeight)
		}
	}

	// Test that the weights map was properly initialized
	if config.Weights == nil {
		t.Error("Weights map should not be nil after setting weights")
	}

	if len(config.Weights) != 4 {
		t.Errorf("Expected 4 weights in map, got %d", len(config.Weights))
	}
}

func TestNewDimensionConfigWithWeightsFunction(t *testing.T) {
	weights := map[MatchType]float64{
		MatchTypeEqual:  15.0,
		MatchTypePrefix: 10.0,
		MatchTypeSuffix: 8.0,
		MatchTypeAny:    3.0,
	}

	config := NewDimensionConfigWithWeights("weighted", 1, true, weights, 5.0)

	if config.Name != "weighted" {
		t.Errorf("Expected name 'weighted', got '%s'", config.Name)
	}

	if config.Index != 1 {
		t.Errorf("Expected index 1, got %d", config.Index)
	}

	if !config.Required {
		t.Error("Expected required to be true")
	}

	if config.DefaultWeight != 5.0 {
		t.Errorf("Expected default weight 5.0, got %.1f", config.DefaultWeight)
	}

	// Test that all weights were set correctly
	for matchType, expectedWeight := range weights {
		actualWeight := config.GetWeight(matchType)
		if actualWeight != expectedWeight {
			t.Errorf("Match type %s: expected weight %.1f, got %.1f", matchType, expectedWeight, actualWeight)
		}
	}

	// Test that weights map is properly initialized
	if config.Weights == nil {
		t.Error("Weights map should not be nil")
	}

	if len(config.Weights) != 4 {
		t.Errorf("Expected 4 weights in map, got %d", len(config.Weights))
	}
}

func TestInitializeDimensionFunction(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Access the forest index to call InitializeDimension
	forestIndex := engine.matcher.getOrCreateForestIndex("", "")

	// Call InitializeDimension (this was at 0% coverage)
	// The function takes a string, not a DimensionConfig
	forestIndex.InitializeDimension("test_dimension")

	// Since InitializeDimension is a no-op, we just verify it doesn't crash
	// and we can still add dimensions normally
	config := NewDimensionConfig("test_dimension", 0, false, 10.0)
	config.SetWeight(MatchTypeEqual, 20.0)
	config.SetWeight(MatchTypePrefix, 15.0)

	err = engine.AddDimension(config)
	if err != nil {
		t.Fatalf("Failed to add dimension after initialization: %v", err)
	}

	// Test that we can add a rule using this dimension
	rule := NewRule("test-rule").
		Dimension("test_dimension", "test-value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule after dimension initialization: %v", err)
	}

	// Verify we can query using this dimension
	query := &QueryRule{
		Values: map[string]string{
			"test_dimension": "test-value",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}

	if len(matches) > 0 && matches[0].Rule.ID != "test-rule" {
		t.Errorf("Expected rule 'test-rule', got '%s'", matches[0].Rule.ID)
	}
}

func TestDeleteDimensionFunction(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add a dimension
	config := NewDimensionConfig("removable", 0, false, 5.0)
	err = engine.AddDimension(config)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add a rule using this dimension
	rule := NewRule("test-rule").
		Dimension("removable", "value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the rule matches before deletion
	query := &QueryRule{
		Values: map[string]string{
			"removable": "value",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match before deletion, got %d", len(matches))
	}

	// Test deleteDimension function (this was at 0% coverage)
	err = engine.matcher.deleteDimension("removable")
	if err != nil {
		t.Fatalf("deleteDimension failed: %v", err)
	}

	// Verify the dimension was removed from the list
	dimensions, err := engine.ListDimensions()
	if err != nil {
		t.Fatalf("ListDimensions failed: %v", err)
	}

	for _, dim := range dimensions {
		if dim.Name == "removable" {
			t.Error("Dimension 'removable' should have been deleted")
		}
	}

	// Test deleting a non-existent dimension (should not error, just do nothing)
	initialDimCount := len(dimensions)
	err = engine.matcher.deleteDimension("non-existent")
	if err != nil {
		t.Fatalf("deleteDimension with non-existent dimension failed: %v", err)
	}

	// Verify dimension count didn't change
	dimensions, err = engine.ListDimensions()
	if err != nil {
		t.Fatalf("ListDimensions failed after deleting non-existent: %v", err)
	}

	if len(dimensions) != initialDimCount {
		t.Errorf("Dimension count changed after deleting non-existent dimension: expected %d, got %d", initialDimCount, len(dimensions))
	}
}

func TestCacheCleanupExpired(t *testing.T) {
	// Create a cache with very short TTL for testing
	cache := NewQueryCache(100, 1*time.Second) // 1 second TTL

	// Create mock results for testing
	mockResult := &MatchResult{
		Rule:        &Rule{ID: "test"},
		TotalWeight: 1.0,
	}

	// Create mock queries
	query1 := &QueryRule{Values: map[string]string{"key": "value1"}}
	query2 := &QueryRule{Values: map[string]string{"key": "value2"}}

	// Add entries with custom TTL - one that will expire quickly
	cache.SetWithTTL(query1, mockResult, 1*time.Millisecond) // Very short TTL
	cache.Set(query2, mockResult)                            // Normal TTL

	initialSize := cache.Size()
	if initialSize != 2 {
		t.Errorf("Expected initial cache size 2, got %d", initialSize)
	}

	// Wait for the first entry to expire
	time.Sleep(10 * time.Millisecond)

	// Call CleanupExpired (this was at 70% coverage, let's get it to 100%)
	cleanedCount := cache.CleanupExpired()
	if cleanedCount != 1 {
		t.Errorf("Expected 1 entry to be cleaned, got %d", cleanedCount)
	}

	// The expired entry should be removed
	finalSize := cache.Size()
	if finalSize != 1 {
		t.Errorf("Expected final cache size 1 after cleanup, got %d", finalSize)
	}

	// Test that cleanup works when all entries are expired
	cache.SetWithTTL(query2, mockResult, 1*time.Millisecond) // Make the remaining entry expire
	time.Sleep(10 * time.Millisecond)

	cleanedCount = cache.CleanupExpired()
	if cleanedCount != 1 {
		t.Errorf("Expected 1 entry to be cleaned in second cleanup, got %d", cleanedCount)
	}

	emptySize := cache.Size()
	if emptySize != 0 {
		t.Errorf("Expected empty cache size 0 after all expired, got %d", emptySize)
	}

	// Test cleanup when no entries are expired
	cache.Set(query1, mockResult) // Add a fresh entry
	cleanedCount = cache.CleanupExpired()
	if cleanedCount != 0 {
		t.Errorf("Expected 0 entries to be cleaned when none expired, got %d", cleanedCount)
	}

	if cache.Size() != 1 {
		t.Errorf("Expected 1 entry to remain after cleanup with no expired entries, got %d", cache.Size())
	}
}
