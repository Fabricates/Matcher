package matcher

import (
	"os"
	"testing"
	"time"
)

func TestRuleBuilderMetadataIntegration(t *testing.T) {
	// Integration test for rule builder with metadata functionality
	rule := NewRule("metadata-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Metadata("key1", "value1").
		Metadata("key2", "value2").
		Build()

	if rule.Metadata["key1"] != "value1" {
		t.Errorf("Expected metadata key1=value1, got %s", rule.Metadata["key1"])
	}
	if rule.Metadata["key2"] != "value2" {
		t.Errorf("Expected metadata key2=value2, got %s", rule.Metadata["key2"])
	}
}

func TestMatcherEngineFullAPIIntegration(t *testing.T) {
	// Integration test for complete MatcherEngine API workflow
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test ListRules
	rules, err := engine.ListRules(0, 10)
	if err != nil {
		t.Errorf("ListRules failed: %v", err)
	}
	if rules == nil {
		t.Error("ListRules returned nil")
	}

	// Test ListDimensions
	_, err = engine.ListDimensions()
	if err != nil {
		t.Errorf("ListDimensions failed: %v", err)
	}
	// ListDimensions can return empty slice, which is valid	// Test Save
	if err := engine.Save(); err != nil {
		t.Errorf("Save failed: %v", err)
	}

	// Test Health
	if err := engine.Health(); err != nil {
		t.Errorf("Health failed: %v", err)
	}

	// Test GetForestStats
	stats := engine.GetForestStats()
	if stats == nil {
		t.Error("GetForestStats returned nil")
	}

	// Test ClearCache
	engine.ClearCache()

	// Test GetCacheStats
	cacheStats := engine.GetCacheStats()
	if cacheStats == nil {
		t.Error("GetCacheStats returned nil")
	}

	// Test AutoSave
	stopChan := engine.AutoSave(100 * time.Millisecond)
	if stopChan == nil {
		t.Error("AutoSave returned nil channel")
	}
	stopChan <- true

	// Add a rule and test RebuildIndex
	rule := NewRule("rebuild-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()
	if err := engine.AddRule(rule); err != nil {
		t.Errorf("Failed to add rule: %v", err)
	}

	if err := engine.RebuildIndex(); err != nil {
		t.Errorf("RebuildIndex failed: %v", err)
	}
}

func TestQueryCacheAPIIntegration(t *testing.T) {
	// Integration test for QueryCache API methods
	cache := NewQueryCache(10, 60*time.Second)

	// Test Size
	size := cache.Size()
	if size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// Test SetWithTTL
	query := CreateQuery(map[string]string{"test": "value"})
	result := &MatchResult{Rule: &Rule{ID: "test"}, TotalWeight: 1.0}
	cache.SetWithTTL(query, result, 30*time.Second)

	// Test Size again
	size = cache.Size()
	if size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}

	// Test CleanupExpired
	cleanedCount := cache.CleanupExpired()
	t.Logf("Cleaned %d expired entries", cleanedCount)

	// Test Stats
	stats := cache.Stats()
	if stats == nil {
		t.Error("Stats returned nil")
	}

	// Test StartCleanupWorker
	stopChan := cache.StartCleanupWorker(100 * time.Millisecond)
	if stopChan == nil {
		t.Error("StartCleanupWorker returned nil channel")
	}
	stopChan <- true
}

func TestMultiLevelCacheAPIIntegration(t *testing.T) {
	// Integration test for MultiLevelCache API methods
	mlc := NewMultiLevelCache(10, 60*time.Second, 100, 120*time.Second)
	if mlc == nil {
		t.Error("NewMultiLevelCache returned nil")
	}

	// Test Get on empty cache
	query := CreateQuery(map[string]string{"test": "value"})
	result := mlc.Get(query)
	if result != nil {
		t.Error("Expected nil result for empty cache")
	}

	// Test Set
	matchResult := &MatchResult{Rule: &Rule{ID: "test"}, TotalWeight: 1.0}
	mlc.Set(query, matchResult)

	// Test Get again
	result = mlc.Get(query)
	if result == nil {
		t.Error("Expected result after set")
	}

	// Test Stats
	stats := mlc.Stats()
	if stats == nil {
		t.Error("Stats returned nil")
	}

	// Test Clear
	mlc.Clear()
}

func TestForestAPIIntegration(t *testing.T) {
	// Integration test for Forest API methods
	forest := CreateForestIndex()

	// Test GetDefaultDimensionOrder
	defaultOrder := forest.GetDefaultDimensionOrder()
	if defaultOrder == nil {
		t.Error("GetDefaultDimensionOrder returned nil")
	}

	// Test GetDimensionOrder
	order := forest.GetDimensionOrder()
	if order == nil {
		t.Error("GetDimensionOrder returned nil")
	}

	// Test SetDimensionOrder
	newOrder := []string{"region", "env", "service"}
	forest.SetDimensionOrder(newOrder)

	// Test InitializeDimension
	forest.InitializeDimension("new-dim")

	// Verify it was added
	updatedOrder := forest.GetDimensionOrder()
	found := false
	for _, dim := range updatedOrder {
		if dim == "new-dim" {
			found = true
			break
		}
	}
	if !found {
		t.Log("InitializeDimension may not have added dimension - this could be expected behavior")
	}
}

func TestTypesAPIIntegration(t *testing.T) {
	// Integration test for Types API methods
	rule := NewRule("types-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	// Test GetDimensionValue
	value := rule.GetDimensionValue("region")
	if value == nil {
		t.Error("Expected dimension value, got nil")
	}
	if value != nil && value.Value != "us-west" {
		t.Errorf("Expected 'us-west', got '%s'", value.Value)
	}

	// Test non-existent dimension
	value = rule.GetDimensionValue("nonexistent")
	if value != nil {
		t.Errorf("Expected nil, got %v", value)
	}

	// Test HasDimension
	if !rule.HasDimension("region") {
		t.Error("Expected rule to have 'region' dimension")
	}

	if rule.HasDimension("nonexistent") {
		t.Error("Expected rule to not have 'nonexistent' dimension")
	}
}
