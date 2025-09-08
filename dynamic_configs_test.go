package matcher

import (
	"os"
	"testing"
)

func TestDynamicDimensionConfigsWithMatchTypes(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configurations with match type-specific weights
	regionConfig := NewDimensionConfig("region", 0, true)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	regionConfig.SetWeight(MatchTypePrefix, 7.0)

	envConfig := NewDimensionConfig("env", 1, true)
	envConfig.SetWeight(MatchTypeEqual, 8.0)
	envConfig.SetWeight(MatchTypeAny, 2.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add test rules with different match types
	rule1 := NewRule("exact-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	rule2 := NewRule("prefix-rule").
		Dimension("region", "us-", MatchTypePrefix).
		Dimension("env", "any", MatchTypeAny).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule2: %v", err)
	}

	// Test 1: Query with default dimension configs
	query1 := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
	}

	matches1, err := engine.FindAllMatches(query1)
	if err != nil {
		t.Fatalf("FindAllMatches with default configs failed: %v", err)
	}

	if len(matches1) != 2 {
		t.Fatalf("Expected 2 matches with default configs, got %d", len(matches1))
	}

	// Expected weights with default configs:
	// exact-rule: region (10.0 Equal) + env (8.0 Equal) = 18.0
	// prefix-rule: region (7.0 Prefix) + env (2.0 Any) = 9.0
	exactMatch1 := findMatchByID(matches1, "exact-rule")
	prefixMatch1 := findMatchByID(matches1, "prefix-rule")

	if exactMatch1.TotalWeight != 18.0 {
		t.Errorf("Default configs: expected exact-rule weight 18.0, got %.1f", exactMatch1.TotalWeight)
	}
	if prefixMatch1.TotalWeight != 9.0 {
		t.Errorf("Default configs: expected prefix-rule weight 9.0, got %.1f", prefixMatch1.TotalWeight)
	}

	// Test 2: Query with dynamic dimension configs (different weights per match type)
	dynamicRegionConfig := NewDimensionConfig("region", 0, true)
	dynamicRegionConfig.SetWeight(MatchTypeEqual, 50.0)  // Much higher for exact matches
	dynamicRegionConfig.SetWeight(MatchTypePrefix, 30.0) // High for prefix matches

	dynamicEnvConfig := NewDimensionConfig("env", 1, true)
	dynamicEnvConfig.SetWeight(MatchTypeEqual, 25.0)
	dynamicEnvConfig.SetWeight(MatchTypeAny, 5.0)

	query2 := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"region": dynamicRegionConfig,
			"env":    dynamicEnvConfig,
		},
	}

	matches2, err := engine.FindAllMatches(query2)
	if err != nil {
		t.Fatalf("FindAllMatches with dynamic configs failed: %v", err)
	}

	if len(matches2) != 2 {
		t.Fatalf("Expected 2 matches with dynamic configs, got %d", len(matches2))
	}

	// Expected weights with dynamic configs:
	// exact-rule: region (50.0 Equal) + env (25.0 Equal) = 75.0
	// prefix-rule: region (30.0 Prefix) + env (5.0 Any) = 35.0
	exactMatch2 := findMatchByID(matches2, "exact-rule")
	prefixMatch2 := findMatchByID(matches2, "prefix-rule")

	if exactMatch2.TotalWeight != 75.0 {
		t.Errorf("Dynamic configs: expected exact-rule weight 75.0, got %.1f", exactMatch2.TotalWeight)
	}
	if prefixMatch2.TotalWeight != 35.0 {
		t.Errorf("Dynamic configs: expected prefix-rule weight 35.0, got %.1f", prefixMatch2.TotalWeight)
	}

	// Test 3: Query with partial dynamic configs (only override one dimension)
	partialDynamicConfig := NewDimensionConfig("region", 0, true)
	partialDynamicConfig.SetWeight(MatchTypeEqual, 100.0) // Very high weight for exact matches
	partialDynamicConfig.SetWeight(MatchTypePrefix, 60.0)

	query3 := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"region": partialDynamicConfig,
			// env will use the default config
		},
	}

	matches3, err := engine.FindAllMatches(query3)
	if err != nil {
		t.Fatalf("FindAllMatches with partial dynamic configs failed: %v", err)
	}

	if len(matches3) != 2 {
		t.Fatalf("Expected 2 matches with partial dynamic configs, got %d", len(matches3))
	}

	// Expected weights with partial dynamic configs:
	// exact-rule: region (100.0 dynamic Equal) + env (8.0 default Equal) = 108.0
	// prefix-rule: region (60.0 dynamic Prefix) + env (2.0 default Any) = 62.0
	exactMatch3 := findMatchByID(matches3, "exact-rule")
	prefixMatch3 := findMatchByID(matches3, "prefix-rule")

	if exactMatch3.TotalWeight != 108.0 {
		t.Errorf("Partial dynamic configs: expected exact-rule weight 108.0, got %.1f", exactMatch3.TotalWeight)
	}
	if prefixMatch3.TotalWeight != 62.0 {
		t.Errorf("Partial dynamic configs: expected prefix-rule weight 62.0, got %.1f", prefixMatch3.TotalWeight)
	}

	t.Logf("Dynamic dimension configs with match types working correctly:")
	t.Logf("  Default configs - exact: %.1f, prefix: %.1f", exactMatch1.TotalWeight, prefixMatch1.TotalWeight)
	t.Logf("  Dynamic configs - exact: %.1f, prefix: %.1f", exactMatch2.TotalWeight, prefixMatch2.TotalWeight)
	t.Logf("  Partial dynamic - exact: %.1f, prefix: %.1f", exactMatch3.TotalWeight, prefixMatch3.TotalWeight)
}

func TestDynamicConfigsWithComplexMatchTypes(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add initial dimension configurations
	priorityConfig := NewDimensionConfig("priority", 0, true)
	priorityConfig.SetWeight(MatchTypeEqual, 15.0)
	priorityConfig.SetWeight(MatchTypePrefix, 10.0)
	priorityConfig.SetWeight(MatchTypeSuffix, 8.0)
	priorityConfig.SetWeight(MatchTypeAny, 3.0)

	categoryConfig := NewDimensionConfig("category", 1, true)
	categoryConfig.SetWeight(MatchTypeEqual, 12.0)
	categoryConfig.SetWeight(MatchTypeAny, 2.0)

	err = engine.AddDimension(priorityConfig)
	if err != nil {
		t.Fatalf("Failed to add priority dimension: %v", err)
	}

	err = engine.AddDimension(categoryConfig)
	if err != nil {
		t.Fatalf("Failed to add category dimension: %v", err)
	}

	// Add rules with different match types
	rules := []*Rule{
		NewRule("high-exact").
			Dimension("priority", "high", MatchTypeEqual).
			Dimension("category", "urgent", MatchTypeEqual).
			Build(),
		NewRule("high-prefix").
			Dimension("priority", "hi", MatchTypePrefix).
			Dimension("category", "any", MatchTypeAny).
			Build(),
		NewRule("suffix-match").
			Dimension("priority", "gh", MatchTypeSuffix).
			Dimension("category", "urgent", MatchTypeEqual).
			Build(),
		NewRule("any-match").
			Dimension("priority", "any", MatchTypeAny).
			Dimension("category", "any", MatchTypeAny).
			Build(),
	}

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Create query that matches all rules
	baseQuery := map[string]string{
		"priority": "high",
		"category": "urgent",
	}

	// Test with dynamic configs that heavily favor prefix matches
	dynamicPriorityConfig := NewDimensionConfig("priority", 0, true)
	dynamicPriorityConfig.SetWeight(MatchTypeEqual, 20.0)
	dynamicPriorityConfig.SetWeight(MatchTypePrefix, 100.0) // Heavily favor prefix matches
	dynamicPriorityConfig.SetWeight(MatchTypeSuffix, 15.0)
	dynamicPriorityConfig.SetWeight(MatchTypeAny, 5.0)

	dynamicCategoryConfig := NewDimensionConfig("category", 1, true)
	dynamicCategoryConfig.SetWeight(MatchTypeEqual, 30.0)
	dynamicCategoryConfig.SetWeight(MatchTypeAny, 10.0)

	query := &QueryRule{
		Values: baseQuery,
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"priority": dynamicPriorityConfig,
			"category": dynamicCategoryConfig,
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 4 {
		t.Fatalf("Expected 4 matches, got %d", len(matches))
	}

	// Expected weights with dynamic configs:
	expectedWeights := map[string]float64{
		"high-exact":   50.0,  // priority (20.0 Equal) + category (30.0 Equal)
		"high-prefix":  110.0, // priority (100.0 Prefix) + category (10.0 Any)
		"suffix-match": 45.0,  // priority (15.0 Suffix) + category (30.0 Equal)
		"any-match":    15.0,  // priority (5.0 Any) + category (10.0 Any)
	}

	// Verify weights and ordering (highest weight first)
	if matches[0].Rule.ID != "high-prefix" {
		t.Errorf("Expected highest weight rule 'high-prefix' first, got '%s'", matches[0].Rule.ID)
	}

	for _, match := range matches {
		expectedWeight := expectedWeights[match.Rule.ID]
		if match.TotalWeight != expectedWeight {
			t.Errorf("Rule %s: expected weight %.1f, got %.1f", match.Rule.ID, expectedWeight, match.TotalWeight)
		}
	}

	t.Logf("Dynamic configs prioritizing prefix matches:")
	for _, match := range matches {
		t.Logf("  Rule %s: weight %.1f", match.Rule.ID, match.TotalWeight)
	}
}

// Helper function to find a match by rule ID
func findMatchByID(matches []*MatchResult, ruleID string) *MatchResult {
	for _, match := range matches {
		if match.Rule.ID == ruleID {
			return match
		}
	}
	return nil
}

func TestDynamicConfigsWithMultipleMatchTypes(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "matcher_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a matcher
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create dimension config with different weights for different match types
	priorityConfig := NewDimensionConfig("priority", 0, true) // default weight 1.0
	priorityConfig.SetWeight(MatchTypeEqual, 10.0)            // exact matches get 10.0
	priorityConfig.SetWeight(MatchTypePrefix, 5.0)            // prefix matches get 5.0
	priorityConfig.SetWeight(MatchTypeAny, 2.0)               // any matches get 2.0

	categoryConfig := NewDimensionConfig("category", 1, true) // default weight 1.0
	categoryConfig.SetWeight(MatchTypeEqual, 8.0)             // exact matches get 8.0
	categoryConfig.SetWeight(MatchTypeSuffix, 3.0)            // suffix matches get 3.0

	err = engine.AddDimension(priorityConfig)
	if err != nil {
		t.Fatalf("Failed to add priority dimension: %v", err)
	}

	err = engine.AddDimension(categoryConfig)
	if err != nil {
		t.Fatalf("Failed to add category dimension: %v", err)
	}

	// Add rules with different match types
	rule1 := NewRule("exact_match")
	rule1.Dimension("priority", "high", MatchTypeEqual)   // should get 10.0 weight
	rule1.Dimension("category", "urgent", MatchTypeEqual) // should get 8.0 weight
	err = engine.AddRule(rule1.Build())
	if err != nil {
		t.Fatalf("Failed to add exact_match rule: %v", err)
	}

	rule2 := NewRule("prefix_match")
	rule2.Dimension("priority", "high", MatchTypePrefix)   // should get 5.0 weight
	rule2.Dimension("category", "urgent", MatchTypeSuffix) // should get 3.0 weight
	err = engine.AddRule(rule2.Build())
	if err != nil {
		t.Fatalf("Failed to add prefix_match rule: %v", err)
	}

	// Test query with exact match values
	query := &QueryRule{
		Values: map[string]string{
			"priority": "high",
			"category": "urgent",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 2 {
		t.Fatalf("Expected 2 matches, got %d", len(matches))
	}

	// The exact match rule should have higher weight (10.0 + 8.0 = 18.0)
	// The prefix/suffix rule should have lower weight (5.0 + 3.0 = 8.0)
	// Results should be sorted by weight (highest first)

	if matches[0].TotalWeight != 18.0 {
		t.Errorf("Expected first match weight 18.0, got %f", matches[0].TotalWeight)
	}

	if matches[1].TotalWeight != 8.0 {
		t.Errorf("Expected second match weight 8.0, got %f", matches[1].TotalWeight)
	}

	if matches[0].Rule.ID != "exact_match" {
		t.Errorf("Expected first match to be 'exact_match', got '%s'", matches[0].Rule.ID)
	}

	if matches[1].Rule.ID != "prefix_match" {
		t.Errorf("Expected second match to be 'prefix_match', got '%s'", matches[1].Rule.ID)
	}

	t.Logf("Exact match rule weight: %f", matches[0].TotalWeight)
	t.Logf("Prefix/suffix match rule weight: %f", matches[1].TotalWeight)

	// Test with dynamic configs that reverse the priority
	dynamicQuery := &QueryRule{
		Values: map[string]string{
			"priority": "high",
			"category": "urgent",
		},
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"priority": NewDimensionConfigWithWeights("priority", 0, true, map[MatchType]float64{
				MatchTypeEqual:  2.0,  // lower weight for exact
				MatchTypePrefix: 20.0, // much higher weight for prefix
			}),
			"category": NewDimensionConfigWithWeights("category", 1, true, map[MatchType]float64{
				MatchTypeEqual:  3.0,  // lower weight for exact
				MatchTypeSuffix: 15.0, // higher weight for suffix
			}),
		},
	}

	dynamicMatches, err := engine.FindAllMatches(dynamicQuery)
	if err != nil {
		t.Fatalf("FindAllMatches with dynamic configs failed: %v", err)
	}

	if len(dynamicMatches) != 2 {
		t.Fatalf("Expected 2 matches with dynamic configs, got %d", len(dynamicMatches))
	}

	// Now the prefix/suffix rule should have higher weight (20.0 + 15.0 = 35.0)
	// The exact match rule should have lower weight (2.0 + 3.0 = 5.0)

	if dynamicMatches[0].TotalWeight != 35.0 {
		t.Errorf("Expected first dynamic match weight 35.0, got %f", dynamicMatches[0].TotalWeight)
	}

	if dynamicMatches[1].TotalWeight != 5.0 {
		t.Errorf("Expected second dynamic match weight 5.0, got %f", dynamicMatches[1].TotalWeight)
	}

	if dynamicMatches[0].Rule.ID != "prefix_match" {
		t.Errorf("Expected first dynamic match to be 'prefix_match', got '%s'", dynamicMatches[0].Rule.ID)
	}

	if dynamicMatches[1].Rule.ID != "exact_match" {
		t.Errorf("Expected second dynamic match to be 'exact_match', got '%s'", dynamicMatches[1].Rule.ID)
	}

	t.Logf("Dynamic: Prefix/suffix match rule weight: %f", dynamicMatches[0].TotalWeight)
	t.Logf("Dynamic: Exact match rule weight: %f", dynamicMatches[1].TotalWeight)
}
