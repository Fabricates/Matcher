package matcher

import (
	"testing"
)

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
