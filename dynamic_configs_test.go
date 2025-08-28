package matcher

import (
	"os"
	"testing"
)

func TestDynamicDimensionConfigs(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "matcher_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a matcher with initial dimension configs
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test since we're testing dynamic configs, not weight conflicts
	engine.SetAllowDuplicateWeights(true)

	// Add dimensions with default weights
	err = engine.AddDimension(NewDimensionConfig("region", 0, true, 10.0))
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("env", 1, true, 5.0))
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add test rules using AddSimpleRule
	dimensions1 := map[string]string{
		"region": "us-west",
		"env":    "prod",
	}
	err = engine.AddSimpleRule("rule1", dimensions1, nil)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	dimensions2 := map[string]string{
		"region": "us-east",
		"env":    "prod",
	}
	err = engine.AddSimpleRule("rule2", dimensions2, nil)
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

	if len(matches1) != 1 {
		t.Fatalf("Expected 1 match with default configs, got %d", len(matches1))
	}

	expectedWeight1 := 15.0 // region (10.0) + env (5.0)
	if matches1[0].TotalWeight != expectedWeight1 {
		t.Errorf("Expected weight %f with default configs, got %f", expectedWeight1, matches1[0].TotalWeight)
	}

	// Test 2: Query with dynamic dimension configs (higher weights)
	query2 := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"region": NewDimensionConfig("region", 0, true, 20.0),
			"env":    NewDimensionConfig("env", 1, true, 15.0),
		},
	}

	matches2, err := engine.FindAllMatches(query2)
	if err != nil {
		t.Fatalf("FindAllMatches with dynamic configs failed: %v", err)
	}

	if len(matches2) != 1 {
		t.Fatalf("Expected 1 match with dynamic configs, got %d", len(matches2))
	}

	expectedWeight2 := 35.0 // region (20.0) + env (15.0)
	if matches2[0].TotalWeight != expectedWeight2 {
		t.Errorf("Expected weight %f with dynamic configs, got %f", expectedWeight2, matches2[0].TotalWeight)
	}

	// Test 3: Query with partial dynamic configs (only override one dimension)
	query3 := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
		DynamicDimensionConfigs: map[string]*DimensionConfig{
			"region": NewDimensionConfig("region", 0, true, 25.0),
			// env will use the default weight of 5.0
		},
	}

	matches3, err := engine.FindAllMatches(query3)
	if err != nil {
		t.Fatalf("FindAllMatches with partial dynamic configs failed: %v", err)
	}

	if len(matches3) != 1 {
		t.Fatalf("Expected 1 match with partial dynamic configs, got %d", len(matches3))
	}

	expectedWeight3 := 30.0 // region (25.0) + env (5.0 default)
	if matches3[0].TotalWeight != expectedWeight3 {
		t.Errorf("Expected weight %f with partial dynamic configs, got %f", expectedWeight3, matches3[0].TotalWeight)
	}

	t.Logf("Test 1 (default configs): Weight = %f", matches1[0].TotalWeight)
	t.Logf("Test 2 (full dynamic configs): Weight = %f", matches2[0].TotalWeight)
	t.Logf("Test 3 (partial dynamic configs): Weight = %f", matches3[0].TotalWeight)
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
	priorityConfig := NewDimensionConfig("priority", 0, true, 1.0) // default weight 1.0
	priorityConfig.SetWeight(MatchTypeEqual, 10.0)                 // exact matches get 10.0
	priorityConfig.SetWeight(MatchTypePrefix, 5.0)                 // prefix matches get 5.0
	priorityConfig.SetWeight(MatchTypeAny, 2.0)                    // any matches get 2.0

	categoryConfig := NewDimensionConfig("category", 1, true, 1.0) // default weight 1.0
	categoryConfig.SetWeight(MatchTypeEqual, 8.0)                  // exact matches get 8.0
	categoryConfig.SetWeight(MatchTypeSuffix, 3.0)                 // suffix matches get 3.0

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
			}, 1.0),
			"category": NewDimensionConfigWithWeights("category", 1, true, map[MatchType]float64{
				MatchTypeEqual:  3.0,  // lower weight for exact
				MatchTypeSuffix: 15.0, // higher weight for suffix
			}, 1.0),
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
