package matcher

import (
	"testing"
)

func TestForestStructure(t *testing.T) {
	// Set up dimension configs for the test
	dimensionConfigs := NewDimensionConfigs()
	dimensionConfigs.Add(NewDimensionConfig("region", 0, false))
	dimensionConfigs.Add(NewDimensionConfig("env", 1, false))
	dimensionConfigs.Add(NewDimensionConfig("service", 2, false))

	forest := CreateRuleForest(dimensionConfigs)

	// Create test rules with different primary dimensions
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			"env":    {DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "10"},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-east", MatchType: MatchTypeEqual},
			"env":    {DimensionName: "env", Value: "staging", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "20"},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"region":  {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			"service": {DimensionName: "service", Value: "api", MatchType: MatchTypePrefix},
		},
		Metadata: map[string]string{"weight": "15"},
	}

	// Add rules to forest
	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Test that rules are organized into separate trees
	stats := forest.GetStats()
	t.Logf("Forest stats: %+v", stats)

	// Verify we have the correct number of trees
	// rule1 and rule3 both have region=us-west with MatchTypeEqual, so they share a tree
	// rule2 has region=us-east with MatchTypeEqual, so it gets its own tree
	// Total: 2 trees
	totalTrees := stats["total_trees"].(int)
	if totalTrees != 2 {
		t.Errorf("Expected 2 trees, got %d", totalTrees)
	}

	// Test query functionality
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates for query %+v", len(candidates), query.Values)

	// Should find rule1 and rule3 (both have region=us-west)
	if len(candidates) < 1 {
		t.Errorf("Expected at least 1 candidate, got %d", len(candidates))
	}

	// Test another query
	query2 := &QueryRule{
		Values: map[string]string{
			"region": "us-east",
			"env":    "staging",
		},
	}

	candidates2 := forest.FindCandidateRules(query2)
	t.Logf("Found %d candidates for query %+v", len(candidates2), query2.Values)

	// Should find rule2
	if len(candidates2) < 1 {
		t.Errorf("Expected at least 1 candidate, got %d", len(candidates2))
	}
}

func TestForestNodeSearch(t *testing.T) {
	// Set up dimension configs for the test
	dimensionConfigs := NewDimensionConfigs()
	dimensionConfigs.Add(NewDimensionConfig("service", 0, false))
	dimensionConfigs.Add(NewDimensionConfig("version", 1, false))

	forest := CreateRuleForest(dimensionConfigs)

	// Create a rule with prefix matching
	rule := &Rule{
		ID: "prefix-rule",
		Dimensions: map[string]*DimensionValue{
			"service": {DimensionName: "service", Value: "api", MatchType: MatchTypePrefix},
			"version": {DimensionName: "version", Value: "v1", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "10"},
	}

	forest.AddRule(rule)

	// Test prefix matching
	query := &QueryRule{
		Values: map[string]string{
			"service": "api-gateway",
			"version": "v1",
		},
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates for prefix query", len(candidates))

	if len(candidates) == 0 {
		t.Error("Expected to find candidates for prefix matching")
	}

	// Verify the found rule is correct
	found := false
	for _, candidate := range candidates {
		if candidate.ID == "prefix-rule" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected to find the prefix rule in candidates")
	}
}

func TestForestRemoveRule(t *testing.T) {
	// Set up dimension configs for the test
	dimensionConfigs := NewDimensionConfigs()
	dimensionConfigs.Add(NewDimensionConfig("region", 0, false))

	forest := CreateRuleForest(dimensionConfigs)

	rule := &Rule{
		ID: "test-rule",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "10"},
	}

	// Add rule
	forest.AddRule(rule)

	// Verify it's found
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
	}

	candidates := forest.FindCandidateRules(query)
	if len(candidates) == 0 {
		t.Error("Expected to find the rule after adding")
	}

	// Remove rule
	forest.RemoveRule(rule)

	// Verify it's no longer found
	candidates = forest.FindCandidateRules(query)
	if len(candidates) > 0 {
		t.Error("Expected not to find the rule after removing")
	}
}

// ========================================
// MERGED DEBUG TESTS (merged from debug_optimization_test.go, very_detailed_debug_test.go, deep_debug_test.go, two_dim_debug_test.go)
// ========================================

func TestSimpleEqualMatch(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a simple rule with exact match
	rule := NewRule("simple-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Query for exact match
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}

	if len(matches) > 0 && matches[0].Rule.ID != "simple-rule" {
		t.Errorf("Expected rule 'simple-rule', got '%s'", matches[0].Rule.ID)
	}
}

func TestVeryDetailedDebug(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a simple rule with exact match
	rule := NewRule("simple-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Examine the forest structure before optimization
	forestIndex := engine.matcher.getOrCreateForestIndex("", "")

	t.Logf("=== FOREST STRUCTURE ===")
	for matchType, trees := range forestIndex.Trees {
		t.Logf("Trees for match type %s: %d trees", matchType, len(trees))
		for i, tree := range trees {
			t.Logf("  Tree %d: Level=%d, DimName=%s, Value=%s", i, tree.Level, tree.DimensionName, tree.Value)
			t.Logf("    Rules: %d", len(tree.Rules))
			t.Logf("    Branches: %d", len(tree.Branches))
			for branchType, branch := range tree.Branches {
				t.Logf("      Branch %s: %d children", branchType, len(branch.Children))
				for childKey, child := range branch.Children {
					t.Logf("        Child key='%s': Level=%d, DimName=%s, Value=%s, Rules=%d",
						childKey, child.Level, child.DimensionName, child.Value, len(child.Rules))
				}
			}
		}
	}

	// Query for exact match
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	t.Logf("Found %d matches", len(matches))
	for _, match := range matches {
		t.Logf("  Match: %s (weight: %.1f)", match.Rule.ID, match.TotalWeight)
	}
}

func TestDeepDebugOptimization(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a simple rule with exact match
	rule := NewRule("simple-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Check the forest structure
	forestIndex := engine.matcher.getOrCreateForestIndex("", "")
	stats := forestIndex.GetStats()
	t.Logf("Forest stats: %+v", stats)

	// Query for exact match
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	t.Logf("Found %d matches", len(matches))
	for _, match := range matches {
		t.Logf("  Match: %s (weight: %.1f)", match.Rule.ID, match.TotalWeight)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}
}

func TestTwoDimensionForestStructure(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add a two-dimensional rule with exact matches
	rule := NewRule("two-dim-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Examine the forest structure
	forestIndex := engine.matcher.getOrCreateForestIndex("", "")

	t.Logf("=== TWO-DIMENSION FOREST STRUCTURE ===")
	for matchType, trees := range forestIndex.Trees {
		t.Logf("Trees for match type %s: %d trees", matchType, len(trees))
		for i, tree := range trees {
			t.Logf("  Tree %d: Level=%d, DimName=%s, Value=%s, Rules=%d", i, tree.Level, tree.DimensionName, tree.Value, len(tree.Rules))
			t.Logf("    Branches: %d", len(tree.Branches))
			for branchType, branch := range tree.Branches {
				t.Logf("      Branch %s: %d children", branchType, len(branch.Children))
				for childKey, child := range branch.Children {
					t.Logf("        Child key='%s': Level=%d, DimName=%s, Value=%s, Rules=%d",
						childKey, child.Level, child.DimensionName, child.Value, len(child.Rules))
					// Check deeper levels
					for subBranchType, subBranch := range child.Branches {
						t.Logf("          SubBranch %s: %d children", subBranchType, len(subBranch.Children))
						for subChildKey, subChild := range subBranch.Children {
							t.Logf("            SubChild key='%s': Level=%d, DimName=%s, Value=%s, Rules=%d",
								subChildKey, subChild.Level, subChild.DimensionName, subChild.Value, len(subChild.Rules))
						}
					}
				}
			}
		}
	}

	// Query for exact match
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

	t.Logf("Found %d matches", len(matches))
	for _, match := range matches {
		t.Logf("  Match: %s (weight: %.1f)", match.Rule.ID, match.TotalWeight)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}
}
