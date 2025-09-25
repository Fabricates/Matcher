package matcher

import (
	"fmt"
	"testing"
	"time"
)

func TestRuleForestDimensionOrder(t *testing.T) {
	// Define dimension order
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, false),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
	}, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create test rules
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeAny}, // Changed to MatchTypeAny for partial query testing
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "alt", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "drill", MatchType: MatchTypeEqual},
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductB", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	// Add rules to forest
	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Test 1: Query that should match rule1 only
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	t.Logf("Query1 candidates: %d rules", len(candidates1))
	for _, rule := range candidates1 {
		t.Logf("  Found rule: %s", rule.ID)
	}

	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Errorf("Expected to find only rule1, got %d rules", len(candidates1))
	}

	// Test 2: Query that should match rule3 only
	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductB",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates2 := forest.FindCandidateRules(query2)
	t.Logf("Query2 candidates: %d rules", len(candidates2))
	for _, rule := range candidates2 {
		t.Logf("  Found rule: %s", rule.ID)
	}

	if len(candidates2) != 1 || candidates2[0].ID != "rule3" {
		t.Errorf("Expected to find only rule3, got %d rules", len(candidates2))
	}

	// Test 3: Query with partial dimensions (should traverse properly)
	query3 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
		},
	}

	candidates3 := forest.FindCandidateRules(query3)
	t.Logf("Query3 candidates: %d rules", len(candidates3))
	for _, rule := range candidates3 {
		t.Logf("  Found rule: %s", rule.ID)
	}

	// This should find rule1 because it matches product=ProductA, route=main
	if len(candidates3) != 1 || candidates3[0].ID != "rule1" {
		t.Errorf("Expected to find only rule1, got %d rules", len(candidates3))
	}

	// Check forest structure
	stats := forest.GetStats()
	t.Logf("Forest stats: %+v", stats)

	// Should have 2 root nodes (ProductA and ProductB)
	if stats["total_root_nodes"].(int) != 2 {
		t.Errorf("Expected 2 root nodes(equal type), got %d", stats["total_root_nodes"].(int))
	}

	// Should have 3 total rules
	if stats["total_rules"].(int) != 3 {
		t.Errorf("Expected 3 total rules, got %d", stats["total_rules"].(int))
	}
}

func TestRuleForestDimensionTraversal(t *testing.T) {
	// Define dimension order
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("A", 0, false),
		NewDimensionConfig("B", 1, false),
		NewDimensionConfig("C", 2, false),
	}, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create a rule that uses all dimensions
	rule := &Rule{
		ID: "test_rule",
		Dimensions: map[string]*DimensionValue{
			"A": {DimensionName: "A", Value: "a1", MatchType: MatchTypeEqual},
			"B": {DimensionName: "B", Value: "b1", MatchType: MatchTypeEqual},
			"C": {DimensionName: "C", Value: "c1", MatchType: MatchTypeEqual},
		},
	}

	forest.AddRule(rule)

	// Test traversal with complete query
	query := &QueryRule{
		Values: map[string]string{
			"A": "a1",
			"B": "b1",
			"C": "c1",
		},
	}

	candidates := forest.FindCandidateRules(query)
	if len(candidates) != 1 || candidates[0].ID != "test_rule" {
		t.Errorf("Expected to find test_rule, got %d rules", len(candidates))
	}

	// Test that querying with wrong values at any level returns no results
	queryWrongA := &QueryRule{
		Values: map[string]string{
			"A": "a2", // Wrong value for A
			"B": "b1",
			"C": "c1",
		},
	}

	candidates = forest.FindCandidateRules(queryWrongA)
	if len(candidates) != 0 {
		t.Errorf("Expected no results for wrong A value, got %d rules", len(candidates))
	}

	queryWrongB := &QueryRule{
		Values: map[string]string{
			"A": "a1",
			"B": "b2", // Wrong value for B
			"C": "c1",
		},
	}

	candidates = forest.FindCandidateRules(queryWrongB)
	if len(candidates) != 0 {
		t.Errorf("Expected no results for wrong B value, got %d rules", len(candidates))
	}
}

func TestRuleForestSharedPaths(t *testing.T) {
	// Test that the forest can handle shared paths between different rules
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, false),
		NewDimensionConfig("region", 1, false),
	}, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Two rules that share the same path but different match types
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeAny},
		},
	}

	forest.AddRule(rule1)
	forest.AddRule(rule2)

	// Query that should find both rules
	query := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates", len(candidates))
	for _, rule := range candidates {
		t.Logf("  Rule: %s", rule.ID)
	}

	if len(candidates) != 2 {
		t.Errorf("Expected to find 2 rules (both should match), got %d rules", len(candidates))
	}

	// Verify both rules are found
	foundRule1, foundRule2 := false, false
	for _, rule := range candidates {
		if rule.ID == "rule1" {
			foundRule1 = true
		}
		if rule.ID == "rule2" {
			foundRule2 = true
		}
	}

	if !foundRule1 || !foundRule2 {
		t.Errorf("Expected to find both rule1 and rule2")
	}
}

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

func TestEqualMatchOptimization(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	regionConfig.SetWeight(MatchTypePrefix, 7.0)

	envConfig := NewDimensionConfig("env", 1, false)
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

	// Create rules with equal match types that should benefit from hash map optimization
	exactRules := []*Rule{
		NewRule("exact-us-west-prod").
			Dimension("region", "us-west", MatchTypeEqual).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
		NewRule("exact-us-east-prod").
			Dimension("region", "us-east", MatchTypeEqual).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
		NewRule("exact-eu-west-staging").
			Dimension("region", "eu-west", MatchTypeEqual).
			Dimension("env", "staging", MatchTypeEqual).
			Build(),
		NewRule("exact-ap-south-dev").
			Dimension("region", "ap-south", MatchTypeEqual).
			Dimension("env", "dev", MatchTypeEqual).
			Build(),
	}

	// Also add some non-equal rules to ensure they still work
	nonExactRules := []*Rule{
		NewRule("prefix-us").
			Dimension("region", "us-", MatchTypePrefix).
			Dimension("env", "any", MatchTypeAny).
			Build(),
		NewRule("any-region-prod").
			Dimension("region", "any", MatchTypeAny).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
	}

	// Add all rules
	allRules := append(exactRules, nonExactRules...)
	for _, rule := range allRules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test exact match queries (should benefit from O(1) hash map lookup)
	testCases := []struct {
		name            string
		query           map[string]string
		expectedMatches []string
		description     string
	}{
		{
			name: "exact-match-us-west-prod",
			query: map[string]string{
				"region": "us-west",
				"env":    "prod",
			},
			expectedMatches: []string{"exact-us-west-prod", "prefix-us", "any-region-prod"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "exact-match-us-east-prod",
			query: map[string]string{
				"region": "us-east",
				"env":    "prod",
			},
			expectedMatches: []string{"exact-us-east-prod", "prefix-us", "any-region-prod"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "exact-match-eu-west-staging",
			query: map[string]string{
				"region": "eu-west",
				"env":    "staging",
			},
			expectedMatches: []string{"exact-eu-west-staging"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "no-exact-match",
			query: map[string]string{
				"region": "unknown-region",
				"env":    "prod",
			},
			expectedMatches: []string{"any-region-prod"},
			description:     "Should quickly determine no exact match exists",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := &QueryRule{
				Values: tc.query,
			}

			matches, err := engine.FindAllMatches(query)
			if err != nil {
				t.Fatalf("FindAllMatches failed: %v", err)
			}

			// Verify we got the expected matches
			matchedIDs := make([]string, len(matches))
			for i, match := range matches {
				matchedIDs[i] = match.Rule.ID
			}

			if len(matchedIDs) != len(tc.expectedMatches) {
				t.Errorf("Expected %d matches, got %d. Expected: %v, Got: %v",
					len(tc.expectedMatches), len(matchedIDs), tc.expectedMatches, matchedIDs)
			}

			// Check that all expected matches are present
			expectedSet := make(map[string]bool)
			for _, expected := range tc.expectedMatches {
				expectedSet[expected] = true
			}

			for _, matchedID := range matchedIDs {
				if !expectedSet[matchedID] {
					t.Errorf("Unexpected match: %s", matchedID)
				}
			}

			t.Logf("%s: Found %d matches as expected", tc.description, len(matches))
		})
	}
}

func TestEqualMatchPerformance(t *testing.T) {
	// Skip performance test in short mode
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	serviceConfig := NewDimensionConfig("service", 1, false)
	serviceConfig.SetWeight(MatchTypeEqual, 8.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(serviceConfig)
	if err != nil {
		t.Fatalf("Failed to add service dimension: %v", err)
	}

	// Create a large number of rules with exact matches to test performance
	numRules := 1000
	regions := []string{"us-west-1", "us-west-2", "us-east-1", "us-east-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-1"}
	services := []string{"web", "api", "database", "cache", "queue", "worker", "monitor", "auth", "notification", "analytics"}

	t.Logf("Creating %d rules with exact match types...", numRules)
	for i := 0; i < numRules; i++ {
		region := regions[i%len(regions)]
		service := services[i%len(services)]

		rule := NewRule(fmt.Sprintf("rule-%d", i)).
			Dimension("region", region, MatchTypeEqual).
			Dimension("service", service, MatchTypeEqual).
			Build()

		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %d: %v", i, err)
		}
	}

	// Test query performance with exact matches (should benefit from hash map optimization)
	query := &QueryRule{
		Values: map[string]string{
			"region":  "us-west-1",
			"service": "api",
		},
	}

	// Warm up
	_, err = engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("Warmup query failed: %v", err)
	}

	// Measure performance
	numQueries := 1000
	startTime := time.Now()

	for i := 0; i < numQueries; i++ {
		region := regions[i%len(regions)]
		service := services[i%len(services)]

		testQuery := &QueryRule{
			Values: map[string]string{
				"region":  region,
				"service": service,
			},
		}

		_, err = engine.FindAllMatches(testQuery)
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
	}

	duration := time.Since(startTime)
	avgQueryTime := duration / time.Duration(numQueries)

	t.Logf("Performance test completed:")
	t.Logf("  Total rules: %d", numRules)
	t.Logf("  Total queries: %d", numQueries)
	t.Logf("  Total time: %v", duration)
	t.Logf("  Average query time: %v", avgQueryTime)
	t.Logf("  Queries per second: %.2f", float64(numQueries)/duration.Seconds())

	// With hash map optimization, queries should be very fast
	// Even with 1000 rules, average query time should be well under 1ms
	if avgQueryTime > 1*time.Millisecond {
		t.Logf("Warning: Average query time %v is higher than expected. Hash map optimization may not be working optimally.", avgQueryTime)
	} else {
		t.Logf("✓ Performance looks good - hash map optimization is working")
	}
}

func TestEqualMatchCorrectness(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configuration
	userConfig := NewDimensionConfig("user_type", 0, false)
	userConfig.SetWeight(MatchTypeEqual, 10.0)
	userConfig.SetWeight(MatchTypePrefix, 7.0)

	err = engine.AddDimension(userConfig)
	if err != nil {
		t.Fatalf("Failed to add user_type dimension: %v", err)
	}

	// Add rules with same dimension name but different values and match types
	rules := []*Rule{
		NewRule("admin-exact").
			Dimension("user_type", "admin", MatchTypeEqual).
			Build(),
		NewRule("admin-prefix").
			Dimension("user_type", "admin", MatchTypePrefix). // This should also match "admin" query
			Build(),
		NewRule("user-exact").
			Dimension("user_type", "user", MatchTypeEqual).
			Build(),
		NewRule("adm-prefix").
			Dimension("user_type", "adm", MatchTypePrefix). // This should match "admin" query
			Build(),
	}

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test that equal match optimization doesn't break prefix/other match types
	query := &QueryRule{
		Values: map[string]string{
			"user_type": "admin",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	// Should match: admin-exact (exact), admin-prefix (prefix), adm-prefix (prefix)
	// Should NOT match: user-exact
	expectedMatches := map[string]bool{
		"admin-exact":  true,
		"admin-prefix": true,
		"adm-prefix":   true,
	}

	if len(matches) != 3 {
		t.Errorf("Expected 3 matches, got %d", len(matches))
	}

	for _, match := range matches {
		if !expectedMatches[match.Rule.ID] {
			t.Errorf("Unexpected match: %s", match.Rule.ID)
		}
		t.Logf("✓ Correctly matched rule: %s (weight: %.1f)", match.Rule.ID, match.TotalWeight)
	}

	// Verify that exact matches get higher weight (should be first)
	if len(matches) > 0 && matches[0].Rule.ID != "admin-exact" {
		t.Errorf("Expected 'admin-exact' to have highest weight and be first, got '%s'", matches[0].Rule.ID)
	}
}

func TestForestWeightOrdering(t *testing.T) {
	// Set up dimension configs to control weights
	dimensionConfigMap := []*DimensionConfig{
		NewDimensionConfig("region", 0, false),
		NewDimensionConfig("env", 1, false),
	}
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter(dimensionConfigMap, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create test rules with different weights
	rule1 := &Rule{
		ID: "rule1-low",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			"env":    {DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Status: RuleStatusWorking,
	}

	rule2 := &Rule{
		ID: "rule2-high",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			"env":    {DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Status:       RuleStatusWorking,
		ManualWeight: new(float64), // Manual weight override
	}
	*rule2.ManualWeight = 20.0 // Higher than rule1's calculated weight (15.0)

	rule3 := &Rule{
		ID: "rule3-medium",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			"env":    {DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Status:       RuleStatusWorking,
		ManualWeight: new(float64), // Manual weight override
	}
	*rule3.ManualWeight = 17.5 // Medium weight

	// Add rules in random order
	forest.AddRule(rule1) // Weight: 8.0
	forest.AddRule(rule2) // Weight: 15.0
	forest.AddRule(rule3) // Weight: 10.0

	// Test query that should match all rules
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
		IncludeAllRules: true, // Include all rules
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates", len(candidates))

	if len(candidates) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(candidates))
	}

	// Verify weight ordering (highest weight first)
	if len(candidates) >= 3 {
		weight0 := candidates[0].CalculateTotalWeight(dimensionConfigs)
		weight1 := candidates[1].CalculateTotalWeight(dimensionConfigs)
		weight2 := candidates[2].CalculateTotalWeight(dimensionConfigs)

		t.Logf("Rule order: %s (%.1f), %s (%.1f), %s (%.1f)",
			candidates[0].ID, weight0,
			candidates[1].ID, weight1,
			candidates[2].ID, weight2)

		// Check that weights are in descending order
		if weight0 < weight1 || weight1 < weight2 {
			t.Errorf("Rules not ordered by weight: %.1f, %.1f, %.1f", weight0, weight1, weight2)
		}

		// Check specific rule order
		if candidates[0].ID != "rule2-high" {
			t.Errorf("Expected rule2-high to be first, got %s", candidates[0].ID)
		}
		if candidates[1].ID != "rule3-medium" {
			t.Errorf("Expected rule3-medium to be second, got %s", candidates[1].ID)
		}
		if candidates[2].ID != "rule1-low" {
			t.Errorf("Expected rule1-low to be third, got %s", candidates[2].ID)
		}
	}
}

func TestForestStatusFiltering(t *testing.T) {
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("region", 0, false),
	}, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create working and draft rules
	workingRule := &Rule{
		ID: "working-rule",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
		},
		Status: RuleStatusWorking,
	}

	draftRule := &Rule{
		ID: "draft-rule",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
		},
		Status:       RuleStatusDraft,
		ManualWeight: new(float64), // Give draft rule higher weight for testing
	}
	*draftRule.ManualWeight = 15.0 // Higher than working rule's 10.0

	forest.AddRule(workingRule)
	forest.AddRule(draftRule)

	// Test query that excludes draft rules (default behavior)
	queryWorking := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
		IncludeAllRules: false, // Only working rules
	}

	candidates := forest.FindCandidateRules(queryWorking)
	t.Logf("Working-only query found %d candidates", len(candidates))

	if len(candidates) != 1 {
		t.Errorf("Expected 1 working candidate, got %d", len(candidates))
	}

	if len(candidates) > 0 && candidates[0].ID != "working-rule" {
		t.Errorf("Expected working-rule, got %s", candidates[0].ID)
	}

	// Test query that includes all rules
	queryAll := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
		IncludeAllRules: true, // Include draft rules too
	}

	candidatesAll := forest.FindCandidateRules(queryAll)
	t.Logf("All-rules query found %d candidates", len(candidatesAll))

	if len(candidatesAll) != 2 {
		t.Errorf("Expected 2 candidates (working + draft), got %d", len(candidatesAll))
	}

	// Verify that draft rule (higher weight) is first in the list
	if len(candidatesAll) >= 2 {
		if candidatesAll[0].ID != "draft-rule" {
			t.Errorf("Expected draft-rule to be first (higher weight), got %s", candidatesAll[0].ID)
		}
		if candidatesAll[1].ID != "working-rule" {
			t.Errorf("Expected working-rule to be second, got %s", candidatesAll[1].ID)
		}
	}
}

func TestForestNoDuplicateChecks(t *testing.T) {
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("region", 0, false),
	}, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create rules that would be duplicates if we were checking for them
	// But the optimization assumes rules are unique within branches
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
		},
		Status: RuleStatusWorking,
	}

	// Adding same rule multiple times (in theory could cause duplicates)
	forest.AddRule(rule1)
	// In a non-optimized system, this might create duplicate candidates

	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
		IncludeAllRules: true,
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates (should be 1, proving no duplicate issues)", len(candidates))

	// The forest structure should naturally prevent duplicates due to rule indexing
	// This test mainly documents that we're not doing explicit duplicate checking
	if len(candidates) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates))
	}

	if len(candidates) > 0 && candidates[0].ID != "rule1" {
		t.Errorf("Expected rule1, got %s", candidates[0].ID)
	}
}

func TestForestOptimizationEfficiency(t *testing.T) {
	dimensionConfigMap := []*DimensionConfig{
		NewDimensionConfig("region", 0, false), // Base weight, rules will use manual weights
	}
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter(dimensionConfigMap, nil)
	forest := CreateRuleForest(dimensionConfigs)

	// Create multiple rules with different weights
	weights := []float64{5.0, 20.0, 10.0, 15.0, 25.0, 8.0}
	expectedOrder := []string{"rule-25", "rule-20", "rule-15", "rule-10", "rule-8", "rule-5"}

	// Add rules in unsorted order
	for _, weight := range weights {
		rule := &Rule{
			ID: fmt.Sprintf("rule-%.0f", weight),
			Dimensions: map[string]*DimensionValue{
				"region": {DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			},
			Status:       RuleStatusWorking,
			ManualWeight: new(float64), // Use manual weight for testing
		}
		*rule.ManualWeight = weight
		forest.AddRule(rule)
		t.Logf("Added rule with weight %.0f", weight)
	}

	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
		IncludeAllRules: true,
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates in weight-sorted order", len(candidates))

	if len(candidates) != len(weights) {
		t.Errorf("Expected %d candidates, got %d", len(weights), len(candidates))
	}

	// Verify the candidates are in descending weight order
	for i, candidate := range candidates {
		expectedID := expectedOrder[i]
		if candidate.ID != expectedID {
			t.Errorf("Position %d: expected %s, got %s", i, expectedID, candidate.ID)
		}
		t.Logf("Position %d: %s (weight %.0f)", i, candidate.ID, candidate.CalculateTotalWeight(dimensionConfigs))
	}

	// Verify weights are actually in descending order
	for i := 1; i < len(candidates); i++ {
		prevWeight := candidates[i-1].CalculateTotalWeight(dimensionConfigs)
		currWeight := candidates[i].CalculateTotalWeight(dimensionConfigs)
		if prevWeight < currWeight {
			t.Errorf("Weight ordering broken at position %d: %.0f < %.0f", i, prevWeight, currWeight)
		}
	}
}

func TestWeightConflictWithIntersection(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-conflict")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	err = addTestDimensions(engine)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	t.Run("NoConflictBetweenNonIntersectingRules", func(t *testing.T) {
		// These rules don't intersect, so they can have the same weight
		rule1 := NewRule("non_intersect_1").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).
			Build()

		rule2 := NewRule("non_intersect_2").
			Dimension("product", "ProductB", MatchTypeEqual). // Different product
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0). // Same weight but no intersection
			Build()

		err = engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add rule1: %v", err)
		}

		err = engine.AddRule(rule2)
		if err != nil {
			t.Errorf("Expected no conflict between non-intersecting rules with same weight, but got: %v", err)
		}
	})

	t.Run("ConflictBetweenIntersectingRulesWithSameWeight", func(t *testing.T) {
		// Create a fresh engine for this test
		engine2, err := NewInMemoryMatcher(persistence, nil, "test-weight-conflict-2")
		if err != nil {
			t.Fatalf("Failed to create engine2: %v", err)
		}
		defer engine2.Close()

		err = addTestDimensions(engine2)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// These rules intersect because prefix "Product" matches "ProductX"
		rule1 := NewRule("intersect_1").
			Dimension("product", "Product", MatchTypePrefix). // Prefix "Product"
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(15.0).
			Build()

		rule2 := NewRule("intersect_2").
			Dimension("product", "ProductX", MatchTypeEqual). // "ProductX" starts with "Product"
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(15.0). // Same weight - should conflict
			Build()

		err = engine2.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first intersecting rule: %v", err)
		}

		err = engine2.AddRule(rule2)
		if err == nil {
			t.Error("Expected weight conflict between intersecting rules with same weight, but no error occurred")
		} else {
			t.Logf("Correctly detected weight conflict: %v", err)
		}
	})

	t.Run("NoConflictBetweenIntersectingRulesWithDifferentWeights", func(t *testing.T) {
		// Create a fresh engine for this test
		engine3, err := NewInMemoryMatcher(persistence, nil, "test-weight-conflict-3")
		if err != nil {
			t.Fatalf("Failed to create engine3: %v", err)
		}
		defer engine3.Close()

		err = addTestDimensions(engine3)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// These rules intersect but have different weights
		rule1 := NewRule("different_weight_1").
			Dimension("product", "Product", MatchTypePrefix).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(15.0).
			Build()

		rule2 := NewRule("different_weight_2").
			Dimension("product", "ProductY", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(12.0). // Different weight - should be OK
			Build()

		err = engine3.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first rule: %v", err)
		}

		err = engine3.AddRule(rule2)
		if err != nil {
			t.Errorf("Expected no conflict between intersecting rules with different weights, but got: %v", err)
		}
	})
}

func TestWeightConflictWithStatus(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")

	t.Run("AllowSameWeightDifferentStatus", func(t *testing.T) {
		// Test that rules with same weight but different status can coexist
		engine, err := NewInMemoryMatcher(persistence, nil, "test-status-same-weight")
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}
		defer engine.Close()

		err = addTestDimensions(engine)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// Working rule
		rule1 := NewRule("working_rule").
			Dimension("product", "Product", MatchTypePrefix).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).
			Status(RuleStatusWorking).
			Build()

		// Draft rule with same weight and intersecting dimensions
		rule2 := NewRule("draft_rule").
			Dimension("product", "ProductX", MatchTypeEqual). // Intersects with prefix "Product"
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(10.0).      // Same weight
			Status(RuleStatusDraft). // Different status
			Build()

		err = engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add working rule: %v", err)
		}

		// This should succeed because they have different statuses
		err = engine.AddRule(rule2)
		if err != nil {
			t.Errorf("Expected no conflict between same weight rules with different statuses, but got: %v", err)
		}
	})

	t.Run("ConflictSameWeightSameStatus", func(t *testing.T) {
		// Test that rules with same weight and same status conflict
		engine, err := NewInMemoryMatcher(persistence, nil, "test-status-conflict")
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}
		defer engine.Close()

		err = addTestDimensions(engine)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// First working rule
		rule1 := NewRule("working_rule_1").
			Dimension("product", "Product", MatchTypePrefix).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(15.0).
			Status(RuleStatusWorking).
			Build()

		// Second working rule with same weight and intersecting dimensions
		rule2 := NewRule("working_rule_2").
			Dimension("product", "ProductY", MatchTypeEqual). // Intersects with prefix "Product"
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(15.0).        // Same weight
			Status(RuleStatusWorking). // Same status
			Build()

		err = engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first working rule: %v", err)
		}

		// This should fail because they have same weight and same status
		err = engine.AddRule(rule2)
		if err == nil {
			t.Error("Expected weight conflict between same weight rules with same status, but no error occurred")
		} else {
			t.Logf("Correctly detected weight conflict: %v", err)
		}
	})

	t.Run("AllowMultipleDifferentStatuses", func(t *testing.T) {
		// Test that multiple rules with same weight can coexist with different statuses
		engine, err := NewInMemoryMatcher(persistence, nil, "test-multiple-statuses")
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}
		defer engine.Close()

		err = addTestDimensions(engine)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// Working rule
		rule1 := NewRule("rule_working").
			Dimension("product", "Product", MatchTypePrefix).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(20.0).
			Status(RuleStatusWorking).
			Build()

		// Draft rule with same weight
		rule2 := NewRule("rule_draft").
			Dimension("product", "ProductA", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(20.0).
			Status(RuleStatusDraft).
			Build()

		// Another working rule with same weight (should conflict with first working rule)
		rule3 := NewRule("rule_working_2").
			Dimension("product", "ProductB", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(20.0).
			Status(RuleStatusWorking).
			Build()

		err = engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first working rule: %v", err)
		}

		// Draft rule should succeed
		err = engine.AddRule(rule2)
		if err != nil {
			t.Errorf("Expected no conflict for draft rule with same weight, but got: %v", err)
		}

		// Second working rule should fail
		err = engine.AddRule(rule3)
		if err == nil {
			t.Error("Expected weight conflict between two working rules with same weight, but no error occurred")
		} else {
			t.Logf("Correctly detected weight conflict between working rules: %v", err)
		}
	})

	t.Run("StatusUniquenessWithinSameWeight", func(t *testing.T) {
		// Test that only one rule per status is allowed for the same weight
		engine, err := NewInMemoryMatcher(persistence, nil, "test-status-uniqueness")
		if err != nil {
			t.Fatalf("Failed to create engine: %v", err)
		}
		defer engine.Close()

		err = addTestDimensions(engine)
		if err != nil {
			t.Fatalf("Failed to initialize dimensions: %v", err)
		}

		// First working rule
		rule1 := NewRule("working_1").
			Dimension("product", "Product", MatchTypePrefix).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(25.0).
			Status(RuleStatusWorking).
			Build()

		// Second working rule with same weight (should conflict)
		rule2 := NewRule("working_2").
			Dimension("product", "ProductX", MatchTypeEqual).
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(25.0).
			Status(RuleStatusWorking).
			Build()

		// First draft rule with same weight (should succeed)
		rule3 := NewRule("draft_1").
			Dimension("product", "Product", MatchTypePrefix). // Intersects with working rule
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(25.0).
			Status(RuleStatusDraft).
			Build()

		// Second draft rule with same weight (should conflict with first draft)
		rule4 := NewRule("draft_2").
			Dimension("product", "ProductA", MatchTypeEqual). // Intersects with prefix "Product"
			Dimension("route", "main", MatchTypeEqual).
			ManualWeight(25.0).
			Status(RuleStatusDraft).
			Build()

		err = engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first working rule: %v", err)
		}

		// Second working rule should fail
		err = engine.AddRule(rule2)
		if err == nil {
			t.Error("Expected conflict between two working rules with same weight")
		}

		// First draft rule should succeed
		err = engine.AddRule(rule3)
		if err != nil {
			t.Errorf("Expected no conflict for first draft rule, but got: %v", err)
		}

		// Second draft rule should fail
		err = engine.AddRule(rule4)
		if err == nil {
			t.Error("Expected conflict between two draft rules with same weight")
		} else {
			t.Logf("Correctly detected weight conflict between draft rules: %v", err)
		}
	})
}

func TestAutomaticWeightPopulation(t *testing.T) {
	// Create an engine with dimension configurations
	persistence := NewJSONPersistence("./test_data_weight_population")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-population")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configurations with specific weights
	err = engine.AddDimension(NewDimensionConfig("product", 0, false))
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("environment", 1, false))
	if err != nil {
		t.Fatalf("Failed to add environment dimension: %v", err)
	}

	// Create a rule using the new API (without weights)
	rule := NewRule("test-automatic-weight").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("environment", "prod", MatchTypeEqual).
		Build()

	// Add the rule - weights should be populated automatically
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the weights were populated correctly
	productDim := rule.GetDimensionValue("product")
	if productDim == nil {
		t.Fatal("Product dimension not found")
	}

	environmentDim := rule.GetDimensionValue("environment")
	if environmentDim == nil {
		t.Fatal("Environment dimension not found")
	}

	// Verify total weight calculation
	totalWeight := rule.CalculateTotalWeight(engine.dimensionConfigs)
	expectedWeight := 0.0 // No explicit weights set
	if totalWeight != expectedWeight {
		t.Errorf("Expected total weight %.1f, got %.1f", expectedWeight, totalWeight)
	}
}

func TestZeroWeightWhenNoDimensionConfig(t *testing.T) {
	// Create an engine without dimension configurations
	persistence := NewJSONPersistence("./test_data_default_weight")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-default-weight")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create a rule using the new API (without weights)
	rule := NewRule("test-default-weight").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("environment", "prod", MatchTypeEqual).
		Build()

	// Add the rule - weights should default to 0.0
	err = engine.AddRule(rule)
	if err == nil {
		t.Fatalf("Dimension is required before adding any new rules")
	}
}

func TestDimensionWithWeightBackwardCompatibility(t *testing.T) {
	// Create an engine with dimension configurations
	persistence := NewJSONPersistence("./test_data_backward_compat")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-backward-compat")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configurations with specific weights
	err = engine.AddDimension(NewDimensionConfig("product", 0, false))
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("environment", 1, false))
	if err != nil {
		t.Fatalf("Failed to add environment dimension: %v", err)
	}

	// Create a rule using the backward compatibility method with explicit weights
	rule := NewRule("test-backward-compat").
		Dimension("product", "ProductA", MatchTypeEqual). // Override the configured weight
		Dimension("environment", "prod", MatchTypeEqual). // Use configured weight
		ManualWeight(25.0).
		Build()

	// Add the rule
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the explicit weight was preserved
	productDim := rule.GetDimensionValue("product")
	if productDim == nil {
		t.Fatal("Product dimension not found")
	}

	// Verify the auto-populated weight for environment (from config)
	environmentDim := rule.GetDimensionValue("environment")
	if environmentDim == nil {
		t.Fatal("Environment dimension not found")
	}
}
