package matcher

import (
	"testing"
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
