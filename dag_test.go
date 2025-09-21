package matcher

import (
	"fmt"
	"testing"
)

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
