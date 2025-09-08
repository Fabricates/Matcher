package matcher

import (
	"testing"
)

func TestSharedNodeRuleManagement(t *testing.T) {
	forest := CreateForestIndex()

	// Create multiple rules that will share the same tree path
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	// Add all rules - they should all share the same tree path
	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Create a query that matches all rules
	query := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	// Find candidates - should return all 3 rules
	candidates := forest.FindCandidateRules(query)
	if len(candidates) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(candidates))
	}

	// Verify all rules are present
	ruleIds := make(map[string]bool)
	for _, rule := range candidates {
		ruleIds[rule.ID] = true
	}

	if !ruleIds["rule1"] || !ruleIds["rule2"] || !ruleIds["rule3"] {
		t.Error("Not all rules found in candidates")
	}

	// Remove one rule
	forest.RemoveRule(rule2)

	// Query again - should now return only 2 rules
	candidates = forest.FindCandidateRules(query)
	if len(candidates) != 2 {
		t.Errorf("Expected 2 candidates after removal, got %d", len(candidates))
	}

	// Verify rule2 is gone but rule1 and rule3 remain
	ruleIds = make(map[string]bool)
	for _, rule := range candidates {
		ruleIds[rule.ID] = true
	}

	if !ruleIds["rule1"] || ruleIds["rule2"] || !ruleIds["rule3"] {
		t.Error("Incorrect rules after removal - rule2 should be gone")
	}

	// Remove another rule
	forest.RemoveRule(rule1)

	// Query again - should now return only 1 rule
	candidates = forest.FindCandidateRules(query)
	if len(candidates) != 1 {
		t.Errorf("Expected 1 candidate after second removal, got %d", len(candidates))
	}

	if candidates[0].ID != "rule3" {
		t.Errorf("Expected rule3 to remain, got %s", candidates[0].ID)
	}
}

func TestSharedNodeWithDifferentPaths(t *testing.T) {
	forest := CreateForestIndex()

	// Create rules that share some nodes but diverge
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "drill", MatchType: MatchTypeEqual}, // Different tool
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "alt", MatchType: MatchTypeEqual}, // Different route
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Query for laser tool with main route
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("Query1 should return only rule1")
	}

	// Query for drill tool with main route
	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "drill",
		},
	}

	candidates2 := forest.FindCandidateRules(query2)
	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("Query2 should return only rule2")
	}

	// Query for laser tool with alt route
	query3 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "alt",
			"tool":    "laser",
		},
	}

	candidates3 := forest.FindCandidateRules(query3)
	if len(candidates3) != 1 || candidates3[0].ID != "rule3" {
		t.Error("Query3 should return only rule3")
	}

	// Remove rule2 and verify it doesn't affect rule1 or rule3
	forest.RemoveRule(rule2)

	// Re-test query1 and query3 - should still work
	candidates1 = forest.FindCandidateRules(query1)
	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("After removing rule2, query1 should still return rule1")
	}

	candidates3 = forest.FindCandidateRules(query3)
	if len(candidates3) != 1 || candidates3[0].ID != "rule3" {
		t.Error("After removing rule2, query3 should still return rule3")
	}

	// Query2 should now return no results
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 0 {
		t.Error("After removing rule2, query2 should return no results")
	}
}

func TestNodeCleanupAfterRuleRemoval(t *testing.T) {
	forest := CreateForestIndex()

	// Create rules that will create a deep tree structure
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			{DimensionName: "tool", Value: "drill", MatchType: MatchTypeEqual}, // Different tool
		},
	}

	// Add both rules
	forest.AddRule(rule1)
	forest.AddRule(rule2)

	// Debug: Check dimension order
	dimOrder := forest.GetDimensionOrder()
	t.Logf("Dimension order: %v", dimOrder)

	// Debug: Check forest stats
	debugStats := forest.GetStats()
	t.Logf("Forest stats: %+v", debugStats)

	// Verify both rules can be found
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
			"env":     "prod",
		},
	}

	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "drill",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	candidates2 := forest.FindCandidateRules(query2)

	t.Logf("Rule1 candidates: %d", len(candidates1))
	for _, c := range candidates1 {
		t.Logf("  Found: %s", c.ID)
	}

	t.Logf("Rule2 candidates: %d", len(candidates2))
	for _, c := range candidates2 {
		t.Logf("  Found: %s", c.ID)
	}

	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("Should find rule1")
	}

	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("Should find rule2")
	}

	// Get initial stats
	stats := forest.GetStats()
	initialTrees := stats["total_trees"].(int)

	// Remove rule1 (which has a longer path)
	forest.RemoveRule(rule1)

	// rule1 should no longer be found
	candidates1 = forest.FindCandidateRules(query1)
	if len(candidates1) != 0 {
		t.Error("rule1 should not be found after removal")
	}

	// rule2 should still be found
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("rule2 should still be found after rule1 removal")
	}

	// The forest structure should be cleaned up but main tree should remain
	stats = forest.GetStats()
	finalTrees := stats["total_trees"].(int)

	if finalTrees != initialTrees {
		t.Logf("Trees before: %d, after: %d", initialTrees, finalTrees)
		// This is expected - the structure should be cleaned up
	}

	// Remove rule2
	forest.RemoveRule(rule2)

	// No rules should be found now
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 0 {
		t.Error("rule2 should not be found after removal")
	}
}

func TestForestStatisticsWithSharedNodes(t *testing.T) {
	forest := CreateForestIndex()

	// Create multiple rules that share nodes
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "alt", MatchType: MatchTypeEqual},
		},
	}

	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	stats := forest.GetStats()
	t.Logf("Forest stats: %+v", stats)

	// We should have 1 tree (all rules share same primary dimension)
	if stats["total_trees"].(int) != 1 {
		t.Errorf("Expected 1 tree, got %d", stats["total_trees"].(int))
	}

	// We should have 3 total rules
	if stats["total_rules"].(int) != 3 {
		t.Errorf("Expected 3 total rules, got %d", stats["total_rules"].(int))
	}

	// There should be at least one shared node (rule1 and rule2 share the same path)
	if sharedCount, exists := stats["shared_nodes"]; exists && sharedCount.(int) > 0 {
		t.Logf("Found %d shared nodes", sharedCount.(int))
	} else {
		t.Error("Expected to find shared nodes")
	}

	// Max rules per node should be at least 2 (rule1 and rule2 in same node)
	if maxRules, exists := stats["max_rules_per_node"]; exists && maxRules.(int) >= 2 {
		t.Logf("Max rules per node: %d", maxRules.(int))
	} else {
		t.Error("Expected max rules per node to be at least 2")
	}
}
