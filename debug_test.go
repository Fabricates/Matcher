package matcher

import (
	"testing"
)

func TestDebugDAGStructure(t *testing.T) {
	forest := CreateForestIndex()

	// Create the exact same rules as the failing test
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

	// Get stats to understand structure
	stats := forest.GetStats()
	t.Logf("Forest structure: %+v", stats)

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
		t.Errorf("Query1 expected rule1, got %d candidates:", len(candidates1))
		for _, rule := range candidates1 {
			t.Logf("  Query1 found: %s", rule.ID)
		}
	} else {
		t.Logf("Query1 correctly found rule1")
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
		t.Errorf("Query2 expected rule2, got %d candidates:", len(candidates2))
		for _, rule := range candidates2 {
			t.Logf("  Query2 found: %s", rule.ID)
		}
	} else {
		t.Logf("Query2 correctly found rule2")
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
		t.Errorf("Query3 expected rule3, got %d candidates:", len(candidates3))
		for _, rule := range candidates3 {
			t.Logf("  Query3 found: %s", rule.ID)
		}
	} else {
		t.Logf("Query3 correctly found rule3")
	}
}

func TestDebugRuleStorage(t *testing.T) {
	forest := CreateForestIndex()

	// Create a simple rule to trace through the storage
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			{DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
		},
	}

	t.Logf("Adding rule1...")
	forest.AddRule(rule1)

	// Check how many times rule1 appears
	query := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
		},
	}

	candidates := forest.FindCandidateRules(query)
	t.Logf("Found %d candidates for rule1:", len(candidates))
	for i, rule := range candidates {
		t.Logf("  %d: %s", i, rule.ID)
	}

	if len(candidates) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates))
	}
}
