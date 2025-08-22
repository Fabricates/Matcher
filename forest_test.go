package matcher

import (
	"testing"
)

func TestForestStructure(t *testing.T) {
	forest := CreateForestIndex()

	// Create test rules with different primary dimensions
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "10"},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-east", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "staging", MatchType: MatchTypeEqual},
		},
		Metadata: map[string]string{"weight": "20"},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			{DimensionName: "service", Value: "api", MatchType: MatchTypePrefix},
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
	forest := CreateForestIndex()

	// Create a rule with prefix matching
	rule := &Rule{
		ID: "prefix-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "service", Value: "api", MatchType: MatchTypePrefix},
			{DimensionName: "version", Value: "v1", MatchType: MatchTypeEqual},
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
	forest := CreateForestIndex()

	rule := &Rule{
		ID: "test-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
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
