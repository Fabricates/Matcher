package matcher

import (
	"testing"
)

func TestForestWeightOrdering(t *testing.T) {
	forest := CreateForestIndex()

	// Create test rules with different weights
	rule1 := &Rule{
		ID: "rule1-low",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual, Weight: 5.0},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual, Weight: 3.0},
		},
		Status: RuleStatusWorking,
	}

	rule2 := &Rule{
		ID: "rule2-high",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual, Weight: 10.0},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual, Weight: 5.0},
		},
		Status: RuleStatusWorking,
	}

	rule3 := &Rule{
		ID: "rule3-medium",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual, Weight: 7.0},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual, Weight: 3.0},
		},
		Status: RuleStatusWorking,
	}

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
		weight0 := candidates[0].CalculateTotalWeight()
		weight1 := candidates[1].CalculateTotalWeight()
		weight2 := candidates[2].CalculateTotalWeight()

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
	forest := CreateForestIndex()

	// Create working and draft rules
	workingRule := &Rule{
		ID: "working-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual, Weight: 10.0},
		},
		Status: RuleStatusWorking,
	}

	draftRule := &Rule{
		ID: "draft-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual, Weight: 15.0},
		},
		Status: RuleStatusDraft,
	}

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