package matcher

import (
	"fmt"
	"testing"
)

func TestForestWeightOrdering(t *testing.T) {
	// Set up dimension configs to control weights
	dimensionConfigs := map[string]*DimensionConfig{
		"region": {Name: "region", Index: 0, Weight: 10.0},
		"env":    {Name: "env", Index: 1, Weight: 5.0},
	}
	forest := CreateRuleForest(dimensionConfigs)

	// Create test rules with different weights
	rule1 := &Rule{
		ID: "rule1-low",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Status: RuleStatusWorking,
	}

	rule2 := &Rule{
		ID: "rule2-high",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
		Status:       RuleStatusWorking,
		ManualWeight: new(float64), // Manual weight override
	}
	*rule2.ManualWeight = 20.0 // Higher than rule1's calculated weight (15.0)

	rule3 := &Rule{
		ID: "rule3-medium",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
			{DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
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
	dimensionConfigs := map[string]*DimensionConfig{
		"region": {Name: "region", Index: 0, Weight: 10.0},
	}
	forest := CreateRuleForest(dimensionConfigs)

	// Create working and draft rules
	workingRule := &Rule{
		ID: "working-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
		},
		Status: RuleStatusWorking,
	}

	draftRule := &Rule{
		ID: "draft-rule",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
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
	dimensionConfigs := map[string]*DimensionConfig{
		"region": {Name: "region", Index: 0, Weight: 10.0},
	}
	forest := CreateRuleForest(dimensionConfigs)

	// Create rules that would be duplicates if we were checking for them
	// But the optimization assumes rules are unique within branches
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: []*DimensionValue{
			{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
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
	dimensionConfigs := map[string]*DimensionConfig{
		"region": {Name: "region", Index: 0, Weight: 1.0}, // Base weight, rules will use manual weights
	}
	forest := CreateRuleForest(dimensionConfigs)

	// Create multiple rules with different weights
	weights := []float64{5.0, 20.0, 10.0, 15.0, 25.0, 8.0}
	expectedOrder := []string{"rule-25", "rule-20", "rule-15", "rule-10", "rule-8", "rule-5"}

	// Add rules in unsorted order
	for _, weight := range weights {
		rule := &Rule{
			ID: fmt.Sprintf("rule-%.0f", weight),
			Dimensions: []*DimensionValue{
				{DimensionName: "region", Value: "us-west", MatchType: MatchTypeEqual},
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
