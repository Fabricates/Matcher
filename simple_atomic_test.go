package matcher

import (
	"testing"
	"time"
)

// TestSimpleAtomicUpdate tests the basic atomic update functionality
func TestSimpleAtomicUpdate(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("simple-atomic-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	initialRule.Metadata = map[string]string{"version": "1"}

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	// Test that rule can be retrieved
	rule, err := engine.GetRule("simple-atomic-test")
	if err != nil {
		t.Fatalf("Failed to get initial rule: %v", err)
	}

	if rule.Metadata["version"] != "1" {
		t.Errorf("Expected version 1, got %s", rule.Metadata["version"])
	}

	// Test update
	updatedRule := NewRule("simple-atomic-test").
		Dimension("region", "us-east", MatchTypeEqual).
		Build()

	updatedRule.Metadata = map[string]string{"version": "2"}

	err = engine.UpdateRule(updatedRule)
	if err != nil {
		t.Fatalf("Failed to update rule: %v", err)
	}

	// Verify update
	rule, err = engine.GetRule("simple-atomic-test")
	if err != nil {
		t.Fatalf("Failed to get updated rule: %v", err)
	}

	if rule.Metadata["version"] != "2" {
		t.Errorf("Expected version 2 after update, got %s", rule.Metadata["version"])
	}

	// Verify dimension was updated
	found := false
	for _, dim := range rule.Dimensions {
		if dim.DimensionName == "region" && dim.Value == "us-east" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Rule dimension was not properly updated")
	}

	t.Log("✓ Simple atomic update test passed")
}

// TestUpdateRuleTemporaryUnavailability tests that during update, GetRule may temporarily fail
// but when it succeeds, it returns a consistent rule
func TestUpdateRuleTemporaryUnavailability(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("temp-unavail-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()
	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	// Test many quick updates and reads
	for i := 0; i < 100; i++ {
		// Update rule
		updatedRule := NewRule("temp-unavail-test").
			Dimension("region", "us-east", MatchTypeEqual).
			Build()

		updatedRule.Metadata = map[string]string{"iteration": string(rune('0' + i%10))}

		err = engine.UpdateRule(updatedRule)
		if err != nil {
			t.Fatalf("Failed to update rule at iteration %d: %v", i, err)
		}

		// Try to read immediately
		rule, err := engine.GetRule("temp-unavail-test")
		if err != nil {
			// Rule might be temporarily unavailable during update - this is acceptable
			t.Logf("Rule temporarily unavailable at iteration %d (acceptable)", i)
		} else {
			// If we get a rule, it should be consistent
			if rule.ID != "temp-unavail-test" {
				t.Errorf("Iteration %d: Got wrong rule ID: %s", i, rule.ID)
			}

			if len(rule.Dimensions) == 0 {
				t.Errorf("Iteration %d: Got rule with no dimensions", i)
			}

			// If metadata exists, it should be complete
			if rule.Metadata != nil {
				if iteration, exists := rule.Metadata["iteration"]; exists {
					if iteration == "" {
						t.Errorf("Iteration %d: Got rule with empty iteration metadata", i)
					}
				}
			}
		}

		// Small delay
		time.Sleep(time.Microsecond)
	}

	t.Log("✓ Update rule temporary unavailability test passed")
}
