package matcher

import (
	"strings"
	"testing"
)

func TestWeightConflictDetection(t *testing.T) {
	persistence := NewJSONPersistence("./test_data_weight_conflict")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-conflict")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test 1: Add two rules with different weights - should succeed
	t.Run("DifferentWeights", func(t *testing.T) {
		rule1 := NewRule("rule1").
			Dimension("product", "ProductA", MatchTypeEqual, 10.0).
			Dimension("route", "main", MatchTypeEqual, 5.0).
			Build()

		rule2 := NewRule("rule2").
			Dimension("product", "ProductB", MatchTypeEqual, 8.0).
			Dimension("route", "backup", MatchTypeEqual, 4.0).
			Build()

		err := engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add rule1 with weight 15.0: %v", err)
		}

		err = engine.AddRule(rule2)
		if err != nil {
			t.Fatalf("Failed to add rule2 with weight 12.0: %v", err)
		}
	})

	// Test 2: Add two rules with same calculated weight - should fail
	t.Run("SameCalculatedWeight", func(t *testing.T) {
		rule3 := NewRule("rule3").
			Dimension("product", "ProductC", MatchTypeEqual, 7.0).
			Dimension("route", "test", MatchTypeEqual, 8.0).
			Build() // Total weight: 15.0 (same as rule1)

		err := engine.AddRule(rule3)
		if err == nil {
			t.Fatal("Expected weight conflict error but rule was added successfully")
		}

		if !strings.Contains(err.Error(), "weight conflict: rule 'rule1' already has weight 15.00") {
			t.Errorf("Expected weight conflict error message containing 'weight conflict: rule 'rule1' already has weight 15.00', got '%s'", err.Error())
		}
	})

	// Test 3: Add two rules with same manual weight - should fail
	t.Run("SameManualWeight", func(t *testing.T) {
		rule4 := NewRule("rule4").
			Dimension("product", "ProductD", MatchTypeEqual, 5.0).
			ManualWeight(20.0).
			Build()

		rule5 := NewRule("rule5").
			Dimension("product", "ProductE", MatchTypeEqual, 10.0).
			ManualWeight(20.0). // Same manual weight as rule4
			Build()

		err := engine.AddRule(rule4)
		if err != nil {
			t.Fatalf("Failed to add rule4 with manual weight 20.0: %v", err)
		}

		err = engine.AddRule(rule5)
		if err == nil {
			t.Fatal("Expected weight conflict error but rule was added successfully")
		}

		if !strings.Contains(err.Error(), "weight conflict: rule 'rule4' already has weight 20.00") {
			t.Errorf("Expected weight conflict error message containing 'weight conflict: rule 'rule4' already has weight 20.00', got '%s'", err.Error())
		}
	})

	// Test 4: Manual weight overrides calculated weight in conflict detection
	t.Run("ManualWeightOverrideConflict", func(t *testing.T) {
		rule6 := NewRule("rule6").
			Dimension("product", "ProductF", MatchTypeEqual, 2.0).
			Dimension("route", "test", MatchTypeEqual, 3.0).
			ManualWeight(12.0). // Manual weight 12.0 (same as rule2's calculated weight)
			Build()

		err := engine.AddRule(rule6)
		if err == nil {
			t.Fatal("Expected weight conflict error but rule was added successfully")
		}

		if !strings.Contains(err.Error(), "weight conflict: rule 'rule2' already has weight 12.00") {
			t.Errorf("Expected weight conflict error message containing 'weight conflict: rule 'rule2' already has weight 12.00', got '%s'", err.Error())
		}
	})

	// Test 5: Zero weight rules should conflict
	t.Run("ZeroWeightConflict", func(t *testing.T) {
		rule7 := NewRule("rule7").
			Dimension("product", "", MatchTypeAny, 0.0).
			Dimension("route", "", MatchTypeAny, 0.0).
			Build() // Total weight: 0.0

		rule8 := NewRule("rule8").
			Dimension("environment", "", MatchTypeAny, 0.0).
			Build() // Total weight: 0.0

		err := engine.AddRule(rule7)
		if err != nil {
			t.Fatalf("Failed to add rule7 with weight 0.0: %v", err)
		}

		err = engine.AddRule(rule8)
		if err == nil {
			t.Fatal("Expected weight conflict error but rule was added successfully")
		}

		if !strings.Contains(err.Error(), "weight conflict: rule 'rule7' already has weight 0.00") {
			t.Errorf("Expected weight conflict error message containing 'weight conflict: rule 'rule7' already has weight 0.00', got '%s'", err.Error())
		}
	})
}

func TestWeightConflictWithUpdateRule(t *testing.T) {
	persistence := NewJSONPersistence("./test_data_weight_update")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-update")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add initial rule
	rule1 := NewRule("update_rule").
		Dimension("product", "ProductA", MatchTypeEqual, 10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	// Add another rule with different weight
	rule2 := NewRule("other_rule").
		Dimension("product", "ProductB", MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add second rule: %v", err)
	}

	// Test: Update rule to have same weight as existing rule - should fail
	t.Run("UpdateToConflictingWeight", func(t *testing.T) {
		updatedRule := NewRule("update_rule").
			Dimension("product", "ProductA", MatchTypeEqual, 5.0). // Same weight as rule2
			Build()

		err = engine.updateRule(updatedRule)
		if err == nil {
			t.Fatal("Expected weight conflict error when updating to conflicting weight")
		}

		if !strings.Contains(err.Error(), "weight conflict: rule 'other_rule' already has weight 5.00") {
			t.Errorf("Expected weight conflict error message containing 'weight conflict: rule 'other_rule' already has weight 5.00', got '%s'", err.Error())
		}
	})

	// Test: Update rule to have unique weight - should succeed
	t.Run("UpdateToUniqueWeight", func(t *testing.T) {
		updatedRule := NewRule("update_rule").
			Dimension("product", "ProductA", MatchTypeEqual, 15.0). // Unique weight
			Build()

		err = engine.updateRule(updatedRule)
		if err != nil {
			t.Fatalf("Failed to update rule to unique weight: %v", err)
		}
	})
}

func TestWeightConflictPrecision(t *testing.T) {
	persistence := NewJSONPersistence("./test_data_weight_precision")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-precision")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test: Very close but different weights should not conflict
	t.Run("CloseButDifferentWeights", func(t *testing.T) {
		rule1 := NewRule("precision1").
			Dimension("product", "ProductA", MatchTypeEqual, 10.001).
			Build()

		rule2 := NewRule("precision2").
			Dimension("product", "ProductB", MatchTypeEqual, 10.002).
			Build()

		err := engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add rule1: %v", err)
		}

		err = engine.AddRule(rule2)
		if err != nil {
			t.Fatalf("Failed to add rule2 with slightly different weight: %v", err)
		}
	})

	// Test: Exactly same weights should conflict
	t.Run("ExactlySameWeights", func(t *testing.T) {
		rule3 := NewRule("precision3").
			Dimension("product", "ProductC", MatchTypeEqual, 7.123456789).
			Build()

		rule4 := NewRule("precision4").
			Dimension("product", "ProductD", MatchTypeEqual, 7.123456789).
			Build()

		err := engine.AddRule(rule3)
		if err != nil {
			t.Fatalf("Failed to add rule3: %v", err)
		}

		err = engine.AddRule(rule4)
		if err == nil {
			t.Fatal("Expected weight conflict error for exactly same weights")
		}
	})
}

func TestWeightConflictConfiguration(t *testing.T) {
	// Test with MatcherEngine API (which supports configuration)
	persistence := NewJSONPersistence("./test_data_weight_config")
	engine, err := NewMatcherEngine(persistence, nil, "test-weight-config")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test 1: Default behavior - weight conflicts should be detected
	t.Run("DefaultDisallowDuplicates", func(t *testing.T) {
		rule1 := NewRule("config_rule1").
			Dimension("product", "ProductA", MatchTypeEqual, 10.0).
			Build()

		rule2 := NewRule("config_rule2").
			Dimension("product", "ProductB", MatchTypeEqual, 10.0). // Same weight
			Build()

		err := engine.AddRule(rule1)
		if err != nil {
			t.Fatalf("Failed to add first rule: %v", err)
		}

		err = engine.AddRule(rule2)
		if err == nil {
			t.Fatal("Expected weight conflict error in default mode")
		}
	})

	// Test 2: Allow duplicate weights
	t.Run("AllowDuplicates", func(t *testing.T) {
		// Enable duplicate weights
		engine.SetAllowDuplicateWeights(true)

		rule3 := NewRule("config_rule3").
			Dimension("product", "ProductC", MatchTypeEqual, 15.0).
			Build()

		rule4 := NewRule("config_rule4").
			Dimension("product", "ProductD", MatchTypeEqual, 15.0). // Same weight
			Build()

		err := engine.AddRule(rule3)
		if err != nil {
			t.Fatalf("Failed to add rule3: %v", err)
		}

		err = engine.AddRule(rule4)
		if err != nil {
			t.Fatalf("Failed to add rule4 with duplicate weight when duplicates are allowed: %v", err)
		}
	})

	// Test 3: Disable duplicate weights again
	t.Run("DisallowDuplicatesAgain", func(t *testing.T) {
		// Disable duplicate weights
		engine.SetAllowDuplicateWeights(false)

		rule5 := NewRule("config_rule5").
			Dimension("product", "ProductE", MatchTypeEqual, 20.0).
			Build()

		rule6 := NewRule("config_rule6").
			Dimension("product", "ProductF", MatchTypeEqual, 20.0). // Same weight
			Build()

		err := engine.AddRule(rule5)
		if err != nil {
			t.Fatalf("Failed to add rule5: %v", err)
		}

		err = engine.AddRule(rule6)
		if err == nil {
			t.Fatal("Expected weight conflict error after disabling duplicates")
		}
	})
}
