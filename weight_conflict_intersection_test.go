package matcher

import (
	"testing"
)

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
