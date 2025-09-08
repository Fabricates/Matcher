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
