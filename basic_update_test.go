package matcher

import (
	"testing"
)

// TestBasicUpdateRule tests basic update functionality
func TestBasicUpdateRule(t *testing.T) {
	// Create engine with mock persistence
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewMatcherEngine(persistence, nil, "test-node-1")
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer engine.Close()

	// Add test dimensions
	err = addTestDimensions(engine.matcher)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Create initial rule using builder pattern
	rule := NewRule("test-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRoute", MatchTypeEqual).
		Metadata("action", "allow").
		Build()

	// Add rule
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Update rule
	updatedRule := NewRule("test-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRouteUpdated", MatchTypeEqual). // Changed route
		Metadata("action", "block").                            // Changed action
		Build()

	err = engine.UpdateRule(updatedRule)
	if err != nil {
		t.Fatalf("Failed to update rule: %v", err)
	}

	// Verify update
	retrieved, err := engine.GetRule("test-rule")
	if err != nil {
		t.Fatalf("Failed to get rule: %v", err)
	}

	// Check dimensions were updated
	routeDim := retrieved.GetDimensionValue("route")
	if routeDim == nil {
		t.Fatalf("Route dimension not found")
	}

	if routeDim.Value != "TestRouteUpdated" {
		t.Errorf("Expected route TestRouteUpdated, got %s", routeDim.Value)
	}

	if retrieved.Metadata["action"] != "block" {
		t.Errorf("Expected action block, got %v", retrieved.Metadata["action"])
	}
}
