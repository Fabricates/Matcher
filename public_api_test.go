package matcher

import (
	"testing"
)

// TestPublicUpdateAndGetRule tests the public UpdateRule and GetRule methods
func TestPublicUpdateAndGetRule(t *testing.T) {
	// Create matcher with mock persistence
	persistence := NewJSONPersistence("./test_data")
	matcher, err := NewInMemoryMatcher(persistence, nil, "test-node-1")
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer matcher.Close()

	// Add test dimensions
	err = addTestDimensions(matcher)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Create and add initial rule
	rule := NewRule("test-public-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRoute", MatchTypeEqual).
		Metadata("action", "allow").
		Metadata("priority", "high").
		Build()

	err = matcher.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Test GetRule
	retrieved, err := matcher.GetRule("test-public-rule")
	if err != nil {
		t.Fatalf("Failed to get rule: %v", err)
	}

	if retrieved.ID != "test-public-rule" {
		t.Errorf("Expected rule ID 'test-public-rule', got %s", retrieved.ID)
	}

	if retrieved.Metadata["action"] != "allow" {
		t.Errorf("Expected action 'allow', got %v", retrieved.Metadata["action"])
	}

	// Test UpdateRule with public method
	updatedRule := NewRule("test-public-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "UpdatedRoute", MatchTypeEqual). // Changed route
		Metadata("action", "block").                        // Changed action
		Metadata("priority", "medium").                     // Changed priority
		Build()

	err = matcher.UpdateRule(updatedRule) // Using public UpdateRule method
	if err != nil {
		t.Fatalf("Failed to update rule: %v", err)
	}

	// Verify update using public GetRule method
	retrievedUpdated, err := matcher.GetRule("test-public-rule") // Using public GetRule method
	if err != nil {
		t.Fatalf("Failed to get updated rule: %v", err)
	}

	// Check that dimensions were updated
	routeDim := retrievedUpdated.GetDimensionValue("route")
	if routeDim == nil {
		t.Fatalf("Route dimension not found")
	}

	if routeDim.Value != "UpdatedRoute" {
		t.Errorf("Expected route 'UpdatedRoute', got %s", routeDim.Value)
	}

	// Check that metadata was updated
	if retrievedUpdated.Metadata["action"] != "block" {
		t.Errorf("Expected action 'block', got %v", retrievedUpdated.Metadata["action"])
	}

	if retrievedUpdated.Metadata["priority"] != "medium" {
		t.Errorf("Expected priority 'medium', got %v", retrievedUpdated.Metadata["priority"])
	}

	// Test GetRule with non-existent rule
	_, err = matcher.GetRule("non-existent-rule")
	if err == nil {
		t.Error("Expected error when getting non-existent rule")
	}
}

// TestPublicAPIImmutability tests that GetRule returns immutable copies
func TestPublicAPIImmutability(t *testing.T) {
	// Create matcher with mock persistence
	persistence := NewJSONPersistence("./test_data")
	matcher, err := NewInMemoryMatcher(persistence, nil, "test-node-1")
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer matcher.Close()

	// Add test dimensions
	err = addTestDimensions(matcher)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Create and add rule
	rule := NewRule("immutable-test").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Metadata("action", "allow").
		Build()

	err = matcher.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Get rule and modify the returned copy
	retrieved, err := matcher.GetRule("immutable-test")
	if err != nil {
		t.Fatalf("Failed to get rule: %v", err)
	}

	// Modify the returned copy
	retrieved.Metadata["action"] = "modified"
	if len(retrieved.Dimensions) > 0 {
		retrieved.Dimensions[0].Value = "modified"
	}

	// Get the rule again and verify it wasn't affected
	retrievedAgain, err := matcher.GetRule("immutable-test")
	if err != nil {
		t.Fatalf("Failed to get rule again: %v", err)
	}

	if retrievedAgain.Metadata["action"] != "allow" {
		t.Error("Rule was modified when it should be immutable")
	}

	if len(retrievedAgain.Dimensions) > 0 && retrievedAgain.Dimensions[0].Value != "TestProduct" {
		t.Error("Dimension was modified when it should be immutable")
	}
}
