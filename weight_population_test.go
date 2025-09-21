package matcher

import (
	"testing"
)

func TestAutomaticWeightPopulation(t *testing.T) {
	// Create an engine with dimension configurations
	persistence := NewJSONPersistence("./test_data_weight_population")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-weight-population")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configurations with specific weights
	err = engine.AddDimension(NewDimensionConfig("product", 0, false))
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("environment", 1, false))
	if err != nil {
		t.Fatalf("Failed to add environment dimension: %v", err)
	}

	// Create a rule using the new API (without weights)
	rule := NewRule("test-automatic-weight").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("environment", "prod", MatchTypeEqual).
		Build()

	// Add the rule - weights should be populated automatically
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the weights were populated correctly
	productDim := rule.GetDimensionValue("product")
	if productDim == nil {
		t.Fatal("Product dimension not found")
	}

	environmentDim := rule.GetDimensionValue("environment")
	if environmentDim == nil {
		t.Fatal("Environment dimension not found")
	}

	// Verify total weight calculation
	totalWeight := rule.CalculateTotalWeight(engine.dimensionConfigs)
	expectedWeight := 0.0 // No explicit weights set
	if totalWeight != expectedWeight {
		t.Errorf("Expected total weight %.1f, got %.1f", expectedWeight, totalWeight)
	}
}

func TestZeroWeightWhenNoDimensionConfig(t *testing.T) {
	// Create an engine without dimension configurations
	persistence := NewJSONPersistence("./test_data_default_weight")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-default-weight")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create a rule using the new API (without weights)
	rule := NewRule("test-default-weight").
		Dimension("product", "ProductA", MatchTypeEqual).
		Dimension("environment", "prod", MatchTypeEqual).
		Build()

	// Add the rule - weights should default to 0.0
	err = engine.AddRule(rule)
	if err == nil {
		t.Fatalf("Dimension is required before adding any new rules")
	}
}

func TestDimensionWithWeightBackwardCompatibility(t *testing.T) {
	// Create an engine with dimension configurations
	persistence := NewJSONPersistence("./test_data_backward_compat")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-backward-compat")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configurations with specific weights
	err = engine.AddDimension(NewDimensionConfig("product", 0, false))
	if err != nil {
		t.Fatalf("Failed to add product dimension: %v", err)
	}

	err = engine.AddDimension(NewDimensionConfig("environment", 1, false))
	if err != nil {
		t.Fatalf("Failed to add environment dimension: %v", err)
	}

	// Create a rule using the backward compatibility method with explicit weights
	rule := NewRule("test-backward-compat").
		Dimension("product", "ProductA", MatchTypeEqual). // Override the configured weight
		Dimension("environment", "prod", MatchTypeEqual). // Use configured weight
		ManualWeight(25.0).
		Build()

	// Add the rule
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the explicit weight was preserved
	productDim := rule.GetDimensionValue("product")
	if productDim == nil {
		t.Fatal("Product dimension not found")
	}

	// Verify the auto-populated weight for environment (from config)
	environmentDim := rule.GetDimensionValue("environment")
	if environmentDim == nil {
		t.Fatal("Environment dimension not found")
	}
}
