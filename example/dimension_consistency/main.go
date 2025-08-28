package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Dimension Consistency Example ===")

	// Create a matcher with persistence
	engine, err := matcher.NewMatcherEngineWithDefaults("./example_data")
	if err != nil {
		slog.Error("Failed to create matcher engine", "error", err); os.Exit(1)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			slog.Error("Error closing engine: %v", err); os.Exit(1)
		}
	}()

	fmt.Println("\n1. Adding rules without configured dimensions (flexible mode):")

	// Without configured dimensions, any rule structure is allowed
	rule1 := matcher.NewRule("flexible_rule_1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "production", matcher.MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		slog.Error("Failed to add flexible rule", "error", err); os.Exit(1)
	} else {
		fmt.Println("✅ Added rule with 2 dimensions (product, environment)")
	}

	rule2 := matcher.NewRule("flexible_rule_2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual, 10.0).
		Dimension("region", "us-west", matcher.MatchTypeEqual, 8.0).
		Dimension("tier", "premium", matcher.MatchTypeEqual, 3.0).
		Build()

	err = engine.AddRule(rule2)
	if err != nil {
		slog.Error("Failed to add flexible rule", "error", err); os.Exit(1)
	} else {
		fmt.Println("✅ Added rule with 3 dimensions (product, region, tier)")
	}

	fmt.Println("\n2. Configuring dimensions to enforce consistency:")

	// Now configure dimensions to enforce consistency
	err = engine.AddDimension(&matcher.DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	})
	if err != nil {
		slog.Error("Failed to add product dimension", "error", err); os.Exit(1)
	}

	err = engine.AddDimension(&matcher.DimensionConfig{
		Name:     "environment",
		Index:    1,
		Required: true,
		Weight:   8.0,
	})
	if err != nil {
		slog.Error("Failed to add environment dimension", "error", err); os.Exit(1)
	}

	err = engine.AddDimension(&matcher.DimensionConfig{
		Name:     "region",
		Index:    2,
		Required: false,
		Weight:   5.0,
	})
	if err != nil {
		slog.Error("Failed to add region dimension", "error", err); os.Exit(1)
	}

	fmt.Println("✅ Configured dimensions: product (required), environment (required), region (optional)")

	fmt.Println("\n3. Testing dimension consistency validation:")

	// This should work - matches configured dimensions exactly
	validRule := matcher.NewRule("valid_rule").
		Dimension("product", "ProductC", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "staging", matcher.MatchTypeEqual, 8.0).
		Dimension("region", "eu-west", matcher.MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(validRule)
	if err != nil {
		fmt.Printf("❌ Failed to add valid rule: %v\n", err)
	} else {
		fmt.Println("✅ Added rule with all configured dimensions")
	}

	// This should work - only required dimensions
	minimalRule := matcher.NewRule("minimal_rule").
		Dimension("product", "ProductD", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "development", matcher.MatchTypeEqual, 8.0).
		Build()

	err = engine.AddRule(minimalRule)
	if err != nil {
		fmt.Printf("❌ Failed to add minimal rule: %v\n", err)
	} else {
		fmt.Println("✅ Added rule with only required dimensions")
	}

	// This should fail - missing required dimension
	fmt.Println("\n4. Testing validation failures:")

	missingRequired := matcher.NewRule("missing_required").
		Dimension("environment", "production", matcher.MatchTypeEqual, 8.0).
		Dimension("region", "us-east", matcher.MatchTypeEqual, 5.0).
		Build()

	err = engine.AddRule(missingRequired)
	if err != nil {
		fmt.Printf("✅ Correctly rejected rule missing required dimension: %v\n", err)
	} else {
		fmt.Println("❌ Should have rejected rule missing required dimension")
	}

	// This should fail - extra dimension not in configuration
	extraDimension := matcher.NewRule("extra_dimension").
		Dimension("product", "ProductE", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "production", matcher.MatchTypeEqual, 8.0).
		Dimension("tier", "premium", matcher.MatchTypeEqual, 3.0). // Not in config
		Build()

	err = engine.AddRule(extraDimension)
	if err != nil {
		fmt.Printf("✅ Correctly rejected rule with extra dimension: %v\n", err)
	} else {
		fmt.Println("❌ Should have rejected rule with extra dimension")
	}

	fmt.Println("\n5. Testing queries with consistent structure:")

	// Test query matching
	query := matcher.CreateQuery(map[string]string{
		"product":     "ProductC",
		"environment": "staging",
		"region":      "eu-west",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else if result != nil {
		fmt.Printf("✅ Found matching rule: %s (weight: %.1f)\n", result.Rule.ID, result.TotalWeight)
	} else {
		fmt.Println("No matches found")
	}

	// Show statistics
	stats := engine.GetStats()
	fmt.Printf("\n📊 Engine Statistics:\n")
	fmt.Printf("   Total Rules: %d\n", stats.TotalRules)
	fmt.Printf("   Total Dimensions: %d\n", stats.TotalDimensions)
	fmt.Printf("   Total Queries: %d\n", stats.TotalQueries)

	fmt.Println("\n✅ Dimension consistency validation demo completed!")
}
