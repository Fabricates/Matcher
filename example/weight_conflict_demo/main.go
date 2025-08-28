package main

import (
	"fmt"
	"log/slog"
	"os"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Weight Conflict Detection Demo ===")

	// Create a new matcher engine
	persistence := matcher.NewJSONPersistence("./data")
	engine, err := matcher.NewMatcherEngine(persistence, nil, "weight-demo")
	if err != nil {
		slog.Error("Failed to create engine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	// Add dimension configurations with weights
	dimensionConfigs := []*matcher.DimensionConfig{
		matcher.NewDimensionConfig("product", 0, true, 10.0),
		matcher.NewDimensionConfig("environment", 1, false, 5.0),
	}

	for _, config := range dimensionConfigs {
		if err := engine.AddDimension(config); err != nil {
			slog.Error("Failed to add dimension", "dimension", config.Name, "error", err)
			os.Exit(1)
		}
	}

	// Create a map of dimension configs for weight calculation
	configMap := make(map[string]*matcher.DimensionConfig)
	for _, config := range dimensionConfigs {
		configMap[config.Name] = config
	}

	fmt.Println("\n1. Testing default behavior (weight conflicts disabled)")

	// Add first rule
	rule1 := matcher.NewRule("rule1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual).
		Dimension("environment", "production", matcher.MatchTypeEqual).
		Metadata("description", "First rule with weight 15.0").
		Build()

	if err := engine.AddRule(rule1); err != nil {
		slog.Error("Failed to add rule1", "error", err)
		os.Exit(1)
	} else {
		fmt.Printf("✅ Added rule1 with total weight: %.2f\n", rule1.CalculateTotalWeight(configMap))
	}

	// Try to add second rule with same calculated weight
	rule2 := matcher.NewRule("rule2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual).
		Dimension("environment", "staging", matcher.MatchTypeEqual).
		Metadata("description", "Second rule also with weight 15.0").
		Build()

	if err := engine.AddRule(rule2); err != nil {
		fmt.Printf("❌ Failed to add rule2: %v\n", err)
	} else {
		fmt.Printf("✅ Added rule2 with total weight: %.2f\n", rule2.CalculateTotalWeight(configMap))
	}

	fmt.Println("\n2. Enabling duplicate weights")
	engine.SetAllowDuplicateWeights(true)

	// Now try to add the same rule again
	if err := engine.AddRule(rule2); err != nil {
		fmt.Printf("❌ Failed to add rule2 after enabling duplicates: %v\n", err)
	} else {
		fmt.Printf("✅ Successfully added rule2 with duplicate weight: %.2f\n", rule2.CalculateTotalWeight(configMap))
	}

	// Add another rule with the same weight
	rule3 := matcher.NewRule("rule3").
		Dimension("product", "ProductC", matcher.MatchTypeEqual).
		Dimension("environment", "development", matcher.MatchTypeEqual).
		Metadata("description", "Third rule also with weight 15.0").
		Build()

	if err := engine.AddRule(rule3); err != nil {
		fmt.Printf("❌ Failed to add rule3: %v\n", err)
	} else {
		fmt.Printf("✅ Added rule3 with total weight: %.2f\n", rule3.CalculateTotalWeight(configMap))
	}

	fmt.Println("\n3. Testing manual weight conflicts")

	// Add rule with manual weight
	rule4 := matcher.NewRule("rule4").
		Dimension("product", "ProductD", matcher.MatchTypeEqual).
		ManualWeight(25.0).
		Metadata("description", "Rule with manual weight override").
		Build()

	if err := engine.AddRule(rule4); err != nil {
		fmt.Printf("❌ Failed to add rule4: %v\n", err)
	} else {
		fmt.Printf("✅ Added rule4 with manual weight: %.2f\n", rule4.CalculateTotalWeight(configMap))
	}

	// Try to add another rule with same manual weight
	rule5 := matcher.NewRule("rule5").
		Dimension("product", "ProductE", matcher.MatchTypeEqual).
		ManualWeight(25.0). // Same manual weight
		Metadata("description", "Another rule with same manual weight").
		Build()

	if err := engine.AddRule(rule5); err != nil {
		fmt.Printf("❌ Failed to add rule5: %v\n", err)
	} else {
		fmt.Printf("✅ Added rule5 with manual weight: %.2f\n", rule5.CalculateTotalWeight(configMap))
	}

	fmt.Println("\n4. Disabling duplicate weights again")
	engine.SetAllowDuplicateWeights(false)

	// Try to add another rule with conflicting weight
	rule6 := matcher.NewRule("rule6").
		Dimension("product", "ProductF", matcher.MatchTypeEqual).
		Metadata("description", "Rule that conflicts with manual weight").
		Build()

	if err := engine.AddRule(rule6); err != nil {
		fmt.Printf("❌ Failed to add rule6: %v\n", err)
	} else {
		fmt.Printf("✅ Added rule6 with total weight: %.2f\n", rule6.CalculateTotalWeight(configMap))
	}

	fmt.Println("\n5. Testing queries with duplicate weights")

	// Test query that matches multiple rules
	query := matcher.CreateQuery(map[string]string{
		"product":     "ProductB",
		"environment": "staging",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		slog.Error("Query failed", "error", err)
		os.Exit(1)
	} else if result != nil {
		fmt.Printf("🎯 Best match: %s (weight: %.2f, description: %s)\n",
			result.Rule.ID, result.TotalWeight, result.Rule.Metadata["description"])
	} else {
		fmt.Println("❌ No matches found")
	}

	// Find all matches
	allMatches, err := engine.FindAllMatches(query)
	if err != nil {
		slog.Error("FindAllMatches failed", "error", err)
		os.Exit(1)
	} else {
		fmt.Printf("\n📋 All matches (%d found):\n", len(allMatches))
		for i, match := range allMatches {
			fmt.Printf("  %d. %s (weight: %.2f)\n", i+1, match.Rule.ID, match.TotalWeight)
		}
	}

	// Get engine statistics
	stats := engine.GetStats()
	fmt.Printf("\n📊 Engine Statistics:\n")
	fmt.Printf("  Total Rules: %d\n", stats.TotalRules)
	fmt.Printf("  Cache Hit Rate: %.2f%%\n", stats.CacheHitRate*100)

	fmt.Println("\n🎉 Weight conflict detection demo completed!")
}
