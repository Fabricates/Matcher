package main

import (
	"fmt"
	"log"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Demonstration: Automatic Weight Population from Dimension Configuration ===")
	fmt.Println()

	// Create a matcher engine
	engine, err := matcher.NewMatcherEngineWithDefaults("./demo_data")
	if err != nil {
		log.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Step 1: Configure dimensions with specific weights
	fmt.Println("1. Configuring dimensions with predefined weights...")

	productDim := matcher.NewDimensionConfig("product", 0, false, 15.0)

	environmentDim := matcher.NewDimensionConfig("environment", 1, false, 8.0)

	regionDim := matcher.NewDimensionConfig("region", 2, false, 5.0)

	if err := engine.AddDimension(productDim); err != nil {
		log.Fatalf("Failed to add product dimension: %v", err)
	}
	fmt.Printf("   Added dimension 'product' with weight %.1f\n", productDim.DefaultWeight)

	if err := engine.AddDimension(environmentDim); err != nil {
		log.Fatalf("Failed to add environment dimension: %v", err)
	}
	fmt.Printf("   Added dimension 'environment' with weight %.1f\n", environmentDim.DefaultWeight)

	if err := engine.AddDimension(regionDim); err != nil {
		log.Fatalf("Failed to add region dimension: %v", err)
	}
	fmt.Printf("   Added dimension 'region' with weight %.1f\n", regionDim.DefaultWeight)

	fmt.Println()

	// Step 2: Create rules using the NEW API (without specifying weights)
	fmt.Println("2. Creating rules using the NEW API (no weight parameters needed)...")

	rule1 := matcher.NewRule("auto-weight-rule-1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual). // Weight will be 15.0 from config
		Dimension("environment", "prod", matcher.MatchTypeEqual). // Weight will be 8.0 from config
		Dimension("region", "us-west", matcher.MatchTypeEqual).   // Weight will be 5.0 from config
		Build()

	rule2 := matcher.NewRule("auto-weight-rule-2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual).    // Weight will be 15.0 from config
		Dimension("environment", "staging", matcher.MatchTypeEqual). // Weight will be 8.0 from config
		Build()

	rule3 := matcher.NewRule("mixed-rule").
		Dimension("product", "ProductC", matcher.MatchTypeEqual). // Auto weight from config
		Dimension("environment", "dev", matcher.MatchTypeEqual).
		ManualWeight(12.0). // Explicit weight override
		Build()

	rule4 := matcher.NewRule("flexible-rule").
		Dimension("product", "ProductD", matcher.MatchTypeEqual). // Auto weight from config
		Build()

	// Add rules to the engine
	if err := engine.AddRule(rule1); err != nil {
		log.Fatalf("Failed to add rule1: %v", err)
	}
	if err := engine.AddRule(rule2); err != nil {
		log.Fatalf("Failed to add rule2: %v", err)
	}
	if err := engine.AddRule(rule3); err != nil {
		log.Fatalf("Failed to add rule3: %v", err)
	}
	if err := engine.AddRule(rule4); err != nil {
		log.Fatalf("Failed to add rule4: %v", err)
	}

	fmt.Println("   ✓ Created rules without specifying weights - they are auto-populated!")
	fmt.Println()

	// Step 3: Verify the weights were populated correctly
	fmt.Println("3. Verifying automatic weight population...")

	// Create dimension configs map for weight calculation
	dimensionConfigs := map[string]*matcher.DimensionConfig{
		"product":     productDim,
		"environment": environmentDim,
		"region":      regionDim,
	}

	// Check rule1
	fmt.Printf("Rule 1 ('%s'):\n", rule1.ID)
	fmt.Printf("   product dimension weight: %.1f (from config)\n", productDim.DefaultWeight)
	fmt.Printf("   environment dimension weight: %.1f (from config)\n", environmentDim.DefaultWeight)
	fmt.Printf("   region dimension weight: %.1f (from config)\n", regionDim.DefaultWeight)
	fmt.Printf("   Total calculated weight: %.1f\n", rule1.CalculateTotalWeight(dimensionConfigs))
	fmt.Println()

	// Check rule2
	fmt.Printf("Rule 2 ('%s'):\n", rule2.ID)
	fmt.Printf("   product dimension weight: %.1f (from config)\n", productDim.DefaultWeight)
	fmt.Printf("   environment dimension weight: %.1f (from config)\n", environmentDim.DefaultWeight)
	fmt.Printf("   Total calculated weight: %.1f\n", rule2.CalculateTotalWeight(dimensionConfigs))
	fmt.Println()

	// Check rule3 (mixed)
	fmt.Printf("Rule 3 ('%s') - Mixed approach:\n", rule3.ID)
	fmt.Printf("   product dimension weight: %.1f (from config)\n", productDim.DefaultWeight)
	fmt.Printf("   environment dimension weight: %.1f (explicit override via ManualWeight)\n", *rule3.ManualWeight)
	fmt.Printf("   Total calculated weight: %.1f\n", rule3.CalculateTotalWeight(dimensionConfigs))
	fmt.Println()

	// Check rule4 (single dimension)
	fmt.Printf("Rule 4 ('%s') - Single dimension:\n", rule4.ID)
	fmt.Printf("   product dimension weight: %.1f (from config)\n", productDim.DefaultWeight)
	fmt.Printf("   Total calculated weight: %.1f\n", rule4.CalculateTotalWeight(dimensionConfigs))
	fmt.Println()

	// Step 4: Test matching behavior
	fmt.Println("4. Testing matching with auto-populated weights...")

	query := matcher.CreateQuery(map[string]string{
		"product":     "ProductA",
		"environment": "prod",
		"region":      "us-west",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	if result != nil {
		fmt.Printf("   Best match: Rule '%s' with total weight %.1f\n", result.Rule.ID, result.TotalWeight)
		fmt.Printf("   Matched %d dimensions\n", result.MatchedDims)
	} else {
		fmt.Println("   No matches found")
	}

	fmt.Println()
	fmt.Println("=== Summary ===")
	fmt.Println("✓ Dimension configurations define default weights")
	fmt.Println("✓ New Dimension() API automatically uses configured weights")
	fmt.Println("✓ DimensionWithWeight() API allows explicit weight overrides")
	fmt.Println("✓ Unconfigured dimensions default to weight 1.0")
	fmt.Println("✓ Backward compatibility maintained")
	fmt.Println()

	// Step 5: Demonstrate default weights with no configuration
	fmt.Println("5. Demonstrating default weights (no dimension configuration)...")

	// Create a new engine without dimension configurations
	engine2, err := matcher.NewMatcherEngineWithDefaults("./demo_data2")
	if err != nil {
		log.Fatalf("Failed to create engine2: %v", err)
	}
	defer engine2.Close()

	// Create a rule without any dimension configurations
	flexibleRule := matcher.NewRule("flexible-no-config").
		Dimension("any_dimension", "any_value", matcher.MatchTypeEqual).
		Dimension("another_dimension", "another_value", matcher.MatchTypeEqual).
		Build()

	if err := engine2.AddRule(flexibleRule); err != nil {
		log.Fatalf("Failed to add flexible rule: %v", err)
	}

	// For rules without dimension configs, they use default weight of 1.0
	fmt.Printf("   any_dimension weight: %.1f (default)\n", 1.0)
	fmt.Printf("   another_dimension weight: %.1f (default)\n", 1.0)
	fmt.Printf("   Total weight: %.1f\n", flexibleRule.CalculateTotalWeight(make(map[string]*matcher.DimensionConfig)))

	fmt.Println()
	fmt.Println("The weight parameter in RuleBuilder.Dimension() method is no longer necessary!")
}
