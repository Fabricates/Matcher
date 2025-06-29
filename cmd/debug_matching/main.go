package main

import (
	"fmt"
	"log"

	"github.com/worthies/matcher"
)

func main() {
	fmt.Println("=== Debug Matching Test ===")

	// Create engine
	engine, err := matcher.NewInMemoryMatcher(
		matcher.NewJSONPersistence("./debug_data"),
		nil,
		"debug-test",
	)
	if err != nil {
		log.Fatalf("Failed to create matcher: %v", err)
	}
	defer engine.Close()

	// Configure 3 simple dimensions
	engine.AddDimension(&matcher.DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	})
	engine.AddDimension(&matcher.DimensionConfig{
		Name:     "environment",
		Index:    1,
		Required: true,
		Weight:   5.0,
	})
	engine.AddDimension(&matcher.DimensionConfig{
		Name:     "region",
		Index:    2,
		Required: false,
		Weight:   3.0,
	})

	// Add a simple test rule
	rule := matcher.NewRule("test_rule").
		Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "production", matcher.MatchTypeEqual, 5.0).
		Dimension("region", "us-west", matcher.MatchTypeEqual, 3.0).
		Build()

	fmt.Printf("Adding rule: %+v\n", rule)
	if err := engine.AddRule(rule); err != nil {
		log.Fatalf("Failed to add rule: %v", err)
	}

	// Add a rule with MatchTypeAny for partial matching
	rule2 := matcher.NewRule("any_rule").
		Dimension("product", "ProductB", matcher.MatchTypeEqual, 10.0).
		Dimension("environment", "staging", matcher.MatchTypeEqual, 5.0).
		Dimension("region", "", matcher.MatchTypeAny, 3.0).
		Build()

	fmt.Printf("Adding any rule: %+v\n", rule2)
	if err := engine.AddRule(rule2); err != nil {
		log.Fatalf("Failed to add any rule: %v", err)
	}

	// Test exact match
	query1 := matcher.CreateQuery(map[string]string{
		"product":     "ProductA",
		"environment": "production",
		"region":      "us-west",
	})

	fmt.Printf("\nTesting exact match query: %+v\n", query1)
	result1, err := engine.FindBestMatch(query1)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else if result1 != nil {
		fmt.Printf("✅ Found match: %s (weight: %.1f)\n", result1.Rule.ID, result1.TotalWeight)
	} else {
		fmt.Println("❌ No match found")
	}

	// Test partial match
	query2 := matcher.CreateQuery(map[string]string{
		"product":     "ProductB",
		"environment": "staging",
		// region not specified - should match MatchTypeAny
	})

	fmt.Printf("\nTesting partial match query: %+v\n", query2)
	result2, err := engine.FindBestMatch(query2)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else if result2 != nil {
		fmt.Printf("✅ Found partial match: %s (weight: %.1f)\n", result2.Rule.ID, result2.TotalWeight)
	} else {
		fmt.Println("❌ No partial match found")
	}

	// Test with all matches
	fmt.Printf("\nTesting all matches for exact query:\n")
	allMatches, err := engine.FindAllMatches(query1)
	if err != nil {
		log.Printf("FindAllMatches failed: %v", err)
	} else {
		fmt.Printf("Found %d total matches:\n", len(allMatches))
		for i, match := range allMatches {
			fmt.Printf("  %d: %s (weight: %.1f)\n", i+1, match.Rule.ID, match.TotalWeight)
		}
	}

	// Get statistics
	stats := engine.GetStats()
	fmt.Printf("\nEngine Stats:\n")
	fmt.Printf("  Total Rules: %d\n", stats.TotalRules)
	fmt.Printf("  Total Dimensions: %d\n", stats.TotalDimensions)
	fmt.Printf("  Total Queries: %d\n", stats.TotalQueries)
}
