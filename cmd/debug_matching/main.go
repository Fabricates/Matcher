package main

import (
	"fmt"
	"log/slog"
	"os"

	matcher "github.com/Fabricates/Matcher"
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
		slog.Error("Failed to create matcher", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	// Configure 3 simple dimensions
	engine.AddDimension(matcher.NewDimensionConfig("product", 0, true))
	engine.AddDimension(matcher.NewDimensionConfig("environment", 1, true))
	engine.AddDimension(matcher.NewDimensionConfig("region", 2, false))

	// Add a simple test rule
	rule := matcher.NewRule("test_rule").
		Dimension("product", "ProductA", matcher.MatchTypeEqual).
		Dimension("environment", "production", matcher.MatchTypeEqual).
		Dimension("region", "us-west", matcher.MatchTypeEqual).
		Build()

	fmt.Printf("Adding rule: %+v\n", rule)
	if err := engine.AddRule(rule); err != nil {
		slog.Error("Failed to add rule", "error", err)
		os.Exit(1)
	}

	// Add a rule with MatchTypeAny for partial matching
	rule2 := matcher.NewRule("any_rule").
		Dimension("product", "ProductB", matcher.MatchTypeEqual).
		Dimension("environment", "staging", matcher.MatchTypeEqual).
		Dimension("region", "", matcher.MatchTypeAny).
		Build()

	fmt.Printf("Adding any rule: %+v\n", rule2)
	if err := engine.AddRule(rule2); err != nil {
		slog.Error("Failed to add any rule", "error", err)
		os.Exit(1)
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
		slog.Error("Query failed", "error", err)
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
		slog.Error("Query failed", "error", err)
	} else if result2 != nil {
		fmt.Printf("✅ Found partial match: %s (weight: %.1f)\n", result2.Rule.ID, result2.TotalWeight)
	} else {
		fmt.Println("❌ No partial match found")
	}

	// Test with all matches
	fmt.Printf("\nTesting all matches for exact query:\n")
	allMatches, err := engine.FindAllMatches(query1)
	if err != nil {
		slog.Error("FindAllMatches failed", "error", err)
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
