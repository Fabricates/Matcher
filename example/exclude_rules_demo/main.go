package main

import (
	"fmt"
	"log"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	// Create engine with JSON persistence
	engine, err := matcher.NewMatcherEngineWithDefaults("./test_data")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Add dimensions
	dimensions := []*matcher.DimensionConfig{
		matcher.NewDimensionConfig("product", 0, true),
		matcher.NewDimensionConfig("route", 1, false),
	}

	for _, dim := range dimensions {
		err = engine.AddDimension(dim)
		if err != nil {
			log.Fatalf("Failed to add dimension %s: %v", dim.Name, err)
		}
	}

	// Add multiple rules with different weights
	rules := []struct {
		id     string
		weight float64
	}{
		{"high_priority_rule", 100.0},
		{"medium_priority_rule", 50.0},
		{"low_priority_rule", 10.0},
	}

	for _, rule := range rules {
		r := matcher.NewRule(rule.id).
			Dimension("product", "ProductA", matcher.MatchTypeEqual).
			Dimension("route", "main", matcher.MatchTypeEqual).
			ManualWeight(rule.weight).
			Build()

		err = engine.AddRule(r)
		if err != nil {
			log.Fatalf("Failed to add rule %s: %v", rule.id, err)
		}
	}

	// Regular query - finds highest priority rule
	fmt.Println("=== Regular Query ===")
	query := matcher.CreateQuery(map[string]string{
		"product": "ProductA",
		"route":   "main",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		log.Fatal(err)
	}
	if result != nil {
		fmt.Printf("Best match: %s (weight: %.1f)\n", result.Rule.ID, result.TotalWeight)
	}

	// Query excluding highest priority rule - finds second best
	fmt.Println("\n=== Query Excluding High Priority Rule ===")
	excludeQuery := matcher.CreateQueryWithExcludedRules(map[string]string{
		"product": "ProductA",
		"route":   "main",
	}, []string{"high_priority_rule"})

	result, err = engine.FindBestMatch(excludeQuery)
	if err != nil {
		log.Fatal(err)
	}
	if result != nil {
		fmt.Printf("Best match: %s (weight: %.1f)\n", result.Rule.ID, result.TotalWeight)
	}

	// Query excluding multiple rules - finds last option
	fmt.Println("\n=== Query Excluding Multiple Rules ===")
	multiExcludeQuery := matcher.CreateQueryWithExcludedRules(map[string]string{
		"product": "ProductA",
		"route":   "main",
	}, []string{"high_priority_rule", "medium_priority_rule"})

	result, err = engine.FindBestMatch(multiExcludeQuery)
	if err != nil {
		log.Fatal(err)
	}
	if result != nil {
		fmt.Printf("Best match: %s (weight: %.1f)\n", result.Rule.ID, result.TotalWeight)
	}

	// Query excluding all rules - finds nothing
	fmt.Println("\n=== Query Excluding All Rules ===")
	allExcludeQuery := matcher.CreateQueryWithExcludedRules(map[string]string{
		"product": "ProductA",
		"route":   "main",
	}, []string{"high_priority_rule", "medium_priority_rule", "low_priority_rule"})

	result, err = engine.FindBestMatch(allExcludeQuery)
	if err != nil {
		log.Fatal(err)
	}
	if result != nil {
		fmt.Printf("Best match: %s (weight: %.1f)\n", result.Rule.ID, result.TotalWeight)
	} else {
		fmt.Println("No match found (all rules excluded)")
	}

	// Show all matches with and without exclusions
	fmt.Println("\n=== All Matches Without Exclusions ===")
	allMatches, err := engine.FindAllMatches(query)
	if err != nil {
		log.Fatal(err)
	}
	for i, match := range allMatches {
		fmt.Printf("%d. %s (weight: %.1f)\n", i+1, match.Rule.ID, match.TotalWeight)
	}

	fmt.Println("\n=== All Matches With Exclusions ===")
	allMatchesExcluded, err := engine.FindAllMatches(excludeQuery)
	if err != nil {
		log.Fatal(err)
	}
	for i, match := range allMatchesExcluded {
		fmt.Printf("%d. %s (weight: %.1f)\n", i+1, match.Rule.ID, match.TotalWeight)
	}
}
