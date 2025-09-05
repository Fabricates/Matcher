package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Forest Index Structure Demo ===")

	// Create a matcher with the forest index
	persistence := matcher.NewJSONPersistence("./demo_data")

	engine, err := matcher.NewMatcherEngine(persistence, nil, "demo-node")
	if err != nil {
		slog.Error("Failed to create matcher engine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	// Initialize default dimensions
	// Add required dimensions
	dimensions := []*matcher.DimensionConfig{
		matcher.NewDimensionConfig("product", 0, true),
		matcher.NewDimensionConfig("route", 1, false),
		matcher.NewDimensionConfig("tool", 2, false),
	}

	for _, dim := range dimensions {
		if err := engine.AddDimension(dim); err != nil {
			slog.Error("Failed to add dimension", "dimension", dim.Name, "error", err)
		}
	}

	// Create test rules that will be organized into different trees
	rules := []*matcher.Rule{
		// Tree 1: product="WebApp", MatchType=Equal
		matcher.NewRule("rule1").
			Dimension("product", "WebApp", matcher.MatchTypeEqual).
			Dimension("route", "API", matcher.MatchTypeEqual).
			Build(),
		matcher.NewRule("rule2").
			Dimension("product", "WebApp", matcher.MatchTypeEqual).
			Dimension("tool", "Database", matcher.MatchTypePrefix).
			Build(),
		matcher.NewRule("rule3").
			Dimension("product", "WebApp", matcher.MatchTypeEqual).
			Dimension("route", "Frontend", matcher.MatchTypeEqual).
			Build(),
		// Tree 2: product="MobileApp", MatchType=Equal
		matcher.NewRule("rule4").
			Dimension("product", "MobileApp", matcher.MatchTypeEqual).
			Dimension("route", "Native", matcher.MatchTypeEqual).
			Build(),
		// Tree 3: route="Service", MatchType=Equal
		matcher.NewRule("rule5").
			Dimension("route", "Service", matcher.MatchTypeEqual).
			Dimension("product", "Backend", matcher.MatchTypeEqual).
			Build(),
		// Tree 4: tool="micro", MatchType=Prefix
		matcher.NewRule("rule6").
			Dimension("tool", "micro", matcher.MatchTypePrefix).
			Dimension("product", "Platform", matcher.MatchTypeSuffix).
			Build(),
	}

	// Add all rules
	fmt.Println("\n--- Adding Rules ---")
	for _, rule := range rules {
		if err := engine.AddRule(rule); err != nil {
			slog.Error("Error adding rule", "rule_id", rule.ID, "error", err)
			continue
		}
		fmt.Printf("Added rule %s\n", rule.ID)
	}

	// Show forest structure statistics
	fmt.Println("\n--- Forest Structure Statistics ---")
	stats := engine.GetStats()
	fmt.Printf("Total Rules: %d\n", stats.TotalRules)
	fmt.Printf("Total Dimensions: %d\n", stats.TotalDimensions)

	// Test queries and show which trees are searched
	queries := []*matcher.QueryRule{
		matcher.CreateQuery(map[string]string{
			"product": "WebApp",
			"route":   "API",
		}),
		matcher.CreateQuery(map[string]string{
			"product": "MobileApp",
			"route":   "Native",
		}),
		matcher.CreateQuery(map[string]string{
			"route":   "Service",
			"product": "Backend",
		}),
		matcher.CreateQuery(map[string]string{
			"tool":    "microservice",
			"product": "Platform-Service",
		}),
		matcher.CreateQuery(map[string]string{
			"tool":    "Database-Server",
			"product": "WebApp",
		}),
	}

	fmt.Println("\n--- Query Performance Demo ---")
	for i, query := range queries {
		start := time.Now()
		result, err := engine.FindBestMatch(query)
		elapsed := time.Since(start)

		fmt.Printf("\nQuery %d: %v\n", i+1, query.Values)
		fmt.Printf("Time: %v\n", elapsed)

		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		if result != nil {
			fmt.Printf("Best Match: Rule %s (Weight: %.2f, Matched: %d dims)\n",
				result.Rule.ID, result.TotalWeight, result.MatchedDims)
		} else {
			fmt.Printf("No match found\n")
		}

		// Also show all matches
		allMatches, err := engine.FindAllMatches(query)
		if err == nil && len(allMatches) > 0 {
			fmt.Printf("All Matches: ")
			for j, match := range allMatches {
				if j > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%s(%.1f)", match.Rule.ID, match.TotalWeight)
			}
			fmt.Println()
		}
	}

	// Performance test
	fmt.Println("\n--- Performance Test ---")
	testQuery := matcher.CreateQuery(map[string]string{
		"product": "WebApp",
		"route":   "API",
		"tool":    "Database-Engine",
	})

	numQueries := 10000
	start := time.Now()
	for i := 0; i < numQueries; i++ {
		_, err := engine.FindBestMatch(testQuery)
		if err != nil {
			slog.Error("Query failed", "query_index", i, "error", err)
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("Executed %d queries in %v\n", numQueries, elapsed)
	fmt.Printf("Average query time: %v\n", elapsed/time.Duration(numQueries))
	fmt.Printf("Queries per second: %.2f\n", float64(numQueries)/elapsed.Seconds())

	fmt.Println("\n=== Demo Complete ===")
}
