package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== High-Performance Rule Matching Engine Demo ===")

	// Create matcher engine with JSON persistence
	engine, err := matcher.NewMatcherEngineWithDefaults("./data")
	if err != nil {
		slog.Error("Failed to create matcher engine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	fmt.Println("\n1. Adding dimensions...")

	// Add core dimensions
	coreDims := []*matcher.DimensionConfig{
		matcher.NewDimensionConfig("product", 0, true),
		matcher.NewDimensionConfig("route", 1, false),
		matcher.NewDimensionConfig("tool", 2, false),
		matcher.NewDimensionConfig("tool_id", 3, false),
		matcher.NewDimensionConfig("recipe", 4, false),
	}

	for _, dim := range coreDims {
		if err := engine.AddDimension(dim); err != nil {
			slog.Error("Failed to add dimension", "dimension", dim.Name, "error", err)
		} else {
			fmt.Printf("  Added dimension: %s\n", dim.Name)
		}
	}

	// Add custom dimensions
	customDims := []*matcher.DimensionConfig{
		matcher.NewDimensionConfig("region", 5, false),
		matcher.NewDimensionConfig("priority", 6, false),
		matcher.NewDimensionConfig("environment", 7, false),
	}

	for _, dim := range customDims {
		if err := engine.AddDimension(dim); err != nil {
			slog.Error("Failed to add dimension", "dimension", dim.Name, "error", err)
		} else {
			fmt.Printf("  Added dimension: %s\n", dim.Name)
		}
	}

	fmt.Println("\n2. Adding rules with different match types...")

	// Rule 1: Exact match rule for production environment
	rule1 := matcher.NewRule("production_rule").
		Dimension("product", "ProductA", matcher.MatchTypeEqual).
		Dimension("route", "main", matcher.MatchTypeEqual).
		Dimension("tool", "laser", matcher.MatchTypeEqual).
		Dimension("tool_id", "LASER_001", matcher.MatchTypeEqual).
		Dimension("recipe", "recipe_alpha", matcher.MatchTypeEqual).
		Dimension("region", "us-west", matcher.MatchTypeEqual).
		Dimension("priority", "high", matcher.MatchTypeEqual).
		Dimension("environment", "production", matcher.MatchTypeEqual).
		Metadata("description", "High-priority production rule").
		Build()

	if err := engine.AddRule(rule1); err != nil {
		slog.Error("Failed to add rule1", "error", err)
	} else {
		fmt.Println("  ✓ Added production rule (exact matches)")
	}

	// Rule 2: Prefix matching rule
	rule2 := matcher.NewRule("prefix_rule").
		Dimension("product", "Prod", matcher.MatchTypePrefix).  // Matches "Prod*"
		Dimension("route", "", matcher.MatchTypeAny).           // Matches any route
		Dimension("tool", "laser", matcher.MatchTypePrefix).    // Matches "laser*"
		Dimension("tool_id", "", matcher.MatchTypeAny).         // Matches any tool ID
		Dimension("recipe", "recipe", matcher.MatchTypePrefix). // Matches "recipe*"
		Dimension("environment", "dev", matcher.MatchTypeEqual).
		Metadata("description", "Development prefix matching rule").
		Build()

	if err := engine.AddRule(rule2); err != nil {
		slog.Error("Failed to add rule2", "error", err)
	} else {
		fmt.Println("  ✓ Added prefix matching rule")
	}

	// Rule 3: Suffix matching rule
	rule3 := matcher.NewRule("suffix_rule").
		Dimension("product", "ProductB", matcher.MatchTypeEqual).
		Dimension("route", "", matcher.MatchTypeAny).
		Dimension("tool", "", matcher.MatchTypeAny).
		Dimension("tool_id", "_001", matcher.MatchTypeSuffix). // Matches "*_001"
		Dimension("recipe", "_beta", matcher.MatchTypeSuffix). // Matches "*_beta"
		Dimension("priority", "medium", matcher.MatchTypeEqual).
		Metadata("description", "Suffix matching rule for beta recipes").
		Build()

	if err := engine.AddRule(rule3); err != nil {
		slog.Error("Failed to add rule3", "error", err)
	} else {
		fmt.Println("  ✓ Added suffix matching rule")
	}

	// Rule 4: Fallback rule with manual weight
	rule4 := matcher.NewRule("fallback_rule").
		Dimension("product", "", matcher.MatchTypeAny).
		Dimension("route", "", matcher.MatchTypeAny).
		Dimension("tool", "", matcher.MatchTypeAny).
		Dimension("tool_id", "", matcher.MatchTypeAny).
		Dimension("recipe", "", matcher.MatchTypeAny).
		ManualWeight(1.0). // Low manual weight as fallback
		Metadata("description", "Fallback rule for unmatched queries").
		Build()

	if err := engine.AddRule(rule4); err != nil {
		slog.Error("Failed to add rule4", "error", err)
	} else {
		fmt.Println("  ✓ Added fallback rule (manual weight)")
	}

	fmt.Println("\n3. Testing queries...")

	// Test queries
	testQueries := []struct {
		name  string
		query *matcher.QueryRule
	}{
		{
			name: "Exact production match",
			query: matcher.CreateQuery(map[string]string{
				"product":     "ProductA",
				"route":       "main",
				"tool":        "laser",
				"tool_id":     "LASER_001",
				"recipe":      "recipe_alpha",
				"region":      "us-west",
				"priority":    "high",
				"environment": "production",
			}),
		},
		{
			name: "Prefix match test",
			query: matcher.CreateQuery(map[string]string{
				"product":     "ProductABC", // Should match "Prod*"
				"route":       "backup",
				"tool":        "laser_cutter", // Should match "laser*"
				"tool_id":     "TOOL_123",
				"recipe":      "recipe_gamma", // Should match "recipe*"
				"environment": "dev",
			}),
		},
		{
			name: "Suffix match test",
			query: matcher.CreateQuery(map[string]string{
				"product":  "ProductB",
				"route":    "alternative",
				"tool":     "drill",
				"tool_id":  "DRILL_001",   // Should match "*_001"
				"recipe":   "custom_beta", // Should match "*_beta"
				"priority": "medium",
			}),
		},
		{
			name: "Fallback match test",
			query: matcher.CreateQuery(map[string]string{
				"product": "UnknownProduct",
				"route":   "unknown_route",
				"tool":    "unknown_tool",
			}),
		},
	}

	for _, test := range testQueries {
		fmt.Printf("\n  Testing: %s\n", test.name)

		start := time.Now()
		result, err := engine.FindBestMatch(test.query)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("    ❌ Error: %v\n", err)
			continue
		}

		if result == nil {
			fmt.Printf("    ❌ No match found\n")
			continue
		}

		fmt.Printf("    ✓ Best match: %s (weight: %.2f, matched dims: %d)\n",
			result.Rule.ID, result.TotalWeight, result.MatchedDims)
		fmt.Printf("    ⏱  Query time: %v\n", duration)

		if desc, exists := result.Rule.Metadata["description"]; exists {
			fmt.Printf("    📝 Description: %s\n", desc)
		}
	}

	fmt.Println("\n4. Performance testing...")

	// Performance test
	testQuery := matcher.CreateQuery(map[string]string{
		"product": "ProductA",
		"route":   "main",
		"tool":    "laser",
	})

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		_, err := engine.FindBestMatch(testQuery)
		if err != nil {
			slog.Error("Query failed", "query_index", i, "error", err)
		}
	}

	totalTime := time.Since(start)
	avgTime := totalTime / time.Duration(iterations)
	qps := float64(iterations) / totalTime.Seconds()

	fmt.Printf("  Performed %d queries in %v\n", iterations, totalTime)
	fmt.Printf("  Average query time: %v\n", avgTime)
	fmt.Printf("  Queries per second: %.2f\n", qps)

	fmt.Println("\n5. Engine statistics...")

	stats := engine.GetStats()
	fmt.Printf("  Total rules: %d\n", stats.TotalRules)
	fmt.Printf("  Total dimensions: %d\n", stats.TotalDimensions)
	fmt.Printf("  Total queries: %d\n", stats.TotalQueries)
	fmt.Printf("  Average query time: %v\n", stats.AverageQueryTime)
	fmt.Printf("  Cache hit rate: %.2f%%\n", stats.CacheHitRate*100)

	fmt.Println("\n6. Cache statistics...")
	cacheStats := engine.GetCacheStats()
	fmt.Printf("  Cache entries: %v\n", cacheStats["total_entries"])
	fmt.Printf("  Expired entries: %v\n", cacheStats["expired_entries"])
	fmt.Printf("  Max cache size: %v\n", cacheStats["max_size"])

	fmt.Println("\n7. Forest index statistics...")
	forestStats := engine.GetForestStats()
	fmt.Printf("  Total dimensions in forest: %v\n", forestStats["total_dimensions"])

	fmt.Println("\n8. Saving data...")
	if err := engine.Save(); err != nil {
		slog.Error("Failed to save", "error", err)
	} else {
		fmt.Println("  ✓ Data saved successfully")
	}

	fmt.Println("\n=== Demo completed successfully! ===")
	fmt.Println("\nKey features demonstrated:")
	fmt.Println("✓ Dynamic dimension management")
	fmt.Println("✓ Multiple match types (equal, prefix, suffix, any)")
	fmt.Println("✓ Forest index for high-performance searching")
	fmt.Println("✓ Query caching for improved performance")
	fmt.Println("✓ Flexible rule weighting system")
	fmt.Println("✓ JSON persistence")
	fmt.Println("✓ Comprehensive statistics and monitoring")
}
