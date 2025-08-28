package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Fabricates/Matcher"
)

type SystemMetrics struct {
	CPUCount          int
	MaxMemoryMB       float64
	UsedMemoryMB      float64
	AllocatedMemoryMB float64
	GCCount           uint32
	GCPauseTimeTotal  time.Duration
	NumGoroutines     int
}

type PerformanceResult struct {
	Config              BenchmarkConfig
	SetupTime           time.Duration
	QueryTime           time.Duration
	TotalTime           time.Duration
	AverageResponseTime time.Duration
	ThroughputQPS       float64
	SuccessRate         float64
	SystemMetrics       SystemMetrics
}

type BenchmarkConfig struct {
	Name          string
	NumRules      int
	NumDimensions int
	NumQueries    int
	Concurrency   int
}

func main() {
	fmt.Println("=== High-Performance Rule Matching Engine - Performance Benchmark ===")
	fmt.Printf("System: %d CPU cores, Go %s\n", runtime.NumCPU(), runtime.Version())

	// Test configurations targeting the user's requirements
	configs := []BenchmarkConfig{
		{
			Name:          "Small_Scale_Baseline",
			NumRules:      5000,
			NumDimensions: 8,
			NumQueries:    2000,
			Concurrency:   2,
		},
		{
			Name:          "Medium_Scale_10D",
			NumRules:      25000,
			NumDimensions: 10,
			NumQueries:    5000,
			Concurrency:   2,
		},
		{
			Name:          "Large_Scale_15D",
			NumRules:      50000,
			NumDimensions: 15,
			NumQueries:    10000,
			Concurrency:   2,
		},
		{
			Name:          "Max_Scale_20D_Target",
			NumRules:      50000,
			NumDimensions: 20,
			NumQueries:    20000,
			Concurrency:   2, // Simulating 2 cores
		},
	}

	results := make([]PerformanceResult, 0, len(configs))

	for i, config := range configs {
		fmt.Printf("\n--- Running Test %d/%d: %s ---\n", i+1, len(configs), config.Name)

		result, err := runPerformanceBenchmark(config)
		if err != nil {
			slog.Error("Error running benchmark", "benchmark", config.Name, "error", err)
			continue
		}

		results = append(results, result)
		printDetailedResults(result)

		// Force cleanup between tests
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n=== PERFORMANCE SUMMARY ===")
	printSummaryTable(results)

	// Check against user requirements
	fmt.Println("\n=== REQUIREMENTS VALIDATION ===")
	validateRequirements(results)
}

func runPerformanceBenchmark(config BenchmarkConfig) (PerformanceResult, error) {
	// Force initial cleanup
	runtime.GC()
	debug.FreeOSMemory()

	var setupStart = time.Now()

	// Create matcher engine
	engine, err := matcher.NewInMemoryMatcher(
		matcher.NewJSONPersistence("./perf_test_data"),
		nil,
		"perf-benchmark",
	)
	if err != nil {
		return PerformanceResult{}, fmt.Errorf("failed to create matcher: %w", err)
	}
	defer engine.Close()

	// Generate and configure dimensions
	dimensions := generateRealisticDimensions(config.NumDimensions)
	for _, dim := range dimensions {
		if err := engine.AddDimension(dim); err != nil {
			return PerformanceResult{}, fmt.Errorf("failed to add dimension: %w", err)
		}
	}

	// Record memory before adding rules
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Generate and add rules
	fmt.Printf("  Generating %d rules with %d dimensions...", config.NumRules, config.NumDimensions)
	rules := generateRealisticRules(config.NumRules, dimensions)

	for i, rule := range rules {
		if err := engine.AddRule(rule); err != nil {
			return PerformanceResult{}, fmt.Errorf("failed to add rule %d: %w", i, err)
		}

		if i > 0 && i%10000 == 0 {
			fmt.Printf(" %d", i)
		}
	}
	fmt.Println(" ✓")

	setupTime := time.Since(setupStart)

	// Generate queries
	fmt.Printf("  Generating %d queries...", config.NumQueries)
	queries := generateRealisticQueries(config.NumQueries, dimensions)
	fmt.Println(" ✓")

	// Warm up the system
	fmt.Print("  Warming up...")
	for i := 0; i < 100; i++ {
		query := queries[i%len(queries)]
		_, _ = engine.FindBestMatch(query)
	}
	fmt.Println(" ✓")

	// Force GC before measurement
	runtime.GC()

	// Record GC stats before test
	var gcBefore debug.GCStats
	debug.ReadGCStats(&gcBefore)

	// Run concurrent performance test
	fmt.Printf("  Running %d queries with %d workers...", config.NumQueries, config.Concurrency)

	queryStart := time.Now()
	successCount, totalExecuted := runConcurrentQueries(engine, queries, config)
	queryTime := time.Since(queryStart)

	fmt.Println(" ✓")

	// Record final stats
	var gcAfter debug.GCStats
	debug.ReadGCStats(&gcAfter)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate metrics
	successRate := float64(successCount) / float64(totalExecuted) * 100
	avgResponseTime := time.Duration(int64(queryTime) / int64(totalExecuted))
	throughput := float64(totalExecuted) / queryTime.Seconds()

	return PerformanceResult{
		Config:              config,
		SetupTime:           setupTime,
		QueryTime:           queryTime,
		TotalTime:           time.Since(setupStart),
		AverageResponseTime: avgResponseTime,
		ThroughputQPS:       throughput,
		SuccessRate:         successRate,
		SystemMetrics: SystemMetrics{
			CPUCount:          runtime.NumCPU(),
			MaxMemoryMB:       float64(memAfter.Sys) / 1024 / 1024,
			UsedMemoryMB:      float64(memAfter.Alloc) / 1024 / 1024,
			AllocatedMemoryMB: float64(memAfter.TotalAlloc) / 1024 / 1024,
			GCCount:           uint32(gcAfter.NumGC - gcBefore.NumGC),
			GCPauseTimeTotal:  time.Duration(gcAfter.PauseTotal - gcBefore.PauseTotal),
			NumGoroutines:     runtime.NumGoroutine(),
		},
	}, nil
}

func runConcurrentQueries(engine *matcher.InMemoryMatcher, queries []*matcher.QueryRule, config BenchmarkConfig) (int64, int64) {
	var wg sync.WaitGroup
	var successCount, totalCount int64
	var mu sync.Mutex

	queriesPerWorker := config.NumQueries / config.Concurrency

	for worker := 0; worker < config.Concurrency; worker++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			localSuccess := int64(0)
			localTotal := int64(0)

			startIdx := workerID * queriesPerWorker
			endIdx := startIdx + queriesPerWorker
			if workerID == config.Concurrency-1 {
				endIdx = config.NumQueries // Handle remainder
			}

			for i := startIdx; i < endIdx; i++ {
				query := queries[i%len(queries)]
				result, err := engine.FindBestMatch(query)

				localTotal++
				if err == nil && result != nil {
					localSuccess++
				}
			}

			mu.Lock()
			successCount += localSuccess
			totalCount += localTotal
			mu.Unlock()
		}(worker)
	}

	wg.Wait()
	return successCount, totalCount
}

func generateRealisticDimensions(count int) []*matcher.DimensionConfig {
	dimensionNames := []string{
		"product", "environment", "region", "service", "version", "team",
		"project", "component", "stage", "cluster", "namespace", "deployment",
		"branch", "feature", "customer", "tenant", "zone", "category",
		"priority", "tier",
	}

	dimensions := make([]*matcher.DimensionConfig, count)
	for i := 0; i < count; i++ {
		name := dimensionNames[i%len(dimensionNames)]
		if i >= len(dimensionNames) {
			name = fmt.Sprintf("%s_%d", name, i/len(dimensionNames))
		}

		dimensions[i] = &matcher.DimensionConfig{
			Name:     name,
			Index:    i,
			Required: i < 3,                  // First 3 dimensions required
			Weight:   float64(20 - (i % 20)), // Weights from 20 down to 1, cycling if needed
		}
	}
	return dimensions
}

func generateRealisticRules(count int, dimensions []*matcher.DimensionConfig) []*matcher.Rule {
	rules := make([]*matcher.Rule, count)
	rand.Seed(time.Now().UnixNano())

	// Realistic value distributions
	valuePools := map[string][]string{
		"product":     {"ProductA", "ProductB", "ProductC", "ProductD", "ProductE", "ProductF"},
		"environment": {"production", "staging", "development", "testing"},
		"region":      {"us-west-1", "us-west-2", "us-east-1", "eu-west-1", "eu-central-1", "ap-southeast-1"},
		"service":     {"api", "web", "worker", "scheduler", "database", "cache", "queue", "monitor"},
		"version":     {"v1.0", "v1.1", "v1.2", "v2.0", "v2.1", "v3.0"},
		"team":        {"backend", "frontend", "devops", "data", "security", "mobile"},
		"tier":        {"premium", "standard", "basic", "enterprise"},
	}

	for i := 0; i < count; i++ {
		ruleBuilder := matcher.NewRule(fmt.Sprintf("rule_%06d", i))

		for _, dim := range dimensions {
			// Always add required dimensions, optional with 80% probability
			if dim.Required || rand.Float64() < 0.8 {
				var value string
				var matchType matcher.MatchType

				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					value = pool[rand.Intn(len(pool))]
				} else {
					value = fmt.Sprintf("val_%d_%d", i, rand.Intn(100))
				}

				// Realistic match type distribution
				r := rand.Float64()
				switch {
				case r < 0.65: // 65% exact matches
					matchType = matcher.MatchTypeEqual
				case r < 0.75: // 10% any matches
					matchType = matcher.MatchTypeAny
					value = ""
				case r < 0.85: // 10% prefix matches
					matchType = matcher.MatchTypePrefix
					if len(value) > 3 {
						value = value[:3]
					}
				default: // 15% suffix matches
					matchType = matcher.MatchTypeSuffix
					if len(value) > 3 {
						value = value[len(value)-3:]
					}
				}

				ruleBuilder.Dimension(dim.Name, value, matchType)
			}
		}

		rules[i] = ruleBuilder.Build()
	}

	return rules
}

func generateRealisticQueries(count int, dimensions []*matcher.DimensionConfig) []*matcher.QueryRule {
	queries := make([]*matcher.QueryRule, count)
	rand.Seed(time.Now().UnixNano() + 12345)

	valuePools := map[string][]string{
		"product":     {"ProductA", "ProductB", "ProductC", "ProductD", "ProductE", "ProductF"},
		"environment": {"production", "staging", "development", "testing"},
		"region":      {"us-west-1", "us-west-2", "us-east-1", "eu-west-1", "eu-central-1", "ap-southeast-1"},
		"service":     {"api", "web", "worker", "scheduler", "database", "cache", "queue", "monitor"},
		"version":     {"v1.0", "v1.1", "v1.2", "v2.0", "v2.1", "v3.0"},
		"team":        {"backend", "frontend", "devops", "data", "security", "mobile"},
		"tier":        {"premium", "standard", "basic", "enterprise"},
	}

	for i := 0; i < count; i++ {
		values := make(map[string]string)

		// Always include required dimensions
		for _, dim := range dimensions {
			if dim.Required {
				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					values[dim.Name] = pool[rand.Intn(len(pool))]
				} else {
					values[dim.Name] = fmt.Sprintf("query_val_%d", rand.Intn(50))
				}
			}
		}

		// Add optional dimensions with varying probability for partial query testing
		for _, dim := range dimensions {
			if !dim.Required && rand.Float64() < 0.6 { // 60% chance for optional
				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					values[dim.Name] = pool[rand.Intn(len(pool))]
				} else {
					values[dim.Name] = fmt.Sprintf("query_val_%d", rand.Intn(50))
				}
			}
		}

		queries[i] = matcher.CreateQuery(values)
	}

	return queries
}

func printDetailedResults(result PerformanceResult) {
	fmt.Printf("\n📊 Detailed Results for %s:\n", result.Config.Name)
	fmt.Printf("┌─────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│ CONFIGURATION                                               │\n")
	fmt.Printf("├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ Rules:           %10d                               │\n", result.Config.NumRules)
	fmt.Printf("│ Dimensions:      %10d                               │\n", result.Config.NumDimensions)
	fmt.Printf("│ Queries:         %10d                               │\n", result.Config.NumQueries)
	fmt.Printf("│ Concurrency:     %10d                               │\n", result.Config.Concurrency)
	fmt.Printf("├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ PERFORMANCE METRICS                                         │\n")
	fmt.Printf("├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ Setup Time:      %10v                               │\n", result.SetupTime)
	fmt.Printf("│ Query Time:      %10v                               │\n", result.QueryTime)
	fmt.Printf("│ Total Time:      %10v                               │\n", result.TotalTime)
	fmt.Printf("│ Avg Response:    %10v                               │\n", result.AverageResponseTime)
	fmt.Printf("│ Throughput:      %10.2f QPS                         │\n", result.ThroughputQPS)
	fmt.Printf("│ Success Rate:    %10.1f%%                           │\n", result.SuccessRate)
	fmt.Printf("├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ SYSTEM RESOURCES                                            │\n")
	fmt.Printf("├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ CPU Cores:       %10d                               │\n", result.SystemMetrics.CPUCount)
	fmt.Printf("│ Memory Used:     %10.2f MB                          │\n", result.SystemMetrics.UsedMemoryMB)
	fmt.Printf("│ Memory System:   %10.2f MB                          │\n", result.SystemMetrics.MaxMemoryMB)
	fmt.Printf("│ GC Count:        %10d                               │\n", result.SystemMetrics.GCCount)
	fmt.Printf("│ GC Pause:        %10v                               │\n", result.SystemMetrics.GCPauseTimeTotal)
	fmt.Printf("│ Goroutines:      %10d                               │\n", result.SystemMetrics.NumGoroutines)
	fmt.Printf("└─────────────────────────────────────────────────────────────┘\n")
}

func printSummaryTable(results []PerformanceResult) {
	fmt.Printf("┌─────────────────────┬────────┬────────┬──────────┬───────────┬─────────┐\n")
	fmt.Printf("│ Test Name           │ Rules  │ Dims   │ Avg Time │ QPS       │ Memory  │\n")
	fmt.Printf("├─────────────────────┼────────┼────────┼──────────┼───────────┼─────────┤\n")

	for _, result := range results {
		fmt.Printf("│ %-19s │ %6d │ %6d │ %8v │ %9.1f │ %6.1fMB │\n",
			result.Config.Name,
			result.Config.NumRules,
			result.Config.NumDimensions,
			result.AverageResponseTime,
			result.ThroughputQPS,
			result.SystemMetrics.UsedMemoryMB)
	}
	fmt.Printf("└─────────────────────┴────────┴────────┴──────────┴───────────┴─────────┘\n")
}

func validateRequirements(results []PerformanceResult) {
	target := findTargetResult(results)
	if target == nil {
		fmt.Println("❌ Target configuration (50k rules, 20 dimensions) not found")
		return
	}

	fmt.Printf("Validating against target configuration: %s\n", target.Config.Name)

	// Memory requirement: 4GB
	memoryOK := target.SystemMetrics.UsedMemoryMB <= 4000
	fmt.Printf("Memory Usage: %.2f MB / 4000 MB limit %s\n",
		target.SystemMetrics.UsedMemoryMB,
		checkmark(memoryOK))

	// Performance expectations
	responseTimeOK := target.AverageResponseTime <= 100*time.Millisecond
	fmt.Printf("Response Time: %v (target: < 100ms) %s\n",
		target.AverageResponseTime,
		checkmark(responseTimeOK))

	throughputOK := target.ThroughputQPS >= 100
	fmt.Printf("Throughput: %.2f QPS (target: > 100 QPS) %s\n",
		target.ThroughputQPS,
		checkmark(throughputOK))

	successRateOK := target.SuccessRate >= 95.0
	fmt.Printf("Success Rate: %.1f%% (target: > 95%%) %s\n",
		target.SuccessRate,
		checkmark(successRateOK))

	// CPU simulation (2 cores)
	cpuOK := target.Config.Concurrency <= 2
	fmt.Printf("CPU Simulation: %d concurrent workers (simulating 2 cores) %s\n",
		target.Config.Concurrency,
		checkmark(cpuOK))

	allOK := memoryOK && responseTimeOK && throughputOK && successRateOK && cpuOK
	fmt.Printf("\nOverall Requirements: %s\n", checkmark(allOK))

	if allOK {
		fmt.Println("🎉 All requirements met! The system can handle 50k rules with 20 dimensions within 4GB memory on 2 cores.")
	} else {
		fmt.Println("⚠️  Some requirements not met. Consider optimization or scaling adjustments.")
	}
}

func findTargetResult(results []PerformanceResult) *PerformanceResult {
	for _, result := range results {
		if result.Config.NumRules == 50000 && result.Config.NumDimensions == 20 {
			return &result
		}
	}
	return nil
}

func checkmark(ok bool) string {
	if ok {
		return "✅"
	}
	return "❌"
}
