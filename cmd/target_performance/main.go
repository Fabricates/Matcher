package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Targeted Performance Test: 50k Rules, 20 Dimensions, 2 Cores ===")

	// Simulate 2-core constraint by setting GOMAXPROCS
	runtime.GOMAXPROCS(2)
	fmt.Printf("Limited to %d CPU cores\n", runtime.GOMAXPROCS(0))

	// Force initial cleanup
	runtime.GC()
	debug.FreeOSMemory()

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Create engine
	engine, err := matcher.NewInMemoryMatcher(
		matcher.NewJSONPersistence("./target_perf_data"),
		nil,
		"target-test",
	)
	if err != nil {
		slog.Error("Failed to create matcher", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	// Configure 20 dimensions
	fmt.Print("Configuring 20 dimensions...")
	for i := 0; i < 20; i++ {
		dim := &matcher.DimensionConfig{
			Name:     fmt.Sprintf("dim_%02d", i),
			Index:    i,
			Required: i < 3,           // First 3 required
			Weight:   float64(21 - i), // 20, 19, 18, ... 1
		}
		if err := engine.AddDimension(dim); err != nil {
			slog.Error("Failed to add dimension", "error", err)
			os.Exit(1)
		}
	}
	fmt.Println(" ✓")

	// Generate and add 50k rules
	fmt.Print("Adding 50,000 rules...")
	startTime := time.Now()

	for i := 0; i < 50000; i++ {
		rule := generateSimpleRule(i)
		if err := engine.AddRule(rule); err != nil {
			slog.Error("Failed to add rule", "rule_id", i, "error", err)
			os.Exit(1)
		}

		if i > 0 && i%10000 == 0 {
			fmt.Printf(" %dk", i/1000)
		}
	}

	ruleLoadTime := time.Since(startTime)
	fmt.Printf(" ✓ (%.2fs)\n", ruleLoadTime.Seconds())

	// Memory check after loading rules
	var memAfterRules runtime.MemStats
	runtime.ReadMemStats(&memAfterRules)
	memUsedMB := float64(memAfterRules.Alloc) / 1024 / 1024
	memSysMB := float64(memAfterRules.Sys) / 1024 / 1024

	fmt.Printf("Memory after loading rules: %.1f MB allocated, %.1f MB system\n", memUsedMB, memSysMB)

	// Generate test queries
	queries := make([]*matcher.QueryRule, 10000)
	for i := range queries {
		queries[i] = generateSimpleQuery(i)
	}

	// Warm up
	fmt.Print("Warming up...")
	for i := 0; i < 100; i++ {
		_, _ = engine.FindBestMatch(queries[i%len(queries)])
	}
	fmt.Println(" ✓")

	// Performance test with 2 workers (simulating 2 cores)
	fmt.Print("Running performance test (2 workers, 10,000 queries)...")

	queryStart := time.Now()
	successCount := runConcurrentTest(engine, queries, 2)
	queryDuration := time.Since(queryStart)

	fmt.Println(" ✓")

	// Final memory measurement
	runtime.GC()
	var memFinal runtime.MemStats
	runtime.ReadMemStats(&memFinal)

	// Calculate metrics
	avgResponseTime := queryDuration / time.Duration(len(queries))
	throughput := float64(len(queries)) / queryDuration.Seconds()
	successRate := float64(successCount) / float64(len(queries)) * 100
	finalMemMB := float64(memFinal.Alloc) / 1024 / 1024
	finalSysMB := float64(memFinal.Sys) / 1024 / 1024

	// Results
	fmt.Println("\n=== PERFORMANCE RESULTS ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Rules: 50,000\n")
	fmt.Printf("  Dimensions: 20\n")
	fmt.Printf("  Queries: 10,000\n")
	fmt.Printf("  CPU Cores: 2 (simulated)\n")
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("  Rule Load Time: %.2fs\n", ruleLoadTime.Seconds())
	fmt.Printf("  Query Time: %.2fs\n", queryDuration.Seconds())
	fmt.Printf("  Average Response Time: %v\n", avgResponseTime)
	fmt.Printf("  Throughput: %.1f QPS\n", throughput)
	fmt.Printf("  Success Rate: %.1f%%\n", successRate)
	fmt.Printf("\nMemory Usage:\n")
	fmt.Printf("  Final Allocated: %.1f MB\n", finalMemMB)
	fmt.Printf("  Final System: %.1f MB\n", finalSysMB)
	fmt.Printf("  Memory per Rule: %.1f KB\n", finalMemMB*1024/50000)

	// Requirements validation
	fmt.Println("\n=== REQUIREMENTS CHECK ===")
	checkRequirement("Memory Usage", finalSysMB, 4000, "MB", "<=")
	checkRequirement("Response Time", float64(avgResponseTime.Microseconds()), 50000, "μs", "<=") // 50ms
	checkRequirement("Throughput", throughput, 100, "QPS", ">=")
	checkRequirement("Success Rate", successRate, 95, "%", ">=")

	fmt.Printf("\n🎯 Target Scenario (2 cores, 4GB, 50k rules, 20 dims): ")
	if finalSysMB <= 4000 && avgResponseTime <= 50*time.Millisecond && throughput >= 100 && successRate >= 95 {
		fmt.Println("PASSED ✅")
	} else {
		fmt.Println("NEEDS OPTIMIZATION ⚠️")
	}
}

func generateSimpleRule(id int) *matcher.Rule {
	rand.Seed(int64(id))

	rule := matcher.NewRule(fmt.Sprintf("rule_%06d", id))

	// Add required dimensions (first 3) with more predictable values
	rule.Dimension("dim_00", fmt.Sprintf("product_%d", id%20))
	rule.Dimension("dim_01", fmt.Sprintf("env_%d", id%4))
	rule.Dimension("dim_02", fmt.Sprintf("region_%d", id%8))

	// Add optional dimensions (with probability)
	for i := 3; i < 20; i++ {
		if rand.Float64() < 0.7 { // 70% probability
			var matchType matcher.MatchType
			var value string

			r := rand.Float64()
			if r < 0.6 {
				matchType = matcher.MatchTypeEqual
				value = fmt.Sprintf("val_%d_%d", i, id%10)
			} else if r < 0.8 {
				matchType = matcher.MatchTypePrefix
				value = fmt.Sprintf("pre_%d", id%5)
			} else {
				matchType = matcher.MatchTypeAny
				value = ""
			}

			rule.Dimension(fmt.Sprintf("dim_%02d", i), value))
		}
	}

	return rule.Build()
}

func generateSimpleQuery(id int) *matcher.QueryRule {
	rand.Seed(int64(id + 100000))

	values := make(map[string]string)

	// Always include required dimensions with overlapping values
	values["dim_00"] = fmt.Sprintf("product_%d", id%20) // Same pattern as rules
	values["dim_01"] = fmt.Sprintf("env_%d", id%4)      // Same pattern as rules
	values["dim_02"] = fmt.Sprintf("region_%d", id%8)   // Same pattern as rules

	// Include some optional dimensions (partial query)
	for i := 3; i < 20; i++ {
		if rand.Float64() < 0.4 { // 40% probability for optional dims
			values[fmt.Sprintf("dim_%02d", i)] = fmt.Sprintf("val_%d_%d", i, id%10)
		}
	}

	return matcher.CreateQuery(values)
}

func runConcurrentTest(engine *matcher.InMemoryMatcher, queries []*matcher.QueryRule, workers int) int64 {
	var wg sync.WaitGroup
	var successCount int64
	var mu sync.Mutex

	queriesPerWorker := len(queries) / workers

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			localSuccess := int64(0)
			startIdx := workerID * queriesPerWorker
			endIdx := startIdx + queriesPerWorker
			if workerID == workers-1 {
				endIdx = len(queries) // Handle remainder
			}

			for i := startIdx; i < endIdx; i++ {
				result, err := engine.FindBestMatch(queries[i])
				if err == nil && result != nil {
					localSuccess++
				}
			}

			mu.Lock()
			successCount += localSuccess
			mu.Unlock()
		}(worker)
	}

	wg.Wait()
	return successCount
}

func checkRequirement(name string, actual, target float64, unit, operator string) {
	var passed bool
	var symbol string

	switch operator {
	case "<=":
		passed = actual <= target
		symbol = "≤"
	case ">=":
		passed = actual >= target
		symbol = "≥"
	case "==":
		passed = actual == target
		symbol = "="
	}

	status := "❌"
	if passed {
		status = "✅"
	}

	fmt.Printf("  %s: %.1f %s %s %.1f %s %s\n",
		name, actual, unit, symbol, target, unit, status)
}
