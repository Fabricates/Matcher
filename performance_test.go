package matcher

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

// BenchmarkConfig holds configuration for performance tests
type BenchmarkConfig struct {
	NumRules      int
	NumDimensions int
	NumQueries    int
	Concurrency   int
}

// PerformanceMetrics holds detailed performance metrics
type PerformanceMetrics struct {
	TotalTime           time.Duration
	AverageResponseTime time.Duration
	ThroughputQPS       float64
	MemoryUsedMB        float64
	MemoryAllocatedMB   float64
	CPUUsagePercent     float64
	GCCount             uint32
	GCPauseTime         time.Duration
}

// TestLargeScalePerformance tests with 50k rules and up to 20 dimensions
func TestLargeScalePerformance(t *testing.T) {
	configs := []BenchmarkConfig{
		{NumRules: 10000, NumDimensions: 5, NumQueries: 1000, Concurrency: 1},
		{NumRules: 25000, NumDimensions: 10, NumQueries: 2000, Concurrency: 2},
		{NumRules: 50000, NumDimensions: 15, NumQueries: 5000, Concurrency: 4},
		{NumRules: 50000, NumDimensions: 20, NumQueries: 10000, Concurrency: 8},
	}

	for _, config := range configs {
		t.Run(fmt.Sprintf("Rules_%d_Dims_%d_Queries_%d_Concurrency_%d",
			config.NumRules, config.NumDimensions, config.NumQueries, config.Concurrency), func(t *testing.T) {

			metrics := runPerformanceTest(t, config)
			logPerformanceMetrics(t, config, metrics)

			// Performance assertions
			if metrics.AverageResponseTime > 50*time.Millisecond {
				t.Errorf("Average response time too high: %v (expected < 50ms)", metrics.AverageResponseTime)
			}

			if metrics.ThroughputQPS < 100 {
				t.Errorf("Throughput too low: %.2f QPS (expected > 100)", metrics.ThroughputQPS)
			}

			if metrics.MemoryUsedMB > 4000 { // 4GB limit
				t.Errorf("Memory usage too high: %.2f MB (expected < 4000MB)", metrics.MemoryUsedMB)
			}
		})
	}
}

// runPerformanceTest executes a performance test with the given configuration
func runPerformanceTest(t *testing.T, config BenchmarkConfig) PerformanceMetrics {
	// Force garbage collection before test
	runtime.GC()
	debug.FreeOSMemory()

	// Create matcher with optimized settings
	engine, err := NewInMemoryMatcher(NewJSONPersistence("./test_perf_data"), nil, "perf-test")
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for performance testing to avoid weight conflicts with similar rules
	engine.allowDuplicateWeights = true

	// Configure dimensions
	dimensions := generateDimensions(config.NumDimensions)
	for _, dim := range dimensions {
		if err := engine.AddDimension(dim); err != nil {
			t.Fatalf("Failed to add dimension: %v", err)
		}
	}

	// Record memory before adding rules
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Generate and add rules
	rules := generateRules(config.NumRules, dimensions)

	start := time.Now()
	for i, rule := range rules {
		if err := engine.AddRule(rule); err != nil {
			t.Fatalf("Failed to add rule %d: %v", i, err)
		}

		// Progress logging for large datasets
		if i > 0 && i%10000 == 0 {
			t.Logf("Added %d/%d rules", i, config.NumRules)
		}
	}
	ruleAddTime := time.Since(start)
	t.Logf("Added %d rules in %v", config.NumRules, ruleAddTime)

	// Record memory after adding rules
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Generate queries
	queries := generateQueries(config.NumQueries, dimensions)

	// Warm up
	for i := 0; i < 100; i++ {
		query := queries[i%len(queries)]
		_, _ = engine.FindBestMatch(query)
	}

	// Force GC before measurement
	runtime.GC()

	// Record initial GC stats
	var gcBefore debug.GCStats
	debug.ReadGCStats(&gcBefore)

	// Performance measurement with concurrency
	var wg sync.WaitGroup
	queryStart := time.Now()

	queriesPerWorker := config.NumQueries / config.Concurrency
	successCount := int64(0)
	var successMutex sync.Mutex

	for worker := 0; worker < config.Concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			localSuccess := 0
			startIdx := workerID * queriesPerWorker
			endIdx := startIdx + queriesPerWorker
			if workerID == config.Concurrency-1 {
				endIdx = config.NumQueries // Handle remainder
			}

			for i := startIdx; i < endIdx; i++ {
				query := queries[i%len(queries)]
				result, err := engine.FindBestMatch(query)
				if err == nil && result != nil {
					localSuccess++
				}
			}

			successMutex.Lock()
			successCount += int64(localSuccess)
			successMutex.Unlock()
		}(worker)
	}

	wg.Wait()
	totalQueryTime := time.Since(queryStart)

	// Record final GC stats
	var gcAfter debug.GCStats
	debug.ReadGCStats(&gcAfter)

	// Record final memory stats
	var memFinal runtime.MemStats
	runtime.ReadMemStats(&memFinal)

	// Calculate metrics
	return PerformanceMetrics{
		TotalTime:           totalQueryTime,
		AverageResponseTime: time.Duration(int64(totalQueryTime) / int64(config.NumQueries)),
		ThroughputQPS:       float64(config.NumQueries) / totalQueryTime.Seconds(),
		MemoryUsedMB:        float64(memFinal.Sys) / 1024 / 1024,
		MemoryAllocatedMB:   float64(memFinal.Alloc) / 1024 / 1024,
		CPUUsagePercent:     calculateCPUUsage(totalQueryTime, config.Concurrency),
		GCCount:             uint32(gcAfter.NumGC - gcBefore.NumGC),
		GCPauseTime:         time.Duration(gcAfter.PauseTotal - gcBefore.PauseTotal),
	}
}

// generateDimensions creates dimension configurations for testing
func generateDimensions(count int) []*DimensionConfig {
	dimensionNames := []string{
		"product", "environment", "region", "tier", "service", "version",
		"team", "project", "component", "stage", "cluster", "namespace",
		"deployment", "branch", "feature", "customer", "tenant", "zone",
		"category", "priority",
	}

	dimensions := make([]*DimensionConfig, count)
	for i := 0; i < count; i++ {
		dimensions[i] = NewDimensionConfig(
			dimensionNames[i%len(dimensionNames)] + fmt.Sprintf("_%d", i/len(dimensionNames)),
			i,
			i < 3,                        // First 3 dimensions are required
			float64(10 - i%10),          // Varying weights
		)
	}
	return dimensions
}

// generateRules creates test rules with realistic distribution
func generateRules(count int, dimensions []*DimensionConfig) []*Rule {
	rules := make([]*Rule, count)

	// Value pools for realistic data
	valuePools := map[string][]string{
		"product":     {"ProductA", "ProductB", "ProductC", "ProductD", "ProductE"},
		"environment": {"production", "staging", "development", "testing"},
		"region":      {"us-west", "us-east", "eu-west", "eu-central", "ap-southeast"},
		"tier":        {"premium", "standard", "basic"},
		"service":     {"api", "web", "worker", "scheduler", "database"},
		"version":     {"v1.0", "v1.1", "v2.0", "v2.1", "v3.0"},
	}

	for i := 0; i < count; i++ {
		ruleBuilder := NewRule(fmt.Sprintf("rule_%d", i))

		// Add dimensions with varying probability
		for _, dim := range dimensions {
			// Required dimensions always added, optional ones with 70% probability
			if dim.Required || rand.Float64() < 0.7 {
				var value string
				var matchType MatchType

				// Get base name without suffix
				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					value = pool[rand.Intn(len(pool))]
				} else {
					value = fmt.Sprintf("value_%d_%d", i, rand.Intn(100))
				}

				// Distribute match types realistically
				switch rand.Intn(10) {
				case 0, 1: // 20% MatchTypeAny
					matchType = MatchTypeAny
					value = ""
				case 2: // 10% MatchTypePrefix
					matchType = MatchTypePrefix
					if len(value) > 3 {
						value = value[:3]
					}
				case 3: // 10% MatchTypeSuffix
					matchType = MatchTypeSuffix
					if len(value) > 3 {
						value = value[len(value)-3:]
					}
				default: // 60% MatchTypeEqual
					matchType = MatchTypeEqual
				}

				ruleBuilder.Dimension(dim.Name, value, matchType)
			}
		}

		rules[i] = ruleBuilder.Build()
	}

	return rules
}

// generateQueries creates test queries with realistic patterns
func generateQueries(count int, dimensions []*DimensionConfig) []*QueryRule {
	queries := make([]*QueryRule, count)

	// Value pools matching rule generation
	valuePools := map[string][]string{
		"product":     {"ProductA", "ProductB", "ProductC", "ProductD", "ProductE"},
		"environment": {"production", "staging", "development", "testing"},
		"region":      {"us-west", "us-east", "eu-west", "eu-central", "ap-southeast"},
		"tier":        {"premium", "standard", "basic"},
		"service":     {"api", "web", "worker", "scheduler", "database"},
		"version":     {"v1.0", "v1.1", "v2.0", "v2.1", "v3.0"},
	}

	for i := 0; i < count; i++ {
		values := make(map[string]string)

		// Add required dimensions (always specified in queries)
		for _, dim := range dimensions {
			if dim.Required {
				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					values[dim.Name] = pool[rand.Intn(len(pool))]
				} else {
					values[dim.Name] = fmt.Sprintf("query_value_%d", rand.Intn(50))
				}
			}
		}

		// Optionally add some optional dimensions (partial query testing)
		for _, dim := range dimensions {
			if !dim.Required && rand.Float64() < 0.4 { // 40% chance for optional dims
				baseName := dim.Name
				if idx := len(baseName) - 2; idx > 0 && baseName[idx] == '_' {
					baseName = baseName[:idx]
				}

				if pool, exists := valuePools[baseName]; exists {
					values[dim.Name] = pool[rand.Intn(len(pool))]
				} else {
					values[dim.Name] = fmt.Sprintf("query_value_%d", rand.Intn(50))
				}
			}
		}

		queries[i] = CreateQuery(values)
	}

	return queries
}

// calculateCPUUsage estimates CPU usage based on execution time and concurrency
func calculateCPUUsage(duration time.Duration, concurrency int) float64 {
	// Simplified CPU usage calculation
	// In a real test, you'd measure actual CPU time
	totalCPUTime := duration * time.Duration(concurrency)
	return float64(totalCPUTime) / float64(duration) * 100.0 / float64(runtime.NumCPU())
}

// logPerformanceMetrics logs detailed performance metrics
func logPerformanceMetrics(t *testing.T, config BenchmarkConfig, metrics PerformanceMetrics) {
	t.Logf("\n"+
		"=== PERFORMANCE METRICS ===\n"+
		"Configuration:\n"+
		"  Rules: %d\n"+
		"  Dimensions: %d\n"+
		"  Queries: %d\n"+
		"  Concurrency: %d\n"+
		"Performance:\n"+
		"  Total Time: %v\n"+
		"  Avg Response Time: %v\n"+
		"  Throughput: %.2f QPS\n"+
		"Memory:\n"+
		"  System Memory: %.2f MB\n"+
		"  Allocated Memory: %.2f MB\n"+
		"Garbage Collection:\n"+
		"  GC Count: %d\n"+
		"  GC Pause Time: %v\n"+
		"CPU (estimated):\n"+
		"  CPU Usage: %.2f%%\n"+
		"===========================",
		config.NumRules, config.NumDimensions, config.NumQueries, config.Concurrency,
		metrics.TotalTime, metrics.AverageResponseTime, metrics.ThroughputQPS,
		metrics.MemoryUsedMB, metrics.MemoryAllocatedMB,
		metrics.GCCount, metrics.GCPauseTime,
		metrics.CPUUsagePercent)
}

// BenchmarkQueryPerformance provides Go benchmark results
func BenchmarkQueryPerformance(b *testing.B) {
	// Create test engine
	engine, err := NewInMemoryMatcher(NewJSONPersistence("./bench_data"), nil, "bench-test")
	if err != nil {
		b.Fatalf("Failed to create matcher: %v", err)
	}
	defer engine.Close()

	// Add dimensions
	dimensions := generateDimensions(10)
	for _, dim := range dimensions {
		if err := engine.AddDimension(dim); err != nil {
			b.Fatalf("Failed to add dimension: %v", err)
		}
	}

	// Add rules
	rules := generateRules(10000, dimensions)
	for _, rule := range rules {
		if err := engine.AddRule(rule); err != nil {
			b.Fatalf("Failed to add rule: %v", err)
		}
	}

	// Generate test queries
	queries := generateQueries(1000, dimensions)

	// Reset timer before benchmark
	b.ResetTimer()

	// Run benchmark
	b.RunParallel(func(pb *testing.PB) {
		queryIndex := 0
		for pb.Next() {
			query := queries[queryIndex%len(queries)]
			_, _ = engine.FindBestMatch(query)
			queryIndex++
		}
	})
}

// BenchmarkMemoryEfficiency measures memory usage under different loads
func BenchmarkMemoryEfficiency(b *testing.B) {
	testCases := []struct {
		name     string
		numRules int
		numDims  int
	}{
		{"Small_1K_5D", 1000, 5},
		{"Medium_10K_10D", 10000, 10},
		{"Large_50K_15D", 50000, 15},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			var memBefore, memAfter runtime.MemStats

			// Measure memory before
			runtime.GC()
			runtime.ReadMemStats(&memBefore)

			// Create and populate engine
			engine, err := NewInMemoryMatcher(NewJSONPersistence("./mem_bench_data"), nil, "mem-test")
			if err != nil {
				b.Fatalf("Failed to create matcher: %v", err)
			}

			dimensions := generateDimensions(tc.numDims)
			for _, dim := range dimensions {
				if err := engine.AddDimension(dim); err != nil {
					b.Fatalf("Failed to add dimension: %v", err)
				}
			}

			rules := generateRules(tc.numRules, dimensions)
			for _, rule := range rules {
				if err := engine.AddRule(rule); err != nil {
					b.Fatalf("Failed to add rule: %v", err)
				}
			}

			// Measure memory after
			runtime.GC()
			runtime.ReadMemStats(&memAfter)

			engine.Close()

			// Calculate memory usage
			memUsed := memAfter.Alloc - memBefore.Alloc
			memUsedMB := float64(memUsed) / 1024 / 1024
			bytesPerRule := float64(memUsed) / float64(tc.numRules)

			b.Logf("Memory usage for %d rules with %d dimensions:", tc.numRules, tc.numDims)
			b.Logf("  Total memory: %.2f MB", memUsedMB)
			b.Logf("  Memory per rule: %.2f bytes", bytesPerRule)
			b.Logf("  Memory efficiency: %.2f MB/1K rules", memUsedMB*1000/float64(tc.numRules))
		})
	}
}
