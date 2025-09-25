package matcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRebuildStarvationFix verifies that FindBestMatch doesn't get starved by frequent rebuilds
func TestRebuildStarvationFix(t *testing.T) {
	// Create persistence and engine
	persistence := NewJSONPersistence(t.TempDir())
	engine, err := NewInMemoryMatcher(persistence, nil, "test-starvation")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add a rule
	rule := NewRule("starvation-test-rule").
		Dimension("region", "us-east", MatchTypeEqual).
		Build()
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Save to persistence
	err = engine.SaveToPersistence()
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	query := CreateQuery(map[string]string{
		"region": "us-east",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var rebuildCount int64
	var queryCount int64
	var queryErrors int64

	// Start aggressive rebuilders
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := engine.Rebuild()
					if err != nil {
						t.Errorf("Rebuild failed: %v", err)
					} else {
						atomic.AddInt64(&rebuildCount, 1)
					}
				}
			}
		}()
	}

	// Start query workers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					result, err := engine.FindBestMatch(query)
					if err != nil {
						atomic.AddInt64(&queryErrors, 1)
					} else if result != nil {
						atomic.AddInt64(&queryCount, 1)
					}
				}
			}
		}()
	}

	// Wait for completion
	<-ctx.Done()
	wg.Wait()

	finalRebuildCount := atomic.LoadInt64(&rebuildCount)
	finalQueryCount := atomic.LoadInt64(&queryCount)
	finalQueryErrors := atomic.LoadInt64(&queryErrors)

	t.Logf("Starvation test results:")
	t.Logf("  Total rebuilds: %d", finalRebuildCount)
	t.Logf("  Total successful queries: %d", finalQueryCount)
	t.Logf("  Total query errors: %d", finalQueryErrors)

	// Calculate rates
	rebuildRate := float64(finalRebuildCount) / 3.0
	queryRate := float64(finalQueryCount) / 3.0

	t.Logf("  Rebuild rate: %.1f/sec", rebuildRate)
	t.Logf("  Query rate: %.1f/sec", queryRate)

	// Verify there were no query errors
	if finalQueryErrors > 0 {
		t.Errorf("Expected no query errors, got %d", finalQueryErrors)
	}

	// Verify that queries were not starved
	// With the fix, we should get a reasonable query rate even with aggressive rebuilds
	minExpectedQueryRate := 5000.0 // queries per second
	if queryRate < minExpectedQueryRate {
		t.Errorf("Query rate too low: %.1f/sec, expected at least %.1f/sec - possible starvation", 
			queryRate, minExpectedQueryRate)
	}

	// Verify that rebuilds were happening (ensuring the test is valid)
	minExpectedRebuildRate := 1000.0 // rebuilds per second
	if rebuildRate < minExpectedRebuildRate {
		t.Errorf("Rebuild rate too low: %.1f/sec, expected at least %.1f/sec - test may not be valid", 
			rebuildRate, minExpectedRebuildRate)
	}

	t.Logf("✓ Starvation fix verified: query rate %.1f/sec with rebuild rate %.1f/sec", 
		queryRate, rebuildRate)
}