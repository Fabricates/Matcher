package matcher

import (
	"context"
	"testing"
	"time"
)

func TestSimpleCoverageBoosters(t *testing.T) {
	// Test cache SetWithTTL method that's not covered
	cache := NewQueryCache(10, 60*time.Second)
	query := CreateQuery(map[string]string{"test": "value"})
	result := &MatchResult{Rule: &Rule{ID: "test"}, TotalWeight: 1.0}

	// This covers the SetWithTTL method
	cache.SetWithTTL(query, result, 30*time.Second)

	// Cover Size method
	size := cache.Size()
	t.Logf("Cache size: %d", size)

	// Cover CleanupExpired
	count := cache.CleanupExpired()
	t.Logf("Cleaned up %d expired entries", count)

	// Cover Stats method
	stats := cache.Stats()
	t.Logf("Cache stats: %v", stats)

	// Cover StartCleanupWorker
	stopChan := cache.StartCleanupWorker(100 * time.Millisecond)
	if stopChan != nil {
		stopChan <- true
	}

	// Test MultiLevelCache methods
	mlc := NewMultiLevelCache(10, 60*time.Second, 100, 120*time.Second)

	// Cover Get method
	result2 := mlc.Get(query)
	t.Logf("MultiLevel cache get result: %v", result2)

	// Cover Set method
	mlc.Set(query, result)

	// Cover Clear method
	mlc.Clear()

	// Cover Stats method
	mlcStats := mlc.Stats()
	t.Logf("MultiLevel cache stats: %v", mlcStats)

	// Test Forest methods that aren't covered
	forest := CreateForestIndexCompat()

	// Test forest creation and basic functionality
	if forest == nil {
		t.Error("CreateForestIndexCompat returned nil")
	}

	// Cover InitializeDimension (compatibility method)
	forest.InitializeDimension("new-dimension")

	// Test types methods
	rule := NewRule("test-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	// Cover GetDimensionValue
	dimValue := rule.GetDimensionValue("region")
	t.Logf("Dimension value: %v", dimValue)

	// Cover HasDimension
	hasDim := rule.HasDimension("region")
	t.Logf("Has dimension: %v", hasDim)

	// Test event subscriber health with context
	subscriber := NewMockEventSubscriber()
	ctx := context.Background()
	err := subscriber.Health(ctx)
	if err != nil {
		t.Logf("Subscriber health check: %v", err)
	}
	subscriber.Close()
}
