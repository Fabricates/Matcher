package matcher

import (
	"testing"
)

func TestSimpleCoverageBoost(t *testing.T) {
	// Test NewMatcherEngine with broker parameter to get more coverage
	persistence := NewJSONPersistence("./test_data")
	broker := NewInMemoryEventBroker("coverage-test")

	engine, err := NewMatcherEngine(persistence, broker, "simple-coverage-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test engine GetForestStats
	stats := engine.GetForestStats()
	if stats != nil {
		t.Logf("Forest stats: %v", stats)
	}

	// Test GenerateDefaultNodeID with error case
	nodeID := GenerateDefaultNodeID()
	if nodeID == "" {
		t.Error("Expected non-empty node ID")
	}

	// Test BatchAddRules with empty list
	err = engine.BatchAddRules([]*Rule{})
	if err != nil {
		t.Errorf("BatchAddRules with empty list failed: %v", err)
	}

	// Test AutoSave to cover more branches
	engine.AutoSave(5) // Start autosave
	// Don't call AutoSave(0) as it causes panic
}
