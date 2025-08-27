package matcher

import (
	"os"
	"regexp"
	"testing"
)

func TestNewMatcherEngineWithDefaults(t *testing.T) {
	// Create temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Errorf("NewMatcherEngineWithDefaults failed: %v", err)
	}
	defer engine.Close()

	if engine == nil {
		t.Error("Expected non-nil engine")
	}
}

func TestAPIUpdateRule(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a rule first
	rule := NewRule("api-update-test").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()
	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Update the rule
	updatedRule := NewRule("api-update-test").
		Dimension("region", "us-east", MatchTypeEqual, 1.0).
		Build()
	if err := engine.UpdateRule(updatedRule); err != nil {
		t.Errorf("UpdateRule failed: %v", err)
	}
}

func TestAPIDeleteRule(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a rule first
	rule := NewRule("api-delete-test").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()
	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Delete the rule
	if err := engine.DeleteRule("api-delete-test"); err != nil {
		t.Errorf("DeleteRule failed: %v", err)
	}
}

func TestAPIAddDimension(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a dimension
	dim := &DimensionConfig{
		Name:     "api-test-dim",
		Index:    100,
		Required: false,
		Weight:   1.0,
	}
	if err := engine.AddDimension(dim); err != nil {
		t.Errorf("AddDimension failed: %v", err)
	}
}

func TestAPIFindBestMatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a rule
	rule := NewRule("best-match-test").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()
	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Find best match
	query := CreateQuery(map[string]string{"region": "us-west"})
	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Errorf("FindBestMatch failed: %v", err)
	}
	if result == nil {
		t.Error("Expected match result")
	}
	if result != nil && result.Rule.ID != "best-match-test" {
		t.Errorf("Expected rule ID 'best-match-test', got '%s'", result.Rule.ID)
	}
}

func TestAPIFindAllMatches(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add multiple rules
	for i := 0; i < 3; i++ {
		rule := NewRule("all-match-test").
			Dimension("region", "us-west", MatchTypeEqual, 1.0+float64(i)*0.1). // Different weights
			Build()
		rule.ID = rule.ID + string(rune('a'+i)) // Make unique IDs
		if err := engine.AddRule(rule); err != nil {
			t.Fatalf("Failed to add rule %d: %v", i, err)
		}
	}

	// Find all matches
	query := CreateQuery(map[string]string{"region": "us-west"})
	results, err := engine.FindAllMatches(query)
	if err != nil {
		t.Errorf("FindAllMatches failed: %v", err)
	}
	if len(results) == 0 {
		t.Error("Expected at least one match")
	}
}

func TestAPIBatchAddRules(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create batch rules
	var rules []*Rule
	for i := 0; i < 5; i++ {
		rule := NewRule("batch-test").
			Dimension("region", "us-west", MatchTypeEqual, 1.0+float64(i)*0.1). // Different weights
			Build()
		rule.ID = rule.ID + string(rune('a'+i)) // Make unique IDs
		rules = append(rules, rule)
	}

	// Batch add rules
	err = engine.BatchAddRules(rules)
	if err != nil {
		t.Errorf("BatchAddRules failed: %v", err)
	}
}

func TestAPIAddSimpleRule(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add simple rule
	dimensions := map[string]string{"region": "us-west", "env": "prod"}
	weights := map[string]float64{"region": 1.0, "env": 0.5}
	manualWeight := 5.0

	err = engine.AddSimpleRule("simple-test", dimensions, weights, &manualWeight)
	if err != nil {
		t.Errorf("AddSimpleRule failed: %v", err)
	}
}

func TestAPIAddAnyRule(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add any rule
	dimensionNames := []string{"region", "env", "service"}
	manualWeight := 10.0

	err = engine.AddAnyRule("any-test", dimensionNames, manualWeight)
	if err != nil {
		t.Errorf("AddAnyRule failed: %v", err)
	}
}

func TestAPIGetStats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Get stats
	stats := engine.GetStats()
	if stats == nil {
		t.Error("GetStats returned nil")
	}
}

func TestAPIValidateRule(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Validate a good rule
	rule := NewRule("validate-test").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	err = engine.ValidateRule(rule)
	if err != nil {
		t.Errorf("ValidateRule failed for valid rule: %v", err)
	}
}

func TestGenerateDefaultNodeID(t *testing.T) {
	// Test that function is publicly accessible
	nodeID := GenerateDefaultNodeID()
	
	// Check that result is not empty
	if nodeID == "" {
		t.Fatal("GenerateDefaultNodeID returned empty string")
	}
	
	// Check format: hostname-6digits
	pattern := regexp.MustCompile(`^.+-\d{6}$`)
	if !pattern.MatchString(nodeID) {
		t.Errorf("NodeID format incorrect. Expected hostname-6digits, got: %s", nodeID)
	}
	
	// Test that it contains hostname
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	
	expectedPrefix := hostname + "-"
	if len(nodeID) < len(expectedPrefix) || nodeID[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("NodeID should start with '%s', got: %s", expectedPrefix, nodeID)
	}
	
	// Test that suffix is exactly 6 digits
	suffix := nodeID[len(expectedPrefix):]
	if len(suffix) != 6 {
		t.Errorf("Suffix should be 6 digits, got %d digits: %s", len(suffix), suffix)
	}
	
	// Test that each call generates different IDs (randomness)
	nodeID2 := GenerateDefaultNodeID()
	if nodeID == nodeID2 {
		t.Log("Warning: Two consecutive calls generated the same nodeID. This is unlikely but possible due to randomness.")
	}
	
	// Test multiple calls to ensure format consistency
	for i := 0; i < 10; i++ {
		testNodeID := GenerateDefaultNodeID()
		if !pattern.MatchString(testNodeID) {
			t.Errorf("Iteration %d: NodeID format incorrect: %s", i, testNodeID)
		}
	}
}
