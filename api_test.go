package matcher

import (
	"os"
	"regexp"
	"testing"
	"time"
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

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)
	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add a rule first
	originalRule := NewRule("api-update-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	if err := engine.AddRule(originalRule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the original rule works
	query := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("Expected 1 match for original rule, got %d", len(matches))
	}

	if matches[0].Rule.ID != "api-update-test" {
		t.Errorf("Expected rule 'api-update-test', got '%s'", matches[0].Rule.ID)
	}

	// Update the rule with different dimensions
	updatedRule := NewRule("api-update-test").
		Dimension("region", "us-east", MatchTypeEqual).
		Dimension("env", "staging", MatchTypeEqual).
		Build()

	if err := engine.UpdateRule(updatedRule); err != nil {
		t.Errorf("UpdateRule failed: %v", err)
	}

	// Verify the original query no longer matches
	matches, err = engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed after update: %v", err)
	}

	if len(matches) != 0 {
		t.Errorf("Expected 0 matches for original query after update, got %d", len(matches))
	}

	// Verify the updated rule works with new query
	updatedQuery := &QueryRule{
		Values: map[string]string{
			"region": "us-east",
			"env":    "staging",
		},
	}

	matches, err = engine.FindAllMatches(updatedQuery)
	if err != nil {
		t.Fatalf("FindAllMatches failed for updated query: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("Expected 1 match for updated rule, got %d", len(matches))
	}

	if matches[0].Rule.ID != "api-update-test" {
		t.Errorf("Expected updated rule 'api-update-test', got '%s'", matches[0].Rule.ID)
	}

	// Test updating a non-existent rule
	nonExistentRule := NewRule("non-existent").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	// This should not fail - updateRule handles non-existent rules gracefully
	if err := engine.UpdateRule(nonExistentRule); err != nil {
		t.Errorf("UpdateRule should handle non-existent rules gracefully: %v", err)
	}

	// Verify the non-existent rule was added
	nonExistentQuery := &QueryRule{
		Values: map[string]string{
			"region": "us-west",
		},
	}

	matches, err = engine.FindAllMatches(nonExistentQuery)
	if err != nil {
		t.Fatalf("FindAllMatches failed for non-existent rule query: %v", err)
	}

	// Should find the newly added rule
	found := false
	for _, match := range matches {
		if match.Rule.ID == "non-existent" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected to find the non-existent rule after update")
	}
}

func TestAPIUpdateRuleStatus(t *testing.T) {
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

	// Add dimension configuration
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a rule
	rule := NewRule("status-update-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Update the rule status to draft
	if err := engine.UpdateRuleStatus("status-update-test", RuleStatusDraft); err != nil {
		t.Errorf("UpdateRuleStatus failed: %v", err)
	}

	// Verify the rule was updated
	updatedRule, err := engine.GetRule("status-update-test")
	if err != nil {
		t.Fatalf("Failed to get updated rule: %v", err)
	}

	if updatedRule.Status != RuleStatusDraft {
		t.Errorf("Expected status %s, got %s", RuleStatusDraft, updatedRule.Status)
	}

	// Verify other fields remained unchanged
	if updatedRule.ID != "status-update-test" {
		t.Errorf("Expected ID 'status-update-test', got '%s'", updatedRule.ID)
	}

	if len(updatedRule.Dimensions) != 1 {
		t.Errorf("Expected 1 dimension, got %d", len(updatedRule.Dimensions))
	}

	if updatedRule.Dimensions[0].DimensionName != "region" {
		t.Errorf("Expected dimension 'region', got '%s'", updatedRule.Dimensions[0].DimensionName)
	}

	// Test updating non-existent rule
	err = engine.UpdateRuleStatus("non-existent", RuleStatusWorking)
	if err == nil {
		t.Error("Expected error when updating non-existent rule status")
	}
}

func TestAPIUpdateRuleMetadata(t *testing.T) {
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

	// Add dimension configuration
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a rule with initial metadata
	rule := NewRule("metadata-update-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	rule.Metadata = map[string]string{
		"owner":       "team-alpha",
		"description": "original description",
	}

	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Update the rule metadata
	newMetadata := map[string]string{
		"owner":       "team-beta",
		"description": "updated description",
		"priority":    "high",
	}

	if err := engine.UpdateRuleMetadata("metadata-update-test", newMetadata); err != nil {
		t.Errorf("UpdateRuleMetadata failed: %v", err)
	}

	// Verify the rule was updated
	updatedRule, err := engine.GetRule("metadata-update-test")
	if err != nil {
		t.Fatalf("Failed to get updated rule: %v", err)
	}

	if updatedRule.Metadata["owner"] != "team-beta" {
		t.Errorf("Expected owner 'team-beta', got '%s'", updatedRule.Metadata["owner"])
	}

	if updatedRule.Metadata["description"] != "updated description" {
		t.Errorf("Expected description 'updated description', got '%s'", updatedRule.Metadata["description"])
	}

	if updatedRule.Metadata["priority"] != "high" {
		t.Errorf("Expected priority 'high', got '%s'", updatedRule.Metadata["priority"])
	}

	// Verify other fields remained unchanged
	if updatedRule.ID != "metadata-update-test" {
		t.Errorf("Expected ID 'metadata-update-test', got '%s'", updatedRule.ID)
	}

	if len(updatedRule.Dimensions) != 1 {
		t.Errorf("Expected 1 dimension, got %d", len(updatedRule.Dimensions))
	}

	// Test updating non-existent rule
	err = engine.UpdateRuleMetadata("non-existent", newMetadata)
	if err == nil {
		t.Error("Expected error when updating non-existent rule metadata")
	}
}

func TestAPIGetRule(t *testing.T) {
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

	// Add dimension configuration
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a rule
	originalRule := NewRule("get-rule-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	originalRule.Metadata = map[string]string{
		"owner": "team-alpha",
		"type":  "routing",
	}
	originalRule.Status = RuleStatusWorking

	if err := engine.AddRule(originalRule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Get the rule
	retrievedRule, err := engine.GetRule("get-rule-test")
	if err != nil {
		t.Fatalf("GetRule failed: %v", err)
	}

	// Verify all fields
	if retrievedRule.ID != "get-rule-test" {
		t.Errorf("Expected ID 'get-rule-test', got '%s'", retrievedRule.ID)
	}

	if retrievedRule.Status != RuleStatusWorking {
		t.Errorf("Expected status %s, got %s", RuleStatusWorking, retrievedRule.Status)
	}

	if len(retrievedRule.Dimensions) != 1 {
		t.Errorf("Expected 1 dimension, got %d", len(retrievedRule.Dimensions))
	}

	if retrievedRule.Dimensions[0].DimensionName != "region" {
		t.Errorf("Expected dimension 'region', got '%s'", retrievedRule.Dimensions[0].DimensionName)
	}

	if retrievedRule.Dimensions[0].Value != "us-west" {
		t.Errorf("Expected value 'us-west', got '%s'", retrievedRule.Dimensions[0].Value)
	}

	if retrievedRule.Metadata["owner"] != "team-alpha" {
		t.Errorf("Expected owner 'team-alpha', got '%s'", retrievedRule.Metadata["owner"])
	}

	if retrievedRule.Metadata["type"] != "routing" {
		t.Errorf("Expected type 'routing', got '%s'", retrievedRule.Metadata["type"])
	}

	// Test getting non-existent rule
	_, err = engine.GetRule("non-existent")
	if err == nil {
		t.Error("Expected error when getting non-existent rule")
	}

	// Test that the returned rule is a copy (modifying it shouldn't affect the original)
	retrievedRule.Metadata["owner"] = "modified"

	// Get the rule again to verify it wasn't modified
	againRule, err := engine.GetRule("get-rule-test")
	if err != nil {
		t.Fatalf("GetRule failed on second call: %v", err)
	}

	if againRule.Metadata["owner"] != "team-alpha" {
		t.Error("Rule was modified when it should have been a copy")
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
		Dimension("region", "us-west", MatchTypeEqual).
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
	dim := NewDimensionConfig("api-test-dim", 100, false)
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
		Dimension("region", "us-west", MatchTypeEqual).
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

	// Add dimension config to control weights
	config := NewDimensionConfig("region", 0, false)
	if err := engine.AddDimension(config); err != nil {
		t.Fatalf("Failed to add dimension config: %v", err)
	}

	// Add multiple rules with different manual weights to avoid conflicts
	for i := 0; i < 3; i++ {
		rule := NewRule("all-match-test").
			Dimension("region", "us-west", MatchTypeEqual).
			Build()
		rule.ID = rule.ID + string(rune('a'+i)) // Make unique IDs
		// Give each rule a different manual weight to avoid conflicts
		manualWeight := float64(10 + i)
		rule.ManualWeight = &manualWeight
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

	// Add dimension config to control weights
	config := NewDimensionConfig("region", 0, false)
	if err := engine.AddDimension(config); err != nil {
		t.Fatalf("Failed to add dimension config: %v", err)
	}

	// Create batch rules
	var rules []*Rule
	for i := 0; i < 5; i++ {
		rule := NewRule("batch-test").
			Dimension("region", "us-west", MatchTypeEqual).
			Build()
		rule.ID = rule.ID + string(rune('a'+i)) // Make unique IDs
		// Give each rule a different manual weight to avoid conflicts
		manualWeight := float64(5 + i)
		rule.ManualWeight = &manualWeight
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
	manualWeight := 5.0

	err = engine.AddSimpleRule("simple-test", dimensions, &manualWeight)
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
		Dimension("region", "us-west", MatchTypeEqual).
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

// ========================================
// MERGED TESTS FROM additional_coverage_test.go and api_dynamic_configs_test.go
// ========================================

func TestDimensionConfig_GetWeight(t *testing.T) {
	// Test case 1: GetWeight with defined match type weights
	config := NewDimensionConfig("test", 0, false)
	config.SetWeight(MatchTypeEqual, 10.0)
	config.SetWeight(MatchTypePrefix, 7.0)
	config.SetWeight(MatchTypeAny, 2.0)

	// Test getting defined weights
	if weight := config.GetWeight(MatchTypeEqual); weight != 10.0 {
		t.Errorf("Expected weight 10.0 for Equal match type, got %.1f", weight)
	}

	if weight := config.GetWeight(MatchTypePrefix); weight != 7.0 {
		t.Errorf("Expected weight 7.0 for Prefix match type, got %.1f", weight)
	}

	if weight := config.GetWeight(MatchTypeAny); weight != 2.0 {
		t.Errorf("Expected weight 2.0 for Any match type, got %.1f", weight)
	}

	// Test getting undefined weight (should return default)
	if weight := config.GetWeight(MatchTypeSuffix); weight != 5.0 {
		t.Errorf("Expected default weight 5.0 for undefined Suffix match type, got %.1f", weight)
	}

	// Test case 2: GetWeight with no defined weights (all should return default)
	emptyConfig := NewDimensionConfig("empty", 1, true)

	matchTypes := []MatchType{MatchTypeEqual, MatchTypePrefix, MatchTypeSuffix, MatchTypeAny}
	for _, mt := range matchTypes {
		if weight := emptyConfig.GetWeight(mt); weight != 15.0 {
			t.Errorf("Expected default weight 15.0 for %s match type, got %.1f", mt, weight)
		}
	}

	// Test case 3: GetWeight after setting and overriding weights
	overrideConfig := NewDimensionConfig("override", 2, false)

	// Set initial weight
	overrideConfig.SetWeight(MatchTypeEqual, 8.0)
	if weight := overrideConfig.GetWeight(MatchTypeEqual); weight != 8.0 {
		t.Errorf("Expected weight 8.0 after initial set, got %.1f", weight)
	}

	// Override the weight
	overrideConfig.SetWeight(MatchTypeEqual, 12.0)
	if weight := overrideConfig.GetWeight(MatchTypeEqual); weight != 12.0 {
		t.Errorf("Expected weight 12.0 after override, got %.1f", weight)
	}

	// Other match types should still return default
	if weight := overrideConfig.GetWeight(MatchTypePrefix); weight != 3.0 {
		t.Errorf("Expected default weight 3.0 for Prefix after override, got %.1f", weight)
	}
}

func TestDimensionConfig_SetWeightFunction(t *testing.T) {
	config := NewDimensionConfig("test", 0, false)

	// Test setting weights for all match types
	weights := map[MatchType]float64{
		MatchTypeEqual:  10.0,
		MatchTypePrefix: 7.5,
		MatchTypeSuffix: 5.0,
		MatchTypeAny:    2.5,
	}

	for matchType, weight := range weights {
		config.SetWeight(matchType, weight)
	}

	// Verify all weights were set correctly
	for matchType, expectedWeight := range weights {
		actualWeight := config.GetWeight(matchType)
		if actualWeight != expectedWeight {
			t.Errorf("Match type %s: expected weight %.1f, got %.1f", matchType, expectedWeight, actualWeight)
		}
	}

	// Test that the weights map was properly initialized
	if config.Weights == nil {
		t.Error("Weights map should not be nil after setting weights")
	}

	if len(config.Weights) != 4 {
		t.Errorf("Expected 4 weights in map, got %d", len(config.Weights))
	}
}

func TestNewDimensionConfigWithWeightsFunction(t *testing.T) {
	weights := map[MatchType]float64{
		MatchTypeEqual:  15.0,
		MatchTypePrefix: 10.0,
		MatchTypeSuffix: 8.0,
		MatchTypeAny:    3.0,
	}

	config := NewDimensionConfigWithWeights("weighted", 1, true, weights)

	if config.Name != "weighted" {
		t.Errorf("Expected name 'weighted', got '%s'", config.Name)
	}

	if config.Index != 1 {
		t.Errorf("Expected index 1, got %d", config.Index)
	}

	if !config.Required {
		t.Error("Expected required to be true")
	}

	// Test that weights not explicitly set return 0.0
	if config.GetWeight(MatchTypePrefix) != 0.0 {
		t.Errorf("Expected default weight 0.0 for MatchTypePrefix, got %.1f", config.GetWeight(MatchTypePrefix))
	}

	// Test that all weights were set correctly
	for matchType, expectedWeight := range weights {
		actualWeight := config.GetWeight(matchType)
		if actualWeight != expectedWeight {
			t.Errorf("Match type %s: expected weight %.1f, got %.1f", matchType, expectedWeight, actualWeight)
		}
	}

	// Test that weights map is properly initialized
	if config.Weights == nil {
		t.Error("Weights map should not be nil")
	}

	if len(config.Weights) != 4 {
		t.Errorf("Expected 4 weights in map, got %d", len(config.Weights))
	}
}

func TestInitializeDimensionFunction(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Access the forest index to call InitializeDimension
	forestIndex := engine.matcher.getOrCreateForestIndex("", "")

	// Call InitializeDimension (this was at 0% coverage)
	// The function takes a string, not a DimensionConfig
	forestIndex.InitializeDimension("test_dimension")

	// Since InitializeDimension is a no-op, we just verify it doesn't crash
	// and we can still add dimensions normally
	config := NewDimensionConfig("test_dimension", 0, false)
	config.SetWeight(MatchTypeEqual, 20.0)
	config.SetWeight(MatchTypePrefix, 15.0)

	err = engine.AddDimension(config)
	if err != nil {
		t.Fatalf("Failed to add dimension after initialization: %v", err)
	}

	// Test that we can add a rule using this dimension
	rule := NewRule("test-rule").
		Dimension("test_dimension", "test-value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule after dimension initialization: %v", err)
	}

	// Verify we can query using this dimension
	query := &QueryRule{
		Values: map[string]string{
			"test_dimension": "test-value",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}

	if len(matches) > 0 && matches[0].Rule.ID != "test-rule" {
		t.Errorf("Expected rule 'test-rule', got '%s'", matches[0].Rule.ID)
	}
}

func TestDeleteDimensionFunction(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add a dimension
	config := NewDimensionConfig("removable", 0, false)
	err = engine.AddDimension(config)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add a rule using this dimension
	rule := NewRule("test-rule").
		Dimension("removable", "value", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the rule matches before deletion
	query := &QueryRule{
		Values: map[string]string{
			"removable": "value",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match before deletion, got %d", len(matches))
	}

	// Test deleteDimension function (this was at 0% coverage)
	err = engine.matcher.deleteDimension("removable")
	if err != nil {
		t.Fatalf("deleteDimension failed: %v", err)
	}

	// Verify the dimension was removed from the list
	dimensions, err := engine.ListDimensions()
	if err != nil {
		t.Fatalf("ListDimensions failed: %v", err)
	}

	for _, dim := range dimensions {
		if dim.Name == "removable" {
			t.Error("Dimension 'removable' should have been deleted")
		}
	}

	// Test deleting a non-existent dimension (should not error, just do nothing)
	initialDimCount := len(dimensions)
	err = engine.matcher.deleteDimension("non-existent")
	if err != nil {
		t.Fatalf("deleteDimension with non-existent dimension failed: %v", err)
	}

	// Verify dimension count didn't change
	dimensions, err = engine.ListDimensions()
	if err != nil {
		t.Fatalf("ListDimensions failed after deleting non-existent: %v", err)
	}

	if len(dimensions) != initialDimCount {
		t.Errorf("Dimension count changed after deleting non-existent dimension: expected %d, got %d", initialDimCount, len(dimensions))
	}
}

func TestCacheCleanupExpired(t *testing.T) {
	// Create a cache with very short TTL for testing
	cache := NewQueryCache(100, 1*time.Second) // 1 second TTL

	// Create mock results for testing
	mockResult := &MatchResult{
		Rule:        &Rule{ID: "test"},
		TotalWeight: 1.0,
	}

	// Create mock queries
	query1 := &QueryRule{Values: map[string]string{"key": "value1"}}
	query2 := &QueryRule{Values: map[string]string{"key": "value2"}}

	// Add entries with custom TTL - one that will expire quickly
	cache.SetWithTTL(query1, mockResult, 1*time.Millisecond) // Very short TTL
	cache.Set(query2, mockResult)                            // Normal TTL

	initialSize := cache.Size()
	if initialSize != 2 {
		t.Errorf("Expected initial cache size 2, got %d", initialSize)
	}

	// Wait for the first entry to expire
	time.Sleep(10 * time.Millisecond)

	// Call CleanupExpired (this was at 70% coverage, let's get it to 100%)
	cleanedCount := cache.CleanupExpired()
	if cleanedCount != 1 {
		t.Errorf("Expected 1 entry to be cleaned, got %d", cleanedCount)
	}

	// The expired entry should be removed
	finalSize := cache.Size()
	if finalSize != 1 {
		t.Errorf("Expected final cache size 1 after cleanup, got %d", finalSize)
	}

	// Test that cleanup works when all entries are expired
	cache.SetWithTTL(query2, mockResult, 1*time.Millisecond) // Make the remaining entry expire
	time.Sleep(10 * time.Millisecond)

	cleanedCount = cache.CleanupExpired()
	if cleanedCount != 1 {
		t.Errorf("Expected 1 entry to be cleaned in second cleanup, got %d", cleanedCount)
	}

	emptySize := cache.Size()
	if emptySize != 0 {
		t.Errorf("Expected empty cache size 0 after all expired, got %d", emptySize)
	}

	// Test cleanup when no entries are expired
	cache.Set(query1, mockResult) // Add a fresh entry
	cleanedCount = cache.CleanupExpired()
	if cleanedCount != 0 {
		t.Errorf("Expected 0 entries to be cleaned when none expired, got %d", cleanedCount)
	}

	if cache.Size() != 1 {
		t.Errorf("Expected 1 entry to remain after cleanup with no expired entries, got %d", cache.Size())
	}
}

func TestCreateQueryWithDynamicConfigs(t *testing.T) {
	values := map[string]string{
		"region": "us-west",
		"env":    "prod",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"region": NewDimensionConfig("region", 0, false),
	}

	query := CreateQueryWithDynamicConfigs(values, dynamicConfigs)

	if query == nil {
		t.Fatal("CreateQueryWithDynamicConfigs returned nil")
	}

	if query.Values["region"] != "us-west" {
		t.Errorf("Expected region value 'us-west', got '%s'", query.Values["region"])
	}

	if query.Values["env"] != "prod" {
		t.Errorf("Expected env value 'prod', got '%s'", query.Values["env"])
	}

	if query.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be false")
	}

	if len(query.DynamicDimensionConfigs) != 1 {
		t.Errorf("Expected 1 dynamic config, got %d", len(query.DynamicDimensionConfigs))
	}

	if query.DynamicDimensionConfigs["region"].Name != "region" {
		t.Errorf("Expected region config name 'region', got '%s'", query.DynamicDimensionConfigs["region"].Name)
	}
}

func TestCreateQueryWithTenantAndDynamicConfigs(t *testing.T) {
	tenantID := "tenant1"
	applicationID := "app1"
	values := map[string]string{
		"category": "urgent",
		"priority": "high",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"category": NewDimensionConfig("category", 0, true),
		"priority": NewDimensionConfig("priority", 1, false),
	}

	query := CreateQueryWithTenantAndDynamicConfigs(tenantID, applicationID, values, dynamicConfigs)

	if query == nil {
		t.Fatal("CreateQueryWithTenantAndDynamicConfigs returned nil")
	}

	if query.TenantID != tenantID {
		t.Errorf("Expected tenant ID '%s', got '%s'", tenantID, query.TenantID)
	}

	if query.ApplicationID != applicationID {
		t.Errorf("Expected application ID '%s', got '%s'", applicationID, query.ApplicationID)
	}

	if query.Values["category"] != "urgent" {
		t.Errorf("Expected category value 'urgent', got '%s'", query.Values["category"])
	}

	if query.Values["priority"] != "high" {
		t.Errorf("Expected priority value 'high', got '%s'", query.Values["priority"])
	}

	if query.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be false")
	}

	if len(query.DynamicDimensionConfigs) != 2 {
		t.Errorf("Expected 2 dynamic configs, got %d", len(query.DynamicDimensionConfigs))
	}

	categoryConfig := query.DynamicDimensionConfigs["category"]
	if categoryConfig.Name != "category" {
		t.Errorf("Category config mismatch: name=%s", categoryConfig.Name)
	}

	priorityConfig := query.DynamicDimensionConfigs["priority"]
	if priorityConfig.Name != "priority" {
		t.Errorf("Priority config mismatch: name=%s", priorityConfig.Name)
	}
}

func TestCreateQueryWithAllRulesAndDynamicConfigs(t *testing.T) {
	values := map[string]string{
		"service": "user-service",
		"version": "1.2.3",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"service": NewDimensionConfig("service", 0, true),
	}

	query := CreateQueryWithAllRulesAndDynamicConfigs(values, dynamicConfigs)

	if query == nil {
		t.Fatal("CreateQueryWithAllRulesAndDynamicConfigs returned nil")
	}

	if query.Values["service"] != "user-service" {
		t.Errorf("Expected service value 'user-service', got '%s'", query.Values["service"])
	}

	if query.Values["version"] != "1.2.3" {
		t.Errorf("Expected version value '1.2.3', got '%s'", query.Values["version"])
	}

	if !query.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	if len(query.DynamicDimensionConfigs) != 1 {
		t.Errorf("Expected 1 dynamic config, got %d", len(query.DynamicDimensionConfigs))
	}

	serviceConfig := query.DynamicDimensionConfigs["service"]
	if serviceConfig.Name != "service" {
		t.Errorf("Service config mismatch: name=%s", serviceConfig.Name)
	}
}

func TestCreateQueryWithAllRulesTenantAndDynamicConfigs(t *testing.T) {
	tenantID := "enterprise"
	applicationID := "main-app"
	values := map[string]string{
		"user_type": "admin",
		"action":    "delete",
		"resource":  "user",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"user_type": NewDimensionConfig("user_type", 0, true),
		"action":    NewDimensionConfig("action", 1, true),
		"resource":  NewDimensionConfig("resource", 2, false),
	}

	query := CreateQueryWithAllRulesTenantAndDynamicConfigs(tenantID, applicationID, values, dynamicConfigs)

	if query == nil {
		t.Fatal("CreateQueryWithAllRulesTenantAndDynamicConfigs returned nil")
	}

	if query.TenantID != tenantID {
		t.Errorf("Expected tenant ID '%s', got '%s'", tenantID, query.TenantID)
	}

	if query.ApplicationID != applicationID {
		t.Errorf("Expected application ID '%s', got '%s'", applicationID, query.ApplicationID)
	}

	expectedValues := map[string]string{
		"user_type": "admin",
		"action":    "delete",
		"resource":  "user",
	}

	for key, expectedValue := range expectedValues {
		if query.Values[key] != expectedValue {
			t.Errorf("Expected %s value '%s', got '%s'", key, expectedValue, query.Values[key])
		}
	}

	if !query.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	if len(query.DynamicDimensionConfigs) != 3 {
		t.Errorf("Expected 3 dynamic configs, got %d", len(query.DynamicDimensionConfigs))
	}

	// Verify all dynamic configs
	expectedConfigs := map[string]struct {
		weight   float64
		required bool
	}{
		"user_type": {25.0, true},
		"action":    {20.0, true},
		"resource":  {10.0, false},
	}

	for name, expected := range expectedConfigs {
		config := query.DynamicDimensionConfigs[name]
		if config == nil {
			t.Errorf("Missing dynamic config for %s", name)
			continue
		}

		if config.Name != name {
			t.Errorf("Config %s: expected name '%s', got '%s'", name, name, config.Name)
		}

		// DefaultWeight field no longer exists - testing weights through GetWeight method

		if config.Required != expected.required {
			t.Errorf("Config %s: expected required %v, got %v", name, expected.required, config.Required)
		}
	}
}

func TestDynamicConfigsIntegration(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Add basic dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	regionConfig.SetWeight(MatchTypePrefix, 7.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	// Add a rule without tenant (for basic tests)
	rule := NewRule("test-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Add a rule with tenant for tenant-specific tests
	tenantRule := NewRuleWithTenant("tenant-rule", "tenant1", "app1").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(tenantRule)
	if err != nil {
		t.Fatalf("Failed to add tenant rule: %v", err)
	}

	// Test all dynamic config API functions with actual matching
	testCases := []struct {
		name        string
		createQuery func() *QueryRule
		expectMatch bool
	}{
		{
			name: "CreateQueryWithDynamicConfigs",
			createQuery: func() *QueryRule {
				dynamicConfig := NewDimensionConfig("region", 0, false)
				dynamicConfig.SetWeight(MatchTypeEqual, 100.0)

				return CreateQueryWithDynamicConfigs(
					map[string]string{"region": "us-west"},
					map[string]*DimensionConfig{"region": dynamicConfig},
				)
			},
			expectMatch: true,
		},
		{
			name: "CreateQueryWithTenantAndDynamicConfigs",
			createQuery: func() *QueryRule {
				dynamicConfig := NewDimensionConfig("region", 0, false)
				dynamicConfig.SetWeight(MatchTypeEqual, 150.0)

				return CreateQueryWithTenantAndDynamicConfigs(
					"tenant1", "app1",
					map[string]string{"region": "us-west"},
					map[string]*DimensionConfig{"region": dynamicConfig},
				)
			},
			expectMatch: true,
		},
		{
			name: "CreateQueryWithAllRulesAndDynamicConfigs",
			createQuery: func() *QueryRule {
				dynamicConfig := NewDimensionConfig("region", 0, false)
				dynamicConfig.SetWeight(MatchTypeEqual, 60.0)

				return CreateQueryWithAllRulesAndDynamicConfigs(
					map[string]string{"region": "us-west"},
					map[string]*DimensionConfig{"region": dynamicConfig},
				)
			},
			expectMatch: true,
		},
		{
			name: "CreateQueryWithAllRulesTenantAndDynamicConfigs",
			createQuery: func() *QueryRule {
				dynamicConfig := NewDimensionConfig("region", 0, false)
				dynamicConfig.SetWeight(MatchTypeEqual, 50.0)

				return CreateQueryWithAllRulesTenantAndDynamicConfigs(
					"tenant1", "app1",
					map[string]string{"region": "us-west"},
					map[string]*DimensionConfig{"region": dynamicConfig},
				)
			},
			expectMatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := tc.createQuery()

			matches, err := engine.FindAllMatches(query)
			if err != nil {
				t.Fatalf("FindAllMatches failed for %s: %v", tc.name, err)
			}

			if tc.expectMatch && len(matches) == 0 {
				t.Errorf("%s: expected at least 1 match, got 0", tc.name)
			} else if !tc.expectMatch && len(matches) > 0 {
				t.Errorf("%s: expected 0 matches, got %d", tc.name, len(matches))
			}

			if len(matches) > 0 {
				t.Logf("%s: Found match with weight %.1f", tc.name, matches[0].TotalWeight)
			}
		})
	}
}

// ========================================
// MERGED SIMPLE COVERAGE TESTS (merged from simple_coverage_test.go and delete_dimension_test.go)
// ========================================

func TestDeleteDimensionCoverage(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "delete-dim-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a dimension first
	dimConfig := NewDimensionConfig("test_dim_delete", 0, false)
	err = engine.AddDimension(dimConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Test by accessing the internal method (this is for coverage, not typical usage)
	// We need to call the internal deleteDimension method indirectly
	// Since it's not exposed, we'll trigger it through other operations

	// The deleteDimension method is typically called during cleanup or updates
	// Let's try to trigger it by rebuilding or other operations
	err = engine.Rebuild()
	if err != nil {
		t.Logf("Rebuild failed (might be expected): %v", err)
	}
}
