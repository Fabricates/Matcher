package matcher

import (
	"os"
	"testing"
)

func TestRuleStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-status-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a working rule
	workingRule := NewRule("working-rule").
		Dimension("product", "ProductA", MatchTypeEqual, 10.0).
		Status(RuleStatusWorking).
		Build()
	if err := engine.AddRule(workingRule); err != nil {
		t.Fatalf("Failed to add working rule: %v", err)
	}

	// Add a draft rule
	draftRule := NewRule("draft-rule").
		Dimension("product", "ProductA", MatchTypeEqual, 15.0).
		Status(RuleStatusDraft).
		Build()
	if err := engine.AddRule(draftRule); err != nil {
		t.Fatalf("Failed to add draft rule: %v", err)
	}

	// Test default query (should only find working rules)
	query := CreateQuery(map[string]string{"product": "ProductA"})
	results, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("Failed to find matches: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result (working rule only), got %d", len(results))
	}
	if results[0].Rule.ID != "working-rule" {
		t.Fatalf("Expected working-rule, got %s", results[0].Rule.ID)
	}

	// Test query with all rules (should find both working and draft rules)
	queryAll := CreateQueryWithAllRules(map[string]string{"product": "ProductA"})
	resultsAll, err := engine.FindAllMatches(queryAll)
	if err != nil {
		t.Fatalf("Failed to find all matches: %v", err)
	}
	if len(resultsAll) != 2 {
		t.Fatalf("Expected 2 results (working and draft), got %d", len(resultsAll))
	}

	// Verify both rules are found
	ruleIDs := make(map[string]bool)
	for _, result := range resultsAll {
		ruleIDs[result.Rule.ID] = true
	}
	if !ruleIDs["working-rule"] || !ruleIDs["draft-rule"] {
		t.Fatalf("Expected both working-rule and draft-rule in results")
	}
}

func TestRuleDefaultStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-default-status-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a rule without explicitly setting status
	rule := NewRule("default-status-rule").
		Dimension("product", "ProductB", MatchTypeEqual, 10.0).
		Build()
	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Verify the rule has working status by default
	if rule.Status != RuleStatusWorking {
		t.Fatalf("Expected default status to be %s, got %s", RuleStatusWorking, rule.Status)
	}

	// Verify the rule is found in default queries
	query := CreateQuery(map[string]string{"product": "ProductB"})
	results, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("Failed to find matches: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Rule.ID != "default-status-rule" {
		t.Fatalf("Expected default-status-rule, got %s", results[0].Rule.ID)
	}
}

func TestFindBestMatchWithStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "matcher-best-match-status-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a working rule with lower weight
	workingRule := NewRule("working-rule-lower").
		Dimension("product", "ProductC", MatchTypeEqual, 10.0).
		Status(RuleStatusWorking).
		Build()
	if err := engine.AddRule(workingRule); err != nil {
		t.Fatalf("Failed to add working rule: %v", err)
	}

	// Add a draft rule with higher weight
	draftRule := NewRule("draft-rule-higher").
		Dimension("product", "ProductC", MatchTypeEqual, 20.0).
		Status(RuleStatusDraft).
		Build()
	if err := engine.AddRule(draftRule); err != nil {
		t.Fatalf("Failed to add draft rule: %v", err)
	}

	// Test FindBestMatch with default query (should find working rule despite lower weight)
	query := CreateQuery(map[string]string{"product": "ProductC"})
	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Failed to find best match: %v", err)
	}
	if result == nil {
		t.Fatalf("Expected result, got nil")
	}
	if result.Rule.ID != "working-rule-lower" {
		t.Fatalf("Expected working-rule-lower, got %s", result.Rule.ID)
	}

	// Test FindBestMatch with all rules (should find draft rule with higher weight)
	queryAll := CreateQueryWithAllRules(map[string]string{"product": "ProductC"})
	resultAll, err := engine.FindBestMatch(queryAll)
	if err != nil {
		t.Fatalf("Failed to find best match with all rules: %v", err)
	}
	if resultAll == nil {
		t.Fatalf("Expected result, got nil")
	}
	if resultAll.Rule.ID != "draft-rule-higher" {
		t.Fatalf("Expected draft-rule-higher, got %s", resultAll.Rule.ID)
	}
}

func TestQueryRuleIncludeAllRulesField(t *testing.T) {
	// Test that QueryRule can be manually constructed with IncludeAllRules field
	query1 := &QueryRule{
		// Values: map[string]string{"product": "ProductD"},
		IncludeAllRules: false,
	}
	if query1.IncludeAllRules != false {
		t.Fatalf("Expected IncludeAllRules to be false, got %v", query1.IncludeAllRules)
	}

	query2 := &QueryRule{
		// Values: map[string]string{"product": "ProductD"},
		IncludeAllRules: true,
	}
	if query2.IncludeAllRules != true {
		t.Fatalf("Expected IncludeAllRules to be true, got %v", query2.IncludeAllRules)
	}
}
