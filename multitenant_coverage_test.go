package matcher

import (
	"testing"
)

func TestMultiTenantFunctionality(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "multitenant-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test NewRuleWithTenant
	rule := NewRuleWithTenant("test-rule", "tenant1", "app1").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Build()

	if rule.TenantID != "tenant1" {
		t.Errorf("Expected tenant 'tenant1', got '%s'", rule.TenantID)
	}

	if rule.ApplicationID != "app1" {
		t.Errorf("Expected application 'app1', got '%s'", rule.ApplicationID)
	}

	// Test Tenant method
	rule2 := NewRule("test-rule-2").
		Tenant("tenant2").
		Application("app2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		Build()

	if rule2.TenantID != "tenant2" {
		t.Errorf("Expected tenant 'tenant2', got '%s'", rule2.TenantID)
	}

	if rule2.ApplicationID != "app2" {
		t.Errorf("Expected application 'app2', got '%s'", rule2.ApplicationID)
	}

	// Test CreateQueryWithTenant
	query := CreateQueryWithTenant("tenant1", "app1", map[string]string{
		"product": "TestProduct",
	})

	if query.TenantID != "tenant1" {
		t.Errorf("Expected query tenant 'tenant1', got '%s'", query.TenantID)
	}

	if query.ApplicationID != "app1" {
		t.Errorf("Expected query application 'app1', got '%s'", query.ApplicationID)
	}

	// Test CreateQueryWithAllRules
	queryAll := CreateQueryWithAllRules(map[string]string{
		"product": "TestProduct",
	})

	if !queryAll.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	// Test CreateQueryWithAllRulesAndTenant
	queryAllTenant := CreateQueryWithAllRulesAndTenant("tenant3", "app3", map[string]string{
		"product": "TestProduct",
	})

	if queryAllTenant.TenantID != "tenant3" {
		t.Errorf("Expected tenant 'tenant3', got '%s'", queryAllTenant.TenantID)
	}

	if queryAllTenant.ApplicationID != "app3" {
		t.Errorf("Expected application 'app3', got '%s'", queryAllTenant.ApplicationID)
	}

	if !queryAllTenant.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}

	// Test GetTenantContext
	tenantID, appID := rule.GetTenantContext()
	if tenantID != "tenant1" || appID != "app1" {
		t.Errorf("Expected tenant context 'tenant1:app1', got '%s:%s'", tenantID, appID)
	}

	queryTenantID, queryAppID := query.GetTenantContext()
	if queryTenantID != "tenant1" || queryAppID != "app1" {
		t.Errorf("Expected query context 'tenant1:app1', got '%s:%s'", queryTenantID, queryAppID)
	}

	// Test MatchesTenantContext
	if !rule.MatchesTenantContext("tenant1", "app1") {
		t.Error("Expected rule to match query tenant context")
	}

	// Test with different tenant context
	if rule.MatchesTenantContext("other-tenant", "other-app") {
		t.Error("Expected rule not to match different tenant context")
	}
}

func TestRuleStatus(t *testing.T) {
	// Test Status method in RuleBuilder
	rule := NewRule("status-test").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Status(RuleStatusDraft).
		Build()

	if rule.Status != RuleStatusDraft {
		t.Errorf("Expected status %v, got %v", RuleStatusDraft, rule.Status)
	}

	// Test with working status
	rule2 := NewRule("status-test-2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		Status(RuleStatusWorking).
		Build()

	if rule2.Status != RuleStatusWorking {
		t.Errorf("Expected status %v, got %v", RuleStatusWorking, rule2.Status)
	}
}

func TestSetAllowDuplicateWeights(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewMatcherEngine(persistence, nil, "duplicate-weights-test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension config first
	dimConfig := NewDimensionConfig("product", 0, true)
	err = engine.AddDimension(dimConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension config: %v", err)
	}

	// Test SetAllowDuplicateWeights
	engine.SetAllowDuplicateWeights(true)

	// Add two rules with the same weight
	rule1 := NewRule("rule1").
		Dimension("product", "TestProduct", MatchTypeEqual).
		ManualWeight(10.0).
		Build()

	rule2 := NewRule("rule2").
		Dimension("product", "TestProduct2", MatchTypeEqual).
		ManualWeight(10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	// This should succeed because duplicate weights are allowed
	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule2 with duplicate weight: %v", err)
	}

	// Test disabling duplicate weights
	engine.SetAllowDuplicateWeights(false)

	// Add a rule that will intersect with the next one
	rule3_intersect := NewRule("rule3_intersect").
		Dimension("product", "Test", MatchTypePrefix). // This will intersect with prefix "Test"
		ManualWeight(15.0).
		Build()

	err = engine.AddRule(rule3_intersect)
	if err != nil {
		t.Fatalf("Failed to add intersecting rule: %v", err)
	}

	// This rule should conflict because it intersects and has the same weight
	rule3_conflict := NewRule("rule3_conflict").
		Dimension("product", "TestProduct", MatchTypeEqual). // "TestProduct" matches prefix "Test"
		ManualWeight(15.0).                                  // Same weight as rule3_intersect
		Build()

	// This should fail because duplicate weights are not allowed for intersecting rules
	err = engine.AddRule(rule3_conflict)
	if err == nil {
		t.Error("Expected error when adding rule with duplicate weight that intersects")
	}
}

func TestInitializeDimension(t *testing.T) {
	// Create forest with dimension configs
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
	}, nil)

	forest := CreateRuleForest(dimensionConfigs)

	// Test InitializeDimension
	forest.InitializeDimension("region")
	// InitializeDimension doesn't return error, so we just test it doesn't panic

	// Test initializing existing dimension (should not cause error)
	forest.InitializeDimension("product")
	// Again, just test it doesn't panic
}

func TestMatchTypeString(t *testing.T) {
	// Test String method for MatchType
	tests := []struct {
		matchType MatchType
		expected  string
	}{
		{MatchTypeEqual, "equal"},
		{MatchTypePrefix, "prefix"},
		{MatchTypeSuffix, "suffix"},
		{MatchTypeAny, "any"},
		{MatchType(99), "unknown"}, // Test unknown match type
	}

	for _, test := range tests {
		result := test.matchType.String()
		if result != test.expected {
			t.Errorf("Expected %s, got %s for match type %v", test.expected, result, test.matchType)
		}
	}
}
