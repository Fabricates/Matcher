package matcher

import (
	"testing"
)

func TestMultiTenantBasicFunctionality(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-multitenant")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configs for different tenants
	config1 := &DimensionConfig{
		Name:          "product",
		Index:         0,
		Required:      true,
		Weight:        10.0,
		TenantID:      "tenant1",
		ApplicationID: "app1",
	}

	config2 := &DimensionConfig{
		Name:          "product",
		Index:         0,
		Required:      true,
		Weight:        10.0,
		TenantID:      "tenant2",
		ApplicationID: "app1",
	}

	err = engine.AddDimension(config1)
	if err != nil {
		t.Fatalf("Failed to add dimension for tenant1: %v", err)
	}

	err = engine.AddDimension(config2)
	if err != nil {
		t.Fatalf("Failed to add dimension for tenant2: %v", err)
	}

	// Add rules for different tenants
	rule1 := NewRuleWithTenant("rule1", "tenant1", "app1").
		Dimension("product", "ProductA", MatchTypeEqual, 10.0).
		Build()

	rule2 := NewRuleWithTenant("rule2", "tenant2", "app1").
		Dimension("product", "ProductA", MatchTypeEqual, 10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule for tenant1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule for tenant2: %v", err)
	}

	// Query tenant1 - should only find rule1
	query1 := CreateQueryWithTenant("tenant1", "app1", map[string]string{
		"product": "ProductA",
	})

	result1, err := engine.FindBestMatch(query1)
	if err != nil {
		t.Fatalf("Query for tenant1 failed: %v", err)
	}

	if result1 == nil {
		t.Fatal("Expected match for tenant1 but got none")
	}

	if result1.Rule.ID != "rule1" {
		t.Errorf("Expected rule1 for tenant1, got %s", result1.Rule.ID)
	}

	// Query tenant2 - should only find rule2
	query2 := CreateQueryWithTenant("tenant2", "app1", map[string]string{
		"product": "ProductA",
	})

	result2, err := engine.FindBestMatch(query2)
	if err != nil {
		t.Fatalf("Query for tenant2 failed: %v", err)
	}

	if result2 == nil {
		t.Fatal("Expected match for tenant2 but got none")
	}

	if result2.Rule.ID != "rule2" {
		t.Errorf("Expected rule2 for tenant2, got %s", result2.Rule.ID)
	}

	// Query without tenant context - should find no results
	queryNoTenant := CreateQuery(map[string]string{
		"product": "ProductA",
	})

	resultNoTenant, err := engine.FindBestMatch(queryNoTenant)
	if err != nil {
		t.Fatalf("Query without tenant failed: %v", err)
	}

	if resultNoTenant != nil {
		t.Errorf("Expected no match for query without tenant context, but got rule %s", resultNoTenant.Rule.ID)
	}
}

func TestMultiApplicationFunctionality(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-multiapp")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add dimension configs for different applications within same tenant
	config1 := &DimensionConfig{
		Name:          "service",
		Index:         0,
		Required:      true,
		Weight:        10.0,
		TenantID:      "tenant1",
		ApplicationID: "app1",
	}

	config2 := &DimensionConfig{
		Name:          "service",
		Index:         0,
		Required:      true,
		Weight:        10.0,
		TenantID:      "tenant1",
		ApplicationID: "app2",
	}

	err = engine.AddDimension(config1)
	if err != nil {
		t.Fatalf("Failed to add dimension for app1: %v", err)
	}

	err = engine.AddDimension(config2)
	if err != nil {
		t.Fatalf("Failed to add dimension for app2: %v", err)
	}

	// Add rules for different applications
	rule1 := NewRuleWithTenant("app1_rule", "tenant1", "app1").
		Dimension("service", "auth", MatchTypeEqual, 10.0).
		Build()

	rule2 := NewRuleWithTenant("app2_rule", "tenant1", "app2").
		Dimension("service", "auth", MatchTypeEqual, 10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule for app1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule for app2: %v", err)
	}

	// Query app1 - should only find app1_rule
	query1 := CreateQueryWithTenant("tenant1", "app1", map[string]string{
		"service": "auth",
	})

	result1, err := engine.FindBestMatch(query1)
	if err != nil {
		t.Fatalf("Query for app1 failed: %v", err)
	}

	if result1 == nil {
		t.Fatal("Expected match for app1 but got none")
	}

	if result1.Rule.ID != "app1_rule" {
		t.Errorf("Expected app1_rule, got %s", result1.Rule.ID)
	}

	// Query app2 - should only find app2_rule
	query2 := CreateQueryWithTenant("tenant1", "app2", map[string]string{
		"service": "auth",
	})

	result2, err := engine.FindBestMatch(query2)
	if err != nil {
		t.Fatalf("Query for app2 failed: %v", err)
	}

	if result2 == nil {
		t.Fatal("Expected match for app2 but got none")
	}

	if result2.Rule.ID != "app2_rule" {
		t.Errorf("Expected app2_rule, got %s", result2.Rule.ID)
	}
}

func TestBackwardCompatibility(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-compat")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add global dimension config (no tenant/app specified)
	config := &DimensionConfig{
		Name:     "region",
		Index:    0,
		Required: true,
		Weight:   10.0,
		// TenantID and ApplicationID are empty for global config
	}

	err = engine.AddDimension(config)
	if err != nil {
		t.Fatalf("Failed to add global dimension: %v", err)
	}

	// Add rule without tenant/app (backward compatibility)
	rule := NewRule("global_rule").
		Dimension("region", "us-west", MatchTypeEqual, 10.0).
		Build()

	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add global rule: %v", err)
	}

	// Query without tenant context should work (backward compatibility)
	query := CreateQuery(map[string]string{
		"region": "us-west",
	})

	result, err := engine.FindBestMatch(query)
	if err != nil {
		t.Fatalf("Global query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected match for global query but got none")
	}

	if result.Rule.ID != "global_rule" {
		t.Errorf("Expected global_rule, got %s", result.Rule.ID)
	}

	// Verify that tenant-scoped queries don't find global rules by default
	tenantQuery := CreateQueryWithTenant("tenant1", "app1", map[string]string{
		"region": "us-west",
	})

	tenantResult, err := engine.FindBestMatch(tenantQuery)
	if err != nil {
		t.Fatalf("Tenant query failed: %v", err)
	}

	if tenantResult != nil {
		t.Errorf("Expected no match for tenant query against global rule, but got %s", tenantResult.Rule.ID)
	}
}

func TestTenantIsolation(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-isolation")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add rules for multiple tenants with unique IDs but similar content
	rule1 := NewRuleWithTenant("tenant1_rule", "tenant1", "app1").
		Dimension("environment", "prod", MatchTypeEqual, 10.0).
		Build()

	rule2 := NewRuleWithTenant("tenant2_rule", "tenant2", "app1").
		Dimension("environment", "prod", MatchTypeEqual, 15.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule for tenant1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule for tenant2: %v", err)
	}

	// Both rules should exist in global tracking
	if len(engine.rules) != 2 {
		t.Errorf("Expected 2 rules in global tracking, got %d", len(engine.rules))
	}

	// But they should be isolated by tenant
	tenant1Rules := engine.tenantRules[engine.getTenantKey("tenant1", "app1")]
	tenant2Rules := engine.tenantRules[engine.getTenantKey("tenant2", "app1")]

	if len(tenant1Rules) != 1 {
		t.Errorf("Expected 1 rule for tenant1, got %d", len(tenant1Rules))
	}

	if len(tenant2Rules) != 1 {
		t.Errorf("Expected 1 rule for tenant2, got %d", len(tenant2Rules))
	}

	// Delete rule from tenant1
	err = engine.DeleteRule("tenant1_rule")
	if err != nil {
		t.Fatalf("Failed to delete rule: %v", err)
	}

	// Only one rule should remain
	if len(engine.rules) != 1 {
		t.Errorf("Expected 1 rule after deletion, got %d", len(engine.rules))
	}

	// Remaining rule should be from tenant2
	remaining := engine.rules["tenant2_rule"]
	if remaining == nil {
		t.Fatal("Expected tenant2_rule to remain after deletion")
	}
	if remaining.TenantID != "tenant2" {
		t.Errorf("Expected remaining rule to be from tenant2, got %s", remaining.TenantID)
	}
}

func TestMultiTenantStats(t *testing.T) {
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-stats")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add rules for multiple tenants
	rule1 := NewRuleWithTenant("rule1", "tenant1", "app1").
		Dimension("type", "A", MatchTypeEqual, 10.0).
		Build()

	rule2 := NewRuleWithTenant("rule2", "tenant1", "app2").
		Dimension("type", "B", MatchTypeEqual, 10.0).
		Build()

	rule3 := NewRuleWithTenant("rule3", "tenant2", "app1").
		Dimension("type", "C", MatchTypeEqual, 10.0).
		Build()

	err = engine.AddRule(rule1)
	if err != nil {
		t.Fatalf("Failed to add rule1: %v", err)
	}

	err = engine.AddRule(rule2)
	if err != nil {
		t.Fatalf("Failed to add rule2: %v", err)
	}

	err = engine.AddRule(rule3)
	if err != nil {
		t.Fatalf("Failed to add rule3: %v", err)
	}

	// Check global stats
	stats := engine.GetStats()
	if stats.TotalRules != 3 {
		t.Errorf("Expected 3 total rules, got %d", stats.TotalRules)
	}

	// Check forest stats
	me := &MatcherEngine{matcher: engine}
	forestStats := me.GetForestStats()

	totalForests, ok := forestStats["total_forests"].(int)
	if !ok || totalForests != 3 {
		t.Errorf("Expected 3 forests, got %v", forestStats["total_forests"])
	}

	totalRules, ok := forestStats["total_rules"].(int)
	if !ok || totalRules != 3 {
		t.Errorf("Expected 3 total rules in forest stats, got %v", forestStats["total_rules"])
	}

	// Each tenant/app combination should have its own forest
	expectedKeys := []string{
		"tenant1:app1",
		"tenant1:app2",
		"tenant2:app1",
	}

	for _, key := range expectedKeys {
		if _, exists := forestStats[key]; !exists {
			t.Errorf("Expected forest stats for key %s, but not found", key)
		}
	}
}
