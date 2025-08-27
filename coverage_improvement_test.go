package matcher

import (
	"context"
	"os"
	"testing"
)

// TestRuleBuilderTenantMethods tests the uncovered Tenant and Application methods
func TestRuleBuilderTenantMethods(t *testing.T) {
	// Test Tenant method
	rule := NewRule("tenant-test").
		Tenant("test-tenant").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	if rule.TenantID != "test-tenant" {
		t.Errorf("Expected TenantID 'test-tenant', got '%s'", rule.TenantID)
	}

	// Test Application method
	rule2 := NewRule("app-test").
		Application("test-app").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	if rule2.ApplicationID != "test-app" {
		t.Errorf("Expected ApplicationID 'test-app', got '%s'", rule2.ApplicationID)
	}

	// Test both together
	rule3 := NewRule("both-test").
		Tenant("my-tenant").
		Application("my-app").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	if rule3.TenantID != "my-tenant" {
		t.Errorf("Expected TenantID 'my-tenant', got '%s'", rule3.TenantID)
	}
	if rule3.ApplicationID != "my-app" {
		t.Errorf("Expected ApplicationID 'my-app', got '%s'", rule3.ApplicationID)
	}
}

// TestCreateQueryWithAllRulesAndTenant tests the uncovered query creation function
func TestCreateQueryWithAllRulesAndTenant(t *testing.T) {
	values := map[string]string{
		"region": "us-west",
		"env":    "prod",
	}

	query := CreateQueryWithAllRulesAndTenant("test-tenant", "test-app", values)

	if query.TenantID != "test-tenant" {
		t.Errorf("Expected TenantID 'test-tenant', got '%s'", query.TenantID)
	}
	if query.ApplicationID != "test-app" {
		t.Errorf("Expected ApplicationID 'test-app', got '%s'", query.ApplicationID)
	}
	if !query.IncludeAllRules {
		t.Error("Expected IncludeAllRules to be true")
	}
	if len(query.Values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(query.Values))
	}
	if query.Values["region"] != "us-west" {
		t.Errorf("Expected region 'us-west', got '%s'", query.Values["region"])
	}
	if query.Values["env"] != "prod" {
		t.Errorf("Expected env 'prod', got '%s'", query.Values["env"])
	}
}

// TestRuleGetTenantContext tests the uncovered GetTenantContext method for Rule
func TestRuleGetTenantContext(t *testing.T) {
	rule := NewRule("context-test").
		Tenant("test-tenant").
		Application("test-app").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	tenantID, applicationID := rule.GetTenantContext()

	if tenantID != "test-tenant" {
		t.Errorf("Expected tenantID 'test-tenant', got '%s'", tenantID)
	}
	if applicationID != "test-app" {
		t.Errorf("Expected applicationID 'test-app', got '%s'", applicationID)
	}

	// Test with empty values
	rule2 := NewRule("empty-context-test").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	tenantID2, applicationID2 := rule2.GetTenantContext()

	if tenantID2 != "" {
		t.Errorf("Expected empty tenantID, got '%s'", tenantID2)
	}
	if applicationID2 != "" {
		t.Errorf("Expected empty applicationID, got '%s'", applicationID2)
	}
}

// TestQueryRuleGetTenantContext tests the uncovered GetTenantContext method for QueryRule
func TestQueryRuleGetTenantContext(t *testing.T) {
	query := CreateQueryWithAllRulesAndTenant("query-tenant", "query-app", map[string]string{
		"region": "us-west",
	})

	tenantID, applicationID := query.GetTenantContext()

	if tenantID != "query-tenant" {
		t.Errorf("Expected tenantID 'query-tenant', got '%s'", tenantID)
	}
	if applicationID != "query-app" {
		t.Errorf("Expected applicationID 'query-app', got '%s'", applicationID)
	}

	// Test with regular query (no tenant context)
	query2 := CreateQuery(map[string]string{"region": "us-east"})
	tenantID2, applicationID2 := query2.GetTenantContext()

	if tenantID2 != "" {
		t.Errorf("Expected empty tenantID, got '%s'", tenantID2)
	}
	if applicationID2 != "" {
		t.Errorf("Expected empty applicationID, got '%s'", applicationID2)
	}
}

// TestForestInitializeDimension tests the uncovered InitializeDimension method
func TestForestInitializeDimension(t *testing.T) {
	forest := CreateForestIndex()

	// Test calling InitializeDimension (it's a no-op for compatibility)
	// This test just ensures the method can be called without errors
	forest.InitializeDimension("new-dimension")
	forest.InitializeDimension("another-dimension")

	// Since InitializeDimension is a no-op, we just verify it doesn't crash
	// and we can still use the forest normally
	order := forest.GetDimensionOrder()
	t.Logf("Current dimension order after InitializeDimension calls: %v", order)

	// The method should be callable without side effects
	originalOrderLength := len(order)
	forest.InitializeDimension("test-dimension")
	newOrder := forest.GetDimensionOrder()
	
	// Order should remain the same since InitializeDimension is a no-op
	if len(newOrder) != originalOrderLength {
		t.Errorf("Expected dimension order length to remain %d after no-op InitializeDimension, got %d", originalOrderLength, len(newOrder))
	}
}

// TestLoadRulesByTenant tests the uncovered LoadRulesByTenant functions
func TestLoadRulesByTenant(t *testing.T) {
	// Create temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "load-rules-by-tenant-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create engine with persistence
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add some tenant-specific rules
	rule1 := NewRule("tenant1-rule").
		Tenant("tenant1").
		Application("app1").
		Dimension("region", "us-west", MatchTypeEqual, 1.0).
		Build()

	rule2 := NewRule("tenant2-rule").
		Tenant("tenant2").
		Application("app1").
		Dimension("region", "us-east", MatchTypeEqual, 1.0).
		Build()

	rule3 := NewRule("global-rule").
		Dimension("region", "us-central", MatchTypeEqual, 1.0).
		Build()

	// Add rules to engine
	if err := engine.AddRule(rule1); err != nil {
		t.Fatalf("Failed to add tenant1 rule: %v", err)
	}
	if err := engine.AddRule(rule2); err != nil {
		t.Fatalf("Failed to add tenant2 rule: %v", err)
	}
	if err := engine.AddRule(rule3); err != nil {
		t.Fatalf("Failed to add global rule: %v", err)
	}

	// Save to persistence
	if err := engine.Save(); err != nil {
		t.Fatalf("Failed to save engine: %v", err)
	}

	// Test LoadRulesByTenant for JSONPersistence
	ctx := context.Background()
	persistence := engine.persistence.(*JSONPersistence)

	// Load rules for tenant1
	tenant1Rules, err := persistence.LoadRulesByTenant(ctx, "tenant1", "app1")
	if err != nil {
		t.Errorf("Failed to load rules for tenant1: %v", err)
	}
	if len(tenant1Rules) != 1 {
		t.Errorf("Expected 1 rule for tenant1, got %d", len(tenant1Rules))
	}
	if len(tenant1Rules) > 0 && tenant1Rules[0].ID != "tenant1-rule" {
		t.Errorf("Expected tenant1-rule, got %s", tenant1Rules[0].ID)
	}

	// Load rules for tenant2
	tenant2Rules, err := persistence.LoadRulesByTenant(ctx, "tenant2", "app1")
	if err != nil {
		t.Errorf("Failed to load rules for tenant2: %v", err)
	}
	if len(tenant2Rules) != 1 {
		t.Errorf("Expected 1 rule for tenant2, got %d", len(tenant2Rules))
	}
	if len(tenant2Rules) > 0 && tenant2Rules[0].ID != "tenant2-rule" {
		t.Errorf("Expected tenant2-rule, got %s", tenant2Rules[0].ID)
	}

	// Load rules for non-existent tenant
	nonexistentRules, err := persistence.LoadRulesByTenant(ctx, "nonexistent", "app1")
	if err != nil {
		t.Errorf("Failed to load rules for nonexistent tenant: %v", err)
	}
	if len(nonexistentRules) != 0 {
		t.Errorf("Expected 0 rules for nonexistent tenant, got %d", len(nonexistentRules))
	}
}

// TestLoadDimensionConfigsByTenant tests the uncovered LoadDimensionConfigsByTenant functions
func TestLoadDimensionConfigsByTenant(t *testing.T) {
	// Create temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "load-dims-by-tenant-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create engine with persistence
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add some tenant-specific dimension configs
	config1 := &DimensionConfig{
		Name:          "region",
		Index:         0,
		Required:      true,
		Weight:        1.0,
		TenantID:      "tenant1",
		ApplicationID: "app1",
	}

	config2 := &DimensionConfig{
		Name:          "env",
		Index:         1,
		Required:      false,
		Weight:        0.5,
		TenantID:      "tenant2",
		ApplicationID: "app1",
	}

	config3 := &DimensionConfig{
		Name:          "service",
		Index:         2,
		Required:      false,
		Weight:        0.8,
		// Global config (no tenant/app)
	}

	// Add dimension configs to engine
	if err := engine.AddDimension(config1); err != nil {
		t.Fatalf("Failed to add tenant1 dimension: %v", err)
	}
	if err := engine.AddDimension(config2); err != nil {
		t.Fatalf("Failed to add tenant2 dimension: %v", err)
	}
	if err := engine.AddDimension(config3); err != nil {
		t.Fatalf("Failed to add global dimension: %v", err)
	}

	// Save to persistence
	if err := engine.Save(); err != nil {
		t.Fatalf("Failed to save engine: %v", err)
	}

	// Test LoadDimensionConfigsByTenant for JSONPersistence
	ctx := context.Background()
	persistence := engine.persistence.(*JSONPersistence)

	// Load dimension configs for tenant1
	tenant1Configs, err := persistence.LoadDimensionConfigsByTenant(ctx, "tenant1", "app1")
	if err != nil {
		t.Errorf("Failed to load dimension configs for tenant1: %v", err)
	}
	// Should include tenant1 config + global config
	if len(tenant1Configs) != 2 {
		t.Errorf("Expected 2 dimension configs for tenant1 (1 tenant-specific + 1 global), got %d", len(tenant1Configs))
	}
	
	// Check that we have both the tenant-specific and global configs
	foundRegion := false
	foundService := false
	for _, config := range tenant1Configs {
		if config.Name == "region" && config.TenantID == "tenant1" {
			foundRegion = true
		}
		if config.Name == "service" && config.TenantID == "" {
			foundService = true
		}
	}
	if !foundRegion {
		t.Error("Expected to find tenant1-specific region dimension")
	}
	if !foundService {
		t.Error("Expected to find global service dimension")
	}

	// Load dimension configs for tenant2
	tenant2Configs, err := persistence.LoadDimensionConfigsByTenant(ctx, "tenant2", "app1")
	if err != nil {
		t.Errorf("Failed to load dimension configs for tenant2: %v", err)
	}
	// Should include tenant2 config + global config
	if len(tenant2Configs) != 2 {
		t.Errorf("Expected 2 dimension configs for tenant2 (1 tenant-specific + 1 global), got %d", len(tenant2Configs))
	}

	// Load dimension configs for non-existent tenant
	nonexistentConfigs, err := persistence.LoadDimensionConfigsByTenant(ctx, "nonexistent", "app1")
	if err != nil {
		t.Errorf("Failed to load dimension configs for nonexistent tenant: %v", err)
	}
	// Should include only global config
	if len(nonexistentConfigs) != 1 {
		t.Errorf("Expected 1 dimension config for nonexistent tenant (global only), got %d", len(nonexistentConfigs))
	}
	if len(nonexistentConfigs) > 0 && nonexistentConfigs[0].Name != "service" {
		t.Errorf("Expected global service dimension, got %s", nonexistentConfigs[0].Name)
	}
}

// TestDeleteDimension tests the uncovered deleteDimension function
func TestDeleteDimension(t *testing.T) {
	// Create an in-memory matcher
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewInMemoryMatcher(persistence, nil, "test-node-delete-dim")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add a dimension
	config := &DimensionConfig{
		Name:     "test-dimension",
		Index:    0,
		Required: false,
		Weight:   1.0,
	}

	if err := engine.AddDimension(config); err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Verify dimension exists
	dimensions, err := engine.ListDimensions()
	if err != nil {
		t.Fatalf("Failed to list dimensions: %v", err)
	}
	found := false
	for _, dim := range dimensions {
		if dim.Name == "test-dimension" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected test-dimension to exist")
	}

	// Test deleteDimension (this is an internal method)
	if err := engine.deleteDimension("test-dimension"); err != nil {
		t.Errorf("Failed to delete dimension: %v", err)
	}

	// Verify dimension no longer exists
	dimensionsAfter, err := engine.ListDimensions()
	if err != nil {
		t.Fatalf("Failed to list dimensions after delete: %v", err)
	}
	foundAfter := false
	for _, dim := range dimensionsAfter {
		if dim.Name == "test-dimension" {
			foundAfter = true
			break
		}
	}
	if foundAfter {
		t.Error("Expected test-dimension to be deleted")
	}

	// Test deleting non-existent dimension (should not error)
	if err := engine.deleteDimension("nonexistent-dimension"); err != nil {
		t.Errorf("Failed to delete nonexistent dimension: %v", err)
	}
}