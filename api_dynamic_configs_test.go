package matcher

import (
	"testing"
)

func TestCreateQueryWithDynamicConfigs(t *testing.T) {
	values := map[string]string{
		"region": "us-west",
		"env":    "prod",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"region": NewDimensionConfig("region", 0, false, 10.0),
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
		"category": NewDimensionConfig("category", 0, true, 15.0),
		"priority": NewDimensionConfig("priority", 1, false, 8.0),
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
	if categoryConfig.Name != "category" || categoryConfig.DefaultWeight != 15.0 {
		t.Errorf("Category config mismatch: name=%s, weight=%.1f", categoryConfig.Name, categoryConfig.DefaultWeight)
	}

	priorityConfig := query.DynamicDimensionConfigs["priority"]
	if priorityConfig.Name != "priority" || priorityConfig.DefaultWeight != 8.0 {
		t.Errorf("Priority config mismatch: name=%s, weight=%.1f", priorityConfig.Name, priorityConfig.DefaultWeight)
	}
}

func TestCreateQueryWithAllRulesAndDynamicConfigs(t *testing.T) {
	values := map[string]string{
		"service": "user-service",
		"version": "1.2.3",
	}

	dynamicConfigs := map[string]*DimensionConfig{
		"service": NewDimensionConfig("service", 0, true, 20.0),
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
	if serviceConfig.Name != "service" || serviceConfig.DefaultWeight != 20.0 {
		t.Errorf("Service config mismatch: name=%s, weight=%.1f", serviceConfig.Name, serviceConfig.DefaultWeight)
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
		"user_type": NewDimensionConfig("user_type", 0, true, 25.0),
		"action":    NewDimensionConfig("action", 1, true, 20.0),
		"resource":  NewDimensionConfig("resource", 2, false, 10.0),
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

		if config.DefaultWeight != expected.weight {
			t.Errorf("Config %s: expected weight %.1f, got %.1f", name, expected.weight, config.DefaultWeight)
		}

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
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
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
				dynamicConfig := NewDimensionConfig("region", 0, false, 50.0)
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
				dynamicConfig := NewDimensionConfig("region", 0, false, 75.0)
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
				dynamicConfig := NewDimensionConfig("region", 0, false, 30.0)
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
				dynamicConfig := NewDimensionConfig("region", 0, false, 25.0)
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
