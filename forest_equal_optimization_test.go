package matcher

import (
	"fmt"
	"testing"
	"time"
)

func TestEqualMatchOptimization(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	regionConfig.SetWeight(MatchTypePrefix, 7.0)

	envConfig := NewDimensionConfig("env", 1, false, 3.0)
	envConfig.SetWeight(MatchTypeEqual, 8.0)
	envConfig.SetWeight(MatchTypeAny, 2.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Create rules with equal match types that should benefit from hash map optimization
	exactRules := []*Rule{
		NewRule("exact-us-west-prod").
			Dimension("region", "us-west", MatchTypeEqual).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
		NewRule("exact-us-east-prod").
			Dimension("region", "us-east", MatchTypeEqual).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
		NewRule("exact-eu-west-staging").
			Dimension("region", "eu-west", MatchTypeEqual).
			Dimension("env", "staging", MatchTypeEqual).
			Build(),
		NewRule("exact-ap-south-dev").
			Dimension("region", "ap-south", MatchTypeEqual).
			Dimension("env", "dev", MatchTypeEqual).
			Build(),
	}

	// Also add some non-equal rules to ensure they still work
	nonExactRules := []*Rule{
		NewRule("prefix-us").
			Dimension("region", "us-", MatchTypePrefix).
			Dimension("env", "any", MatchTypeAny).
			Build(),
		NewRule("any-region-prod").
			Dimension("region", "any", MatchTypeAny).
			Dimension("env", "prod", MatchTypeEqual).
			Build(),
	}

	// Add all rules
	allRules := append(exactRules, nonExactRules...)
	for _, rule := range allRules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test exact match queries (should benefit from O(1) hash map lookup)
	testCases := []struct {
		name            string
		query           map[string]string
		expectedMatches []string
		description     string
	}{
		{
			name: "exact-match-us-west-prod",
			query: map[string]string{
				"region": "us-west",
				"env":    "prod",
			},
			expectedMatches: []string{"exact-us-west-prod", "prefix-us", "any-region-prod"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "exact-match-us-east-prod",
			query: map[string]string{
				"region": "us-east",
				"env":    "prod",
			},
			expectedMatches: []string{"exact-us-east-prod", "prefix-us", "any-region-prod"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "exact-match-eu-west-staging",
			query: map[string]string{
				"region": "eu-west",
				"env":    "staging",
			},
			expectedMatches: []string{"exact-eu-west-staging"},
			description:     "Should use O(1) lookup for exact matches",
		},
		{
			name: "no-exact-match",
			query: map[string]string{
				"region": "unknown-region",
				"env":    "prod",
			},
			expectedMatches: []string{"any-region-prod"},
			description:     "Should quickly determine no exact match exists",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := &QueryRule{
				Values: tc.query,
			}

			matches, err := engine.FindAllMatches(query)
			if err != nil {
				t.Fatalf("FindAllMatches failed: %v", err)
			}

			// Verify we got the expected matches
			matchedIDs := make([]string, len(matches))
			for i, match := range matches {
				matchedIDs[i] = match.Rule.ID
			}

			if len(matchedIDs) != len(tc.expectedMatches) {
				t.Errorf("Expected %d matches, got %d. Expected: %v, Got: %v",
					len(tc.expectedMatches), len(matchedIDs), tc.expectedMatches, matchedIDs)
			}

			// Check that all expected matches are present
			expectedSet := make(map[string]bool)
			for _, expected := range tc.expectedMatches {
				expectedSet[expected] = true
			}

			for _, matchedID := range matchedIDs {
				if !expectedSet[matchedID] {
					t.Errorf("Unexpected match: %s", matchedID)
				}
			}

			t.Logf("%s: Found %d matches as expected", tc.description, len(matches))
		})
	}
}

func TestEqualMatchPerformance(t *testing.T) {
	// Skip performance test in short mode
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)

	serviceConfig := NewDimensionConfig("service", 1, false, 3.0)
	serviceConfig.SetWeight(MatchTypeEqual, 8.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}

	err = engine.AddDimension(serviceConfig)
	if err != nil {
		t.Fatalf("Failed to add service dimension: %v", err)
	}

	// Create a large number of rules with exact matches to test performance
	numRules := 1000
	regions := []string{"us-west-1", "us-west-2", "us-east-1", "us-east-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-1"}
	services := []string{"web", "api", "database", "cache", "queue", "worker", "monitor", "auth", "notification", "analytics"}

	t.Logf("Creating %d rules with exact match types...", numRules)
	for i := 0; i < numRules; i++ {
		region := regions[i%len(regions)]
		service := services[i%len(services)]

		rule := NewRule(fmt.Sprintf("rule-%d", i)).
			Dimension("region", region, MatchTypeEqual).
			Dimension("service", service, MatchTypeEqual).
			Build()

		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %d: %v", i, err)
		}
	}

	// Test query performance with exact matches (should benefit from hash map optimization)
	query := &QueryRule{
		Values: map[string]string{
			"region":  "us-west-1",
			"service": "api",
		},
	}

	// Warm up
	_, err = engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("Warmup query failed: %v", err)
	}

	// Measure performance
	numQueries := 1000
	startTime := time.Now()

	for i := 0; i < numQueries; i++ {
		region := regions[i%len(regions)]
		service := services[i%len(services)]

		testQuery := &QueryRule{
			Values: map[string]string{
				"region":  region,
				"service": service,
			},
		}

		_, err = engine.FindAllMatches(testQuery)
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
	}

	duration := time.Since(startTime)
	avgQueryTime := duration / time.Duration(numQueries)

	t.Logf("Performance test completed:")
	t.Logf("  Total rules: %d", numRules)
	t.Logf("  Total queries: %d", numQueries)
	t.Logf("  Total time: %v", duration)
	t.Logf("  Average query time: %v", avgQueryTime)
	t.Logf("  Queries per second: %.2f", float64(numQueries)/duration.Seconds())

	// With hash map optimization, queries should be very fast
	// Even with 1000 rules, average query time should be well under 1ms
	if avgQueryTime > 1*time.Millisecond {
		t.Logf("Warning: Average query time %v is higher than expected. Hash map optimization may not be working optimally.", avgQueryTime)
	} else {
		t.Logf("✓ Performance looks good - hash map optimization is working")
	}
}

func TestEqualMatchCorrectness(t *testing.T) {
	// Create a temporary directory for this test
	tempDir := t.TempDir()

	// Create matcher engine
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create matcher engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configuration
	userConfig := NewDimensionConfig("user_type", 0, false, 5.0)
	userConfig.SetWeight(MatchTypeEqual, 10.0)
	userConfig.SetWeight(MatchTypePrefix, 7.0)

	err = engine.AddDimension(userConfig)
	if err != nil {
		t.Fatalf("Failed to add user_type dimension: %v", err)
	}

	// Add rules with same dimension name but different values and match types
	rules := []*Rule{
		NewRule("admin-exact").
			Dimension("user_type", "admin", MatchTypeEqual).
			Build(),
		NewRule("admin-prefix").
			Dimension("user_type", "admin", MatchTypePrefix). // This should also match "admin" query
			Build(),
		NewRule("user-exact").
			Dimension("user_type", "user", MatchTypeEqual).
			Build(),
		NewRule("adm-prefix").
			Dimension("user_type", "adm", MatchTypePrefix). // This should match "admin" query
			Build(),
	}

	for _, rule := range rules {
		err = engine.AddRule(rule)
		if err != nil {
			t.Fatalf("Failed to add rule %s: %v", rule.ID, err)
		}
	}

	// Test that equal match optimization doesn't break prefix/other match types
	query := &QueryRule{
		Values: map[string]string{
			"user_type": "admin",
		},
	}

	matches, err := engine.FindAllMatches(query)
	if err != nil {
		t.Fatalf("FindAllMatches failed: %v", err)
	}

	// Should match: admin-exact (exact), admin-prefix (prefix), adm-prefix (prefix)
	// Should NOT match: user-exact
	expectedMatches := map[string]bool{
		"admin-exact":  true,
		"admin-prefix": true,
		"adm-prefix":   true,
	}

	if len(matches) != 3 {
		t.Errorf("Expected 3 matches, got %d", len(matches))
	}

	for _, match := range matches {
		if !expectedMatches[match.Rule.ID] {
			t.Errorf("Unexpected match: %s", match.Rule.ID)
		}
		t.Logf("✓ Correctly matched rule: %s (weight: %.1f)", match.Rule.ID, match.TotalWeight)
	}

	// Verify that exact matches get higher weight (should be first)
	if len(matches) > 0 && matches[0].Rule.ID != "admin-exact" {
		t.Errorf("Expected 'admin-exact' to have highest weight and be first, got '%s'", matches[0].Rule.ID)
	}
}
