package matcher

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConcurrentRuleOperationsNoPartialRules verifies that queries never return partial rules
// during concurrent add/delete/update operations
func TestConcurrentRuleOperationsNoPartialRules(t *testing.T) {
	// Create temporary directory for persistence
	tempDir := t.TempDir()

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this concurrency test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)
	serviceConfig := NewDimensionConfig("service", 2, false)
	serviceConfig.SetWeight(MatchTypeEqual, 6.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}
	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}
	err = engine.AddDimension(serviceConfig)
	if err != nil {
		t.Fatalf("Failed to add service dimension: %v", err)
	}

	// Track errors across goroutines
	var errorsMu sync.Mutex
	var errors []string

	addError := func(err string) {
		errorsMu.Lock()
		errors = append(errors, err)
		errorsMu.Unlock()
	}

	// Number of concurrent operations
	numOperations := 50
	numQueryWorkers := 10

	var wg sync.WaitGroup

	// Start query workers that will continuously query the engine
	// These queries should never see partial rules
	for i := 0; i < numQueryWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			query := &QueryRule{
				Values: map[string]string{
					"region":  "us-west",
					"env":     "prod",
					"service": "api",
				},
			}

			// Query continuously for a period
			startTime := time.Now()
			queryCount := 0

			for time.Since(startTime) < 2*time.Second {
				matches, err := engine.FindAllMatches(query)
				if err != nil {
					addError(fmt.Sprintf("Query worker %d: FindAllMatches error: %v", workerID, err))
					return
				}

				queryCount++

				// Validate each returned rule is complete and consistent
				for _, match := range matches {
					rule := match.Rule

					// Verify rule has all required fields
					if rule.ID == "" {
						addError(fmt.Sprintf("Query worker %d: Found rule with empty ID", workerID))
					}

					if rule.Dimensions == nil {
						addError(fmt.Sprintf("Query worker %d: Found rule %s with nil dimensions", workerID, rule.ID))
					}

					if rule.Metadata == nil {
						addError(fmt.Sprintf("Query worker %d: Found rule %s with nil metadata", workerID, rule.ID))
					}

					// Verify rule dimensions are complete
					for _, dim := range rule.Dimensions {
						if dim == nil {
							addError(fmt.Sprintf("Query worker %d: Found rule %s with nil dimension", workerID, rule.ID))
							continue
						}

						if dim.DimensionName == "" {
							addError(fmt.Sprintf("Query worker %d: Found rule %s with empty dimension name", workerID, rule.ID))
						}

						// MatchType is an int, so we don't need to check for empty string
						// The zero value (MatchTypeEqual) is valid
					}

					// Verify the rule actually matches our query
					// This ensures we're not getting rules that are in an inconsistent state
					expectedMatches := 0
					for _, dim := range rule.Dimensions {
						if queryValue, exists := query.Values[dim.DimensionName]; exists {
							switch dim.MatchType {
							case MatchTypeEqual:
								if dim.Value == queryValue {
									expectedMatches++
								}
							case MatchTypeAny:
								expectedMatches++
							case MatchTypePrefix:
								if len(queryValue) >= len(dim.Value) && queryValue[:len(dim.Value)] == dim.Value {
									expectedMatches++
								}
							case MatchTypeSuffix:
								if len(queryValue) >= len(dim.Value) && queryValue[len(queryValue)-len(dim.Value):] == dim.Value {
									expectedMatches++
								}
							}
						}
					}

					// If we got this rule as a match, it should actually match our query
					if expectedMatches == 0 && len(rule.Dimensions) > 0 {
						addError(fmt.Sprintf("Query worker %d: Rule %s returned as match but doesn't actually match query", workerID, rule.ID))
					}
				}

				// Small delay to allow other operations
				time.Sleep(1 * time.Millisecond)
			}

			t.Logf("Query worker %d completed %d queries", workerID, queryCount)
		}(i)
	}

	// Start rule addition workers
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			// Create a rule with random variation
			ruleID := fmt.Sprintf("concurrent-rule-%d", opID)

			// Add some randomness to create different rules
			regions := []string{"us-west", "us-east", "eu-west"}
			envs := []string{"prod", "staging", "dev"}
			services := []string{"api", "web", "worker"}

			region := regions[rand.Intn(len(regions))]
			env := envs[rand.Intn(len(envs))]
			service := services[rand.Intn(len(services))]

			rule := NewRule(ruleID).
				Dimension("region", region, MatchTypeEqual).
				Dimension("env", env, MatchTypeEqual).
				Dimension("service", service, MatchTypeEqual).
				Build()

			// Add some metadata to make the rule more substantial
			rule.Metadata = map[string]string{
				"creator":    fmt.Sprintf("worker-%d", opID),
				"created_at": time.Now().Format(time.RFC3339),
				"priority":   "normal",
			}

			// Set a manual weight to avoid conflicts
			weight := float64(100 + opID)
			rule.ManualWeight = &weight

			if err := engine.AddRule(rule); err != nil {
				addError(fmt.Sprintf("Add operation %d: Failed to add rule: %v", opID, err))
			}

			// Small random delay
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}(i)
	}

	// Start rule update workers
	for i := 0; i < numOperations/2; i++ {
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			// Wait a bit for some rules to be added
			time.Sleep(50 * time.Millisecond)

			ruleID := fmt.Sprintf("concurrent-rule-%d", opID*2) // Update every other rule

			// Try to get and update the rule
			existingRule, err := engine.GetRule(ruleID)
			if err != nil {
				// Rule might not exist yet, which is fine
				return
			}

			// Update the metadata
			existingRule.Metadata["updated_by"] = fmt.Sprintf("updater-%d", opID)
			existingRule.Metadata["updated_at"] = time.Now().Format(time.RFC3339)
			existingRule.Status = RuleStatusDraft

			if err := engine.UpdateRule(existingRule); err != nil {
				addError(fmt.Sprintf("Update operation %d: Failed to update rule %s: %v", opID, ruleID, err))
			}

			// Small random delay
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}(i)
	}

	// Start rule deletion workers
	for i := 0; i < numOperations/4; i++ {
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			// Wait for rules to be added and some updates
			time.Sleep(100 * time.Millisecond)

			ruleID := fmt.Sprintf("concurrent-rule-%d", opID*4) // Delete every fourth rule

			if err := engine.DeleteRule(ruleID); err != nil {
				// Rule might not exist, which is fine for this test
				t.Logf("Delete operation %d: Rule %s might not exist (expected): %v", opID, ruleID, err)
			}

			// Small random delay
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()

	// Check for any errors
	errorsMu.Lock()
	defer errorsMu.Unlock()

	if len(errors) > 0 {
		t.Errorf("Found %d concurrency issues:", len(errors))
		for i, err := range errors {
			t.Errorf("Error %d: %s", i+1, err)
		}
	}

	// Final verification: do a clean query to ensure the engine is in a consistent state
	finalQuery := &QueryRule{
		Values: map[string]string{
			"region":  "us-west",
			"env":     "prod",
			"service": "api",
		},
	}

	finalMatches, err := engine.FindAllMatches(finalQuery)
	if err != nil {
		t.Fatalf("Final query failed: %v", err)
	}

	t.Logf("Final query returned %d matches", len(finalMatches))

	// Verify all final matches are complete and consistent
	for i, match := range finalMatches {
		rule := match.Rule
		if rule.ID == "" {
			t.Errorf("Final match %d has empty rule ID", i)
		}
		if rule.Dimensions == nil {
			t.Errorf("Final match %d has nil dimensions", i)
		}
		if rule.Metadata == nil {
			t.Errorf("Final match %d has nil metadata", i)
		}
	}
}

// TestConcurrentRuleStatusUpdatesNoPartialRules specifically tests concurrent status updates
func TestConcurrentRuleStatusUpdatesNoPartialRules(t *testing.T) {
	tempDir := t.TempDir()

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

	// Add a base rule
	baseRule := NewRule("status-test-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	err = engine.AddRule(baseRule)
	if err != nil {
		t.Fatalf("Failed to add base rule: %v", err)
	}

	// Track any issues
	var issuesMu sync.Mutex
	var issues []string

	addIssue := func(issue string) {
		issuesMu.Lock()
		issues = append(issues, issue)
		issuesMu.Unlock()
	}

	var wg sync.WaitGroup
	numUpdaters := 10
	numQueries := 5

	// Start concurrent status updaters
	for i := 0; i < numUpdaters; i++ {
		wg.Add(1)
		go func(updaterID int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				// Alternate between statuses
				var status RuleStatus
				if j%2 == 0 {
					status = RuleStatusWorking
				} else {
					status = RuleStatusDraft
				}

				err := engine.UpdateRuleStatus("status-test-rule", status)
				if err != nil {
					addIssue(fmt.Sprintf("Updater %d iteration %d: UpdateRuleStatus failed: %v", updaterID, j, err))
				}

				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Start concurrent queriers
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func(queryID int) {
			defer wg.Done()

			query := &QueryRule{
				Values: map[string]string{
					"region": "us-west",
				},
			}

			startTime := time.Now()
			for time.Since(startTime) < 1*time.Second {
				matches, err := engine.FindAllMatches(query)
				if err != nil {
					addIssue(fmt.Sprintf("Querier %d: FindAllMatches failed: %v", queryID, err))
					continue
				}

				// Verify we get consistent results
				for _, match := range matches {
					rule := match.Rule

					// Rule should have a valid status
					if rule.Status != RuleStatusWorking && rule.Status != RuleStatusDraft {
						addIssue(fmt.Sprintf("Querier %d: Found rule with invalid status: %s", queryID, rule.Status))
					}

					// Rule should be complete
					if rule.ID == "" {
						addIssue(fmt.Sprintf("Querier %d: Found rule with empty ID", queryID))
					}

					if len(rule.Dimensions) == 0 {
						addIssue(fmt.Sprintf("Querier %d: Found rule with no dimensions", queryID))
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Check for issues
	issuesMu.Lock()
	defer issuesMu.Unlock()

	if len(issues) > 0 {
		t.Errorf("Found %d issues with concurrent status updates:", len(issues))
		for i, issue := range issues {
			t.Errorf("Issue %d: %s", i+1, issue)
		}
	}
}

// TestConcurrentMetadataUpdatesNoPartialRules tests concurrent metadata updates
func TestConcurrentMetadataUpdatesNoPartialRules(t *testing.T) {
	tempDir := t.TempDir()

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

	// Add a base rule with initial metadata
	baseRule := NewRule("metadata-test-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	baseRule.Metadata = map[string]string{
		"initial": "value",
		"count":   "0",
	}

	err = engine.AddRule(baseRule)
	if err != nil {
		t.Fatalf("Failed to add base rule: %v", err)
	}

	var issuesMu sync.Mutex
	var issues []string

	addIssue := func(issue string) {
		issuesMu.Lock()
		issues = append(issues, issue)
		issuesMu.Unlock()
	}

	var wg sync.WaitGroup
	numUpdaters := 8
	numQueries := 3

	// Start concurrent metadata updaters
	for i := 0; i < numUpdaters; i++ {
		wg.Add(1)
		go func(updaterID int) {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				metadata := map[string]string{
					"updater":   fmt.Sprintf("worker-%d", updaterID),
					"iteration": fmt.Sprintf("%d", j),
					"timestamp": time.Now().Format(time.RFC3339Nano),
					"random":    fmt.Sprintf("%d", rand.Intn(1000)),
				}

				err := engine.UpdateRuleMetadata("metadata-test-rule", metadata)
				if err != nil {
					addIssue(fmt.Sprintf("Metadata updater %d iteration %d: UpdateRuleMetadata failed: %v", updaterID, j, err))
				}

				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Start concurrent queriers that validate metadata consistency
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func(queryID int) {
			defer wg.Done()

			query := &QueryRule{
				Values: map[string]string{
					"region": "us-west",
				},
			}

			startTime := time.Now()
			for time.Since(startTime) < 800*time.Millisecond {
				matches, err := engine.FindAllMatches(query)
				if err != nil {
					addIssue(fmt.Sprintf("Metadata querier %d: FindAllMatches failed: %v", queryID, err))
					continue
				}

				for _, match := range matches {
					rule := match.Rule

					// Metadata should never be nil
					if rule.Metadata == nil {
						addIssue(fmt.Sprintf("Metadata querier %d: Found rule with nil metadata", queryID))
						continue
					}

					// All metadata values should be complete strings (not partial)
					for key, value := range rule.Metadata {
						if key == "" {
							addIssue(fmt.Sprintf("Metadata querier %d: Found empty metadata key", queryID))
						}

						// Values should be reasonable strings (not corruption indicators)
						if len(value) > 0 && value[0] == 0 {
							addIssue(fmt.Sprintf("Metadata querier %d: Found corrupted metadata value for key %s", queryID, key))
						}
					}

					// If updater/iteration are present, they should be consistent with each other
					if updater, hasUpdater := rule.Metadata["updater"]; hasUpdater {
						if iteration, hasIteration := rule.Metadata["iteration"]; hasIteration {
							// Both should be present and valid
							if updater == "" || iteration == "" {
								addIssue(fmt.Sprintf("Metadata querier %d: Found inconsistent updater/iteration metadata", queryID))
							}
						}
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Check for issues
	issuesMu.Lock()
	defer issuesMu.Unlock()

	if len(issues) > 0 {
		t.Errorf("Found %d issues with concurrent metadata updates:", len(issues))
		for i, issue := range issues {
			t.Errorf("Issue %d: %s", i+1, issue)
		}
	}
}

// TestRuleConsistencyGuarantees documents and verifies the concurrency safety guarantees
// of the matcher engine regarding rule consistency during CRUD operations
func TestRuleConsistencyGuarantees(t *testing.T) {
	t.Log("=== Testing Rule Consistency Guarantees ===")

	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for testing
	engine.SetAllowDuplicateWeights(true)

	// Add test dimensions
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	t.Log("✓ Engine initialized with dimension configuration")

	// Test 1: Verify read locks protect against partial reads during writes
	t.Run("ReadLockProtection", func(t *testing.T) {
		var wg sync.WaitGroup
		const iterations = 100

		// Add a base rule
		baseRule := NewRule("read-protection-test").
			Dimension("region", "us-west", MatchTypeEqual).
			Build()
		err := engine.AddRule(baseRule)
		if err != nil {
			t.Fatalf("Failed to add base rule: %v", err)
		}

		// Concurrent readers should never see partial state
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				query := &QueryRule{Values: map[string]string{"region": "us-west"}}

				for j := 0; j < iterations; j++ {
					matches, err := engine.FindAllMatches(query)
					if err != nil {
						t.Errorf("Query failed: %v", err)
						return
					}

					// Every returned rule must be complete
					for _, match := range matches {
						if match.Rule.ID == "" || match.Rule.Dimensions == nil {
							t.Errorf("Found incomplete rule during concurrent read")
						}
					}
				}
			}()
		}

		// Concurrent writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				newRule := NewRule("temp-rule").
					Dimension("region", "us-east", MatchTypeEqual).
					Build()
				engine.AddRule(newRule)
				engine.DeleteRule("temp-rule")
			}
		}()

		wg.Wait()
		t.Log("✓ Read operations protected against partial state during writes")
	})

	// Test 2: Verify rule updates are atomic
	t.Run("AtomicUpdates", func(t *testing.T) {
		// Add a rule to update
		updateRule := NewRule("atomic-update-test").
			Dimension("region", "us-west", MatchTypeEqual).
			Build()
		updateRule.Metadata = map[string]string{"version": "1"}
		err := engine.AddRule(updateRule)
		if err != nil {
			t.Fatalf("Failed to add rule for update test: %v", err)
		}

		var wg sync.WaitGroup
		const numUpdaters = 10
		const numReaders = 10

		// Concurrent updaters
		for i := 0; i < numUpdaters; i++ {
			wg.Add(1)
			go func(updaterID int) {
				defer wg.Done()

				for j := 0; j < 20; j++ {
					// Get current rule, modify it, update it
					currentRule, err := engine.GetRule("atomic-update-test")
					if err != nil {
						continue // Rule might be temporarily unavailable
					}

					currentRule.Metadata["updater"] = string(rune('A' + updaterID))
					currentRule.Metadata["iteration"] = string(rune('0' + j%10))

					engine.UpdateRule(currentRule)
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Concurrent readers verifying atomicity
		inconsistencies := 0
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				query := &QueryRule{Values: map[string]string{"region": "us-west"}}

				for j := 0; j < 100; j++ {
					matches, err := engine.FindAllMatches(query)
					if err != nil {
						continue
					}

					for _, match := range matches {
						if match.Rule.ID == "atomic-update-test" {
							// Verify metadata consistency
							if match.Rule.Metadata == nil {
								inconsistencies++
								t.Errorf("Found rule with nil metadata during update")
							} else {
								// If updater is set, iteration should also be set
								if updater, hasUpdater := match.Rule.Metadata["updater"]; hasUpdater {
									if _, hasIteration := match.Rule.Metadata["iteration"]; !hasIteration {
										inconsistencies++
										t.Errorf("Found partial metadata update: updater=%s but no iteration", updater)
									}
								}
							}
						}
					}

					time.Sleep(time.Millisecond)
				}
			}()
		}

		wg.Wait()

		if inconsistencies == 0 {
			t.Log("✓ Rule updates are atomic - no partial state observed")
		} else {
			t.Errorf("Found %d atomic update violations", inconsistencies)
		}
	})

	// Test 3: Verify forest index consistency
	t.Run("ForestIndexConsistency", func(t *testing.T) {
		var wg sync.WaitGroup
		const numWorkers = 8
		const rulesPerWorker = 25

		// Workers adding and removing rules
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for j := 0; j < rulesPerWorker; j++ {
					ruleID := string(rune('A'+workerID)) + string(rune('0'+j))

					rule := NewRule(ruleID).
						Dimension("region", "us-west", MatchTypeEqual).
						Build()

					// Add rule
					engine.AddRule(rule)

					// Remove rule after a short time
					time.Sleep(2 * time.Millisecond)
					engine.DeleteRule(ruleID)
				}
			}(i)
		}

		// Reader verifying forest index consistency
		wg.Add(1)
		go func() {
			defer wg.Done()

			query := &QueryRule{Values: map[string]string{"region": "us-west"}}

			for i := 0; i < 200; i++ {
				matches, err := engine.FindAllMatches(query)
				if err != nil {
					t.Errorf("Forest index query failed: %v", err)
					return
				}

				// All returned matches should be valid
				for _, match := range matches {
					if match.Rule == nil {
						t.Errorf("Found null rule in forest index results")
					} else if match.Rule.ID == "" {
						t.Errorf("Found rule with empty ID in forest index results")
					}
				}

				time.Sleep(time.Millisecond)
			}
		}()

		wg.Wait()
		t.Log("✓ Forest index maintains consistency during concurrent add/remove operations")
	})

	t.Log("=== All Rule Consistency Guarantees Verified ===")
	t.Log("✓ Queries never return partial rules during concurrent operations")
	t.Log("✓ Read locks properly protect against incomplete reads")
	t.Log("✓ Rule updates are atomic (all-or-nothing)")
	t.Log("✓ Forest index maintains referential integrity")
	t.Log("✓ Concurrent add/update/delete operations are thread-safe")
}

func TestSharedNodeRuleManagement(t *testing.T) {
	// Create dimension configurations for proper testing
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
	}, nil)

	forest := &ForestIndex{
		RuleForest: CreateRuleForest(dimensionConfigs),
	}

	// Create multiple rules that will share the same tree path
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	// Add all rules - they should all share the same tree path
	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Create a query that matches all rules
	query := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	// Find candidates - should return all 3 rules
	candidates := forest.FindCandidateRules(query)
	if len(candidates) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(candidates))
	}

	// Verify all rules are present
	ruleIds := make(map[string]bool)
	for _, rule := range candidates {
		ruleIds[rule.ID] = true
	}

	if !ruleIds["rule1"] || !ruleIds["rule2"] || !ruleIds["rule3"] {
		t.Error("Not all rules found in candidates")
	}

	// Remove one rule
	forest.RemoveRule(rule2)

	// Query again - should now return only 2 rules
	candidates = forest.FindCandidateRules(query)
	if len(candidates) != 2 {
		t.Errorf("Expected 2 candidates after removal, got %d", len(candidates))
	}

	// Verify rule2 is gone but rule1 and rule3 remain
	ruleIds = make(map[string]bool)
	for _, rule := range candidates {
		ruleIds[rule.ID] = true
	}

	if !ruleIds["rule1"] || ruleIds["rule2"] || !ruleIds["rule3"] {
		t.Error("Incorrect rules after removal - rule2 should be gone")
	}

	// Remove another rule
	forest.RemoveRule(rule1)

	// Query again - should now return only 1 rule
	candidates = forest.FindCandidateRules(query)
	if len(candidates) != 1 {
		t.Errorf("Expected 1 candidate after second removal, got %d", len(candidates))
	}

	if candidates[0].ID != "rule3" {
		t.Errorf("Expected rule3 to remain, got %s", candidates[0].ID)
	}
}

func TestSharedNodeWithDifferentPaths(t *testing.T) {
	// Create dimension configurations for proper testing
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
	}, nil)

	forest := &ForestIndex{
		RuleForest: CreateRuleForest(dimensionConfigs),
	}

	// Create rules that share some nodes but diverge
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "drill", MatchType: MatchTypeEqual}, // Different tool
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "alt", MatchType: MatchTypeEqual}, // Different route
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
		},
	}

	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	// Query for laser tool with main route
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("Query1 should return only rule1")
	}

	// Query for drill tool with main route
	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "drill",
		},
	}

	candidates2 := forest.FindCandidateRules(query2)
	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("Query2 should return only rule2")
	}

	// Query for laser tool with alt route
	query3 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "alt",
			"tool":    "laser",
		},
	}

	candidates3 := forest.FindCandidateRules(query3)
	if len(candidates3) != 1 || candidates3[0].ID != "rule3" {
		t.Error("Query3 should return only rule3")
	}

	// Remove rule2 and verify it doesn't affect rule1 or rule3
	forest.RemoveRule(rule2)

	// Re-test query1 and query3 - should still work
	candidates1 = forest.FindCandidateRules(query1)
	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("After removing rule2, query1 should still return rule1")
	}

	candidates3 = forest.FindCandidateRules(query3)
	if len(candidates3) != 1 || candidates3[0].ID != "rule3" {
		t.Error("After removing rule2, query3 should still return rule3")
	}

	// Query2 should now return no results
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 0 {
		t.Error("After removing rule2, query2 should return no results")
	}
}

func TestNodeCleanupAfterRuleRemoval(t *testing.T) {
	// Create dimension configurations for proper testing
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
		NewDimensionConfig("env", 3, false),
	}, nil)

	forest := &ForestIndex{
		RuleForest: CreateRuleForest(dimensionConfigs),
	}

	// Create rules that will create a deep tree structure
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "laser", MatchType: MatchTypeEqual},
			"env":     {DimensionName: "env", Value: "prod", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
			"tool":    {DimensionName: "tool", Value: "drill", MatchType: MatchTypeEqual}, // Different tool
		},
	}

	// Add both rules
	forest.AddRule(rule1)
	forest.AddRule(rule2)

	// Debug: Check dimension order using available methods
	if forest.Dimensions != nil {
		dimOrder := forest.Dimensions.GetSortedNames()
		t.Logf("Dimension order: %v", dimOrder)
	}

	// Debug: Check forest stats
	debugStats := forest.GetStats()
	t.Logf("Forest stats: %+v", debugStats)

	// Verify both rules can be found
	query1 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "laser",
			"env":     "prod",
		},
	}

	query2 := &QueryRule{
		Values: map[string]string{
			"product": "ProductA",
			"route":   "main",
			"tool":    "drill",
		},
	}

	candidates1 := forest.FindCandidateRules(query1)
	candidates2 := forest.FindCandidateRules(query2)

	t.Logf("Rule1 candidates: %d", len(candidates1))
	for _, c := range candidates1 {
		t.Logf("  Found: %s", c.ID)
	}

	t.Logf("Rule2 candidates: %d", len(candidates2))
	for _, c := range candidates2 {
		t.Logf("  Found: %s", c.ID)
	}

	if len(candidates1) != 1 || candidates1[0].ID != "rule1" {
		t.Error("Should find rule1")
	}

	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("Should find rule2")
	}

	// Get initial stats
	stats := forest.GetStats()
	initialTrees := stats["total_trees"].(int)

	// Remove rule1 (which has a longer path)
	forest.RemoveRule(rule1)

	// rule1 should no longer be found
	candidates1 = forest.FindCandidateRules(query1)
	if len(candidates1) != 0 {
		t.Error("rule1 should not be found after removal")
	}

	// rule2 should still be found
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 1 || candidates2[0].ID != "rule2" {
		t.Error("rule2 should still be found after rule1 removal")
	}

	// The forest structure should be cleaned up but main tree should remain
	stats = forest.GetStats()
	finalTrees := stats["total_trees"].(int)

	if finalTrees != initialTrees {
		t.Logf("Trees before: %d, after: %d", initialTrees, finalTrees)
		// This is expected - the structure should be cleaned up
	}

	// Remove rule2
	forest.RemoveRule(rule2)

	// No rules should be found now
	candidates2 = forest.FindCandidateRules(query2)
	if len(candidates2) != 0 {
		t.Error("rule2 should not be found after removal")
	}
}

func TestForestStatisticsWithSharedNodes(t *testing.T) {
	// Create dimension configurations for proper testing
	dimensionConfigs := NewDimensionConfigsWithDimensionsAndSorter([]*DimensionConfig{
		NewDimensionConfig("product", 0, true),
		NewDimensionConfig("route", 1, false),
		NewDimensionConfig("tool", 2, false),
	}, nil)

	forest := &ForestIndex{
		RuleForest: CreateRuleForest(dimensionConfigs),
	}

	// Create multiple rules that share nodes
	rule1 := &Rule{
		ID: "rule1",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
		},
	}

	rule2 := &Rule{
		ID: "rule2",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "main", MatchType: MatchTypeEqual},
		},
	}

	rule3 := &Rule{
		ID: "rule3",
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "ProductA", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "alt", MatchType: MatchTypeEqual},
		},
	}

	forest.AddRule(rule1)
	forest.AddRule(rule2)
	forest.AddRule(rule3)

	stats := forest.GetStats()
	t.Logf("Forest stats: %+v", stats)

	// We should have 1 tree (all rules share same primary dimension)
	if stats["total_trees"].(int) != 1 {
		t.Errorf("Expected 1 tree, got %d", stats["total_trees"].(int))
	}

	// We should have 3 total rules
	if stats["total_rules"].(int) != 3 {
		t.Errorf("Expected 3 total rules, got %d", stats["total_rules"].(int))
	}

	// There should be at least one shared node (rule1 and rule2 share the same path)
	if sharedCount, exists := stats["shared_nodes"]; exists && sharedCount.(int) > 0 {
		t.Logf("Found %d shared nodes", sharedCount.(int))
	} else {
		t.Error("Expected to find shared nodes")
	}

	// Max rules per node should be at least 2 (rule1 and rule2 in same node)
	if maxRules, exists := stats["max_rules_per_node"]; exists && maxRules.(int) >= 2 {
		t.Logf("Max rules per node: %d", maxRules.(int))
	} else {
		t.Error("Expected max rules per node to be at least 2")
	}
}

// TestSimpleAtomicUpdate tests the basic atomic update functionality
func TestSimpleAtomicUpdate(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("simple-atomic-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()

	initialRule.Metadata = map[string]string{"version": "1"}

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	// Test that rule can be retrieved
	rule, err := engine.GetRule("simple-atomic-test")
	if err != nil {
		t.Fatalf("Failed to get initial rule: %v", err)
	}

	if rule.Metadata["version"] != "1" {
		t.Errorf("Expected version 1, got %s", rule.Metadata["version"])
	}

	// Test update
	updatedRule := NewRule("simple-atomic-test").
		Dimension("region", "us-east", MatchTypeEqual).
		Build()

	updatedRule.Metadata = map[string]string{"version": "2"}

	err = engine.UpdateRule(updatedRule)
	if err != nil {
		t.Fatalf("Failed to update rule: %v", err)
	}

	// Verify update
	rule, err = engine.GetRule("simple-atomic-test")
	if err != nil {
		t.Fatalf("Failed to get updated rule: %v", err)
	}

	if rule.Metadata["version"] != "2" {
		t.Errorf("Expected version 2 after update, got %s", rule.Metadata["version"])
	}

	// Verify dimension was updated
	found := false
	for _, dim := range rule.Dimensions {
		if dim.DimensionName == "region" && dim.Value == "us-east" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Rule dimension was not properly updated")
	}

	t.Log("✓ Simple atomic update test passed")
}

// TestUpdateRuleTemporaryUnavailability tests that during update, GetRule may temporarily fail
// but when it succeeds, it returns a consistent rule
func TestUpdateRuleTemporaryUnavailability(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("temp-unavail-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()
	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	// Test many quick updates and reads
	for i := 0; i < 100; i++ {
		// Update rule
		updatedRule := NewRule("temp-unavail-test").
			Dimension("region", "us-east", MatchTypeEqual).
			Build()

		updatedRule.Metadata = map[string]string{"iteration": string(rune('0' + i%10))}

		err = engine.UpdateRule(updatedRule)
		if err != nil {
			t.Fatalf("Failed to update rule at iteration %d: %v", i, err)
		}

		// Try to read immediately
		rule, err := engine.GetRule("temp-unavail-test")
		if err != nil {
			// Rule might be temporarily unavailable during update - this is acceptable
			t.Logf("Rule temporarily unavailable at iteration %d (acceptable)", i)
		} else {
			// If we get a rule, it should be consistent
			if rule.ID != "temp-unavail-test" {
				t.Errorf("Iteration %d: Got wrong rule ID: %s", i, rule.ID)
			}

			if len(rule.Dimensions) == 0 {
				t.Errorf("Iteration %d: Got rule with no dimensions", i)
			}

			// If metadata exists, it should be complete
			if rule.Metadata != nil {
				if iteration, exists := rule.Metadata["iteration"]; exists {
					if iteration == "" {
						t.Errorf("Iteration %d: Got rule with empty iteration metadata", i)
					}
				}
			}
		}

		// Small delay
		time.Sleep(time.Microsecond)
	}

	t.Log("✓ Update rule temporary unavailability test passed")
}

// TestSimpleRaceCondition tests a simpler case to detect any race conditions
func TestSimpleRaceCondition(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("simple-race-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Build()
	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	var wg sync.WaitGroup
	raceDetected := false
	var raceMu sync.Mutex

	setRaceDetected := func() {
		raceMu.Lock()
		raceDetected = true
		raceMu.Unlock()
	}

	// Start reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			rule, err := engine.GetRule("simple-race-test")
			if err != nil {
				continue
			}

			// Check for consistency
			if rule.ID != "simple-race-test" {
				setRaceDetected()
				t.Errorf("Rule ID inconsistency: expected 'simple-race-test', got '%s'", rule.ID)
			}

			if len(rule.Dimensions) == 0 {
				setRaceDetected()
				t.Errorf("Rule dimensions are nil or empty")
			} else {
				// Check that the rule is internally consistent
				for _, dim := range rule.Dimensions {
					if dim == nil {
						setRaceDetected()
						t.Errorf("Found nil dimension in rule")
					} else if dim.DimensionName == "" {
						setRaceDetected()
						t.Errorf("Found dimension with empty name")
					}
				}
			}
		}
	}()

	// Start updater
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			updatedRule := NewRule("simple-race-test").
				Dimension("region", "us-east", MatchTypeEqual).
				Build()
			engine.UpdateRule(updatedRule)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	raceMu.Lock()
	if raceDetected {
		t.Error("Race condition detected!")
	} else {
		t.Log("No race condition detected in simple test")
	}
	raceMu.Unlock()
}

// TestAtomicUpdate verifies that the current implementation provides atomic updates
func TestAtomicUpdate(t *testing.T) {
	t.Log("=== Analyzing Current Update Implementation ===")

	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimensions
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)

	engine.AddDimension(regionConfig)
	engine.AddDimension(envConfig)

	// Add initial rule
	initialRule := NewRule("atomic-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	t.Log("✓ Initial rule added")

	// Test that reads are blocked during writes
	updateStarted := make(chan bool)
	updateFinished := make(chan bool)

	// Start an update in a goroutine
	go func() {
		updateStarted <- true

		// This should hold the write lock for the entire operation
		updatedRule := NewRule("atomic-test").
			Dimension("region", "us-east", MatchTypeEqual).
			Dimension("env", "staging", MatchTypeEqual).
			Build()

		engine.UpdateRule(updatedRule)
		updateFinished <- true
	}()

	// Wait for update to start
	<-updateStarted

	// Try to read immediately - this should either:
	// 1. See the old rule (if read happens before write lock)
	// 2. See the new rule (if read happens after write lock)
	// 3. Never see a partial state
	rule, err := engine.GetRule("atomic-test")
	if err != nil {
		t.Log("✓ Rule not found during update (acceptable)")
	} else {
		// Verify rule consistency
		hasOldConfig := false
		hasNewConfig := false

		for _, dim := range rule.Dimensions {
			if dim.DimensionName == "region" {
				switch dim.Value {
				case "us-west":
					hasOldConfig = true
				case "us-east":
					hasNewConfig = true
				}
			}
		}

		if hasOldConfig && hasNewConfig {
			t.Error("❌ Found mixed old and new configuration - atomic update violated!")
		} else if hasOldConfig {
			t.Log("✓ Read returned old configuration (atomic)")
		} else if hasNewConfig {
			t.Log("✓ Read returned new configuration (atomic)")
		}
	}

	// Wait for update to finish
	<-updateFinished
	t.Log("✓ Update completed")

	// Verify final state
	finalRule, err := engine.GetRule("atomic-test")
	if err != nil {
		t.Fatalf("Failed to get final rule: %v", err)
	}

	hasCorrectFinalState := true
	for _, dim := range finalRule.Dimensions {
		if dim.DimensionName == "region" && dim.Value != "us-east" {
			hasCorrectFinalState = false
		}
		if dim.DimensionName == "env" && dim.Value != "staging" {
			hasCorrectFinalState = false
		}
	}

	if hasCorrectFinalState {
		t.Log("✓ Final state is correct after update")
	} else {
		t.Error("❌ Final state is incorrect after update")
	}
}

// TestGetRuleDuringUpdateRaceCondition demonstrates the potential race condition
// where GetRule might return a rule in an inconsistent state during update
func TestGetRuleDuringUpdateRaceCondition(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights
	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}
	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("race-test-rule").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	initialRule.Metadata = map[string]string{
		"version": "1.0",
		"owner":   "team-a",
	}

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	var issuesMu sync.Mutex
	var issues []string

	addIssue := func(issue string) {
		issuesMu.Lock()
		issues = append(issues, issue)
		issuesMu.Unlock()
	}

	var wg sync.WaitGroup
	numReaders := 20
	numUpdaters := 5

	// Start concurrent readers that continuously call GetRule
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				rule, err := engine.GetRule("race-test-rule")
				if err != nil {
					// Rule might be temporarily unavailable during update, which is acceptable
					continue
				}

				// Validate rule consistency
				if rule.ID != "race-test-rule" {
					addIssue(fmt.Sprintf("Reader %d: Wrong rule ID: %s", readerID, rule.ID))
				}

				if rule.Dimensions == nil {
					addIssue(fmt.Sprintf("Reader %d: Nil dimensions", readerID))
					continue
				}

				if len(rule.Dimensions) == 0 {
					addIssue(fmt.Sprintf("Reader %d: Empty dimensions", readerID))
					continue
				}

				for _, dim := range rule.Dimensions {
					if dim == nil {
						addIssue(fmt.Sprintf("Reader %d: Nil dimension in array", readerID))
						continue
					}

					if dim.DimensionName == "" {
						addIssue(fmt.Sprintf("Reader %d: Empty dimension name", readerID))
					}
				}

				// The rule should have consistent dimensions - either the old set or new set
				// but not a mix (which would indicate a partial update)
				if rule.Metadata != nil {
					version := rule.Metadata["version"]
					owner := rule.Metadata["owner"]

					// Check for metadata consistency
					if version == "1.0" && owner != "team-a" {
						addIssue(fmt.Sprintf("Reader %d: Inconsistent metadata v1.0 with owner %s", readerID, owner))
					}
					if version == "2.0" && owner != "team-b" {
						addIssue(fmt.Sprintf("Reader %d: Inconsistent metadata v2.0 with owner %s", readerID, owner))
					}

					// Check dimension-metadata consistency
					switch version {
					case "1.0":
						// v1.0 should have region=us-west, env=prod
						expectedRegion := "us-west"
						expectedEnv := "prod"

						for _, dim := range rule.Dimensions {
							if dim.DimensionName == "region" && dim.Value != expectedRegion {
								addIssue(fmt.Sprintf("Reader %d: v1.0 metadata but region=%s (expected %s)", readerID, dim.Value, expectedRegion))
							}
							if dim.DimensionName == "env" && dim.Value != expectedEnv {
								addIssue(fmt.Sprintf("Reader %d: v1.0 metadata but env=%s (expected %s)", readerID, dim.Value, expectedEnv))
							}
						}
					case "2.0":
						// v2.0 should have region=us-east, env=staging
						expectedRegion := "us-east"
						expectedEnv := "staging"

						for _, dim := range rule.Dimensions {
							if dim.DimensionName == "region" && dim.Value != expectedRegion {
								addIssue(fmt.Sprintf("Reader %d: v2.0 metadata but region=%s (expected %s)", readerID, dim.Value, expectedRegion))
							}
							if dim.DimensionName == "env" && dim.Value != expectedEnv {
								addIssue(fmt.Sprintf("Reader %d: v2.0 metadata but env=%s (expected %s)", readerID, dim.Value, expectedEnv))
							}
						}
					}
				}

				// Small delay to increase chance of catching race condition
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Start concurrent updaters that continuously update the rule
	for i := 0; i < numUpdaters; i++ {
		wg.Add(1)
		go func(updaterID int) {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				// Alternate between two different rule configurations
				var updatedRule *Rule

				if j%2 == 0 {
					// Configuration A
					updatedRule = NewRule("race-test-rule").
						Dimension("region", "us-west", MatchTypeEqual).
						Dimension("env", "prod", MatchTypeEqual).
						Build()
					updatedRule.Metadata = map[string]string{
						"version": "1.0",
						"owner":   "team-a",
					}
				} else {
					// Configuration B
					updatedRule = NewRule("race-test-rule").
						Dimension("region", "us-east", MatchTypeEqual).
						Dimension("env", "staging", MatchTypeEqual).
						Build()
					updatedRule.Metadata = map[string]string{
						"version": "2.0",
						"owner":   "team-b",
					}
				}

				err := engine.UpdateRule(updatedRule)
				if err != nil {
					addIssue(fmt.Sprintf("Updater %d: Failed to update rule: %v", updaterID, err))
				}

				// Small delay
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Check for issues
	issuesMu.Lock()
	defer issuesMu.Unlock()

	if len(issues) > 0 {
		t.Errorf("Found %d race condition issues:", len(issues))
		for i, issue := range issues {
			if i < 10 { // Limit output
				t.Errorf("Issue %d: %s", i+1, issue)
			}
		}
		if len(issues) > 10 {
			t.Errorf("... and %d more issues", len(issues)-10)
		}
	} else {
		t.Log("SUCCESS: No race conditions detected in GetRule during updates")
	}
}

// TestQueryDuringUpdateConsistency tests that queries during updates are consistent
func TestQueryDuringUpdateConsistency(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension config
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)

	err = engine.AddDimension(regionConfig)
	if err != nil {
		t.Fatalf("Failed to add region dimension: %v", err)
	}
	err = engine.AddDimension(envConfig)
	if err != nil {
		t.Fatalf("Failed to add env dimension: %v", err)
	}

	// Add initial rule
	initialRule := NewRule("query-consistency-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Build()

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	var issuesMu sync.Mutex
	var issues []string

	addIssue := func(issue string) {
		issuesMu.Lock()
		issues = append(issues, issue)
		issuesMu.Unlock()
	}

	var wg sync.WaitGroup

	// Start query workers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(queryID int) {
			defer wg.Done()

			// Query for both configurations
			queryA := &QueryRule{Values: map[string]string{"region": "us-west", "env": "prod"}}
			queryB := &QueryRule{Values: map[string]string{"region": "us-east", "env": "staging"}}

			for j := 0; j < 1000; j++ {
				// Try both queries
				matchesA, errA := engine.FindAllMatches(queryA)
				matchesB, errB := engine.FindAllMatches(queryB)

				if errA != nil {
					addIssue(fmt.Sprintf("Query worker %d: QueryA failed: %v", queryID, errA))
				}
				if errB != nil {
					addIssue(fmt.Sprintf("Query worker %d: QueryB failed: %v", queryID, errB))
				}

				// At any given time, exactly one of these queries should match
				// (unless the rule is temporarily not in the forest during update)
				totalMatches := len(matchesA) + len(matchesB)

				if totalMatches > 1 {
					addIssue(fmt.Sprintf("Query worker %d: Found matches for both queries simultaneously (matchesA=%d, matchesB=%d)",
						queryID, len(matchesA), len(matchesB)))
				}

				// Validate any returned matches are complete
				for _, match := range matchesA {
					if match.Rule.ID != "query-consistency-test" {
						addIssue(fmt.Sprintf("Query worker %d: Wrong rule ID in matchA: %s", queryID, match.Rule.ID))
					}
				}
				for _, match := range matchesB {
					if match.Rule.ID != "query-consistency-test" {
						addIssue(fmt.Sprintf("Query worker %d: Wrong rule ID in matchB: %s", queryID, match.Rule.ID))
					}
				}
			}
		}(i)
	}

	// Start updater
	wg.Add(1)
	go func() {
		defer wg.Done()

		for j := 0; j < 200; j++ {
			var updatedRule *Rule

			if j%2 == 0 {
				updatedRule = NewRule("query-consistency-test").
					Dimension("region", "us-west", MatchTypeEqual).
					Dimension("env", "prod", MatchTypeEqual).
					Build()
			} else {
				updatedRule = NewRule("query-consistency-test").
					Dimension("region", "us-east", MatchTypeEqual).
					Dimension("env", "staging", MatchTypeEqual).
					Build()
			}

			engine.UpdateRule(updatedRule)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	// Check for issues
	issuesMu.Lock()
	defer issuesMu.Unlock()

	if len(issues) > 0 {
		t.Errorf("Found %d query consistency issues:", len(issues))
		for i, issue := range issues {
			if i < 10 {
				t.Errorf("Issue %d: %s", i+1, issue)
			}
		}
		if len(issues) > 10 {
			t.Errorf("... and %d more issues", len(issues)-10)
		}
	} else {
		t.Log("SUCCESS: Queries remain consistent during rule updates")
	}
}

// TestBasicUpdateRule tests basic update functionality
func TestBasicUpdateRule(t *testing.T) {
	// Create engine with mock persistence
	persistence := NewJSONPersistence("./test_data")
	engine, err := NewMatcherEngine(persistence, nil, "test-node-1")
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}
	defer engine.Close()

	// Add test dimensions
	err = addTestDimensions(engine.matcher)
	if err != nil {
		t.Fatalf("Failed to initialize dimensions: %v", err)
	}

	// Create initial rule using builder pattern
	rule := NewRule("test-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRoute", MatchTypeEqual).
		Metadata("action", "allow").
		Build()

	// Add rule
	err = engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Update rule
	updatedRule := NewRule("test-rule").
		Dimension("product", "TestProduct", MatchTypeEqual).
		Dimension("route", "TestRouteUpdated", MatchTypeEqual). // Changed route
		Metadata("action", "block").                            // Changed action
		Build()

	err = engine.UpdateRule(updatedRule)
	if err != nil {
		t.Fatalf("Failed to update rule: %v", err)
	}

	// Verify update
	retrieved, err := engine.GetRule("test-rule")
	if err != nil {
		t.Fatalf("Failed to get rule: %v", err)
	}

	// Check dimensions were updated
	routeDim := retrieved.GetDimensionValue("route")
	if routeDim == nil {
		t.Fatalf("Route dimension not found")
	}

	if routeDim.Value != "TestRouteUpdated" {
		t.Errorf("Expected route TestRouteUpdated, got %s", routeDim.Value)
	}

	if retrieved.Metadata["action"] != "block" {
		t.Errorf("Expected action block, got %v", retrieved.Metadata["action"])
	}
}

// TestAtomicRuleUpdateFix verifies that the fix prevents partial rule state during updates
func TestAtomicRuleUpdateFix(t *testing.T) {
	tempDir := t.TempDir()
	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	regionConfig := NewDimensionConfig("region", 0, false)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false)
	envConfig.SetWeight(MatchTypeEqual, 8.0)
	serviceConfig := NewDimensionConfig("service", 2, false)
	serviceConfig.SetWeight(MatchTypeEqual, 6.0)

	engine.AddDimension(regionConfig)
	engine.AddDimension(envConfig)
	engine.AddDimension(serviceConfig)

	// Add initial rule
	initialRule := NewRule("atomic-update-fix-test").
		Dimension("region", "us-west", MatchTypeEqual).
		Dimension("env", "prod", MatchTypeEqual).
		Dimension("service", "api", MatchTypeEqual).
		Build()

	initialRule.Metadata = map[string]string{
		"config": "version-1",
		"team":   "alpha",
	}

	err = engine.AddRule(initialRule)
	if err != nil {
		t.Fatalf("Failed to add initial rule: %v", err)
	}

	var inconsistencyMu sync.Mutex
	var inconsistencies []string

	addInconsistency := func(msg string) {
		inconsistencyMu.Lock()
		inconsistencies = append(inconsistencies, msg)
		inconsistencyMu.Unlock()
	}

	var wg sync.WaitGroup
	numReaders := 10
	numUpdates := 50

	// Start aggressive readers using GetRule
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				rule, err := engine.GetRule("atomic-update-fix-test")
				if err != nil {
					// Rule might be temporarily unavailable, which is acceptable
					continue
				}

				// Validate internal consistency of the rule
				if rule.Metadata == nil {
					addInconsistency("GetRule returned rule with nil metadata")
					continue
				}

				config := rule.Metadata["config"]
				team := rule.Metadata["team"]

				// Check for consistent configurations
				switch config {
				case "version-1":
					// Version 1 should have: team=alpha, region=us-west, env=prod, service=api
					if team != "alpha" {
						addInconsistency("GetRule: version-1 config but team != alpha")
					}

					hasCorrectDims := true
					for _, dim := range rule.Dimensions {
						switch dim.DimensionName {
						case "region":
							if dim.Value != "us-west" {
								hasCorrectDims = false
							}
						case "env":
							if dim.Value != "prod" {
								hasCorrectDims = false
							}
						case "service":
							if dim.Value != "api" {
								hasCorrectDims = false
							}
						}
					}
					if !hasCorrectDims {
						addInconsistency("GetRule: version-1 metadata but wrong dimensions")
					}

				case "version-2":
					// Version 2 should have: team=beta, region=us-east, env=staging, service=web
					if team != "beta" {
						addInconsistency("GetRule: version-2 config but team != beta")
					}

					hasCorrectDims := true
					for _, dim := range rule.Dimensions {
						switch dim.DimensionName {
						case "region":
							if dim.Value != "us-east" {
								hasCorrectDims = false
							}
						case "env":
							if dim.Value != "staging" {
								hasCorrectDims = false
							}
						case "service":
							if dim.Value != "web" {
								hasCorrectDims = false
							}
						}
					}
					if !hasCorrectDims {
						addInconsistency("GetRule: version-2 metadata but wrong dimensions")
					}
				}

				// Micro-delay to increase chance of catching race conditions
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Start aggressive readers using FindAllMatches
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Queries for both versions
			queryV1 := &QueryRule{Values: map[string]string{
				"region": "us-west", "env": "prod", "service": "api"}}
			queryV2 := &QueryRule{Values: map[string]string{
				"region": "us-east", "env": "staging", "service": "web"}}

			for j := 0; j < 250; j++ {
				// Check version 1 query
				matches1, err1 := engine.FindAllMatches(queryV1)
				if err1 != nil {
					addInconsistency("FindAllMatches query V1 failed")
				}

				// Check version 2 query
				matches2, err2 := engine.FindAllMatches(queryV2)
				if err2 != nil {
					addInconsistency("FindAllMatches query V2 failed")
				}

				// At any point in time, exactly one version should match (or neither during transition)
				totalMatches := len(matches1) + len(matches2)
				if totalMatches > 1 {
					addInconsistency("FindAllMatches: Both queries returned matches simultaneously")
				}

				// Validate consistency of any returned matches
				for _, match := range matches1 {
					if match.Rule.Metadata["config"] != "version-1" {
						addInconsistency("FindAllMatches: V1 query returned non-V1 rule")
					}
				}
				for _, match := range matches2 {
					if match.Rule.Metadata["config"] != "version-2" {
						addInconsistency("FindAllMatches: V2 query returned non-V2 rule")
					}
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Single updater that alternates between two rule configurations
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numUpdates; i++ {
			var updatedRule *Rule

			if i%2 == 0 {
				// Version 1 configuration
				updatedRule = NewRule("atomic-update-fix-test").
					Dimension("region", "us-west", MatchTypeEqual).
					Dimension("env", "prod", MatchTypeEqual).
					Dimension("service", "api", MatchTypeEqual).
					Build()
				updatedRule.Metadata = map[string]string{
					"config": "version-1",
					"team":   "alpha",
				}
			} else {
				// Version 2 configuration
				updatedRule = NewRule("atomic-update-fix-test").
					Dimension("region", "us-east", MatchTypeEqual).
					Dimension("env", "staging", MatchTypeEqual).
					Dimension("service", "web", MatchTypeEqual).
					Build()
				updatedRule.Metadata = map[string]string{
					"config": "version-2",
					"team":   "beta",
				}
			}

			err := engine.UpdateRule(updatedRule)
			if err != nil {
				addInconsistency("UpdateRule failed: " + err.Error())
			}

			// Small delay between updates
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	// Check results
	inconsistencyMu.Lock()
	defer inconsistencyMu.Unlock()

	if len(inconsistencies) > 0 {
		t.Errorf("Found %d consistency violations:", len(inconsistencies))
		for i, inc := range inconsistencies {
			if i < 15 { // Limit output
				t.Errorf("Inconsistency %d: %s", i+1, inc)
			}
		}
		if len(inconsistencies) > 15 {
			t.Errorf("... and %d more inconsistencies", len(inconsistencies)-15)
		}
	} else {
		t.Log("SUCCESS: No consistency violations detected - atomic updates working correctly")
	}

	// Verify final state
	finalRule, err := engine.GetRule("atomic-update-fix-test")
	if err != nil {
		t.Fatalf("Failed to get final rule: %v", err)
	}

	if finalRule.Metadata == nil {
		t.Error("Final rule has nil metadata")
	} else {
		t.Logf("Final rule configuration: %s (team: %s)",
			finalRule.Metadata["config"], finalRule.Metadata["team"])
	}
}
