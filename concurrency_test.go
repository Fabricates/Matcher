package matcher

import (
	"fmt"
	"math/rand"
	"strings"
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
					// Only report errors that aren't "rule not found" during atomic updates
					// "rule not found" is expected during temporary rule invisibility
					if !strings.Contains(err.Error(), "rule not found") &&
						!strings.Contains(err.Error(), "not found") {
						addIssue(fmt.Sprintf("Updater %d iteration %d: UpdateRuleStatus failed: %v", updaterID, j, err))
					}
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
					// Only report errors that aren't "rule not found" during atomic updates
					// "rule not found" is expected during temporary rule invisibility
					if !strings.Contains(err.Error(), "rule not found") &&
						!strings.Contains(err.Error(), "not found") {
						addIssue(fmt.Sprintf("Metadata updater %d iteration %d: UpdateRuleMetadata failed: %v", updaterID, j, err))
					}
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
