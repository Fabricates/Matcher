package matcher

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

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
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false, 3.0)
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
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false, 3.0)
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
