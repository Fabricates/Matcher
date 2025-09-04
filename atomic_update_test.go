package matcher

import (
	"sync"
	"testing"
	"time"
)

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
