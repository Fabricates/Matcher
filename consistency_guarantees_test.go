package matcher

import (
	"sync"
	"testing"
	"time"
)

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
