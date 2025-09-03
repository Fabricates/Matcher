package matcher

import (
	"sync"
	"testing"
	"time"
)

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
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
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
	regionConfig := NewDimensionConfig("region", 0, false, 5.0)
	regionConfig.SetWeight(MatchTypeEqual, 10.0)
	envConfig := NewDimensionConfig("env", 1, false, 3.0)
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
