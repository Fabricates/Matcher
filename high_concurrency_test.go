package matcher

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestHighConcurrencyNoPartialRules - More intensive concurrency test
func TestHighConcurrencyNoPartialRules(t *testing.T) {
	tempDir := t.TempDir()

	engine, err := NewMatcherEngineWithDefaults(tempDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Allow duplicate weights for this test
	engine.SetAllowDuplicateWeights(true)

	// Add dimension configurations
	for i, dimName := range []string{"region", "env", "service", "version", "tier"} {
		config := NewDimensionConfig(dimName, i, false)
		config.SetWeight(MatchTypeEqual, float64(10+i*2))
		err = engine.AddDimension(config)
		if err != nil {
			t.Fatalf("Failed to add dimension %s: %v", dimName, err)
		}
	}

	var issuesMu sync.Mutex
	var issues []string

	addIssue := func(issue string) {
		issuesMu.Lock()
		issues = append(issues, issue)
		issuesMu.Unlock()
	}

	var wg sync.WaitGroup

	// Higher concurrency numbers
	numRuleWorkers := 20
	numQueryWorkers := 20
	rulesPerWorker := 10

	// Start aggressive query workers
	for i := 0; i < numQueryWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			queries := []*QueryRule{
				{Values: map[string]string{"region": "us-west", "env": "prod"}},
				{Values: map[string]string{"service": "api", "tier": "web"}},
				{Values: map[string]string{"region": "us-east", "version": "v1.0"}},
				{Values: map[string]string{"env": "staging", "service": "worker"}},
			}

			startTime := time.Now()
			queryCount := 0

			for time.Since(startTime) < 3*time.Second {
				query := queries[queryCount%len(queries)]

				matches, err := engine.FindAllMatches(query)
				if err != nil {
					addIssue(fmt.Sprintf("High-concurrency query worker %d: FindAllMatches error: %v", workerID, err))
					return
				}

				queryCount++

				// Aggressive validation of each match
				for matchIdx, match := range matches {
					rule := match.Rule

					// Basic completeness checks
					if rule.ID == "" {
						addIssue(fmt.Sprintf("Query worker %d match %d: Empty rule ID", workerID, matchIdx))
					}

					if rule.Dimensions == nil {
						addIssue(fmt.Sprintf("Query worker %d match %d: Nil dimensions for rule %s", workerID, matchIdx, rule.ID))
						continue
					}

					// Deep validation of dimensions
					for dimIdx, dim := range rule.Dimensions {
						if dim == nil {
							addIssue(fmt.Sprintf("Query worker %d match %d: Nil dimension %d in rule %s", workerID, matchIdx, dimIdx, rule.ID))
							continue
						}

						if dim.DimensionName == "" {
							addIssue(fmt.Sprintf("Query worker %d match %d: Empty dimension name at index %d in rule %s", workerID, matchIdx, dimIdx, rule.ID))
						}
					}

					// Validate rule actually matches the query
					matchesQuery := false
					for _, dim := range rule.Dimensions {
						if queryValue, exists := query.Values[dim.DimensionName]; exists {
							switch dim.MatchType {
							case MatchTypeEqual:
								if dim.Value == queryValue {
									matchesQuery = true
								}
							case MatchTypeAny:
								matchesQuery = true
							case MatchTypePrefix:
								if len(queryValue) >= len(dim.Value) && queryValue[:len(dim.Value)] == dim.Value {
									matchesQuery = true
								}
							case MatchTypeSuffix:
								if len(queryValue) >= len(dim.Value) && queryValue[len(queryValue)-len(dim.Value):] == dim.Value {
									matchesQuery = true
								}
							}
							if matchesQuery {
								break
							}
						}
					}

					// For rules with dimensions, at least one should match
					if len(rule.Dimensions) > 0 && !matchesQuery {
						addIssue(fmt.Sprintf("Query worker %d match %d: Rule %s doesn't actually match query", workerID, matchIdx, rule.ID))
					}
				}

				// No delay - maximum pressure
			}

			t.Logf("High-concurrency query worker %d completed %d queries", workerID, queryCount)
		}(i)
	}

	// Start aggressive rule manipulation workers
	for i := 0; i < numRuleWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			values := []string{"us-west", "us-east", "eu-west", "prod", "staging", "dev", "api", "web", "worker", "v1.0", "v2.0", "v3.0"}

			for j := 0; j < rulesPerWorker; j++ {
				ruleID := fmt.Sprintf("high-conc-rule-%d-%d", workerID, j)

				// Add rule
				rule := NewRule(ruleID).
					Dimension("region", values[j%len(values)], MatchTypeEqual).
					Dimension("env", values[(j+1)%len(values)], MatchTypeEqual).
					Dimension("service", values[(j+2)%len(values)], MatchTypeEqual).
					Build()

				weight := float64(1000 + workerID*100 + j)
				rule.ManualWeight = &weight

				if err := engine.AddRule(rule); err != nil {
					addIssue(fmt.Sprintf("Rule worker %d: Failed to add rule %s: %v", workerID, ruleID, err))
					continue
				}

				// Immediately try to update it
				rule.Status = RuleStatusDraft
				rule.Metadata = map[string]string{
					"worker":    fmt.Sprintf("worker-%d", workerID),
					"iteration": fmt.Sprintf("%d", j),
				}

				if err := engine.UpdateRule(rule); err != nil {
					addIssue(fmt.Sprintf("Rule worker %d: Failed to update rule %s: %v", workerID, ruleID, err))
				}

				// Maybe delete it
				if j%3 == 0 {
					if err := engine.DeleteRule(ruleID); err != nil {
						addIssue(fmt.Sprintf("Rule worker %d: Failed to delete rule %s: %v", workerID, ruleID, err))
					}
				}

				// No delay - maximum pressure
			}
		}(i)
	}

	wg.Wait()

	// Check for issues
	issuesMu.Lock()
	defer issuesMu.Unlock()

	if len(issues) > 0 {
		t.Errorf("Found %d issues under high concurrency:", len(issues))
		for i, issue := range issues {
			if i < 20 { // Limit output to first 20 issues
				t.Errorf("Issue %d: %s", i+1, issue)
			}
		}
		if len(issues) > 20 {
			t.Errorf("... and %d more issues", len(issues)-20)
		}
	} else {
		t.Logf("SUCCESS: No partial rules found under high concurrency stress test")
	}
}
