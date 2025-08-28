package matcher

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// MatcherEngine provides a simple, high-level API for the rule matching system
type MatcherEngine struct {
	matcher      *InMemoryMatcher
	persistence  PersistenceInterface
	eventBroker  Broker // Changed from eventSub to eventBroker
	nodeID       string // Create forest index
	autoSaveStop chan bool
}

// NewMatcherEngine creates a new matcher engine with the specified persistence and event broker
func NewMatcherEngine(persistence PersistenceInterface, eventBroker Broker, nodeID string) (*MatcherEngine, error) {
	matcher, err := NewInMemoryMatcher(persistence, eventBroker, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create matcher: %w", err)
	}

	return &MatcherEngine{
		matcher:     matcher,
		persistence: persistence,
		eventBroker: eventBroker,
		nodeID:      nodeID,
	}, nil
}

// NewMatcherEngineWithDefaults creates a matcher engine with default JSON persistence
func NewMatcherEngineWithDefaults(dataDir string) (*MatcherEngine, error) {
	persistence := NewJSONPersistence(dataDir)
	// Generate a default node ID based on hostname or use a UUID
	nodeID := GenerateDefaultNodeID()
	return NewMatcherEngine(persistence, nil, nodeID)
}

// RuleBuilder provides a fluent API for building rules
type RuleBuilder struct {
	rule *Rule
}

// NewRule creates a new rule builder
func NewRule(id string) *RuleBuilder {
	return &RuleBuilder{
		rule: &Rule{
			ID:         id,
			Dimensions: make([]*DimensionValue, 0),
			Metadata:   make(map[string]string),
		},
	}
}

// NewRuleWithTenant creates a new rule builder for a specific tenant and application
func NewRuleWithTenant(id, tenantID, applicationID string) *RuleBuilder {
	return &RuleBuilder{
		rule: &Rule{
			ID:            id,
			TenantID:      tenantID,
			ApplicationID: applicationID,
			Dimensions:    make([]*DimensionValue, 0),
			Metadata:      make(map[string]string),
		},
	}
}

// Tenant sets the tenant ID for the rule
func (rb *RuleBuilder) Tenant(tenantID string) *RuleBuilder {
	rb.rule.TenantID = tenantID
	return rb
}

// Application sets the application ID for the rule
func (rb *RuleBuilder) Application(applicationID string) *RuleBuilder {
	rb.rule.ApplicationID = applicationID
	return rb
}

// Dimension adds a dimension to the rule being built
// The weight will be automatically populated from dimension configuration when the rule is added to the engine
func (rb *RuleBuilder) Dimension(name, value string, matchType MatchType) *RuleBuilder {
	dimValue := &DimensionValue{
		DimensionName: name,
		Value:         value,
		MatchType:     matchType,
	}
	rb.rule.Dimensions = append(rb.rule.Dimensions, dimValue)
	return rb
}

// ManualWeight sets a manual weight override for the rule
func (rb *RuleBuilder) ManualWeight(weight float64) *RuleBuilder {
	rb.rule.ManualWeight = &weight
	return rb
}

// Metadata adds metadata to the rule
func (rb *RuleBuilder) Metadata(key, value string) *RuleBuilder {
	rb.rule.Metadata[key] = value
	return rb
}

// Status sets the status of the rule
func (rb *RuleBuilder) Status(status RuleStatus) *RuleBuilder {
	rb.rule.Status = status
	return rb
}

// Build returns the constructed rule
func (rb *RuleBuilder) Build() *Rule {
	now := time.Now()
	if rb.rule.CreatedAt.IsZero() {
		rb.rule.CreatedAt = now
	}
	rb.rule.UpdatedAt = now

	// Set default status if not specified
	if rb.rule.Status == "" {
		rb.rule.Status = RuleStatusWorking
	}

	return rb.rule
}

// AddRule adds a rule to the engine
func (me *MatcherEngine) AddRule(rule *Rule) error {
	return me.matcher.AddRule(rule)
}

// UpdateRule updates an existing rule
func (me *MatcherEngine) UpdateRule(rule *Rule) error {
	return me.matcher.updateRule(rule)
}

// DeleteRule removes a rule by ID
func (me *MatcherEngine) DeleteRule(ruleID string) error {
	return me.matcher.DeleteRule(ruleID)
}

// AddDimension adds a new dimension configuration
func (me *MatcherEngine) AddDimension(config *DimensionConfig) error {
	return me.matcher.AddDimension(config)
}

// SetAllowDuplicateWeights configures whether rules with duplicate weights are allowed
// By default, duplicate weights are not allowed to ensure deterministic matching
func (me *MatcherEngine) SetAllowDuplicateWeights(allow bool) {
	me.matcher.mu.Lock()
	defer me.matcher.mu.Unlock()
	me.matcher.allowDuplicateWeights = allow
}

// FindBestMatch finds the best matching rule for a query
func (me *MatcherEngine) FindBestMatch(query *QueryRule) (*MatchResult, error) {
	return me.matcher.FindBestMatch(query)
}

// FindAllMatches finds all matching rules for a query
func (me *MatcherEngine) FindAllMatches(query *QueryRule) ([]*MatchResult, error) {
	return me.matcher.FindAllMatches(query)
}

// ListRules returns all rules with pagination
func (me *MatcherEngine) ListRules(offset, limit int) ([]*Rule, error) {
	return me.matcher.ListRules(offset, limit)
}

// ListDimensions returns all dimension configurations
func (me *MatcherEngine) ListDimensions() ([]*DimensionConfig, error) {
	return me.matcher.ListDimensions()
}

// GetStats returns current engine statistics
func (me *MatcherEngine) GetStats() *MatcherStats {
	return me.matcher.GetStats()
}

// Save saves the current state to persistence
func (me *MatcherEngine) Save() error {
	return me.matcher.SaveToPersistence()
}

// Health checks if the engine is healthy
func (me *MatcherEngine) Health() error {
	return me.matcher.Health()
}

// Close closes the engine and cleans up resources
func (me *MatcherEngine) Close() error {
	if me.autoSaveStop != nil {
		close(me.autoSaveStop)
	}
	return me.matcher.Close()
}

// AutoSave starts automatic saving at the specified interval
func (me *MatcherEngine) AutoSave(interval time.Duration) chan<- bool {
	stopChan := make(chan bool)
	me.autoSaveStop = stopChan

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := me.Save(); err != nil {
					// Log error but continue
					slog.Error("Auto-save error", "error", err)
				}
			case <-stopChan:
				return
			}
		}
	}()

	return stopChan
}

// BatchAddRules adds multiple rules in a single operation
func (me *MatcherEngine) BatchAddRules(rules []*Rule) error {
	for _, rule := range rules {
		if err := me.AddRule(rule); err != nil {
			return fmt.Errorf("failed to add rule %s: %w", rule.ID, err)
		}
	}
	return nil
}

// Convenience methods for quick rule creation

// AddSimpleRule creates a rule with all exact matches
func (me *MatcherEngine) AddSimpleRule(id string, dimensions map[string]string, manualWeight *float64) error {
	builder := NewRule(id)

	for dimName, value := range dimensions {
		// Use configured weight from dimension config
		builder.Dimension(dimName, value, MatchTypeEqual)
	}

	if manualWeight != nil {
		builder.ManualWeight(*manualWeight)
	}

	rule := builder.Build()
	return me.AddRule(rule)
}

// AddAnyRule creates a rule that matches any input with manual weight
func (me *MatcherEngine) AddAnyRule(id string, dimensionNames []string, manualWeight float64) error {
	builder := NewRule(id)

	for _, dimName := range dimensionNames {
		builder.Dimension(dimName, "", MatchTypeAny)
	}

	builder.ManualWeight(manualWeight)

	rule := builder.Build()
	return me.AddRule(rule)
}

// CreateQuery creates a query from a map of dimension values
func CreateQuery(values map[string]string) *QueryRule {
	return &QueryRule{
		Values:          values,
		IncludeAllRules: false, // Default to working rules only
	}
}

// CreateQueryWithTenant creates a query for a specific tenant and application
func CreateQueryWithTenant(tenantID, applicationID string, values map[string]string) *QueryRule {
	return &QueryRule{
		TenantID:        tenantID,
		ApplicationID:   applicationID,
		Values:          values,
		IncludeAllRules: false, // Default to working rules only
	}
}

// CreateQueryWithAllRules creates a query that includes all rules (working and draft)
func CreateQueryWithAllRules(values map[string]string) *QueryRule {
	return &QueryRule{
		Values:          values,
		IncludeAllRules: true,
	}
}

// CreateQueryWithAllRulesAndTenant creates a tenant-scoped query that includes all rules
func CreateQueryWithAllRulesAndTenant(tenantID, applicationID string, values map[string]string) *QueryRule {
	return &QueryRule{
		TenantID:        tenantID,
		ApplicationID:   applicationID,
		Values:          values,
		IncludeAllRules: true,
	}
}

// GetForestStats returns detailed forest index statistics
func (me *MatcherEngine) GetForestStats() map[string]interface{} {
	me.matcher.mu.RLock()
	defer me.matcher.mu.RUnlock()

	stats := make(map[string]interface{})

	for key, forestIndex := range me.matcher.forestIndexes {
		stats[key] = forestIndex.GetStats()
	}

	// Add summary stats
	stats["total_forests"] = len(me.matcher.forestIndexes)
	stats["total_rules"] = len(me.matcher.rules)

	return stats
}

// ClearCache clears the query cache
func (me *MatcherEngine) ClearCache() {
	me.matcher.cache.Clear()
}

// GetCacheStats returns cache statistics
func (me *MatcherEngine) GetCacheStats() map[string]interface{} {
	return me.matcher.cache.Stats()
}

// ValidateRule validates a rule before adding it
func (me *MatcherEngine) ValidateRule(rule *Rule) error {
	return me.matcher.validateRule(rule)
}

// RebuildIndex rebuilds the forest index (useful after bulk operations)
func (me *MatcherEngine) RebuildIndex() error {
	// Use the existing Rebuild method which is already tenant-aware
	return me.matcher.Rebuild()
}

// GenerateDefaultNodeID generates a default node ID based on hostname and random suffix
func GenerateDefaultNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Add 6-digit random suffix to ensure uniqueness
	randBytes := make([]byte, 3)
	rand.Read(randBytes)
	// Convert 3 bytes to a 6-digit decimal number (0-999999)
	randNum := int(randBytes[0])<<16 | int(randBytes[1])<<8 | int(randBytes[2])
	randNum = randNum % 1000000 // Ensure it's within 6 digits
	return fmt.Sprintf("%s-%06d", hostname, randNum)
}
