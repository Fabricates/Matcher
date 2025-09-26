package matcher

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// logger is a package-level alias to the default slog logger. Use the
// contextual logging helpers (InfoContext, WarnContext, ErrorContext,
// DebugContext) when a context is available from the matcher instance.
var logger = slog.Default()

func SetLogger(log *slog.Logger) {
	if log != nil {
		logger = log
	}
}

// MatcherEngine provides a simple, high-level API for the rule matching system
type MatcherEngine struct {
	matcher      *MemoryMatcherEngine
	persistence  PersistenceInterface
	eventBroker  Broker // Changed from eventSub to eventBroker
	nodeID       string // Create forest index
	autoSaveStop chan bool
}

// NewMatcherEngine creates a new matcher engine with the specified persistence and event broker
// Note: ctx should not be cancelled in normal status
func NewMatcherEngine(ctx context.Context, persistence PersistenceInterface, broker Broker, nodeID string, dcs *DimensionConfigs, initialTimeout time.Duration) (*MatcherEngine, error) {
	matcher, err := newInMemoryMatcherEngine(ctx, persistence, broker, nodeID, dcs, initialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create matcher: %w", err)
	}

	logger.InfoContext(matcher.ctx, "created matcher engine", "node_id", nodeID)

	return &MatcherEngine{
		matcher:     matcher,
		persistence: persistence,
		eventBroker: broker,
		nodeID:      nodeID,
	}, nil
}

// NewMatcherEngineWithDefaults creates a matcher engine with default JSON persistence
func NewMatcherEngineWithDefaults(dataDir string) (*MatcherEngine, error) {
	persistence := NewJSONPersistence(dataDir)
	// Generate a default node ID based on hostname or use a UUID
	nodeID := GenerateDefaultNodeID()
	logger.InfoContext(context.Background(), "creating matcher engine with defaults", "data_dir", dataDir)
	return NewMatcherEngine(context.Background(), persistence, nil, nodeID, nil, 0)
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
			Dimensions: make(map[string]*DimensionValue),
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
			Dimensions:    make(map[string]*DimensionValue),
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
	if rb.rule.Dimensions == nil {
		rb.rule.Dimensions = make(map[string]*DimensionValue)
	}
	rb.rule.Dimensions[name] = dimValue
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
	logger.InfoContext(me.matcher.ctx, "engine.AddRule", "rule_id", rule.ID)
	return me.matcher.AddRule(rule)
}

// UpdateRule updates an existing rule
func (me *MatcherEngine) UpdateRule(rule *Rule) error {
	logger.InfoContext(me.matcher.ctx, "engine.UpdateRule", "rule_id", rule.ID)
	return me.matcher.updateRule(rule)
}

// UpdateRuleStatus updates only the status of an existing rule
func (me *MatcherEngine) UpdateRuleStatus(ruleID string, status RuleStatus) error {
	// Get the existing rule first
	rule, err := me.GetRule(ruleID)
	if err != nil {
		return fmt.Errorf("rule not found: %w", err)
	}

	// Create a copy and update the status
	updatedRule := &Rule{
		ID:            rule.ID,
		TenantID:      rule.TenantID,
		ApplicationID: rule.ApplicationID,
		Dimensions:    rule.Dimensions,
		Metadata:      rule.Metadata,
		Status:        status, // Update only the status
		CreatedAt:     rule.CreatedAt,
		UpdatedAt:     rule.UpdatedAt,
	}

	return me.UpdateRule(updatedRule)
}

// UpdateRuleMetadata updates only the metadata of an existing rule
func (me *MatcherEngine) UpdateRuleMetadata(ruleID string, metadata map[string]string) error {
	// Get the existing rule first
	rule, err := me.GetRule(ruleID)
	if err != nil {
		return fmt.Errorf("rule not found: %w", err)
	}

	// Create a copy and update the metadata
	updatedRule := &Rule{
		ID:            rule.ID,
		TenantID:      rule.TenantID,
		ApplicationID: rule.ApplicationID,
		Dimensions:    rule.Dimensions,
		Metadata:      metadata, // Update only the metadata
		Status:        rule.Status,
		CreatedAt:     rule.CreatedAt,
		UpdatedAt:     rule.UpdatedAt,
	}

	return me.UpdateRule(updatedRule)
}

// GetRule retrieves a rule by ID
func (me *MatcherEngine) GetRule(ruleID string) (*Rule, error) {
	me.matcher.mu.RLock()
	defer me.matcher.mu.RUnlock()

	rule, exists := me.matcher.rules[ruleID]
	if !exists {
		return nil, fmt.Errorf("rule with ID '%s' not found", ruleID)
	}

	// Return a copy to prevent external modification
	return rule.Clone(), nil
}

// DeleteRule removes a rule by ID
func (me *MatcherEngine) DeleteRule(ruleID string) error {
	logger.InfoContext(me.matcher.ctx, "engine.DeleteRule", "rule_id", ruleID)
	return me.matcher.DeleteRule(ruleID)
}

// GetDimensionConfigs returns deep cloned DimensionConfigs instance
func (me *MatcherEngine) GetDimensionConfigs() *DimensionConfigs {
	return me.matcher.GetDimensionConfigs()
}

// AddDimension adds a new dimension configuration
func (me *MatcherEngine) AddDimension(config *DimensionConfig) error {
	return me.matcher.AddDimension(config)
}

// SetAllowDuplicateWeights configures whether rules with duplicate weights are allowed
// By default, duplicate weights are not allowed to ensure deterministic matching
func (me *MatcherEngine) SetAllowDuplicateWeights(allow bool) {
	me.matcher.SetAllowDuplicateWeights(allow)
}

// FindBestMatch finds the best matching rule for a query
func (me *MatcherEngine) FindBestMatch(query *QueryRule) (*MatchResult, error) {
	logger.DebugContext(me.matcher.ctx, "engine.FindBestMatch", "query", query.Values)
	return me.matcher.FindBestMatch(query)
}

// FindBestMatchInBatch runs multiple queries under a single matcher read-lock and
// returns the best match for each query in the same order. This provides an
// atomic snapshot view for a group of queries with respect to concurrent
// updates.
func (me *MatcherEngine) FindBestMatchInBatch(queries ...*QueryRule) ([]*MatchResult, error) {
	logger.DebugContext(me.matcher.ctx, "engine.FindBestMatchInBatch", "num_queries", len(queries))
	return me.matcher.FindBestMatchInBatch(queries)
}

// FindAllMatches finds all matching rules for a query
func (me *MatcherEngine) FindAllMatches(query *QueryRule) ([]*MatchResult, error) {
	logger.DebugContext(me.matcher.ctx, "engine.FindAllMatches", "query", query.Values)
	return me.matcher.FindAllMatches(query)
}

// FindAllMatches finds all matching rules for a query
func (me *MatcherEngine) FindAllMatchesInBatch(query ...*QueryRule) ([][]*MatchResult, error) {
	logger.DebugContext(me.matcher.ctx, "engine.FindAllMatchesInBatch", "num_queries", len(query))
	return me.matcher.FindAllMatchesInBatch(query)
}

// LoadDimensions loads all dimensions in bulk
func (me *MatcherEngine) LoadDimensions(configs []*DimensionConfig) {
	me.matcher.LoadDimensions(configs)
}

// ListRules returns all rules with pagination
func (me *MatcherEngine) ListRules(offset, limit int) ([]*Rule, error) {
	logger.DebugContext(me.matcher.ctx, "engine.ListRules", "offset", offset, "limit", limit)
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
	logger.InfoContext(me.matcher.ctx, "engine.Save requested")
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
	logger.InfoContext(me.matcher.ctx, "engine closing")
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
					logger.ErrorContext(me.matcher.ctx, "Auto-save error", "error", err)
				}
			case <-stopChan:
				logger.InfoContext(me.matcher.ctx, "autosave stopped")
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

// Rebuild rebuilds the forest index (useful after bulk operations)
func (me *MatcherEngine) Rebuild() error {
	// Use the existing Rebuild method which is already tenant-aware
	return me.matcher.Rebuild()
}

func (me *MatcherEngine) SaveToPersistence() error {
	return me.matcher.SaveToPersistence()
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

// CreateQueryWithDynamicConfigs creates a query with custom dimension configurations
// This allows for dynamic weight adjustment per query without modifying the global configs
func CreateQueryWithDynamicConfigs(values map[string]string, dynamicConfigs *DimensionConfigs) *QueryRule {
	return &QueryRule{
		Values:                  values,
		IncludeAllRules:         false,
		DynamicDimensionConfigs: dynamicConfigs,
	}
}

// CreateQueryWithTenantAndDynamicConfigs creates a tenant-scoped query with custom dimension configurations
func CreateQueryWithTenantAndDynamicConfigs(tenantID, applicationID string, values map[string]string, dynamicConfigs *DimensionConfigs) *QueryRule {
	return &QueryRule{
		TenantID:                tenantID,
		ApplicationID:           applicationID,
		Values:                  values,
		IncludeAllRules:         false,
		DynamicDimensionConfigs: dynamicConfigs,
	}
}

// CreateQueryWithAllRulesAndDynamicConfigs creates a query that includes all rules and uses custom dimension configurations
func CreateQueryWithAllRulesAndDynamicConfigs(values map[string]string, dynamicConfigs *DimensionConfigs) *QueryRule {
	return &QueryRule{
		Values:                  values,
		IncludeAllRules:         true,
		DynamicDimensionConfigs: dynamicConfigs,
	}
}

// CreateQueryWithAllRulesTenantAndDynamicConfigs creates a comprehensive query with all options
func CreateQueryWithAllRulesTenantAndDynamicConfigs(tenantID, applicationID string, values map[string]string, dynamicConfigs *DimensionConfigs) *QueryRule {
	return &QueryRule{
		TenantID:                tenantID,
		ApplicationID:           applicationID,
		Values:                  values,
		IncludeAllRules:         true,
		DynamicDimensionConfigs: dynamicConfigs,
	}
}

// CreateQueryWithExcludedRules creates a query that excludes specific rules by ID
func CreateQueryWithExcludedRules(values map[string]string, excludeRuleIDs []string) *QueryRule {
	excludeMap := make(map[string]bool)
	for _, ruleID := range excludeRuleIDs {
		excludeMap[ruleID] = true
	}
	return &QueryRule{
		Values:          values,
		IncludeAllRules: false, // Default to working rules only
		ExcludeRules:    excludeMap,
	}
}

// CreateQueryWithAllRulesAndExcluded creates a query that includes all rules but excludes specific ones
func CreateQueryWithAllRulesAndExcluded(values map[string]string, excludeRuleIDs []string) *QueryRule {
	excludeMap := make(map[string]bool)
	for _, ruleID := range excludeRuleIDs {
		excludeMap[ruleID] = true
	}
	return &QueryRule{
		Values:          values,
		IncludeAllRules: true,
		ExcludeRules:    excludeMap,
	}
}

// CreateQueryWithTenantAndExcluded creates a tenant-scoped query with excluded rules
func CreateQueryWithTenantAndExcluded(tenantID, applicationID string, values map[string]string, excludeRuleIDs []string) *QueryRule {
	excludeMap := make(map[string]bool)
	for _, ruleID := range excludeRuleIDs {
		excludeMap[ruleID] = true
	}
	return &QueryRule{
		TenantID:        tenantID,
		ApplicationID:   applicationID,
		Values:          values,
		IncludeAllRules: false, // Default to working rules only
		ExcludeRules:    excludeMap,
	}
}

// CreateQueryWithAllRulesTenantAndExcluded creates a comprehensive query with tenant scope, all rules, and exclusions
func CreateQueryWithAllRulesTenantAndExcluded(tenantID, applicationID string, values map[string]string, excludeRuleIDs []string) *QueryRule {
	excludeMap := make(map[string]bool)
	for _, ruleID := range excludeRuleIDs {
		excludeMap[ruleID] = true
	}
	return &QueryRule{
		TenantID:        tenantID,
		ApplicationID:   applicationID,
		Values:          values,
		IncludeAllRules: true,
		ExcludeRules:    excludeMap,
	}
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
