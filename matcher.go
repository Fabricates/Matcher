package matcher

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// InMemoryMatcher implements the core matching logic using forest indexes
type InMemoryMatcher struct {
	forestIndexes         *sync.Map // tenant_app_key -> *ForestIndex
	dimensions            *sync.Map // dimension_name -> *DimensionConfig (global or scoped)
	rules                 *sync.Map // rule_id -> *Rule
	stats                 *MatcherStats
	cache                 *QueryCache
	persistence           PersistenceInterface
	broker                Broker // Changed from eventSub to eventBroker
	eventsChan            chan *Event
	nodeID                string    // Node identifier for filtering events
	allowDuplicateWeights bool      // When false (default), prevents rules with same weight
	forestLocks           *sync.Map // tenant_app_key -> *sync.RWMutex
	mu                    *sync.RWMutex
	ctx                   context.Context
	cancel                context.CancelFunc
}

// CreateInMemoryMatcher creates an in-memory matcher
func NewInMemoryMatcher(persistence PersistenceInterface, broker Broker, nodeID string) (*InMemoryMatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	matcher := &InMemoryMatcher{
		forestIndexes: &sync.Map{},
		dimensions:    &sync.Map{},
		rules:         &sync.Map{},
		stats: &MatcherStats{
			LastUpdated: time.Now(),
		},
		cache:       NewQueryCache(1000, 10*time.Minute), // 1000 entries, 10 min TTL
		persistence: persistence,
		broker:      broker,
		forestLocks: &sync.Map{},
		mu:          &sync.RWMutex{},
		eventsChan:  make(chan *Event, 100),
		nodeID:      nodeID,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize with data from persistence
	if err := matcher.loadFromPersistence(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load data from persistence: %w", err)
	}

	// Start event subscription if provided
	if broker != nil {
		if err := matcher.startEventSubscription(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to start event subscription: %w", err)
		}
	}

	return matcher, nil
}

// getTenantKey generates a unique key for tenant and application combination
func (m *InMemoryMatcher) getTenantKey(tenantID, applicationID string) string {
	if tenantID == "" && applicationID == "" {
		return "default"
	}
	return fmt.Sprintf("%s:%s", tenantID, applicationID)
}

// getDimensionsMap converts sync.Map to regular map for functions that need it
func (m *InMemoryMatcher) getDimensionsMap() map[string]*DimensionConfig {
	result := make(map[string]*DimensionConfig)
	return rlock(m.mu, func() map[string]*DimensionConfig {
		m.dimensions.Range(func(key, value any) bool {
			result[key.(string)] = value.(*DimensionConfig)
			return true
		})
		return result
	})
}

// syncMapLen counts entries in a sync.Map
func syncMapLen(sm *sync.Map) int {
	count := 0
	sm.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// updateStats safely updates the matcher statistics
func (m *InMemoryMatcher) updateStats() {
	wlock(m.mu, func() any {
		m.stats.TotalRules = syncMapLen(m.rules)
		m.stats.TotalDimensions = syncMapLen(m.dimensions)
		m.stats.LastUpdated = time.Now()
		return nil
	})
}

// getOrCreateForestIndex gets or creates a forest index for the specified tenant/application
func (m *InMemoryMatcher) getOrCreateForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)

	if forestIndexVal, exists := m.forestIndexes.Load(key); exists {
		return forestIndexVal.(*ForestIndex)
	}

	// Create new forest index for this tenant/application
	return m.wforest(key, func() any {
		newForest := CreateRuleForestWithTenant(tenantID, applicationID, m.getDimensionsMap())
		forestIndex := &ForestIndex{RuleForest: newForest}
		m.forestIndexes.Store(key, forestIndex)
		return forestIndex
	}).(*ForestIndex)
}

// getForestIndex gets the forest index for the specified tenant/application (read-only)
func (m *InMemoryMatcher) getForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)

	return m.rforest(key, func() any {
		if forestIndexVal, exists := m.forestIndexes.Load(key); exists {
			return forestIndexVal.(*ForestIndex)
		}
		return nil
	}).(*ForestIndex)
}

// loadFromPersistence loads rules and dimensions from persistence layer
func (m *InMemoryMatcher) loadFromPersistence() error {
	// Load dimension configurations
	configs, err := m.persistence.LoadDimensionConfigs(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load dimension configs: %w", err)
	}

	// Initialize dimensions
	for _, config := range configs {
		m.dimensions.Store(config.Name, config)
		// Initialize dimensions for the appropriate tenant/application forest
		forestIndex := m.getOrCreateForestIndex(config.TenantID, config.ApplicationID)
		forestIndex.InitializeDimension(config.Name)
	}

	// Load rules
	rules, err := m.persistence.LoadRules(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load rules: %w", err)
	}

	// Add rules to appropriate indexes
	for _, rule := range rules {
		m.rules.Store(rule.ID, rule)

		// Add to tenant-specific tracking
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)

		// Add to appropriate forest index
		m.wforest(key, func() any {
			forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
			forestIndex.AddRule(rule)
			return nil
		})
	}

	// Count rules in sync.Map
	m.updateStats()

	return nil
}

// startEventSubscription starts listening for events from the event subscriber
func (m *InMemoryMatcher) startEventSubscription() error {
	if err := m.broker.Subscribe(m.ctx, m.eventsChan); err != nil {
		return fmt.Errorf("failed to subscribe to events: %w", err)
	}

	go m.handleEvents()
	return nil
}

// handleEvents processes events from the event channel
func (m *InMemoryMatcher) handleEvents() {
	for {
		select {
		case event := <-m.eventsChan:
			// Filter out events from this node to avoid processing our own messages
			if event.NodeID == m.nodeID {
				continue
			}

			if err := m.processEvent(event); err != nil {
				// Log error but continue processing
				fmt.Printf("Error processing event: %v\n", err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// processEvent processes a single event
func (m *InMemoryMatcher) processEvent(event *Event) error {
	switch event.Type {
	case EventTypeRuleAdded, EventTypeRuleUpdated:
		ruleEvent, ok := event.Data.(*RuleEvent)
		if !ok {
			return fmt.Errorf("invalid rule event data")
		}
		return m.updateRule(ruleEvent.Rule)

	case EventTypeRuleDeleted:
		ruleEvent, ok := event.Data.(*RuleEvent)
		if !ok {
			return fmt.Errorf("invalid rule event data")
		}
		return m.deleteRule(ruleEvent.Rule.ID)

	case EventTypeDimensionAdded, EventTypeDimensionUpdated:
		dimEvent, ok := event.Data.(*DimensionEvent)
		if !ok {
			return fmt.Errorf("invalid dimension event data")
		}
		return m.updateDimension(dimEvent.Dimension)

	case EventTypeDimensionDeleted:
		dimEvent, ok := event.Data.(*DimensionEvent)
		if !ok {
			return fmt.Errorf("invalid dimension event data")
		}
		return m.deleteDimension(dimEvent.Dimension.Name)

	case EventTypeRebuild:
		// For rebuild events, reload entire state from persistence
		return m.Rebuild()

	default:
		return fmt.Errorf("unknown event type: %s", event.Type)
	}
}

func (m *InMemoryMatcher) SetAllowDuplicateWeights(allow bool) {
	m.allowDuplicateWeights = allow
}

// AddRule adds a new rule to the matcher
func (m *InMemoryMatcher) AddRule(rule *Rule) error {
	// Validate rule
	if err := m.validateRule(rule); err != nil {
		return fmt.Errorf("invalid rule: %w", err)
	}

	// Set timestamps
	now := time.Now()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}
	rule.UpdatedAt = now

	// Add to internal structures
	m.rules.Store(rule.ID, rule)

	// Add to tenant-specific tracking
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)

	// Add to appropriate forest index with per-forest locking
	forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
	m.wforest(key, func() any {
		forestIndex.AddRule(rule)
		return nil
	})

	// Clear cache since we added a new rule
	m.cache.Clear()

	// Update stats (with global write lock)
	m.updateStats()

	// Publish event to message queue
	if m.broker != nil {
		event := &Event{
			Type:      EventTypeRuleAdded,
			Timestamp: now,
			NodeID:    m.nodeID,
			Data: &RuleEvent{
				Rule: rule,
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	return nil
}

// UpdateRule updates an existing rule (public method)
func (m *InMemoryMatcher) UpdateRule(rule *Rule) error {
	return m.updateRule(rule)
}

func (m *InMemoryMatcher) GetForestStats() map[string]interface{} {
	stats := make(map[string]any)
	m.forestIndexes.Range(func(key any, value any) bool {
		stats[key.(string)] = value.(*ForestIndex).GetStats()
		return true
	})
	return stats
}

// GetRule retrieves a rule by ID (public method)
func (m *InMemoryMatcher) GetRule(ruleID string) (*Rule, error) {
	ruleVal, exists := m.rules.Load(ruleID)
	if !exists {
		return nil, fmt.Errorf("rule with ID '%s' not found", ruleID)
	}
	rule := ruleVal.(*Rule)

	// Return a copy to prevent external modification
	ruleCopy := &Rule{
		ID:            rule.ID,
		TenantID:      rule.TenantID,
		ApplicationID: rule.ApplicationID,
		Dimensions:    make([]*DimensionValue, len(rule.Dimensions)),
		Metadata:      make(map[string]string),
		Status:        rule.Status,
		CreatedAt:     rule.CreatedAt,
		UpdatedAt:     rule.UpdatedAt,
	}

	// Deep copy dimensions
	for i, dim := range rule.Dimensions {
		ruleCopy.Dimensions[i] = &DimensionValue{
			DimensionName: dim.DimensionName,
			Value:         dim.Value,
			MatchType:     dim.MatchType,
		}
	}

	// Copy metadata
	for k, v := range rule.Metadata {
		ruleCopy.Metadata[k] = v
	}

	// Copy manual weight if it exists
	if rule.ManualWeight != nil {
		weight := *rule.ManualWeight
		ruleCopy.ManualWeight = &weight
	}

	return ruleCopy, nil
}

// updateRule updates an existing rule
func (m *InMemoryMatcher) updateRule(rule *Rule) error {
	// Validate rule (this should be done without holding locks)
	if err := m.validateRule(rule); err != nil {
		return fmt.Errorf("invalid rule: %w", err)
	}

	// Set update timestamp
	rule.UpdatedAt = time.Now()

	// Get old rule and forest index info before any modifications
	var oldRule *Rule
	if v, exists := m.rules.Load(rule.ID); exists {
		oldRule = v.(*Rule)
	}

	// Get the new tenant key and ensure forest exists (without locking forest operations yet)
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)

	// Ensure forest exists without doing operations on it yet
	// We'll lock it separately for actual operations
	if _, exists := m.forestIndexes.Load(key); !exists {
		// Create new forest index for this tenant/application (with temporary lock)
		m.wforest(key, func() any {
			if _, exists := m.forestIndexes.Load(key); !exists {
				newForest := CreateRuleForestWithTenant(rule.TenantID, rule.ApplicationID, m.getDimensionsMap())
				forestIndex := &ForestIndex{RuleForest: newForest}
				m.forestIndexes.Store(key, forestIndex)
			}
			return nil
		})
	}

	// Get forest index (now we know it exists)
	forestIndexVal, _ := m.forestIndexes.Load(key)
	forestIndex := forestIndexVal.(*ForestIndex)

	// CRITICAL ATOMIC UPDATE STRATEGY:
	// Use per-forest locking to ensure atomic operations within each forest.
	// Remove from m.rules during update to prevent visibility of both versions.

	// Step 1: Remove rule from m.rules if it exists (makes it invisible to all queries)
	if oldRule != nil {
		m.rules.Delete(rule.ID)
	}

	// Step 2: Perform forest operations with proper per-forest locking
	m.wforest(key, func() any {
		if oldRule != nil {
			forestIndex.RemoveRule(oldRule)
		}
		forestIndex.AddRule(rule)
		return nil
	})

	// Step 3: Only now make the updated rule visible by adding to m.rules
	m.rules.Store(rule.ID, rule)

	// Step 4: Clear cache
	m.cache.Clear()

	// Step 5: Update stats
	m.updateStats()

	// Step 6: Publish event to message queue
	if m.broker != nil {
		event := &Event{
			Type:      EventTypeRuleUpdated,
			Timestamp: time.Now(),
			NodeID:    m.nodeID,
			Data: &RuleEvent{
				Rule: rule,
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	return nil
}

// DeleteRule removes a rule from the matcher
func (m *InMemoryMatcher) DeleteRule(ruleID string) error {
	return m.deleteRule(ruleID)
}

// deleteRule removes a rule from the matcher (internal method)
func (m *InMemoryMatcher) deleteRule(ruleID string) error {
	// Get rule info (under read lock)
	ruleVal, exists := m.rules.Load(ruleID)
	if !exists {
		return fmt.Errorf("rule not found: %s", ruleID)
	}
	rule := ruleVal.(*Rule)

	// Remove from appropriate forest index with per-forest locking
	forestIndex := m.getForestIndex(rule.TenantID, rule.ApplicationID)
	if forestIndex != nil {
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
		m.wforest(key, func() any {
			forestIndex.RemoveRule(rule)
			return nil
		})
	}

	// Remove from rules map
	m.rules.Delete(ruleID)

	// Clear cache
	m.cache.Clear()

	// Update stats
	m.updateStats()

	// Publish event to message queue
	if m.broker != nil {
		event := &Event{
			Type:      EventTypeRuleDeleted,
			Timestamp: time.Now(),
			NodeID:    m.nodeID,
			Data: &RuleEvent{
				Rule: rule,
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	return nil
}

// AddDimension adds a new dimension configuration
func (m *InMemoryMatcher) AddDimension(config *DimensionConfig) error {
	return m.updateDimension(config)
}

// updateDimension updates dimension configuration
func (m *InMemoryMatcher) updateDimension(config *DimensionConfig) error {
	// Validate dimension config
	if config.Name == "" {
		return fmt.Errorf("dimension name cannot be empty")
	}

	// Add to configurations
	m.dimensions.Store(config.Name, config)

	// Initialize forest for this dimension in appropriate tenant/application context
	forestIndex := m.getOrCreateForestIndex(config.TenantID, config.ApplicationID)
	forestIndex.InitializeDimension(config.Name)

	// Update stats
	m.updateStats()

	// Publish event to message queue
	if m.broker != nil {
		event := &Event{
			Type:      EventTypeDimensionAdded,
			Timestamp: time.Now(),
			NodeID:    m.nodeID,
			Data: &DimensionEvent{
				Dimension: config,
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	return nil
}

// deleteDimension removes a dimension configuration
func (m *InMemoryMatcher) deleteDimension(dimensionName string) error {
	// Get the dimension config before deleting it for the event
	configVal, exists := m.dimensions.Load(dimensionName)
	var config *DimensionConfig
	if exists {
		config = configVal.(*DimensionConfig)
	}

	m.dimensions.Delete(dimensionName)

	// Note: We don't remove the forest here as it might still contain rules
	// In production, you might want to handle this more carefully

	// Update stats
	m.updateStats()

	// Publish event to message queue if dimension existed
	if exists && m.broker != nil {
		event := &Event{
			Type:      EventTypeDimensionDeleted,
			Timestamp: time.Now(),
			NodeID:    m.nodeID,
			Data: &DimensionEvent{
				Dimension: config,
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	return nil
}

// FindBestMatch finds the best matching rule for a query
func (m *InMemoryMatcher) FindBestMatch(query *QueryRule) (*MatchResult, error) {
	start := time.Now()

	// Check cache first
	if result := m.cache.Get(query); result != nil {
		m.updateCacheStats(true)
		// Update stats without deadlock risk
		m.updateQueryStats(start)
		return result, nil
	}

	m.updateCacheStats(false)

	// Find all matches
	matches, err := m.FindAllMatches(query)
	if err != nil {
		m.updateQueryStats(start)
		return nil, err
	}

	if len(matches) == 0 {
		m.updateQueryStats(start)
		return nil, nil
	}

	best := matches[0] // already sorted

	// Cache the result
	m.cache.Set(query, best)

	// Update stats after all operations complete
	m.updateQueryStats(start)

	return best, nil
}

// FindAllMatches finds all matching rules for a query
func (m *InMemoryMatcher) FindAllMatches(query *QueryRule) ([]*MatchResult, error) {
	// Get candidate rules from appropriate forest index
	// Since we hold the main matcher lock, we can access the forest directly
	key := m.getTenantKey(query.TenantID, query.ApplicationID)
	var forestIndex *ForestIndex
	if forestIndexVal, exists := m.forestIndexes.Load(key); exists {
		forestIndex = forestIndexVal.(*ForestIndex)
	}
	var candidates []RuleWithWeight

	if forestIndex != nil {
		// Lock the specific forest for reading
		candidates = m.rforest(key, func() any {
			return forestIndex.FindCandidateRules(query)
		}).([]RuleWithWeight)
	} else {
		// If no specific tenant forest, return empty results
		candidates = []RuleWithWeight{}
	}

	var matches []*MatchResult

	// ATOMIC CONSISTENCY: Double-check approach to prevent race conditions
	// For each candidate from forest, verify it actually matches the query dimensions
	// AND exists in m.rules AND its dimensions in m.rules still match the query
	for _, candidate := range candidates {
		// Check 1: Rule must exist in m.rules (authoritative source)
		actualRuleVal, exists := m.rules.Load(candidate.Rule.ID)
		if !exists {
			continue // Skip rules that don't exist in m.rules (being updated)
		}
		actualRule := actualRuleVal.(*Rule)

		// Check 2: The rule from m.rules must actually match this query
		// This prevents returning rules that matched old dimensions but not current ones
		if !m.isFullMatch(actualRule, query) {
			continue // Skip rules whose current dimensions don't match this query
		}

		// Check 3: Verify the candidate rule from forest has same dimensions as m.rules
		// This catches cases where forest has stale entries during updates
		if !m.dimensionsEqual(candidate.Rule, actualRule) {
			continue // Skip stale forest entries
		}

		matchedDims := m.countMatchedDimensions(actualRule, query)

		matches = append(matches, &MatchResult{
			Rule:        actualRule, // Always use the current rule from m.rules
			TotalWeight: candidate.Weight,
			MatchedDims: matchedDims,
		})
	}
	return matches, nil
}

// dimensionsEqual checks if two rules have identical dimensions
func (m *InMemoryMatcher) dimensionsEqual(rule1, rule2 *Rule) bool {
	if len(rule1.Dimensions) != len(rule2.Dimensions) {
		return false
	}

	// Create maps for comparison
	dims1 := make(map[string]*DimensionValue)
	dims2 := make(map[string]*DimensionValue)

	for _, dim := range rule1.Dimensions {
		dims1[dim.DimensionName] = dim
	}
	for _, dim := range rule2.Dimensions {
		dims2[dim.DimensionName] = dim
	}

	if len(dims1) != len(dims2) {
		return false
	}

	for key, dim1 := range dims1 {
		dim2, exists := dims2[key]
		if !exists || dim1.Value != dim2.Value || dim1.MatchType != dim2.MatchType {
			return false
		}
	}

	return true
}

// isFullMatch checks if a rule fully matches a query
func (m *InMemoryMatcher) isFullMatch(rule *Rule, query *QueryRule) bool {
	// First check tenant context - rules must match the query's tenant/application context
	if !rule.MatchesTenantContext(query.TenantID, query.ApplicationID) {
		return false
	}

	// Check each dimension in the rule
	for _, dimValue := range rule.Dimensions {
		queryValue, hasQueryValue := query.Values[dimValue.DimensionName]

		// If dimension is required but not in query, no match
		if dimConfigVal, exists := m.dimensions.Load(dimValue.DimensionName); exists {
			dimConfig := dimConfigVal.(*DimensionConfig)
			if dimConfig.Required && !hasQueryValue {
				return false
			}
		}

		// If we have a query value, check if it matches
		if hasQueryValue {
			if !m.matchesDimension(dimValue, queryValue) {
				return false
			}
		}
	}

	return true
}

// matchesDimension checks if a dimension value matches the query value
func (m *InMemoryMatcher) matchesDimension(dimValue *DimensionValue, queryValue string) bool {
	switch dimValue.MatchType {
	case MatchTypeEqual:
		return dimValue.Value == queryValue
	case MatchTypeAny:
		return true
	case MatchTypePrefix:
		return len(queryValue) >= len(dimValue.Value) && queryValue[:len(dimValue.Value)] == dimValue.Value
	case MatchTypeSuffix:
		return len(queryValue) >= len(dimValue.Value) && queryValue[len(queryValue)-len(dimValue.Value):] == dimValue.Value
	default:
		return false
	}
}

// countMatchedDimensions counts how many dimensions matched in the query
func (m *InMemoryMatcher) countMatchedDimensions(rule *Rule, query *QueryRule) int {
	count := 0
	for _, dimValue := range rule.Dimensions {
		if queryValue, exists := query.Values[dimValue.DimensionName]; exists {
			if m.matchesDimension(dimValue, queryValue) {
				count++
			}
		}
	}
	return count
}

// validateRule validates a rule before adding it
func (m *InMemoryMatcher) validateRule(rule *Rule) error {
	if rule.ID == "" {
		return fmt.Errorf("rule ID cannot be empty")
	}

	if len(rule.Dimensions) == 0 {
		return fmt.Errorf("rule must have at least one dimension")
	}

	// Enforce dimension consistency by default
	if err := m.validateDimensionConsistency(rule); err != nil {
		return err
	}

	// Check for weight conflicts
	if err := m.validateWeightConflict(rule); err != nil {
		return err
	}

	// Validate each dimension
	for _, dimValue := range rule.Dimensions {
		if dimValue.DimensionName == "" {
			return fmt.Errorf("dimension name cannot be empty")
		}
	}

	return nil
}

// validateDimensionConsistency ensures rule dimensions match the configured dimensions
func (m *InMemoryMatcher) validateDimensionConsistency(rule *Rule) error {
	// Check if any dimensions are configured
	if syncMapLen(m.dimensions) == 0 {
		// If no dimensions configured, allow any dimensions (for backward compatibility)
		return nil
	}

	// Create maps for efficient lookup
	ruleDimensions := make(map[string]*DimensionValue)
	for _, dimValue := range rule.Dimensions {
		ruleDimensions[dimValue.DimensionName] = dimValue
	}

	// Check all configured dimensions
	var validationErr error
	m.dimensions.Range(func(key, value any) bool {
		configDim := value.(*DimensionConfig)
		_, exists := ruleDimensions[configDim.Name]

		if configDim.Required && !exists {
			validationErr = fmt.Errorf("rule missing required dimension '%s'", configDim.Name)
			return false // Stop iteration
		}

		if exists {
			// Remove from map to track processed dimensions
			delete(ruleDimensions, configDim.Name)
		}
		return true // Continue iteration
	})

	if validationErr != nil {
		return validationErr
	}

	// Check for extra dimensions not in configuration
	if len(ruleDimensions) > 0 {
		var extraDims []string
		for dimName := range ruleDimensions {
			extraDims = append(extraDims, dimName)
		}
		return fmt.Errorf("rule contains dimensions not in configuration: %v", extraDims)
	}

	return nil
}

// validateWeightConflict ensures no two rules have the same total weight within the same tenant/application
func (m *InMemoryMatcher) validateWeightConflict(rule *Rule) error {
	// Skip weight conflict check if duplicate weights are allowed
	if m.allowDuplicateWeights {
		return nil
	}

	newRuleWeight := rule.CalculateTotalWeight(m.getDimensionsMap())

	// Check against existing rules in the same tenant/application context
	var conflictErr error
	m.rules.Range(func(key, value any) bool {
		existingRuleID := key.(string)
		existingRule := value.(*Rule)

		// Skip if it's the same rule (for updates)
		if existingRuleID == rule.ID {
			return true // Continue iteration
		}

		existingWeight := existingRule.CalculateTotalWeight(m.getDimensionsMap())
		if existingWeight == newRuleWeight {
			conflictErr = fmt.Errorf("invalid rule: weight conflict: rule '%s' already has weight %.2f in tenant '%s' application '%s'",
				existingRuleID, existingWeight, rule.TenantID, rule.ApplicationID)
			return false // Stop iteration
		}
		return true // Continue iteration
	})

	return conflictErr
}

// validateRuleForRebuild validates a rule during rebuild with provided dimension configs
func (m *InMemoryMatcher) validateRuleForRebuild(rule *Rule, dimensionConfigs map[string]*DimensionConfig) error {
	if rule.ID == "" {
		return fmt.Errorf("rule ID cannot be empty")
	}

	if len(rule.Dimensions) == 0 {
		return fmt.Errorf("rule must have at least one dimension")
	}

	// Validate dimension consistency with provided configs
	if err := m.validateDimensionConsistencyWithConfigs(rule, dimensionConfigs); err != nil {
		return err
	}

	// Validate each dimension
	for _, dimValue := range rule.Dimensions {
		if dimValue.DimensionName == "" {
			return fmt.Errorf("dimension name cannot be empty")
		}
	}

	// Note: We skip weight conflict validation during rebuild since we're starting fresh
	return nil
}

// validateDimensionConsistencyWithConfigs validates dimensions against provided configs
func (m *InMemoryMatcher) validateDimensionConsistencyWithConfigs(rule *Rule, dimensionConfigs map[string]*DimensionConfig) error {
	// Check if any dimensions are configured
	if len(dimensionConfigs) == 0 {
		// If no dimensions configured, allow any dimensions (for backward compatibility)
		return nil
	}

	// Create maps for efficient lookup
	ruleDimensions := make(map[string]*DimensionValue)
	for _, dimValue := range rule.Dimensions {
		ruleDimensions[dimValue.DimensionName] = dimValue
	}

	// Check all configured dimensions
	for _, configDim := range dimensionConfigs {
		_, exists := ruleDimensions[configDim.Name]

		if configDim.Required && !exists {
			return fmt.Errorf("rule missing required dimension '%s'", configDim.Name)
		}

		if exists {
			// Remove from map to track processed dimensions
			delete(ruleDimensions, configDim.Name)
		}
	}

	// Check for extra dimensions not in configuration
	if len(ruleDimensions) > 0 {
		var extraDims []string
		for dimName := range ruleDimensions {
			extraDims = append(extraDims, dimName)
		}
		return fmt.Errorf("rule contains dimensions not in configuration: %v", extraDims)
	}

	return nil
}

// updateCacheStats updates cache hit rate statistics
func (m *InMemoryMatcher) updateCacheStats(hit bool) {
	// Simple moving average for cache hit rate
	if hit {
		m.stats.CacheHitRate = (m.stats.CacheHitRate*0.9 + 0.1)
	} else {
		m.stats.CacheHitRate = (m.stats.CacheHitRate * 0.9)
	}
}

// ListRules returns all rules with pagination
func (m *InMemoryMatcher) ListRules(offset, limit int) ([]*Rule, error) {
	var rules []*Rule
	rlock(m.mu, func() any {
		m.rules.Range(func(key, value any) bool {
			rules = append(rules, value.(*Rule))
			return true
		})
		return nil
	})

	// Sort by creation time (newest first)
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].CreatedAt.After(rules[j].CreatedAt)
	})

	// Apply pagination
	if offset >= len(rules) {
		return []*Rule{}, nil
	}

	end := offset + limit
	if end > len(rules) {
		end = len(rules)
	}

	return rules[offset:end], nil
}

// ListDimensions returns all dimension configurations
func (m *InMemoryMatcher) ListDimensions() ([]*DimensionConfig, error) {
	var configs []*DimensionConfig
	rlock(m.mu, func() any {
		m.dimensions.Range(func(key, value any) bool {
			configs = append(configs, value.(*DimensionConfig))
			return true
		})
		return nil
	})

	// Sort by index
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Index < configs[j].Index
	})

	return configs, nil
}

// GetStats returns current statistics
func (m *InMemoryMatcher) GetStats() *MatcherStats {
	// Create a copy to avoid race conditions
	stats := *m.stats
	return &stats
}

// SaveToPersistence saves current state to persistence layer
func (m *InMemoryMatcher) SaveToPersistence() error {
	return rlock(m.mu, func() error {
		// Save rules
		var rules []*Rule
		m.rules.Range(func(key, value any) bool {
			rules = append(rules, value.(*Rule))
			return true
		})
		if err := m.persistence.SaveRules(m.ctx, rules); err != nil {
			return fmt.Errorf("failed to save rules: %w", err)
		}

		// Save dimension configurations
		var configs []*DimensionConfig
		m.dimensions.Range(func(key, value any) bool {
			configs = append(configs, value.(*DimensionConfig))
			return true
		})

		if err := m.persistence.SaveDimensionConfigs(m.ctx, configs); err != nil {
			return fmt.Errorf("failed to save dimension configs: %w", err)
		}

		return nil
	})
}

// Close closes the matcher and cleans up resources
func (m *InMemoryMatcher) Close() error {
	m.cancel()

	if m.broker != nil {
		return m.broker.Close()
	}

	return nil
}

// Rebuild clears all data and rebuilds the forest from the persistence interface
func (m *InMemoryMatcher) Rebuild() error {
	// Create new structures for atomic replacement
	newForestIndexes := sync.Map{}
	newDimensionConfigs := make(map[string]*DimensionConfig)
	newRules := make(map[string]*Rule)
	newTenantRules := make(map[string]map[string]*Rule)

	// Load dimension configurations from persistence
	configs, err := m.persistence.LoadDimensionConfigs(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load dimension configs during rebuild: %w", err)
	}

	// Initialize dimensions in appropriate forests
	for _, config := range configs {
		newDimensionConfigs[config.Name] = config

		// Get or create forest for this tenant/application
		key := m.getTenantKey(config.TenantID, config.ApplicationID)
		m.wforest(key, func() any {
			if forrest, ok := newForestIndexes.Load(key); !ok {
				newForest := CreateRuleForestWithTenant(config.TenantID, config.ApplicationID, newDimensionConfigs)
				forrestIndex := &ForestIndex{RuleForest: newForest}
				forrestIndex.InitializeDimension(config.Name)
				newForestIndexes.Store(key, forrestIndex)
			} else {
				forrest.(*ForestIndex).InitializeDimension(config.Name)
			}
			return nil
		})
	}

	// Load rules from persistence
	rules, err := m.persistence.LoadRules(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load rules during rebuild: %w", err)
	}

	// Add rules to new structures
	for _, rule := range rules {
		// Validate rule before adding
		if err := m.validateRuleForRebuild(rule, newDimensionConfigs); err != nil {
			return fmt.Errorf("invalid rule during rebuild (ID: %s): %w", rule.ID, err)
		}
		newRules[rule.ID] = rule

		// Add to tenant tracking
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
		if newTenantRules[key] == nil {
			newTenantRules[key] = make(map[string]*Rule)
		}
		newTenantRules[key][rule.ID] = rule

		// Get or create forest for this tenant/application
		m.wforest(key, func() any {
			if fi, ok := newForestIndexes.Load(key); !ok {
				newForest := CreateRuleForestWithTenant(rule.TenantID, rule.ApplicationID, newDimensionConfigs)
				newForestIndexes.Store(key, &ForestIndex{RuleForest: newForest})
			} else {
				fi.(*ForestIndex).AddRule(rule)
			}
			return nil
		})
	}

	// Replace all core data structures atomically
	// This ensures no query can see an inconsistent state
	m.forestIndexes = &newForestIndexes

	// Replace dimensions sync.Map
	newDimensionsSyncMap := &sync.Map{}
	for name, config := range newDimensionConfigs {
		newDimensionsSyncMap.Store(name, config)
	}
	m.dimensions = newDimensionsSyncMap

	// Replace rules sync.Map
	newRulesSyncMap := &sync.Map{}
	for id, rule := range newRules {
		newRulesSyncMap.Store(id, rule)
	}
	m.rules = newRulesSyncMap

	// Clear cache after successful replacement
	m.cache.Clear()

	// Update stats
	m.updateStats()

	return nil
}

// Health checks if the matcher is healthy
func (m *InMemoryMatcher) Health() error {
	// Check persistence health
	if err := m.persistence.Health(m.ctx); err != nil {
		return fmt.Errorf("persistence unhealthy: %w", err)
	}

	// Check event broker health if available
	if m.broker != nil {
		if err := m.broker.Health(m.ctx); err != nil {
			return fmt.Errorf("event broker unhealthy: %w", err)
		}
	}

	return nil
}

// publishEvent publishes an event to the message queue
func (m *InMemoryMatcher) publishEvent(event *Event) {
	if m.broker == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.broker.Publish(ctx, event); err != nil {
		// Log error but don't fail the operation
		fmt.Printf("Failed to publish event: %v\n", err)
	}
}

// updateQueryStats safely updates query statistics
func (m *InMemoryMatcher) updateQueryStats(start time.Time) {
	atomic.AddInt64(&m.stats.TotalQueries, 1)
	queryTime := time.Since(start)
	m.stats.AverageQueryTime = time.Duration(
		(int64(m.stats.AverageQueryTime)*(m.stats.TotalQueries-1) + int64(queryTime)) /
			int64(m.stats.TotalQueries),
	)
}

// getForestLock gets or creates a forest-specific lock
func (m *InMemoryMatcher) getForestLock(key string) *sync.RWMutex {
	// Create new lock for this forest
	lock, _ := m.forestLocks.LoadOrStore(key, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// rforest acquires a read lock on the specified forest
func (m *InMemoryMatcher) rforest(key string, f func() any) any {
	return rlock(m.getForestLock(key), f)
}

// rforest acquires a read lock on the specified forest
func (m *InMemoryMatcher) wforest(key string, f func() any) any {
	return wlock(m.getForestLock(key), f)
}

func rlock[T any](lc *sync.RWMutex, f func() T) T {
	lc.RLock()
	defer lc.RUnlock()
	return f()
}

func wlock[T any](lc *sync.RWMutex, f func() T) T {
	lc.Lock()
	defer lc.Unlock()
	return f()
}
