package matcher

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// InMemoryMatcher implements the core matching logic using forest indexes
type InMemoryMatcher struct {
	forestIndexes         map[string]*ForestIndex         // tenant_app_key -> ForestIndex
	dimensionConfigs      map[string]*DimensionConfig     // dimension_name -> config (global or scoped)
	rules                 map[string]*Rule                // rule_id -> rule
	tenantRules           map[string]map[string]*Rule     // tenant_app_key -> rule_id -> rule
	stats                 *MatcherStats
	cache                 *QueryCache
	persistence           PersistenceInterface
	eventBroker           EventBrokerInterface // Changed from eventSub to eventBroker
	eventsChan            chan *Event
	nodeID                string // Node identifier for filtering events
	allowDuplicateWeights bool   // When false (default), prevents rules with same weight
	mu                    sync.RWMutex
	ctx                   context.Context
	cancel                context.CancelFunc
}

// CreateInMemoryMatcher creates an in-memory matcher
func NewInMemoryMatcher(persistence PersistenceInterface, eventBroker EventBrokerInterface, nodeID string) (*InMemoryMatcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	matcher := &InMemoryMatcher{
		forestIndexes:    make(map[string]*ForestIndex),
		dimensionConfigs: make(map[string]*DimensionConfig),
		rules:            make(map[string]*Rule),
		tenantRules:      make(map[string]map[string]*Rule),
		stats: &MatcherStats{
			LastUpdated: time.Now(),
		},
		cache:       NewQueryCache(1000, 10*time.Minute), // 1000 entries, 10 min TTL
		persistence: persistence,
		eventBroker: eventBroker,
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
	if eventBroker != nil {
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

// getOrCreateForestIndex gets or creates a forest index for the specified tenant/application
func (m *InMemoryMatcher) getOrCreateForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)
	
	if forestIndex, exists := m.forestIndexes[key]; exists {
		return forestIndex
	}
	
	// Create new forest index for this tenant/application
	newForest := CreateRuleForestWithTenant(tenantID, applicationID)
	forestIndex := &ForestIndex{RuleForest: newForest}
	m.forestIndexes[key] = forestIndex
	
	// Initialize tenant rules map if needed
	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}
	
	return forestIndex
}

// getForestIndex gets the forest index for the specified tenant/application (read-only)
func (m *InMemoryMatcher) getForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)
	return m.forestIndexes[key]
}

// loadFromPersistence loads rules and dimensions from persistence layer
func (m *InMemoryMatcher) loadFromPersistence() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load dimension configurations
	configs, err := m.persistence.LoadDimensionConfigs(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load dimension configs: %w", err)
	}

	// Initialize dimensions
	for _, config := range configs {
		m.dimensionConfigs[config.Name] = config
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
		m.rules[rule.ID] = rule
		
		// Add to tenant-specific tracking
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
		if m.tenantRules[key] == nil {
			m.tenantRules[key] = make(map[string]*Rule)
		}
		m.tenantRules[key][rule.ID] = rule
		
		// Add to appropriate forest index
		forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
		forestIndex.AddRule(rule)
	}

	m.stats.TotalRules = len(m.rules)
	m.stats.TotalDimensions = len(m.dimensionConfigs)

	return nil
}

// startEventSubscription starts listening for events from the event subscriber
func (m *InMemoryMatcher) startEventSubscription() error {
	if err := m.eventBroker.Subscribe(m.ctx, m.eventsChan); err != nil {
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

	default:
		return fmt.Errorf("unknown event type: %s", event.Type)
	}
}

// AddRule adds a new rule to the matcher
func (m *InMemoryMatcher) AddRule(rule *Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.rules[rule.ID] = rule
	
	// Add to tenant-specific tracking
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}
	m.tenantRules[key][rule.ID] = rule
	
	// Add to appropriate forest index
	forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
	forestIndex.AddRule(rule)

	// Clear cache since we added a new rule
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.LastUpdated = now

	// Publish event to message queue
	if m.eventBroker != nil {
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

// updateRule updates an existing rule
func (m *InMemoryMatcher) updateRule(rule *Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate rule
	if err := m.validateRule(rule); err != nil {
		return fmt.Errorf("invalid rule: %w", err)
	}

	// Remove old rule if exists
	if oldRule, exists := m.rules[rule.ID]; exists {
		// Remove from old tenant's forest
		oldForestIndex := m.getForestIndex(oldRule.TenantID, oldRule.ApplicationID)
		if oldForestIndex != nil {
			oldForestIndex.RemoveRule(oldRule)
		}
		
		// Remove from old tenant's tracking
		oldKey := m.getTenantKey(oldRule.TenantID, oldRule.ApplicationID)
		if m.tenantRules[oldKey] != nil {
			delete(m.tenantRules[oldKey], rule.ID)
		}
	}

	// Set update timestamp
	rule.UpdatedAt = time.Now()

	// Add updated rule
	m.rules[rule.ID] = rule
	
	// Add to tenant-specific tracking
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}
	m.tenantRules[key][rule.ID] = rule
	
	// Add to appropriate forest index
	forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
	forestIndex.AddRule(rule)

	// Clear cache
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.LastUpdated = time.Now()

	// Publish event to message queue
	if m.eventBroker != nil {
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
	m.mu.Lock()
	defer m.mu.Unlock()

	rule, exists := m.rules[ruleID]
	if !exists {
		return fmt.Errorf("rule not found: %s", ruleID)
	}

	// Remove from appropriate forest index
	forestIndex := m.getForestIndex(rule.TenantID, rule.ApplicationID)
	if forestIndex != nil {
		forestIndex.RemoveRule(rule)
	}
	
	// Remove from tenant-specific tracking
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	if m.tenantRules[key] != nil {
		delete(m.tenantRules[key], ruleID)
	}

	// Remove from rules map
	delete(m.rules, ruleID)

	// Clear cache
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.LastUpdated = time.Now()

	// Publish event to message queue
	if m.eventBroker != nil {
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
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate dimension config
	if config.Name == "" {
		return fmt.Errorf("dimension name cannot be empty")
	}

	// Add to configurations
	m.dimensionConfigs[config.Name] = config

	// Initialize forest for this dimension in appropriate tenant/application context
	forestIndex := m.getOrCreateForestIndex(config.TenantID, config.ApplicationID)
	forestIndex.InitializeDimension(config.Name)

	// Update stats
	m.stats.TotalDimensions = len(m.dimensionConfigs)
	m.stats.LastUpdated = time.Now()

	// Publish event to message queue
	if m.eventBroker != nil {
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
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the dimension config before deleting it for the event
	config, exists := m.dimensionConfigs[dimensionName]

	delete(m.dimensionConfigs, dimensionName)

	// Note: We don't remove the forest here as it might still contain rules
	// In production, you might want to handle this more carefully

	// Update stats
	m.stats.TotalDimensions = len(m.dimensionConfigs)
	m.stats.LastUpdated = time.Now()

	// Publish event to message queue if dimension existed
	if exists && m.eventBroker != nil {
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
	defer func() {
		m.mu.Lock()
		m.stats.TotalQueries++
		m.stats.AverageQueryTime = time.Duration(
			(int64(m.stats.AverageQueryTime)*m.stats.TotalQueries + int64(time.Since(start))) /
				(m.stats.TotalQueries + 1),
		)
		m.mu.Unlock()
	}()

	// Check cache first
	if result := m.cache.Get(query); result != nil {
		m.updateCacheStats(true)
		return result, nil
	}

	m.updateCacheStats(false)

	// Find all matches
	matches, err := m.FindAllMatches(query)
	if err != nil {
		return nil, err
	}

	if len(matches) == 0 {
		return nil, nil
	}

	// Sort by weight (highest first)
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].TotalWeight > matches[j].TotalWeight
	})

	best := matches[0]

	// Cache the result
	m.cache.Set(query, best)

	return best, nil
}

// FindAllMatches finds all matching rules for a query
func (m *InMemoryMatcher) FindAllMatches(query *QueryRule) ([]*MatchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get candidate rules from appropriate forest index
	forestIndex := m.getForestIndex(query.TenantID, query.ApplicationID)
	var candidates []*Rule
	
	if forestIndex != nil {
		candidates = forestIndex.FindCandidateRules(query)
	} else {
		// If no specific tenant forest, return empty results
		candidates = []*Rule{}
	}

	var matches []*MatchResult

	// Validate each candidate
	for _, rule := range candidates {
		if m.isFullMatch(rule, query) {
			weight := rule.CalculateTotalWeight()
			matchedDims := m.countMatchedDimensions(rule, query)

			matches = append(matches, &MatchResult{
				Rule:        rule,
				TotalWeight: weight,
				MatchedDims: matchedDims,
			})
		}
	}

	return matches, nil
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
		if dimConfig, exists := m.dimensionConfigs[dimValue.DimensionName]; exists && dimConfig.Required && !hasQueryValue {
			return false
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

		if dimValue.Weight < 0 {
			return fmt.Errorf("dimension weight cannot be negative")
		}
	}

	return nil
}

// validateDimensionConsistency ensures rule dimensions match the configured dimensions
func (m *InMemoryMatcher) validateDimensionConsistency(rule *Rule) error {
	// Check if any dimensions are configured
	if len(m.dimensionConfigs) == 0 {
		// If no dimensions configured, allow any dimensions (for backward compatibility)
		return nil
	}

	// Create maps for efficient lookup
	ruleDimensions := make(map[string]*DimensionValue)
	for _, dimValue := range rule.Dimensions {
		ruleDimensions[dimValue.DimensionName] = dimValue
	}

	// Check all configured dimensions
	for _, configDim := range m.dimensionConfigs {
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

// validateWeightConflict ensures no two rules have the same total weight within the same tenant/application
func (m *InMemoryMatcher) validateWeightConflict(rule *Rule) error {
	// Skip weight conflict check if duplicate weights are allowed
	if m.allowDuplicateWeights {
		return nil
	}

	newRuleWeight := rule.CalculateTotalWeight()
	tenantKey := m.getTenantKey(rule.TenantID, rule.ApplicationID)

	// Check against existing rules in the same tenant/application context
	if tenantRules, exists := m.tenantRules[tenantKey]; exists {
		for existingRuleID, existingRule := range tenantRules {
			// Skip if it's the same rule (for updates)
			if existingRuleID == rule.ID {
				continue
			}

			existingWeight := existingRule.CalculateTotalWeight()
			if existingWeight == newRuleWeight {
				return fmt.Errorf("invalid rule: weight conflict: rule '%s' already has weight %.2f in tenant '%s' application '%s'",
					existingRuleID, existingWeight, rule.TenantID, rule.ApplicationID)
			}
		}
	}

	return nil
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

		if dimValue.Weight < 0 {
			return fmt.Errorf("dimension weight cannot be negative")
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
	m.mu.Lock()
	defer m.mu.Unlock()

	// Simple moving average for cache hit rate
	if hit {
		m.stats.CacheHitRate = (m.stats.CacheHitRate*0.9 + 0.1)
	} else {
		m.stats.CacheHitRate = (m.stats.CacheHitRate * 0.9)
	}
}

// ListRules returns all rules with pagination
func (m *InMemoryMatcher) ListRules(offset, limit int) ([]*Rule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var rules []*Rule
	for _, rule := range m.rules {
		rules = append(rules, rule)
	}

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
	m.mu.RLock()
	defer m.mu.RUnlock()

	var configs []*DimensionConfig
	for _, config := range m.dimensionConfigs {
		configs = append(configs, config)
	}

	// Sort by index
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Index < configs[j].Index
	})

	return configs, nil
}

// GetStats returns current statistics
func (m *InMemoryMatcher) GetStats() *MatcherStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a copy to avoid race conditions
	stats := *m.stats
	return &stats
}

// SaveToPersistence saves current state to persistence layer
func (m *InMemoryMatcher) SaveToPersistence() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Save rules
	var rules []*Rule
	for _, rule := range m.rules {
		rules = append(rules, rule)
	}

	if err := m.persistence.SaveRules(m.ctx, rules); err != nil {
		return fmt.Errorf("failed to save rules: %w", err)
	}

	// Save dimension configurations
	var configs []*DimensionConfig
	for _, config := range m.dimensionConfigs {
		configs = append(configs, config)
	}

	if err := m.persistence.SaveDimensionConfigs(m.ctx, configs); err != nil {
		return fmt.Errorf("failed to save dimension configs: %w", err)
	}

	return nil
}

// Close closes the matcher and cleans up resources
func (m *InMemoryMatcher) Close() error {
	m.cancel()

	if m.eventBroker != nil {
		return m.eventBroker.Close()
	}

	return nil
}

// Rebuild clears all data and rebuilds the forest from the persistence interface
func (m *InMemoryMatcher) Rebuild() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new structures for atomic replacement
	newForestIndexes := make(map[string]*ForestIndex)
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
		if newForestIndexes[key] == nil {
			newForest := CreateRuleForestWithTenant(config.TenantID, config.ApplicationID)
			newForestIndexes[key] = &ForestIndex{RuleForest: newForest}
		}
		newForestIndexes[key].InitializeDimension(config.Name)
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
		if newForestIndexes[key] == nil {
			newForest := CreateRuleForestWithTenant(rule.TenantID, rule.ApplicationID)
			newForestIndexes[key] = &ForestIndex{RuleForest: newForest}
		}
		newForestIndexes[key].AddRule(rule)
	}

	// Replace all core data structures atomically
	// This ensures no query can see an inconsistent state
	m.forestIndexes, m.dimensionConfigs, m.rules, m.tenantRules = newForestIndexes, newDimensionConfigs, newRules, newTenantRules

	// Clear cache after successful replacement
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.TotalDimensions = len(m.dimensionConfigs)
	m.stats.LastUpdated = time.Now()

	return nil
}

// Health checks if the matcher is healthy
func (m *InMemoryMatcher) Health() error {
	// Check persistence health
	if err := m.persistence.Health(m.ctx); err != nil {
		return fmt.Errorf("persistence unhealthy: %w", err)
	}

	// Check event broker health if available
	if m.eventBroker != nil {
		if err := m.eventBroker.Health(m.ctx); err != nil {
			return fmt.Errorf("event broker unhealthy: %w", err)
		}
	}

	return nil
}

// publishEvent publishes an event to the message queue
func (m *InMemoryMatcher) publishEvent(event *Event) {
	if m.eventBroker == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.eventBroker.Publish(ctx, event); err != nil {
		// Log error but don't fail the operation
		fmt.Printf("Failed to publish event: %v\n", err)
	}
}
