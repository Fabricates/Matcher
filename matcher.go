package matcher

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const snapshotFileName = "snapshot" // .mermaid, .cache
const defaultInitialTimeout = 5 * time.Second

// MemoryMatcherEngine implements the core matching logic using forest indexes
type MemoryMatcherEngine struct {
	forestIndexes         map[string]*ForestIndex     // tenant_app_key -> ForestIndex
	dimensionConfigs      *DimensionConfigs           // Managed dimension configurations
	rules                 map[string]*Rule            // rule_id -> rule
	tenantRules           map[string]map[string]*Rule // tenant_app_key -> rule_id -> rule
	stats                 *MatcherStats
	cache                 *QueryCache
	persistence           PersistenceInterface
	broker                Broker // Changed from eventSub to eventBroker
	eventsChan            chan *Event
	nodeID                string        // Node identifier for filtering events
	allowDuplicateWeights bool          // When false (default), prevents rules with same weight
	initialTimeout        time.Duration // Timeout for loadFromPersistence
	mu                    sync.RWMutex
	ctx                   context.Context
	snapshotChanged       int64 // atomic flag to indicate if snapshot needs to be updated

}

// NewMatcherEngine is a private helper that creates an in-memory matcher with the provided context
func newInMemoryMatcherEngine(ctx context.Context, persistence PersistenceInterface, broker Broker, nodeID string, dcs *DimensionConfigs, initialTimeout time.Duration) (*MemoryMatcherEngine, error) {
	if dcs == nil {
		dcs = NewDimensionConfigs() // Initialize managed dimension configurations
	}

	if int64(initialTimeout) <= 0 {
		initialTimeout = defaultInitialTimeout
	}

	matcher := &MemoryMatcherEngine{
		forestIndexes:    make(map[string]*ForestIndex),
		dimensionConfigs: dcs,
		rules:            make(map[string]*Rule),
		tenantRules:      make(map[string]map[string]*Rule),
		stats: &MatcherStats{
			LastUpdated: time.Now(),
		},
		cache:           NewQueryCache(1000, 10*time.Minute), // 1000 entries, 10 min TTL
		persistence:     persistence,
		broker:          broker,
		eventsChan:      make(chan *Event, 100),
		nodeID:          nodeID,
		initialTimeout:  initialTimeout,
		snapshotChanged: 0, // Initialize atomic flag to 1 (no changes)
		ctx:             ctx,
	}

	// Log matcher creation
	if ctx == nil {
		ctx = context.Background()
	}
	logger.InfoContext(ctx, "created in-memory matcher", "node_id", nodeID)

	// Initialize with data from persistence
	if err := matcher.loadFromPersistence(ctx); err != nil {
		return nil, fmt.Errorf("failed to load data from persistence: %w", err)
	}

	// Start event subscription if provided
	if broker != nil {
		if err := matcher.startEventSubscription(); err != nil {
			return nil, fmt.Errorf("failed to start event subscription: %w", err)
		}
	}

	// Start snapshot monitoring
	matcher.startSnapshotMonitor()

	// Schedule the first snapshot dump
	atomic.StoreInt64(&matcher.snapshotChanged, 1)

	return matcher, nil
}

// startSnapshotMonitor starts a goroutine that monitors for snapshot changes
func (m *MemoryMatcherEngine) startSnapshotMonitor() {
	// Skip snapshot monitoring for test environments
	if testing.Testing() {
		logger.InfoContext(m.ctx, "Snapshot monitor skipped for test environment", "node_id", m.nodeID)
		return
	}

	logger.InfoContext(m.ctx, "Starting snapshot monitor", "node_id", m.nodeID)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				logger.InfoContext(m.ctx, "Snapshot monitor stopped")
				return
			case <-ticker.C:
				// Check if snapshot needs to be updated using CAS
				if atomic.CompareAndSwapInt64(&m.snapshotChanged, 1, 0) {
					logger.InfoContext(m.ctx, "Snapshot change detected, dumping snapshot")
					if err := m.dumpSnapshot(); err != nil {
						logger.ErrorContext(m.ctx, "Failed to dump snapshot", "error", err)
					} else {
						logger.InfoContext(m.ctx, "Snapshot dump complete, check snapshot.*.")
					}
				} else if st, err := os.Stat(snapshotFileName + ".mermaid"); err != nil || st.Size() <= 0 {
					// First time snapshot generation
					logger.InfoContext(m.ctx, "No snapshot file found, triggering initial snapshot")
					atomic.CompareAndSwapInt64(&m.snapshotChanged, 0, 1)
				}
			}
		}
	}()
}

// dumpSnapshot creates a JSON snapshot of the current matcher state
func (m *MemoryMatcherEngine) dumpSnapshot() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Dump cache as key-value pairs
	if err := DumpCacheToFile(m.cache, snapshotFileName); err != nil {
		return fmt.Errorf("failed to dump cache: %w", err)
	}

	// Dump forest in concise graph format
	if err := DumpForestToFile(m, snapshotFileName); err != nil {
		return fmt.Errorf("failed to dump forest: %w", err)
	}

	return nil
}

// getTenantKey generates a unique key for tenant and application combination
func (m *MemoryMatcherEngine) getTenantKey(tenantID, applicationID string) string {
	if tenantID == "" && applicationID == "" {
		return "default"
	}
	return fmt.Sprintf("%s:%s", tenantID, applicationID)
}

// getOrCreateForestIndex gets or creates a forest index for the specified tenant/application
func (m *MemoryMatcherEngine) getOrCreateForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)

	if forestIndex, exists := m.forestIndexes[key]; exists {
		return forestIndex
	}

	// Create new forest index for this tenant/application
	newForest := CreateRuleForestWithTenant(tenantID, applicationID, m.dimensionConfigs)
	forestIndex := &ForestIndex{RuleForest: newForest}
	m.forestIndexes[key] = forestIndex

	// Log creation of new forest index
	logger.InfoContext(m.ctx, "created forest index", "tenant_app", key)

	// Initialize tenant rules map if needed
	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}

	return forestIndex
}

// getForestIndex gets the forest index for the specified tenant/application (read-only)
func (m *MemoryMatcherEngine) getForestIndex(tenantID, applicationID string) *ForestIndex {
	key := m.getTenantKey(tenantID, applicationID)
	return m.forestIndexes[key]
}

// loadFromPersistence loads rules and dimensions from persistence layer
func (m *MemoryMatcherEngine) loadFromPersistence(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.initialTimeout)
	defer cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Log start of persistence load
	logger.InfoContext(ctx, "loading data from persistence", "node_id", m.nodeID)

	// Load dimension configurations
	configs, err := m.persistence.LoadDimensionConfigs(ctx)
	if err != nil {
		return fmt.Errorf("failed to load dimension configs: %w", err)
	}

	// Load all dimensions at once and sort only once (performance optimization)
	m.dimensionConfigs.LoadBulk(configs)

	// Initialize dimensions in forest indexes after bulk loading
	for _, config := range configs {
		forestIndex := m.getOrCreateForestIndex(config.TenantID, config.ApplicationID)
		forestIndex.InitializeDimension(config.Name)
	}

	// Load rules
	rules, err := m.persistence.LoadRules(ctx)
	if err != nil {
		return fmt.Errorf("failed to load rules: %w", err)
	}

	// Add rules to appropriate indexes
	for _, rule := range rules {
		// Add to tenant-specific tracking
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
		if m.tenantRules[key] == nil {
			m.tenantRules[key] = make(map[string]*Rule)
		}
		m.tenantRules[key][rule.ID] = rule

		// Add to appropriate forest index
		forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)
		r, err := forestIndex.AddRule(rule)
		if err != nil {
			return err
		}
		if r != nil {
			m.rules[rule.ID] = r
		}
	}

	logger.InfoContext(ctx, "loaded rules and dimensions from persistence", "num_rules", len(m.rules), "num_dimensions", m.dimensionConfigs.Count())

	m.stats.TotalRules = len(m.rules)
	m.stats.TotalDimensions = m.dimensionConfigs.Count()

	return nil
}

// startEventSubscription starts listening for events from the event subscriber
func (m *MemoryMatcherEngine) startEventSubscription() error {
	if err := m.broker.Subscribe(m.ctx, m.eventsChan); err != nil {
		return fmt.Errorf("failed to subscribe to events: %w", err)
	}

	logger.InfoContext(m.ctx, "subscribed to event broker", "node_id", m.nodeID)

	go m.handleEvents()
	return nil
}

// handleEvents processes events from the event channel
func (m *MemoryMatcherEngine) handleEvents() {
	for {
		select {
		case event := <-m.eventsChan:
			// Filter out events from this node to avoid processing our own messages
			if event.NodeID == m.nodeID {
				continue
			}

			logger.DebugContext(m.ctx, "received event", "type", event.Type, "node_id", event.NodeID)

			if err := m.processEvent(event); err != nil {
				logger.ErrorContext(m.ctx, "error processing event", "error", err, "event_type", event.Type)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// processEvent processes a single event
func (m *MemoryMatcherEngine) processEvent(event *Event) error {
	switch event.Type {
	case EventTypeRuleAdded:
		ruleEvent, ok := event.Data.(*RuleEvent)
		if !ok {
			return fmt.Errorf("invalid rule event data")
		}
		return m.AddRule(ruleEvent.Rule)

	case EventTypeRuleUpdated:
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

// AddRule adds a new rule to the matcher
func (m *MemoryMatcherEngine) AddRule(rule *Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.InfoContext(m.ctx, "adding rule", "rule_id", rule.ID, "tenant", rule.TenantID, "application", rule.ApplicationID)

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

	// Add to tenant-specific tracking
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}
	m.tenantRules[key][rule.ID] = rule

	// Add to appropriate forest index
	forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)

	// Add to internal structures
	r, err := forestIndex.AddRule(rule)
	if err != nil {
		logger.ErrorContext(m.ctx, "failed to add rule to forest", "rule_id", rule.ID, "error", err)
		return err
	}
	if r != nil {
		m.rules[rule.ID] = r
	}

	// Clear cache since we added a new rule
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.LastUpdated = now

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

	logger.InfoContext(m.ctx, "rule added", "rule_id", rule.ID)

	return nil
}

// UpdateRule updates an existing rule (public method)
func (m *MemoryMatcherEngine) UpdateRule(rule *Rule) error {
	return m.updateRule(rule)
}

// GetRule retrieves a rule by ID (public method)
func (m *MemoryMatcherEngine) GetRule(ruleID string) (*Rule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rule, exists := m.rules[ruleID]
	if !exists {
		return nil, fmt.Errorf("rule with ID '%s' not found", ruleID)
	}

	// Return a copy to prevent external modification
	return rule.Clone(), nil
}

// updateRule updates an existing rule
func (m *MemoryMatcherEngine) updateRule(rule *Rule) error {
	// Use write lock to ensure complete atomicity during updates
	// This prevents any concurrent queries from seeing partial state
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.InfoContext(m.ctx, "updating rule", "rule_id", rule.ID, "tenant", rule.TenantID, "application", rule.ApplicationID)

	// Check if rule exists - updateRule should only update existing rules
	existingRule, exists := m.rules[rule.ID]
	if !exists {
		return fmt.Errorf("rule with ID '%s' not found - use AddRule to create new rules", rule.ID)
	}

	// Validate rule
	if err := m.validateRule(rule); err != nil {
		logger.WarnContext(m.ctx, "rule validation failed on update", "rule_id", rule.ID, "error", err)
		return fmt.Errorf("invalid rule: %w", err)
	}

	// Set update timestamp
	rule.UpdatedAt = time.Now()

	// Get old rule and forest index info before any modifications
	oldRule := existingRule
	oldForestIndex := m.getForestIndex(oldRule.TenantID, oldRule.ApplicationID)
	oldKey := m.getTenantKey(oldRule.TenantID, oldRule.ApplicationID)

	// Get the new tenant key and forest index
	key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	forestIndex := m.getOrCreateForestIndex(rule.TenantID, rule.ApplicationID)

	// ATOMIC UPDATE STRATEGY: With write lock held, perform all operations sequentially
	// This ensures no concurrent FindAllMatches can see partial state

	// Step 1 & 2: Use atomic ReplaceRule to prevent intermediate states
	var r *Rule
	var err error
	if oldForestIndex != nil && forestIndex != nil && oldKey == key {
		// Same tenant/app - use atomic replace
		// DEBUG: This should be the path taken for the atomic test
		err = forestIndex.ReplaceRule(oldRule, rule)
		if err != nil {
			logger.ErrorContext(m.ctx, "ReplaceRule failed", "rule_id", rule.ID, "error", err)
			return err
		}
		r = rule
	} else {
		// Different tenant/app - remove from old, add to new
		// DEBUG: If this path is taken, the atomic test will fail
		if oldForestIndex != nil {
			oldForestIndex.RemoveRule(oldRule)
		}
		r, err = forestIndex.AddRule(rule)
		if err != nil {
			return err
		}
	}
	// Step 3: Update m.rules immediately - this is now the authoritative source
	if r != nil {
		m.rules[rule.ID] = r
	}

	logger.InfoContext(m.ctx, "rule updated", "rule_id", rule.ID)

	// Step 4: Update tenant tracking
	if oldKey != key && m.tenantRules[oldKey] != nil {
		delete(m.tenantRules[oldKey], rule.ID)
	}

	if m.tenantRules[key] == nil {
		m.tenantRules[key] = make(map[string]*Rule)
	}
	m.tenantRules[key][rule.ID] = rule

	// Step 5: Clear cache
	m.cache.Clear()

	// Step 6: Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.LastUpdated = time.Now()

	// Step 7: Publish event to message queue
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
func (m *MemoryMatcherEngine) DeleteRule(ruleID string) error {
	return m.deleteRule(ruleID)
}

// deleteRule removes a rule from the matcher (internal method)
func (m *MemoryMatcherEngine) deleteRule(ruleID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rule, exists := m.rules[ruleID]
	if !exists {
		logger.WarnContext(m.ctx, "delete: rule not found", "rule_id", ruleID)
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

	logger.InfoContext(m.ctx, "rule deleted", "rule_id", ruleID)

	return nil
}

// GetDimensionConfigs returns a deep copy of DimensionConfigs
func (m *MemoryMatcherEngine) GetDimensionConfigs() *DimensionConfigs {
	return m.dimensionConfigs.Clone(nil)
}

// LoadDimensions loads dimensions in bulk
func (m *MemoryMatcherEngine) LoadDimensions(configs []*DimensionConfig) {
	m.dimensionConfigs.LoadBulk(configs)
}

// AddDimension adds a new dimension configuration
func (m *MemoryMatcherEngine) AddDimension(config *DimensionConfig) error {
	return m.updateDimension(config)
}

// updateDimension updates dimension configuration
func (m *MemoryMatcherEngine) updateDimension(config *DimensionConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.InfoContext(m.ctx, "updating dimension config", "dimension", config.Name)

	// Validate dimension config
	if config.Name == "" {
		return fmt.Errorf("dimension name cannot be empty")
	}

	// Add to configurations
	m.dimensionConfigs.Add(config)

	// Initialize forest for this dimension in appropriate tenant/application context
	forestIndex := m.getOrCreateForestIndex(config.TenantID, config.ApplicationID)
	forestIndex.InitializeDimension(config.Name)

	// Update stats
	m.stats.TotalDimensions = m.dimensionConfigs.Count()
	m.stats.LastUpdated = time.Now()

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

	logger.InfoContext(m.ctx, "dimension updated", "dimension", config.Name)

	return nil
}

// deleteDimension removes a dimension configuration
func (m *MemoryMatcherEngine) deleteDimension(dimensionName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	exists := m.dimensionConfigs.Remove(dimensionName)

	logger.InfoContext(m.ctx, "delete dimension requested", "dimension", dimensionName, "existed", exists)

	// Note: We don't remove the forest here as it might still contain rules
	// In production, you might want to handle this more carefully

	// Update stats
	m.stats.TotalDimensions = m.dimensionConfigs.Count()
	m.stats.LastUpdated = time.Now()

	// Publish event to message queue if dimension existed
	if exists && m.broker != nil {
		event := &Event{
			Type:      EventTypeDimensionDeleted,
			Timestamp: time.Now(),
			NodeID:    m.nodeID,
			Data: &DimensionEvent{
				Dimension: &DimensionConfig{
					Name: dimensionName,
				},
			},
		}
		go m.publishEvent(event) // Publish asynchronously
	}

	if exists {
		logger.InfoContext(m.ctx, "dimension deleted", "dimension", dimensionName)
	}

	return nil
}

// FindBestMatch finds the best matching rule for a query
func (m *MemoryMatcherEngine) FindBestMatch(query *QueryRule) (*MatchResult, error) {
	start := time.Now()

	// Update query count using atomic operation (no lock needed)
	atomic.AddInt64(&m.stats.TotalQueries, 1)

	// Check cache first while holding read lock
	if result := m.cache.Get(query); result != nil {
		m.updateCacheStats(true)
		// Update average query time without lock
		m.updateQueryTimeStats(time.Since(start))
		logger.DebugContext(m.ctx, "FindBestMatch cache hit", "query", query.Values)
		return result, nil
	}

	m.updateCacheStats(false)

	// Acquire read lock for the entire lookup path so the cache check,
	// candidate computation and cache population happen with a consistent
	// view of the authoritative state. This prevents races where an
	// UpdateRule can interleave and make the query observe partial updates.
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find all matches while holding the read lock using the nop-lock helper
	matches, err := m.findAllMatchesNoLock(query)
	if err != nil {
		// Update average query time without lock
		m.updateQueryTimeStats(time.Since(start))
		return nil, err
	}

	if len(matches) == 0 {
		// Update average query time without lock
		m.updateQueryTimeStats(time.Since(start))
		logger.DebugContext(m.ctx, "FindBestMatch no matches", "query", query.Values)
		return nil, nil
	}

	best := matches[0] // already sorted

	// Cache the result (safe because we used read lock while computing matches)
	m.cache.Set(query, best)

	// Update average query time without lock
	m.updateQueryTimeStats(time.Since(start))
	logger.DebugContext(m.ctx, "FindBestMatch returning best", "rule_id", best.Rule.ID, "duration_ms", time.Since(start).Milliseconds())
	return best, nil
}

// updateQueryTimeStats updates the average query time using lock-free approach
// This method avoids taking write locks in the read path to prevent starvation
func (m *MemoryMatcherEngine) updateQueryTimeStats(queryTime time.Duration) {
	// For now, we'll just periodically update the average query time during writes
	// to avoid taking locks in the read path. This trades off some precision for
	// better concurrency performance and prevents read starvation.
	// The stats will be updated during rebuild, add/update/delete operations.
	atomic.AddInt64(&m.stats.TotalQueryTime, queryTime.Milliseconds())
}

// FindAllMatches finds all matching rules for a query
func (m *MemoryMatcherEngine) FindAllMatches(query *QueryRule) ([]*MatchResult, error) {
	// RLocked compatibility wrapper: acquire read lock and call helper
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.findAllMatchesNoLock(query)
}

// FindAllMatchesInBatch finds the best matching rule for each query in the provided
// slice and returns results in the same order. The entire operation is
// performed while holding the matcher's read lock so the caller sees a
// consistent snapshot with respect to concurrent updates.
func (m *MemoryMatcherEngine) FindAllMatchesInBatch(queries []*QueryRule) ([][]*MatchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([][]*MatchResult, len(queries))

	for i, q := range queries {
		matches, err := m.findAllMatchesNoLock(q)
		if err != nil {
			return nil, err
		}

		if len(matches) == 0 {
			results[i] = nil
			continue
		}

		results[i] = matches
	}

	return results, nil
}

// findAllMatchesNoLock performs the match candidate verification without taking locks.
// Caller must hold appropriate locks (RLock/RWMutex) if concurrency safety is required.
func (m *MemoryMatcherEngine) findAllMatchesNoLock(query *QueryRule) ([]*MatchResult, error) {
	// Get candidate rules from appropriate forest index
	forestIndex := m.getForestIndex(query.TenantID, query.ApplicationID)
	var candidates []RuleWithWeight

	if forestIndex != nil {
		candidates = forestIndex.FindCandidateRules(query)
	} else {
		// If no specific tenant forest, return empty results
		candidates = []RuleWithWeight{}
	}

	var matches []*MatchResult

	// Double-check approach to prevent race conditions
	// For each candidate from forest, verify it actually matches the query dimensions
	// AND exists in m.rules AND its dimensions in m.rules still match the query
	for _, candidate := range candidates {
		// Check 1: Rule must exist in m.rules (authoritative source)
		actualRule, exists := m.rules[candidate.Rule.ID]
		if !exists {
			continue // Skip rules that don't exist in m.rules (being updated)
		}

		// Check 2: Skip excluded rules
		if m.isRuleExcluded(actualRule.ID, query.ExcludeRules) {
			continue // Skip rules that are explicitly excluded
		}

		// Check 3: The rule from m.rules must actually match this query
		// This prevents returning rules that matched old dimensions but not current ones
		if !m.isFullMatch(actualRule, query) {
			continue // Skip rules whose current dimensions don't match this query
		}

		// Check 4: Verify the candidate rule from forest has same dimensions as m.rules
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

// FindBestMatchInBatch finds the best matching rule for each query in the provided
// slice and returns results in the same order. The entire operation is
// performed while holding the matcher's read lock so the caller sees a
// consistent snapshot with respect to concurrent updates.
func (m *MemoryMatcherEngine) FindBestMatchInBatch(queries []*QueryRule) ([]*MatchResult, error) {
	start := time.Now()

	// Count these as queries (approximate) for stats
	atomic.AddInt64(&m.stats.TotalQueries, int64(len(queries)))

	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([]*MatchResult, len(queries))

	// Collect items to cache after computing (we avoid mutating cache while
	// holding the read lock in a way that could conflict with other writers).
	type toCacheItem struct {
		q    *QueryRule
		best *MatchResult
	}
	var toCache []toCacheItem

	for i, q := range queries {
		// Check cache first
		if res := m.cache.Get(q); res != nil {
			m.updateCacheStats(true)
			results[i] = res
			continue
		}

		m.updateCacheStats(false)

		matches, err := m.findAllMatchesNoLock(q)
		if err != nil {
			return nil, err
		}

		if len(matches) == 0 {
			results[i] = nil
			continue
		}

		best := matches[0]
		results[i] = best
		toCache = append(toCache, toCacheItem{q: q, best: best})
	}

	// Populate cache for computed results
	for _, item := range toCache {
		m.cache.Set(item.q, item.best)
	}

	// Update average query time without lock
	m.updateQueryTimeStats(time.Since(start))

	return results, nil
}

// dimensionsEqual checks if two rules have identical dimensions
func (m *MemoryMatcherEngine) dimensionsEqual(rule1, rule2 *Rule) bool {
	if len(rule1.Dimensions) != len(rule2.Dimensions) {
		return false
	}

	// Create maps for comparison
	dims1 := rule1.Dimensions
	dims2 := rule2.Dimensions

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
func (m *MemoryMatcherEngine) isFullMatch(rule *Rule, query *QueryRule) bool {
	// First check tenant context - rules must match the query's tenant/application context
	if !rule.MatchesTenantContext(query.TenantID, query.ApplicationID) {
		return false
	}

	// Check each dimension in the rule
	for _, dimValue := range rule.Dimensions {
		queryValue, hasQueryValue := query.Values[dimValue.DimensionName]

		// If dimension is required but not in query, no match
		if m.dimensionConfigs.IsRequired(dimValue.DimensionName) && !hasQueryValue {
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
func (m *MemoryMatcherEngine) matchesDimension(dimValue *DimensionValue, queryValue string) bool {
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
func (m *MemoryMatcherEngine) countMatchedDimensions(rule *Rule, query *QueryRule) int {
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

// isRuleExcluded checks if a rule ID is in the map of excluded rules
func (m *MemoryMatcherEngine) isRuleExcluded(ruleID string, excludeRules map[string]bool) bool {
	if len(excludeRules) == 0 {
		return false
	}

	return excludeRules[ruleID]
}

// validateRule validates a rule before adding it
func (m *MemoryMatcherEngine) validateRule(rule *Rule) error {
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
func (m *MemoryMatcherEngine) validateDimensionConsistency(rule *Rule) error {
	// Check if any dimensions are configured
	if m.dimensionConfigs.Count() == 0 {
		// If no dimensions configured, allow any dimensions (for backward compatibility)
		return nil
	}

	// Create maps for efficient lookup
	ruleDimensions := make(map[string]*DimensionValue)
	for _, dimValue := range rule.Dimensions {
		ruleDimensions[dimValue.DimensionName] = dimValue
	}

	// Check all configured dimensions
	for _, dim := range m.dimensionConfigs.GetSortedNames() {
		_, exists := ruleDimensions[dim]

		if m.dimensionConfigs.IsRequired(dim) && !exists {
			return fmt.Errorf("rule missing required dimension '%s'", dim)
		}

		if exists {
			// Remove from map to track processed dimensions
			delete(ruleDimensions, dim)
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

// validateWeightConflict ensures no two intersecting rules have the same total weight within the same tenant/application
func (m *MemoryMatcherEngine) validateWeightConflict(rule *Rule) error {
	// Skip weight conflict check if duplicate weights are allowed
	if m.allowDuplicateWeights {
		return nil
	}

	newRuleWeight := rule.CalculateTotalWeight(m.dimensionConfigs)

	// Use efficient forest-based conflict detection
	tenantKey := m.getTenantKey(rule.TenantID, rule.ApplicationID)
	forestIndex, exists := m.forestIndexes[tenantKey]
	if !exists {
		return nil // No forest exists yet, no conflicts possible
	}

	// Find potentially conflicting rules using the forest structure
	var conflictingRules []*Rule
	forestIndex.searchConflict(rule, &conflictingRules)

	statusUnique := map[RuleStatus]bool{
		rule.Status: true,
	}

	// Check if any of the potentially conflicting rules have the same weight
	for _, conflictingRule := range conflictingRules {
		// Skip if it's the same rule (for updates)
		if conflictingRule.ID == rule.ID {
			continue
		}

		// Allow single existence for different status
		conflictingWeight := conflictingRule.CalculateTotalWeight(m.dimensionConfigs)
		if _, ok := statusUnique[conflictingRule.Status]; conflictingWeight == newRuleWeight && ok {
			return fmt.Errorf("invalid rule: weight conflict: new rule '%s' has weight %.2f which conflicts with existing rule '%s' in tenant '%s' application '%s'",
				rule.ID, newRuleWeight, conflictingRule.ID, rule.TenantID, rule.ApplicationID)
		}
		statusUnique[conflictingRule.Status] = true
	}

	return nil
}

// validateRuleForRebuild validates a rule during rebuild with provided dimension configs
func (m *MemoryMatcherEngine) validateRuleForRebuild(rule *Rule, dimensionConfigs *DimensionConfigs) error {
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
func (m *MemoryMatcherEngine) validateDimensionConsistencyWithConfigs(rule *Rule, dimensionConfigs *DimensionConfigs) error {
	// Check if any dimensions are configured
	if dimensionConfigs.Count() == 0 {
		// If no dimensions configured, allow any dimensions (for backward compatibility)
		return nil
	}

	// Create maps for efficient lookup
	ruleDimensions := make(map[string]*DimensionValue)
	for _, dimValue := range rule.Dimensions {
		ruleDimensions[dimValue.DimensionName] = dimValue
	}

	// Check all configured dimensions
	for _, configDim := range dimensionConfigs.GetSortedNames() {
		_, exists := ruleDimensions[configDim]

		if dimensionConfigs.IsRequired(configDim) && !exists {
			return fmt.Errorf("rule missing required dimension '%s'", configDim)
		}

		if exists {
			// Remove from map to track processed dimensions
			delete(ruleDimensions, configDim)
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
func (m *MemoryMatcherEngine) updateCacheStats(hit bool) {
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
func (m *MemoryMatcherEngine) ListRules(offset, limit int) ([]*Rule, error) {
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
func (m *MemoryMatcherEngine) ListDimensions() ([]*DimensionConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := m.dimensionConfigs.CloneSorted()

	return configs, nil
}

// GetStats returns current statistics
func (m *MemoryMatcherEngine) GetStats() *MatcherStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a copy to avoid race conditions
	stats := *m.stats
	// Guard against divide-by-zero when no queries have been recorded yet
	if stats.TotalQueries > 0 {
		stats.AverageQueryTime = stats.TotalQueryTime / stats.TotalQueries
	} else {
		stats.AverageQueryTime = 0
	}
	return &stats
}

// SetAllowDuplicateWeights configures whether rules with duplicate weights are allowed
// By default, duplicate weights are not allowed to ensure deterministic matching
func (m *MemoryMatcherEngine) SetAllowDuplicateWeights(allow bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allowDuplicateWeights = allow
}

// SaveToPersistence saves current state to persistence layer
func (m *MemoryMatcherEngine) SaveToPersistence() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	logger.InfoContext(m.ctx, "saving state to persistence", "num_rules", len(m.rules))

	// Save rules
	var rules []*Rule
	for _, rule := range m.rules {
		rules = append(rules, rule)
	}

	if err := m.persistence.SaveRules(m.ctx, rules); err != nil {
		return fmt.Errorf("failed to save rules: %w", err)
	}

	// Save dimension configurations
	configs := m.dimensionConfigs.CloneSorted()

	if err := m.persistence.SaveDimensionConfigs(m.ctx, configs); err != nil {
		return fmt.Errorf("failed to save dimension configs: %w", err)
	}

	logger.InfoContext(m.ctx, "saved state to persistence", "num_rules", len(m.rules))

	return nil
}

// Close closes the matcher and cleans up resources
func (m *MemoryMatcherEngine) Close() error {
	logger.InfoContext(m.ctx, "closing matcher", "node_id", m.nodeID)

	if m.broker != nil {
		if err := m.broker.Close(); err != nil {
			logger.ErrorContext(m.ctx, "error closing broker", "error", err)
			return err
		}
		logger.InfoContext(m.ctx, "broker closed", "node_id", m.nodeID)
	}

	logger.InfoContext(m.ctx, "matcher closed", "node_id", m.nodeID)
	return nil
}

// Rebuild clears all data and rebuilds the forest from the persistence interface
func (m *MemoryMatcherEngine) Rebuild() error {
	logger.InfoContext(m.ctx, "starting rebuild from persistence")
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new structures for atomic replacement
	newForestIndexes := make(map[string]*ForestIndex)
	newDimensionConfigs := NewDimensionConfigs() // Use new instance for managed dimensions
	newRules := make(map[string]*Rule)
	newTenantRules := make(map[string]map[string]*Rule)

	// Load dimension configurations from persistence
	configs, err := m.persistence.LoadDimensionConfigs(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to load dimension configs during rebuild: %w", err)
	}

	newDimensionConfigs.LoadBulk(configs)

	// Initialize dimensions in appropriate forests
	for _, config := range configs {
		// Get or create forest for this tenant/application
		key := m.getTenantKey(config.TenantID, config.ApplicationID)
		if newForestIndexes[key] == nil {
			newForest := CreateRuleForestWithTenant(config.TenantID, config.ApplicationID, m.dimensionConfigs)
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

		// Add to tenant tracking
		key := m.getTenantKey(rule.TenantID, rule.ApplicationID)
		if newTenantRules[key] == nil {
			newTenantRules[key] = make(map[string]*Rule)
		}
		newTenantRules[key][rule.ID] = rule

		// Get or create forest for this tenant/application
		if newForestIndexes[key] == nil {
			newForest := CreateRuleForestWithTenant(rule.TenantID, rule.ApplicationID, m.dimensionConfigs)
			newForestIndexes[key] = &ForestIndex{RuleForest: newForest}
		}
		r, err := newForestIndexes[key].AddRule(rule)
		if err != nil {
			return err
		}
		if r != nil {
			newRules[rule.ID] = r
		}
	}

	// Replace all core data structures atomically
	// This ensures no query can see an inconsistent state
	m.forestIndexes, m.dimensionConfigs, m.rules, m.tenantRules = newForestIndexes, newDimensionConfigs, newRules, newTenantRules

	// Clear cache after successful replacement
	m.cache.Clear()

	// Update stats
	m.stats.TotalRules = len(m.rules)
	m.stats.TotalDimensions = m.dimensionConfigs.Count()
	m.stats.LastUpdated = time.Now()

	// Signal that snapshot needs to be updated
	atomic.StoreInt64(&m.snapshotChanged, 1)

	logger.InfoContext(m.ctx, "rebuild complete", "num_rules", len(m.rules), "num_dimensions", m.dimensionConfigs.Count())

	return nil
}

// Health checks if the matcher is healthy
func (m *MemoryMatcherEngine) Health() error {
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
func (m *MemoryMatcherEngine) publishEvent(event *Event) {
	if m.broker == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.broker.Publish(ctx, event); err != nil {
		// Log error but don't fail the operation
		logger.ErrorContext(m.ctx, "Failed to publish event", "error", err)
	}
}
