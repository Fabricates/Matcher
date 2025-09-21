package matcher

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MatchType defines the type of matching for a dimension
type MatchType int

const (
	MatchTypeEqual MatchType = iota
	MatchTypeAny
	MatchTypePrefix
	MatchTypeSuffix
)

// RuleStatus defines the status of a rule
type RuleStatus string

const (
	RuleStatusWorking RuleStatus = "working"
	RuleStatusDraft   RuleStatus = "draft"
)

func (mt MatchType) String() string {
	switch mt {
	case MatchTypeEqual:
		return "equal"
	case MatchTypeAny:
		return "any"
	case MatchTypePrefix:
		return "prefix"
	case MatchTypeSuffix:
		return "suffix"
	default:
		return "unknown"
	}
}

// NewDimensionConfig creates a DimensionConfig with empty weights map
func NewDimensionConfig(name string, index int, required bool) *DimensionConfig {
	return &DimensionConfig{
		Name:     name,
		Index:    index,
		Required: required,
		Weights:  make(map[MatchType]float64),
	}
}

// NewDimensionConfigWithWeights creates a DimensionConfig with specific weights per match type
func NewDimensionConfigWithWeights(name string, index int, required bool, weights map[MatchType]float64) *DimensionConfig {
	return &DimensionConfig{
		Name:     name,
		Index:    index,
		Required: required,
		Weights:  weights,
	}
}

// DimensionConfig defines the configuration for a dimension
type DimensionConfig struct {
	Name          string                `json:"name"`
	Index         int                   `json:"index"`                    // Order of this dimension
	Required      bool                  `json:"required"`                 // Whether this dimension is required for matching
	Weights       map[MatchType]float64 `json:"weights"`                  // Weights for each match type
	TenantID      string                `json:"tenant_id,omitempty"`      // Tenant identifier for multi-tenancy
	ApplicationID string                `json:"application_id,omitempty"` // Application identifier for multi-application support
}

// Clone clones current dimension config deeply
func (dc *DimensionConfig) Clone() *DimensionConfig {
	weights := make(map[MatchType]float64)
	for t, w := range dc.Weights {
		weights[t] = w
	}
	return &DimensionConfig{
		Name:          dc.Name,
		Index:         dc.Index,
		Required:      dc.Required,
		Weights:       weights,
		TenantID:      dc.TenantID,
		ApplicationID: dc.ApplicationID,
	}
}

// SetWeight sets the weight for a specific match type
func (dc *DimensionConfig) SetWeight(matchType MatchType, weight float64) {
	if dc.Weights == nil {
		dc.Weights = make(map[MatchType]float64)
	}
	dc.Weights[matchType] = weight
}

// GetWeight returns the weight for a specific match type, returning 0.0 if not configured
func (dc *DimensionConfig) GetWeight(matchType MatchType) (float64, bool) {
	if weight, exists := dc.Weights[matchType]; exists {
		return weight, true
	}
	return 0.0, false
}

// DimensionValue represents a value for a specific dimension in a rule
type DimensionValue struct {
	DimensionName string    `json:"dimension_name"`
	Value         string    `json:"value"`
	MatchType     MatchType `json:"match_type"`
}

// Rule represents a matching rule with dynamic dimensions
type Rule struct {
	ID            string                     `json:"id"`
	TenantID      string                     `json:"tenant_id,omitempty"`      // Tenant identifier for multi-tenancy
	ApplicationID string                     `json:"application_id,omitempty"` // Application identifier for multi-application support
	Dimensions    map[string]*DimensionValue `json:"dimensions"`
	ManualWeight  *float64                   `json:"manual_weight,omitempty"` // Optional manual weight override
	Status        RuleStatus                 `json:"status"`                  // Status of the rule (working, draft, etc.)
	CreatedAt     time.Time                  `json:"created_at"`
	UpdatedAt     time.Time                  `json:"updated_at"`
	Metadata      map[string]string          `json:"metadata,omitempty"` // Additional metadata
}

// CloneAndComplete creates a deep copy of the Rule and fill in dimensions
func (r *Rule) CloneAndComplete(dimensions []string) *Rule {
	if r == nil {
		return nil
	}

	clone := &Rule{
		ID:            r.ID,
		TenantID:      r.TenantID,
		ApplicationID: r.ApplicationID,
		Status:        r.Status,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
	}

	// Deep copy ManualWeight pointer
	if r.ManualWeight != nil {
		weight := *r.ManualWeight
		clone.ManualWeight = &weight
	}

	// Deep copy Dimensions map
	if r.Dimensions != nil {
		clone.Dimensions = make(map[string]*DimensionValue, len(r.Dimensions))
		for _, dim := range dimensions {
			dv := r.GetDimensionValue(dim)
			if dv != nil {
				clone.Dimensions[dim] = &DimensionValue{
					DimensionName: dv.DimensionName,
					Value:         dv.Value,
					MatchType:     dv.MatchType,
				}
			} else {
				clone.Dimensions[dim] = &DimensionValue{
					DimensionName: dim,
					Value:         "",
					MatchType:     MatchTypeAny,
				}
			}
		}
	}

	// Deep copy Metadata map
	if r.Metadata != nil {
		clone.Metadata = make(map[string]string, len(r.Metadata))
		for k, v := range r.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}

// Clone creates a deep copy of the Rule
func (r *Rule) Clone() *Rule {
	if r == nil {
		return nil
	}

	clone := &Rule{
		ID:            r.ID,
		TenantID:      r.TenantID,
		ApplicationID: r.ApplicationID,
		Status:        r.Status,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
	}

	// Deep copy ManualWeight pointer
	if r.ManualWeight != nil {
		weight := *r.ManualWeight
		clone.ManualWeight = &weight
	}

	// Deep copy Dimensions map
	if r.Dimensions != nil {
		clone.Dimensions = make(map[string]*DimensionValue, len(r.Dimensions))
		for k, dim := range r.Dimensions {
			if dim != nil {
				clone.Dimensions[k] = &DimensionValue{
					DimensionName: dim.DimensionName,
					Value:         dim.Value,
					MatchType:     dim.MatchType,
				}
			}
		}
	}

	// Deep copy Metadata map
	if r.Metadata != nil {
		clone.Metadata = make(map[string]string, len(r.Metadata))
		for k, v := range r.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}

// RuleWithWeight can be used as search candidates
type RuleWithWeight struct {
	*Rule
	Weight float64
}

// QueryRule represents a query with values for each dimension
type QueryRule struct {
	TenantID                string            `json:"tenant_id,omitempty"`
	ApplicationID           string            `json:"application_id,omitempty"`
	Values                  map[string]string `json:"values"`
	IncludeAllRules         bool              `json:"include_all_rules,omitempty"`
	DynamicDimensionConfigs *DimensionConfigs `json:"dynamic_dimension_configs,omitempty"`
	ExcludeRules            map[string]bool   `json:"exclude_rules,omitempty"`
}

// MatchResult represents the result of a rule matching operation
type MatchResult struct {
	Rule        *Rule   `json:"rule"`
	TotalWeight float64 `json:"total_weight"`
	MatchedDims int     `json:"matched_dimensions"`
}

// PersistenceInterface defines the interface for data persistence
type PersistenceInterface interface {
	// Rules operations
	LoadRules(ctx context.Context) ([]*Rule, error)
	LoadRulesByTenant(ctx context.Context, tenantID, applicationID string) ([]*Rule, error)
	SaveRules(ctx context.Context, rules []*Rule) error

	// Dimensions operations
	LoadDimensionConfigs(ctx context.Context) ([]*DimensionConfig, error)
	LoadDimensionConfigsByTenant(ctx context.Context, tenantID, applicationID string) ([]*DimensionConfig, error)
	SaveDimensionConfigs(ctx context.Context, configs []*DimensionConfig) error

	// Health check
	Health(ctx context.Context) error
}

// EventType defines the type of events
type EventType string

const (
	EventTypeRuleAdded        EventType = "rule_added"
	EventTypeRuleUpdated      EventType = "rule_updated"
	EventTypeRuleDeleted      EventType = "rule_deleted"
	EventTypeDimensionAdded   EventType = "dimension_added"
	EventTypeDimensionUpdated EventType = "dimension_updated"
	EventTypeDimensionDeleted EventType = "dimension_deleted"
	EventTypeRebuild          EventType = "rebuild" // Indicates full state rebuild needed
)

// Event represents an event from the message queue
type Event struct {
	Type      EventType   `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	NodeID    string      `json:"node_id"` // ID of the node that published this event
	Data      interface{} `json:"data"`
}

// RuleEvent represents rule-related events
type RuleEvent struct {
	Rule *Rule `json:"rule"`
}

// DimensionEvent represents dimension-related events
type DimensionEvent struct {
	Dimension *DimensionConfig `json:"dimension"`
}

// Broker defines the unified interface for both event publishing and subscription
type Broker interface {
	// Publish publishes an event to the message queue
	Publish(ctx context.Context, event *Event) error

	// Subscribe starts listening for events and sends them to the provided channel
	Subscribe(ctx context.Context, events chan<- *Event) error

	// Health check
	Health(ctx context.Context) error

	// Close closes the broker (both publisher and subscriber)
	Close() error
}

// MatcherStats provides statistics about the matcher
type MatcherStats struct {
	TotalRules       int           `json:"total_rules"`
	TotalDimensions  int           `json:"total_dimensions"`
	TotalQueries     int64         `json:"total_queries"`
	AverageQueryTime time.Duration `json:"average_query_time"`
	CacheHitRate     float64       `json:"cache_hit_rate"`
	LastUpdated      time.Time     `json:"last_updated"`
}

// GetDimensionValue returns the value for a specific dimension in the rule
func (r *Rule) GetDimensionValue(dimensionName string) *DimensionValue {
	if r == nil || r.Dimensions == nil {
		return nil
	}
	if dim, ok := r.Dimensions[dimensionName]; ok {
		return dim
	}
	return nil
}

// GetDimensionMatchType returns the match type for a specific dimension in the rule
func (r *Rule) GetDimensionMatchType(dimensionName string) MatchType {
	if r == nil || r.Dimensions == nil {
		return MatchTypeAny
	}
	if dim, ok := r.Dimensions[dimensionName]; ok {
		return dim.MatchType
	}
	return MatchTypeAny
}

// CalculateTotalWeight calculates the total weight of the rule using dimension configurations
func (r *Rule) CalculateTotalWeight(dimensionConfigs *DimensionConfigs) float64 {
	if r.ManualWeight != nil {
		return *r.ManualWeight
	}

	total := 0.0
	if r.Dimensions == nil {
		return total
	}

	for _, dim := range dimensionConfigs.GetSortedNames() {
		if dv := r.GetDimensionValue(dim); dv != nil {
			// Try to get the weight for the specific match type
			if weight, hasWeight := dimensionConfigs.GetWeight(dim, dv.MatchType); hasWeight {
				total += weight
			} else {
				total += 0.0
			}
		} else {
			total += 0.0
		}
	}
	return total
}

// HasDimension checks if the rule has a specific dimension
func (r *Rule) HasDimension(dimensionName string) bool {
	return r.GetDimensionValue(dimensionName) != nil
}

// GetTenantContext returns the tenant and application context for the rule
func (r *Rule) GetTenantContext() (tenantID, applicationID string) {
	return r.TenantID, r.ApplicationID
}

// MatchesTenantContext checks if the rule matches the given tenant and application context
func (r *Rule) MatchesTenantContext(tenantID, applicationID string) bool {
	// Both rule and query must have the same tenant/application context
	return r.TenantID == tenantID && r.ApplicationID == applicationID
}

// GetTenantContext returns the tenant and application context for the query
func (q *QueryRule) GetTenantContext() (tenantID, applicationID string) {
	return q.TenantID, q.ApplicationID
}

// DimensionConfigs manages dimension configurations with automatic sorting
// and provides read-only access to sorted dimension lists
type DimensionConfigs struct {
	configs map[string]*DimensionConfig
	sorted  []*DimensionConfig
	sorter  func([]*DimensionConfig)
	mu      sync.RWMutex
}

// NewDimensionConfigs creates a new DimensionConfigs manager with default equal weight sorter
func NewDimensionConfigs() *DimensionConfigs {
	return NewDimensionConfigsWithSorter(nil)
}

// NewDimensionConfigsWithSorter creates a new DimensionConfigs manager with custom sorter
func NewDimensionConfigsWithSorter(sorter func([]*DimensionConfig)) *DimensionConfigs {
	return NewDimensionConfigsWithDimensionsAndSorter(nil, sorter)
}

// NewDimensionConfigsWithSorter creates a new DimensionConfigs manager with custom sorter
func NewDimensionConfigsWithDimensionsAndSorter(dimensions []*DimensionConfig, sorter func([]*DimensionConfig)) *DimensionConfigs {
	if sorter == nil {
		sorter = sortByEqualIndex
	}
	dcs := &DimensionConfigs{
		configs: make(map[string]*DimensionConfig),
		sorter:  sorter,
	}
	dcs.LoadBulk(dimensions)
	return dcs
}

// sortByEqualIndex is the default sorter function
func sortByEqualIndex(configs []*DimensionConfig) {
	sort.SliceStable(configs, func(i, j int) bool {
		return configs[i].Index < configs[j].Index
	})
}

// updateSortedConfigs updates the sorted configs slice
func (dc *DimensionConfigs) updateSortedConfigs() {
	// Create and sort a temporary slice before assigning to the field
	tempConfigs := make([]*DimensionConfig, 0, len(dc.configs))
	for _, config := range dc.configs {
		tempConfigs = append(tempConfigs, config)
	}
	dc.sorter(tempConfigs)

	// Assign the sorted temporary slice to the field
	dc.sorted = tempConfigs
}

// Clone deep clone all dimension configs, a little heavy operator
func (dc *DimensionConfigs) CloneSorted() []*DimensionConfig {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	dcs := make([]*DimensionConfig, dc.Count())
	for i, dim := range dc.sorted {
		dcs[i] = &DimensionConfig{
			Name:          dim.Name,
			Index:         dim.Index,
			Required:      dim.Required,
			Weights:       make(map[MatchType]float64),
			TenantID:      dim.TenantID,
			ApplicationID: dim.ApplicationID,
		}
		for mt, weight := range dim.Weights {
			dcs[i].Weights[mt] = weight
		}
	}

	return dcs
}

// Add adds or updates a dimension config and automatically updates the sorted list
func (dc *DimensionConfigs) Add(config *DimensionConfig) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.configs[config.Name] = config
	dc.updateSortedConfigs()
}

// Remove removes a dimension config and automatically updates the sorted list
func (dc *DimensionConfigs) Remove(dimensionName string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if _, ok := dc.configs[dimensionName]; ok {
		delete(dc.configs, dimensionName)
		dc.updateSortedConfigs()
		return true
	}

	return false
}

// Get returns the dimension config at index i
func (dc *DimensionConfigs) Get(i int) (string, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if i >= dc.Count() || i < 0 {
		return "", false
	}
	return dc.sorted[i].Name, true
}

// Exist checks the existence of given dimension name
func (dc *DimensionConfigs) Exist(name string) bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	if _, e := dc.configs[name]; e {
		return true
	}
	return false
}

// CloneDimension returns a cloned dimension config if dimension exists
func (dc *DimensionConfigs) CloneDimension(name string) *DimensionConfig {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if e, exist := dc.configs[name]; exist {
		return e.Clone()
	}
	return nil
}

// Clone returns a deep copied instance of current dimension configs with given configs merged
func (dc *DimensionConfigs) Clone(dcs []*DimensionConfig) *DimensionConfigs {
	return NewDimensionConfigsWithDimensionsAndSorter(append(dc.CloneSorted(), dcs...), dc.sorter)
}

// IsRequired returns whether the dimension is required or not
func (dc *DimensionConfigs) IsRequired(name string) bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if dc, ok := dc.configs[name]; ok {
		return dc.Required
	}
	return false
}

// Get returns a dimension config by name (read-only)
func (dc *DimensionConfigs) GetWeight(name string, mt MatchType) (float64, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if config, exists := dc.configs[name]; exists {
		return config.GetWeight(mt)
	}
	return 0.0, false
}

// GetSortedNames returns the sorted list of dimension names
func (dc *DimensionConfigs) GetSortedNames() []string {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	names := make([]string, len(dc.sorted))
	for i, config := range dc.sorted {
		names[i] = config.Name
	}
	return names
}

// SetSorter sets a custom sorter function and re-sorts the configs
func (dc *DimensionConfigs) SetSorter(sorter func([]*DimensionConfig)) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if sorter != nil {
		dc.sorter = sorter
	} else {
		dc.sorter = sortByEqualIndex
	}
	dc.updateSortedConfigs()
}

// Count returns the number of dimension configs
func (dc *DimensionConfigs) Count() int {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	return len(dc.configs)
}

// LoadBulk loads multiple dimension configs and sorts only once at the end
// This is more efficient than calling Add repeatedly during persistence loading
func (dc *DimensionConfigs) LoadBulk(configs []*DimensionConfig) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Add all configs without sorting
	for _, config := range configs {
		dc.configs[config.Name] = config
	}

	// Sort only once after all configs are loaded
	dc.updateSortedConfigs()
}
