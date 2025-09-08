package matcher

import (
	"context"
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

// SetWeight sets the weight for a specific match type
func (dc *DimensionConfig) SetWeight(matchType MatchType, weight float64) {
	if dc.Weights == nil {
		dc.Weights = make(map[MatchType]float64)
	}
	dc.Weights[matchType] = weight
}

// GetWeight returns the weight for a specific match type, returning 0.0 if not configured
func (dc *DimensionConfig) GetWeight(matchType MatchType) float64 {
	if weight, exists := dc.Weights[matchType]; exists {
		return weight
	}
	return 0.0
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

// DimensionValue represents a value for a specific dimension in a rule
type DimensionValue struct {
	DimensionName string    `json:"dimension_name"`
	Value         string    `json:"value"`
	MatchType     MatchType `json:"match_type"`
}

// Rule represents a matching rule with dynamic dimensions
type Rule struct {
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id,omitempty"`      // Tenant identifier for multi-tenancy
	ApplicationID string            `json:"application_id,omitempty"` // Application identifier for multi-application support
	Dimensions    []*DimensionValue `json:"dimensions"`
	ManualWeight  *float64          `json:"manual_weight,omitempty"` // Optional manual weight override
	Status        RuleStatus        `json:"status"`                  // Status of the rule (working, draft, etc.)
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Metadata      map[string]string `json:"metadata,omitempty"` // Additional metadata
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

	// Deep copy Dimensions slice
	if r.Dimensions != nil {
		clone.Dimensions = make([]*DimensionValue, len(r.Dimensions))
		for i, dim := range r.Dimensions {
			if dim != nil {
				clone.Dimensions[i] = &DimensionValue{
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
	TenantID                string                      `json:"tenant_id,omitempty"`
	ApplicationID           string                      `json:"application_id,omitempty"`
	Values                  map[string]string           `json:"values"`
	IncludeAllRules         bool                        `json:"include_all_rules,omitempty"`
	DynamicDimensionConfigs map[string]*DimensionConfig `json:"dynamic_dimension_configs,omitempty"`
	ExcludeRules            map[string]bool             `json:"exclude_rules,omitempty"`
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
	for _, dim := range r.Dimensions {
		if dim.DimensionName == dimensionName {
			return dim
		}
	}
	return nil
}

// CalculateTotalWeight calculates the total weight of the rule using dimension configurations
func (r *Rule) CalculateTotalWeight(dimensionConfigs map[string]*DimensionConfig) float64 {
	if r.ManualWeight != nil {
		return *r.ManualWeight
	}

	total := 0.0
	for _, dim := range r.Dimensions {
		if config, exists := dimensionConfigs[dim.DimensionName]; exists {
			// Try to get the weight for the specific match type
			if weight, hasWeight := config.Weights[dim.MatchType]; hasWeight {
				total += weight
			} else {
				// Use 0.0 when no specific weight is configured for this match type
				total += 0.0
			}
		} else {
			// If no configuration exists, use a default weight of 0.0
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
