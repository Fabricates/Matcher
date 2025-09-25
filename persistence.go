package matcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// JSONPersistence implements PersistenceInterface using JSON files
type JSONPersistence struct {
	rulesPath      string
	dimensionsPath string
}

// NewJSONPersistence creates a new JSON persistence layer
func NewJSONPersistence(dataDir string) *JSONPersistence {
	// Ensure directory exists
	os.MkdirAll(dataDir, 0755)

	return &JSONPersistence{
		rulesPath:      filepath.Join(dataDir, "rules.json"),
		dimensionsPath: filepath.Join(dataDir, "dimensions.json"),
	}
}

// LoadRules loads all rules from JSON file
func (jp *JSONPersistence) LoadRules(ctx context.Context) ([]*Rule, error) {
	data, err := os.ReadFile(jp.rulesPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*Rule{}, nil // Return empty slice if file doesn't exist
		}
		return nil, fmt.Errorf("failed to read rules file: %w", err)
	}

	var rules []*Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("failed to unmarshal rules: %w", err)
	}

	return rules, nil
}

// SaveRules saves all rules to JSON file
func (jp *JSONPersistence) SaveRules(ctx context.Context, rules []*Rule) error {
	data, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal rules: %w", err)
	}

	if err := os.WriteFile(jp.rulesPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write rules file: %w", err)
	}

	return nil
}

// LoadRulesByTenant loads rules for a specific tenant and application
func (jp *JSONPersistence) LoadRulesByTenant(ctx context.Context, tenantID, applicationID string) ([]*Rule, error) {
	// Load all rules and filter by tenant/application
	allRules, err := jp.LoadRules(ctx)
	if err != nil {
		return nil, err
	}

	var filteredRules []*Rule
	for _, rule := range allRules {
		if rule.MatchesTenantContext(tenantID, applicationID) {
			filteredRules = append(filteredRules, rule)
		}
	}

	return filteredRules, nil
}

// LoadDimensionConfigs loads dimension configurations from JSON file
func (jp *JSONPersistence) LoadDimensionConfigs(ctx context.Context) ([]*DimensionConfig, error) {
	data, err := os.ReadFile(jp.dimensionsPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Return default dimensions if file doesn't exist
			return []*DimensionConfig{}, nil
		}
		return nil, fmt.Errorf("failed to read dimensions file: %w", err)
	}

	var configs []*DimensionConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dimensions: %w", err)
	}

	return configs, nil
}

// SaveDimensionConfigs saves dimension configurations to JSON file
func (jp *JSONPersistence) SaveDimensionConfigs(ctx context.Context, configs []*DimensionConfig) error {
	data, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dimensions: %w", err)
	}

	if err := os.WriteFile(jp.dimensionsPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write dimensions file: %w", err)
	}

	return nil
}

// LoadDimensionConfigsByTenant loads dimension configurations for a specific tenant and application
func (jp *JSONPersistence) LoadDimensionConfigsByTenant(ctx context.Context, tenantID, applicationID string) ([]*DimensionConfig, error) {
	// Load all dimension configs and filter by tenant/application
	allConfigs, err := jp.LoadDimensionConfigs(ctx)
	if err != nil {
		return nil, err
	}

	var filteredConfigs []*DimensionConfig
	for _, config := range allConfigs {
		// Match tenant context or include global configs (empty tenant/app)
		if (config.TenantID == "" && config.ApplicationID == "") ||
			(config.TenantID == tenantID && config.ApplicationID == applicationID) {
			filteredConfigs = append(filteredConfigs, config)
		}
	}

	return filteredConfigs, nil
}

// Health checks if the persistence layer is healthy
func (jp *JSONPersistence) Health(ctx context.Context) error {
	// Check if we can write to the directory
	testFile := filepath.Join(filepath.Dir(jp.rulesPath), ".health_check")
	if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
		return fmt.Errorf("cannot write to data directory: %w", err)
	}
	os.Remove(testFile)
	return nil
}

// MockEventSubscriber is a mock implementation for testing
type MockEventSubscriber struct {
	events chan *Event
	closed bool
	mu     sync.RWMutex
}

// NewMockEventSubscriber creates a new mock event subscriber
func NewMockEventSubscriber() *MockEventSubscriber {
	return &MockEventSubscriber{
		events: make(chan *Event, 100),
	}
}

// Publish publishes an event to the mock broker
func (mes *MockEventSubscriber) Publish(ctx context.Context, event *Event) error {
	mes.mu.RLock()
	defer mes.mu.RUnlock()

	if mes.closed {
		return fmt.Errorf("subscriber is closed")
	}

	select {
	case mes.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("event queue is full")
	}
}

// Subscribe starts listening for events
func (mes *MockEventSubscriber) Subscribe(ctx context.Context, events chan<- *Event) error {
	go func() {
		for {
			select {
			case event, ok := <-mes.events:
				if !ok {
					return
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// PublishEvent publishes an event (for testing) - deprecated, use Publish instead
func (mes *MockEventSubscriber) PublishEvent(event *Event) {
	mes.mu.RLock()
	closed := mes.closed
	mes.mu.RUnlock()

	if closed {
		return
	}

	// Best-effort publish without context
	select {
	case mes.events <- event:
	default:
		// drop if full
	}
}

// Close closes the subscriber
func (mes *MockEventSubscriber) Close() error {
	mes.mu.Lock()
	defer mes.mu.Unlock()

	if !mes.closed {
		mes.closed = true
		close(mes.events)
	}
	return nil
}

// Health checks if the subscriber is healthy
func (mes *MockEventSubscriber) Health(ctx context.Context) error {
	if mes.closed {
		return fmt.Errorf("subscriber is closed")
	}
	return nil
}

// KafkaEventSubscriber implements EventSubscriberInterface using Kafka
// Note: This is a basic example. In production, you'd use a proper Kafka client library
type KafkaEventSubscriber struct {
	brokers []string
	topics  []string
	groupID string
	// In a real implementation, you'd have a Kafka consumer here
	// consumer    *kafka.Consumer
	eventsChan chan *Event
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
}

// NewKafkaEventSubscriber creates a new Kafka event subscriber
func NewKafkaEventSubscriber(brokers []string, topics []string, groupID string) *KafkaEventSubscriber {
	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaEventSubscriber{
		brokers:    brokers,
		topics:     topics,
		groupID:    groupID,
		eventsChan: make(chan *Event, 1000),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Subscribe starts listening for events from Kafka
func (kes *KafkaEventSubscriber) Subscribe(ctx context.Context, events chan<- *Event) error {
	// In a real implementation, you would:
	// 1. Create a Kafka consumer
	// 2. Subscribe to the topics
	// 3. Start consuming messages
	// 4. Parse messages into Event structs
	// 5. Send events to the events channel

	// This is a placeholder implementation
	go func() {
		for {
			select {
			case event := <-kes.eventsChan:
				if kes.closed {
					return
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return
				case <-kes.ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			case <-kes.ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Close closes the Kafka subscriber
func (kes *KafkaEventSubscriber) Close() error {
	kes.closed = true
	kes.cancel()
	close(kes.eventsChan)

	// In a real implementation, you would close the Kafka consumer here
	// return kes.consumer.Close()

	return nil
}

// Health checks if the Kafka subscriber is healthy
func (kes *KafkaEventSubscriber) Health(ctx context.Context) error {
	if kes.closed {
		return fmt.Errorf("kafka subscriber is closed")
	}

	// In a real implementation, you would check Kafka connection health
	// For example, try to fetch metadata or ping the brokers

	return nil
}

// DatabasePersistence implements PersistenceInterface using a SQL database
// This is a placeholder - you would use your preferred database driver
type DatabasePersistence struct {
	connectionString string
	// db *sql.DB
}

// NewDatabasePersistence creates a new database persistence layer
func NewDatabasePersistence(connectionString string) *DatabasePersistence {
	return &DatabasePersistence{
		connectionString: connectionString,
	}
}

// LoadRules loads all rules from the database
func (dp *DatabasePersistence) LoadRules(ctx context.Context) ([]*Rule, error) {
	// Placeholder implementation
	// In a real implementation, you would:
	// 1. Connect to the database
	// 2. Execute a SELECT query to get all rules
	// 3. Scan the results into Rule structs
	// 4. Return the rules

	return []*Rule{}, nil
}

// SaveRules saves all rules to the database
func (dp *DatabasePersistence) SaveRules(ctx context.Context, rules []*Rule) error {
	// Placeholder implementation
	// In a real implementation, you would:
	// 1. Start a transaction
	// 2. Clear existing rules (or use UPSERT)
	// 3. Insert all new rules
	// 4. Commit the transaction

	return nil
}

// LoadDimensionConfigs loads dimension configurations from the database
func (dp *DatabasePersistence) LoadDimensionConfigs(ctx context.Context) ([]*DimensionConfig, error) {
	// Placeholder implementation
	return []*DimensionConfig{}, nil
}

// SaveDimensionConfigs saves dimension configurations to the database
func (dp *DatabasePersistence) SaveDimensionConfigs(ctx context.Context, configs []*DimensionConfig) error {
	// Placeholder implementation
	return nil
}

// LoadRulesByTenant loads rules for a specific tenant and application from the database
func (dp *DatabasePersistence) LoadRulesByTenant(ctx context.Context, tenantID, applicationID string) ([]*Rule, error) {
	// Placeholder implementation
	// In a real implementation, you would:
	// 1. Connect to the database
	// 2. Execute a SELECT query with WHERE clause for tenant_id and application_id
	// 3. Scan the results into Rule structs
	// 4. Return the filtered rules

	return []*Rule{}, nil
}

// LoadDimensionConfigsByTenant loads dimension configurations for a specific tenant and application from the database
func (dp *DatabasePersistence) LoadDimensionConfigsByTenant(ctx context.Context, tenantID, applicationID string) ([]*DimensionConfig, error) {
	// Placeholder implementation
	// In a real implementation, you would:
	// 1. Connect to the database
	// 2. Execute a SELECT query with WHERE clause for tenant_id and application_id
	// 3. Include global configs (empty tenant_id and application_id)
	// 4. Scan the results into DimensionConfig structs
	// 5. Return the filtered configs

	return []*DimensionConfig{}, nil
}

// Health checks if the database connection is healthy
func (dp *DatabasePersistence) Health(ctx context.Context) error {
	// Placeholder implementation
	// In a real implementation, you would ping the database
	return nil
}
