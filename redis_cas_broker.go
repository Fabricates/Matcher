package matcher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClientInterface defines the common interface for all Redis client types
type RedisClientInterface interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
	TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// RedisCASBroker implements Broker using Redis with Compare-And-Swap operations
// It uses a fixed event key for CAS operations - no initialization, only latest event
type RedisCASBroker struct {
	client       RedisClientInterface
	nodeID       string
	eventKey     string // Fixed Redis key for CAS operations
	subscription chan<- *Event
	stopChan     chan struct{}
	subscribed   bool
	wg           sync.WaitGroup
	mu           sync.RWMutex

	// CAS polling configuration
	pollInterval  time.Duration // How often to poll for changes (1-5s)
	lastTimestamp int64         // Last known event timestamp
	timestampMu   sync.RWMutex
}

// RedisCASConfig holds configuration for Redis CAS broker
// Auto-detects deployment type based on provided parameters:
// - Single address without MasterName = Single node
// - Multiple addresses without MasterName = Cluster
// - MasterName provided = Sentinel (addresses are sentinel servers)
type RedisCASConfig struct {
	// Redis server configuration (auto-detects deployment type)
	Addrs      []string // Redis server addresses - single for standalone, multiple for cluster/sentinel
	MasterName string   // Master name for sentinel deployment (if provided, Addrs are sentinel servers)

	// Authentication
	Username       string // Redis username (for Redis 6.0+ ACL)
	Password       string // Redis password
	SentinelUser   string // Sentinel username (for sentinel deployment)
	SentinelPasswd string // Sentinel password (for sentinel deployment)
	DB             int    // Redis database number (ignored for cluster)

	// Connection settings
	MaxRetries      int           // Maximum number of retries (default: 3)
	MinRetryBackoff time.Duration // Minimum backoff between retries (default: 8ms)
	MaxRetryBackoff time.Duration // Maximum backoff between retries (default: 512ms)
	DialTimeout     time.Duration // Dial timeout (default: 5s)
	ReadTimeout     time.Duration // Read timeout (default: 3s)
	WriteTimeout    time.Duration // Write timeout (default: 3s)
	PoolSize        int           // Connection pool size (default: 10 per CPU)
	MinIdleConns    int           // Minimum idle connections (default: 0)

	// TLS configuration
	TLSEnabled      bool   // Enable TLS
	TLSInsecureSkip bool   // Skip certificate verification
	TLSServerName   string // Server name for certificate verification
	TLSCertFile     string // Client certificate file path
	TLSKeyFile      string // Client private key file path
	TLSCAFile       string // CA certificate file path

	// Broker-specific configuration
	NodeID       string        // Node identifier (required)
	Namespace    string        // Namespace for keys (optional, defaults to "matcher")
	PollInterval time.Duration // How often to poll for changes (defaults to 2s, should be 1-5s)
}

// LatestEvent represents the latest event stored in Redis (single event only)
type LatestEvent struct {
	Timestamp int64  `json:"timestamp"` // Unix timestamp in nanoseconds for ordering
	NodeID    string `json:"node_id"`   // Node that published this event
	Event     *Event `json:"event"`     // The actual event
}

// NewRedisCASBroker creates a new Redis CAS-based broker
// Auto-detects deployment type based on configuration:
// - Single address without MasterName = Single node
// - Multiple addresses without MasterName = Cluster
// - MasterName provided = Sentinel (addresses are sentinel servers)
// Does NOT initialize any default values in Redis
func NewRedisCASBroker(config RedisCASConfig) (*RedisCASBroker, error) {
	// Validate configuration
	if err := validateRedisCASConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set configuration defaults
	config = setRedisCASDefaults(config)

	// Create TLS config if enabled
	var tlsConfig *tls.Config
	if config.TLSEnabled {
		var err error
		tlsConfig, err = createTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
	}

	// Create Redis client based on auto-detected deployment type
	var client RedisClientInterface
	var err error

	if config.MasterName != "" {
		// Sentinel deployment - addresses are sentinel servers
		client, err = createSentinelClient(config, tlsConfig)
	} else if len(config.Addrs) > 1 {
		// Cluster deployment - multiple addresses
		client, err = createClusterClient(config, tlsConfig)
	} else if len(config.Addrs) == 1 {
		// Single node deployment - one address
		client, err = createSingleNodeClient(config, tlsConfig)
	} else {
		return nil, fmt.Errorf("no Redis addresses provided")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Create broker instance
	broker := &RedisCASBroker{
		client:       client,
		nodeID:       config.NodeID,
		eventKey:     fmt.Sprintf("%s:events", config.Namespace),
		stopChan:     make(chan struct{}),
		pollInterval: config.PollInterval,
	}

	// Load current state if key exists (don't create if it doesn't exist)
	_ = broker.loadCurrentState(context.Background())

	return broker, nil
}

// validateRedisCASConfig validates the Redis CAS configuration
func validateRedisCASConfig(config RedisCASConfig) error {
	if config.NodeID == "" {
		return fmt.Errorf("NodeID is required")
	}

	if len(config.Addrs) == 0 {
		return fmt.Errorf("at least one Redis address is required")
	}

	// Validate sentinel-specific configuration
	if config.MasterName != "" && len(config.Addrs) == 0 {
		return fmt.Errorf("sentinel addresses are required when MasterName is provided")
	}

	if config.PollInterval != 0 && (config.PollInterval < 1*time.Second || config.PollInterval > 5*time.Second) {
		return fmt.Errorf("PollInterval must be between 1-5 seconds, got %v", config.PollInterval)
	}

	return nil
}

// setRedisCASDefaults sets default values for Redis CAS configuration
func setRedisCASDefaults(config RedisCASConfig) RedisCASConfig {
	if config.Namespace == "" {
		config.Namespace = "matcher"
	}

	if config.PollInterval == 0 {
		config.PollInterval = 2 * time.Second
	}

	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}

	if config.MinRetryBackoff == 0 {
		config.MinRetryBackoff = 8 * time.Millisecond
	}

	if config.MaxRetryBackoff == 0 {
		config.MaxRetryBackoff = 512 * time.Millisecond
	}

	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}

	if config.ReadTimeout == 0 {
		config.ReadTimeout = 3 * time.Second
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = 3 * time.Second
	}

	if config.PoolSize == 0 {
		config.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}

	return config
}

// createTLSConfig creates a TLS configuration from the provided settings
func createTLSConfig(config RedisCASConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSInsecureSkip,
		ServerName:         config.TLSServerName,
	}

	// Load client certificate if provided
	if config.TLSCertFile != "" && config.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if config.TLSCAFile != "" {
		caCert, err := os.ReadFile(config.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// createSingleNodeClient creates a Redis client for single node deployment
func createSingleNodeClient(config RedisCASConfig, tlsConfig *tls.Config) (*redis.Client, error) {
	options := &redis.Options{
		Addr:            config.Addrs[0], // Use first (and only) address
		Username:        config.Username,
		Password:        config.Password,
		DB:              config.DB,
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		TLSConfig:       tlsConfig,
	}

	return redis.NewClient(options), nil
}

// createClusterClient creates a Redis client for cluster deployment
func createClusterClient(config RedisCASConfig, tlsConfig *tls.Config) (*redis.ClusterClient, error) {
	options := &redis.ClusterOptions{
		Addrs:           config.Addrs, // Use all provided addresses
		Username:        config.Username,
		Password:        config.Password,
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		TLSConfig:       tlsConfig,
	}

	return redis.NewClusterClient(options), nil
}

// createSentinelClient creates a Redis client for sentinel deployment
func createSentinelClient(config RedisCASConfig, tlsConfig *tls.Config) (*redis.Client, error) {
	options := &redis.FailoverOptions{
		MasterName:       config.MasterName,
		SentinelAddrs:    config.Addrs, // Addresses are sentinel servers
		SentinelUsername: config.SentinelUser,
		SentinelPassword: config.SentinelPasswd,
		Username:         config.Username,
		Password:         config.Password,
		DB:               config.DB,
		MaxRetries:       config.MaxRetries,
		MinRetryBackoff:  config.MinRetryBackoff,
		MaxRetryBackoff:  config.MaxRetryBackoff,
		DialTimeout:      config.DialTimeout,
		ReadTimeout:      config.ReadTimeout,
		WriteTimeout:     config.WriteTimeout,
		PoolSize:         config.PoolSize,
		MinIdleConns:     config.MinIdleConns,
		TLSConfig:        tlsConfig,
	}

	return redis.NewFailoverClient(options), nil
}

// Convenience constructors for common configurations

// NewSingleNodeRedisCASBroker creates a Redis CAS broker for single node deployment
func NewSingleNodeRedisCASBroker(addr, password, nodeID string) (*RedisCASBroker, error) {
	config := RedisCASConfig{
		Addrs:    []string{addr},
		Password: password,
		NodeID:   nodeID,
	}
	return NewRedisCASBroker(config)
}

// NewClusterRedisCASBroker creates a Redis CAS broker for cluster deployment
func NewClusterRedisCASBroker(addrs []string, password, nodeID string) (*RedisCASBroker, error) {
	config := RedisCASConfig{
		Addrs:    addrs,
		Password: password,
		NodeID:   nodeID,
	}
	return NewRedisCASBroker(config)
}

// NewSentinelRedisCASBroker creates a Redis CAS broker for sentinel deployment
func NewSentinelRedisCASBroker(sentinelAddrs []string, masterName, password, nodeID string) (*RedisCASBroker, error) {
	config := RedisCASConfig{
		Addrs:      sentinelAddrs,
		MasterName: masterName,
		Password:   password,
		NodeID:     nodeID,
	}
	return NewRedisCASBroker(config)
}

// loadCurrentState loads the current event state if it exists
func (r *RedisCASBroker) loadCurrentState(ctx context.Context) error {
	r.timestampMu.Lock()
	defer r.timestampMu.Unlock()

	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Key doesn't exist, that's fine - no previous events
			r.lastTimestamp = 0
			return nil
		}
		return err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		// If we can't unmarshal, treat as no previous events
		r.lastTimestamp = 0
		return nil
	}

	r.lastTimestamp = latestEvent.Timestamp
	return nil
}

// Publish publishes an event using CAS operation
func (r *RedisCASBroker) Publish(ctx context.Context, event *Event) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Retry CAS operation up to 3 times
	for retries := 0; retries < 3; retries++ {
		success, err := r.publishWithCAS(ctx, event)
		if err != nil {
			return err
		}
		if success {
			return nil
		}

		// Brief backoff before retry
		time.Sleep(time.Duration(retries+1) * 10 * time.Millisecond)
	}

	return fmt.Errorf("failed to publish event after retries")
}

// publishWithCAS performs a single CAS publish operation
func (r *RedisCASBroker) publishWithCAS(ctx context.Context, event *Event) (bool, error) {
	// Create new event entry
	now := time.Now().UnixNano()
	newEvent := &LatestEvent{
		Timestamp: now,
		NodeID:    r.nodeID,
		Event:     event,
	}

	newEventData, err := json.Marshal(newEvent)
	if err != nil {
		return false, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Use CAS operation to update the key
	// This will set the key regardless of previous value, but we use WATCH for conflict detection
	err = r.client.Watch(ctx, func(tx *redis.Tx) error {
		// Get current value to detect changes (but don't fail if key doesn't exist)
		currentData, err := tx.Get(ctx, r.eventKey).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		// If key exists, check if it changed during our operation
		if err != redis.Nil {
			var currentEvent LatestEvent
			if json.Unmarshal([]byte(currentData), &currentEvent) == nil {
				// Key exists and is valid - this is normal for CAS
			}
		}

		// Set the new event atomically
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, r.eventKey, newEventData, 0)
			return nil
		})

		return err
	}, r.eventKey)

	if err != nil {
		// Check if it's a watch error (concurrent modification)
		if err.Error() == "redis: transaction failed" {
			return false, nil // Indicate retry needed
		}
		return false, err
	}

	return true, nil
}

// Subscribe starts listening for events by polling the Redis key
func (r *RedisCASBroker) Subscribe(ctx context.Context, events chan<- *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		return fmt.Errorf("already subscribed")
	}

	r.subscribed = true
	r.subscription = events

	// Start polling goroutine
	r.wg.Add(1)
	go r.pollForEvents(ctx)

	return nil
}

// pollForEvents runs the event polling loop
func (r *RedisCASBroker) pollForEvents(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			if err := r.checkForNewEvents(ctx); err != nil {
				// Log error but continue
				fmt.Printf("Error checking for new events: %v\n", err)
			}
		}
	}
}

// checkForNewEvents checks for new events since last known timestamp
// For simplicity, all events are treated as rebuild events
func (r *RedisCASBroker) checkForNewEvents(ctx context.Context) error {
	// Get current event from Redis
	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil // No events yet
		}
		return err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		return fmt.Errorf("failed to unmarshal latest event: %w", err)
	}

	r.timestampMu.Lock()
	defer r.timestampMu.Unlock()

	// Check if this is a new event
	if latestEvent.Timestamp <= r.lastTimestamp {
		return nil // No new events
	}

	// Skip events from this node
	if latestEvent.NodeID == r.nodeID {
		// Update timestamp but don't process the event
		r.lastTimestamp = latestEvent.Timestamp
		return nil
	}

	// Convert any event to a rebuild event for simplicity
	rebuildEvent := &Event{
		Type:      EventTypeRebuild,
		Timestamp: latestEvent.Event.Timestamp,
		NodeID:    latestEvent.NodeID,
		Data:      nil, // No specific data needed for rebuild
	}

	// Send rebuild event to subscriber
	select {
	case r.subscription <- rebuildEvent:
		// Event sent successfully
		r.lastTimestamp = latestEvent.Timestamp
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopChan:
		return nil
	default:
		// Channel is full, skip this event but update timestamp
		r.lastTimestamp = latestEvent.Timestamp
	}

	return nil
}

// Health checks the health of the Redis connection
func (r *RedisCASBroker) Health(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the Redis CAS broker
func (r *RedisCASBroker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.subscribed {
		close(r.stopChan)
		r.wg.Wait()
		r.subscribed = false
	}

	return r.client.Close()
}

// GetLastTimestamp returns the last known event timestamp
func (r *RedisCASBroker) GetLastTimestamp() int64 {
	r.timestampMu.RLock()
	defer r.timestampMu.RUnlock()
	return r.lastTimestamp
}

// WaitForTimestamp waits for events to reach at least the specified timestamp
func (r *RedisCASBroker) WaitForTimestamp(ctx context.Context, targetTimestamp int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		r.timestampMu.RLock()
		currentTs := r.lastTimestamp
		r.timestampMu.RUnlock()

		if currentTs >= targetTimestamp {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue polling - check for new events manually
			if err := r.checkForNewEvents(ctx); err != nil {
				return fmt.Errorf("error checking for events: %w", err)
			}
		}
	}

	return fmt.Errorf("timeout waiting for timestamp %d", targetTimestamp)
}

// GetLatestEvent returns the current latest event from Redis (for debugging/testing)
func (r *RedisCASBroker) GetLatestEvent(ctx context.Context) (*LatestEvent, error) {
	eventData, err := r.client.Get(ctx, r.eventKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // No event exists
		}
		return nil, err
	}

	var latestEvent LatestEvent
	if err := json.Unmarshal([]byte(eventData), &latestEvent); err != nil {
		return nil, fmt.Errorf("failed to unmarshal latest event: %w", err)
	}

	return &latestEvent, nil
}
