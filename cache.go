package matcher

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
	"time"
)

// CacheEntry represents a cached query result
type CacheEntry struct {
	Result    *MatchResult  `json:"result"`
	Timestamp time.Time     `json:"timestamp"`
	TTL       time.Duration `json:"ttl"`
}

// IsExpired checks if the cache entry has expired
func (ce *CacheEntry) IsExpired() bool {
	return time.Now().After(ce.Timestamp.Add(ce.TTL))
}

// QueryCache implements a thread-safe LRU cache for query results
type QueryCache struct {
	entries    map[string]*CacheEntry
	accessTime map[string]time.Time // For LRU tracking
	maxSize    int
	defaultTTL time.Duration
	mu         sync.RWMutex
}

// NewQueryCache creates a new query cache
func NewQueryCache(maxSize int, defaultTTL time.Duration) *QueryCache {
	return &QueryCache{
		entries:    make(map[string]*CacheEntry),
		accessTime: make(map[string]time.Time),
		maxSize:    maxSize,
		defaultTTL: defaultTTL,
	}
}

// generateCacheKey generates a cache key from a query
func (qc *QueryCache) generateCacheKey(query *QueryRule) string {
	// Sort dimension values for consistent key generation
	var keys []string
	for dim, value := range query.Values {
		keys = append(keys, fmt.Sprintf("%s:%s", dim, value))
	}
	sort.Strings(keys)

	// Create a hash of the sorted keys
	keyString := ""
	for i, key := range keys {
		if i > 0 {
			keyString += "|"
		}
		keyString += key
	}
	
	// Include IncludeAllRules flag in the key to distinguish cache entries
	if query.IncludeAllRules {
		keyString += "|include_all:true"
	} else {
		keyString += "|include_all:false"
	}
	
	// Include tenant and application context to ensure isolation
	keyString += fmt.Sprintf("|tenant:%s|app:%s", query.TenantID, query.ApplicationID)

	// Generate MD5 hash for consistent key length
	hash := md5.Sum([]byte(keyString))
	return fmt.Sprintf("%x", hash)
}

// Get retrieves a cached result for a query
func (qc *QueryCache) Get(query *QueryRule) *MatchResult {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.generateCacheKey(query)
	entry, exists := qc.entries[key]

	if !exists {
		return nil
	}

	// Check if entry has expired
	if entry.IsExpired() {
		// Remove expired entry
		delete(qc.entries, key)
		delete(qc.accessTime, key)
		return nil
	}

	// Update access time
	qc.accessTime[key] = time.Now()

	return entry.Result
}

// Set stores a result in the cache
func (qc *QueryCache) Set(query *QueryRule, result *MatchResult) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.generateCacheKey(query)
	now := time.Now()

	// Create cache entry
	entry := &CacheEntry{
		Result:    result,
		Timestamp: now,
		TTL:       qc.defaultTTL,
	}

	// Check if we need to evict entries
	if len(qc.entries) >= qc.maxSize {
		qc.evictLRU()
	}

	// Store the entry
	qc.entries[key] = entry
	qc.accessTime[key] = now
}

// SetWithTTL stores a result in the cache with custom TTL
func (qc *QueryCache) SetWithTTL(query *QueryRule, result *MatchResult, ttl time.Duration) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.generateCacheKey(query)
	now := time.Now()

	// Create cache entry with custom TTL
	entry := &CacheEntry{
		Result:    result,
		Timestamp: now,
		TTL:       ttl,
	}

	// Check if we need to evict entries
	if len(qc.entries) >= qc.maxSize {
		qc.evictLRU()
	}

	// Store the entry
	qc.entries[key] = entry
	qc.accessTime[key] = now
}

// evictLRU removes the least recently used entry
func (qc *QueryCache) evictLRU() {
	if len(qc.entries) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, accessTime := range qc.accessTime {
		if first || accessTime.Before(oldestTime) {
			oldestKey = key
			oldestTime = accessTime
			first = false
		}
	}

	if oldestKey != "" {
		delete(qc.entries, oldestKey)
		delete(qc.accessTime, oldestKey)
	}
}

// Clear removes all entries from the cache
func (qc *QueryCache) Clear() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.entries = make(map[string]*CacheEntry)
	qc.accessTime = make(map[string]time.Time)
}

// Size returns the current number of entries in the cache
func (qc *QueryCache) Size() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	return len(qc.entries)
}

// CleanupExpired removes all expired entries from the cache
func (qc *QueryCache) CleanupExpired() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	var expiredKeys []string

	for key, entry := range qc.entries {
		if entry.IsExpired() {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(qc.entries, key)
		delete(qc.accessTime, key)
	}

	return len(expiredKeys)
}

// Stats returns cache statistics
func (qc *QueryCache) Stats() map[string]interface{} {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	totalEntries := len(qc.entries)
	expiredCount := 0

	for _, entry := range qc.entries {
		if entry.IsExpired() {
			expiredCount++
		}
	}

	return map[string]interface{}{
		"total_entries":   totalEntries,
		"expired_entries": expiredCount,
		"max_size":        qc.maxSize,
		"default_ttl":     qc.defaultTTL.String(),
	}
}

// StartCleanupWorker starts a background worker to clean up expired entries
func (qc *QueryCache) StartCleanupWorker(interval time.Duration) chan<- bool {
	stopChan := make(chan bool)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				qc.CleanupExpired()
			case <-stopChan:
				return
			}
		}
	}()

	return stopChan
}

// MultiLevelCache implements a multi-level cache system
type MultiLevelCache struct {
	l1Cache      *QueryCache // Fast, small cache
	l2Cache      *QueryCache // Larger, slower cache
	l1HitRate    float64
	l2HitRate    float64
	totalHits    int64
	totalQueries int64
	mu           sync.RWMutex
}

// NewMultiLevelCache creates a new multi-level cache
func NewMultiLevelCache(l1Size int, l1TTL time.Duration, l2Size int, l2TTL time.Duration) *MultiLevelCache {
	return &MultiLevelCache{
		l1Cache: NewQueryCache(l1Size, l1TTL),
		l2Cache: NewQueryCache(l2Size, l2TTL),
	}
}

// Get retrieves a result from the multi-level cache
func (mlc *MultiLevelCache) Get(query *QueryRule) *MatchResult {
	mlc.mu.Lock()
	mlc.totalQueries++
	mlc.mu.Unlock()

	// Try L1 cache first
	if result := mlc.l1Cache.Get(query); result != nil {
		mlc.updateHitStats(1)
		return result
	}

	// Try L2 cache
	if result := mlc.l2Cache.Get(query); result != nil {
		// Promote to L1 cache
		mlc.l1Cache.Set(query, result)
		mlc.updateHitStats(2)
		return result
	}

	// Cache miss
	return nil
}

// Set stores a result in both cache levels
func (mlc *MultiLevelCache) Set(query *QueryRule, result *MatchResult) {
	mlc.l1Cache.Set(query, result)
	mlc.l2Cache.Set(query, result)
}

// updateHitStats updates hit rate statistics
func (mlc *MultiLevelCache) updateHitStats(level int) {
	mlc.mu.Lock()
	defer mlc.mu.Unlock()

	mlc.totalHits++

	// Update level-specific hit rates using exponential moving average
	switch level {
	case 1:
		mlc.l1HitRate = mlc.l1HitRate*0.95 + 0.05
	case 2:
		mlc.l2HitRate = mlc.l2HitRate*0.95 + 0.05
	}
}

// Clear clears both cache levels
func (mlc *MultiLevelCache) Clear() {
	mlc.l1Cache.Clear()
	mlc.l2Cache.Clear()

	mlc.mu.Lock()
	mlc.l1HitRate = 0
	mlc.l2HitRate = 0
	mlc.totalHits = 0
	mlc.totalQueries = 0
	mlc.mu.Unlock()
}

// Stats returns comprehensive cache statistics
func (mlc *MultiLevelCache) Stats() map[string]interface{} {
	mlc.mu.RLock()
	defer mlc.mu.RUnlock()

	overallHitRate := 0.0
	if mlc.totalQueries > 0 {
		overallHitRate = float64(mlc.totalHits) / float64(mlc.totalQueries)
	}

	return map[string]interface{}{
		"overall_hit_rate": overallHitRate,
		"l1_hit_rate":      mlc.l1HitRate,
		"l2_hit_rate":      mlc.l2HitRate,
		"total_queries":    mlc.totalQueries,
		"total_hits":       mlc.totalHits,
		"l1_stats":         mlc.l1Cache.Stats(),
		"l2_stats":         mlc.l2Cache.Stats(),
	}
}
