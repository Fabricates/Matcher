package matcher

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// DumpCacheToFile dumps the cache as key-value pairs to a file
func DumpCacheToFile(cache interface{}, filename string) error {
	// Handle different cache types
	switch c := cache.(type) {
	case *QueryCache:
		return dumpQueryCacheToFile(c, filename+".cache")
	case *MultiLevelCache:
		return dumpMultiLevelCacheToFile(c, filename+".cache")
	default:
		return fmt.Errorf("unsupported cache type: %T", cache)
	}
}

// dumpQueryCacheToFile dumps a QueryCache as key-value pairs
func dumpQueryCacheToFile(cache *QueryCache, filename string) error {
	if cache == nil {
		return fmt.Errorf("cache is nil")
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	var entries []string

	// Dump cache entries
	for key, entry := range cache.entries {
		if !entry.IsExpired() {
			entries = append(entries, fmt.Sprintf("L1|%s|%s|%s|%.2f|key=%s",
				key,
				entry.Result.Rule.ID,
				entry.Timestamp.Format(time.RFC3339),
				entry.Result.TotalWeight,
				entry.Key))
		}
	}

	// Sort entries for consistent output
	sort.Strings(entries)

	// Write to file
	content := strings.Join(entries, "\n")
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write cache dump file: %w", err)
	}

	return nil
}

// dumpMultiLevelCacheToFile dumps a MultiLevelCache as key-value pairs
func dumpMultiLevelCacheToFile(cache *MultiLevelCache, filename string) error {
	if cache == nil {
		return fmt.Errorf("cache is nil")
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	var entries []string

	// Dump L1 cache entries
	cache.l1Cache.mu.RLock()
	for key, entry := range cache.l1Cache.entries {
		if !entry.IsExpired() {
			entries = append(entries, fmt.Sprintf("L1|%s|%s|%s|%.2f",
				key,
				entry.Result.Rule.ID,
				entry.Timestamp.Format(time.RFC3339),
				entry.Result.TotalWeight))
		}
	}
	cache.l1Cache.mu.RUnlock()

	// Dump L2 cache entries
	cache.l2Cache.mu.RLock()
	for key, entry := range cache.l2Cache.entries {
		if !entry.IsExpired() {
			entries = append(entries, fmt.Sprintf("L2|%s|%s|%s|%.2f",
				key,
				entry.Result.Rule.ID,
				entry.Timestamp.Format(time.RFC3339),
				entry.Result.TotalWeight))
		}
	}
	cache.l2Cache.mu.RUnlock()

	// Sort entries for consistent output
	sort.Strings(entries)

	// Write to file
	content := strings.Join(entries, "\n")
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write cache dump file: %w", err)
	}

	return nil
}

// DumpForestToFile dumps the forest in concise graph format to a file
func DumpForestToFile(m *InMemoryMatcher, filename string) error {
	// We'll produce two files per requested filename:
	//  - <filename>.graph   : concise graph listing edges between node keys (dimension|value+match)
	//  - <filename>.mapping : mapping of node_key -> comma-separated rule IDs (for lookup)

	m.mu.RLock()
	// Snapshot the forestIndexes keys to avoid holding matcher lock while writing files
	tenantKeys := make([]string, 0, len(m.forestIndexes))
	for k := range m.forestIndexes {
		tenantKeys = append(tenantKeys, k)
	}
	m.mu.RUnlock()

	var graphLines []string
	var mappingLines []string

	// For each tenant/application forest, dump relationships and node->rule mapping
	for _, tenantKey := range tenantKeys {
		m.mu.RLock()
		forestIndex := m.forestIndexes[tenantKey]
		m.mu.RUnlock()
		if forestIndex == nil || forestIndex.RuleForest == nil {
			continue
		}

		forest := forestIndex.RuleForest

		// Tenant header
		graphLines = append(graphLines, fmt.Sprintf("# Tenant: %s", tenantKey))
		mappingLines = append(mappingLines, fmt.Sprintf("# Tenant: %s", tenantKey))
		graphs, relationship := make(map[string]any), ""

		// Snapshot NodeRelationships under forest lock
		forest.mu.RLock()
		for current, trans := range forest.NodeRelationships {
			b := strings.Builder{}
			for rid, next := range trans {
				relationship = fmt.Sprintf("%s %s", current, next)
				if _, ok := graphs[relationship]; !ok {
					graphs[relationship] = nil
					graphLines = append(graphLines, relationship)
				}
				b.WriteString(rid)
				b.WriteString(",")
			}
			mappingLines = append(mappingLines, fmt.Sprintf("%s %s", current, b.String()))
		}
		forest.mu.RUnlock()

		// Separator between tenants
		graphLines = append(graphLines, "")
		mappingLines = append(mappingLines, "")
	}

	// Write graph file
	graphFile := filename + ".graph"
	if err := os.WriteFile(graphFile, []byte(strings.Join(graphLines, "\n")), 0644); err != nil {
		return fmt.Errorf("failed to write forest graph file: %w", err)
	}

	// Write mapping file
	mappingFile := filename + ".mapping"
	if err := os.WriteFile(mappingFile, []byte(strings.Join(mappingLines, "\n")), 0644); err != nil {
		return fmt.Errorf("failed to write forest mapping file: %w", err)
	}

	return nil
}
