# Race Condition Analysis and Fix

## Problem Identified

During rule updates, there's a potential race condition where:

1. **`updateRule`** takes a write lock on the main matcher mutex
2. **`GetRule`** can read the rule from `m.rules[ruleID]` while it's being updated
3. **Forest index operations** can take time and introduce windows where rule state is inconsistent

## The Race Condition Window

```go
// In updateRule:
m.mu.Lock()  // Write lock acquired

// Window 1: Rule updated in m.rules but not yet in forest
m.rules[rule.ID] = newRule

// Window 2: Forest operations take time
forestIndex.RemoveRule(oldRule)  // May take time
forestIndex.AddRule(newRule)     // May take time

m.mu.Unlock()
```

During these windows, `GetRule` could:
- See the new rule in `m.rules` before forest index is updated
- See inconsistent state between rule data and forest index

## Current Fix Implementation

Our fix temporarily removes the rule from `m.rules` during forest updates:

```go
// Step 1: Remove rule from m.rules (prevents GetRule from seeing partial state)
if oldRule != nil {
    delete(m.rules, rule.ID)
}

// Step 2: Update forest indexes while rule is not accessible
oldForestIndex.RemoveRule(oldRule)
forestIndex.AddRule(newRule)

// Step 3: Restore rule to m.rules with new data
m.rules[rule.ID] = newRule
```

## Benefits of This Approach

1. **Atomic Visibility**: `GetRule` either sees the complete old rule or complete new rule, never partial state
2. **Temporary Unavailability**: During updates, `GetRule` may return "not found" but never partial data
3. **Query Consistency**: `FindAllMatches` only returns rules that are properly indexed in the forest

## Alternative Approaches Considered

### 1. Double-Buffering
```go
// Keep old and new rules, swap atomically
newRules := make(map[string]*Rule)
// ... populate newRules
m.rules = newRules  // Atomic swap
```
**Issue**: Complex to implement with forest indexes

### 2. Copy-on-Write
```go
// Copy rule before returning
func (m *InMemoryMatcher) GetRule(id string) (*Rule, error) {
    rule := m.rules[id]
    return deepCopy(rule), nil
}
```
**Issue**: Performance overhead, doesn't solve forest consistency

### 3. Versioning
```go
type VersionedRule struct {
    Rule *Rule
    Version int64
    Valid bool
}
```
**Issue**: Complexity, memory overhead

## Verification Strategy

The fix ensures that:
1. Rules are never returned in partial state
2. Temporary unavailability during updates is acceptable
3. Once an update completes, all access methods see the consistent new state
4. No deadlocks occur between matcher and forest mutexes

## Test Results Expected

- `GetRule` during update: Either returns old rule, new rule, or "not found" - never partial
- `FindAllMatches` during update: Only returns properly indexed rules
- No race conditions detected by Go race detector
- All consistency tests pass

This approach prioritizes **consistency over availability** during the brief update window, which is appropriate for a rule matching system where correctness is critical.
