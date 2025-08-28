# Multi-Tenant and Application Support

This document explains the multi-tenant and application support functionality added to the High-Performance Rule Matching Engine.

## Overview

The matcher now supports multiple tenants and applications within the same engine instance, providing complete isolation between different tenant/application contexts while maintaining backward compatibility.

## Key Features

- **Tenant Isolation**: Rules and dimensions can be scoped to specific tenants
- **Application Separation**: Within a tenant, rules can be further scoped by application
- **Isolated Forests**: Each tenant/application combination gets its own rule forest for optimal performance
- **Secure Queries**: Queries are scoped to their tenant/application context and cannot access other tenants' data
- **Cache Isolation**: Query cache includes tenant/application context to prevent cross-tenant data leakage
- **Backward Compatibility**: Existing code without tenant context continues to work

## Core Concepts

### Tenant and Application Context

- **Tenant ID**: Identifies the tenant (customer, organization, etc.)
- **Application ID**: Identifies the specific application within a tenant
- **Default Context**: Empty tenant and application IDs represent the default/global context

### Forest Organization

The engine maintains separate rule forests for each tenant/application combination:

```
ForestIndexes: map[string]*ForestIndex
├── "default" (empty tenant/app) -> Global rules
├── "tenant1:app1" -> Rules for tenant1/app1
├── "tenant1:app2" -> Rules for tenant1/app2
└── "tenant2:app1" -> Rules for tenant2/app1
```

## API Usage

### Creating Rules with Tenant Context

```go
// Create a rule for a specific tenant and application
rule := NewRuleWithTenant("rule1", "tenant1", "app1").
    Dimension("product", "ProductA", MatchTypeEqual).
    Dimension("environment", "prod", MatchTypeEqual).
    Build()

// Or set tenant context on existing rule builder
rule := NewRule("rule2").
    Tenant("tenant1").
    Application("app1").
    Dimension("service", "auth", MatchTypeEqual).
    Build()

// Add the rule to the engine
err := engine.AddRule(rule)
```

### Creating Queries with Tenant Context

```go
// Query for a specific tenant and application
query := CreateQueryWithTenant("tenant1", "app1", map[string]string{
    "product": "ProductA",
    "environment": "prod",
})

result, err := engine.FindBestMatch(query)

// Query including draft rules
queryAll := CreateQueryWithAllRulesAndTenant("tenant1", "app1", map[string]string{
    "service": "auth",
})

results, err := engine.FindAllMatches(queryAll)
```

### Dimension Configuration with Tenant Context

```go
// Create dimension config for specific tenant/application
config := &DimensionConfig{
    Name:          "product",
    Index:         0,
    Required:      true,
    Weight:        10.0,
    TenantID:      "tenant1",
    ApplicationID: "app1",
}

err := engine.AddDimension(config)
```

## Tenant Isolation

### Rule Isolation

Rules are completely isolated by tenant/application context:

```go
// Add rule for tenant1
rule1 := NewRuleWithTenant("rule", "tenant1", "app1").
    Dimension("type", "A", MatchTypeEqual).
    Build()

// Add rule for tenant2 (same dimension values, different tenant)
rule2 := NewRuleWithTenant("rule", "tenant2", "app1").
    Dimension("type", "A", MatchTypeEqual).
    Build()

// Query tenant1 - only finds rule1
query1 := CreateQueryWithTenant("tenant1", "app1", map[string]string{"type": "A"})
result1, _ := engine.FindBestMatch(query1) // Returns rule1

// Query tenant2 - only finds rule2
query2 := CreateQueryWithTenant("tenant2", "app1", map[string]string{"type": "A"})
result2, _ := engine.FindBestMatch(query2) // Returns rule2
```

### Weight Conflict Isolation

Weight conflicts are checked only within the same tenant/application context:

```go
// These rules can have the same weight because they're in different tenant contexts
rule1 := NewRuleWithTenant("rule1", "tenant1", "app1").
    Dimension("type", "A", MatchTypeEqual).
    Build() // Weight: 10.0

rule2 := NewRuleWithTenant("rule2", "tenant2", "app1").
    Dimension("type", "B", MatchTypeEqual).
    Build() // Weight: 10.0 (allowed, different tenant)

// This would cause a conflict (same tenant, same weight)
rule3 := NewRuleWithTenant("rule3", "tenant1", "app1").
    Dimension("type", "C", MatchTypeEqual).
    Build() // Error: weight conflict with rule1
```

### Cache Isolation

The query cache includes tenant/application context in the cache key:

```go
// These queries have different cache keys even with same dimension values
query1 := CreateQueryWithTenant("tenant1", "app1", map[string]string{"type": "A"})
query2 := CreateQueryWithTenant("tenant2", "app1", map[string]string{"type": "A"})

// Each query will be cached separately
result1, _ := engine.FindBestMatch(query1) // Cache miss, result cached
result2, _ := engine.FindBestMatch(query2) // Cache miss, different key
result1Again, _ := engine.FindBestMatch(query1) // Cache hit
```

## Backward Compatibility

### Existing Code

All existing code continues to work without modification:

```go
// This still works - uses default tenant context
rule := NewRule("global_rule").
    Dimension("region", "us-west", MatchTypeEqual).
    Build()

query := CreateQuery(map[string]string{"region": "us-west"})
result, err := engine.FindBestMatch(query)
```

### Global vs Tenant Rules

- Rules without tenant context (empty `TenantID` and `ApplicationID`) are global
- Queries without tenant context only match global rules
- Queries with tenant context only match rules from that specific tenant/application

## Multi-Application Support

Within a single tenant, you can have multiple applications:

```go
// Rules for different applications within the same tenant
app1Rule := NewRuleWithTenant("auth_rule", "tenant1", "auth_service").
    Dimension("endpoint", "/login", MatchTypeEqual).
    Build()

app2Rule := NewRuleWithTenant("payment_rule", "tenant1", "payment_service").
    Dimension("endpoint", "/charge", MatchTypeEqual).
    Build()

// Queries are isolated by application
authQuery := CreateQueryWithTenant("tenant1", "auth_service", 
    map[string]string{"endpoint": "/login"})   // Finds auth_rule

paymentQuery := CreateQueryWithTenant("tenant1", "payment_service", 
    map[string]string{"endpoint": "/charge"}) // Finds payment_rule
```

## Performance Considerations

### Forest Separation

Each tenant/application combination maintains its own rule forest, which provides:

- **Isolation**: No cross-tenant data access
- **Performance**: Smaller forests mean faster queries
- **Scalability**: Adding tenants doesn't impact existing tenant performance

### Memory Usage

- Each forest has its own memory overhead
- Consider the number of tenant/application combinations when planning capacity
- Empty forests (no rules) have minimal overhead

### Statistics

Forest statistics are provided per tenant/application:

```go
// Get statistics for all forests
me := &MatcherEngine{matcher: engine}
stats := me.GetForestStats()

// Returns map with keys like:
// "tenant1:app1": {...forest stats...}
// "tenant2:app1": {...forest stats...}
// "total_forests": 3
// "total_rules": 150
```

## Migration Guide

### From Single-Tenant to Multi-Tenant

1. **Identify Tenant Context**: Determine how to identify tenants and applications in your system
2. **Update Rule Creation**: Add tenant/application context when creating rules
3. **Update Queries**: Add tenant/application context to queries
4. **Update Dimension Configs**: Scope dimension configs by tenant/application if needed
5. **Test Isolation**: Verify that tenants cannot access each other's data

### Example Migration

```go
// Before (single-tenant)
rule := NewRule("user_rule").
    Dimension("user_type", "premium", MatchTypeEqual).
    Build()

query := CreateQuery(map[string]string{"user_type": "premium"})

// After (multi-tenant)
rule := NewRuleWithTenant("user_rule", tenantID, applicationID).
    Dimension("user_type", "premium", MatchTypeEqual).
    Build()

query := CreateQueryWithTenant(tenantID, applicationID, 
    map[string]string{"user_type": "premium"})
```

## Best Practices

1. **Unique Rule IDs**: Ensure rule IDs are globally unique across all tenants
2. **Consistent Context**: Always provide tenant/application context in production
3. **Test Isolation**: Write tests to verify tenant isolation
4. **Monitor Performance**: Monitor forest statistics per tenant
5. **Plan Capacity**: Consider tenant/application count in capacity planning
6. **Secure Context**: Ensure tenant/application context comes from authenticated sources

## Security Considerations

- Tenant isolation is enforced at the engine level
- Cache isolation prevents cross-tenant data leakage
- Always validate tenant/application context from trusted sources
- Consider additional application-level authorization checks
- Rule IDs should not contain sensitive information since they appear in logs and statistics