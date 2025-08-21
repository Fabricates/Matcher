# Redis-based Synchronization

This document describes how to use the Redis-based synchronization system for the matcher database.

## Overview

The Redis-based synchronization system replaces complex message queue dependencies with Redis streams, providing:

- **Idempotent event processing** with unique event IDs
- **Change listeners** for real-time synchronization
- **Data loaders** for processing change events
- **Redis streams** for reliable message delivery between instances

## Architecture

```
┌─────────────────┐    Redis Stream     ┌─────────────────┐
│   Instance A    │ ◄──────────────────► │   Instance B    │
│                 │                      │                 │
│ ┌─────────────┐ │                      │ ┌─────────────┐ │
│ │ RedisEvent  │ │   XADD (publish)     │ │ RedisEvent  │ │
│ │ Broker      │ │ ──────────────────── │ │ Broker      │ │
│ └─────────────┘ │                      │ └─────────────┘ │
│                 │   XREAD (consume)    │                 │
│ ┌─────────────┐ │ ◄──────────────────── │ ┌─────────────┐ │
│ │ InMemory    │ │                      │ │ InMemory    │ │
│ │ Matcher     │ │                      │ │ Matcher     │ │
│ └─────────────┘ │                      │ └─────────────┘ │
└─────────────────┘                      └─────────────────┘
```

## Quick Start

### 1. Create Redis Event Broker

```go
import "github.com/worthies/matcher"

// Redis connection configuration
redisAddr := "localhost:6379"
redisPassword := ""
redisDB := 0
streamKey := "matcher-events"
consumerGroup := "matcher-group"
nodeID := "node-1"

// Create Redis broker
broker := matcher.NewRedisEventBroker(redisAddr, redisPassword, redisDB, streamKey, consumerGroup, nodeID)
defer broker.Close()
```

### 2. Create Matcher with Redis Broker

```go
// Create persistence layer
persistence := matcher.NewJSONPersistence("./data")

// Create matcher with Redis synchronization
matcher, err := matcher.NewInMemoryMatcher(persistence, broker, nodeID)
if err != nil {
    log.Fatalf("Failed to create matcher: %v", err)
}
defer matcher.Close()
```

### 3. Use the Matcher

```go
// Add dimension configuration
dimensionConfig := &matcher.DimensionConfig{
    Name:     "product",
    Index:    0,
    Required: true,
    Weight:   10.0,
}
matcher.AddDimension(dimensionConfig)

// Add rules - automatically synchronized to other instances
rule := matcher.NewRule("rule-1").
    Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
    Build()
matcher.AddRule(rule)

// Query - works on all synchronized instances
query := map[string]string{"product": "ProductA"}
matches, err := matcher.FindAllMatches(matcher.CreateQuery(query))
```

## Event Types and Idempotence

### Event Structure

All events have unique IDs for idempotence:

```go
type Event struct {
    ID        string      `json:"id"`        // Unique event ID for idempotence
    Type      EventType   `json:"type"`      // Event type
    Timestamp time.Time   `json:"timestamp"` // When event occurred
    NodeID    string      `json:"node_id"`   // Source node ID
    Data      interface{} `json:"data"`      // Event payload
}
```

### Supported Event Types

- `EventTypeRuleAdded` - New rule added
- `EventTypeRuleUpdated` - Existing rule modified
- `EventTypeRuleDeleted` - Rule removed
- `EventTypeDimensionAdded` - New dimension configuration
- `EventTypeDimensionUpdated` - Dimension configuration modified
- `EventTypeDimensionDeleted` - Dimension configuration removed

### Idempotence Guarantees

1. **Unique Event IDs**: Each event gets a timestamp-based unique ID
2. **Duplicate Detection**: Consumers track processed event IDs
3. **Source Filtering**: Nodes ignore events from themselves
4. **Redis Streams**: Built-in message ordering and delivery guarantees

## Configuration Options

### Redis Connection

```go
broker := matcher.NewRedisEventBroker(
    "redis:6379",     // Redis address
    "password123",    // Redis password (empty for no auth)
    1,               // Redis database number
    "app-events",    // Stream key name
    "app-group",     // Consumer group name
    "node-uuid"      // Unique node identifier
)
```

### Consumer Groups

Redis streams use consumer groups for:
- **Load balancing**: Multiple consumers share work
- **Fault tolerance**: Failed messages can be reassigned
- **Message acknowledgment**: Ensures reliable processing

### Stream Management

Monitor stream health:

```go
info, err := broker.GetStreamInfo(context.Background())
if err == nil {
    fmt.Printf("Stream length: %v\n", info["length"])
    fmt.Printf("Consumer groups: %v\n", info["groups"])
}
```

## Production Deployment

### 1. Redis Configuration

Recommended Redis configuration:

```ini
# redis.conf
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
```

### 2. Connection Pooling

For high-throughput scenarios, consider connection pooling:

```go
// Custom Redis client with connection pool
client := redis.NewClient(&redis.Options{
    Addr:         "redis:6379",
    PoolSize:     10,
    MinIdleConns: 5,
    MaxRetries:   3,
})
```

### 3. Monitoring

Monitor key metrics:
- Stream length growth rate
- Consumer lag
- Failed message count
- Connection health

### 4. Error Handling

The Redis broker includes comprehensive error handling:

```go
// Health checks
if err := broker.Health(ctx); err != nil {
    log.Printf("Redis unhealthy: %v", err)
}

// Graceful shutdown
defer func() {
    if err := broker.Close(); err != nil {
        log.Printf("Error closing broker: %v", err)
    }
}()
```

## Migration from Kafka

If migrating from the existing Kafka broker:

1. **Interface Compatibility**: `RedisEventBroker` implements the same `EventBrokerInterface`
2. **Drop-in Replacement**: Simply replace broker creation
3. **Event Format**: Same event structure with added ID field
4. **No Code Changes**: Existing matcher code works unchanged

### Migration Example

```go
// Before (Kafka)
broker := matcher.NewKafkaEventBroker(brokers, topic, consumerGroup, nodeID)

// After (Redis)
broker := matcher.NewRedisEventBroker(redisAddr, password, db, streamKey, consumerGroup, nodeID)
```

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure Redis is running and accessible
   - Check firewall/network configuration
   - Verify Redis port (default 6379)

2. **Consumer Group Errors**
   - Consumer groups are auto-created
   - Check Redis logs for permission issues
   - Ensure unique consumer names per instance

3. **Event Processing Delays**
   - Monitor stream length growth
   - Check consumer processing speed
   - Consider scaling consumers

### Debug Logging

Enable debug logging to see event flow:

```go
// Events are logged with [REDIS] prefix
fmt.Printf("[REDIS] Published event %s\n", event.ID)
fmt.Printf("[REDIS] Processed event %s from node %s\n", eventID, nodeID)
```

## Performance Considerations

- **Batch Processing**: Consumer reads up to 10 messages per batch
- **Memory Usage**: Processed event IDs are kept in memory
- **Network**: Redis streams are efficient but consider latency
- **Throughput**: Suitable for typical rule management workloads

## Security

- Use Redis AUTH for authentication
- Enable TLS for network encryption
- Restrict Redis network access
- Use strong consumer group names
- Implement rate limiting if needed

## Example Applications

See `example/redis_sync_example.go` for a complete working example demonstrating distributed synchronization between multiple matcher instances.