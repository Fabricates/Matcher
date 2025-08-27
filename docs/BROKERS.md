# Event Brokers Documentation

This document describes the event broker implementations available in the matcher module. The system supports three different event broker types for distributed event communication.

## Overview

The matcher module provides an `Broker` that can be implemented by different message brokers to enable distributed event communication between matcher instances. This allows multiple nodes to coordinate rule changes, dimension updates, and other events.

## Available Brokers

### 1. Redis Event Broker

Uses Redis Streams for event distribution. Ideal for real-time coordination with automatic consumer group management.

**Features:**
- Redis Streams-based messaging
- Consumer group support for load balancing
- Automatic message acknowledgment
- Node-level event filtering to prevent loops
- Health checking and reconnection

**Configuration:**
```go
config := matcher.RedisEventBrokerConfig{
    RedisAddr:     "localhost:6379",
    Password:      "",           // Redis password (empty if no auth)
    DB:            0,            // Redis database number
    StreamName:    "matcher-events",
    ConsumerGroup: "matcher-group",
    ConsumerName:  "consumer-1",
    NodeID:        "node-1",     // Unique identifier for this node
}

broker, err := matcher.NewRedisEventBroker(config)
```

**Usage Example:**
```go
ctx := context.Background()

// Create event channel
events := make(chan *matcher.Event, 10)

// Subscribe to events
go func() {
    if err := broker.Subscribe(ctx, events); err != nil {
        log.Printf("Subscribe failed: %v", err)
    }
}()

// Publish an event
event := &matcher.Event{
    Type:      matcher.EventTypeRuleAdded,
    Timestamp: time.Now(),
    NodeID:    "node-1",
    Data:      ruleData,
}

if err := broker.Publish(ctx, event); err != nil {
    log.Printf("Publish failed: %v", err)
}

// Clean up
defer broker.Close()
```

### 2. Kafka Event Broker

Uses Apache Kafka for high-throughput, distributed event streaming. Best for large-scale deployments with high event volumes.

**Features:**
- Apache Kafka integration using segmentio/kafka-go
- Producer/consumer patterns with offset management
- Automatic topic creation
- Message headers for metadata
- Built-in partitioning and replication
- Node-level event filtering

**Configuration:**
```go
config := matcher.KafkaConfig{
    Brokers:       []string{"localhost:9092", "kafka2:9092"},
    Topic:         "matcher-events",
    ConsumerGroup: "matcher-group",
    NodeID:        "node-1",
}

broker, err := matcher.NewKafkaEventBroker(config)
```

**Usage Example:**
```go
ctx := context.Background()

// Health check
if err := broker.Health(ctx); err != nil {
    log.Printf("Kafka unhealthy: %v", err)
    return
}

// Create event channel
events := make(chan *matcher.Event, 10)

// Subscribe to events
go func() {
    if err := broker.Subscribe(ctx, events); err != nil {
        log.Printf("Subscribe failed: %v", err)
    }
}()

// Publish an event
event := &matcher.Event{
    Type:      matcher.EventTypeRuleUpdated,
    Timestamp: time.Now(),
    NodeID:    "node-1",
    Data:      updatedRule,
}

if err := broker.Publish(ctx, event); err != nil {
    log.Printf("Publish failed: %v", err)
}

// Process incoming events
go func() {
    for event := range events {
        // Handle event
        fmt.Printf("Received: %+v\n", event)
    }
}()

// Clean up
defer broker.Close()
```

### 3. In-Memory Event Broker

A simple in-memory broker for testing, development, and single-node deployments.

**Features:**
- No external dependencies
- Immediate event delivery
- Event history storage
- Subscriber management
- Perfect for testing and development

**Usage Example:**
```go
broker := matcher.NewInMemoryEventBroker("node-1")
defer broker.Close()

// Same API as other brokers
events := make(chan *matcher.Event, 10)
broker.Subscribe(ctx, events)

// Publish events
event := &matcher.Event{
    Type:      matcher.EventTypeDimensionAdded,
    Timestamp: time.Now(),
    NodeID:    "node-1",
    Data:      dimensionData,
}
broker.Publish(ctx, event)

// Access additional methods for testing
eventCount := broker.GetEventCount()
subscriberCount := broker.GetSubscriberCount()
allEvents := broker.GetStoredEvents()
```

## Event Types

The system supports the following event types:

```go
// Rule-related events
matcher.EventTypeRuleAdded      // New rule added
matcher.EventTypeRuleUpdated    // Existing rule modified
matcher.EventTypeRuleDeleted    // Rule removed

// Dimension-related events
matcher.EventTypeDimensionAdded   // New dimension added
matcher.EventTypeDimensionUpdated // Dimension modified
matcher.EventTypeDimensionDeleted // Dimension removed
```

## Event Structure

All events follow this structure:

```go
type Event struct {
    Type      EventType   `json:"type"`       // Event type from above
    Timestamp time.Time   `json:"timestamp"`  // When event occurred
    NodeID    string      `json:"node_id"`    // Source node identifier
    Data      interface{} `json:"data"`       // Event-specific payload
}
```

## Best Practices

### Node Identification
- Always use unique `NodeID` values for each instance
- Node IDs are used to prevent event loops in distributed setups
- Consider using hostname, IP, or UUID-based identifiers

### Error Handling
- Always check errors from broker operations
- Implement retry logic for transient failures
- Monitor broker health regularly

### Resource Management
- Always call `Close()` when shutting down
- Use context for cancellation and timeouts
- Monitor channel buffer sizes to prevent blocking

### Production Deployment

**Redis Deployment:**
- Use Redis Cluster for high availability
- Configure appropriate stream retention policies
- Monitor memory usage and stream lengths
- Set up Redis authentication and TLS

**Kafka Deployment:**
- Configure appropriate partition counts for parallelism
- Set up proper replication factors for durability
- Monitor consumer lag and throughput
- Use Kafka security features (SASL, TLS)

## Integration Example

Here's how to integrate event brokers with the matcher engine:

```go
// Create matcher with event broker
engine := matcher.NewEngine()

// Set up Redis broker
config := matcher.RedisEventBrokerConfig{
    RedisAddr:     "localhost:6379",
    StreamName:    "matcher-events",
    ConsumerGroup: "matcher-cluster",
    ConsumerName:  "worker-1",
    NodeID:        "matcher-node-1",
}

broker, err := matcher.NewRedisEventBroker(config)
if err != nil {
    log.Fatal(err)
}
defer broker.Close()

// Listen for events
events := make(chan *matcher.Event, 100)
go func() {
    broker.Subscribe(context.Background(), events)
}()

// Handle incoming events
go func() {
    for event := range events {
        switch event.Type {
        case matcher.EventTypeRuleAdded:
            // Sync new rule to local engine
            if ruleData, ok := event.Data.(*matcher.Rule); ok {
                engine.AddRule(ruleData)
            }
        case matcher.EventTypeRuleDeleted:
            // Remove rule from local engine
            if ruleID, ok := event.Data.(string); ok {
                engine.RemoveRule(ruleID)
            }
        }
    }
}()

// When making local changes, broadcast them
newRule := &matcher.Rule{...}
engine.AddRule(newRule)

// Notify other nodes
event := &matcher.Event{
    Type:      matcher.EventTypeRuleAdded,
    Timestamp: time.Now(),
    NodeID:    "matcher-node-1",
    Data:      newRule,
}
broker.Publish(context.Background(), event)
```

## Testing

Use the included demo to test broker functionality:

```bash
cd example/brokers_demo
go run main.go
```

This will test all three broker types and show their behavior with and without external services running.
