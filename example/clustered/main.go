package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	matcher "github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Clustered Matcher with Event Broadcasting Demo ===")

	// Simulate multiple nodes in a cluster
	nodes := []string{"node-1", "node-2", "node-3"}
	engines := make([]*matcher.MatcherEngine, len(nodes))
	brokers := make([]matcher.Broker, len(nodes))

	// Create engines for each node with event brokers
	for i, nodeID := range nodes {
		// Create in-memory event broker for this demo
		// In production, you would use KafkaEventBroker
		broker := matcher.NewInMemoryEventBroker(nodeID)
		brokers[i] = broker

		// Create matcher engine with event broker
		engine, err := matcher.NewMatcherEngine(
			matcher.NewJSONPersistence(fmt.Sprintf("./data/node-%d", i+1)),
			broker,
			nodeID,
		)
		if err != nil {
			slog.Error("Failed to create engine", "nodeID", nodeID, "error", err)
			os.Exit(1)
		}

		engines[i] = engine

		// Add dimensions
		dimensions := []*matcher.DimensionConfig{
			{Name: "product", Index: 0, Required: true, Weight: 10.0},
			{Name: "route", Index: 1, Required: false, Weight: 5.0},
			{Name: "tool", Index: 2, Required: false, Weight: 8.0},
			{Name: "recipe", Index: 3, Required: false, Weight: 12.0},
		}

		for _, dim := range dimensions {
			if err := engine.AddDimension(dim); err != nil {
				slog.Error("Failed to add dimension", "dimension", dim.Name, "nodeID", nodeID, "error", err)
				os.Exit(1)
			}
		}

		fmt.Printf("Created matcher engine for %s\n", nodeID)
	}

	// Connect brokers together to simulate distributed message queue
	// In production, this would be handled by Kafka/RabbitMQ/etc.
	connectBrokers(brokers)

	// Wait a bit for everything to initialize
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n=== Adding rules to different nodes ===")

	// Add rule to node-1
	rule1 := matcher.NewRule("production_rule_1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
		Dimension("route", "main", matcher.MatchTypeEqual, 5.0).
		Dimension("tool", "laser", matcher.MatchTypeEqual, 8.0).
		Dimension("recipe", "recipe_alpha", matcher.MatchTypeEqual, 12.0).
		Metadata("description", "Production rule from node-1").
		Build()

	if err := engines[0].AddRule(rule1); err != nil {
		slog.Error("Failed to add rule to node-1", "error", err)
	} else {
		fmt.Println("✓ Added rule to node-1")
	}

	// Add rule to node-2
	rule2 := matcher.NewRule("production_rule_2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual, 8.0).
		Dimension("route", "backup", matcher.MatchTypeEqual, 3.0).
		Dimension("tool", "plasma", matcher.MatchTypeEqual, 6.0).
		Dimension("recipe", "recipe_beta", matcher.MatchTypeEqual, 10.0).
		Metadata("description", "Production rule from node-2").
		Build()

	if err := engines[1].AddRule(rule2); err != nil {
		slog.Error("Failed to add rule to node-2", "error", err)
	} else {
		fmt.Println("✓ Added rule to node-2")
	}

	// Wait for event propagation
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n=== Verifying rule synchronization ===")

	// Check if rules are synchronized across all nodes
	for i, engine := range engines {
		rules, err := engine.ListRules(0, 10)
		if err != nil {
			slog.Error("Failed to list rules", "node", i+1, "error", err)
			continue
		}

		fmt.Printf("Node-%d has %d rules:\n", i+1, len(rules))
		for _, rule := range rules {
			fmt.Printf("  - %s: %s\n", rule.ID, rule.Metadata["description"])
		}
	}

	fmt.Println("\n=== Testing queries on different nodes ===")

	// Test query on node-3 (which didn't add any rules directly)
	query1 := matcher.CreateQuery(map[string]string{
		"product": "ProductA",
		"route":   "main",
		"tool":    "laser",
		"recipe":  "recipe_alpha",
	})

	result, err := engines[2].FindBestMatch(query1)
	if err != nil {
		slog.Error("Query failed on node-3", "error", err)
	} else if result != nil {
		fmt.Printf("✓ Node-3 found matching rule: %s (weight: %.1f)\n",
			result.Rule.ID, result.TotalWeight)
	} else {
		fmt.Println("✗ Node-3 found no matching rule")
	}

	// Test updating rule from node-3
	fmt.Println("\n=== Updating rule from node-3 ===")
	rule1.Metadata["updated_by"] = "node-3"
	rule1.Metadata["update_time"] = time.Now().Format(time.RFC3339)

	if err := engines[2].UpdateRule(rule1); err != nil {
		slog.Error("Failed to update rule from node-3", "error", err)
	} else {
		fmt.Println("✓ Updated rule from node-3")
	}

	// Wait for event propagation
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n=== Verifying update synchronization ===")

	// Check if update is synchronized
	for i, engine := range engines {
		rules, err := engine.ListRules(0, 10)
		if err != nil {
			continue
		}

		for _, rule := range rules {
			if rule.ID == "production_rule_1" {
				fmt.Printf("Node-%d rule metadata: updated_by=%s\n",
					i+1, rule.Metadata["updated_by"])
				break
			}
		}
	}

	fmt.Println("\n=== Testing rule deletion ===")

	// Delete rule from node-2
	if err := engines[1].DeleteRule("production_rule_1"); err != nil {
		slog.Error("Failed to delete rule from node-2", "error", err)
	} else {
		fmt.Println("✓ Deleted rule from node-2")
	}

	// Wait for event propagation
	time.Sleep(200 * time.Millisecond)

	// Verify deletion is synchronized
	fmt.Println("\n=== Verifying deletion synchronization ===")
	for i, engine := range engines {
		rules, err := engine.ListRules(0, 10)
		if err != nil {
			continue
		}

		found := false
		for _, rule := range rules {
			if rule.ID == "production_rule_1" {
				found = true
				break
			}
		}

		if found {
			fmt.Printf("✗ Node-%d still has the deleted rule\n", i+1)
		} else {
			fmt.Printf("✓ Node-%d rule deletion synchronized\n", i+1)
		}
	}

	// Cleanup
	fmt.Println("\n=== Cleanup ===")
	for i, engine := range engines {
		if err := engine.Close(); err != nil {
			slog.Error("Failed to close engine", "index", i+1, "error", err)
		}
	}

	for i, broker := range brokers {
		if err := broker.Close(); err != nil {
			slog.Error("Failed to close broker", "index", i+1, "error", err)
		}
	}

	fmt.Println("Demo completed successfully!")
}

// connectBrokers simulates connecting brokers in a distributed system
func connectBrokers(brokers []matcher.Broker) {
	// This is a simplified simulation - in reality, brokers would be
	// connected through a message queue like Kafka

	// For this demo, we'll create a simple event forwarding mechanism
	// Note: This is just for demonstration - don't use this in production

	for i, broker := range brokers {
		// Cast to in-memory broker to access internal methods
		if memBroker, ok := broker.(*matcher.InMemoryEventBroker); ok {
			// Create a forwarding channel
			forwardChan := make(chan *matcher.Event, 100)

			// Subscribe this broker to receive events
			go func(brokerIndex int, mb *matcher.InMemoryEventBroker) {
				ctx := context.Background()
				mb.Subscribe(ctx, forwardChan)

				// Forward events to all other brokers
				for event := range forwardChan {
					for j, otherBroker := range brokers {
						if j != brokerIndex { // Don't forward to self
							go func(ob matcher.Broker, e *matcher.Event) {
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								defer cancel()
								ob.Publish(ctx, e)
							}(otherBroker, event)
						}
					}
				}
			}(i, memBroker)
		}
	}
}
