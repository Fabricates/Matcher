package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/worthies/matcher"
)

// Example demonstrating Redis-based synchronization between matcher instances
func main() {
	// Redis connection configuration
	redisAddr := "localhost:6379"
	redisPassword := ""
	redisDB := 0
	streamKey := "matcher-events"
	consumerGroup := "matcher-group"
	
	// Create two nodes to simulate distributed system
	node1ID := "node-1"
	node2ID := "node-2"
	
	fmt.Println("=== Redis-based Matcher Synchronization Demo ===")
	fmt.Printf("Stream: %s, Consumer Group: %s\n", streamKey, consumerGroup)
	
	// Create Redis brokers for both nodes
	broker1 := matcher.NewRedisEventBroker(redisAddr, redisPassword, redisDB, streamKey, consumerGroup, node1ID)
	broker2 := matcher.NewRedisEventBroker(redisAddr, redisPassword, redisDB, streamKey, consumerGroup, node2ID)
	
	// Create persistence (using JSON for demo, stored in temp directories)
	persistence1 := matcher.NewJSONPersistence("/tmp/matcher-node1")
	persistence2 := matcher.NewJSONPersistence("/tmp/matcher-node2")
	
	fmt.Println("\n=== Creating Matcher Instances ===")
	
	// Create matcher instances
	matcher1, err := matcher.NewInMemoryMatcher(persistence1, broker1, node1ID)
	if err != nil {
		// If Redis is not available, show what would happen
		if broker1.Health(context.Background()) != nil {
			fmt.Printf("Redis not available at %s - this example requires Redis\n", redisAddr)
			fmt.Println("To run this example:")
			fmt.Println("1. Install Redis: https://redis.io/download")
			fmt.Println("2. Start Redis: redis-server")
			fmt.Println("3. Run this example again")
			return
		}
		log.Fatalf("Failed to create matcher1: %v", err)
	}
	defer func() {
		matcher1.Close()
		broker1.Close()
	}()
	
	matcher2, err := matcher.NewInMemoryMatcher(persistence2, broker2, node2ID)
	if err != nil {
		log.Fatalf("Failed to create matcher2: %v", err)
	}
	defer func() {
		matcher2.Close()
		broker2.Close()
	}()
	
	fmt.Printf("✓ Node 1 (%s) ready\n", node1ID)
	fmt.Printf("✓ Node 2 (%s) ready\n", node2ID)
	
	// Add dimension configuration to node 1
	fmt.Println("\n=== Adding dimension configuration to Node 1 ===")
	dimensionConfig := &matcher.DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	}
	
	if err := matcher1.AddDimension(dimensionConfig); err != nil {
		log.Printf("Failed to add dimension config: %v", err)
	} else {
		fmt.Printf("✓ Added dimension 'product' to Node 1\n")
	}
	
	// Add a rule to node 1
	fmt.Println("\n=== Adding rule to Node 1 ===")
	rule := matcher.NewRule("distributed-rule-1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
		Build()
	
	if err := matcher1.AddRule(rule); err != nil {
		log.Printf("Failed to add rule: %v", err)
	} else {
		fmt.Printf("✓ Added rule 'distributed-rule-1' for ProductA to Node 1\n")
	}
	
	// Give some time for synchronization
	fmt.Println("\n=== Waiting for synchronization (3 seconds) ===")
	time.Sleep(3 * time.Second)
	
	// Test querying from node 2 (should have received the rule via Redis)
	fmt.Println("\n=== Querying ProductA from Node 2 (should have synchronized rule) ===")
	query := map[string]string{
		"product": "ProductA",
	}
	queryRule := matcher.CreateQuery(query)
	
	matches, err := matcher2.FindAllMatches(queryRule)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else {
		fmt.Printf("✓ Node 2 found %d matches for ProductA:\n", len(matches))
		for _, match := range matches {
			fmt.Printf("  - Rule: %s, Weight: %.2f\n", match.Rule.ID, match.TotalWeight)
		}
		if len(matches) == 0 {
			fmt.Println("  (No matches - synchronization may still be in progress)")
		}
	}
	
	// Add another rule to node 2
	fmt.Println("\n=== Adding rule to Node 2 ===")
	rule2 := matcher.NewRule("distributed-rule-2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual, 15.0).
		Build()
	
	if err := matcher2.AddRule(rule2); err != nil {
		log.Printf("Failed to add rule: %v", err)
	} else {
		fmt.Printf("✓ Added rule 'distributed-rule-2' for ProductB to Node 2\n")
	}
	
	// Give some time for synchronization
	fmt.Println("\n=== Waiting for synchronization (3 seconds) ===")
	time.Sleep(3 * time.Second)
	
	// Query both products from node 1
	fmt.Println("\n=== Querying ProductB from Node 1 (should have synchronized rule) ===")
	query2 := map[string]string{
		"product": "ProductB",
	}
	queryRule2 := matcher.CreateQuery(query2)
	
	matches2, err := matcher1.FindAllMatches(queryRule2)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else {
		fmt.Printf("✓ Node 1 found %d matches for ProductB:\n", len(matches2))
		for _, match := range matches2 {
			fmt.Printf("  - Rule: %s, Weight: %.2f\n", match.Rule.ID, match.TotalWeight)
		}
		if len(matches2) == 0 {
			fmt.Println("  (No matches - synchronization may still be in progress)")
		}
	}
	
	// Test cross-queries to show both nodes have both rules
	fmt.Println("\n=== Final Cross-Validation ===")
	
	// Node 1 should see both rules
	fmt.Print("Node 1 rules: ")
	stats1 := matcher1.GetStats()
	fmt.Printf("%d total rules\n", stats1.TotalRules)
	
	// Node 2 should see both rules
	fmt.Print("Node 2 rules: ")
	stats2 := matcher2.GetStats()
	fmt.Printf("%d total rules\n", stats2.TotalRules)
	
	// Show stream information
	fmt.Println("\n=== Redis Stream Information ===")
	if info, err := broker1.GetStreamInfo(context.Background()); err == nil {
		fmt.Printf("Stream: %s\n", info["stream_key"])
		fmt.Printf("Length: %v events\n", info["length"])
		fmt.Printf("Consumer Group: %s\n", info["consumer_group"])
		fmt.Printf("Consumer Name: %s\n", info["consumer_name"])
	} else {
		fmt.Printf("Could not get stream info: %v\n", err)
	}
	
	fmt.Println("\n=== Demonstration Complete ===")
	fmt.Println("Key Features Demonstrated:")
	fmt.Println("✓ Redis streams enable automatic synchronization")
	fmt.Println("✓ Rules and dimensions sync between distributed instances")
	fmt.Println("✓ Each change event has a unique ID for idempotence")
	fmt.Println("✓ Consumer groups provide reliable message delivery")
	fmt.Println("✓ No code changes needed - same EventBrokerInterface")
}