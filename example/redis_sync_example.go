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
	
	// Create Redis brokers for both nodes
	broker1 := matcher.NewRedisEventBroker(redisAddr, redisPassword, redisDB, streamKey, consumerGroup, node1ID)
	broker2 := matcher.NewRedisEventBroker(redisAddr, redisPassword, redisDB, streamKey, consumerGroup, node2ID)
	
	// Create persistence (using in-memory for demo)
	persistence := &matcher.InMemoryPersistence{}
	
	// Create matcher instances
	matcher1, err := matcher.NewInMemoryMatcher(persistence, broker1, node1ID)
	if err != nil {
		log.Fatalf("Failed to create matcher1: %v", err)
	}
	defer matcher1.Close()
	
	matcher2, err := matcher.NewInMemoryMatcher(persistence, broker2, node2ID)
	if err != nil {
		log.Fatalf("Failed to create matcher2: %v", err)
	}
	defer matcher2.Close()
	
	// Add dimension configuration to node 1
	fmt.Println("=== Adding dimension configuration to Node 1 ===")
	dimensionConfig := &matcher.DimensionConfig{
		Name:     "product",
		Index:    0,
		Required: true,
		Weight:   10.0,
	}
	
	if err := matcher1.AddDimensionConfig(dimensionConfig); err != nil {
		log.Printf("Failed to add dimension config: %v", err)
	}
	
	// Add a rule to node 1
	fmt.Println("=== Adding rule to Node 1 ===")
	rule := matcher.NewRule("distributed-rule-1").
		Dimension("product", "ProductA", matcher.MatchTypeEqual, 10.0).
		Build()
	
	if err := matcher1.AddRule(rule); err != nil {
		log.Printf("Failed to add rule: %v", err)
	}
	
	// Give some time for synchronization
	fmt.Println("=== Waiting for synchronization ===")
	time.Sleep(2 * time.Second)
	
	// Test querying from node 2 (should have received the rule via Redis)
	fmt.Println("=== Querying from Node 2 (should have synchronized rule) ===")
	query := map[string]string{
		"product": "ProductA",
	}
	
	matches, err := matcher2.Match(query)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else {
		fmt.Printf("Node 2 found %d matches:\n", len(matches))
		for _, match := range matches {
			fmt.Printf("  - Rule: %s, Weight: %.2f\n", match.Rule.ID, match.Weight)
		}
	}
	
	// Add another rule to node 2
	fmt.Println("\n=== Adding rule to Node 2 ===")
	rule2 := matcher.NewRule("distributed-rule-2").
		Dimension("product", "ProductB", matcher.MatchTypeEqual, 15.0).
		Build()
	
	if err := matcher2.AddRule(rule2); err != nil {
		log.Printf("Failed to add rule: %v", err)
	}
	
	// Give some time for synchronization
	fmt.Println("=== Waiting for synchronization ===")
	time.Sleep(2 * time.Second)
	
	// Query both products from node 1
	fmt.Println("=== Querying ProductB from Node 1 (should have synchronized rule) ===")
	query2 := map[string]string{
		"product": "ProductB",
	}
	
	matches2, err := matcher1.Match(query2)
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else {
		fmt.Printf("Node 1 found %d matches for ProductB:\n", len(matches2))
		for _, match := range matches2 {
			fmt.Printf("  - Rule: %s, Weight: %.2f\n", match.Rule.ID, match.Weight)
		}
	}
	
	// Show stream information
	fmt.Println("\n=== Redis Stream Information ===")
	if info, err := broker1.GetStreamInfo(context.Background()); err == nil {
		fmt.Printf("Stream: %s\n", info["stream_key"])
		fmt.Printf("Length: %v\n", info["length"])
		fmt.Printf("Consumer Group: %s\n", info["consumer_group"])
	}
	
	fmt.Println("\n=== Demonstration Complete ===")
	fmt.Println("This example shows how Redis streams enable automatic synchronization")
	fmt.Println("of rules and dimensions between distributed matcher instances.")
	fmt.Println("Each change event has a unique ID for idempotence.")
}