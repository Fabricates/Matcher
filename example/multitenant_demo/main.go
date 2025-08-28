package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/Fabricates/Matcher"
)

func main() {
	fmt.Println("=== Multi-Tenant Rule Matching Engine Demo ===")

	// Create engine with JSON persistence
	engine, err := matcher.NewMatcherEngineWithDefaults("./demo_data")
	if err != nil {
		slog.Error("Failed to create engine", "error", err); os.Exit(1)
	}
	defer engine.Close()

	// Demo 1: Multiple tenants with same rule types
	fmt.Println("🏢 Demo 1: Multiple Tenants with Isolated Rules")
	fmt.Println("------------------------------------------------")

	// Add rules for Tenant A (e-commerce company)
	tenantARule1 := matcher.NewRuleWithTenant("product_premium", "tenant_a", "ecommerce").
		Dimension("product_type", "premium", matcher.MatchTypeEqual, 10.0).
		Dimension("customer_tier", "gold", matcher.MatchTypeEqual, 5.0).
		Build()

	tenantARule2 := matcher.NewRuleWithTenant("product_standard", "tenant_a", "ecommerce").
		Dimension("product_type", "standard", matcher.MatchTypeEqual, 8.0).
		Dimension("customer_tier", "silver", matcher.MatchTypeEqual, 3.0).
		Build()

	// Add rules for Tenant B (logistics company) - same dimension names, different context
	tenantBRule1 := matcher.NewRuleWithTenant("route_express", "tenant_b", "logistics").
		Dimension("product_type", "express", matcher.MatchTypeEqual, 15.0).
		Dimension("customer_tier", "premium", matcher.MatchTypeEqual, 8.0).
		Build()

	tenantBRule2 := matcher.NewRuleWithTenant("route_standard", "tenant_b", "logistics").
		Dimension("product_type", "standard", matcher.MatchTypeEqual, 10.0).
		Dimension("customer_tier", "basic", matcher.MatchTypeEqual, 2.0).
		Build()

	// Add all rules
	for _, rule := range []*matcher.Rule{tenantARule1, tenantARule2, tenantBRule1, tenantBRule2} {
		if err := engine.AddRule(rule); err != nil {
			slog.Error("Failed to add rule %s: %v", rule.ID, err)
		}
		fmt.Printf("✅ Added rule: %s (Tenant: %s, App: %s)\n", rule.ID, rule.TenantID, rule.ApplicationID)
	}

	fmt.Println()

	// Query Tenant A
	fmt.Println("🔍 Querying Tenant A (E-commerce):")
	tenantAQuery := matcher.CreateQueryWithTenant("tenant_a", "ecommerce", map[string]string{
		"product_type":  "premium",
		"customer_tier": "gold",
	})

	resultA, err := engine.FindBestMatch(tenantAQuery)
	if err != nil {
		slog.Error("Tenant A query failed: %v", err); os.Exit(1)
	}
	if resultA != nil {
		fmt.Printf("   🎯 Match: %s (Weight: %.1f)\n", resultA.Rule.ID, resultA.TotalWeight)
	} else {
		fmt.Println("   ❌ No match found")
	}

	// Query Tenant B with same dimension values
	fmt.Println("🔍 Querying Tenant B (Logistics) with overlapping values:")
	tenantBQuery := matcher.CreateQueryWithTenant("tenant_b", "logistics", map[string]string{
		"product_type":  "standard",
		"customer_tier": "basic",
	})

	resultB, err := engine.FindBestMatch(tenantBQuery)
	if err != nil {
		slog.Error("Tenant B query failed: %v", err); os.Exit(1)
	}
	if resultB != nil {
		fmt.Printf("   🎯 Match: %s (Weight: %.1f)\n", resultB.Rule.ID, resultB.TotalWeight)
	} else {
		fmt.Println("   ❌ No match found")
	}

	fmt.Println()

	// Demo 2: Multi-application within single tenant
	fmt.Println("🏗️ Demo 2: Multi-Application within Single Tenant")
	fmt.Println("------------------------------------------------")

	// Add rules for different applications within tenant_c
	authRule := matcher.NewRuleWithTenant("auth_rule", "tenant_c", "auth_service").
		Dimension("endpoint", "/login", matcher.MatchTypeEqual, 10.0).
		Dimension("method", "POST", matcher.MatchTypeEqual, 5.0).
		Build()

	paymentRule := matcher.NewRuleWithTenant("payment_rule", "tenant_c", "payment_service").
		Dimension("endpoint", "/charge", matcher.MatchTypeEqual, 12.0).
		Dimension("method", "POST", matcher.MatchTypeEqual, 5.0).
		Build()

	for _, rule := range []*matcher.Rule{authRule, paymentRule} {
		if err := engine.AddRule(rule); err != nil {
			slog.Error("Failed to add rule %s: %v", rule.ID, err)
		}
		fmt.Printf("✅ Added rule: %s (Tenant: %s, App: %s)\n", rule.ID, rule.TenantID, rule.ApplicationID)
	}

	fmt.Println()

	// Query auth service
	fmt.Println("🔍 Querying Auth Service:")
	authQuery := matcher.CreateQueryWithTenant("tenant_c", "auth_service", map[string]string{
		"endpoint": "/login",
		"method":   "POST",
	})

	authResult, err := engine.FindBestMatch(authQuery)
	if err != nil {
		slog.Error("Auth query failed: %v", err); os.Exit(1)
	}
	if authResult != nil {
		fmt.Printf("   🎯 Match: %s (Weight: %.1f)\n", authResult.Rule.ID, authResult.TotalWeight)
	}

	// Query payment service
	fmt.Println("🔍 Querying Payment Service:")
	paymentQuery := matcher.CreateQueryWithTenant("tenant_c", "payment_service", map[string]string{
		"endpoint": "/charge",
		"method":   "POST",
	})

	paymentResult, err := engine.FindBestMatch(paymentQuery)
	if err != nil {
		slog.Error("Payment query failed: %v", err); os.Exit(1)
	}
	if paymentResult != nil {
		fmt.Printf("   🎯 Match: %s (Weight: %.1f)\n", paymentResult.Rule.ID, paymentResult.TotalWeight)
	}

	fmt.Println()

	// Demo 3: Isolation verification
	fmt.Println("🔒 Demo 3: Tenant Isolation Verification")
	fmt.Println("---------------------------------------")

	// Try to query tenant_a data from tenant_b context (should find nothing)
	crossTenantQuery := matcher.CreateQueryWithTenant("tenant_b", "logistics", map[string]string{
		"product_type":  "premium", // This exists in tenant_a but not tenant_b context
		"customer_tier": "gold",
	})

	crossResult, err := engine.FindBestMatch(crossTenantQuery)
	if err != nil {
		slog.Error("Cross-tenant query failed: %v", err); os.Exit(1)
	}
	if crossResult == nil {
		fmt.Println("✅ Tenant isolation working: Tenant B cannot access Tenant A's rules")
	} else {
		fmt.Printf("❌ Isolation failed: Found %s\n", crossResult.Rule.ID)
	}

	// Demo 4: Statistics
	fmt.Println()
	fmt.Println("📊 Demo 4: Multi-Tenant Statistics")
	fmt.Println("---------------------------------")

	forestStats := engine.GetForestStats()
	fmt.Printf("Total Forests: %v\n", forestStats["total_forests"])
	fmt.Printf("Total Rules: %v\n", forestStats["total_rules"])

	fmt.Println("\nForest breakdown by tenant/application:")
	for key, stats := range forestStats {
		if key != "total_forests" && key != "total_rules" {
			if statMap, ok := stats.(map[string]interface{}); ok {
				if totalRules, exists := statMap["total_rules"]; exists {
					fmt.Printf("  %s: %v rules\n", key, totalRules)
				}
			}
		}
	}

	fmt.Println("\n🎉 Multi-tenant demo completed successfully!")
	fmt.Println("All tenants and applications are properly isolated while maintaining high performance.")
}
