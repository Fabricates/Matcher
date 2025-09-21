package matcher

import (
	"testing"
	"time"
)

func TestRuleClone(t *testing.T) {
	// Create original rule with all fields set
	originalTime := time.Now()
	originalWeight := 42.5
	original := &Rule{
		ID:            "test-rule-123",
		TenantID:      "tenant-1",
		ApplicationID: "app-1",
		Status:        RuleStatusWorking,
		CreatedAt:     originalTime,
		UpdatedAt:     originalTime.Add(time.Hour),
		ManualWeight:  &originalWeight,
		Dimensions: map[string]*DimensionValue{
			"product": {DimensionName: "product", Value: "TestProduct", MatchType: MatchTypeEqual},
			"route":   {DimensionName: "route", Value: "TestRoute", MatchType: MatchTypePrefix},
		},
		Metadata: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	// Clone the rule
	cloned := original.Clone()

	// Verify clone is not nil
	if cloned == nil {
		t.Fatal("Clone() returned nil")
	}

	// Verify all basic fields are copied correctly
	if cloned.ID != original.ID {
		t.Errorf("ID mismatch: expected %s, got %s", original.ID, cloned.ID)
	}
	if cloned.TenantID != original.TenantID {
		t.Errorf("TenantID mismatch: expected %s, got %s", original.TenantID, cloned.TenantID)
	}
	if cloned.ApplicationID != original.ApplicationID {
		t.Errorf("ApplicationID mismatch: expected %s, got %s", original.ApplicationID, cloned.ApplicationID)
	}
	if cloned.Status != original.Status {
		t.Errorf("Status mismatch: expected %v, got %v", original.Status, cloned.Status)
	}
	if !cloned.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt mismatch: expected %v, got %v", original.CreatedAt, cloned.CreatedAt)
	}
	if !cloned.UpdatedAt.Equal(original.UpdatedAt) {
		t.Errorf("UpdatedAt mismatch: expected %v, got %v", original.UpdatedAt, cloned.UpdatedAt)
	}

	// Verify ManualWeight is deep copied
	if cloned.ManualWeight == nil {
		t.Error("ManualWeight should not be nil")
	} else {
		if *cloned.ManualWeight != *original.ManualWeight {
			t.Errorf("ManualWeight value mismatch: expected %f, got %f", *original.ManualWeight, *cloned.ManualWeight)
		}
		// Verify it's a different pointer (deep copy)
		if cloned.ManualWeight == original.ManualWeight {
			t.Error("ManualWeight should be a different pointer (deep copy)")
		}
	}

	// Verify Dimensions are deep copied
	if len(cloned.Dimensions) != len(original.Dimensions) {
		t.Errorf("Dimensions length mismatch: expected %d, got %d", len(original.Dimensions), len(cloned.Dimensions))
	}

	for key, originalDim := range original.Dimensions {
		clonedDim := cloned.GetDimensionValue(key)

		if clonedDim == nil {
			t.Errorf("Cloned dimension for key %s is missing", key)
			continue
		}

		if clonedDim == originalDim {
			t.Errorf("Dimension %s should be a different pointer (deep copy)", key)
		}

		if clonedDim.DimensionName != originalDim.DimensionName {
			t.Errorf("Dimension %s DimensionName mismatch: expected %s, got %s", key, originalDim.DimensionName, clonedDim.DimensionName)
		}
		if clonedDim.Value != originalDim.Value {
			t.Errorf("Dimension %s Value mismatch: expected %s, got %s", key, originalDim.Value, clonedDim.Value)
		}
		if clonedDim.MatchType != originalDim.MatchType {
			t.Errorf("Dimension %s MatchType mismatch: expected %v, got %v", key, originalDim.MatchType, clonedDim.MatchType)
		}
	}

	// Verify Metadata is deep copied
	if len(cloned.Metadata) != len(original.Metadata) {
		t.Errorf("Metadata length mismatch: expected %d, got %d", len(original.Metadata), len(cloned.Metadata))
	}

	for key, originalValue := range original.Metadata {
		clonedValue, exists := cloned.Metadata[key]
		if !exists {
			t.Errorf("Metadata key %s missing in clone", key)
		}
		if clonedValue != originalValue {
			t.Errorf("Metadata value mismatch for key %s: expected %s, got %s", key, originalValue, clonedValue)
		}
	}

	// Verify it's a different map pointer (deep copy)
	if &cloned.Metadata == &original.Metadata {
		t.Error("Metadata should be a different map (deep copy)")
	}

	// Test independence: modify clone and verify original is unchanged
	cloned.ID = "modified-id"
	cloned.TenantID = "modified-tenant"
	*cloned.ManualWeight = 999.9
	// Modify a cloned dimension by key
	// original had 'product' as first dimension in the test setup
	if clonedDim := cloned.GetDimensionValue("product"); clonedDim != nil {
		clonedDim.Value = "ModifiedValue"
	}
	cloned.Metadata["key1"] = "modified-value"
	cloned.Metadata["new-key"] = "new-value"

	// Verify original is unchanged
	if original.ID == "modified-id" {
		t.Error("Original ID should not be modified")
	}
	if original.TenantID == "modified-tenant" {
		t.Error("Original TenantID should not be modified")
	}
	if *original.ManualWeight == 999.9 {
		t.Error("Original ManualWeight should not be modified")
	}
	if origDim := original.GetDimensionValue("product"); origDim != nil {
		if origDim.Value == "ModifiedValue" {
			t.Error("Original Dimension value should not be modified")
		}
	}
	if original.Metadata["key1"] == "modified-value" {
		t.Error("Original Metadata should not be modified")
	}
	if _, exists := original.Metadata["new-key"]; exists {
		t.Error("Original Metadata should not have new key")
	}
}

func TestRuleCloneNil(t *testing.T) {
	var rule *Rule = nil
	cloned := rule.Clone()

	if cloned != nil {
		t.Error("Clone of nil rule should return nil")
	}
}

func TestRuleCloneWithNilFields(t *testing.T) {
	// Test with minimal rule (nil optional fields)
	rule := &Rule{
		ID:     "minimal-rule",
		Status: RuleStatusDraft,
		// All other fields are nil/zero values
	}

	cloned := rule.Clone()

	if cloned == nil {
		t.Fatal("Clone() should not return nil for valid rule")
	}

	if cloned.ID != rule.ID {
		t.Errorf("ID mismatch: expected %s, got %s", rule.ID, cloned.ID)
	}

	if cloned.ManualWeight != nil {
		t.Error("ManualWeight should be nil when original is nil")
	}

	if cloned.Dimensions != nil {
		t.Error("Dimensions should be nil when original is nil")
	}

	if cloned.Metadata != nil {
		t.Error("Metadata should be nil when original is nil")
	}
}
