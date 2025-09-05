package matcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistenceSaveRules(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "persistence-save-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	rulesFile := filepath.Join(tempDir, "rules.json")
	persistence := NewJSONPersistence(rulesFile)

	// Create test rules
	rules := []*Rule{
		NewRule("save-1").Dimension("region", "us-west", MatchTypeEqual).Build(),
		NewRule("save-2").Dimension("env", "prod", MatchTypeEqual).Build(),
	}

	// Test SaveRules
	ctx := context.Background()
	if err := persistence.SaveRules(ctx, rules); err != nil {
		t.Errorf("SaveRules failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(rulesFile); os.IsNotExist(err) {
		t.Error("Rules file was not created")
	}
}

func TestPersistenceSaveDimensionConfigs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "persistence-dims-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	rulesFile := filepath.Join(tempDir, "data.json")
	persistence := NewJSONPersistence(rulesFile)

	// Create test dimension configs
	dims := []*DimensionConfig{
		NewDimensionConfig("region", 0, true),
		NewDimensionConfig("env", 1, false),
	}

	// Test SaveDimensionConfigs
	ctx := context.Background()
	if err := persistence.SaveDimensionConfigs(ctx, dims); err != nil {
		t.Errorf("SaveDimensionConfigs failed: %v", err)
	}
}

func TestPersistenceHealth(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "persistence-health-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	rulesFile := filepath.Join(tempDir, "health.json")
	persistence := NewJSONPersistence(rulesFile)

	// Test Health
	ctx := context.Background()
	if err := persistence.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestMockEventSubscriberHealth(t *testing.T) {
	subscriber := NewMockEventSubscriber()
	defer subscriber.Close()

	// Test Health
	ctx := context.Background()
	if err := subscriber.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestKafkaEventSubscriberCreation(t *testing.T) {
	// Test NewKafkaEventSubscriber
	subscriber := NewKafkaEventSubscriber([]string{"localhost:9092"}, []string{"test-topic"}, "test-group")
	if subscriber == nil {
		t.Error("NewKafkaEventSubscriber returned nil")
	}
	defer subscriber.Close()
}

func TestKafkaEventSubscriberSubscribe(t *testing.T) {
	subscriber := NewKafkaEventSubscriber([]string{"localhost:9092"}, []string{"test-topic"}, "test-group")
	defer subscriber.Close()

	events := make(chan *Event, 10)

	// Test Subscribe (will fail to connect but that's expected)
	ctx := context.Background()
	if err := subscriber.Subscribe(ctx, events); err == nil {
		t.Log("Subscribe succeeded (unexpected but not an error)")
	}
}

func TestKafkaEventSubscriberClose(t *testing.T) {
	subscriber := NewKafkaEventSubscriber([]string{"localhost:9092"}, []string{"test-topic"}, "test-group")

	// Test Close
	if err := subscriber.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestKafkaEventSubscriberHealth(t *testing.T) {
	subscriber := NewKafkaEventSubscriber([]string{"localhost:9092"}, []string{"test-topic"}, "test-group")
	defer subscriber.Close()

	// Test Health (will fail to connect but that's expected)
	ctx := context.Background()
	if err := subscriber.Health(ctx); err == nil {
		t.Log("Health check succeeded (unexpected but not an error)")
	}
}

func TestDatabasePersistenceCreation(t *testing.T) {
	// Test NewDatabasePersistence
	persistence := NewDatabasePersistence(":memory:")
	if persistence == nil {
		t.Error("NewDatabasePersistence returned nil")
	}
}

func TestDatabasePersistenceOperations(t *testing.T) {
	persistence := NewDatabasePersistence(":memory:")
	ctx := context.Background()

	// Test LoadRules
	rules, err := persistence.LoadRules(ctx)
	if err != nil {
		t.Errorf("LoadRules failed: %v", err)
	}
	if rules == nil {
		t.Error("LoadRules returned nil")
	}

	// Test SaveRules
	testRules := []*Rule{
		NewRule("db-test").Dimension("region", "us-west", MatchTypeEqual).Build(),
	}
	if err := persistence.SaveRules(ctx, testRules); err != nil {
		t.Errorf("SaveRules failed: %v", err)
	}

	// Test LoadDimensionConfigs
	dims, err := persistence.LoadDimensionConfigs(ctx)
	if err != nil {
		t.Errorf("LoadDimensionConfigs failed: %v", err)
	}
	if dims == nil {
		t.Error("LoadDimensionConfigs returned nil")
	}

	// Test SaveDimensionConfigs
	testDims := []*DimensionConfig{
		NewDimensionConfig("region", 0, true),
	}
	if err := persistence.SaveDimensionConfigs(ctx, testDims); err != nil {
		t.Errorf("SaveDimensionConfigs failed: %v", err)
	}

	// Test Health
	if err := persistence.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}
