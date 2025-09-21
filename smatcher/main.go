package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	matcher "github.com/Fabricates/Matcher"
)

// DimensionJSON represents the JSON structure for dimensions
type DimensionJSON struct {
	ID       string                 `json:"id"`
	TenantID string                 `json:"tenantId"`
	AppID    string                 `json:"appId"`
	Key      string                 `json:"key"`
	Weights  map[string]interface{} `json:"weights"`
	Status   string                 `json:"status"`
}

// DimensionsResponse represents the response from dimensions JSON
type DimensionsResponse struct {
	TotalCount int             `json:"totalCount"`
	List       []DimensionJSON `json:"list"`
}

// RuleParameter represents a parameter in a rule
type RuleParameter struct {
	ParameterKey   string `json:"parameterKey"`
	ParameterType  int    `json:"parameterType"`
	ParameterValue string `json:"parameterValue"`
}

// RuleJSON represents the JSON structure for rules
type RuleJSON struct {
	ID         string          `json:"id"`
	TenantID   string          `json:"tenantId"`
	AppID      string          `json:"appId"`
	Weight     int             `json:"weight"`
	Parameters []RuleParameter `json:"parameters"`
	Enabled    bool            `json:"enabled"`
	Status     int             `json:"status"`
}

// RulesResponse represents the response from rules JSON
type RulesResponse struct {
	List []RuleJSON `json:"list"`
}

// QueryRequest represents the HTTP request body
type QueryRequest struct {
	Tenant      string            `json:"tenant"`
	Application string            `json:"app"`
	Dimensions  map[string]string `json:"dimensions"`
}

// QueryResponse represents the HTTP response
type QueryResponse []MatchResult

// MatchResult represents a matching rule result
type MatchResult struct {
	RuleID     string           `json:"rule_id"`
	Weight     float64          `json:"weight"`
	Dimensions []DimensionMatch `json:"dimensions"`
}

// DimensionMatch represents a matched dimension
type DimensionMatch struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

// MatcherTool represents the matching tool
type MatcherTool struct {
	engine *matcher.MatcherEngine
}

// NewMatcherTool creates a new MatcherTool
func NewMatcherTool(dataDir string) (*MatcherTool, error) {
	engine, err := matcher.NewMatcherEngineWithDefaults(dataDir)
	if err != nil {
		return nil, err
	}
	return &MatcherTool{engine: engine}, nil
}

// Close closes the matcher tool
func (mt *MatcherTool) Close() {
	mt.engine.Close()
}

// LoadDimensionsFromJSON loads dimensions from JSON content
func (mt *MatcherTool) LoadDimensionsFromJSON(jsonContent []byte) error {
	var resp DimensionsResponse
	if err := json.Unmarshal(jsonContent, &resp); err != nil {
		return err
	}

	for _, dimJSON := range resp.List {
		if dimJSON.Status != "Working" {
			continue
		}

		weights := make(map[matcher.MatchType]float64)
		for k, v := range dimJSON.Weights {
			mt, err := strconv.Atoi(k)
			if err != nil {
				continue
			}
			matchType := mt - 1
			if matchType < 0 {
				matchType = 0
			}
			var weight float64
			switch val := v.(type) {
			case float64:
				weight = val
			case int:
				weight = float64(val)
			case string:
				weight, err = strconv.ParseFloat(val, 64)
				if err != nil {
					continue
				}
			default:
				continue
			}
			weights[matcher.MatchType(matchType)] = weight
		}

		config := matcher.NewDimensionConfigWithWeights(dimJSON.Key, 0, false, weights)
		config.TenantID = dimJSON.TenantID
		config.ApplicationID = dimJSON.AppID

		if err := mt.engine.AddDimension(config); err != nil {
			return fmt.Errorf("failed to add dimension %s: %w", dimJSON.Key, err)
		}
	}

	return nil
}

// LoadRulesFromJSON loads rules from JSON content
func (mt *MatcherTool) LoadRulesFromJSON(jsonContent []byte) error {
	var resp RulesResponse
	if err := json.Unmarshal(jsonContent, &resp); err != nil {
		return err
	}

	var status matcher.RuleStatus
	for _, ruleJSON := range resp.List {
		status = matcher.RuleStatusWorking
		if !ruleJSON.Enabled || (ruleJSON.Status != 3 && ruleJSON.Status != 4 && ruleJSON.Status != 6) {
			status = matcher.RuleStatusDraft
		}

		rule := &matcher.Rule{
			ID:            ruleJSON.ID,
			TenantID:      ruleJSON.TenantID,
			ApplicationID: ruleJSON.AppID,
			Status:        status,
		}

		if ruleJSON.Weight > 0 {
			weight := float64(ruleJSON.Weight)
			rule.ManualWeight = &weight
		}

		for _, param := range ruleJSON.Parameters {
			var matchType matcher.MatchType
			switch param.ParameterType {
			case 0, 1:
				matchType = matcher.MatchTypeEqual
			case 2:
				matchType = matcher.MatchTypeAny
			case 3:
				matchType = matcher.MatchTypePrefix
			case 4:
				matchType = matcher.MatchTypeSuffix
			default:
				continue
			}

			dimValue := &matcher.DimensionValue{
				DimensionName: param.ParameterKey,
				Value:         param.ParameterValue,
				MatchType:     matchType,
			}
			if rule.Dimensions == nil {
				rule.Dimensions = make(map[string]*matcher.DimensionValue)
			}
			rule.Dimensions[param.ParameterKey] = dimValue
		}

		if err := mt.engine.AddRule(rule); err != nil {
			return fmt.Errorf("failed to add rule %s: %w", ruleJSON.ID, err)
		}
	}

	return nil
}

// Match performs matching on the given dimensions
func (mt *MatcherTool) Match(r *QueryRequest) ([]MatchResult, error) {
	// Create query
	query := &matcher.QueryRule{
		TenantID:      r.Tenant,      // Assuming default tenant
		ApplicationID: r.Application, // Assuming default app
		Values:        r.Dimensions,
	}

	// Find all matches
	match, err := mt.engine.FindBestMatch(query)
	if err != nil || match == nil {
		return nil, err
	}

	// Convert to response format
	var responseMatches []MatchResult
	result := MatchResult{
		RuleID: match.Rule.ID,
		Weight: match.TotalWeight,
	}

	for _, dim := range match.Rule.Dimensions {
		dimMatch := DimensionMatch{
			Name:  dim.DimensionName,
			Value: dim.Value,
			Type:  dim.MatchType.String(),
		}
		result.Dimensions = append(result.Dimensions, dimMatch)
	}

	responseMatches = append(responseMatches, result)

	return responseMatches, nil
}

// StartServer starts an HTTP server for matching
func (mt *MatcherTool) StartServer(port string) error {
	http.HandleFunc("/match", mt.handleMatch)
	return http.ListenAndServe(port, nil)
}

func (mt *MatcherTool) handleMatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Find all matches
	matches, err := mt.Match(&req)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := QueryResponse(matches)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: smatcher <keys_file> <rules_file> [port]")
	}

	keysFile := os.Args[1]
	rulesFile := os.Args[2]
	port := ":8080" // default port
	if len(os.Args) > 3 {
		port = os.Args[3]
	}

	mt, err := NewMatcherTool("./data")
	if err != nil {
		log.Fatal("Failed to create matcher tool:", err)
	}
	defer mt.Close()

	// Load dimensions from keys file
	dimData, err := os.ReadFile(keysFile)
	if err != nil {
		log.Fatal("Failed to read dimensions file:", err)
	}
	if err := mt.LoadDimensionsFromJSON(dimData); err != nil {
		log.Fatal("Failed to load dimensions:", err)
	}

	// Load rules from rules file
	rulesData, err := os.ReadFile(rulesFile)
	if err != nil {
		log.Fatal("Failed to read rules file:", err)
	}
	if err := mt.LoadRulesFromJSON(rulesData); err != nil {
		log.Fatal("Failed to load rules:", err)
	}

	// Start HTTP server
	log.Printf("Server starting on %s", port)
	log.Fatal(mt.StartServer(port))
}
