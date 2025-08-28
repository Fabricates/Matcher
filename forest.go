package matcher

import (
	"fmt"
	"sort"
	"sync"
)

// SharedNode represents a node in the forest where rules share paths
// Each node can have multiple branches based on different match types
type SharedNode struct {
	Level         int                        `json:"level"`          // Which dimension level (0-based)
	DimensionName string                     `json:"dimension_name"` // Which dimension this level represents
	Value         string                     `json:"value"`          // The value for this dimension
	Rules         []*Rule                    `json:"rules"`          // All rules that terminate at this node
	Branches      map[MatchType]*MatchBranch `json:"branches"`       // Branches organized by match type
	mu            sync.RWMutex
}

// MatchBranch represents a branch for a specific match type
type MatchBranch struct {
	MatchType MatchType              `json:"match_type"` // The match type for this branch
	Rules     []*Rule                `json:"rules"`      // Rules that use this match type at this level
	Children  map[string]*SharedNode `json:"children"`   // Child nodes (key = dimension value)
}

// RuleForest represents the forest structure with shared nodes
type RuleForest struct {
	TenantID         string                      `json:"tenant_id,omitempty"`      // Tenant identifier for this forest
	ApplicationID    string                      `json:"application_id,omitempty"` // Application identifier for this forest
	Trees            map[MatchType][]*SharedNode `json:"trees"`                    // Trees organized by first dimension match type
	DimensionOrder   []string                    `json:"dimension_order"`          // Order of dimensions for tree traversal
	RuleIndex        map[string][]*SharedNode    `json:"rule_index"`               // Index of rules to their nodes for quick removal
	DimensionConfigs map[string]*DimensionConfig `json:"-"`                        // Reference to dimension configurations (not serialized)
	mu               sync.RWMutex
}

// CreateSharedNode creates a shared node
func CreateSharedNode(level int, dimensionName, value string) *SharedNode {
	return &SharedNode{
		Level:         level,
		DimensionName: dimensionName,
		Value:         value,
		Rules:         []*Rule{},
		Branches:      make(map[MatchType]*MatchBranch),
	}
}

// AddRule adds a rule to this node for a specific match type (only for leaf nodes)
func (sn *SharedNode) AddRule(rule *Rule, matchType MatchType) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	// Add to general rules list for backward compatibility
	sn.Rules = append(sn.Rules, rule)

	// Also add to the specific match type branch for organizational purposes
	if branch, exists := sn.Branches[matchType]; exists {
		branch.Rules = append(branch.Rules, rule)
	} else {
		sn.Branches[matchType] = &MatchBranch{
			MatchType: matchType,
			Rules:     []*Rule{rule},
			Children:  make(map[string]*SharedNode),
		}
	}
}

// RemoveRule removes a rule from this node
func (sn *SharedNode) RemoveRule(ruleID string) bool {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	// Remove from general rules list
	for i, rule := range sn.Rules {
		if rule.ID == ruleID {
			sn.Rules = append(sn.Rules[:i], sn.Rules[i+1:]...)
			break
		}
	}

	// Remove from all match type branches
	for matchType, branch := range sn.Branches {
		for i, rule := range branch.Rules {
			if rule.ID == ruleID {
				branch.Rules = append(branch.Rules[:i], branch.Rules[i+1:]...)
				if len(branch.Rules) == 0 {
					delete(sn.Branches, matchType)
				}
				return true
			}
		}
	}

	return false
}

// CreateRuleForest creates a rule forest with the structure
func CreateRuleForest(dimensionConfigs map[string]*DimensionConfig) *RuleForest {
	return &RuleForest{
		Trees:            make(map[MatchType][]*SharedNode),
		DimensionOrder:   []string{},
		RuleIndex:        make(map[string][]*SharedNode),
		DimensionConfigs: dimensionConfigs,
	}
}

// CreateRuleForestWithTenant creates a rule forest for a specific tenant and application
func CreateRuleForestWithTenant(tenantID, applicationID string, dimensionConfigs map[string]*DimensionConfig) *RuleForest {
	return &RuleForest{
		TenantID:         tenantID,
		ApplicationID:    applicationID,
		Trees:            make(map[MatchType][]*SharedNode),
		DimensionOrder:   []string{},
		RuleIndex:        make(map[string][]*SharedNode),
		DimensionConfigs: dimensionConfigs,
	}
}

// CreateForestIndexCompat creates a forest index compatible with the old interface
func CreateForestIndexCompat() *RuleForest {
	return CreateRuleForest(make(map[string]*DimensionConfig))
}

// GetDefaultDimensionOrder returns the default dimension order from types.go
func (rf *RuleForest) GetDefaultDimensionOrder() []string {
	if len(rf.DimensionOrder) == 0 {
		// Return some default order - this should be customizable
		return []string{"user_id", "event_type", "platform"}
	}
	return rf.DimensionOrder
}

// GetDimensionOrder returns the current dimension order
func (rf *RuleForest) GetDimensionOrder() []string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	result := make([]string, len(rf.DimensionOrder))
	copy(result, rf.DimensionOrder)
	return result
}

// SetDimensionOrder sets the dimension order for the forest
func (rf *RuleForest) SetDimensionOrder(order []string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DimensionOrder = make([]string, len(order))
	copy(rf.DimensionOrder, order)
}

// AddRule adds a rule to the forest following the dimension order
func (rf *RuleForest) AddRule(rule *Rule) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rule.Dimensions) == 0 {
		return
	}

	// Validate that rule has dimensions and auto-expand dimension order
	if len(rf.DimensionOrder) == 0 || len(rule.Dimensions) != len(rf.DimensionOrder) {
		// Auto-expand dimension order if dimensions are encountered
		rf.ensureDimensionsInOrder(rule.Dimensions)
	}

	// Sort rule dimensions according to our dimension order
	sortedDims := rf.sortDimensionsByOrder(rule.Dimensions)

	if len(sortedDims) == 0 {
		return
	}

	// Get the first dimension to determine which tree to use
	firstDim := sortedDims[0]

	// Find or create the root node for this first dimension value and match type
	var rootNode *SharedNode
	rootNodes := rf.Trees[firstDim.MatchType]

	// Look for existing root node with the same dimension name and value
	for _, node := range rootNodes {
		if node.DimensionName == firstDim.DimensionName && node.Value == firstDim.Value {
			rootNode = node
			break
		}
	}

	// Create root node if not found
	if rootNode == nil {
		rootNode = CreateSharedNode(0, firstDim.DimensionName, firstDim.Value)
		rf.Trees[firstDim.MatchType] = append(rf.Trees[firstDim.MatchType], rootNode)
	}

	var ruleNodes []*SharedNode
	ruleNodes = append(ruleNodes, rootNode)

	// Traverse/create path for remaining dimensions
	current := rootNode
	for i := 1; i < len(sortedDims); i++ {
		dim := sortedDims[i]

		// Use the original match type from the rule definition - do NOT change it
		matchType := dim.MatchType

		// Get or create the match branch for the CURRENT dimension's match type
		branch, exists := current.Branches[matchType]
		if !exists {
			branch = &MatchBranch{
				MatchType: matchType,
				Rules:     []*Rule{},
				Children:  make(map[string]*SharedNode),
			}
			current.Branches[matchType] = branch
		}

		// Get or create child node for this dimension value within the match branch
		child, exists := branch.Children[dim.Value]
		if !exists {
			child = CreateSharedNode(i, dim.DimensionName, dim.Value)
			branch.Children[dim.Value] = child
		}

		ruleNodes = append(ruleNodes, child)
		current = child
	}

	// Add rule to the final node (the node for the last dimension the rule specifies)
	// Use the original match type from the last dimension
	finalMatchType := sortedDims[len(sortedDims)-1].MatchType
	current.AddRule(rule, finalMatchType)

	// Index the rule for quick removal
	rf.RuleIndex[rule.ID] = ruleNodes
}

// ensureDimensionsInOrder ensures all rule dimensions are in the dimension order
// Auto-expands the dimension order to include dimensions from rules
func (rf *RuleForest) ensureDimensionsInOrder(dimensions []*DimensionValue) {
	// If we don't have a dimension order yet, establish it from the first rule
	if len(rf.DimensionOrder) == 0 {
		for _, dim := range dimensions {
			rf.DimensionOrder = append(rf.DimensionOrder, dim.DimensionName)
		}
		return
	}

	// Add any dimensions to the order
	for _, dim := range dimensions {
		found := false
		for _, existingDim := range rf.DimensionOrder {
			if existingDim == dim.DimensionName {
				found = true
				break
			}
		}
		if !found {
			rf.DimensionOrder = append(rf.DimensionOrder, dim.DimensionName)
		}
	}
}

// sortDimensionsByOrder sorts dimensions according to the forest's dimension order
func (rf *RuleForest) sortDimensionsByOrder(dimensions []*DimensionValue) []*DimensionValue {
	// Create a map for quick lookup of dimension order
	orderMap := make(map[string]int)
	for i, dimName := range rf.DimensionOrder {
		orderMap[dimName] = i
	}

	// Create a copy to sort
	sorted := make([]*DimensionValue, len(dimensions))
	copy(sorted, dimensions)

	// Sort according to dimension order
	sort.Slice(sorted, func(i, j int) bool {
		orderI, existsI := orderMap[sorted[i].DimensionName]
		orderJ, existsJ := orderMap[sorted[j].DimensionName]

		// If both dimensions exist in order, use the order
		if existsI && existsJ {
			return orderI < orderJ
		}

		// If only one exists in order, prioritize it
		if existsI {
			return true
		}
		if existsJ {
			return false
		}

		// If neither exists in order, sort alphabetically
		return sorted[i].DimensionName < sorted[j].DimensionName
	})

	return sorted
}

// findCandidateRulesWithQueryRule is the actual implementation for QueryRule
func (rf *RuleForest) findCandidateRulesWithQueryRule(query *QueryRule) []RuleWithWeight {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	candidateRules := make([]RuleWithWeight, 0)

	if len(rf.DimensionOrder) == 0 {
		return candidateRules
	}

	// Process each tree type to find matching rules
	for matchType, trees := range rf.Trees {
		for _, tree := range trees {
			rf.searchTree(tree, query, 0, matchType, &candidateRules)
		}
	}

	return candidateRules
}

// FindCandidateRules finds rules that could match the query (map interface for compatibility)
func (rf *RuleForest) FindCandidateRules(queryValues interface{}) []RuleWithWeight {
	switch v := queryValues.(type) {
	case *QueryRule:
		return rf.findCandidateRulesWithQueryRule(v)
	case map[string]interface{}:
		// Convert map to QueryRule
		query := &QueryRule{Values: make(map[string]string)}
		for k, val := range v {
			if val != nil {
				query.Values[k] = fmt.Sprintf("%v", val)
			}
		}
		return rf.findCandidateRulesWithQueryRule(query)
	case map[string]string:
		// Direct conversion
		query := &QueryRule{Values: v}
		return rf.findCandidateRulesWithQueryRule(query)
	default:
		return []RuleWithWeight{}
	}
}

// searchTree searches a single tree for matching rules (optimized with slice and status filtering)
func (rf *RuleForest) searchTree(node *SharedNode, query *QueryRule, depth int, rootMatchType MatchType, candidateRules *[]RuleWithWeight) {
	if node == nil {
		return
	}

	// Determine current dimension name if within our dimension order
	var currentDimName string
	var hasQueryValue bool
	var queryValue string

	if depth < len(rf.DimensionOrder) {
		currentDimName = rf.DimensionOrder[depth]
		queryValue, hasQueryValue = query.Values[currentDimName]
	} else {
		// Beyond our dimension order, but we should still traverse nodes
		hasQueryValue = false
	}

	// Check if this node matches at the current depth
	nodeMatches := false
	if depth == 0 {
		// For root nodes, match against the tree's match type
		if hasQueryValue {
			nodeMatches = rf.matchesValue(queryValue, node.Value, rootMatchType)
		} else {
			// For partial queries, accept all root nodes and let deeper levels filter
			nodeMatches = true
		}
	} else {
		// For deeper nodes, match based on the current branch's match type being searched
		if hasQueryValue {
			// Instead of exact matching, we should use the match type we're traversing through
			// This is passed as the rootMatchType parameter (which gets updated as we traverse)
			nodeMatches = rf.matchesValue(queryValue, node.Value, rootMatchType)
		} else {
			// For partial queries, accept all nodes at unspecified dimension levels
			nodeMatches = true
		}
	}

	if !nodeMatches {
		return
	}

	// Collect rules from this node (since rules can terminate at any depth)
	node.mu.RLock()
	for _, rule := range node.Rules {
		// Filter by rule status unless IncludeAllRules is true (only collect 'working' rules)
		// Consider empty status as working (for backward compatibility)
		if !query.IncludeAllRules && rule.Status != RuleStatusWorking && rule.Status != "" {
			continue
		}

		// For partial queries, check if all specified query dimensions match the rule
		if rf.ruleMatchesPartialQuery(rule, query) {
			// Insert rule in weight-ordered position (highest weight at front)
			rf.insertRuleByWeight(candidateRules, rule)
		}
	}
	node.mu.RUnlock()

	// Continue searching deeper through branches (don't stop at dimension order limit)
	node.mu.RLock()
	if hasQueryValue {
		// If query specifies this dimension, search all branches
		for branchMatchType, branch := range node.Branches {
			for _, child := range branch.Children {
				rf.searchTree(child, query, depth+1, branchMatchType, candidateRules)
			}
		}
	} else {
		// If query doesn't specify this dimension, only search MatchTypeAny branches
		if branch, exists := node.Branches[MatchTypeAny]; exists {
			for _, child := range branch.Children {
				rf.searchTree(child, query, depth+1, MatchTypeAny, candidateRules)
			}
		}
	}
	node.mu.RUnlock()
}

// insertRuleByWeight inserts a rule into the candidate slice maintaining weight order (highest first)
func (rf *RuleForest) insertRuleByWeight(candidateRules *[]RuleWithWeight, newRule *Rule) {
	newWeight := newRule.CalculateTotalWeight(rf.DimensionConfigs)

	// If slice is empty or new rule has highest weight, insert at front
	if len(*candidateRules) == 0 {
		*candidateRules = append(*candidateRules, RuleWithWeight{newRule, newWeight})
		return
	}

	if newWeight > (*candidateRules)[0].Weight {
		// Insert at front
		*candidateRules = append([]RuleWithWeight{{newRule, newWeight}}, *candidateRules...)
		return
	}

	// Find the right position to maintain weight order
	insertPos := len(*candidateRules)
	for i, rule := range *candidateRules {
		if newWeight > rule.CalculateTotalWeight(rf.DimensionConfigs) {
			insertPos = i
			break
		}
	}

	// Insert at the found position
	if insertPos == len(*candidateRules) {
		// Append at end
		*candidateRules = append(*candidateRules, RuleWithWeight{newRule, newWeight})
	} else {
		// Insert in middle
		*candidateRules = append(*candidateRules, RuleWithWeight{})
		copy((*candidateRules)[insertPos+1:], (*candidateRules)[insertPos:])
		(*candidateRules)[insertPos] = RuleWithWeight{newRule, newWeight}
	}
}

// ruleMatchesPartialQuery checks if a rule matches all dimensions specified in a partial query
func (rf *RuleForest) ruleMatchesPartialQuery(rule *Rule, query *QueryRule) bool {
	// Check each dimension specified in the query
	for queryDimName, queryValue := range query.Values {
		// Find the corresponding dimension in the rule
		var ruleDim *DimensionValue
		for _, dim := range rule.Dimensions {
			if dim.DimensionName == queryDimName {
				ruleDim = dim
				break
			}
		}

		// If rule doesn't have this dimension, it doesn't match
		if ruleDim == nil {
			return false
		}

		// Check if the rule dimension matches the query value
		if !rf.matchesValue(queryValue, ruleDim.Value, ruleDim.MatchType) {
			return false
		}
	}

	return true
}

// matchesValue checks if a query value matches a rule value with the given match type
func (rf *RuleForest) matchesValue(queryValue, ruleValue string, matchType MatchType) bool {
	switch matchType {
	case MatchTypeEqual:
		return queryValue == ruleValue
	case MatchTypePrefix:
		return len(queryValue) >= len(ruleValue) && queryValue[:len(ruleValue)] == ruleValue
	case MatchTypeSuffix:
		return len(queryValue) >= len(ruleValue) && queryValue[len(queryValue)-len(ruleValue):] == ruleValue
	case MatchTypeAny:
		return true // MatchTypeAny with empty value matches everything
	default:
		return queryValue == ruleValue
	}
}

// RemoveRule removes a rule from the forest
func (rf *RuleForest) RemoveRule(rule *Rule) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Get nodes where this rule is stored
	nodes, exists := rf.RuleIndex[rule.ID]
	if !exists {
		return
	}

	// Remove rule from all nodes
	for _, node := range nodes {
		node.RemoveRule(rule.ID)
	}

	// Clean up empty nodes (traverse upward)
	rf.cleanupEmptyNodes()

	// Remove from index
	delete(rf.RuleIndex, rule.ID)
}

// cleanupEmptyNodes removes empty nodes from the forest
func (rf *RuleForest) cleanupEmptyNodes() {
	// This is a simplified cleanup - in practice, you might want more sophisticated cleanup
	for matchType, trees := range rf.Trees {
		var cleanTrees []*SharedNode
		for _, tree := range trees {
			if rf.hasRulesOrChildren(tree) {
				cleanTrees = append(cleanTrees, tree)
			}
		}
		rf.Trees[matchType] = cleanTrees
	}
}

// hasRulesOrChildren checks if a node has rules or non-empty children
func (rf *RuleForest) hasRulesOrChildren(node *SharedNode) bool {
	node.mu.RLock()
	defer node.mu.RUnlock()

	if len(node.Rules) > 0 {
		return true
	}

	for _, branch := range node.Branches {
		if len(branch.Rules) > 0 || len(branch.Children) > 0 {
			return true
		}
	}

	return false
}

// GetStats returns statistics about the forest
func (rf *RuleForest) GetStats() map[string]interface{} {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	stats := make(map[string]interface{})

	totalTrees := 0
	for _, trees := range rf.Trees {
		totalTrees += len(trees)
	}

	levelCounts := make(map[int]int)
	totalNodes, sharedNodes, maxRules, totalRules := 0, 0, 0, 0

	// Count nodes across all trees
	for _, trees := range rf.Trees {
		for _, tree := range trees {
			count, shared, max, ruleCount := rf.countNodesAndSharing(tree, levelCounts)
			totalNodes += count
			sharedNodes += shared
			totalRules += ruleCount
			if max > maxRules {
				maxRules = max
			}
		}
	}

	stats["total_trees"] = totalTrees
	stats["total_nodes"] = totalNodes
	stats["shared_nodes"] = sharedNodes
	stats["max_rules_per_node"] = maxRules
	stats["total_rules"] = totalRules
	stats["levels"] = levelCounts
	stats["dimension_order"] = rf.DimensionOrder
	stats["total_root_nodes"] = totalTrees // Same as total trees since each tree has one root

	return stats
}

// countNodesAndSharing recursively counts nodes and identifies sharing
func (rf *RuleForest) countNodesAndSharing(node *SharedNode, levelCounts map[int]int) (int, int, int, int) {
	if node == nil {
		return 0, 0, 0, 0
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	count := 1
	sharedNodes := 0
	maxRules := len(node.Rules)
	totalRules := len(node.Rules)

	// A node is "shared" if it has more than one rule
	if len(node.Rules) > 1 {
		sharedNodes = 1
	}

	level := node.Level
	if level >= 0 { // Don't count root node (level -1)
		levelCounts[level]++
	}

	for _, branch := range node.Branches {
		for _, child := range branch.Children {
			childCount, childShared, childMaxRules, childTotalRules := rf.countNodesAndSharing(child, levelCounts)
			count += childCount
			sharedNodes += childShared
			totalRules += childTotalRules
			if childMaxRules > maxRules {
				maxRules = childMaxRules
			}
		}
	}

	return count, sharedNodes, maxRules, totalRules
}

// InitializeDimension is a compatibility method (no-op in the implementation)
func (rf *RuleForest) InitializeDimension(dimensionName string) {
	// No longer needed in the forest structure
	// Dimensions are automatically handled when rules are added
}

// ForestIndex provides backward compatibility by embedding RuleForest
type ForestIndex struct {
	*RuleForest
}

// CreateForestIndex creates a forest index using the implementation
func CreateForestIndex() *ForestIndex {
	return &ForestIndex{
		RuleForest: CreateForestIndexCompat(),
	}
}
