package matcher

import (
	"fmt"
	"strings"
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
	TenantID          string                       `json:"tenant_id,omitempty"`      // Tenant identifier for this forest
	ApplicationID     string                       `json:"application_id,omitempty"` // Application identifier for this forest
	Trees             map[MatchType][]*SharedNode  `json:"trees"`                    // Trees organized by first dimension match type
	EqualTreesIndex   map[string]*SharedNode       `json:"-"`                        // Hash map index for O(1) lookup of equal match trees by first dimension value
	Dimensions        *DimensionConfigs            `json:"dimension_order"`          // Order of dimensions for tree traversal
	RuleIndex         map[string][]*SharedNode     `json:"rule_index"`               // Index of rules to their nodes for quick removal
	NodeRelationships map[string]map[string]string `json:"-"`                        // Efficient relationship map for fast dumping: current_node -> rule_id -> next_node
	mu                sync.RWMutex
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
func CreateRuleForest(dimensionConfigs *DimensionConfigs) *RuleForest {
	forest := &RuleForest{
		Trees:             make(map[MatchType][]*SharedNode),
		EqualTreesIndex:   make(map[string]*SharedNode),
		Dimensions:        dimensionConfigs,
		RuleIndex:         make(map[string][]*SharedNode),
		NodeRelationships: make(map[string]map[string]string),
	}
	return forest
}

// CreateRuleForestWithTenant creates a rule forest for a specific tenant and application
func CreateRuleForestWithTenant(tenantID, applicationID string, dimensionConfigs *DimensionConfigs) *RuleForest {
	forest := &RuleForest{
		TenantID:          tenantID,
		ApplicationID:     applicationID,
		Trees:             make(map[MatchType][]*SharedNode),
		EqualTreesIndex:   make(map[string]*SharedNode),
		Dimensions:        dimensionConfigs,
		RuleIndex:         make(map[string][]*SharedNode),
		NodeRelationships: make(map[string]map[string]string),
	}
	return forest
}

// CreateForestIndexCompat creates a forest index compatible with the old interface
func CreateForestIndexCompat() *RuleForest {
	return CreateRuleForest(NewDimensionConfigs())
}

// generateNodeName generates a unique node name in the pattern 'dimension|value+match_type'
func generateNodeName(dimensionName, value string, matchType MatchType) string {
	var matchTypeStr string
	switch matchType {
	case MatchTypeEqual:
		matchTypeStr = ""
	case MatchTypeAny:
		matchTypeStr = "*"
	case MatchTypePrefix:
		matchTypeStr = "*"
		value = value + "*"
	case MatchTypeSuffix:
		matchTypeStr = "*"
		value = "*" + value
	}
	return fmt.Sprintf("%s|%s%s", dimensionName, value, matchTypeStr)
}

// cleanupNodeRelationshipsForRule removes relationships for a specific rule from a specific node's relationships.
// It generates the possible node name variants (equal/any/prefix/suffix) and removes the mapping
// rf.NodeRelationships[nodeName][ruleID] if present. This keeps cleanup targeted and O(1) per node.
func (rf *RuleForest) cleanupNodeRelationshipsForRule(node *SharedNode, rule *Rule) {
	// Generate possible node names for this node (different match types)
	nodeName := generateNodeName(node.DimensionName, node.Value, rule.GetDimensionMatchType(node.DimensionName))

	// Remove outgoing relationship entries keyed by these node names
	if ruleMap, exists := rf.NodeRelationships[nodeName]; exists {
		if _, ok := ruleMap[rule.ID]; ok {
			delete(ruleMap, rule.ID)
			if len(ruleMap) == 0 {
				delete(rf.NodeRelationships, nodeName)
			}
		}
	}
}

// AddRule adds a rule to the forest following the dimension order
func (rf *RuleForest) AddRule(rule *Rule) (*Rule, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rule.Dimensions) == 0 {
		return nil, fmt.Errorf("no rule dimension found")
	}
	if rf.Dimensions.Count() <= 0 {
		return nil, fmt.Errorf("no dimension configured yet")
	}

	var sorted = rf.Dimensions.GetSortedNames()

	// Auto-fill missing dimensions with MatchTypeAny if dimension order is established
	completeRule := rule.CloneAndComplete(sorted)

	var firstDim *DimensionValue = completeRule.GetDimensionValue(sorted[0])

	// Find or create the root node for this first dimension value and match type
	var rootNode *SharedNode
	rootNodes := rf.Trees[firstDim.MatchType]

	// Look for existing root node with the same dimension name and value
	if firstDim.MatchType == MatchTypeEqual {
		indexKey := firstDim.DimensionName + ":" + firstDim.Value
		if mn, ok := rf.EqualTreesIndex[indexKey]; ok {
			rootNode = mn
		}
	} else {
		for _, node := range rootNodes {
			if node.DimensionName == firstDim.DimensionName && node.Value == firstDim.Value {
				rootNode = node
				break
			}
		}
	}

	// Create root node if not found
	if rootNode == nil {
		rootNode = CreateSharedNode(0, firstDim.DimensionName, firstDim.Value)
		// OPTIMIZATION: For equal match types, also add to the hash index for O(1) lookup
		if firstDim.MatchType == MatchTypeEqual {
			// Create a composite key: dimensionName:value for uniqueness across different dimensions
			indexKey := firstDim.DimensionName + ":" + firstDim.Value
			rf.EqualTreesIndex[indexKey] = rootNode
			if len(rf.Trees[firstDim.MatchType]) <= 0 {
				// Append an empty node as a placeholder to maintain index alignment for MatchTypeEqual.
				// This is required for legacy compatibility: some matching logic expects at least one node in the slice.
				rf.Trees[firstDim.MatchType] = append(rf.Trees[firstDim.MatchType], &SharedNode{})
			}
		} else {
			rf.Trees[firstDim.MatchType] = append(rf.Trees[firstDim.MatchType], rootNode)
		}
	}

	var ruleNodes []*SharedNode
	ruleNodes = append(ruleNodes, rootNode)

	// Track parent node for relationship building
	var parentNodeName string

	// Traverse/create path for remaining dimensions
	current := rootNode
	for i := 1; i < len(sorted); i++ {
		dim := completeRule.GetDimensionValue(sorted[i])

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

			// MAINTAIN RELATIONSHIPS: Track parent-child relationship for efficient dumping
			parentNodeName = generateNodeName(current.DimensionName, current.Value, dim.MatchType)
			childNodeName := generateNodeName(child.DimensionName, child.Value, matchType)

			// MAINTAIN NODE RELATIONSHIPS: Track rule transitions for efficient dumping
			if rf.NodeRelationships[parentNodeName] == nil {
				rf.NodeRelationships[parentNodeName] = make(map[string]string)
			}
			rf.NodeRelationships[parentNodeName][completeRule.ID] = childNodeName
		} else {
			// Even if child exists, still record the rule transition
			parentNodeName = generateNodeName(current.DimensionName, current.Value, dim.MatchType)
			childNodeName := generateNodeName(child.DimensionName, child.Value, matchType)

			if rf.NodeRelationships[parentNodeName] == nil {
				rf.NodeRelationships[parentNodeName] = make(map[string]string)
			}
			rf.NodeRelationships[parentNodeName][completeRule.ID] = childNodeName
		}

		ruleNodes = append(ruleNodes, child)
		current = child
	}

	// Add rule to the final node (the node for the last dimension the rule specifies)
	// Use the original match type from the last dimension
	var finalMatchType MatchType = MatchTypeAny // default
	if len(sorted) > 0 {
		lastDim := completeRule.GetDimensionValue(sorted[len(sorted)-1])
		if lastDim != nil {
			finalMatchType = lastDim.MatchType
		}
	}
	current.AddRule(completeRule, finalMatchType)

	// Index the rule for quick removal
	rf.RuleIndex[completeRule.ID] = ruleNodes

	return completeRule, nil
}

// findCandidateRulesWithQueryRule is the actual implementation for QueryRule
func (rf *RuleForest) findCandidateRulesWithQueryRule(query *QueryRule) []RuleWithWeight {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	candidateRules := make([]RuleWithWeight, 0)

	if rf.Dimensions.Count() == 0 {
		return candidateRules
	}

	// Resolve dimension configs once for this query
	dimensionConfigs := rf.resolveDimensionConfigs(query)

	// OPTIMIZATION: For equal match types, if we have the first dimension value in the query,
	// we can directly lookup trees that match that specific value instead of iterating all trees
	firstDimName, _ := rf.Dimensions.Get(0)
	firstDimValue, hasFirstDimValue := query.Values[firstDimName]

	// Process each tree type to find matching rules
	for matchType, trees := range rf.Trees {
		if matchType == MatchTypeEqual && hasFirstDimValue {
			// OPTIMIZATION: For equal match types, use hash index for O(1) direct lookup
			indexKey := firstDimName + ":" + firstDimValue
			if tree, exists := rf.EqualTreesIndex[indexKey]; exists {
				rf.searchTree(tree, query, 0, &candidateRules, dimensionConfigs)
			}
		} else {
			// For non-equal match types or when we don't have the first dimension value,
			// iterate through all trees (no optimization possible)
			for _, tree := range trees {
				if rf.matchesValue(firstDimValue, tree.Value, matchType) {
					rf.searchTree(tree, query, 0, &candidateRules, dimensionConfigs)
				}
			}
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

// searchTree searches a single tree for matching rules following dimension order methodology
func (rf *RuleForest) searchTree(node *SharedNode, query *QueryRule, depth int, candidateRules *[]RuleWithWeight, dimensionConfigs *DimensionConfigs) {
	if node == nil {
		return
	}

	// Check if we should continue to the next dimension based on dimension order
	if depth+1 < rf.Dimensions.Count() {
		// Get the next dimension according to dimension order
		nextDimName, _ := rf.Dimensions.Get(depth + 1)
		nextQueryValue, hasNextQueryValue := query.Values[nextDimName]

		// If the query doesn't specify this dimension, check if we can still continue
		if !hasNextQueryValue {
			// We can only continue if there are MatchTypeAny branches at this level
			// that can handle the missing dimension
			node.mu.RLock()
			anyBranch, hasAnyBranch := node.Branches[MatchTypeAny]
			if hasAnyBranch {
				// Continue searching in MatchTypeAny branches since they can match missing dimensions
				for _, child := range anyBranch.Children {
					rf.searchTree(child, query, depth+1, candidateRules, dimensionConfigs)
				}
			}
			// If no MatchTypeAny branches exist, we cannot continue
			node.mu.RUnlock()
			return
		}

		node.mu.RLock()
		// Search through branches that could match with the next dimension
		for branchMatchType, branch := range node.Branches {
			// For performance, use direct lookup for exact matches when possible
			if branchMatchType == MatchTypeEqual {
				if child, exists := branch.Children[nextQueryValue]; exists {
					if rf.matchesValue(nextQueryValue, child.Value, branchMatchType) {
						rf.searchTree(child, query, depth+1, candidateRules, dimensionConfigs)
					}
				}
			} else {
				// Check all children in this branch
				for _, child := range branch.Children {
					if rf.matchesValue(nextQueryValue, child.Value, branchMatchType) {
						rf.searchTree(child, query, depth+1, candidateRules, dimensionConfigs)
					}
				}
			}
		}
		node.mu.RUnlock()
	} else {
		// We've traversed all dimensions according to dimension order
		// Check if the current node's dimension value matches the query for the final dimension
		node.mu.RLock()

		currentDimName := node.DimensionName
		queryValue, hasQueryValue := query.Values[currentDimName]

		for _, rule := range node.Rules {
			// Filter by rule status unless IncludeAllRules is true
			if !query.IncludeAllRules && rule.Status != RuleStatusWorking && rule.Status != "" {
				continue
			}

			// Check if this rule matches the query at the current dimension level
			if hasQueryValue {
				// Query specifies this dimension - check if rule's dimension matches
				ruleDim := rule.GetDimensionValue(currentDimName)
				if ruleDim != nil && rf.matchesValue(queryValue, ruleDim.Value, ruleDim.MatchType) {
					rf.insertRuleByWeight(candidateRules, rule, dimensionConfigs)
				}
			} else {
				// Query doesn't specify this dimension - rule must have MatchTypeAny or not have this dimension
				ruleDim := rule.GetDimensionValue(currentDimName)
				if ruleDim == nil || ruleDim.MatchType == MatchTypeAny {
					rf.insertRuleByWeight(candidateRules, rule, dimensionConfigs)
				}
			}
		}
		node.mu.RUnlock()
	}
}

// resolveDimensionConfigs merges dynamic configs with initialized configs for a query
func (rf *RuleForest) resolveDimensionConfigs(query *QueryRule) *DimensionConfigs {
	if query.DynamicDimensionConfigs == nil || query.DynamicDimensionConfigs.Count() <= 0 {
		return rf.Dimensions
	}

	var dcs []*DimensionConfig
	for _, dim := range rf.Dimensions.GetSortedNames() {
		if !query.DynamicDimensionConfigs.Exist(dim) {
			dcs = append(dcs, rf.Dimensions.CloneDimension(dim))
		}
	}
	return query.DynamicDimensionConfigs.Clone(dcs)
}

// searchConflict efficiently finds rules that could conflict with the given rule by traversing the forest structure
func (rf *RuleForest) searchConflict(newRule *Rule, candidateRules *[]*Rule) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if len(newRule.Dimensions) == 0 || rf.Dimensions.Count() == 0 {
		return
	}

	// Get the first dimension according to dimension order
	firstDimName, _ := rf.Dimensions.Get(0)
	firstDim := newRule.GetDimensionValue(firstDimName)
	if firstDim == nil {
		// Rule doesn't have the first dimension, can't traverse
		return
	}

	firstDimValue := firstDim.Value
	firstMatchType := firstDim.MatchType

	// Search trees that could potentially intersect with this rule
	// We need to check different trees based on match type compatibility
	for treeMatchType, trees := range rf.Trees {
		if rf.matchTypesCanIntersect(firstMatchType, treeMatchType) {
			for _, rootNode := range trees {
				if rf.valuesCanIntersect(firstDimValue, firstMatchType, rootNode.Value, treeMatchType) {
					rf.searchConflictInTree(rootNode, newRule, 0, candidateRules)
				}
			}
		}
	}
}

// searchConflictInTree recursively searches for conflicting rules in a specific tree
func (rf *RuleForest) searchConflictInTree(node *SharedNode, newRule *Rule, depth int, candidateRules *[]*Rule) {
	if node == nil {
		return
	}

	// Check if we should continue to the next dimension based on dimension order
	if depth+1 < rf.Dimensions.Count() {
		// Get the next dimension according to dimension order
		nextDimName, _ := rf.Dimensions.Get(depth + 1)
		nextDim := newRule.GetDimensionValue(nextDimName)

		// If the new rule doesn't have this dimension, check if we can still continue
		if nextDim == nil {
			// We can only continue if there are MatchTypeAny branches at this level
			// that can handle the missing dimension
			node.mu.RLock()
			anyBranch, hasAnyBranch := node.Branches[MatchTypeAny]
			if hasAnyBranch {
				// Continue searching in MatchTypeAny branches since they can match missing dimensions
				for _, child := range anyBranch.Children {
					rf.searchConflictInTree(child, newRule, depth+1, candidateRules)
				}
			}
			// If no MatchTypeAny branches exist, we cannot continue - return immediately
			// No rules at this level can intersect with our rule that's missing this dimension
			node.mu.RUnlock()
			return
		}

		nextDimValue := nextDim.Value
		nextMatchType := nextDim.MatchType

		node.mu.RLock()
		// Search through branches that could intersect with the next dimension
		for branchMatchType, branch := range node.Branches {
			if rf.matchTypesCanIntersect(nextMatchType, branchMatchType) {
				// For performance, use direct lookup for exact matches when possible
				if branchMatchType == MatchTypeEqual && nextMatchType == MatchTypeEqual {
					if child, exists := branch.Children[nextDimValue]; exists {
						rf.searchConflictInTree(child, newRule, depth+1, candidateRules)
					}
				} else {
					// Check all children in this branch
					for childValue, child := range branch.Children {
						if rf.valuesCanIntersect(nextDimValue, nextMatchType, childValue, branchMatchType) {
							rf.searchConflictInTree(child, newRule, depth+1, candidateRules)
						}
					}
				}
			}
		}
		node.mu.RUnlock()
	} else {
		// We've traversed all dimensions according to dimension order
		// At this point, we only need to check if the current node's dimension value
		// can intersect with the new rule's corresponding dimension value
		node.mu.RLock()

		// Get the current dimension name and the new rule's value for this dimension
		currentDimName := node.DimensionName
		newRuleDim := newRule.GetDimensionValue(currentDimName)

		if newRuleDim != nil {
			// New rule has this dimension - check if values can intersect at this dimension level
			for _, existingRule := range node.Rules {
				existingRuleDim := existingRule.GetDimensionValue(currentDimName)
				if existingRuleDim != nil {
					if rf.valuesCanIntersect(newRuleDim.Value, newRuleDim.MatchType, existingRuleDim.Value, existingRuleDim.MatchType) {
						*candidateRules = append(*candidateRules, existingRule)
					}
				}
			}
		} else {
			// New rule is missing this dimension - can intersect with MatchTypeAny rules
			for _, existingRule := range node.Rules {
				existingRuleDim := existingRule.GetDimensionValue(currentDimName)
				if existingRuleDim != nil && existingRuleDim.MatchType == MatchTypeAny {
					*candidateRules = append(*candidateRules, existingRule)
				}
			}
		}

		node.mu.RUnlock()

		// Stop here - don't search deeper since we've exhausted the new rule's dimensions
		// according to the dimension order
	}
}

// matchTypesCanIntersect checks if two match types can potentially intersect
func (rf *RuleForest) matchTypesCanIntersect(matchType1, matchType2 MatchType) bool {
	// MatchTypeAny can intersect with any match type
	if matchType1 == MatchTypeAny || matchType2 == MatchTypeAny {
		return true
	}

	// Same match types can intersect
	if matchType1 == matchType2 {
		return true
	}

	// Prefix and suffix can intersect with equal if the values are compatible
	// Equal can intersect with prefix/suffix if the values are compatible
	if (matchType1 == MatchTypeEqual && (matchType2 == MatchTypePrefix || matchType2 == MatchTypeSuffix)) ||
		(matchType2 == MatchTypeEqual && (matchType1 == MatchTypePrefix || matchType1 == MatchTypeSuffix)) {
		return true
	}

	// Prefix and suffix can potentially intersect (e.g., prefix "abc" and suffix "cde" both match "abcde")
	if (matchType1 == MatchTypePrefix && matchType2 == MatchTypeSuffix) ||
		(matchType1 == MatchTypeSuffix && matchType2 == MatchTypePrefix) {
		return true
	}

	return false
}

// valuesCanIntersect checks if two dimension values with their match types can intersect
func (rf *RuleForest) valuesCanIntersect(value1 string, matchType1 MatchType, value2 string, matchType2 MatchType) bool {
	// MatchTypeAny always intersects
	if matchType1 == MatchTypeAny || matchType2 == MatchTypeAny {
		return true
	}

	// Equal match types
	if matchType1 == MatchTypeEqual && matchType2 == MatchTypeEqual {
		return value1 == value2
	}

	// Prefix match types
	if matchType1 == MatchTypePrefix && matchType2 == MatchTypePrefix {
		// Two prefixes intersect if one is a prefix of the other
		return strings.HasPrefix(value1, value2) || strings.HasPrefix(value2, value1)
	}

	// Suffix match types
	if matchType1 == MatchTypeSuffix && matchType2 == MatchTypeSuffix {
		// Two suffixes intersect if one is a suffix of the other
		return strings.HasSuffix(value1, value2) || strings.HasSuffix(value2, value1)
	}

	// Equal with Prefix
	if matchType1 == MatchTypeEqual && matchType2 == MatchTypePrefix {
		return strings.HasPrefix(value1, value2)
	}
	if matchType1 == MatchTypePrefix && matchType2 == MatchTypeEqual {
		return strings.HasPrefix(value2, value1)
	}

	// Equal with Suffix
	if matchType1 == MatchTypeEqual && matchType2 == MatchTypeSuffix {
		return strings.HasSuffix(value1, value2)
	}
	if matchType1 == MatchTypeSuffix && matchType2 == MatchTypeEqual {
		return strings.HasSuffix(value2, value1)
	}

	// Prefix with Suffix
	return (matchType1 == MatchTypePrefix && matchType2 == MatchTypeSuffix) ||
		(matchType1 == MatchTypeSuffix && matchType2 == MatchTypePrefix)
}

// insertRuleByWeight inserts a rule into the candidate slice maintaining weight order (highest first)
func (rf *RuleForest) insertRuleByWeight(candidateRules *[]RuleWithWeight, newRule *Rule, dimensionConfigs *DimensionConfigs) {
	newWeight := newRule.CalculateTotalWeight(dimensionConfigs)

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
		if newWeight > rule.CalculateTotalWeight(dimensionConfigs) {
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

	// Remove rule from all nodes and clean up relationships immediately
	for _, node := range nodes {
		node.RemoveRule(rule.ID)
		rf.cleanupNodeRelationshipsForRule(node, rule)
	}

	// Clean up empty nodes (traverse upward)
	rf.cleanupEmptyNodes()

	// Remove from index
	delete(rf.RuleIndex, rule.ID)
}

// ReplaceRule atomically replaces one rule with another to prevent partial state visibility
// This method ensures no intermediate state where both rules coexist in the forest
func (rf *RuleForest) ReplaceRule(oldRule, newRule *Rule) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sorted := rf.Dimensions.GetSortedNames()
	if len(sorted) <= 0 {
		return fmt.Errorf("no dimension configured yet")
	}

	// TRULY ATOMIC APPROACH: Instead of remove-then-add, find the target nodes
	// for both rules and perform atomic replacement at the node level

	var newNodePath []*SharedNode

	// Step 1: Remove old rule from existing nodes
	if oldRule != nil {
		if nodes, exists := rf.RuleIndex[oldRule.ID]; exists {
			for _, node := range nodes {
				node.RemoveRule(oldRule.ID)
				rf.cleanupNodeRelationshipsForRule(node, oldRule)
			}
			delete(rf.RuleIndex, oldRule.ID)
		}
	}

	newRule = newRule.CloneAndComplete(sorted)

	// Step 2: Build path for new rule (but don't add the rule to final node yet)
	if newRule != nil && len(newRule.Dimensions) > 0 {
		firstDim := newRule.GetDimensionValue(sorted[0])
		var rootNode *SharedNode
		rootNodes := rf.Trees[firstDim.MatchType]

		// Find or create root
		if firstDim.MatchType == MatchTypeEqual {
			indexKey := firstDim.DimensionName + ":" + firstDim.Value
			if mn, ok := rf.EqualTreesIndex[indexKey]; ok {
				rootNode = mn
			}
		} else {
			for _, node := range rootNodes {
				if node.DimensionName == firstDim.DimensionName && node.Value == firstDim.Value {
					rootNode = node
					break
				}
			}
		}

		if rootNode == nil {
			rootNode = CreateSharedNode(0, firstDim.DimensionName, firstDim.Value)
			if firstDim.MatchType == MatchTypeEqual {
				indexKey := firstDim.DimensionName + ":" + firstDim.Value
				rf.EqualTreesIndex[indexKey] = rootNode
				if len(rf.Trees[firstDim.MatchType]) <= 0 {
					rf.Trees[firstDim.MatchType] = append(rf.Trees[firstDim.MatchType], &SharedNode{})
				}
			} else {
				rf.Trees[firstDim.MatchType] = append(rf.Trees[firstDim.MatchType], rootNode)
			}
		}

		newNodePath = append(newNodePath, rootNode)
		current := rootNode

		// Build path to final node
		for i := 1; i < len(sorted); i++ {
			dim := newRule.GetDimensionValue(sorted[i])
			matchType := dim.MatchType

			branch, exists := current.Branches[matchType]
			if !exists {
				branch = &MatchBranch{
					MatchType: matchType,
					Rules:     []*Rule{},
					Children:  make(map[string]*SharedNode),
				}
				current.Branches[matchType] = branch
			}

			child, exists := branch.Children[dim.Value]
			if !exists {
				child = CreateSharedNode(i, dim.DimensionName, dim.Value)
				branch.Children[dim.Value] = child

				// Track relationships
				parentNodeName := generateNodeName(current.DimensionName, current.Value, dim.MatchType)
				childNodeName := generateNodeName(child.DimensionName, child.Value, matchType)
				if rf.NodeRelationships[parentNodeName] == nil {
					rf.NodeRelationships[parentNodeName] = make(map[string]string)
				}
				rf.NodeRelationships[parentNodeName][newRule.ID] = childNodeName
			} else {
				// Track relationships for existing nodes
				parentNodeName := generateNodeName(current.DimensionName, current.Value, dim.MatchType)
				childNodeName := generateNodeName(child.DimensionName, child.Value, matchType)
				if rf.NodeRelationships[parentNodeName] == nil {
					rf.NodeRelationships[parentNodeName] = make(map[string]string)
				}
				rf.NodeRelationships[parentNodeName][newRule.ID] = childNodeName
			}

			newNodePath = append(newNodePath, child)
			current = child
		}

		// Step 3: Now atomically add the new rule to the final node
		finalMatchType := newRule.GetDimensionValue(sorted[len(sorted)-1]).MatchType
		current.AddRule(newRule, finalMatchType)
		rf.RuleIndex[newRule.ID] = newNodePath
	}

	// Step 4: Clean up empty nodes from old rule removal after new rule is fully added
	rf.cleanupEmptyNodes()

	return nil
}

// cleanupEmptyNodes removes empty nodes from the forest
func (rf *RuleForest) cleanupEmptyNodes() {
	// This is a simplified cleanup - in practice, you might want more sophisticated cleanup
	for matchType, trees := range rf.Trees {
		var cleanTrees []*SharedNode
		for _, tree := range trees {
			if rf.hasRulesOrChildren(tree) {
				cleanTrees = append(cleanTrees, tree)
			} else if matchType == MatchTypeEqual {
				// OPTIMIZATION: Remove from equal trees index if the tree is being cleaned up
				indexKey := tree.DimensionName + ":" + tree.Value
				delete(rf.EqualTreesIndex, indexKey)
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
	// Exclude one empty tree node in trees
	totalTrees += len(rf.EqualTreesIndex) - 1

	levelCounts := make(map[int]int)
	totalNodes, sharedNodes, maxRules, totalRules := 0, 0, 0, 0

	// Count nodes across all trees
	for mt, trees := range rf.Trees {
		// only one equal tree index but many trees
		if mt == MatchTypeEqual {
			for _, tree := range rf.EqualTreesIndex {
				count, shared, max, ruleCount := rf.countNodesAndSharing(tree, levelCounts)
				totalNodes += count
				sharedNodes += shared
				totalRules += ruleCount
				if max > maxRules {
					maxRules = max
				}
			}
		} else {
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
	}

	stats["total_trees"] = totalTrees
	stats["total_nodes"] = totalNodes
	stats["shared_nodes"] = sharedNodes
	stats["max_rules_per_node"] = maxRules
	stats["total_rules"] = totalRules
	stats["levels"] = levelCounts
	stats["dimension_order"] = rf.Dimensions
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
