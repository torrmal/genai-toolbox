// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package helpers provides utility functions for transforming and processing Neo4j
// schema data. It includes functions for converting raw query results from both
// APOC and native Cypher queries into a standardized, structured format.
package helpers

import (
	"fmt"
	"sort"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/tools/neo4j/neo4jschema/types"
)

// ConvertToStringSlice converts a slice of any type to a slice of strings.
// It uses fmt.Sprintf to perform the conversion for each element.
// Example:
//
//	input: []any{"user", 123, true}
//	output: []string{"user", "123", "true"}
func ConvertToStringSlice(slice []any) []string {
	result := make([]string, len(slice))
	for i, v := range slice {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

// GetStringValue safely converts any value to its string representation.
// If the input value is nil, it returns an empty string.
func GetStringValue(val any) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

// MapToAPOCSchema converts a raw map from a Cypher query into a structured
// APOCSchemaResult. This is a workaround for database drivers that may return
// complex nested structures as `map[string]any` instead of unmarshalling
// directly into a struct. It achieves this by marshalling the map to YAML and
// then unmarshalling into the target struct.
func MapToAPOCSchema(schemaMap map[string]any) (*types.APOCSchemaResult, error) {
	schemaBytes, err := yaml.Marshal(schemaMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema map: %w", err)
	}
	var entities map[string]types.APOCEntity
	if err = yaml.Unmarshal(schemaBytes, &entities); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema map into entities: %w", err)
	}
	return &types.APOCSchemaResult{Value: entities}, nil
}

// ProcessAPOCSchema transforms the nested result from the `apoc.meta.schema()`
// procedure into flat lists of node labels and relationships, along with
// aggregated database statistics. It iterates through entities, processes nodes,
// and extracts outgoing relationship information nested within those nodes.
func ProcessAPOCSchema(apocSchema *types.APOCSchemaResult) ([]types.NodeLabel, []types.Relationship, *types.Statistics) {
	var nodeLabels []types.NodeLabel
	relMap := make(map[string]*types.Relationship)
	stats := &types.Statistics{
		NodesByLabel:        make(map[string]int64),
		RelationshipsByType: make(map[string]int64),
		PropertiesByLabel:   make(map[string]int64),
		PropertiesByRelType: make(map[string]int64),
	}

	for name, entity := range apocSchema.Value {
		// We only process top-level entities of type "node". Relationship info is
		// derived from the "relationships" field within each node entity.
		if entity.Type != "node" {
			continue
		}

		nodeLabel := types.NodeLabel{
			Name:       name,
			Count:      entity.Count,
			Properties: extractAPOCProperties(entity.Properties),
		}
		nodeLabels = append(nodeLabels, nodeLabel)

		// Aggregate statistics for the node.
		stats.NodesByLabel[name] = entity.Count
		stats.TotalNodes += entity.Count
		propCount := int64(len(nodeLabel.Properties))
		stats.PropertiesByLabel[name] = propCount
		stats.TotalProperties += propCount * entity.Count

		// Extract relationship information from the node.
		for relName, relInfo := range entity.Relationships {
			// Only process outgoing relationships to avoid double-counting.
			if relInfo.Direction != "out" {
				continue
			}

			rel, exists := relMap[relName]
			if !exists {
				rel = &types.Relationship{
					Type:       relName,
					Properties: extractAPOCProperties(relInfo.Properties),
				}
				if len(relInfo.Labels) > 0 {
					rel.EndNode = relInfo.Labels[0]
				}
				rel.StartNode = name
				relMap[relName] = rel
			}
			rel.Count += relInfo.Count
		}
	}

	// Consolidate the relationships from the map into a slice and update stats.
	relationships := make([]types.Relationship, 0, len(relMap))
	for _, rel := range relMap {
		relationships = append(relationships, *rel)
		stats.RelationshipsByType[rel.Type] = rel.Count
		stats.TotalRelationships += rel.Count
		propCount := int64(len(rel.Properties))
		stats.PropertiesByRelType[rel.Type] = propCount
		stats.TotalProperties += propCount * rel.Count
	}

	sortAndClean(nodeLabels, relationships, stats)

	// Set empty maps and lists to nil for cleaner output.
	if len(nodeLabels) == 0 {
		nodeLabels = nil
	}
	if len(relationships) == 0 {
		relationships = nil
	}
	return nodeLabels, relationships, stats
}

// ProcessNonAPOCSchema serves as an alternative to ProcessAPOCSchema for environments
// where APOC procedures are not available. It converts schema data gathered from
// multiple separate, native Cypher queries (providing node counts, property maps, etc.)
// into the same standardized, structured format.
func ProcessNonAPOCSchema(
	nodeCounts map[string]int64,
	nodePropsMap map[string]map[string]map[string]bool,
	relCounts map[string]int64,
	relPropsMap map[string]map[string]map[string]bool,
	relConnectivity map[string]types.RelConnectivityInfo,
) ([]types.NodeLabel, []types.Relationship, *types.Statistics) {
	stats := &types.Statistics{
		NodesByLabel:        make(map[string]int64),
		RelationshipsByType: make(map[string]int64),
		PropertiesByLabel:   make(map[string]int64),
		PropertiesByRelType: make(map[string]int64),
	}

	// Process node information.
	nodeLabels := make([]types.NodeLabel, 0, len(nodeCounts))
	for label, count := range nodeCounts {
		properties := make([]types.PropertyInfo, 0)
		if props, ok := nodePropsMap[label]; ok {
			for key, typeSet := range props {
				typeList := make([]string, 0, len(typeSet))
				for tp := range typeSet {
					typeList = append(typeList, tp)
				}
				sort.Strings(typeList)
				properties = append(properties, types.PropertyInfo{Name: key, Types: typeList})
			}
		}
		sort.Slice(properties, func(i, j int) bool { return properties[i].Name < properties[j].Name })

		nodeLabels = append(nodeLabels, types.NodeLabel{Name: label, Count: count, Properties: properties})

		// Aggregate node statistics.
		stats.NodesByLabel[label] = count
		stats.TotalNodes += count
		propCount := int64(len(properties))
		stats.PropertiesByLabel[label] = propCount
		stats.TotalProperties += propCount * count
	}

	// Process relationship information.
	relationships := make([]types.Relationship, 0, len(relCounts))
	for relType, count := range relCounts {
		properties := make([]types.PropertyInfo, 0)
		if props, ok := relPropsMap[relType]; ok {
			for key, typeSet := range props {
				typeList := make([]string, 0, len(typeSet))
				for tp := range typeSet {
					typeList = append(typeList, tp)
				}
				sort.Strings(typeList)
				properties = append(properties, types.PropertyInfo{Name: key, Types: typeList})
			}
		}
		sort.Slice(properties, func(i, j int) bool { return properties[i].Name < properties[j].Name })

		conn := relConnectivity[relType]
		relationships = append(relationships, types.Relationship{
			Type:       relType,
			Count:      count,
			StartNode:  conn.StartNode,
			EndNode:    conn.EndNode,
			Properties: properties,
		})

		// Aggregate relationship statistics.
		stats.RelationshipsByType[relType] = count
		stats.TotalRelationships += count
		propCount := int64(len(properties))
		stats.PropertiesByRelType[relType] = propCount
		stats.TotalProperties += propCount * count
	}

	sortAndClean(nodeLabels, relationships, stats)

	// Set empty maps and lists to nil for cleaner output.
	if len(nodeLabels) == 0 {
		nodeLabels = nil
	}
	if len(relationships) == 0 {
		relationships = nil
	}
	return nodeLabels, relationships, stats
}

// extractAPOCProperties is a helper that converts a map of APOC property
// information into a slice of standardized PropertyInfo structs. The resulting
// slice is sorted by property name for consistent ordering.
func extractAPOCProperties(props map[string]types.APOCProperty) []types.PropertyInfo {
	properties := make([]types.PropertyInfo, 0, len(props))
	for name, info := range props {
		properties = append(properties, types.PropertyInfo{
			Name:      name,
			Types:     []string{info.Type},
			Indexed:   info.Indexed,
			Unique:    info.Unique,
			Mandatory: info.Existence,
		})
	}
	sort.Slice(properties, func(i, j int) bool {
		return properties[i].Name < properties[j].Name
	})
	return properties
}

// sortAndClean performs final processing on the schema data. It sorts node and
// relationship slices for consistent output, primarily by count (descending) and
// secondarily by name/type. It also sets any empty maps in the statistics
// struct to nil, which can simplify downstream serialization (e.g., omitting
// empty fields in JSON).
func sortAndClean(nodeLabels []types.NodeLabel, relationships []types.Relationship, stats *types.Statistics) {
	// Sort nodes by count (desc) then name (asc).
	sort.Slice(nodeLabels, func(i, j int) bool {
		if nodeLabels[i].Count != nodeLabels[j].Count {
			return nodeLabels[i].Count > nodeLabels[j].Count
		}
		return nodeLabels[i].Name < nodeLabels[j].Name
	})
	// Sort relationships by count (desc) then type (asc).
	sort.Slice(relationships, func(i, j int) bool {
		if relationships[i].Count != relationships[j].Count {
			return relationships[i].Count > relationships[j].Count
		}
		return relationships[i].Type < relationships[j].Type
	})
	// Nil out empty maps for cleaner output.
	if len(stats.NodesByLabel) == 0 {
		stats.NodesByLabel = nil
	}
	if len(stats.RelationshipsByType) == 0 {
		stats.RelationshipsByType = nil
	}
	if len(stats.PropertiesByLabel) == 0 {
		stats.PropertiesByLabel = nil
	}
	if len(stats.PropertiesByRelType) == 0 {
		stats.PropertiesByRelType = nil
	}
}
