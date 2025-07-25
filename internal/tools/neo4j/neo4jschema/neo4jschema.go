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

package neo4jschema

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	neo4jsc "github.com/googleapis/genai-toolbox/internal/sources/neo4j"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/tools/neo4j/neo4jschema/cache"
	"github.com/googleapis/genai-toolbox/internal/tools/neo4j/neo4jschema/helpers"
	"github.com/googleapis/genai-toolbox/internal/tools/neo4j/neo4jschema/types"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// kind defines the unique identifier for this tool.
const kind string = "neo4j-schema"

// init registers the tool with the application's tool registry when the package is initialized.
func init() {
	if !tools.Register(kind, newConfig) {
		panic(fmt.Sprintf("tool kind %q already registered", kind))
	}
}

// newConfig decodes a YAML configuration into a Config struct.
// This function is called by the tool registry to create a new configuration object.
func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

// compatibleSource defines the interface a data source must implement to be used by this tool.
// It ensures that the source can provide a Neo4j driver and database name.
type compatibleSource interface {
	Neo4jDriver() neo4j.DriverWithContext
	Neo4jDatabase() string
}

// Statically verify that our compatible source implementation is valid.
var _ compatibleSource = &neo4jsc.Source{}

// compatibleSources lists the kinds of sources that are compatible with this tool.
var compatibleSources = [...]string{neo4jsc.SourceKind}

// Config holds the configuration settings for the Neo4j schema tool.
// These settings are typically read from a YAML file.
type Config struct {
	Name               string   `yaml:"name" validate:"required"`
	Kind               string   `yaml:"kind" validate:"required"`
	Source             string   `yaml:"source" validate:"required"`
	Description        string   `yaml:"description" validate:"required"`
	AuthRequired       []string `yaml:"authRequired"`
	CacheExpireMinutes *int     `yaml:"cacheExpireMinutes,omitempty"` // Cache expiration time in minutes.
}

// Statically verify that Config implements the tools.ToolConfig interface.
var _ tools.ToolConfig = Config{}

// ToolConfigKind returns the kind of this tool configuration.
func (cfg Config) ToolConfigKind() string {
	return kind
}

// Initialize sets up the tool with its dependencies and returns a ready-to-use Tool instance.
func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	// Verify that the specified source exists.
	rawS, ok := srcs[cfg.Source]
	if !ok {
		return nil, fmt.Errorf("no source named %q configured", cfg.Source)
	}

	// Verify the source is of a compatible kind.
	s, ok := rawS.(compatibleSource)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be one of %q", kind, compatibleSources)
	}

	parameters := tools.Parameters{}
	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: cfg.Description,
		InputSchema: parameters.McpManifest(),
	}

	// Set a default cache expiration if not provided in the configuration.
	if cfg.CacheExpireMinutes == nil {
		defaultExpiration := cache.DefaultExpiration // Default to 60 minutes
		cfg.CacheExpireMinutes = &defaultExpiration
	}

	// Finish tool setup by creating the Tool instance.
	t := Tool{
		Name:               cfg.Name,
		Kind:               kind,
		AuthRequired:       cfg.AuthRequired,
		Driver:             s.Neo4jDriver(),
		Database:           s.Neo4jDatabase(),
		cache:              cache.NewCache(),
		cacheExpireMinutes: cfg.CacheExpireMinutes,
		manifest:           tools.Manifest{Description: cfg.Description, Parameters: parameters.Manifest(), AuthRequired: cfg.AuthRequired},
		mcpManifest:        mcpManifest,
	}
	return t, nil
}

// Statically verify that Tool implements the tools.Tool interface.
var _ tools.Tool = Tool{}

// Tool represents the Neo4j schema extraction tool.
// It holds the Neo4j driver, database information, and a cache for the schema.
type Tool struct {
	Name               string   `yaml:"name"`
	Kind               string   `yaml:"kind"`
	AuthRequired       []string `yaml:"authRequired"`
	Driver             neo4j.DriverWithContext
	Database           string
	cache              *cache.Cache
	cacheExpireMinutes *int
	manifest           tools.Manifest
	mcpManifest        tools.McpManifest
}

// Invoke executes the tool's main logic: fetching the Neo4j schema.
// It first checks the cache for a valid schema before extracting it from the database.
func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) (any, error) {
	// Check if a valid schema is already in the cache.
	if cachedSchema, ok := t.cache.Get("schema"); ok {
		if schema, ok := cachedSchema.(*types.SchemaInfo); ok {
			return schema, nil
		}
	}

	// If not cached, extract the schema from the database.
	schema, err := t.extractSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract database schema: %w", err)
	}

	// Cache the newly extracted schema for future use.
	expiration := time.Duration(*t.cacheExpireMinutes) * time.Minute
	t.cache.Set("schema", schema, expiration)

	return schema, nil
}

// ParseParams is a placeholder as this tool does not require input parameters.
func (t Tool) ParseParams(data map[string]any, claimsMap map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParamValues{}, nil
}

// Manifest returns the tool's manifest, which describes its purpose and parameters.
func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

// McpManifest returns the machine-consumable manifest for the tool.
func (t Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

// Authorized checks if the tool is authorized to run based on the provided authentication services.
func (t Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.AuthRequired, verifiedAuthServices)
}

// checkAPOCProcedures verifies if essential APOC procedures are available in the database.
// It returns true only if all required procedures are found.
func (t Tool) checkAPOCProcedures(ctx context.Context) (bool, error) {
	proceduresToCheck := []string{"apoc.meta.schema", "apoc.meta.cypher.types"}

	session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
	defer session.Close(ctx)

	// This query efficiently counts how many of the specified procedures exist.
	query := "SHOW PROCEDURES YIELD name WHERE name IN $procs RETURN count(name) AS procCount"
	params := map[string]any{"procs": proceduresToCheck}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return false, fmt.Errorf("failed to execute procedure check query: %w", err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve single result for procedure check: %w", err)
	}

	rawCount, found := record.Get("procCount")
	if !found {
		return false, fmt.Errorf("field 'procCount' not found in result record")
	}

	procCount, ok := rawCount.(int64)
	if !ok {
		return false, fmt.Errorf("expected 'procCount' to be of type int64, but got %T", rawCount)
	}

	// Return true only if the number of found procedures matches the number we were looking for.
	return procCount == int64(len(proceduresToCheck)), nil
}

// extractSchema orchestrates the concurrent extraction of different parts of the database schema.
// It runs several extraction tasks in parallel for efficiency.
func (t Tool) extractSchema(ctx context.Context) (*types.SchemaInfo, error) {
	schema := &types.SchemaInfo{}
	var mu sync.Mutex

	// Define the different schema extraction tasks.
	tasks := []struct {
		name string
		fn   func() error
	}{
		{
			name: "database-info",
			fn: func() error {
				dbInfo, err := t.extractDatabaseInfo(ctx)
				if err != nil {
					return fmt.Errorf("failed to extract database info: %w", err)
				}
				mu.Lock()
				defer mu.Unlock()
				schema.DatabaseInfo = *dbInfo
				return nil
			},
		},
		{
			name: "schema-extraction",
			fn: func() error {
				// Check if APOC procedures are available.
				hasAPOC, err := t.checkAPOCProcedures(ctx)
				if err != nil {
					return fmt.Errorf("failed to check APOC procedures: %w", err)
				}

				var nodeLabels []types.NodeLabel
				var relationships []types.Relationship
				var stats *types.Statistics

				// Use APOC if available for a more detailed schema; otherwise, use native queries.
				if hasAPOC {
					nodeLabels, relationships, stats, err = t.GetAPOCSchema(ctx)
				} else {
					nodeLabels, relationships, stats, err = t.GetSchemaWithoutAPOC(ctx, 100)
				}
				if err != nil {
					return fmt.Errorf("failed to get schema: %w", err)
				}

				mu.Lock()
				defer mu.Unlock()
				schema.NodeLabels = nodeLabels
				schema.Relationships = relationships
				schema.Statistics = *stats
				return nil
			},
		},
		{
			name: "constraints",
			fn: func() error {
				constraints, err := t.extractConstraints(ctx)
				if err != nil {
					return fmt.Errorf("failed to extract constraints: %w", err)
				}
				mu.Lock()
				defer mu.Unlock()
				schema.Constraints = constraints
				return nil
			},
		},
		{
			name: "indexes",
			fn: func() error {
				indexes, err := t.extractIndexes(ctx)
				if err != nil {
					return fmt.Errorf("failed to extract indexes: %w", err)
				}
				mu.Lock()
				defer mu.Unlock()
				schema.Indexes = indexes
				return nil
			},
		},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(tasks))

	// Execute all tasks concurrently.
	for _, task := range tasks {
		wg.Add(1)
		go func(task struct {
			name string
			fn   func() error
		}) {
			defer wg.Done()
			if err := task.fn(); err != nil {
				errCh <- err
			}
		}(task)
	}

	wg.Wait()
	close(errCh)

	// Collect any errors that occurred during the concurrent tasks.
	for err := range errCh {
		if err != nil {
			schema.Errors = append(schema.Errors, err.Error())
		}
	}
	return schema, nil
}

// GetAPOCSchema extracts schema information using the APOC library, which provides detailed metadata.
func (t Tool) GetAPOCSchema(ctx context.Context) ([]types.NodeLabel, []types.Relationship, *types.Statistics, error) {
	var nodeLabels []types.NodeLabel
	var relationships []types.Relationship
	stats := &types.Statistics{
		NodesByLabel:        make(map[string]int64),
		RelationshipsByType: make(map[string]int64),
		PropertiesByLabel:   make(map[string]int64),
		PropertiesByRelType: make(map[string]int64),
	}

	var mu sync.Mutex
	var firstErr error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	handleError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel() // Cancel other operations on the first error.
		}
	}

	tasks := []struct {
		name string
		fn   func(session neo4j.SessionWithContext) error
	}{
		{
			name: "apoc-schema",
			fn: func(session neo4j.SessionWithContext) error {
				result, err := session.Run(ctx, "CALL apoc.meta.schema({sample: 10}) YIELD value RETURN value", nil)
				if err != nil {
					return fmt.Errorf("failed to run APOC schema query: %w", err)
				}
				if !result.Next(ctx) {
					return fmt.Errorf("no results from APOC schema query")
				}
				schemaMap, ok := result.Record().Values[0].(map[string]any)
				if !ok {
					return fmt.Errorf("unexpected result format from APOC schema query: %T", result.Record().Values[0])
				}
				apocSchema, err := helpers.MapToAPOCSchema(schemaMap)
				if err != nil {
					return fmt.Errorf("failed to convert schema map to APOCSchemaResult: %w", err)
				}
				nodes, _, apocStats := helpers.ProcessAPOCSchema(apocSchema)
				mu.Lock()
				defer mu.Unlock()
				nodeLabels = nodes
				stats.TotalNodes = apocStats.TotalNodes
				stats.TotalProperties += apocStats.TotalProperties
				stats.NodesByLabel = apocStats.NodesByLabel
				stats.PropertiesByLabel = apocStats.PropertiesByLabel
				return nil
			},
		},
		{
			name: "apoc-relationships",
			fn: func(session neo4j.SessionWithContext) error {
				query := `
					MATCH (startNode)-[rel]->(endNode)
					WITH
					  labels(startNode)[0] AS startNode,
					  type(rel) AS relType,
					  apoc.meta.cypher.types(rel) AS relProperties,
					  labels(endNode)[0] AS endNode,
					  count(*) AS count
					RETURN relType, startNode, endNode, relProperties, count`
				result, err := session.Run(ctx, query, nil)
				if err != nil {
					return fmt.Errorf("failed to extract relationships: %w", err)
				}
				for result.Next(ctx) {
					record := result.Record()
					relType, startNode, endNode := record.Values[0].(string), record.Values[1].(string), record.Values[2].(string)
					properties, count := record.Values[3].(map[string]any), record.Values[4].(int64)

					if relType == "" || count == 0 {
						continue
					}
					relationship := types.Relationship{Type: relType, StartNode: startNode, EndNode: endNode, Count: count, Properties: []types.PropertyInfo{}}
					for prop, propType := range properties {
						relationship.Properties = append(relationship.Properties, types.PropertyInfo{Name: prop, Types: []string{propType.(string)}})
					}
					mu.Lock()
					relationships = append(relationships, relationship)
					stats.RelationshipsByType[relType] += count
					stats.TotalRelationships += count
					propCount := int64(len(relationship.Properties))
					stats.TotalProperties += propCount
					stats.PropertiesByRelType[relType] += propCount
					mu.Unlock()
				}
				mu.Lock()
				defer mu.Unlock()
				if len(stats.RelationshipsByType) == 0 {
					stats.RelationshipsByType = nil
				}
				if len(stats.PropertiesByRelType) == 0 {
					stats.PropertiesByRelType = nil
				}
				return nil
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for _, task := range tasks {
		go func(task struct {
			name string
			fn   func(session neo4j.SessionWithContext) error
		}) {
			defer wg.Done()
			session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
			defer session.Close(ctx)
			if err := task.fn(session); err != nil {
				handleError(fmt.Errorf("task %s failed: %w", task.name, err))
			}
		}(task)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, nil, nil, firstErr
	}
	return nodeLabels, relationships, stats, nil
}

// GetSchemaWithoutAPOC extracts schema information using native Cypher queries.
// This serves as a fallback for databases without APOC installed.
func (t Tool) GetSchemaWithoutAPOC(ctx context.Context, sampleSize int) ([]types.NodeLabel, []types.Relationship, *types.Statistics, error) {
	nodePropsMap := make(map[string]map[string]map[string]bool)
	relPropsMap := make(map[string]map[string]map[string]bool)
	nodeCounts := make(map[string]int64)
	relCounts := make(map[string]int64)
	relConnectivity := make(map[string]types.RelConnectivityInfo)

	var mu sync.Mutex
	var firstErr error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	handleError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	tasks := []struct {
		name string
		fn   func(session neo4j.SessionWithContext) error
	}{
		{
			name: "node-schema",
			fn: func(session neo4j.SessionWithContext) error {
				countResult, err := session.Run(ctx, `MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS count ORDER BY count DESC`, nil)
				if err != nil {
					return fmt.Errorf("node count query failed: %w", err)
				}
				var labelsList []string
				mu.Lock()
				for countResult.Next(ctx) {
					record := countResult.Record()
					label, count := record.Values[0].(string), record.Values[1].(int64)
					nodeCounts[label] = count
					labelsList = append(labelsList, label)
				}
				mu.Unlock()
				if err = countResult.Err(); err != nil {
					return fmt.Errorf("node count result error: %w", err)
				}

				for _, label := range labelsList {
					propQuery := fmt.Sprintf(`MATCH (n:%s) WITH n LIMIT $sampleSize UNWIND keys(n) AS key WITH key, n[key] AS value WHERE value IS NOT NULL RETURN key, COLLECT(DISTINCT valueType(value)) AS types`, label)
					propResult, err := session.Run(ctx, propQuery, map[string]any{"sampleSize": sampleSize})
					if err != nil {
						return fmt.Errorf("node properties query for label %s failed: %w", label, err)
					}
					mu.Lock()
					if nodePropsMap[label] == nil {
						nodePropsMap[label] = make(map[string]map[string]bool)
					}
					for propResult.Next(ctx) {
						record := propResult.Record()
						key, types := record.Values[0].(string), record.Values[1].([]any)
						if nodePropsMap[label][key] == nil {
							nodePropsMap[label][key] = make(map[string]bool)
						}
						for _, tp := range types {
							nodePropsMap[label][key][tp.(string)] = true
						}
					}
					mu.Unlock()
					if err = propResult.Err(); err != nil {
						return fmt.Errorf("node properties result error for label %s: %w", label, err)
					}
				}
				return nil
			},
		},
		{
			name: "relationship-schema",
			fn: func(session neo4j.SessionWithContext) error {
				relQuery := `
					MATCH (start)-[r]->(end)
					WITH type(r) AS relType, labels(start) AS startLabels, labels(end) AS endLabels, count(*) AS count
					RETURN relType, CASE WHEN size(startLabels) > 0 THEN startLabels[0] ELSE null END AS startLabel, CASE WHEN size(endLabels) > 0 THEN endLabels[0] ELSE null END AS endLabel, sum(count) AS totalCount
					ORDER BY totalCount DESC`
				relResult, err := session.Run(ctx, relQuery, nil)
				if err != nil {
					return fmt.Errorf("relationship count query failed: %w", err)
				}
				var relTypesList []string
				mu.Lock()
				for relResult.Next(ctx) {
					record := relResult.Record()
					relType := record.Values[0].(string)
					startLabel := ""
					if record.Values[1] != nil {
						startLabel = record.Values[1].(string)
					}
					endLabel := ""
					if record.Values[2] != nil {
						endLabel = record.Values[2].(string)
					}
					count := record.Values[3].(int64)
					relCounts[relType] = count
					relTypesList = append(relTypesList, relType)
					if existing, ok := relConnectivity[relType]; !ok || count > existing.Count {
						relConnectivity[relType] = types.RelConnectivityInfo{StartNode: startLabel, EndNode: endLabel, Count: count}
					}
				}
				mu.Unlock()
				if err = relResult.Err(); err != nil {
					return fmt.Errorf("relationship count result error: %w", err)
				}

				for _, relType := range relTypesList {
					propQuery := fmt.Sprintf(`MATCH ()-[r:%s]->() WITH r LIMIT $sampleSize WHERE size(keys(r)) > 0 UNWIND keys(r) AS key WITH key, r[key] AS value WHERE value IS NOT NULL RETURN key, COLLECT(DISTINCT valueType(value)) AS types`, relType)
					propResult, err := session.Run(ctx, propQuery, map[string]any{"sampleSize": sampleSize})
					if err != nil {
						return fmt.Errorf("relationship properties query for type %s failed: %w", relType, err)
					}
					mu.Lock()
					if relPropsMap[relType] == nil {
						relPropsMap[relType] = make(map[string]map[string]bool)
					}
					for propResult.Next(ctx) {
						record := propResult.Record()
						key, propTypes := record.Values[0].(string), record.Values[1].([]any)
						if relPropsMap[relType][key] == nil {
							relPropsMap[relType][key] = make(map[string]bool)
						}
						for _, t := range propTypes {
							relPropsMap[relType][key][t.(string)] = true
						}
					}
					mu.Unlock()
					if err = propResult.Err(); err != nil {
						return fmt.Errorf("relationship properties result error for type %s: %w", relType, err)
					}
				}
				return nil
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for _, task := range tasks {
		go func(task struct {
			name string
			fn   func(session neo4j.SessionWithContext) error
		}) {
			defer wg.Done()
			session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
			defer session.Close(ctx)
			if err := task.fn(session); err != nil {
				handleError(fmt.Errorf("task %s failed: %w", task.name, err))
			}
		}(task)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, nil, nil, firstErr
	}

	nodeLabels, relationships, stats := helpers.ProcessNonAPOCSchema(nodeCounts, nodePropsMap, relCounts, relPropsMap, relConnectivity)
	return nodeLabels, relationships, stats, nil
}

// extractDatabaseInfo retrieves general information about the Neo4j database instance.
func (t Tool) extractDatabaseInfo(ctx context.Context) (*types.DatabaseInfo, error) {
	session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition", nil)
	if err != nil {
		return nil, err
	}

	dbInfo := &types.DatabaseInfo{}
	if result.Next(ctx) {
		record := result.Record()
		dbInfo.Name = record.Values[0].(string)
		if versions, ok := record.Values[1].([]any); ok && len(versions) > 0 {
			dbInfo.Version = versions[0].(string)
		}
		dbInfo.Edition = record.Values[2].(string)
	}
	return dbInfo, result.Err()
}

// extractConstraints fetches all schema constraints from the database.
func (t Tool) extractConstraints(ctx context.Context) ([]types.Constraint, error) {
	session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "SHOW CONSTRAINTS", nil)
	if err != nil {
		return nil, err
	}

	var constraints []types.Constraint
	for result.Next(ctx) {
		record := result.Record().AsMap()
		constraint := types.Constraint{
			Name:       helpers.GetStringValue(record["name"]),
			Type:       helpers.GetStringValue(record["type"]),
			EntityType: helpers.GetStringValue(record["entityType"]),
		}
		if labels, ok := record["labelsOrTypes"].([]any); ok && len(labels) > 0 {
			constraint.Label = labels[0].(string)
		}
		if props, ok := record["properties"].([]any); ok {
			constraint.Properties = helpers.ConvertToStringSlice(props)
		}
		constraints = append(constraints, constraint)
	}
	return constraints, result.Err()
}

// extractIndexes fetches all schema indexes from the database.
func (t Tool) extractIndexes(ctx context.Context) ([]types.Index, error) {
	session := t.Driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: t.Database})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "SHOW INDEXES", nil)
	if err != nil {
		return nil, err
	}

	var indexes []types.Index
	for result.Next(ctx) {
		record := result.Record().AsMap()
		index := types.Index{
			Name:       helpers.GetStringValue(record["name"]),
			State:      helpers.GetStringValue(record["state"]),
			Type:       helpers.GetStringValue(record["type"]),
			EntityType: helpers.GetStringValue(record["entityType"]),
		}
		if labels, ok := record["labelsOrTypes"].([]any); ok && len(labels) > 0 {
			index.Label = labels[0].(string)
		}
		if props, ok := record["properties"].([]any); ok {
			index.Properties = helpers.ConvertToStringSlice(props)
		}
		indexes = append(indexes, index)
	}
	return indexes, result.Err()
}
