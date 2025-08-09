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

package neo4jexecutecypher

import (
	"context"
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	neo4jsc "github.com/googleapis/genai-toolbox/internal/sources/neo4j"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/tools/neo4j/neo4jexecutecypher/classifier"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const kind string = "neo4j-execute-cypher"

func init() {
	if !tools.Register(kind, newConfig) {
		panic(fmt.Sprintf("tool kind %q already registered", kind))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type compatibleSource interface {
	Neo4jDriver() neo4j.DriverWithContext
	Neo4jDatabase() string
}

// validate compatible sources are still compatible
var _ compatibleSource = &neo4jsc.Source{}

var compatibleSources = [...]string{neo4jsc.SourceKind}

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description" validate:"required"`
	ReadOnly     bool     `yaml:"readOnly"`
	AuthRequired []string `yaml:"authRequired"`
}

// validate interface
var _ tools.ToolConfig = Config{}

func (cfg Config) ToolConfigKind() string {
	return kind
}

func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	// verify source exists
	rawS, ok := srcs[cfg.Source]
	if !ok {
		return nil, fmt.Errorf("no source named %q configured", cfg.Source)
	}

	// verify the source is compatible
	var s compatibleSource
	s, ok = rawS.(compatibleSource)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be one of %q", kind, compatibleSources)
	}

	cypherParameter := tools.NewStringParameter("cypher", "The cypher to execute.")
	parameters := tools.Parameters{cypherParameter}

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: cfg.Description,
		InputSchema: parameters.McpManifest(),
	}

	// finish tool setup
	t := Tool{
		Name:         cfg.Name,
		Kind:         kind,
		Parameters:   parameters,
		AuthRequired: cfg.AuthRequired,
		ReadOnly:     cfg.ReadOnly,
		Driver:       s.Neo4jDriver(),
		Database:     s.Neo4jDatabase(),
		classifier:   classifier.NewQueryClassifier(),
		manifest:     tools.Manifest{Description: cfg.Description, Parameters: parameters.Manifest(), AuthRequired: cfg.AuthRequired},
		mcpManifest:  mcpManifest,
	}
	return t, nil
}

// validate interface
var _ tools.Tool = Tool{}

type Tool struct {
	Name         string           `yaml:"name"`
	Kind         string           `yaml:"kind"`
	Parameters   tools.Parameters `yaml:"parameters"`
	AuthRequired []string         `yaml:"authRequired"`
	ReadOnly     bool             `yaml:"readOnly"`
	Database     string
	Driver       neo4j.DriverWithContext
	classifier   *classifier.QueryClassifier
	manifest     tools.Manifest
	mcpManifest  tools.McpManifest
}

func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) (any, error) {
	paramsMap := params.AsMap()
	cypherStr, ok := paramsMap["cypher"].(string)
	if !ok {
		return nil, fmt.Errorf("unable to get cast %s", paramsMap["cypher"])
	}

	if cypherStr == "" {
		return nil, fmt.Errorf("parameter 'cypher' must be a non-empty string")
	}

	// validate the cypher query before executing
	cf := t.classifier.Classify(cypherStr)
	if cf.Error != nil {
		return nil, cf.Error
	}

	if cf.Type == classifier.WriteQuery && t.ReadOnly {
		return nil, fmt.Errorf("this tool is read-only and cannot execute write queries")
	}

	config := neo4j.ExecuteQueryWithDatabase(t.Database)
	results, err := neo4j.ExecuteQuery(ctx, t.Driver, cypherStr, nil,
		neo4j.EagerResultTransformer, config)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query: %w", err)
	}

	var out []any
	keys := results.Keys
	records := results.Records
	for _, record := range records {
		vMap := make(map[string]any)
		for col, value := range record.Values {
			vMap[keys[col]] = value
		}
		out = append(out, vMap)
	}

	return out, nil
}

func (t Tool) ParseParams(data map[string]any, claimsMap map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.Parameters, data, claimsMap)
}

func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

func (t Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

func (t Tool) Authorized(verifiedAuthServices []string) bool {
	return tools.IsAuthorized(t.AuthRequired, verifiedAuthServices)
}
