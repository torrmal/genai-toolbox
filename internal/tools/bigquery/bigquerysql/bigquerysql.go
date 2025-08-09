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

package bigquerysql

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	bigqueryapi "cloud.google.com/go/bigquery"
	yaml "github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	bigqueryds "github.com/googleapis/genai-toolbox/internal/sources/bigquery"
	"github.com/googleapis/genai-toolbox/internal/tools"
	bigqueryrestapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
)

const kind string = "bigquery-sql"

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
	BigQueryClient() *bigqueryapi.Client
	BigQueryRestService() *bigqueryrestapi.Service
}

// validate compatible sources are still compatible
var _ compatibleSource = &bigqueryds.Source{}

var compatibleSources = [...]string{bigqueryds.SourceKind}

type Config struct {
	Name               string           `yaml:"name" validate:"required"`
	Kind               string           `yaml:"kind" validate:"required"`
	Source             string           `yaml:"source" validate:"required"`
	Description        string           `yaml:"description" validate:"required"`
	Statement          string           `yaml:"statement" validate:"required"`
	AuthRequired       []string         `yaml:"authRequired"`
	Parameters         tools.Parameters `yaml:"parameters"`
	TemplateParameters tools.Parameters `yaml:"templateParameters"`
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
	s, ok := rawS.(compatibleSource)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be one of %q", kind, compatibleSources)
	}

	allParameters, paramManifest, paramMcpManifest := tools.ProcessParameters(cfg.TemplateParameters, cfg.Parameters)

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: cfg.Description,
		InputSchema: paramMcpManifest,
	}

	// finish tool setup
	t := Tool{
		Name:               cfg.Name,
		Kind:               kind,
		Parameters:         cfg.Parameters,
		TemplateParameters: cfg.TemplateParameters,
		AllParams:          allParameters,
		Statement:          cfg.Statement,
		AuthRequired:       cfg.AuthRequired,
		Client:             s.BigQueryClient(),
		RestService:        s.BigQueryRestService(),
		manifest:           tools.Manifest{Description: cfg.Description, Parameters: paramManifest, AuthRequired: cfg.AuthRequired},
		mcpManifest:        mcpManifest,
	}
	return t, nil
}

// validate interface
var _ tools.Tool = Tool{}

type Tool struct {
	Name               string           `yaml:"name"`
	Kind               string           `yaml:"kind"`
	AuthRequired       []string         `yaml:"authRequired"`
	Parameters         tools.Parameters `yaml:"parameters"`
	TemplateParameters tools.Parameters `yaml:"templateParameters"`
	AllParams          tools.Parameters `yaml:"allParams"`
	Statement          string
	Client             *bigqueryapi.Client
	RestService        *bigqueryrestapi.Service
	manifest           tools.Manifest
	mcpManifest        tools.McpManifest
}

func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) (any, error) {
	highLevelParams := make([]bigqueryapi.QueryParameter, 0, len(t.Parameters))
	lowLevelParams := make([]*bigqueryrestapi.QueryParameter, 0, len(t.Parameters))

	paramsMap := params.AsMap()
	newStatement, err := tools.ResolveTemplateParams(t.TemplateParameters, t.Statement, paramsMap)
	if err != nil {
		return nil, fmt.Errorf("unable to extract template params %w", err)
	}

	for _, p := range t.Parameters {
		name := p.GetName()
		value := paramsMap[name]

		// This block for converting []any to typed slices is still necessary and correct.
		if arrayParam, ok := p.(*tools.ArrayParameter); ok {
			arrayParamValue, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("unable to convert parameter `%s` to []any", name)
			}
			itemType := arrayParam.GetItems().GetType()
			var err error
			value, err = tools.ConvertAnySliceToTyped(arrayParamValue, itemType)
			if err != nil {
				return nil, fmt.Errorf("unable to convert parameter `%s` from []any to typed slice: %w", name, err)
			}
		}

		// Determine if the parameter is named or positional for the high-level client.
		var paramNameForHighLevel string
		if strings.Contains(newStatement, "@"+name) {
			paramNameForHighLevel = name
		}

		// 1. Create the high-level parameter for the final query execution.
		highLevelParams = append(highLevelParams, bigqueryapi.QueryParameter{
			Name:  paramNameForHighLevel,
			Value: value,
		})

		// 2. Create the low-level parameter for the dry run, using the defined type from `p`.
		lowLevelParam := &bigqueryrestapi.QueryParameter{
			Name:           paramNameForHighLevel,
			ParameterType:  &bigqueryrestapi.QueryParameterType{},
			ParameterValue: &bigqueryrestapi.QueryParameterValue{},
		}

		if arrayParam, ok := p.(*tools.ArrayParameter); ok {
			// Handle array types based on their defined item type.
			lowLevelParam.ParameterType.Type = "ARRAY"
			itemType, err := BQTypeStringFromToolType(arrayParam.GetItems().GetType())
			if err != nil {
				return nil, err
			}
			lowLevelParam.ParameterType.ArrayType = &bigqueryrestapi.QueryParameterType{Type: itemType}

			// Build the array values.
			sliceVal := reflect.ValueOf(value)
			arrayValues := make([]*bigqueryrestapi.QueryParameterValue, sliceVal.Len())
			for i := 0; i < sliceVal.Len(); i++ {
				arrayValues[i] = &bigqueryrestapi.QueryParameterValue{
					Value: fmt.Sprintf("%v", sliceVal.Index(i).Interface()),
				}
			}
			lowLevelParam.ParameterValue.ArrayValues = arrayValues
		} else {
			// Handle scalar types based on their defined type.
			bqType, err := BQTypeStringFromToolType(p.GetType())
			if err != nil {
				return nil, err
			}
			lowLevelParam.ParameterType.Type = bqType
			lowLevelParam.ParameterValue.Value = fmt.Sprintf("%v", value)
		}
		lowLevelParams = append(lowLevelParams, lowLevelParam)
	}

	query := t.Client.Query(newStatement)
	query.Parameters = highLevelParams
	query.Location = t.Client.Location

	dryRunJob, err := dryRunQuery(ctx, t.RestService, t.Client.Project(), t.Client.Location, newStatement, lowLevelParams, query.ConnectionProperties)
	if err != nil {
		// This is a fallback check in case the switch logic was bypassed.
		return nil, fmt.Errorf("final query validation failed: %w", err)
	}
	statementType := dryRunJob.Statistics.Query.StatementType

	// This block handles SELECT statements, which return a row set.
	// We iterate through the results, convert each row into a map of
	// column names to values, and return the collection of rows.
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query: %w", err)
	}

	var out []any
	for {
		var row map[string]bigqueryapi.Value
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("unable to iterate through query results: %w", err)
		}
		vMap := make(map[string]any)
		for key, value := range row {
			vMap[key] = value
		}
		out = append(out, vMap)
	}
	// If the query returned any rows, return them directly.
	if len(out) > 0 {
		return out, nil
	}

	// This handles the standard case for a SELECT query that successfully
	// executes but returns zero rows.
	if statementType == "SELECT" {
		return "The query returned 0 rows.", nil
	}
	// This is the fallback for a successful query that doesn't return content.
	// In most cases, this will be for DML/DDL statements like INSERT, UPDATE, CREATE, etc.
	// However, it is also possible that this was a query that was expected to return rows
	// but returned none, a case that we cannot distinguish here.
	return "Query executed successfully and returned no content.", nil
}

func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.AllParams, data, claims)
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

func BQTypeStringFromToolType(toolType string) (string, error) {
	switch toolType {
	case "string":
		return "STRING", nil
	case "integer":
		return "INT64", nil
	case "float":
		return "FLOAT64", nil
	case "boolean":
		return "BOOL", nil
	// Note: 'array' is handled separately as it has a nested item type.
	default:
		return "", fmt.Errorf("unsupported tool parameter type for BigQuery: %s", toolType)
	}
}

func dryRunQuery(
	ctx context.Context,
	restService *bigqueryrestapi.Service,
	projectID string,
	location string,
	sql string,
	params []*bigqueryrestapi.QueryParameter,
	connProps []*bigqueryapi.ConnectionProperty,
) (*bigqueryrestapi.Job, error) {
	useLegacySql := false

	restConnProps := make([]*bigqueryrestapi.ConnectionProperty, len(connProps))
	for i, prop := range connProps {
		restConnProps[i] = &bigqueryrestapi.ConnectionProperty{Key: prop.Key, Value: prop.Value}
	}

	jobToInsert := &bigqueryrestapi.Job{
		JobReference: &bigqueryrestapi.JobReference{
			ProjectId: projectID,
			Location:  location,
		},
		Configuration: &bigqueryrestapi.JobConfiguration{
			DryRun: true,
			Query: &bigqueryrestapi.JobConfigurationQuery{
				Query:                sql,
				UseLegacySql:         &useLegacySql,
				ConnectionProperties: restConnProps,
				QueryParameters:      params,
			},
		},
	}

	insertResponse, err := restService.Jobs.Insert(projectID, jobToInsert).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to insert dry run job: %w", err)
	}
	return insertResponse, nil
}
