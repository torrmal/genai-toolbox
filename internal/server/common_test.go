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

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/googleapis/genai-toolbox/internal/log"
	"github.com/googleapis/genai-toolbox/internal/telemetry"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

// fakeVersionString is used as a temporary version string in tests
const fakeVersionString = "0.0.0"

var _ tools.Tool = &MockTool{}

// MockTool is used to mock tools in tests
type MockTool struct {
	Name        string
	Description string
	Params      []tools.Parameter
	manifest    tools.Manifest
}

func (t MockTool) Invoke(context.Context, tools.ParamValues) (any, error) {
	mock := []any{t.Name}
	return mock, nil
}

// claims is a map of user info decoded from an auth token
func (t MockTool) ParseParams(data map[string]any, claimsMap map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.Params, data, claimsMap)
}

func (t MockTool) Manifest() tools.Manifest {
	pMs := make([]tools.ParameterManifest, 0, len(t.Params))
	for _, p := range t.Params {
		pMs = append(pMs, p.Manifest())
	}
	return tools.Manifest{Description: t.Description, Parameters: pMs}
}
func (t MockTool) Authorized(verifiedAuthServices []string) bool {
	return true
}

func (t MockTool) McpManifest() tools.McpManifest {
	properties := make(map[string]tools.ParameterMcpManifest)
	required := make([]string, 0)

	for _, p := range t.Params {
		name := p.GetName()
		properties[name] = p.McpManifest()
		required = append(required, name)
	}

	toolsSchema := tools.McpToolsSchema{
		Type:       "object",
		Properties: properties,
		Required:   required,
	}

	return tools.McpManifest{
		Name:        t.Name,
		Description: t.Description,
		InputSchema: toolsSchema,
	}
}

var tool1 = MockTool{
	Name:   "no_params",
	Params: []tools.Parameter{},
}

var tool2 = MockTool{
	Name: "some_params",
	Params: tools.Parameters{
		tools.NewIntParameter("param1", "This is the first parameter."),
		tools.NewIntParameter("param2", "This is the second parameter."),
	},
}

var tool3 = MockTool{
	Name:        "array_param",
	Description: "some description",
	Params: tools.Parameters{
		tools.NewArrayParameter("my_array", "this param is an array of strings", tools.NewStringParameter("my_string", "string item")),
	},
}

// setUpResources setups resources to test against
func setUpResources(t *testing.T, mockTools []MockTool) (map[string]tools.Tool, map[string]tools.Toolset) {
	toolsMap := make(map[string]tools.Tool)
	var allTools []string
	for _, tool := range mockTools {
		tool.manifest = tool.Manifest()
		toolsMap[tool.Name] = tool
		allTools = append(allTools, tool.Name)
	}

	toolsets := make(map[string]tools.Toolset)
	for name, l := range map[string][]string{
		"":           allTools,
		"tool1_only": {allTools[0]},
		"tool2_only": {allTools[1]},
	} {
		tc := tools.ToolsetConfig{Name: name, ToolNames: l}
		m, err := tc.Initialize(fakeVersionString, toolsMap)
		if err != nil {
			t.Fatalf("unable to initialize toolset %q: %s", name, err)
		}
		toolsets[name] = m
	}
	return toolsMap, toolsets
}

// setUpServer create a new server with tools and toolsets that are given
func setUpServer(t *testing.T, router string, tools map[string]tools.Tool, toolsets map[string]tools.Toolset) (chi.Router, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	testLogger, err := log.NewStdLogger(os.Stdout, os.Stderr, "info")
	if err != nil {
		t.Fatalf("unable to initialize logger: %s", err)
	}

	otelShutdown, err := telemetry.SetupOTel(ctx, fakeVersionString, "", false, "toolbox")
	if err != nil {
		t.Fatalf("unable to setup otel: %s", err)
	}

	instrumentation, err := telemetry.CreateTelemetryInstrumentation(fakeVersionString)
	if err != nil {
		t.Fatalf("unable to create custom metrics: %s", err)
	}

	sseManager := newSseManager(ctx)

	resourceManager := NewResourceManager(nil, nil, tools, toolsets)

	server := Server{
		version:         fakeVersionString,
		logger:          testLogger,
		instrumentation: instrumentation,
		sseManager:      sseManager,
		ResourceMgr:     resourceManager,
	}

	var r chi.Router
	switch router {
	case "api":
		r, err = apiRouter(&server)
		if err != nil {
			t.Fatalf("unable to initialize api router: %s", err)
		}
	case "mcp":
		r, err = mcpRouter(&server)
		if err != nil {
			t.Fatalf("unable to initialize mcp router: %s", err)
		}
	default:
		t.Fatalf("unknown router")
	}
	shutdown := func() {
		// cancel context
		cancel()
		// shutdown otel
		err := otelShutdown(ctx)
		if err != nil {
			t.Fatalf("error shutting down OpenTelemetry: %s", err)
		}
	}

	return r, shutdown
}

func runServer(r chi.Router, tls bool) *httptest.Server {
	var ts *httptest.Server
	if tls {
		ts = httptest.NewTLSServer(r)
	} else {
		ts = httptest.NewServer(r)
	}
	return ts
}

func runRequest(ts *httptest.Server, method, path string, body io.Reader, header map[string]string) (*http.Response, []byte, error) {
	req, err := http.NewRequest(method, ts.URL+path, body)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to send request: %w", err)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read request body: %w", err)
	}
	defer resp.Body.Close()

	return resp, respBody, nil
}
