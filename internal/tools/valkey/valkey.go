// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package valkey

import (
	"context"
	"fmt"

	yaml "github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	valkeysrc "github.com/googleapis/genai-toolbox/internal/sources/valkey"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/valkey-io/valkey-go"
)

const kind string = "valkey"

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
	ValkeyClient() valkey.Client
}

// validate compatible sources are still compatible
var _ compatibleSource = &valkeysrc.Source{}

var compatibleSources = [...]string{valkeysrc.SourceKind, valkeysrc.SourceKind}

type Config struct {
	Name         string           `yaml:"name" validate:"required"`
	Kind         string           `yaml:"kind" validate:"required"`
	Source       string           `yaml:"source" validate:"required"`
	Description  string           `yaml:"description" validate:"required"`
	Commands     [][]string       `yaml:"commands" validate:"required"`
	AuthRequired []string         `yaml:"authRequired"`
	Parameters   tools.Parameters `yaml:"parameters"`
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

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: cfg.Description,
		InputSchema: cfg.Parameters.McpManifest(),
	}

	// finish tool setup
	t := Tool{
		Name:         cfg.Name,
		Kind:         kind,
		Parameters:   cfg.Parameters,
		Commands:     cfg.Commands,
		AuthRequired: cfg.AuthRequired,
		Client:       s.ValkeyClient(),
		manifest:     tools.Manifest{Description: cfg.Description, Parameters: cfg.Parameters.Manifest(), AuthRequired: cfg.AuthRequired},
		mcpManifest:  mcpManifest,
	}
	return t, nil
}

// validate interface
var _ tools.Tool = Tool{}

type Tool struct {
	Name         string           `yaml:"name"`
	Kind         string           `yaml:"kind"`
	AuthRequired []string         `yaml:"authRequired"`
	Parameters   tools.Parameters `yaml:"parameters"`

	Client      valkey.Client
	Commands    [][]string
	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) (any, error) {
	// Replace parameters
	commands, err := replaceCommandsParams(t.Commands, t.Parameters, params)
	if err != nil {
		return nil, fmt.Errorf("error replacing commands' parameters: %s", err)
	}

	// Build commands
	builtCmds := make(valkey.Commands, len(commands))

	for i, cmd := range commands {
		builtCmds[i] = t.Client.B().Arbitrary(cmd...).Build()
	}

	if len(builtCmds) == 0 {
		return nil, fmt.Errorf("no valid commands were built to execute")
	}

	// Execute commands
	responses := t.Client.DoMulti(ctx, builtCmds...)

	// Parse responses
	out := make([]any, len(t.Commands))
	for i, resp := range responses {
		if err := resp.Error(); err != nil {
			// Add error from each command to `errSum`
			out[i] = fmt.Sprintf("error from executing command at index %d: %s", i, err)
			continue
		}
		val, err := resp.ToAny()
		if err != nil {
			out[i] = fmt.Sprintf("error parsing response: %s", err)
			continue
		}
		out[i] = val
	}

	return out, nil
}

// replaceCommandsParams is a helper function to replace parameters in the commands
func replaceCommandsParams(commands [][]string, params tools.Parameters, paramValues tools.ParamValues) ([][]string, error) {
	paramMap := paramValues.AsMapWithDollarPrefix()
	typeMap := make(map[string]string, len(params))
	for _, p := range params {
		placeholder := "$" + p.GetName()
		typeMap[placeholder] = p.GetType()
	}

	newCommands := make([][]string, len(commands))
	for i, cmd := range commands {
		newCmd := make([]string, 0)
		for _, part := range cmd {
			v, ok := paramMap[part]
			if !ok {
				// Command part is not a Parameter placeholder
				newCmd = append(newCmd, part)
				continue
			}
			if typeMap[part] == "array" {
				for _, item := range v.([]any) {
					// Nested arrays will only be expanded once
					// e.g., [A, [B, C]]  --> ["A", "[B C]"]
					newCmd = append(newCmd, fmt.Sprintf("%s", item))
				}
				continue
			}
			newCmd = append(newCmd, fmt.Sprintf("%s", v))
		}
		newCommands[i] = newCmd
	}
	return newCommands, nil
}

func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.Parameters, data, claims)
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
