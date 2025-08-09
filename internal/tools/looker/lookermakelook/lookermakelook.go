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
package lookermakelook

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	yaml "github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	lookersrc "github.com/googleapis/genai-toolbox/internal/sources/looker"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/tools/looker/lookercommon"
	"github.com/googleapis/genai-toolbox/internal/util"

	"github.com/looker-open-source/sdk-codegen/go/rtl"
	v4 "github.com/looker-open-source/sdk-codegen/go/sdk/v4"
)

const kind string = "looker-make-look"

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

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description" validate:"required"`
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
	s, ok := rawS.(*lookersrc.Source)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be `looker`", kind)
	}

	parameters := lookercommon.GetQueryParameters()

	titleParameter := tools.NewStringParameter("title", "The title of the Look")
	parameters = append(parameters, titleParameter)
	descParameter := tools.NewStringParameterWithDefault("description", "", "The description of the Look")
	parameters = append(parameters, descParameter)
	vizParameter := tools.NewMapParameterWithDefault("vis_config",
		map[string]any{},
		"The visualization config for the query",
		"",
	)
	parameters = append(parameters, vizParameter)

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: cfg.Description,
		InputSchema: parameters.McpManifest(),
	}

	// finish tool setup
	return Tool{
		Name:         cfg.Name,
		Kind:         kind,
		Parameters:   parameters,
		AuthRequired: cfg.AuthRequired,
		Client:       s.Client,
		ApiSettings:  s.ApiSettings,
		manifest: tools.Manifest{
			Description:  cfg.Description,
			Parameters:   parameters.Manifest(),
			AuthRequired: cfg.AuthRequired,
		},
		mcpManifest: mcpManifest,
	}, nil
}

// validate interface
var _ tools.Tool = Tool{}

type Tool struct {
	Name         string `yaml:"name"`
	Kind         string `yaml:"kind"`
	Client       *v4.LookerSDK
	ApiSettings  *rtl.ApiSettings
	AuthRequired []string         `yaml:"authRequired"`
	Parameters   tools.Parameters `yaml:"parameters"`
	manifest     tools.Manifest
	mcpManifest  tools.McpManifest
}

func (t Tool) Invoke(ctx context.Context, params tools.ParamValues) (any, error) {
	logger, err := util.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get logger from ctx: %s", err)
	}
	logger.DebugContext(ctx, "params = ", params)
	wq, err := lookercommon.ProcessQueryArgs(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("error building query request: %w", err)
	}

	mrespFields := "id,personal_folder_id"
	mresp, err := t.Client.Me(mrespFields, t.ApiSettings)
	if err != nil {
		return nil, fmt.Errorf("error making me request: %s", err)
	}

	paramsMap := params.AsMap()
	title := paramsMap["title"].(string)
	description := paramsMap["description"].(string)

	looks, err := t.Client.FolderLooks(*mresp.PersonalFolderId, "title", t.ApiSettings)
	if err != nil {
		return nil, fmt.Errorf("error getting existing looks in folder: %s", err)
	}

	lookTitles := []string{}
	for _, look := range looks {
		lookTitles = append(lookTitles, *look.Title)
	}
	if slices.Contains(lookTitles, title) {
		lt, _ := json.Marshal(lookTitles)
		return nil, fmt.Errorf("title %s already used in user's folder. Currently used titles are %v. Make the call again with a unique title", title, string(lt))
	}

	visConfig := paramsMap["vis_config"].(map[string]any)
	wq.VisConfig = &visConfig

	qrespFields := "id"
	qresp, err := t.Client.CreateQuery(*wq, qrespFields, t.ApiSettings)
	if err != nil {
		return nil, fmt.Errorf("error making create query request: %s", err)
	}

	wlwq := v4.WriteLookWithQuery{
		Title:       &title,
		UserId:      mresp.Id,
		Description: &description,
		QueryId:     qresp.Id,
		FolderId:    mresp.PersonalFolderId,
	}
	resp, err := t.Client.CreateLook(wlwq, "", t.ApiSettings)
	if err != nil {
		return nil, fmt.Errorf("error making create look request: %s", err)
	}
	logger.DebugContext(ctx, "resp = %v", resp)

	setting, err := t.Client.GetSetting("host_url", t.ApiSettings)
	if err != nil {
		logger.ErrorContext(ctx, "error getting settings: %s", err)
	}

	data := make(map[string]any)
	if resp.Id != nil {
		data["id"] = *resp.Id
	}
	if resp.ShortUrl != nil {
		if setting.HostUrl != nil {
			data["short_url"] = *setting.HostUrl + *resp.ShortUrl
		} else {
			data["short_url"] = *resp.ShortUrl
		}
	}
	logger.DebugContext(ctx, "data = %v", data)

	return data, nil
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
