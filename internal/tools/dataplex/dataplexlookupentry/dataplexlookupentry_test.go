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

package dataplexlookupentry_test

import (
	"testing"

	yaml "github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/genai-toolbox/internal/server"
	"github.com/googleapis/genai-toolbox/internal/testutils"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/tools/dataplex/dataplexlookupentry"
)

func TestParseFromYamlDataplexLookupEntry(t *testing.T) {
	ctx, err := testutils.ContextWithNewLogger()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tcs := []struct {
		desc string
		in   string
		want server.ToolConfigs
	}{
		{
			desc: "basic example",
			in: `
			tools:
				example_tool:
					kind: dataplex-lookup-entry
					source: my-instance
					description: some description
			`,
			want: server.ToolConfigs{
				"example_tool": dataplexlookupentry.Config{
					Name:         "example_tool",
					Kind:         "dataplex-lookup-entry",
					Source:       "my-instance",
					Description:  "some description",
					AuthRequired: []string{},
				},
			},
		},
		{
			desc: "advanced example",
			in: `
			tools:
				example_tool:
					kind: dataplex-lookup-entry
					source: my-instance
					description: some description
					parameters:
						- name: name
							type: string
							description: some name description
						- name: view
							type: string
							description: some view description
						- name: aspectTypes
							type: array
							description: some aspect types description
							default: []
							items: 
								name: aspectType
								type: string
								description: some aspect type description
						- name: entry
							type: string
							description: some entry description
			`,
			want: server.ToolConfigs{
				"example_tool": dataplexlookupentry.Config{
					Name:         "example_tool",
					Kind:         "dataplex-lookup-entry",
					Source:       "my-instance",
					Description:  "some description",
					AuthRequired: []string{},
					Parameters: []tools.Parameter{
						tools.NewStringParameter("name", "some name description"),
						tools.NewStringParameter("view", "some view description"),
						tools.NewArrayParameterWithDefault("aspectTypes", []any{}, "some aspect types description", tools.NewStringParameter("aspectType", "some aspect type description")),
						tools.NewStringParameter("entry", "some entry description"),
					},
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			got := struct {
				Tools server.ToolConfigs `yaml:"tools"`
			}{}
			// Parse contents
			err := yaml.UnmarshalContext(ctx, testutils.FormatYaml(tc.in), &got)
			if err != nil {
				t.Fatalf("unable to unmarshal: %s", err)
			}
			if diff := cmp.Diff(tc.want, got.Tools); diff != "" {
				t.Fatalf("incorrect parse: diff %v", diff)
			}
		})
	}

}
