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

package mongodb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/googleapis/genai-toolbox/internal/testutils"
	"github.com/googleapis/genai-toolbox/tests"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	MongoDbSourceKind   = "mongodb"
	MongoDbToolKind     = "mongodb-find"
	MongoDbUri          = os.Getenv("MONGODB_URI")
	MongoDbDatabase     = os.Getenv("MONGODB_DATABASE")
	ServiceAccountEmail = os.Getenv("SERVICE_ACCOUNT_EMAIL")
)

func getMongoDBVars(t *testing.T) map[string]any {
	switch "" {
	case MongoDbUri:
		t.Fatal("'MongoDbUri' not set")
	case MongoDbDatabase:
		t.Fatal("'MongoDbDatabase' not set")
	}
	return map[string]any{
		"kind": MongoDbSourceKind,
		"uri":  MongoDbUri,
	}
}

func initMongoDbDatabase(ctx context.Context, uri, database string) (*mongo.Database, error) {
	// Create a new mongodb Database
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to mongodb: %s", err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to mongodb: %s", err)
	}
	return client.Database(database), nil
}

func TestMongoDBToolEndpoints(t *testing.T) {
	sourceConfig := getMongoDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	database, err := initMongoDbDatabase(ctx, MongoDbUri, MongoDbDatabase)
	if err != nil {
		t.Fatalf("unable to create MongoDB connection: %s", err)
	}

	// set up data for param tool
	teardownDB := setupMongoDB(t, ctx, database)
	defer teardownDB(t)

	// Write config into a file and pass it to command
	toolsFile := getMongoDBToolsConfig(sourceConfig, MongoDbToolKind)

	cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, args...)
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	tests.RunToolGetTest(t)

	select1Want := `[{"_id":3,"id":3,"name":"Sid"}]`
	failInvocationWant := `invalid JSON input: missing colon after key `
	invokeParamWant := `[{"_id":5,"id":3,"name":"Alice"}]`
	invokeIdNullWant := `[{"_id":4,"id":4,"name":null}]`
	mcpInvokeParamWant := `{"jsonrpc":"2.0","id":"my-tool","result":{"content":[{"type":"text","text":"{\"_id\":5,\"id\":3,\"name\":\"Alice\"}"}]}}`
	nullString := "null"

	tests.RunToolInvokeTest(t, select1Want, invokeParamWant, invokeIdNullWant, nullString, true, true)
	tests.RunMCPToolCallMethod(t, mcpInvokeParamWant, failInvocationWant)

	delete1Want := "1"
	deleteManyWant := "2"
	RunToolDeleteInvokeTest(t, delete1Want, deleteManyWant)
	insert1Want := `["68666e1035bb36bf1b4d47fb"]`
	insertManyWant := `["68667a6436ec7d0363668db7","68667a6436ec7d0363668db8","68667a6436ec7d0363668db9"]`
	RunToolInsertInvokeTest(t, insert1Want, insertManyWant)
	update1Want := "1"
	updateManyWant := "[2,0,2]"
	RunToolUpdateInvokeTest(t, update1Want, updateManyWant)
	aggregate1Want := `[{"id":2}]`
	aggregateManyWant := `[{"id":500},{"id":501}]`
	RunToolAggregateInvokeTest(t, aggregate1Want, aggregateManyWant)

}

func RunToolDeleteInvokeTest(t *testing.T, delete1Want, deleteManyWant string) {
	// Test tool invoke endpoint
	invokeTcs := []struct {
		name          string
		api           string
		requestHeader map[string]string
		requestBody   io.Reader
		want          string
		isErr         bool
	}{
		{
			name:          "invoke my-delete-one-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-delete-one-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "id" : 100 }`)),
			want:          delete1Want,
			isErr:         false,
		},
		{
			name:          "invoke my-delete-many-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-delete-many-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "id" : 101 }`)),
			want:          deleteManyWant,
			isErr:         false,
		},
	}

	for _, tc := range invokeTcs {

		t.Run(tc.name, func(t *testing.T) {
			// Send Tool invocation request
			req, err := http.NewRequest(http.MethodPost, tc.api, tc.requestBody)
			if err != nil {
				t.Fatalf("unable to create request: %s", err)
			}
			req.Header.Add("Content-type", "application/json")
			for k, v := range tc.requestHeader {
				req.Header.Add(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unable to send request: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				if tc.isErr {
					return
				}
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
			}

			// Check response body
			var body map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&body)
			if err != nil {
				t.Fatalf("error parsing response body")
			}

			got, ok := body["result"].(string)
			if !ok {
				t.Fatalf("unable to find result in response body")
			}

			if got != tc.want {
				t.Fatalf("unexpected value: got %q, want %q", got, tc.want)
			}
		})
	}
}

func RunToolInsertInvokeTest(t *testing.T, insert1Want, insertManyWant string) {
	// Test tool invoke endpoint
	invokeTcs := []struct {
		name          string
		api           string
		requestHeader map[string]string
		requestBody   io.Reader
		want          string
		isErr         bool
	}{
		{
			name:          "invoke my-insert-one-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-insert-one-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "data" : "{ \"_id\": { \"$oid\": \"68666e1035bb36bf1b4d47fb\" },  \"id\" : 200 }" }"`)),
			want:          insert1Want,
			isErr:         false,
		},
		{
			name:          "invoke my-insert-many-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-insert-many-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "data" : "[{ \"_id\": { \"$oid\": \"68667a6436ec7d0363668db7\"} , \"id\" : 201 }, { \"_id\" : { \"$oid\": \"68667a6436ec7d0363668db8\"}, \"id\" : 202 }, { \"_id\": { \"$oid\": \"68667a6436ec7d0363668db9\"}, \"id\": 203 }]" }`)),
			want:          insertManyWant,
			isErr:         false,
		},
	}

	for _, tc := range invokeTcs {

		t.Run(tc.name, func(t *testing.T) {
			// Send Tool invocation request
			req, err := http.NewRequest(http.MethodPost, tc.api, tc.requestBody)
			if err != nil {
				t.Fatalf("unable to create request: %s", err)
			}
			req.Header.Add("Content-type", "application/json")
			for k, v := range tc.requestHeader {
				req.Header.Add(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unable to send request: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				if tc.isErr {
					return
				}
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
			}

			// Check response body
			var body map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&body)
			if err != nil {
				t.Fatalf("error parsing response body")
			}

			got, ok := body["result"].(string)
			if !ok {
				t.Fatalf("unable to find result in response body")
			}

			if got != tc.want {
				t.Fatalf("unexpected value: got %q, want %q", got, tc.want)
			}
		})
	}
}

func RunToolUpdateInvokeTest(t *testing.T, update1Want, updateManyWant string) {
	// Test tool invoke endpoint
	invokeTcs := []struct {
		name          string
		api           string
		requestHeader map[string]string
		requestBody   io.Reader
		want          string
		isErr         bool
	}{
		{
			name:          "invoke my-update-one-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-update-one-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "id": 300, "name": "Bob" }`)),
			want:          update1Want,
			isErr:         false,
		},
		{
			name:          "invoke my-update-many-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-update-many-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "id": 400, "name" : "Alice" }`)),
			want:          updateManyWant,
			isErr:         false,
		},
	}

	for _, tc := range invokeTcs {

		t.Run(tc.name, func(t *testing.T) {
			// Send Tool invocation request
			req, err := http.NewRequest(http.MethodPost, tc.api, tc.requestBody)
			if err != nil {
				t.Fatalf("unable to create request: %s", err)
			}
			req.Header.Add("Content-type", "application/json")
			for k, v := range tc.requestHeader {
				req.Header.Add(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unable to send request: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				if tc.isErr {
					return
				}
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
			}

			// Check response body
			var body map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&body)
			if err != nil {
				t.Fatalf("error parsing response body")
			}

			got, ok := body["result"].(string)
			if !ok {
				t.Fatalf("unable to find result in response body")
			}

			if got != tc.want {
				t.Fatalf("unexpected value: got %q, want %q", got, tc.want)
			}
		})
	}
}
func RunToolAggregateInvokeTest(t *testing.T, aggregate1Want string, aggregateManyWant string) {
	// Test tool invoke endpoint
	invokeTcs := []struct {
		name          string
		api           string
		requestHeader map[string]string
		requestBody   io.Reader
		want          string
		isErr         bool
	}{
		{
			name:          "invoke my-aggregate-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-aggregate-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "name": "Jane" }`)),
			want:          aggregate1Want,
			isErr:         false,
		},
		{
			name:          "invoke my-aggregate-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-aggregate-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "name" : "ToBeAggregated" }`)),
			want:          aggregateManyWant,
			isErr:         false,
		},
		{
			name:          "invoke my-read-only-aggregate-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-read-only-aggregate-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "name" : "ToBeAggregated" }`)),
			want:          "",
			isErr:         true,
		},
		{
			name:          "invoke my-read-write-aggregate-tool",
			api:           "http://127.0.0.1:5000/api/tool/my-read-write-aggregate-tool/invoke",
			requestHeader: map[string]string{},
			requestBody:   bytes.NewBuffer([]byte(`{ "name" : "ToBeAggregated" }`)),
			want:          "[]",
			isErr:         false,
		},
	}

	for _, tc := range invokeTcs {

		t.Run(tc.name, func(t *testing.T) {
			// Send Tool invocation request
			req, err := http.NewRequest(http.MethodPost, tc.api, tc.requestBody)
			if err != nil {
				t.Fatalf("unable to create request: %s", err)
			}
			req.Header.Add("Content-type", "application/json")
			for k, v := range tc.requestHeader {
				req.Header.Add(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unable to send request: %s", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				if tc.isErr {
					return
				}
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Fatalf("response status code is not 200, got %d: %s", resp.StatusCode, string(bodyBytes))
			}

			// Check response body
			var body map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&body)
			if err != nil {
				t.Fatalf("error parsing response body")
			}

			got, ok := body["result"].(string)
			if !ok {
				t.Fatalf("unable to find result in response body")
			}

			if got != tc.want {
				t.Fatalf("unexpected value: got %q, want %q", got, tc.want)
			}
		})
	}
}

func setupMongoDB(t *testing.T, ctx context.Context, database *mongo.Database) func(*testing.T) {
	collectionName := "test_collection"

	documents := []map[string]any{
		{"_id": 1, "id": 1, "name": "Alice", "email": ServiceAccountEmail},
		{"_id": 2, "id": 2, "name": "Jane"},
		{"_id": 3, "id": 3, "name": "Sid"},
		{"_id": 4, "id": 4, "name": nil},
		{"_id": 5, "id": 3, "name": "Alice", "email": "alice@gmail.com"},
		{"_id": 6, "id": 100, "name": "ToBeDeleted", "email": "bob@gmail.com"},
		{"_id": 7, "id": 101, "name": "ToBeDeleted", "email": "bob1@gmail.com"},
		{"_id": 8, "id": 101, "name": "ToBeDeleted", "email": "bob2@gmail.com"},
		{"_id": 9, "id": 300, "name": "ToBeUpdatedToBob", "email": "bob@gmail.com"},
		{"_id": 10, "id": 400, "name": "ToBeUpdatedToAlice", "email": "alice@gmail.com"},
		{"_id": 11, "id": 400, "name": "ToBeUpdatedToAlice", "email": "alice@gmail.com"},
		{"_id": 12, "id": 500, "name": "ToBeAggregated", "email": "agatha@gmail.com"},
		{"_id": 13, "id": 501, "name": "ToBeAggregated", "email": "agatha@gmail.com"},
	}
	for _, doc := range documents {
		_, err := database.Collection(collectionName).InsertOne(ctx, doc)
		if err != nil {
			t.Fatalf("unable to insert test data: %s", err)
		}
	}

	return func(t *testing.T) {
		// tear down test
		err := database.Collection(collectionName).Drop(ctx)
		if err != nil {
			t.Errorf("Teardown failed: %s", err)
		}
	}

}

func getMongoDBToolsConfig(sourceConfig map[string]any, toolKind string) map[string]any {
	toolsFile := map[string]any{
		"sources": map[string]any{
			"my-instance": sourceConfig,
		},
		"authServices": map[string]any{
			"my-google-auth": map[string]any{
				"kind":     "google",
				"clientId": tests.ClientId,
			},
		},
		"tools": map[string]any{
			"my-simple-tool": map[string]any{
				"kind":          "mongodb-find-one",
				"source":        "my-instance",
				"description":   "Simple tool to test end to end functionality.",
				"collection":    "test_collection",
				"filterPayload": `{ "_id" : 3 }`,
				"filterParams":  []any{},
				"database":      MongoDbDatabase,
			},
			"my-tool": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test invocation with params.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "id" : {{ .id }}, "name" : {{json .name }} }`,
				"filterParams": []map[string]any{
					{
						"name":        "id",
						"type":        "integer",
						"description": "user id",
					},
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"projectPayload": `{ "_id": 1, "id": 1, "name" : 1 }`,
				"database":       MongoDbDatabase,
			},
			"my-tool-by-id": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test invocation with params.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "id" : {{ .id }} }`,
				"filterParams": []map[string]any{
					{
						"name":        "id",
						"type":        "integer",
						"description": "user id",
					},
				},
				"projectPayload": `{ "_id": 1, "id": 1, "name" : 1 }`,
				"database":       MongoDbDatabase,
			},
			"my-tool-by-name": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test invocation with params.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "name" : {{ .name }} }`,
				"filterParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
						"required":    false,
					},
				},
				"projectPayload": `{ "_id": 1, "id": 1, "name" : 1 }`,
				"database":       MongoDbDatabase,
			},
			"my-array-tool": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test invocation with array.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "name": { "$in": {{json .nameArray}} }, "_id": 5 })`,
				"filterParams": []map[string]any{
					{
						"name":        "nameArray",
						"type":        "array",
						"description": "user names",
						"items": map[string]any{
							"name":        "username",
							"type":        "string",
							"description": "string item"},
					},
				},
				"projectPayload": `{ "_id": 1, "id": 1, "name" : 1 }`,
				"database":       MongoDbDatabase,
			},
			"my-auth-tool": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test authenticated parameters.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "email" : {{json .email }} }`,
				"filterParams": []map[string]any{
					{
						"name":        "email",
						"type":        "string",
						"description": "user email",
						"authServices": []map[string]string{
							{
								"name":  "my-google-auth",
								"field": "email",
							},
						},
					},
				},
				"projectPayload": `{ "_id": 0, "name" : 1 }`,
				"database":       MongoDbDatabase,
			},
			"my-auth-required-tool": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test auth required invocation.",
				"authRequired": []string{
					"my-google-auth",
				},
				"collection":    "test_collection",
				"filterPayload": `{ "_id": 3, "id": 3 }`,
				"filterParams":  []any{},
				"database":      MongoDbDatabase,
			},
			"my-fail-tool": map[string]any{
				"kind":          toolKind,
				"source":        "my-instance",
				"description":   "Tool to test statement with incorrect syntax.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "id" ; 1 }"}`,
				"filterParams":  []any{},
				"database":      MongoDbDatabase,
			},
			"my-delete-one-tool": map[string]any{
				"kind":          "mongodb-delete-one",
				"source":        "my-instance",
				"description":   "Tool to test deleting an entry.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "id" : 100 }"}`,
				"filterParams":  []any{},
				"database":      MongoDbDatabase,
			},
			"my-delete-many-tool": map[string]any{
				"kind":          "mongodb-delete-many",
				"source":        "my-instance",
				"description":   "Tool to test deleting multiple entries.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"filterPayload": `{ "id" : 101 }"}`,
				"filterParams":  []any{},
				"database":      MongoDbDatabase,
			},
			"my-insert-one-tool": map[string]any{
				"kind":         "mongodb-insert-one",
				"source":       "my-instance",
				"description":  "Tool to test inserting an entry.",
				"authRequired": []string{},
				"collection":   "test_collection",
				"canonical":    true,
				"database":     MongoDbDatabase,
			},
			"my-insert-many-tool": map[string]any{
				"kind":         "mongodb-insert-many",
				"source":       "my-instance",
				"description":  "Tool to test inserting multiple entries.",
				"authRequired": []string{},
				"collection":   "test_collection",
				"canonical":    true,
				"database":     MongoDbDatabase,
			},
			"my-update-one-tool": map[string]any{
				"kind":          "mongodb-update-one",
				"source":        "my-instance",
				"description":   "Tool to test updating an entry.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"canonical":     true,
				"filterPayload": `{ "id" : 300 }`,
				"filterParams":  []any{},
				"updatePayload": `{ "$set" : { "name": {{json .name}} } }`,
				"updateParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"database": MongoDbDatabase,
			},
			"my-update-many-tool": map[string]any{
				"kind":          "mongodb-update-many",
				"source":        "my-instance",
				"description":   "Tool to test updating multiple entries.",
				"authRequired":  []string{},
				"collection":    "test_collection",
				"canonical":     true,
				"filterPayload": `{ "id" : {{ .id }} }`,
				"filterParams": []map[string]any{
					{
						"name":        "id",
						"type":        "integer",
						"description": "id",
					},
				},
				"updatePayload": `{ "$set" : { "name": {{json .name}} } }`,
				"updateParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"database": MongoDbDatabase,
			},
			"my-aggregate-tool": map[string]any{
				"kind":            "mongodb-aggregate",
				"source":          "my-instance",
				"description":     "Tool to test an aggregation.",
				"authRequired":    []string{},
				"collection":      "test_collection",
				"canonical":       true,
				"pipelinePayload": `[{ "$match" : { "name": {{json .name}} } }, { "$project" : { "id" : 1, "_id" : 0 }}]`,
				"pipelineParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"database": MongoDbDatabase,
			},
			"my-read-only-aggregate-tool": map[string]any{
				"kind":            "mongodb-aggregate",
				"source":          "my-instance",
				"description":     "Tool to test an aggregation.",
				"authRequired":    []string{},
				"collection":      "test_collection",
				"canonical":       true,
				"readOnly":        true,
				"pipelinePayload": `[{ "$match" : { "name": {{json .name}} } }, { "$out" : "target_collection" }]`,
				"pipelineParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"database": MongoDbDatabase,
			},
			"my-read-write-aggregate-tool": map[string]any{
				"kind":            "mongodb-aggregate",
				"source":          "my-instance",
				"description":     "Tool to test an aggregation.",
				"authRequired":    []string{},
				"collection":      "test_collection",
				"canonical":       true,
				"readOnly":        false,
				"pipelinePayload": `[{ "$match" : { "name": {{json .name}} } }, { "$out" : "target_collection" }]`,
				"pipelineParams": []map[string]any{
					{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
				"database": MongoDbDatabase,
			},
		},
	}

	return toolsFile

}
