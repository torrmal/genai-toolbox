// Copyright 2024 Google LLC
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

package couchbase

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/googleapis/genai-toolbox/internal/testutils"
	"github.com/googleapis/genai-toolbox/tests"
)

const (
	couchbaseSourceKind = "couchbase"
	couchbaseToolKind   = "couchbase-sql"
)

var (
	couchbaseConnection = os.Getenv("COUCHBASE_CONNECTION")
	couchbaseBucket     = os.Getenv("COUCHBASE_BUCKET")
	couchbaseScope      = os.Getenv("COUCHBASE_SCOPE")
	couchbaseUser       = os.Getenv("COUCHBASE_USER")
	couchbasePass       = os.Getenv("COUCHBASE_PASS")
)

// getCouchbaseVars validates and returns Couchbase configuration variables
func getCouchbaseVars(t *testing.T) map[string]any {
	switch "" {
	case couchbaseConnection:
		t.Fatal("'COUCHBASE_CONNECTION' not set")
	case couchbaseBucket:
		t.Fatal("'COUCHBASE_BUCKET' not set")
	case couchbaseScope:
		t.Fatal("'COUCHBASE_SCOPE' not set")
	case couchbaseUser:
		t.Fatal("'COUCHBASE_USER' not set")
	case couchbasePass:
		t.Fatal("'COUCHBASE_PASS' not set")
	}

	return map[string]any{
		"kind":                 couchbaseSourceKind,
		"connectionString":     couchbaseConnection,
		"bucket":               couchbaseBucket,
		"scope":                couchbaseScope,
		"username":             couchbaseUser,
		"password":             couchbasePass,
		"queryScanConsistency": 2,
	}
}

// initCouchbaseCluster initializes a connection to the Couchbase cluster
func initCouchbaseCluster(connectionString, username, password string) (*gocb.Cluster, error) {
	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}

	cluster, err := gocb.Connect(connectionString, opts)
	if err != nil {
		return nil, fmt.Errorf("gocb.Connect: %w", err)
	}
	return cluster, nil
}

func TestCouchbaseToolEndpoints(t *testing.T) {
	sourceConfig := getCouchbaseVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	cluster, err := initCouchbaseCluster(couchbaseConnection, couchbaseUser, couchbasePass)
	if err != nil {
		t.Fatalf("unable to create Couchbase connection: %s", err)
	}
	defer cluster.Close(nil)

	// Create collection names with UUID
	collectionNameParam := "param_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	collectionNameAuth := "auth_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	collectionNameTemplateParam := "template_param_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// Set up data for param tool
	paramToolStatement, idParamToolStmt, nameParamToolStmt, arrayToolStatement, paramTestParams := getCouchbaseParamToolInfo(collectionNameParam)
	teardownCollection1 := setupCouchbaseCollection(t, ctx, cluster, couchbaseBucket, couchbaseScope, collectionNameParam, paramTestParams)
	defer teardownCollection1(t)

	// Set up data for auth tool
	authToolStatement, authTestParams := getCouchbaseAuthToolInfo(collectionNameAuth)
	teardownCollection2 := setupCouchbaseCollection(t, ctx, cluster, couchbaseBucket, couchbaseScope, collectionNameAuth, authTestParams)
	defer teardownCollection2(t)

	// Setup up table for template param tool
	tmplSelectCombined, tmplSelectFilterCombined, tmplSelectAll, params3 := getCouchbaseTemplateParamToolInfo()
	teardownCollection3 := setupCouchbaseCollection(t, ctx, cluster, couchbaseBucket, couchbaseScope, collectionNameTemplateParam, params3)
	defer teardownCollection3(t)

	// Write config into a file and pass it to command
	toolsFile := tests.GetToolsConfig(sourceConfig, couchbaseToolKind, paramToolStatement, idParamToolStmt, nameParamToolStmt, arrayToolStatement, authToolStatement)
	toolsFile = tests.AddTemplateParamConfig(t, toolsFile, couchbaseToolKind, tmplSelectCombined, tmplSelectFilterCombined, tmplSelectAll)

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

	select1Want := "[{\"$1\":1}]"
	failMcpInvocationWant := "{\"jsonrpc\":\"2.0\",\"id\":\"invoke-fail-tool\",\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"unable to execute query: parsing failure | {\\\"statement\\\":\\\"SELEC 1;\\\""

	invokeParamWant, invokeIdNullWant, nullWant, mcpInvokeParamWant := tests.GetNonSpannerInvokeParamWant()
	tests.RunToolInvokeTest(t, select1Want, invokeParamWant, invokeIdNullWant, nullWant, true, true)
	tests.RunMCPToolCallMethod(t, mcpInvokeParamWant, failMcpInvocationWant)

	templateParamTestConfig := tests.NewTemplateParameterTestConfig(
		tests.WithIgnoreDdl(),
		tests.WithIgnoreInsert(),
		tests.WithSelect1Want("[{\"age\":21,\"id\":1,\"name\":\"Alex\"}]"),
		tests.WithSelectAllWant("[{\"age\":21,\"id\":1,\"name\":\"Alex\"},{\"age\":100,\"id\":2,\"name\":\"Alice\"}]"),
	)
	tests.RunToolInvokeWithTemplateParameters(t, collectionNameTemplateParam, templateParamTestConfig)
}

// setupCouchbaseCollection creates a scope and collection and inserts test data
func setupCouchbaseCollection(t *testing.T, ctx context.Context, cluster *gocb.Cluster,
	bucketName, scopeName, collectionName string, params []map[string]any) func(t *testing.T) {

	// Get bucket reference
	bucket := cluster.Bucket(bucketName)

	// Wait for bucket to be ready
	err := bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		t.Fatalf("failed to connect to bucket: %v", err)
	}

	// Create scope if it doesn't exist
	bucketMgr := bucket.CollectionsV2()
	err = bucketMgr.CreateScope(scopeName, nil)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Logf("failed to create scope (might already exist): %v", err)
	}

	// Create a collection if it doesn't exist
	err = bucketMgr.CreateCollection(scopeName, collectionName, nil, nil)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("failed to create collection: %v", err)
	}

	// Get a reference to the collection
	collection := bucket.Scope(scopeName).Collection(collectionName)

	// Create primary index if it doesn't exist
	// Create primary index with retry logic
	maxRetries := 5
	retryDelay := 50 * time.Millisecond
	actualRetries := 0
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = collection.QueryIndexes().CreatePrimaryIndex(
			&gocb.CreatePrimaryQueryIndexOptions{
				IgnoreIfExists: true,
			})
		if err == nil {
			lastErr = err // clear previous error
			break
		}

		lastErr = err
		t.Logf("Attempt %d: failed to create primary index: %v, retrying in %v", attempt+1, err, retryDelay)
		time.Sleep(retryDelay)
		// Exponential backoff
		retryDelay *= 2
		actualRetries += 1
	}

	if lastErr != nil {
		t.Fatalf("failed to create primary index collection after %d attempts: %v", actualRetries, lastErr)
	}

	// Insert test documents
	for i, param := range params {
		_, err = collection.Upsert(fmt.Sprintf("%d", i+1), param, &gocb.UpsertOptions{
			DurabilityLevel: gocb.DurabilityLevelMajority,
		})
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	// Return a cleanup function
	return func(t *testing.T) {
		// Drop the collection
		err := bucketMgr.DropCollection(scopeName, collectionName, nil)
		if err != nil {
			t.Logf("failed to drop collection: %v", err)
		}
	}
}

// getCouchbaseParamToolInfo returns statements and params for my-tool couchbase-sql kind
func getCouchbaseParamToolInfo(collectionName string) (string, string, string, string, []map[string]any) {
	// N1QL uses positional or named parameters with $ prefix
	toolStatement := fmt.Sprintf("SELECT TONUMBER(meta().id) as id, "+
		"%s.* FROM %s WHERE meta().id = TOSTRING($id) OR name = $name order by meta().id",
		collectionName, collectionName)
	idToolStatement := fmt.Sprintf("SELECT TONUMBER(meta().id) as id, "+
		"%s.* FROM %s WHERE meta().id = TOSTRING($id) order by meta().id",
		collectionName, collectionName)
	nameToolStatement := fmt.Sprintf("SELECT TONUMBER(meta().id) as id, "+
		"%s.* FROM %s WHERE name = $name order by meta().id",
		collectionName, collectionName)
	arrayToolStatemnt := fmt.Sprintf("SELECT TONUMBER(meta().id) as id, "+
		"%s.* FROM %s WHERE TONUMBER(meta().id) IN $idArray AND name IN $nameArray order by meta().id", collectionName, collectionName)
	params := []map[string]any{
		{"name": "Alice"},
		{"name": "Jane"},
		{"name": "Sid"},
		{"name": nil},
	}
	return toolStatement, idToolStatement, nameToolStatement, arrayToolStatemnt, params
}

// getCouchbaseAuthToolInfo returns statements and param of my-auth-tool for couchbase-sql kind
func getCouchbaseAuthToolInfo(collectionName string) (string, []map[string]any) {
	toolStatement := fmt.Sprintf("SELECT name FROM %s WHERE email = $email", collectionName)

	params := []map[string]any{
		{"name": "Alice", "email": tests.ServiceAccountEmail},
		{"name": "Jane", "email": "janedoe@gmail.com"},
	}
	return toolStatement, params
}

func getCouchbaseTemplateParamToolInfo() (string, string, string, []map[string]any) {
	tmplSelectCombined := "SELECT {{.tableName}}.* FROM {{.tableName}} WHERE id = $id"
	tmplSelectFilterCombined := "SELECT {{.tableName}}.* FROM {{.tableName}} WHERE {{.columnFilter}} = $name"
	tmplSelectAll := "SELECT {{.tableName}}.* FROM {{.tableName}}"

	params := []map[string]any{
		{"name": "Alex", "id": 1, "age": 21},
		{"name": "Alice", "id": 2, "age": 100},
	}
	return tmplSelectCombined, tmplSelectFilterCombined, tmplSelectAll, params
}
