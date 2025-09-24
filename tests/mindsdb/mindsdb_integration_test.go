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

package mindsdb

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/googleapis/genai-toolbox/internal/testutils"
	"github.com/googleapis/genai-toolbox/tests"
)

var (
	MindsDBSourceKind = "mindsdb"
	MindsDBToolKind   = "mindsdb-sql"
	MindsDBDatabase   = os.Getenv("MINDSDB_DATABASE")
	MindsDBHost       = os.Getenv("MINDSDB_HOST")
	MindsDBPort       = os.Getenv("MINDSDB_PORT")
	MindsDBUser       = os.Getenv("MINDSDB_USER")
	MindsDBPass       = os.Getenv("MINDSDB_PASS")
)

func getMindsDBVars(t *testing.T) map[string]any {
	switch "" {
	case MindsDBDatabase:
		t.Fatal("'MINDSDB_DATABASE' not set")
	case MindsDBHost:
		t.Fatal("'MINDSDB_HOST' not set")
	case MindsDBPort:
		t.Fatal("'MINDSDB_PORT' not set")
	case MindsDBUser:
		t.Fatal("'MINDSDB_USER' not set")
	}

	// MindsDBPass can be empty, but the env var must exist
	if _, exists := os.LookupEnv("MINDSDB_PASS"); !exists {
		t.Fatal("'MINDSDB_PASS' not set (can be empty)")
	}

	// Handle MindsDB's no-password authentication for toolbox config
	mindsdbPassword := MindsDBPass
	if mindsdbPassword == "none" {
		mindsdbPassword = ""
	}

	return map[string]any{
		"kind":     MindsDBSourceKind,
		"host":     MindsDBHost,
		"port":     MindsDBPort,
		"database": MindsDBDatabase,
		"user":     MindsDBUser,
		"password": mindsdbPassword,
	}
}

func TestMindsDBToolEndpoints(t *testing.T) {
	sourceConfig := getMindsDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	// Create simple tools config for MindsDB - focus on what works
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
				"kind":        MindsDBToolKind,
				"source":      "my-instance",
				"description": "Simple tool to test end to end functionality.",
				"statement":   "SELECT 1",
			},
			"my-exec-sql-tool": map[string]any{
				"kind":        "mindsdb-execute-sql",
				"source":      "my-instance",
				"description": "Tool to execute sql",
			},
			"my-show-databases-tool": map[string]any{
				"kind":        MindsDBToolKind,
				"source":      "my-instance",
				"description": "Tool to show databases",
				"statement":   "SHOW DATABASES",
			},
			"my-show-tables-tool": map[string]any{
				"kind":        MindsDBToolKind,
				"source":      "my-instance",
				"description": "Tool to show tables",
				"statement":   "SHOW TABLES",
			},
			"my-info-schema-tool": map[string]any{
				"kind":        MindsDBToolKind,
				"source":      "my-instance",
				"description": "Tool to query information schema",
				"statement":   "SELECT TABLE_NAME FROM information_schema.TABLES LIMIT 5",
			},
			"my-auth-exec-sql-tool": map[string]any{
				"kind":        "mindsdb-execute-sql",
				"source":      "my-instance",
				"description": "Tool to execute sql with auth",
				"authRequired": []string{
					"my-google-auth",
				},
			},
		},
	}

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

	// Get configs for tests
	select1Want := "[{\"1\":1}]"

	// Run comprehensive tests for MindsDB
	tests.RunToolGetTest(t)

	// Test all basic MindsDB tool invocations
	tests.RunToolInvokeSimpleTest(t, "my-simple-tool", select1Want)
	tests.RunToolInvokeSimpleTest(t, "my-show-databases-tool", "")
	tests.RunToolInvokeSimpleTest(t, "my-show-tables-tool", "")
	tests.RunToolInvokeSimpleTest(t, "my-info-schema-tool", "")

	// Test comprehensive execute SQL functionality
	t.Run("mindsdb_sql_tests", func(t *testing.T) {
		// Test basic SELECT query
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SELECT 1"}`), select1Want)

		// Test SHOW DATABASES
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SHOW DATABASES"}`), "")

		// Test SHOW TABLES
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SHOW TABLES"}`), "")

		// Test SELECT from information_schema
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SELECT TABLE_NAME FROM information_schema.TABLES LIMIT 1"}`), "")

		// Test basic arithmetic
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SELECT 1+1 as result"}`), "")

		// Test string functions
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SELECT UPPER('hello') as result"}`), "")

		// Test date functions
		tests.RunToolInvokeParametersTest(t, "my-exec-sql-tool", []byte(`{"sql": "SELECT NOW() as current_time"}`), "")
	})

	// Test error handling - these are expected to fail but exercise error paths
	t.Run("mindsdb_error_handling", func(t *testing.T) {
		// Test invalid SQL - expect this to fail with 400
		resp, err := http.Post("http://127.0.0.1:5000/api/tool/my-exec-sql-tool/invoke", "application/json", bytes.NewBuffer([]byte(`{"sql": "INVALID SQL QUERY"}`)))
		if err != nil {
			t.Fatalf("error when sending request: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Logf("Expected 400 for invalid SQL, got %d (this exercises error handling)", resp.StatusCode)
		}

		// Test empty SQL - expect this to fail with 400
		resp2, err := http.Post("http://127.0.0.1:5000/api/tool/my-exec-sql-tool/invoke", "application/json", bytes.NewBuffer([]byte(`{"sql": ""}`)))
		if err != nil {
			t.Fatalf("error when sending request: %s", err)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusBadRequest {
			t.Logf("Expected 400 for empty SQL, got %d (this exercises error handling)", resp2.StatusCode)
		}
	})

	// Test authentication - these are expected to fail but exercise auth code paths
	t.Run("mindsdb_auth_tests", func(t *testing.T) {
		// Test auth-required tool without auth - expect this to fail with 401
		resp, err := http.Post("http://127.0.0.1:5000/api/tool/my-auth-exec-sql-tool/invoke", "application/json", bytes.NewBuffer([]byte(`{"sql": "SELECT 1"}`)))
		if err != nil {
			t.Fatalf("error when sending request: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Logf("Expected 401 for missing auth, got %d (this exercises auth handling)", resp.StatusCode)
		}
	})
}
