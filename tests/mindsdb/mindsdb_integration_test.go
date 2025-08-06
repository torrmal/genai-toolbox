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
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/googleapis/genai-toolbox/internal/testutils"
	"github.com/googleapis/genai-toolbox/tests"
)

var (
	MindsDBSourceKind = "mindsdb"
	MindsDBToolKind   = "mindsdb-execute-sql"
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

	config := map[string]any{
		"kind":     MindsDBSourceKind,
		"host":     MindsDBHost,
		"port":     MindsDBPort,
		"database": MindsDBDatabase,
		"user":     MindsDBUser,
	}

	// Only add password if it's set
	if MindsDBPass != "" {
		config["password"] = MindsDBPass
	}

	return config
}

// initMindsDBConnectionPool connects directly to MindsDB
func initMindsDBConnectionPool(host, port, user, pass, dbname string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, pass, host, port, dbname)
	pool, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return pool, nil
}

func TestMindsDBExecuteSQLTool(t *testing.T) {
	sourceConfig := getMindsDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	// Create tools configuration with only execute-sql tool
	toolsFile := getExecuteSQLToolsConfig(sourceConfig)

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

	// Test the execute-sql tool functionality
	// Skip the get test since mindsdb-execute-sql automatically adds sql parameter
	// tests.RunToolGetTest(t)

	// Test basic SELECT query
	select1Want := "[{\"1\":1}]"
	runMindsDBExecuteSQLTest(t, "SELECT 1", select1Want)

	// Test CREATE TABLE
	tableName := "test_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	createTableStatement := fmt.Sprintf("CREATE TABLE files.%s (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))", tableName)
	runMindsDBExecuteSQLTest(t, createTableStatement, "null")

	// Test INSERT
	insertStatement := fmt.Sprintf("INSERT INTO files.%s (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')", tableName)
	runMindsDBExecuteSQLTest(t, insertStatement, "null")

	// Test SELECT from created table
	selectTableWant := `[{"email":"alice@example.com","id":1,"name":"Alice"},{"email":"bob@example.com","id":2,"name":"Bob"}]`
	runMindsDBExecuteSQLTest(t, fmt.Sprintf("SELECT * FROM files.%s", tableName), selectTableWant)

	// Test DROP TABLE
	dropTableStatement := fmt.Sprintf("DROP TABLE files.%s", tableName)
	runMindsDBExecuteSQLTest(t, dropTableStatement, "null")
}

// runMindsDBExecuteSQLTest runs a test for the mindsdb-execute-sql tool
// The tool takes a single 'sql' parameter as a string containing the query
func runMindsDBExecuteSQLTest(t *testing.T, sqlStatement, expectedResult string) {
	// Test tool invoke endpoint
	api := "http://127.0.0.1:5000/api/tool/mindsdb-execute-sql/invoke"
	// The parameter is just 'sql' with the query as a string
	requestBody := fmt.Sprintf(`{"sql":"%s"}`, sqlStatement)
	
	req, err := http.NewRequest(http.MethodPost, api, bytes.NewBuffer([]byte(requestBody)))
	if err != nil {
		t.Fatalf("unable to create request: %s", err)
	}
	req.Header.Add("Content-type", "application/json")
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unable to send request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read the response body to see the error message
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("response status code is not 200, got %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var body map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	if err != nil {
		t.Fatalf("error parsing response body: %s", err)
	}

	// Check if the response contains the expected result
	result, ok := body["result"]
	if !ok {
		t.Fatalf("response does not contain 'result' field")
	}

	// Convert result to string for comparison
	resultStr := fmt.Sprintf("%v", result)
	if resultStr != expectedResult {
		t.Fatalf("got %q, want %q", resultStr, expectedResult)
	}
}

// getExecuteSQLToolsConfig creates a tools configuration with only the execute-sql tool
func getExecuteSQLToolsConfig(sourceConfig map[string]any) map[string]any {
	return map[string]any{
		"sources": map[string]any{
			"my-instance": sourceConfig,
		},
		"tools": map[string]any{
			"my-simple-tool": map[string]any{
				"kind":        "mindsdb-execute-sql",
				"source":      "my-instance",
				"description": "Simple tool to test end to end functionality.",
			},
			"mindsdb-execute-sql": map[string]any{
				"kind":        "mindsdb-execute-sql",
				"source":      "my-instance",
				"description": "Execute SQL queries directly on MindsDB database. Use this tool to run any SQL statement against your MindsDB instance. Example: SELECT * FROM my_table LIMIT 10",
			},
		},
	}
}
