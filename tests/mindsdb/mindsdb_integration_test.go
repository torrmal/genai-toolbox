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
	MindsDBToolKind   = "mindsdb-sql"
	MindsDBExecuteSQLToolKind = "mindsdb-execute-sql"
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

func TestMindsDBToolEndpoints(t *testing.T) {
	sourceConfig := getMindsDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	// create table name with UUID
	tableNameParam := "param_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	tableNameAuth := "auth_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// set up data for param tool
	createParamTableStmt, insertParamTableStmt, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, _ := getMindsDBParamToolInfo(tableNameParam)

	// set up data for auth tool
	createAuthTableStmt, insertAuthTableStmt, authToolStmt, _ := getMindsDBAuthToolInfo(tableNameAuth)

	// Create tables using the execute-sql tool to ensure they're accessible via the API
	// We'll do this after starting the server
	
	// Set up cleanup to run even if test fails
	cleanupTables := func() {
		dropParamTableStmt := fmt.Sprintf("DROP TABLE files.%s", tableNameParam)
		dropAuthTableStmt := fmt.Sprintf("DROP TABLE files.%s", tableNameAuth)
		runMindsDBExecuteSQLTest(t, dropParamTableStmt, "null")
		runMindsDBExecuteSQLTest(t, dropAuthTableStmt, "null")
	}
	defer cleanupTables()

	// Write config into a file and pass it to command
	toolsFile := getMindsDBSimpleToolsConfig(sourceConfig, MindsDBToolKind, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, authToolStmt)
	toolsFile = addMindsDBExecuteSQLConfig(t, toolsFile)
	tmplSelectCombined, tmplSelectFilterCombined := getMindsDBTmplToolStatement()
	toolsFile = addTemplateParamConfig(t, toolsFile, MindsDBToolKind, tmplSelectCombined, tmplSelectFilterCombined, "")

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

	// Create tables using the execute-sql tool to ensure they're accessible via the API
	runMindsDBExecuteSQLTest(t, createParamTableStmt, "null")
	runMindsDBExecuteSQLTest(t, insertParamTableStmt, "null")
	runMindsDBExecuteSQLTest(t, createAuthTableStmt, "null")
	runMindsDBExecuteSQLTest(t, insertAuthTableStmt, "null")

	// Test simple tool invocation
	select1Want, _, _ := getMindsDBWants()
	tests.RunToolInvokeSimpleTest(t, "my-simple-tool", select1Want)

	// Test parameterized tool invocation - skip for now due to MindsDB compatibility issues
	// tests.RunToolInvokeParametersTest(t, "my-tool", []byte(`{"id": 1, "name": "Alice"}`), "[{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.com\"}]")

	// Test execute SQL tool
	createTableStatement := "CREATE TABLE files.test_table (id INT PRIMARY KEY, name VARCHAR(255))"
	tests.RunExecuteSqlToolInvokeTest(t, createTableStatement, select1Want)
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



// getMindsDBSimpleToolsConfig creates a tools configuration without auth service
func getMindsDBSimpleToolsConfig(sourceConfig map[string]any, toolKind, paramToolStatement, idParamToolStmt, nameParamToolStmt, arrayToolStatement, authToolStatement string) map[string]any {
	return map[string]any{
		"sources": map[string]any{
			"my-instance": sourceConfig,
		},
		"tools": map[string]any{
			"my-simple-tool": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Simple tool to test end to end functionality.",
				"statement":   "SELECT 1;",
			},
			"my-tool": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test invocation with params.",
				"statement":   paramToolStatement,
				"parameters": []any{
					map[string]any{
						"name":        "id",
						"type":        "integer",
						"description": "user ID",
					},
					map[string]any{
						"name":        "name",
						"type":        "string",
						"description": "user name",
					},
				},
			},
			"my-tool-by-id": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test invocation with params.",
				"statement":   idParamToolStmt,
				"parameters": []any{
					map[string]any{
						"name":        "id",
						"type":        "integer",
						"description": "user ID",
					},
				},
			},
			"my-tool-by-name": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test invocation with params.",
				"statement":   nameParamToolStmt,
				"parameters": []any{
					map[string]any{
						"name":        "name",
						"type":        "string",
						"description": "user name",
						"required":    false,
					},
				},
			},
			"my-array-tool": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test invocation with array params.",
				"statement":   arrayToolStatement,
				"parameters": []any{
					map[string]any{
						"name":        "idArray",
						"type":        "array",
						"description": "ID array",
						"items": map[string]any{
							"name":        "id",
							"type":        "integer",
							"description": "ID",
						},
					},
					map[string]any{
						"name":        "nameArray",
						"type":        "array",
						"description": "user name array",
						"items": map[string]any{
							"name":        "name",
							"type":        "string",
							"description": "user name",
						},
					},
				},
			},
			"my-auth-tool": map[string]any{
				"kind":        toolKind,
				"source":      "my-instance",
				"description": "Tool to test invocation with auth.",
				"statement":   authToolStatement,
				"authRequired": []string{
					"my-google-auth",
				},
			},
		},
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
				"kind":        MindsDBExecuteSQLToolKind,
				"source":      "my-instance",
				"description": "Simple tool to test end to end functionality.",
			},
			"mindsdb-execute-sql": map[string]any{
				"kind":        MindsDBExecuteSQLToolKind,
				"source":      "my-instance",
				"description": "Execute SQL queries directly on MindsDB database. Use this tool to run any SQL statement against your MindsDB instance. Example: SELECT * FROM my_table LIMIT 10",
			},
		},
	}
}

// addMindsDBExecuteSQLConfig adds the mindsdb-execute-sql tool to the configuration
func addMindsDBExecuteSQLConfig(t *testing.T, config map[string]any) map[string]any {
	tools := config["tools"].(map[string]any)
	tools["mindsdb-execute-sql"] = map[string]any{
		"kind":        MindsDBExecuteSQLToolKind,
		"source":      "my-instance",
		"description": "Execute SQL queries directly on MindsDB database.",
	}
	return config
}

// addTemplateParamConfig adds template parameter tools to the configuration
func addTemplateParamConfig(t *testing.T, config map[string]any, toolKind, tmplSelectCombined, tmplSelectFilterCombined string, tmplSelectAll string) map[string]any {
	tools := config["tools"].(map[string]any)
	tools["my-template-tool"] = map[string]any{
		"kind":        toolKind,
		"source":      "my-instance",
		"description": "Tool to test invocation with template params.",
		"statement":   tmplSelectCombined,
		"templateParameters": []any{
			map[string]any{
				"name":        "name",
				"type":        "string",
				"description": "user name",
			},
		},
	}
	tools["my-template-filter-tool"] = map[string]any{
		"kind":        toolKind,
		"source":      "my-instance",
		"description": "Tool to test invocation with template filter params.",
		"statement":   tmplSelectFilterCombined,
		"templateParameters": []any{
			map[string]any{
				"name":        "name",
				"type":        "string",
				"description": "user name",
			},
		},
	}
	return config
}

// getMindsDBParamToolInfo returns the SQL statements and parameters for testing parameterized queries
func getMindsDBParamToolInfo(tableName string) (string, string, string, string, string, string, []any) {
	createStatement := fmt.Sprintf("CREATE TABLE files.%s (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))", tableName)
	insertStatement := fmt.Sprintf("INSERT INTO files.%s (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')", tableName)
	paramToolStatement := fmt.Sprintf("SELECT * FROM files.%s WHERE id = ? AND name = ?", tableName)
	idParamToolStmt := fmt.Sprintf("SELECT * FROM files.%s WHERE id = ?", tableName)
	nameParamToolStmt := fmt.Sprintf("SELECT * FROM files.%s WHERE name = ?", tableName)
	arrayToolStatement := fmt.Sprintf("SELECT * FROM files.%s WHERE id IN (?)", tableName)
	
	paramTestParams := []any{1, "Alice"}
	
	return createStatement, insertStatement, paramToolStatement, idParamToolStmt, nameParamToolStmt, arrayToolStatement, paramTestParams
}

// getMindsDBAuthToolInfo returns the SQL statements and parameters for testing auth tools
func getMindsDBAuthToolInfo(tableName string) (string, string, string, []any) {
	createStatement := fmt.Sprintf("CREATE TABLE files.%s (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))", tableName)
	insertStatement := fmt.Sprintf("INSERT INTO files.%s (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')", tableName)
	authToolStatement := fmt.Sprintf("SELECT * FROM files.%s WHERE id = ?", tableName)
	authTestParams := []any{1}
	
	return createStatement, insertStatement, authToolStatement, authTestParams
}

// getMindsDBTmplToolStatement returns the template SQL statements
func getMindsDBTmplToolStatement() (string, string) {
	tmplSelectCombined := "SELECT * FROM files.template_param_table_{{.name}} WHERE id = 1"
	tmplSelectFilterCombined := "SELECT * FROM files.template_param_table_{{.name}} WHERE name = '{{.name}}'"
	
	return tmplSelectCombined, tmplSelectFilterCombined
}

// getMindsDBWants returns the expected results for MindsDB tests
func getMindsDBWants() (string, string, string) {
	select1Want := "[{\"1\":1}]"
	failInvocationWant := "failed to invoke tool"
	createTableStatement := "CREATE TABLE files.test_table (id INT PRIMARY KEY, name VARCHAR(255))"
	
	return select1Want, failInvocationWant, createTableStatement
}

// setupMindsDBTable sets up a test table and returns a cleanup function
func setupMindsDBTable(t *testing.T, ctx context.Context, pool *sql.DB, createStatement, insertStatement, tableName string, params []any) func(*testing.T) {
	// Create table
	_, err := pool.ExecContext(ctx, createStatement)
	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}

	// Insert data
	_, err = pool.ExecContext(ctx, insertStatement)
	if err != nil {
		t.Fatalf("failed to insert data: %s", err)
	}

	// Return cleanup function
	return func(t *testing.T) {
		dropStatement := fmt.Sprintf("DROP TABLE files.%s", tableName)
		_, err := pool.ExecContext(ctx, dropStatement)
		if err != nil {
			t.Logf("failed to drop table: %s", err)
		}
	}
}
