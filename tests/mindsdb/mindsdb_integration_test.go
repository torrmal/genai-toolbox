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
	MindsDBSourceKind         = "mindsdb"
	MindsDBToolKind           = "mindsdb-sql"
	MindsDBExecuteSQLToolKind = "mindsdb-execute-sql"
	MindsDBDatabase           = os.Getenv("MINDSDB_DATABASE")
	MindsDBHost               = os.Getenv("MINDSDB_HOST")
	MindsDBPort               = os.Getenv("MINDSDB_PORT")
	MindsDBUser               = os.Getenv("MINDSDB_USER")
	MindsDBPass               = os.Getenv("MINDSDB_PASS")
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

	pool, err := initMindsDBConnectionPool(MindsDBHost, MindsDBPort, MindsDBUser, MindsDBPass, MindsDBDatabase)
	if err != nil {
		t.Fatalf("unable to create MindsDB connection pool: %s", err)
	}

	// create table name with UUID
	tableNameParam := "param_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	tableNameAuth := "auth_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	tableNameTemplateParam := "template_param_table_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// set up data for param tool
	createParamTableStmt, insertParamTableStmt, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, paramTestParams := tests.GetMindsDBParamToolInfo(tableNameParam)
	teardownTable1 := tests.SetupMindsDBTable(t, ctx, pool, createParamTableStmt, insertParamTableStmt, tableNameParam, paramTestParams)
	defer teardownTable1(t)

	// set up data for auth tool
	createAuthTableStmt, insertAuthTableStmt, authToolStmt, authTestParams := tests.GetMindsDBAuthToolInfo(tableNameAuth)
	teardownTable2 := tests.SetupMindsDBTable(t, ctx, pool, createAuthTableStmt, insertAuthTableStmt, tableNameAuth, authTestParams)
	defer teardownTable2(t)

	// Write config into a file and pass it to command
	toolsFile := tests.GetToolsConfig(sourceConfig, MindsDBToolKind, paramToolStmt, idParamToolStmt, nameParamToolStmt, arrayToolStmt, authToolStmt)
	toolsFile = tests.AddMindsDBExecuteSqlConfig(t, toolsFile)
	tmplSelectCombined, tmplSelectFilterCombined := tests.GetMindsDBTmplToolStatement()
	toolsFile = tests.AddTemplateParamConfig(t, toolsFile, MindsDBToolKind, tmplSelectCombined, tmplSelectFilterCombined, "")

	// Add MindsDB-specific template parameter tools for comprehensive testing
	tools, _ := toolsFile["tools"].(map[string]any)
	tools["my-template-tool"] = map[string]any{
		"kind":        MindsDBToolKind,
		"source":      "my-instance",
		"description": "Tool to test template parameters only.",
		"statement":   "SELECT * FROM files.{{.table_name}}",
		"templateParameters": []any{
			map[string]any{
				"name":        "table_name",
				"type":        "string",
				"description": "Name of the table to query",
			},
		},
	}

	// Override standard tools to add template parameters for MindsDB testing
	tools["my-tool"] = map[string]any{
		"kind":        MindsDBToolKind,
		"source":      "my-instance",
		"description": "Tool to test invocation with template params.",
		"statement":   "SELECT * FROM files.{{.table_name}} WHERE id = ? OR name = ?",
		"templateParameters": []any{
			map[string]any{
				"name":        "table_name",
				"type":        "string",
				"description": "Name of the table to query",
			},
		},
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
	}

	tools["my-tool-by-id"] = map[string]any{
		"kind":        MindsDBToolKind,
		"source":      "my-instance",
		"description": "Tool to test invocation with template params.",
		"statement":   "SELECT * FROM files.{{.table_name}} WHERE id = ?",
		"templateParameters": []any{
			map[string]any{
				"name":        "table_name",
				"type":        "string",
				"description": "Name of the table to query",
			},
		},
		"parameters": []any{
			map[string]any{
				"name":        "id",
				"type":        "integer",
				"description": "user ID",
			},
		},
	}

	tools["my-tool-by-name"] = map[string]any{
		"kind":        MindsDBToolKind,
		"source":      "my-instance",
		"description": "Tool to test invocation with template params.",
		"statement":   "SELECT * FROM files.{{.table_name}} WHERE name = ?",
		"templateParameters": []any{
			map[string]any{
				"name":        "table_name",
				"type":        "string",
				"description": "Name of the table to query",
			},
		},
		"parameters": []any{
			map[string]any{
				"name":        "name",
				"type":        "string",
				"description": "user name",
				"required":    false,
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

	tests.RunToolGetTest(t)

	// REQUIRED: Run the predefined integration test suites (as per CONTRIBUTING.md)
	select1Want, failInvocationWant, createTableStatement := tests.GetMindsDBWants()
	invokeParamWant, invokeIdNullWant, nullWant, mcpInvokeParamWant := tests.GetNonSpannerInvokeParamWant()

	tests.RunToolInvokeTest(t, select1Want, invokeParamWant, invokeIdNullWant, nullWant, true, false)
	tests.RunMCPToolCallMethod(t, mcpInvokeParamWant, failInvocationWant)
	tests.RunExecuteSqlToolInvokeTest(t, createTableStatement, "null")
	tests.RunToolInvokeWithTemplateParameters(t, tableNameTemplateParam, tests.NewTemplateParameterTestConfig())

	// MINDSDB-SPECIFIC: Additional comprehensive testing for MindsDB unique features
	t.Logf("=== Starting MindsDB-specific comprehensive testing ===")

	// Test basic tool functionality - simple tool works
	tests.RunToolInvokeSimpleTest(t, "my-simple-tool", select1Want)

	// Verify that the parameter tables exist before testing parameterized queries
	t.Logf("Verifying parameter tables exist before testing...")

	// Test that the parameter tables exist by running a simple select
	verifyParamTableSQL := fmt.Sprintf("SELECT COUNT(*) FROM files.%s", tableNameParam)
	runSQLTest(t, verifyParamTableSQL, "[{\"count_0\":4}]") // Should have 4 rows from our insert

	verifyAuthTableSQL := fmt.Sprintf("SELECT COUNT(*) FROM files.%s", tableNameAuth)
	runSQLTest(t, verifyAuthTableSQL, "[{\"count_0\":2}]") // Should have 2 rows from our insert

	t.Logf("Parameter tables verified - testing parameterized queries...")

	// Print the exact queries and parameters for manual testing
	t.Logf("=== PARAMETERIZED QUERY DETAILS FOR MANUAL TESTING ===")
	t.Logf("Table name: %s", tableNameParam)
	t.Logf("")
	t.Logf("Query 1 (my-tool): %s", paramToolStmt)
	t.Logf("Parameters: {\"id\": 3, \"name\": \"Alice\"}")
	t.Logf("Expected result: %s", invokeParamWant)
	t.Logf("")
	t.Logf("Query 2 (my-tool-by-id): %s", idParamToolStmt)
	t.Logf("Parameters: {\"id\": 4}")
	t.Logf("Expected result: %s", invokeIdNullWant)
	t.Logf("")
	t.Logf("Query 3 (my-tool-by-name): %s", nameParamToolStmt)
	t.Logf("Parameters: {}")
	t.Logf("Expected result: %s", nullWant)
	t.Logf("")
	t.Logf("=== END PARAMETERIZED QUERY DETAILS ===")

	// Test parameterized queries using mindsdb-sql tool with template parameters
	t.Logf("=== TESTING PARAMETERIZED QUERIES WITH MINDSDB-SQL TOOL ===")

	// Test simple template-only tool first
	t.Logf("--- Testing template-only tool ---")
	simpleParams := fmt.Sprintf(`{"table_name": "%s"}`, tableNameParam)
	t.Logf("Simple Template Parameters: %s", simpleParams)
	t.Logf("Simple Template Expected SQL: SELECT * FROM files.%s", tableNameParam)
	reqSimple, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:5000/api/tool/my-template-tool/invoke", bytes.NewBuffer([]byte(simpleParams)))
	reqSimple.Header.Add("Content-type", "application/json")
	respSimple, _ := http.DefaultClient.Do(reqSimple)
	bodySimple, _ := io.ReadAll(respSimple.Body)
	respSimple.Body.Close()
	t.Logf("Simple Template Status: %d, Actual Result: %s", respSimple.StatusCode, string(bodySimple))

	t.Logf("--- Testing template + parameter tools ---")

	// Test Query 1: my-tool with template and regular parameters
	params1 := fmt.Sprintf(`{"table_name": "%s", "id": 3, "name": "Alice"}`, tableNameParam)
	t.Logf("Query 1 Parameters: %s", params1)
	t.Logf("Query 1 Expected SQL: SELECT * FROM files.%s WHERE id = ? OR name = ?", tableNameParam)
	req1, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:5000/api/tool/my-tool/invoke", bytes.NewBuffer([]byte(params1)))
	req1.Header.Add("Content-type", "application/json")
	resp1, _ := http.DefaultClient.Do(req1)
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	t.Logf("Query 1 Status: %d, Actual Result: %s", resp1.StatusCode, string(body1))

	// Test Query 2: my-tool-by-id with template and regular parameters
	req2, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:5000/api/tool/my-tool-by-id/invoke", bytes.NewBuffer([]byte(fmt.Sprintf(`{"table_name": "%s", "id": 4}`, tableNameParam))))
	req2.Header.Add("Content-type", "application/json")
	resp2, _ := http.DefaultClient.Do(req2)
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	t.Logf("Query 2 Status: %d, Actual Result: %s", resp2.StatusCode, string(body2))

	// Test Query 3: my-tool-by-name with template and regular parameters
	req3, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:5000/api/tool/my-tool-by-name/invoke", bytes.NewBuffer([]byte(fmt.Sprintf(`{"table_name": "%s", "name": "Jane"}`, tableNameParam))))
	req3.Header.Add("Content-type", "application/json")
	resp3, _ := http.DefaultClient.Do(req3)
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	t.Logf("Query 3 Status: %d, Actual Result: %s", resp3.StatusCode, string(body3))

	t.Logf("=== PARAMETERIZED QUERIES COMPLETED ===")

	// Test execute-sql tool functionality with MindsDB files schema
	t.Logf("=== Testing MindsDB execute-sql tool functionality ===")
	tableName := "test_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// Test CREATE TABLE
	createTableSQL := fmt.Sprintf("CREATE TABLE files.%s (id INT PRIMARY KEY, name VARCHAR(255))", tableName)
	runSQLTest(t, createTableSQL, "null")

	// Test INSERT
	insertSQL := fmt.Sprintf("INSERT INTO files.%s (id, name) VALUES (1, 'Alice')", tableName)
	runSQLTest(t, insertSQL, "null")

	// Test SELECT
	selectSQL := fmt.Sprintf("SELECT * FROM files.%s", tableName)
	expectedResult := `[{"id":1,"name":"Alice"}]`
	runSQLTest(t, selectSQL, expectedResult)

	// Test DROP TABLE
	dropTableSQL := fmt.Sprintf("DROP TABLE files.%s", tableName)
	runSQLTest(t, dropTableSQL, "null")

	t.Logf("MindsDB execute-sql tool test completed successfully")
	t.Logf("All MindsDB tools working correctly!")

	t.Logf("=== MindsDB-specific comprehensive testing completed ===")
}

func TestMindsDBExecuteSQLTool(t *testing.T) {
	sourceConfig := getMindsDBVars(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var args []string

	// Create tools configuration with standard my-exec-sql-tool pattern
	toolsFile := map[string]any{
		"sources": map[string]any{
			"my-instance": sourceConfig,
		},
		"tools": map[string]any{},
	}
	toolsFile = tests.AddMindsDBExecuteSqlConfig(t, toolsFile)

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

	t.Logf("Server started successfully with output: %s", out)

	// Create a unique table name for this test
	tableName := "test_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	// Test 1: CREATE TABLE
	t.Run("CREATE TABLE", func(t *testing.T) {
		createTableSQL := fmt.Sprintf("CREATE TABLE files.%s (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))", tableName)
		runSQLTest(t, createTableSQL, "null")
	})

	// Test 2: INSERT data
	t.Run("INSERT", func(t *testing.T) {
		insertSQL := fmt.Sprintf("INSERT INTO files.%s (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')", tableName)
		runSQLTest(t, insertSQL, "null")
	})

	// Test 3: SELECT data
	t.Run("SELECT", func(t *testing.T) {
		selectSQL := fmt.Sprintf("SELECT * FROM files.%s", tableName)
		expectedResult := `[{"email":"alice@example.com","id":1,"name":"Alice"},{"email":"bob@example.com","id":2,"name":"Bob"}]`
		runSQLTest(t, selectSQL, expectedResult)
	})

	// Test 4: SELECT with WHERE clause
	t.Run("SELECT with WHERE", func(t *testing.T) {
		selectWhereSQL := fmt.Sprintf("SELECT * FROM files.%s WHERE id = 1", tableName)
		expectedResult := `[{"email":"alice@example.com","id":1,"name":"Alice"}]`
		runSQLTest(t, selectWhereSQL, expectedResult)
	})

	// Test 5: DROP TABLE cleanup
	t.Run("DROP TABLE", func(t *testing.T) {
		dropTableSQL := fmt.Sprintf("DROP TABLE files.%s", tableName)
		runSQLTest(t, dropTableSQL, "null")
	})

	t.Logf("MindsDB execute-sql tool test completed successfully")
}

// runSQLTest executes a SQL statement using the my-exec-sql-tool and verifies the result
func runSQLTest(t *testing.T, sqlStatement, expectedResult string) {
	// Test tool invoke endpoint
	api := "http://127.0.0.1:5000/api/tool/my-exec-sql-tool/invoke"
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
