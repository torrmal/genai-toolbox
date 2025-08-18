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

	// Get predefined test wants for MindsDB
	select1Want, failInvocationWant, createTableStatement := tests.GetMindsDBWants()
	invokeParamWant, invokeIdNullWant, nullWant, mcpInvokeParamWant := tests.GetNonSpannerInvokeParamWant()

	// Run the required predefined integration test suites
	tests.RunToolInvokeTest(t, select1Want, invokeParamWant, invokeIdNullWant, nullWant, true, false)
	tests.RunMCPToolCallMethod(t, mcpInvokeParamWant, failInvocationWant)
	tests.RunExecuteSqlToolInvokeTest(t, createTableStatement, "null")
	tests.RunToolInvokeWithTemplateParameters(t, tableNameTemplateParam, tests.NewTemplateParameterTestConfig())
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
