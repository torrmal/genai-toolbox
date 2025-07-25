---
title: "spanner-sql"
type: docs
weight: 1
description: > 
  A "spanner-sql" tool executes a pre-defined SQL statement against a Google 
  Cloud Spanner database.
aliases:
- /resources/tools/spanner-sql
---

## About

A `spanner-sql` tool executes a pre-defined SQL statement (either `googlesql` or
`postgresql`) against a Cloud Spanner database. It's compatible with any of the
following sources:

- [spanner](../sources/spanner.md)

### GoogleSQL

For the `googlesql` dialect, the specified SQL statement is executed as a [data
manipulation language (DML)][gsql-dml] statements, and specified parameters will
inserted according to their name: e.g. `@name`.

> **Note:** This tool uses parameterized queries to prevent SQL injections.
> Query parameters can be used as substitutes for arbitrary expressions.
> Parameters cannot be used as substitutes for identifiers, column names, table
> names, or other parts of the query.

[gsql-dml]:
    https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax

### PostgreSQL

For the `postgresql` dialect, the specified SQL statement is executed as a [prepared
statement][pg-prepare], and specified parameters will inserted according to
their position: e.g. `$1` will be the first parameter specified, `$@` will be
the second parameter, and so on.

[pg-prepare]: https://www.postgresql.org/docs/current/sql-prepare.html

## Example

> **Note:** This tool uses parameterized queries to prevent SQL injections.
> Query parameters can be used as substitutes for arbitrary expressions.
> Parameters cannot be used as substitutes for identifiers, column names, table
> names, or other parts of the query.

{{< tabpane persist="header" >}}
{{< tab header="GoogleSQL" lang="yaml" >}}

tools:
 search_flights_by_number:
    kind: spanner-sql
    source: my-spanner-instance
    statement: |
      SELECT * FROM flights
      WHERE airline = @airline
      AND flight_number = @flight_number
      LIMIT 10
    description: |
      Use this tool to get information for a specific flight.
      Takes an airline code and flight number and returns info on the flight.
      Do NOT use this tool with a flight id. Do NOT guess an airline code or flight number.
      A airline code is a code for an airline service consisting of two-character
      airline designator and followed by flight number, which is 1 to 4 digit number.
      For example, if given CY 0123, the airline is "CY", and flight_number is "123".
      Another example for this is DL 1234, the airline is "DL", and flight_number is "1234".
      If the tool returns more than one option choose the date closes to today.
      Example:
      {{
          "airline": "CY",
          "flight_number": "888",
      }}
      Example:
      {{
          "airline": "DL",
          "flight_number": "1234",
      }}
    parameters:
      - name: airline
        type: string
        description: Airline unique 2 letter identifier
      - name: flight_number
        type: string
        description: 1 to 4 digit number

{{< /tab >}}
{{< tab header="PostgreSQL" lang="yaml" >}}

tools:
 search_flights_by_number:
    kind: spanner
    source: my-spanner-instance
    statement: |
      SELECT * FROM flights
      WHERE airline = $1
      AND flight_number = $2
      LIMIT 10
    description: |
      Use this tool to get information for a specific flight.
      Takes an airline code and flight number and returns info on the flight.
      Do NOT use this tool with a flight id. Do NOT guess an airline code or flight number.
      A airline code is a code for an airline service consisting of two-character
      airline designator and followed by flight number, which is 1 to 4 digit number.
      For example, if given CY 0123, the airline is "CY", and flight_number is "123".
      Another example for this is DL 1234, the airline is "DL", and flight_number is "1234".
      If the tool returns more than one option choose the date closes to today.
      Example:
      {{
          "airline": "CY",
          "flight_number": "888",
      }}
      Example:
      {{
          "airline": "DL",
          "flight_number": "1234",
      }}
    parameters:
      - name: airline
        type: string
        description: Airline unique 2 letter identifier
      - name: flight_number
        type: string
        description: 1 to 4 digit number

{{< /tab >}}
{{< /tabpane >}}

### Example with Template Parameters

> **Note:** This tool allows direct modifications to the SQL statement,
> including identifiers, column names, and table names. **This makes it more
> vulnerable to SQL injections**. Using basic parameters only (see above) is
> recommended for performance and safety reasons. For more details, please check
> [templateParameters](_index#template-parameters).

```yaml
tools:
 list_table:
    kind: spanner
    source: my-spanner-instance
    statement: |
      SELECT * FROM {{.tableName}};
    description: |
      Use this tool to list all information from a specific table.
      Example:
      {{
          "tableName": "flights",
      }}
    templateParameters:
      - name: tableName
        type: string
        description: Table to select from
```

## Reference

| **field**          |                  **type**                        | **required** | **description**                                                                                                                            |
|--------------------|:------------------------------------------------:|:------------:|--------------------------------------------------------------------------------------------------------------------------------------------|
| kind               |                   string                         |     true     | Must be "spanner-sql".                                                                                                                     |
| source             |                   string                         |     true     | Name of the source the SQL should execute on.                                                                                              |
| description        |                   string                         |     true     | Description of the tool that is passed to the LLM.                                                                                         |
| statement          |                   string                         |     true     | SQL statement to execute on.                                                                                                               |
| parameters         | [parameters](_index#specifying-parameters)       |    false     | List of [parameters](_index#specifying-parameters) that will be inserted into the SQL statement.                                           |
| readOnly           |                   bool                           |    false     | When set to `true`, the `statement` is run as a read-only transaction. Default: `false`.                                                   |
| templateParameters | [templateParameters](_index#template-parameters) |    false     | List of [templateParameters](_index#template-parameters) that will be inserted into the SQL statement before executing prepared statement. |
