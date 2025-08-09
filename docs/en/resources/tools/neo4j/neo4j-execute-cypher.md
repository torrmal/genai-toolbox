---
title: "neo4j-execute-cypher"
type: docs
weight: 1
description: >
  A "neo4j-execute-cypher" tool executes any arbitrary Cypher statement against a Neo4j
  database.
aliases:
- /resources/tools/neo4j-execute-cypher
---

## About

A `neo4j-execute-cypher` tool executes an arbitrary Cypher query provided as a
string parameter against a Neo4j database. It's designed to be a flexible tool
for interacting with the database when a pre-defined query is not sufficient.
This tool is compatible with any of the following sources:

- [neo4j](../../sources/neo4j.md)

For security, the tool can be configured to be read-only. If the `readOnly` flag
is set to `true`, the tool will analyze the incoming Cypher query and reject any
write operations (like `CREATE`, `MERGE`, `DELETE`, etc.) before execution.

The Cypher query uses standard [Neo4j
Cypher](https://neo4j.com/docs/cypher-manual/current/queries/) syntax and
supports all Cypher features, including pattern matching, filtering, and
aggregation.

`neo4j-execute-cypher` takes one input parameter `cypher` and run the cypher
query against the `source`.

> **Note:** This tool is intended for developer assistant workflows with
> human-in-the-loop and shouldn't be used for production agents.

## Example

```yaml
tools:
  query_neo4j:
    kind: neo4j-execute-cypher
    source: my-neo4j-prod-db
    readOnly: true
    description: |
      Use this tool to execute a Cypher query against the production database.
      Only read-only queries are allowed.
      Takes a single 'cypher' parameter containing the full query string.
      Example:
      {{
          "cypher": "MATCH (m:Movie {title: 'The Matrix'}) RETURN m.released"
      }}
```

## Reference

| **field**   |                  **type**                  | **required** | **description**                                                                                 |
|-------------|:------------------------------------------:|:------------:|-------------------------------------------------------------------------------------------------|
| kind        |                   string                   |     true     | Must be "neo4j-cypher".                                                                         |
| source      |                   string                   |     true     | Name of the source the Cypher query should execute on.                                          |
| description |                   string                   |     true     | Description of the tool that is passed to the LLM.                                              |
| readOnly    |                   boolean                  |     false    | If set to `true`, the tool will reject any write operations in the Cypher query. Default is `false`. |
