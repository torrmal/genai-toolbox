---
title: "couchbase-sql"
type: docs
weight: 1
description: >
  A "couchbase-sql" tool executes a pre-defined SQL statement against a Couchbase
  database.
aliases:
- /resources/tools/couchbase-sql
---

## About

A `couchbase-sql` tool executes a pre-defined SQL statement against a Couchbase
database. It's compatible with any of the following sources:

- [couchbase](../../sources/couchbase.md)

The specified SQL statement is executed as a parameterized statement, and specified
parameters will be used according to their name: e.g. `$id`.

## Example

> **Note:** This tool uses parameterized queries to prevent SQL injections.
> Query parameters can be used as substitutes for arbitrary expressions.
> Parameters cannot be used as substitutes for identifiers, column names, table
> names, or other parts of the query.

```yaml
tools:
    search_products_by_category:
        kind: couchbase-sql
        source: my-couchbase-instance
        statement: |
            SELECT p.name, p.price, p.description
            FROM products p
            WHERE p.category = $category AND p.price < $max_price
            ORDER BY p.price DESC
            LIMIT 10
        description: |
            Use this tool to get a list of products for a specific category under a maximum price.
            Takes a category name, e.g. "Electronics" and a maximum price e.g 500 and returns a list of product names, prices, and descriptions.
            Do NOT use this tool with invalid category names. Do NOT guess a category name, Do NOT guess a price.
            Example:
            {{
                "category": "Electronics",
                "max_price": 500
            }}
            Example:
            {{
                "category": "Furniture",
                "max_price": 1000
            }}
        parameters:
            - name: category
              type: string
              description: Product category name
            - name: max_price
              type: integer
              description: Maximum price (positive integer)
```

### Example with Template Parameters

> **Note:** This tool allows direct modifications to the SQL statement,
> including identifiers, column names, and table names. **This makes it more
> vulnerable to SQL injections**. Using basic parameters only (see above) is
> recommended for performance and safety reasons. For more details, please check
> [templateParameters](..#template-parameters).

```yaml
tools:
 list_table:
    kind: couchbase-sql
    source: my-couchbase-instance
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
| kind               |                   string                         |     true     | Must be "couchbase-sql".                                                                                                                   |
| source             |                   string                         |     true     | Name of the source the SQL query should execute on.                                                                                        |
| description        |                   string                         |     true     | Description of the tool that is passed to the LLM.                                                                                         |
| statement          |                   string                         |     true     | SQL statement to execute                                                                                                                   |
| parameters         | [parameters](../#specifying-parameters)       |    false     | List of [parameters](../#specifying-parameters) that will be used with the SQL statement.                                               |
| templateParameters | [templateParameters](..#template-parameters) |    false     | List of [templateParameters](..#template-parameters) that will be inserted into the SQL statement before executing prepared statement. |
| authRequired       |                array[string]                     |    false     | List of auth services that are required to use this tool.                                                                                  |
