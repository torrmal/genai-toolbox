---
title: "looker-query-sql"
type: docs
weight: 1
description: >
  "looker-query-sql" generates a sql query using the Looker
  semantic model.
aliases:
- /resources/tools/looker-query-sql
---

## About

The `looker-query-sql` generates a sql query using the Looker
semantic model.

It's compatible with the following sources:

- [looker](../../sources/looker.md)

`looker-query-sql` takes eight parameters:

1. the `model`
2. the `explore`
3. the `fields` list
4. an optional set of `filters`
5. an optional set of `pivots`
6. an optional set of `sorts`
7. an optional `limit`
8. an optional `tz`

## Example

```yaml
tools:
    query_sql:
        kind: looker-query-sql
        source: looker-source
        description: |
          Query SQL Tool

          This tool is used to generate a sql query against the LookML model. The
          model, explore, and fields list must be specified. Pivots,
          filters and sorts are optional.

          The model can be found from the get_models tool. The explore
          can be found from the get_explores tool passing in the model.
          The fields can be found from the get_dimensions, get_measures,
          get_filters, and get_parameters tools, passing in the model
          and the explore.

          Provide a model_id and explore_name, then a list
          of fields. Optionally a list of pivots can be provided.
          The pivots must also be included in the fields list.

          Filters are provided as a map of {"field.id": "condition",
          "field.id2": "condition2", ...}. Do not put the field.id in
          quotes. Filter expressions can be found at
          https://cloud.google.com/looker/docs/filter-expressions.

          Sorts can be specified like [ "field.id desc 0" ].

          An optional row limit can be added. If not provided the limit
          will default to 500. "-1" can be specified for unlimited.

          An optional query timezone can be added. The query_timezone to
          will default to that of the workstation where this MCP server
          is running, or Etc/UTC if that can't be determined. Not all
          models support custom timezones.

          The result of the query tool is the sql string.
```

## Reference

| **field**   |                  **type**                  | **required** | **description**                                                                                  |
|-------------|:------------------------------------------:|:------------:|--------------------------------------------------------------------------------------------------|
| kind        |                   string                   |     true     | Must be "looker-query-sql"                                                                       |
| source      |                   string                   |     true     | Name of the source the SQL should execute on.                                                    |
| description |                   string                   |     true     | Description of the tool that is passed to the LLM.                                               |
