---
title: "mongodb-find-one"
type: docs
weight: 1
description: > 
  A "mongodb-find-one" tool finds and retrieves a single document from a MongoDB collection.
aliases:
- /resources/tools/mongodb-find-one
---

## About

A `mongodb-find-one` tool is used to retrieve the **first single document** that
matches a specified filter from a MongoDB collection. If multiple documents
match the filter, you can use `sort` options to control which document is
returned. Otherwise, the selection is not guaranteed.

The tool returns a single JSON object representing the document, wrapped in a
JSON array.

This tool is compatible with the following source kind:

* [`mongodb`](../../sources/mongodb.md)

---

## Example

Here's a common use case: finding a specific user by their unique email address
and returning their profile information, while excluding sensitive fields like
the password hash.

```yaml
tools:
  get_user_profile:
    kind: mongodb-find-one
    source: my-mongo-source
    description: Retrieves a user's profile by their email address.
    database: user_data
    collection: profiles
    filterPayload: |
        { "email": {{json .email}} }
    filterParams:
      - name: email
        type: string
        description: The email address of the user to find.
    projectPayload: |
        { 
          "password_hash": 0,
          "login_history": 0
        }
```

## Reference

| **field**      | **type** | **required** | **description**                                                                                                                              |
|:---------------|:---------|:-------------|:---------------------------------------------------------------------------------------------------------------------------------------------|
| kind           | string   | true         | Must be `mongodb-find-one`.                                                                                                                  |
| source         | string   | true         | The name of the `mongodb` source to use.                                                                                                     |
| description    | string   | true         | A description of the tool that is passed to the LLM.                                                                                         |
| database       | string   | true         | The name of the MongoDB database to query.                                                                                                   |
| collection     | string   | true         | The name of the MongoDB collection to query.                                                                                                 |
| filterPayload  | string   | true         | The MongoDB query filter document to select the document. Uses `{{json .param_name}}` for templating.                                        |
| filterParams   | list     | true         | A list of parameter objects that define the variables used in the `filterPayload`.                                                           |
| projectPayload | string   | false        | An optional MongoDB projection document to specify which fields to include (1) or exclude (0) in the result.                                 |
| projectParams  | list     | false        | A list of parameter objects for the `projectPayload`.                                                                                        |
| sortPayload    | string   | false        | An optional MongoDB sort document. Useful for selecting which document to return if the filter matches multiple (e.g., get the most recent). |
| sortParams     | list     | false        | A list of parameter objects for the `sortPayload`.                                                                                           |
