---
title: "mongodb-delete-one"
type: docs
weight: 1
description: > 
  A "mongodb-delete-one" tool deletes a single document from a MongoDB collection.
aliases:
- /resources/tools/mongodb-delete-one
---

## About

The `mongodb-delete-one` tool performs a destructive operation, deleting the
**first single document** that matches a specified filter from a MongoDB
collection.

If the filter matches multiple documents, only the first one found by the
database will be deleted. This tool is useful for removing specific entries,
such as a user account or a single item from an inventory based on a unique ID.

The tool returns the number of documents deleted, which will be either `1` if a
document was found and deleted, or `0` if no matching document was found.

This tool is compatible with the following source kind:

* [`mongodb`](../../sources/mongodb.md)

---

## Example

Here is an example that deletes a specific user account from the `users`
collection by matching their unique email address. This is a permanent action.

```yaml
tools:
  delete_user_account:
    kind: mongodb-delete-one
    source: my-mongo-source
    description: Permanently deletes a user account by their email address.
    database: user_data
    collection: users
    filterPayload: |
        { "email": {{json .email_address}} }
    filterParams:
      - name: email_address
        type: string
        description: The email of the user account to delete.
```

## Reference

| **field**     | **type** | **required** | **description**                                                                                                    |
|:--------------|:---------|:-------------|:-------------------------------------------------------------------------------------------------------------------|
| kind          | string   | true         | Must be `mongodb-delete-one`.                                                                                      |
| source        | string   | true         | The name of the `mongodb` source to use.                                                                           |
| description   | string   | true         | A description of the tool that is passed to the LLM.                                                               |
| database      | string   | true         | The name of the MongoDB database containing the collection.                                                        |
| collection    | string   | true         | The name of the MongoDB collection from which to delete a document.                                                |
| filterPayload | string   | true         | The MongoDB query filter document to select the document for deletion. Uses `{{json .param_name}}` for templating. |
| filterParams  | list     | true         | A list of parameter objects that define the variables used in the `filterPayload`.                                 |
