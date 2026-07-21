import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import { Document, Mutation, Query } from "../src/index.js"

type Equal<A, B,> = (<T,>() => T extends A ? 1 : 2) extends <T,>() => T extends B ? 1 : 2 ? true : false

describe("public API types", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.Struct({ title: Schema.String }),
    success: Schema.Boolean,
    error: Schema.String
  })
  const Read = Query.make("Read", {
    payload: Schema.String,
    success: Schema.Array(Task.schema),
    error: Schema.String,
    dependsOn: []
  })

  it("preserves document, mutation, and query inference", () => {
    const documentDecoded: Equal<typeof Task.schema.Type, { readonly title: string }> = true
    const mutationPayload: Equal<typeof Rename.payload.Type, { readonly title: string }> = true
    const mutationSuccess: Equal<typeof Rename.success.Type, boolean> = true
    const mutationError: Equal<typeof Rename.error.Type, string> = true
    const queryPayload: Equal<typeof Read.payload.Type, string> = true
    const querySuccess: Equal<typeof Read.success.Type, ReadonlyArray<{ readonly title: string }>> = true
    assert.isTrue(documentDecoded)
    assert.isTrue(mutationPayload)
    assert.isTrue(mutationSuccess)
    assert.isTrue(mutationError)
    assert.isTrue(queryPayload)
    assert.isTrue(querySuccess)
  })
})
