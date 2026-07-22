import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import { Document, DocumentSet, Mutation, Query } from "../src/index.js"

type Equal<A, B,> = (<T,>() => T extends A ? 1 : 2) extends <T,>() => T extends B ? 1 : 2 ? true : false

describe("public API types", () => {
  class ReadError extends Schema.TaggedErrorClass<ReadError>()("ReadError", {}) {}
  class RenameError extends Schema.TaggedErrorClass<RenameError>()("RenameError", {}) {}
  const DomainError = Schema.Union([ReadError, RenameError])

  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Note = Document.make("Note", {
    schema: Schema.Struct({ body: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.Struct({ title: Schema.String }),
    success: Schema.Boolean,
    error: RenameError
  })
  const Read = Query.make("Read", {
    payload: Schema.String,
    success: Schema.Array(Task.schema),
    error: ReadError,
    dependsOn: []
  })
  const RenameWithUnion = Mutation.make("RenameWithUnion", {
    document: Task,
    payload: Schema.Struct({ title: Schema.String }),
    error: DomainError
  })
  const ReadWithUnion = Query.make("ReadWithUnion", {
    success: Schema.Array(Task.schema),
    error: DomainError,
    dependsOn: []
  })

  it("preserves document, mutation, and query inference", () => {
    const documentDecoded: Equal<typeof Task.schema.Type, { readonly title: string }> = true
    const mutationPayload: Equal<typeof Rename.payloadSchema.Type, { readonly title: string }> = true
    const mutationSuccess: Equal<typeof Rename.successSchema.Type, boolean> = true
    const mutationError: Equal<typeof Rename.errorSchema.Type, RenameError> = true
    const queryPayload: Equal<typeof Read.payloadSchema.Type, string> = true
    const querySuccess: Equal<typeof Read.successSchema.Type, ReadonlyArray<{ readonly title: string }>> = true
    const queryError: Equal<typeof Read.errorSchema.Type, ReadError> = true
    const mutationErrorUnion: Equal<typeof RenameWithUnion.errorSchema.Type, ReadError | RenameError> = true
    const queryErrorUnion: Equal<typeof ReadWithUnion.errorSchema.Type, ReadError | RenameError> = true
    assert.isTrue(documentDecoded)
    assert.isTrue(mutationPayload)
    assert.isTrue(mutationSuccess)
    assert.isTrue(mutationError)
    assert.isTrue(queryPayload)
    assert.isTrue(querySuccess)
    assert.isTrue(queryError)
    assert.isTrue(mutationErrorUnion)
    assert.isTrue(queryErrorUnion)
  })

  it("narrows document lookup by literal name", () => {
    const documents = DocumentSet.make(Task, Note)
    const task = DocumentSet.get(documents, "Task")
    const name: string = "Task"
    const dynamic = DocumentSet.get(documents, name)
    const literalLookup: Equal<typeof task, typeof Task | undefined> = true
    const dynamicLookup: Equal<typeof dynamic, typeof Task | typeof Note | undefined> = true
    assert.isTrue(literalLookup)
    assert.isTrue(dynamicLookup)
  })
})
