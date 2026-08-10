import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "../src/Canonical.js"
import * as Definition from "../src/Definition.js"
import * as Field from "../src/Field.js"
import * as Model from "../src/Model.js"
import * as Mutation from "../src/Mutation.js"
import * as Protocol from "../src/Protocol.js"
import * as Query from "../src/Query.js"

const Todo = Model.make("Todo", {
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { payload: Todo.schema, success: Todo.schema })
const ListTodos = Query.make("ListTodos", { success: Schema.Array(Todo.schema), dependsOn: [Todo] })

describe("domain contracts", () => {
  it("uses JSON null as the wire representation for void payloads and results", () => {
    const mutation = Mutation.make("Touch")
    const query = Query.make("Count", { dependsOn: [] })
    assert.strictEqual(Schema.encodeSync(mutation.payloadSchema)(undefined), null)
    assert.strictEqual(Schema.decodeUnknownSync(mutation.successSchema)(null), undefined)
    assert.strictEqual(Schema.encodeSync(query.payloadSchema)(undefined), null)
  })

  it("builds a stable definition hash from Schema contracts", () => {
    const first = Definition.make({ models: [Todo], mutations: [PutTodo], queries: [ListTodos] })
    const second = Definition.make({ models: [Todo], mutations: [PutTodo], queries: [ListTodos] })
    assert.strictEqual(first.hash, second.hash)
    assert.strictEqual(first.modelByName.get("Todo"), Todo)
  })

  it("rejects duplicate names and unregistered query dependencies", () => {
    assert.throws(() => Definition.make({ models: [Todo, Todo], mutations: [PutTodo] }), /Duplicate model name/)
    const Other = Model.make("Other", { key: Schema.String, schema: Schema.Struct({ id: Schema.String }) })
    const InvalidQuery = Query.make("Invalid", { dependsOn: [Other] })
    assert.throws(
      () => Definition.make({ models: [Todo], mutations: [PutTodo], queries: [InvalidQuery] }),
      /unregistered model/
    )
  })

  it("reserves protocol names", () => {
    assert.throws(() => Model.make("$Model", { key: Schema.String, schema: Schema.String }), /must not start/)
    assert.throws(() => Mutation.make("$Mutation"), /must not start/)
    assert.throws(() => Query.make("$Query", { dependsOn: [] }), /must not start/)
  })

  it.effect("applies opt in field semantics without replication metadata", () =>
    Effect.gen(function*() {
      assert.strictEqual(yield* Field.counter.apply(10, { _tag: "Increment", delta: 3 }), 13)
    }))

  it.effect("deduplicates grow only set values by canonical identity", () =>
    Effect.gen(function*() {
      const semantics = Field.growOnlySet(Schema.Struct({ id: Schema.String, value: Schema.Number }))
      assert.deepStrictEqual(
        yield* semantics.apply([{ id: "a", value: 1 }], { _tag: "Add", value: { value: 1, id: "a" } }),
        [{ id: "a", value: 1 }]
      )
    }))

  it("canonicalizes object order and enforces protocol page limits", () => {
    assert.strictEqual(Canonical.stringify({ b: 2, a: 1 }), Canonical.stringify({ a: 1, b: 2 }))
    assert.throws(() =>
      Schema.decodeUnknownSync(Protocol.PullRequest)({
        spaceId: "spc_00000000-0000-4000-8000-000000000001",
        after: 0,
        limit: Protocol.maximumBatchEntries + 1
      })
    )
  })
})
