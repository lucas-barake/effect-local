import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Definition from "../src/Definition.js"
import * as Evolution from "../src/Evolution.js"
import * as Identity from "../src/Identity.js"
import * as Model from "../src/Model.js"
import * as Mutation from "../src/Mutation.js"
import * as Query from "../src/Query.js"

const TodoV1 = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodoV1 = Mutation.make("PutTodo", {
  version: 1,
  payload: TodoV1.schema,
  success: TodoV1.schema,
  rejection: Schema.Literal("Missing")
})
const definitionV1 = Definition.make({ version: 1, models: [TodoV1], mutations: [PutTodoV1] })

const TodoV2 = Model.make("Todo", {
  version: 2,
  key: Schema.Number,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String, done: Schema.Boolean })
})
const PutTodoV2 = Mutation.make("PutTodo", {
  version: 2,
  payload: TodoV2.schema,
  success: TodoV2.schema,
  rejection: Schema.Literals(["Missing", "Forbidden"])
})
const definitionV2 = Definition.make({ version: 2, models: [TodoV2], mutations: [PutTodoV2] })

const todoMigration = Evolution.model({
  id: "todo/1-2",
  from: TodoV1,
  to: TodoV2,
  key: Number,
  value: ({ value }) => ({ ...value, done: false })
})
const putTodoMigration = Evolution.mutation({
  id: "put-todo/1-2",
  from: PutTodoV1,
  to: PutTodoV2,
  payload: (payload) => ({ ...payload, done: false }),
  success: (success) => ({ ...success, done: false }),
  rejection: (rejection) => rejection
})
const oneToTwo = Evolution.step({
  id: "definition/1-2",
  from: definitionV1,
  to: definitionV2,
  models: [todoMigration],
  mutations: [putTodoMigration]
})
const evolution = Evolution.make({
  current: definitionV2,
  steps: [oneToTwo],
  legacyBaselines: [Evolution.legacyBaseline({
    id: "mutation-log-v1",
    hash: "0123456789abcdef",
    definition: definitionV1
  })]
})

describe("schema evolution", () => {
  it("uses an order independent schema identity and excludes queries from it", () => {
    const First = Mutation.make("First", { version: 1 })
    const Second = Mutation.make("Second", { version: 1 })
    const left = Definition.make({ version: 1, models: [TodoV1], mutations: [First, Second] })
    const right = Definition.make({ version: 1, models: [TodoV1], mutations: [Second, First] })
    assert.deepStrictEqual(left.schemaIdentity, right.schemaIdentity)

    const withQuery = Definition.make({
      version: 1,
      models: [TodoV1],
      mutations: [First, Second],
      queries: [
        Query.make("ListTodos", {
          success: Schema.Array(TodoV1.schema),
          dependsOn: [TodoV1]
        })
      ]
    })
    assert.deepStrictEqual(left.schemaIdentity, withQuery.schemaIdentity)
    assert.notStrictEqual(left.hash, withQuery.hash)
  })

  it("requires complete contiguous forward definitions and exact component migrations", () => {
    assert.throws(
      () => Evolution.step({ id: "missing-components", from: definitionV1, to: definitionV2 }),
      /requires an exact source and target migration/
    )
    const definitionV3 = Definition.make({ version: 3, models: [TodoV2], mutations: [PutTodoV2] })
    assert.throws(
      () => Evolution.make({
        current: definitionV3,
        steps: [oneToTwo]
      }),
      /does not terminate/
    )
    assert.strictEqual(evolution.legacyBaselineByHash.get("0123456789abcdef")?.definition, definitionV1)
  })

  it.effect("validates and transforms model keys, values, payloads, results, and rejections", () =>
    Effect.gen(function*() {
      const model = yield* Evolution.migrateModel({
        evolution,
        source: definitionV1.schemaIdentity,
        model: "Todo",
        modelVersion: Identity.SchemaVersion.make(1),
        key: "42",
        value: { id: "42", title: "old" }
      })
      assert.strictEqual(model.key, 42)
      assert.deepStrictEqual(model.value, { id: "42", title: "old", done: false })
      assert.deepStrictEqual(model.aliases.map((alias) => alias.key), ["42", 42])

      const payload = yield* Evolution.migrateMutationPayload({
        evolution,
        source: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(1),
        value: { id: "42", title: "old" }
      })
      assert.deepStrictEqual(payload.value, { id: "42", title: "old", done: false })

      const success = yield* Evolution.migrateMutationSuccess({
        evolution,
        source: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(1),
        value: { id: "42", title: "old" }
      })
      assert.deepStrictEqual(success.value, { id: "42", title: "old", done: false })

      const rejection = yield* Evolution.migrateMutationRejection({
        evolution,
        source: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(1),
        value: "Missing"
      })
      assert.strictEqual(rejection.value, "Missing")
    }))

  it.effect("reports invalid transform output as a typed failure with exact context", () => {
    const invalid = Evolution.step({
      id: "definition/invalid-output",
      from: definitionV1,
      to: definitionV2,
      models: [Evolution.model({
        id: "todo/invalid-output",
        from: TodoV1,
        to: TodoV2,
        key: () => Number.NaN,
        value: ({ value }) => ({ ...value, done: false })
      })],
      mutations: [putTodoMigration]
    })
    const configured = Evolution.make({ current: definitionV2, steps: [invalid] })
    return Effect.gen(function*() {
      const error = yield* Evolution.migrateModel({
        evolution: configured,
        source: definitionV1.schemaIdentity,
        model: "Todo",
        modelVersion: Identity.SchemaVersion.make(1),
        key: "42"
      }).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SchemaEvolutionFailed")
      if (error._tag === "SchemaEvolutionFailed") {
        assert.strictEqual(error.stepId, "definition/invalid-output")
        assert.strictEqual(error.part, "Key")
      }
    })
  })

  it.effect("preserves thrown migration failures as defects", () => {
    const defect = new Error("migration implementation defect")
    const broken = Evolution.step({
      id: "definition/defect",
      from: definitionV1,
      to: definitionV2,
      models: [Evolution.model({
        id: "todo/defect",
        from: TodoV1,
        to: TodoV2,
        key: () => {
          throw defect
        },
        value: ({ value }) => ({ ...value, done: false })
      })],
      mutations: [putTodoMigration]
    })
    const configured = Evolution.make({ current: definitionV2, steps: [broken] })
    return Effect.gen(function*() {
      const exit = yield* Evolution.migrateModel({
        evolution: configured,
        source: definitionV1.schemaIdentity,
        model: "Todo",
        modelVersion: Identity.SchemaVersion.make(1),
        key: "42"
      }).pipe(Effect.exit)
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        assert.strictEqual(Cause.squash(exit.cause), defect)
      }
    })
  })
})
