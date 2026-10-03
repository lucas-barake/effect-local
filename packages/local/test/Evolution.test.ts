import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
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
class Missing extends Schema.TaggedError<Missing>(
  "@lucas-barake/effect-local/test/Evolution/Missing"
)("Missing", {}) {}
class Forbidden extends Schema.TaggedError<Forbidden>(
  "@lucas-barake/effect-local/test/Evolution/Forbidden"
)("Forbidden", {}) {}
const PutTodoV1 = Mutation.make("PutTodo", {
  version: 1,
  payload: TodoV1.schema,
  success: TodoV1.schema,
  rejection: Missing
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
  rejection: Schema.Union([Missing, Forbidden])
})
const definitionV2 = Definition.make({ version: 2, models: [TodoV2], mutations: [PutTodoV2] })

const todoMigration = Evolution.model({
  id: "todo/1-2",
  from: TodoV1,
  to: TodoV2,
  key: Number,
  value: ({ value }) => ({ ...value, done: false }),
  downgradeKey: String,
  downgradeValue: ({ value }) => ({ id: value.id, title: value.title })
})
const putTodoMigration = Evolution.mutation({
  id: "put-todo/1-2",
  from: PutTodoV1,
  to: PutTodoV2,
  payload: (payload) => ({ ...payload, done: false }),
  success: (success) => ({ ...success, done: false }),
  rejection: (rejection) => rejection,
  downgradePayload: ({ id, title }) => ({ id, title }),
  downgradeSuccess: ({ id, title }) => ({ id, title }),
  downgradeRejection: (rejection) => {
    if (rejection._tag === "Forbidden") return new Missing()
    return rejection
  }
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
  steps: [oneToTwo]
})

const expectFailure = <A, E,>(result: Result.Result<A, E>): E => {
  if (Result.isFailure(result)) return result.failure
  return assert.fail("expected Effect failure")
}

const PriceCents = Model.make("Price", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ amount: Schema.Number })
})
const PriceDollars = Model.make("Price", {
  version: 2,
  key: Schema.String,
  schema: Schema.Struct({ amount: Schema.Number })
})
const SkuV1 = Model.make("Sku", { version: 1, key: Schema.String, schema: Schema.Struct({ id: Schema.String }) })
const SkuV2 = Model.make("Sku", { version: 2, key: Schema.String, schema: Schema.Struct({ id: Schema.String }) })
const ChargeCents = Mutation.make("Charge", { version: 1, payload: Schema.String, success: Schema.Number })
const ChargeDollars = Mutation.make("Charge", { version: 2, payload: Schema.String, success: Schema.Number })
const centsDefinition = Definition.make({ version: 1, models: [PriceCents, SkuV1], mutations: [ChargeCents] })
const dollarsDefinition = Definition.make({ version: 2, models: [PriceDollars, SkuV2], mutations: [ChargeDollars] })
const centsToDollars = (reversible: boolean) => {
  if (reversible) {
    return Evolution.step({
      id: "price/cents-to-dollars",
      from: centsDefinition,
      to: dollarsDefinition,
      models: [
        Evolution.model({
          id: "price/cents-to-dollars",
          from: PriceCents,
          to: PriceDollars,
          value: ({ value }) => ({ amount: value.amount / 100 }),
          downgradeValue: ({ value }) => ({ amount: value.amount * 100 })
        }),
        Evolution.model({
          id: "sku/prefix",
          from: SkuV1,
          to: SkuV2,
          key: (key) => `sku:${key}`,
          downgradeKey: (key) => key.slice(4)
        })
      ],
      mutations: [Evolution.mutation({
        id: "charge/cents-to-dollars",
        from: ChargeCents,
        to: ChargeDollars,
        success: (cents) => cents / 100,
        downgradeSuccess: (dollars) => dollars * 100
      })]
    })
  }
  return Evolution.step({
    id: "price/cents-to-dollars",
    from: centsDefinition,
    to: dollarsDefinition,
    models: [
      Evolution.model({
        id: "price/cents-to-dollars",
        from: PriceCents,
        to: PriceDollars,
        value: ({ value }) => ({ amount: value.amount / 100 })
      }),
      Evolution.model({ id: "sku/prefix", from: SkuV1, to: SkuV2, key: (key) => `sku:${key}` })
    ],
    mutations: [Evolution.mutation({
      id: "charge/cents-to-dollars",
      from: ChargeCents,
      to: ChargeDollars,
      success: (cents) => cents / 100
    })]
  })
}

describe("schema evolution", () => {
  it.effect(
    "refuses to project a transformed part back without its reverse hook even when the schemas match",
    Effect.fnUntraced(function*() {
      const forwardOnly = Evolution.make({ current: dollarsDefinition, steps: [centsToDollars(false)] })
      const forward = yield* Evolution.migrateModel({
        evolution: forwardOnly,
        source: centsDefinition.schemaIdentity,
        model: "Price",
        modelVersion: Identity.SchemaVersion.make(1),
        key: "coffee",
        value: { amount: 100 }
      })
      assert.deepStrictEqual(forward.value, { amount: 1 })

      const price = yield* Evolution.migrateModelTo({
        evolution: forwardOnly,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        model: "Price",
        modelVersion: Identity.SchemaVersion.make(2),
        key: "coffee",
        value: { amount: 1 }
      }).pipe(Effect.result)
      assert.strictEqual(expectFailure(price)._tag, "SchemaEvolutionUnsupported")

      const sku = yield* Evolution.migrateModelTo({
        evolution: forwardOnly,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        model: "Sku",
        modelVersion: Identity.SchemaVersion.make(2),
        key: "sku:coffee"
      }).pipe(Effect.result)
      assert.strictEqual(expectFailure(sku)._tag, "SchemaEvolutionUnsupported")

      const charge = yield* Evolution.migrateMutationSuccessTo({
        evolution: forwardOnly,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        mutation: "Charge",
        mutationVersion: Identity.SchemaVersion.make(2),
        value: 1
      }).pipe(Effect.result)
      assert.strictEqual(expectFailure(charge)._tag, "SchemaEvolutionUnsupported")

      const admission = yield* Evolution.validateDowngradeTarget(forwardOnly, centsDefinition.schemaIdentity).pipe(
        Effect.result
      )
      assert.strictEqual(expectFailure(admission)._tag, "SchemaEvolutionUnsupported")
    })
  )

  it.effect(
    "projects a transformed part back through its explicit reverse hook",
    Effect.fnUntraced(function*() {
      const reversible = Evolution.make({ current: dollarsDefinition, steps: [centsToDollars(true)] })
      yield* Evolution.validateDowngradeTarget(reversible, centsDefinition.schemaIdentity)
      const price = yield* Evolution.migrateModelTo({
        evolution: reversible,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        model: "Price",
        modelVersion: Identity.SchemaVersion.make(2),
        key: "coffee",
        value: { amount: 1 }
      })
      assert.deepStrictEqual(price.value, { amount: 100 })
      const sku = yield* Evolution.migrateModelTo({
        evolution: reversible,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        model: "Sku",
        modelVersion: Identity.SchemaVersion.make(2),
        key: "sku:coffee"
      })
      assert.strictEqual(sku.key, "coffee")
      const charge = yield* Evolution.migrateMutationSuccessTo({
        evolution: reversible,
        source: dollarsDefinition.schemaIdentity,
        target: centsDefinition.schemaIdentity,
        mutation: "Charge",
        mutationVersion: Identity.SchemaVersion.make(2),
        value: 1
      })
      assert.strictEqual(charge.value, 100)
    })
  )

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
          error: Missing
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
      () =>
        Evolution.make({
          current: definitionV3,
          steps: [oneToTwo]
        }),
      /does not terminate/
    )
  })

  it.effect(
    "validates and transforms model keys, values, payloads, results, and rejections",
    Effect.fnUntraced(function*() {
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
        value: { _tag: "Missing" }
      })
      assert.deepStrictEqual(rejection.value, { _tag: "Missing" })
    })
  )

  it.effect(
    "projects models and mutation outcomes to an explicit older definition",
    Effect.fnUntraced(function*() {
      const model = yield* Evolution.migrateModelTo({
        evolution,
        source: definitionV2.schemaIdentity,
        target: definitionV1.schemaIdentity,
        model: "Todo",
        modelVersion: Identity.SchemaVersion.make(2),
        key: 42,
        value: { id: "42", title: "new", done: true }
      })
      assert.deepStrictEqual(model.schemaIdentity, definitionV1.schemaIdentity)
      assert.strictEqual(model.key, "42")
      assert.deepStrictEqual(model.value, { id: "42", title: "new" })
      assert.deepStrictEqual(model.aliases.map((alias) => alias.key), [42, "42"])

      const payload = yield* Evolution.migrateMutationPayloadTo({
        evolution,
        source: definitionV2.schemaIdentity,
        target: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(2),
        value: { id: "42", title: "new", done: true }
      })
      assert.deepStrictEqual(payload.schemaIdentity, definitionV1.schemaIdentity)
      assert.deepStrictEqual(payload.value, { id: "42", title: "new" })

      const success = yield* Evolution.migrateMutationSuccessTo({
        evolution,
        source: definitionV2.schemaIdentity,
        target: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(2),
        value: { id: "42", title: "new", done: true }
      })
      assert.deepStrictEqual(success.schemaIdentity, definitionV1.schemaIdentity)
      assert.deepStrictEqual(success.value, { id: "42", title: "new" })

      const rejection = yield* Evolution.migrateMutationRejectionTo({
        evolution,
        source: definitionV2.schemaIdentity,
        target: definitionV1.schemaIdentity,
        mutation: "PutTodo",
        mutationVersion: Identity.SchemaVersion.make(2),
        value: { _tag: "Forbidden" }
      })
      assert.deepStrictEqual(rejection.schemaIdentity, definitionV1.schemaIdentity)
      assert.deepStrictEqual(rejection.value, { _tag: "Missing" })
    })
  )

  it.effect(
    "reports invalid transform output as a typed failure with exact context",
    Effect.fnUntraced(function*() {
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
      const result = yield* Evolution.migrateModel({
        evolution: configured,
        source: definitionV1.schemaIdentity,
        model: "Todo",
        modelVersion: Identity.SchemaVersion.make(1),
        key: "42"
      }).pipe(Effect.result)
      assert.strictEqual(result._tag, "Failure")
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "SchemaEvolutionFailed")
        if (result.failure._tag === "SchemaEvolutionFailed") {
          assert.strictEqual(result.failure.stepId, "definition/invalid-output")
          assert.strictEqual(result.failure.part, "Key")
        }
      }
    })
  )

  it.effect(
    "preserves thrown migration failures as defects",
    Effect.fnUntraced(function*() {
      const defect = Error("migration implementation defect")
      const broken = Evolution.step({
        id: "definition/defect",
        from: definitionV1,
        to: definitionV2,
        models: [Evolution.model({
          id: "todo/defect",
          from: TodoV1,
          to: TodoV2,
          key: () => {
            // This migration hook is synchronous, and the test must exercise its thrown defect boundary.
            // oxlint-disable-next-line effect-local/noManualEffectBoundary
            return Effect.runSync(Effect.die(defect))
          },
          value: ({ value }) => ({ ...value, done: false })
        })],
        mutations: [putTodoMigration]
      })
      const configured = Evolution.make({ current: definitionV2, steps: [broken] })
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
  )
})
