import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as SchemaEvolution from "../src/SchemaEvolution.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const TodoV1 = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodoV1 = Mutation.make("PutTodo", {
  version: 1,
  payload: TodoV1.schema,
  success: TodoV1.schema
})
const definitionV1 = Definition.make({ version: 1, models: [TodoV1], mutations: [PutTodoV1] })
const handlersV1 = PutTodoV1.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV1, payload.id, payload).pipe(Effect.as(payload))
)

const TodoV2 = Model.make("Todo", {
  version: 2,
  key: Schema.Number,
  schema: Schema.Struct({ id: Schema.Number, title: Schema.String, done: Schema.Boolean })
})
const PutTodoV2 = Mutation.make("PutTodo", {
  version: 2,
  payload: TodoV2.schema,
  success: TodoV2.schema
})
const definitionV2 = Definition.make({ version: 2, models: [TodoV2], mutations: [PutTodoV2] })
const handlersV2 = PutTodoV2.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV2, payload.id, payload).pipe(Effect.as(payload))
)

const evolution = Evolution.make({
  current: definitionV2,
  steps: [Evolution.step({
    id: "definition/1-to-2",
    from: definitionV1,
    to: definitionV2,
    models: [Evolution.model({
      id: "todo/1-to-2",
      from: TodoV1,
      to: TodoV2,
      key: Number,
      value: ({ value }) => ({ id: Number(value.id), title: value.title, done: false })
    })],
    mutations: [Evolution.mutation({
      id: "put-todo/1-to-2",
      from: PutTodoV1,
      to: PutTodoV2,
      payload: (payload) => ({ id: Number(payload.id), title: payload.title, done: false }),
      success: (success) => ({ id: Number(success.id), title: success.title, done: false })
    })]
  })]
})

const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

const buildStore = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  configuredEvolution?: Evolution.Evolution
) => {
  const runtime = MutationRuntime.layer(definition, configuredEvolution).pipe(Layer.provide(handlers))
  return Layer.build(
    LocalStore.layer({
      definition,
      spaceId,
      clientId,
      ...(configuredEvolution === undefined ? {} : { evolution: configuredEvolution }),
      schemaEvolutionBatchSize: 1
    }).pipe(Layer.provide(runtime))
  ).pipe(
    Effect.map((context) => Context.get(context, LocalStore.Store))
  )
}

describe("client schema evolution", () => {
  it.effect("atomically promotes canonical state, receipts, and replayed pending mutations", () =>
    Effect.scoped(Effect.gen(function*() {
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const accepted = yield* v1.mutate(PutTodoV1, { id: "1", title: "accepted" })
      yield* v1.applyReceipt(Protocol.AcceptedReceipt.make({
        spaceId,
        clientId,
        mutationId: accepted.envelope.mutationId,
        localSequence: accepted.envelope.localSequence,
        name: PutTodoV1.name,
        sourceSchema: definitionV1.schemaIdentity,
        mutationVersion: PutTodoV1.version,
        serverSequence: Identity.ServerSequence.make(1),
        result: accepted.optimisticResult
      }))
      yield* v1.applyEntries([Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId,
        mutationId: accepted.envelope.mutationId,
        localSequence: accepted.envelope.localSequence,
        sourceSchema: definitionV1.schemaIdentity,
        digest: accepted.envelope.digest,
        changes: accepted.changes
      })])
      const pending = yield* v1.mutate(PutTodoV1, { id: "2", title: "pending" })

      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 1)), {
        id: 1,
        title: "accepted",
        done: false
      })
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 2)), {
        id: 2,
        title: "pending",
        done: false
      })

      const promotedReceipt = Option.getOrThrow(yield* v2.receipt(accepted.envelope.mutationId))
      assert.strictEqual(promotedReceipt._tag, "Accepted")
      if (promotedReceipt._tag === "Accepted") {
        assert.deepStrictEqual(promotedReceipt.sourceSchema, definitionV2.schemaIdentity)
        assert.deepStrictEqual(promotedReceipt.result, { id: 1, title: "accepted", done: false })
      }

      const promotedPending = yield* v2.pending
      assert.strictEqual(promotedPending.length, 1)
      assert.strictEqual(promotedPending[0]!.envelope.digest, pending.envelope.digest)
      assert.deepStrictEqual(promotedPending[0]!.envelope.sourceSchema, definitionV1.schemaIdentity)
      assert.deepStrictEqual(promotedPending[0]!.optimisticResult, {
        id: 2,
        title: "pending",
        done: false
      })
      assert.strictEqual(yield* v2.cursor, 1)
      assert.strictEqual((yield* v1.get(TodoV1, "1").pipe(Effect.flip))._tag, "SchemaGenerationConflict")
    })).pipe(Effect.provide(database)))

  it.effect("resumes committed shadow progress after interruption", () =>
    Effect.scoped(Effect.gen(function*() {
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const pending = yield* v1.mutate(PutTodoV1, { id: "3", title: "resume" })
      const reached = yield* Deferred.make<void>()
      const runtimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const fiber = yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 1,
        afterBatch: Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never))
      }).pipe(Effect.provide(runtimeV2), Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)

      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 3)), {
        id: 3,
        title: "resume",
        done: false
      })
      assert.strictEqual((yield* v2.pending)[0]!.envelope.digest, pending.envelope.digest)
    })).pipe(Effect.provide(database)))

  it.effect("rejects unrelated source keys that converge on one target", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const v1 = yield* buildStore(definitionV1, handlersV1)
      yield* v1.mutate(PutTodoV1, { id: "01", title: "first" })
      yield* v1.mutate(PutTodoV1, { id: "1", title: "second" })

      const error = yield* buildStore(definitionV2, handlersV2, evolution).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SchemaKeyCollision")
      const CountRow = Schema.Struct({ count: Schema.Number })
      const source = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_pending`
      })(undefined)
      const published = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () =>
          sql`SELECT COUNT(*) AS count FROM effect_local_visible_entities
          WHERE model_version = ${TodoV2.version}`
      })(undefined)
      assert.strictEqual(source.count, 2)
      assert.strictEqual(published.count, 0)
    })).pipe(Effect.provide(database)))
})
