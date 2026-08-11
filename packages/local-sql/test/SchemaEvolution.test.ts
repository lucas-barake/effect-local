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
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as SchemaEvolution from "../src/SchemaEvolution.js"
import * as ServerStore from "../src/ServerStore.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}
const serverHistory = {
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}

const incremental = (result: Protocol.PullResult): Protocol.PullPage => {
  if ("_tag" in result) assert.fail(`Unexpected bootstrap ${result.manifest.snapshotId}`)
  return result
}

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

const TodoV3 = Model.make("Todo", {
  version: 3,
  key: Schema.Number,
  schema: Schema.Struct({ id: Schema.Number, title: Schema.String, done: Schema.Boolean, priority: Schema.Number })
})
const PutTodoV3 = Mutation.make("PutTodo", {
  version: 3,
  payload: TodoV3.schema,
  success: TodoV3.schema
})
const definitionV3 = Definition.make({ version: 3, models: [TodoV3], mutations: [PutTodoV3] })
const handlersV3 = PutTodoV3.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV3, payload.id, payload).pipe(Effect.as(payload))
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

const legacyBaselineV1 = Evolution.legacyBaseline({
  id: "legacy-v1",
  hash: "1111111111111111",
  definition: definitionV1
})
const evolutionWithLegacyBaseline = Evolution.make({
  current: definitionV2,
  steps: evolution.steps,
  legacyBaselines: [legacyBaselineV1]
})
const ambiguousLegacyEvolution = Evolution.make({
  current: definitionV2,
  steps: evolution.steps,
  legacyBaselines: [
    legacyBaselineV1,
    Evolution.legacyBaseline({ id: "legacy-v2", hash: "2222222222222222", definition: definitionV2 })
  ]
})

const evolutionV3 = Evolution.make({
  current: definitionV3,
  steps: [
    ...evolution.steps,
    Evolution.step({
      id: "definition/2-to-3",
      from: definitionV2,
      to: definitionV3,
      models: [Evolution.model({
        id: "todo/2-to-3",
        from: TodoV2,
        to: TodoV3,
        value: ({ value }) => ({ ...value, priority: 0 })
      })],
      mutations: [Evolution.mutation({
        id: "put-todo/2-to-3",
        from: PutTodoV2,
        to: PutTodoV3,
        payload: (payload) => ({ ...payload, priority: 0 }),
        success: (success) => ({ ...success, priority: 0 })
      })]
    })
  ]
})

const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

const buildStore = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  configuredEvolution?: Evolution.Evolution,
  storeClientId: Identity.ClientId = clientId
) => {
  const runtime = MutationRuntime.layer(definition, configuredEvolution).pipe(Layer.provide(handlers))
  return Layer.build(
    LocalStore.layer({
      ...clientHistory,
      definition,
      spaceId,
      clientId: storeClientId,
      ...(configuredEvolution === undefined ? {} : { evolution: configuredEvolution }),
      schemaEvolutionBatchSize: 1
    }).pipe(Layer.provide(runtime))
  ).pipe(
    Effect.map((context) => Context.get(context, LocalStore.Store))
  )
}

const buildServer = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  configuredEvolution?: Evolution.Evolution
) => {
  const runtime = MutationRuntime.layer(definition, configuredEvolution).pipe(Layer.provide(handlers))
  return Layer.build(
    ServerStore.layerTrusted({
      ...serverHistory,
      definition,
      ...(configuredEvolution === undefined ? {} : { evolution: configuredEvolution }),
      schemaEvolutionBatchSize: 1
    }).pipe(Layer.provide(runtime))
  ).pipe(
    Effect.map((context) => Context.get(context, ServerStore.ServerStore))
  )
}

const v1Envelope = (
  envelopeClientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: number,
  payload: typeof TodoV1.schema.Type
) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: envelopeClientId,
      mutationId,
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name: PutTodoV1.name,
      payload,
      digestVersion: 2 as const,
      sourceSchema: definitionV1.schemaIdentity,
      mutationVersion: PutTodoV1.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const legacyV1Envelope = (
  mutationId: Identity.MutationId,
  payload: typeof TodoV1.schema.Type,
  localSequence = 1
) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId,
      mutationId,
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name: PutTodoV1.name,
      payload,
      digestVersion: 1 as const,
      sourceSchema: definitionV1.schemaIdentity,
      mutationVersion: PutTodoV1.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

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
      assert.strictEqual(promotedPending[0].envelope.digest, pending.envelope.digest)
      assert.deepStrictEqual(promotedPending[0].envelope.sourceSchema, definitionV1.schemaIdentity)
      assert.deepStrictEqual(promotedPending[0].optimisticResult, {
        id: 2,
        title: "pending",
        done: false
      })
      assert.strictEqual(yield* v2.cursor, 1)
      assert.strictEqual((yield* v1.get(TodoV1, "1").pipe(Effect.flip))._tag, "SchemaGenerationConflict")
    })).pipe(Effect.provide(database)))

  it.effect("replays an old pending envelope after applying a current accepted entry", () =>
    Effect.scoped(Effect.gen(function*() {
      const v1 = yield* buildStore(definitionV1, handlersV1)
      yield* v1.mutate(PutTodoV1, { id: "2", title: "pending" })
      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      yield* v2.applyEntries([Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000201"),
        localSequence: Identity.LocalSequence.make(1),
        sourceSchema: definitionV2.schemaIdentity,
        digest: "1".repeat(64),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV2.name, modelVersion: TodoV2.version, key: 20 },
          value: { id: 20, title: "remote", done: true }
        })]
      })])

      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 2)), {
        id: 2,
        title: "pending",
        done: false
      })
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 20)), {
        id: 20,
        title: "remote",
        done: true
      })
      assert.strictEqual(yield* v2.cursor, 1)
    })).pipe(Effect.provide(database)))

  it.effect("promotes pending optimistic changes across multiple schema versions", () =>
    Effect.scoped(Effect.gen(function*() {
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const pending = yield* v1.mutate(PutTodoV1, { id: "3", title: "multi-hop" })
      yield* buildStore(definitionV2, handlersV2, evolution)
      const v3 = yield* buildStore(definitionV3, handlersV3, evolutionV3)

      assert.deepStrictEqual((yield* v3.pending)[0]!.envelope, pending.envelope)
      assert.deepStrictEqual(Option.getOrThrow(yield* v3.get(TodoV3, 3)), {
        id: 3,
        title: "multi-hop",
        done: false,
        priority: 0
      })
    })).pipe(Effect.provide(database)))

  it.effect("rejects a different client before it can promote the owned database", () =>
    Effect.scoped(Effect.gen(function*() {
      const owner = yield* buildStore(definitionV1, handlersV1)
      yield* owner.mutate(PutTodoV1, { id: "4", title: "owned" })
      const otherClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")

      const error = yield* buildStore(definitionV2, handlersV2, evolution, otherClientId).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ReplicaIdentityMismatch")
      assert.deepStrictEqual(Option.getOrThrow(yield* owner.get(TodoV1, "4")), { id: "4", title: "owned" })
    })).pipe(Effect.provide(database)))

  it.effect("rejects a later historical key that collides with promoted lineage", () =>
    Effect.scoped(Effect.gen(function*() {
      const remoteClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const v1 = yield* buildStore(definitionV1, handlersV1)
      yield* v1.applyEntries([Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId: remoteClientId,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000202"),
        localSequence: Identity.LocalSequence.make(1),
        sourceSchema: definitionV1.schemaIdentity,
        digest: "2".repeat(64),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "01" },
          value: { id: "01", title: "original" }
        })]
      })])
      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      const collision = Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(2),
        spaceId,
        clientId: remoteClientId,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000203"),
        localSequence: Identity.LocalSequence.make(2),
        sourceSchema: definitionV1.schemaIdentity,
        digest: "3".repeat(64),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "1" },
          value: { id: "1", title: "collision" }
        })]
      })

      assert.strictEqual((yield* v2.applyEntries([collision]).pipe(Effect.flip))._tag, "SchemaKeyCollision")
      assert.strictEqual(yield* v2.cursor, 1)
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 1)), {
        id: 1,
        title: "original",
        done: false
      })
    })).pipe(Effect.provide(database)))

  it.effect("rejects receipt JSON that conflicts with its durable SQL identity", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const pending = yield* v1.mutate(PutTodoV1, { id: "5", title: "receipt" })
      const receipt = Protocol.AcceptedReceipt.make({
        spaceId,
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        name: PutTodoV1.name,
        sourceSchema: definitionV1.schemaIdentity,
        mutationVersion: PutTodoV1.version,
        serverSequence: Identity.ServerSequence.make(1),
        result: pending.optimisticResult
      })
      yield* v1.applyReceipt(receipt)
      const conflicting = Protocol.AcceptedReceipt.make({
        ...receipt,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000999")
      })
      yield* sql`UPDATE effect_local_receipts SET receipt_json = ${JSON.stringify(conflicting)}`

      const result = yield* buildStore(definitionV2, handlersV2, evolution).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "StorageCorrupt")
    })).pipe(Effect.provide(database)))

  it.effect("rejects log JSON that conflicts with its durable SQL identity", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const remoteClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const entry = Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId: remoteClientId,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000204"),
        localSequence: Identity.LocalSequence.make(1),
        sourceSchema: definitionV1.schemaIdentity,
        digest: "4".repeat(64),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "6" },
          value: { id: "6", title: "log" }
        })]
      })
      yield* v1.applyEntries([entry])
      const conflicting = Protocol.AcceptedMutation.make({
        ...entry,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000999")
      })
      yield* sql`UPDATE effect_local_server_log SET entry_json = ${JSON.stringify(conflicting)}`

      const result = yield* buildStore(definitionV2, handlersV2, evolution).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "StorageCorrupt")
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
      assert.strictEqual((yield* v2.pending)[0].envelope.digest, pending.envelope.digest)
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

  it.effect("promotes a server space and admits an old offline envelope through the current handler", () =>
    Effect.scoped(Effect.gen(function*() {
      const localV1 = yield* buildStore(definitionV1, handlersV1)
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const accepted = yield* localV1.mutate(PutTodoV1, { id: "4", title: "accepted-old" })
      assert.strictEqual((yield* serverV1.submit(accepted.envelope))._tag, "Accepted")
      const offline = yield* localV1.mutate(PutTodoV1, { id: "5", title: "offline-old" })

      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      const historical = yield* serverV2.pull({
        spaceId,
        schema: definitionV2.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      assert.deepStrictEqual(incremental(historical).entries[0].sourceSchema, definitionV1.schemaIdentity)

      const offlineReceipt = yield* serverV2.submit({
        envelope: offline.envelope,
        schema: definitionV2.schemaIdentity
      })
      assert.strictEqual(offlineReceipt._tag, "Accepted")
      if (offlineReceipt._tag === "Accepted") {
        assert.deepStrictEqual(offlineReceipt.sourceSchema, definitionV2.schemaIdentity)
        assert.deepStrictEqual(offlineReceipt.result, { id: 5, title: "offline-old", done: false })
      }
      const current = yield* serverV2.pull({
        spaceId,
        schema: definitionV2.schemaIdentity,
        after: Identity.ServerSequence.make(1),
        limit: 10
      })
      assert.deepStrictEqual(incremental(current).entries[0].sourceSchema, definitionV2.schemaIdentity)
      assert.deepStrictEqual(incremental(current).entries[0].changes[0], {
        _tag: "Upsert",
        entity: { model: "Todo", modelVersion: TodoV2.version, key: 5 },
        value: { id: 5, title: "offline-old", done: false }
      })
    })).pipe(Effect.provide(database)))

  it.effect("rejects a first legacy digest when configured baselines have distinct schema identities", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* buildServer(definitionV2, handlersV2, ambiguousLegacyEvolution)
      const envelope = yield* legacyV1Envelope(
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000205"),
        { id: "7", title: "ambiguous" }
      )

      assert.strictEqual((yield* server.submit(envelope).pipe(Effect.flip))._tag, "ProtocolInvalid")
    })).pipe(Effect.provide(database)))

  it.effect("keeps the legacy mutation byte limit for a digest version one envelope", () =>
    Effect.scoped(Effect.gen(function*() {
      const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000206")
      const legacyWithoutTitle = {
        spaceId,
        clientId,
        mutationId,
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: PutTodoV1.name,
        payload: { id: "8", title: "" },
        digest: "0".repeat(64)
      }
      const title = "x".repeat(Protocol.maximumMutationBytes - Protocol.encodedBytes(legacyWithoutTitle))
      const envelope = yield* legacyV1Envelope(mutationId, { id: "8", title })
      const legacyWire = { ...legacyWithoutTitle, digest: envelope.digest, payload: envelope.payload }
      assert.strictEqual(Protocol.encodedBytes(legacyWire), Protocol.maximumMutationBytes)
      assert.isAbove(Protocol.encodedBytes(envelope), Protocol.maximumMutationBytes)

      const server = yield* buildServer(definitionV2, handlersV2, evolutionWithLegacyBaseline)
      assert.strictEqual((yield* server.submit(envelope))._tag, "Accepted")
    })).pipe(Effect.provide(database)))

  it.effect("retains the legacy baseline for exact retries and later offline mutations", () =>
    Effect.scoped(Effect.gen(function*() {
      const first = yield* legacyV1Envelope(
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000207"),
        { id: "9", title: "retry" }
      )
      const serverV1 = yield* buildServer(
        definitionV1,
        handlersV1,
        Evolution.make({ current: definitionV1, legacyBaselines: [legacyBaselineV1] })
      )
      assert.strictEqual((yield* serverV1.submit(first))._tag, "Accepted")

      const serverV2 = yield* buildServer(definitionV2, handlersV2, ambiguousLegacyEvolution)
      yield* serverV2.pull({
        spaceId,
        schema: definitionV2.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      assert.strictEqual(
        (yield* serverV2.submit({ envelope: first, schema: definitionV2.schemaIdentity }))._tag,
        "Accepted"
      )

      const second = yield* legacyV1Envelope(
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000208"),
        { id: "10", title: "offline" },
        2
      )
      assert.strictEqual(
        (yield* serverV2.submit({ envelope: second, schema: definitionV2.schemaIdentity }))._tag,
        "Accepted"
      )
    })).pipe(Effect.provide(database)))

  it.effect("does not replace a persisted legacy identity with another configured baseline", () =>
    Effect.scoped(Effect.gen(function*() {
      const first = yield* legacyV1Envelope(
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000209"),
        { id: "11", title: "first" }
      )
      const server = yield* buildServer(definitionV2, handlersV2, evolutionWithLegacyBaseline)
      assert.strictEqual((yield* server.submit(first))._tag, "Accepted")

      const droppedV1 = Evolution.make({
        current: definitionV2,
        legacyBaselines: [Evolution.legacyBaseline({
          id: "legacy-v2-only",
          hash: "3333333333333333",
          definition: definitionV2
        })]
      })
      const restarted = yield* buildServer(definitionV2, handlersV2, droppedV1)
      const second = yield* legacyV1Envelope(
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000210"),
        { id: "12", title: "second" },
        2
      )
      assert.strictEqual((yield* restarted.submit(second).pipe(Effect.flip))._tag, "ProtocolInvalid")
    })).pipe(Effect.provide(database)))

  it.effect("resumes a server promotion after interruption", () =>
    Effect.scoped(Effect.gen(function*() {
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const submitted = yield* v1Envelope(
        clientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000101"),
        1,
        { id: "6", title: "resume-server" }
      )
      yield* serverV1.submit(submitted)
      const reached = yield* Deferred.make<void>()
      const fiber = yield* SchemaEvolution.server({
        definition: definitionV2,
        evolution,
        spaceId,
        batchSize: 1,
        afterBatch: Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never))
      }).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)

      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      const page = yield* serverV2.pull({
        spaceId,
        schema: definitionV2.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      assert.deepStrictEqual(incremental(page).entries[0].sourceSchema, definitionV1.schemaIdentity)
      assert.deepStrictEqual(incremental(page).entries[0].changes[0], {
        _tag: "Upsert",
        entity: { model: "Todo", modelVersion: TodoV1.version, key: "6" },
        value: { id: "6", title: "resume-server" }
      })
    })).pipe(Effect.provide(database)))

  it.effect("migrates every server receipt when client sequences overlap", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const first = yield* v1Envelope(
        clientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000102"),
        1,
        { id: "7", title: "first-client" }
      )
      const second = yield* v1Envelope(
        secondClientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000103"),
        1,
        { id: "8", title: "second-client" }
      )
      yield* serverV1.submit(first)
      yield* serverV1.submit(second)
      yield* sql`UPDATE effect_local_server_receipts SET source_schema_version = NULL,
        source_schema_hash = NULL, mutation_version = NULL, mutation_name = NULL`

      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      yield* serverV2.pull({
        spaceId,
        schema: definitionV2.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      const CountRow = Schema.Struct({
        total: Schema.Number,
        schema_count: Schema.Number,
        mutation_count: Schema.Number
      })
      const migrated = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () =>
          sql`SELECT COUNT(*) AS total,
          COUNT(source_schema_version) AS schema_count, COUNT(mutation_version) AS mutation_count
          FROM effect_local_server_receipts`
      })(undefined)
      assert.deepStrictEqual(migrated, { total: 2, schema_count: 2, mutation_count: 2 })
    })).pipe(Effect.provide(database)))

  it.effect("authorizes the migrated current mutation view", () =>
    Effect.scoped(Effect.gen(function*() {
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const historical = yield* v1Envelope(
        clientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000104"),
        1,
        { id: "9", title: "historical" }
      )
      yield* serverV1.submit(historical)
      const offline = yield* v1Envelope(
        clientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000105"),
        2,
        { id: "10", title: "authorized" }
      )
      const authorizedPayload = yield* Ref.make<Schema.Json>(null)
      const runtime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const server = yield* Layer.build(
        ServerStore.layer({
          ...serverHistory,
          definition: definitionV2,
          evolution,
          authorizeAccess: () => Effect.void,
          authorizeMutation: ({ mutation }) => Ref.set(authorizedPayload, mutation.payload),
          authorizeRead: () => Effect.void
        }).pipe(Layer.provide(runtime))
      ).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const receipt = yield* server.admit({ envelope: offline, schema: definitionV2.schemaIdentity }, "principal")
      assert.strictEqual(receipt._tag, "Accepted")
      assert.deepStrictEqual(yield* Ref.get(authorizedPayload), { id: 10, title: "authorized", done: false })
    })).pipe(Effect.provide(database)))

  it.effect("authorizes reads before schema disclosure or lazy space creation", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const runtime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const server = yield* Layer.build(
        ServerStore.layer({
          ...serverHistory,
          definition: definitionV2,
          evolution,
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: ({ principal }) => principal === "allowed" ? Effect.void : Effect.fail("denied")
        }).pipe(Layer.provide(runtime))
      ).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const stale = {
        spaceId,
        schema: definitionV1.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      }
      assert.strictEqual((yield* server.pullAuthorized(stale, "denied").pipe(Effect.flip))._tag, "AuthorizationDenied")
      const CountRow = Schema.Struct({ count: Schema.Number })
      const countSpaces = SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_server_spaces`
      })
      assert.strictEqual((yield* countSpaces(undefined)).count, 0)
      assert.strictEqual((yield* server.pullAuthorized(stale, "allowed").pipe(Effect.flip))._tag, "StaleSchema")
      assert.strictEqual((yield* countSpaces(undefined)).count, 0)
    })).pipe(Effect.provide(database)))
})
