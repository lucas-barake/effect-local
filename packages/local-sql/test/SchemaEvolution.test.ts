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
import * as Codec from "../src/internal/codec.js"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as SchemaEvolution from "../src/SchemaEvolution.js"
import * as ServerStore from "../src/ServerStore.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  scope: Protocol.ReplicationScope.make({ models: ["Todo"] }),
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}
const serverHistory = {
  readAuthorizationRefreshInterval: "1 second" as const,
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
const replicationScope = Protocol.ReplicationScope.make({ models: [TodoV2.name] })
const pullRequest = (
  definition: Definition.Any,
  cursor: Protocol.ReplicationCursor | null = null
): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId,
    schema: definition.schemaIdentity,
    scope: replicationScope,
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor,
    limit: 10
  })
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
  let options: LocalStore.Options = {
    ...clientHistory,
    definition,
    spaceId,
    clientId: storeClientId,
    schemaEvolutionBatchSize: 1
  }
  if (configuredEvolution !== undefined) options = { ...options, evolution: configuredEvolution }
  return Layer.build(
    LocalStore.layer(options).pipe(Layer.provide(runtime))
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
  let options: Parameters<typeof ServerStore.layerTrusted>[0] = {
    ...serverHistory,
    definition,
    schemaEvolutionBatchSize: 1
  }
  if (configuredEvolution !== undefined) options = { ...options, evolution: configuredEvolution }
  return Layer.build(
    ServerStore.layerTrusted(options).pipe(Layer.provide(runtime))
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

describe("mutation digest evolution", () => {
  it.effect("preserves legacy digests and binds schema provenance in version 2", () =>
    Effect.gen(function*() {
      const common = {
        spaceId,
        clientId,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000201"),
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: PutTodoV1.name,
        payload: { id: "1", title: "digest" }
      }
      const legacyV1 = yield* Protocol.mutationDigest({
        ...common,
        digestVersion: 1,
        sourceSchema: definitionV1.schemaIdentity,
        mutationVersion: PutTodoV1.version
      })
      const legacyV2 = yield* Protocol.mutationDigest({
        ...common,
        digestVersion: 1,
        sourceSchema: definitionV2.schemaIdentity,
        mutationVersion: PutTodoV2.version
      })
      const currentV1 = yield* Protocol.mutationDigest({
        ...common,
        digestVersion: 2,
        sourceSchema: definitionV1.schemaIdentity,
        mutationVersion: PutTodoV1.version
      })
      const currentV2 = yield* Protocol.mutationDigest({
        ...common,
        digestVersion: 2,
        sourceSchema: definitionV2.schemaIdentity,
        mutationVersion: PutTodoV2.version
      })

      assert.strictEqual(legacyV1, legacyV2)
      assert.notStrictEqual(currentV1, currentV2)
      assert.match(currentV1, /^[0-9a-f]{64}$/)
    }).pipe(Effect.provide(NodeCrypto.layer)))
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

      assert.deepStrictEqual((yield* v3.pending)[0].envelope, pending.envelope)
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
      yield* sql`UPDATE effect_local_receipts SET receipt_json = ${yield* Codec.stringify(conflicting)}`

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
      yield* sql`UPDATE effect_local_server_log SET entry_json = ${yield* Codec.stringify(conflicting)}`

      const result = yield* buildStore(definitionV2, handlersV2, evolution).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "StorageCorrupt")
    })).pipe(Effect.provide(database)))

  it.effect("resumes committed shadow progress after interruption", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const v1 = yield* buildStore(definitionV1, handlersV1)
      const pending = yield* v1.mutate(PutTodoV1, { id: "3", title: "resume" })
      yield* v1.applyEntries([Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000202"),
        localSequence: Identity.LocalSequence.make(1),
        sourceSchema: definitionV1.schemaIdentity,
        digest: "2".repeat(64),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "4" },
          value: { id: "4", title: "committed" }
        })]
      })])
      const reached = yield* Deferred.make<void>()
      const afterBatch = Effect.gen(function*() {
        const progress = (yield* sql<{
          readonly generation: number
          readonly cursor_model: string | null
        }>`SELECT generation, cursor_model FROM effect_local_client_evolution WHERE singleton = 1`)[0]
        if (progress === undefined || progress.cursor_model === null) return
        const copied = (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_client_canonical_entities_data WHERE generation = ${progress.generation}`)[0]
        if (copied.count !== 1) return
        yield* Deferred.succeed(reached, undefined)
        yield* Effect.never
      }).pipe(Effect.orDie)
      const runtimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const fiber = yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 1,
        afterBatch
      }).pipe(Effect.provide(runtimeV2), Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)

      const progress = (yield* sql<{
        readonly generation: number
        readonly phase: string
        readonly cursor_model: string | null
      }>`SELECT generation, phase, cursor_model FROM effect_local_client_evolution WHERE singleton = 1`)[0]
      assert.strictEqual(progress.phase, "Entities")
      assert.strictEqual(progress.cursor_model, TodoV1.name)
      const copied = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_client_canonical_entities_data WHERE generation = ${progress.generation}`
      assert.strictEqual(copied[0].count, 1)

      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      assert.deepStrictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 3)), {
        id: 3,
        title: "resume",
        done: false
      })
      assert.strictEqual((yield* v2.pending)[0].envelope.digest, pending.envelope.digest)
    })).pipe(Effect.provide(database)))

  it.effect("migrates durable retractions before flipping the active client generation", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const v1 = yield* buildStore(definitionV1, handlersV1)
      yield* v1.applyEntries([Protocol.AcceptedMutation.make({
        sequence: Identity.ServerSequence.make(1),
        spaceId,
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000220"),
        localSequence: Identity.LocalSequence.make(1),
        sourceSchema: definitionV1.schemaIdentity,
        digest: Protocol.MutationDigest.make("2".repeat(64)),
        changes: [Protocol.Upsert.make({
          entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "3" },
          value: { id: "3", title: "canonical" }
        })]
      })])
      yield* v1.mutate(PutTodoV1, { id: "3", title: "pending" })
      yield* v1.revokeReplication

      const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001")
      const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
      yield* sql`UPDATE effect_local_client_meta SET replication_view_id = ${viewId},
        replication_view_revision = 4, installed_snapshot_id = ${snapshotId},
        installed_snapshot_sequence = 1, installed_snapshot_terminal_sequence = 1 WHERE singleton = 1`
      yield* sql`INSERT INTO effect_local_client_scoped_bootstrap
        (singleton, snapshot_id, space_id, client_id, definition_hash, schema_version, schema_hash,
          scope_digest, scope_generation, view_id, view_revision, server_sequence, terminal_sequence,
          entry_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest)
        VALUES (1, ${snapshotId}, ${spaceId}, ${clientId}, ${definitionV1.hash},
          ${definitionV1.schemaIdentity.version}, ${definitionV1.schemaIdentity.hash}, ${"0".repeat(64)},
          1, ${viewId}, 4, 1, 1, 1, 1, ${"0".repeat(64)}, 1, 1, ${"0".repeat(64)})`
      yield* sql`INSERT INTO effect_local_client_scoped_bootstrap_entries
        (snapshot_id, ordinal, model, entity_key, change_json, entry_bytes)
        VALUES (${snapshotId}, 0, ${TodoV1.name}, ${yield* Codec.stringify("3")},
          ${yield* Codec.stringify(Protocol.Retract.make({
        entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "3" }
      }))}, 1)`

      const reached = yield* Deferred.make<void>()
      const PhaseRow = Schema.Struct({ phase: Schema.String, generation: Schema.Number })
      const readProgress = SqlSchema.findOne({
        Request: Schema.Void,
        Result: PhaseRow,
        execute: () => sql`SELECT phase, generation FROM effect_local_client_evolution WHERE singleton = 1`
      })
      const afterBatch = Effect.gen(function*() {
        const row = yield* readProgress(undefined)
        if (row.phase !== "Flip") return
        yield* Deferred.succeed(reached, undefined)
        yield* Effect.never
      }).pipe(Effect.orDie)
      const runtimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const fiber = yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 1,
        afterBatch
      }).pipe(Effect.provide(runtimeV2), Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)

      const progress = yield* readProgress(undefined)
      const CountRow = Schema.Struct({ count: Schema.Number })
      const migratedKey = yield* Codec.stringify(3)
      const targetRetractions = yield* SqlSchema.findOne({
        Request: Schema.Number,
        Result: CountRow,
        execute: (generation) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_client_retractions
          WHERE generation = ${generation} AND model = ${TodoV2.name} AND model_version = ${TodoV2.version}
            AND entity_key = ${migratedKey}`
      })(progress.generation)
      const targetVisible = yield* SqlSchema.findOne({
        Request: Schema.Number,
        Result: CountRow,
        execute: (generation) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_client_visible_entities_data
          WHERE generation = ${generation} AND model = ${TodoV2.name} AND entity_key = ${migratedKey}`
      })(progress.generation)
      assert.strictEqual(targetRetractions.count, 1)
      assert.strictEqual(targetVisible.count, 0)

      const v2 = yield* buildStore(definitionV2, handlersV2, evolution)
      assert.isTrue(Option.isNone(yield* v2.get(TodoV2, 3)))
      const final = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: Schema.Struct({
          replication_view_id: Schema.NullOr(Schema.String),
          replication_view_revision: Schema.Number,
          staged: Schema.Number,
          source_retractions: Schema.Number
        }),
        execute: () =>
          sql`SELECT replication_view_id, replication_view_revision,
          (SELECT COUNT(*) FROM effect_local_client_scoped_bootstrap) AS staged,
          (SELECT COUNT(*) FROM effect_local_client_retractions WHERE generation = 1) AS source_retractions
          FROM effect_local_client_meta WHERE singleton = 1`
      })(undefined)
      assert.strictEqual(final.replication_view_id, null)
      assert.strictEqual(final.replication_view_revision, 0)
      assert.strictEqual(final.staged, 0)
      assert.strictEqual(final.source_retractions, 0)
    })).pipe(Effect.provide(database)))

  it.effect("bounds generation work and resumes cleanup after the active flip", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* buildStore(definitionV1, handlersV1)
      const source = (yield* sql<{ readonly active_schema_generation: number }>`SELECT active_schema_generation
        FROM effect_local_client_meta WHERE singleton = 1`)[0].active_schema_generation
      for (let index = 1; index <= 3; index++) {
        const key = yield* Codec.stringify(String(index))
        const value = yield* Codec.stringify({ id: String(index), title: `todo-${index}` })
        yield* sql`INSERT INTO effect_local_canonical_entities (model, model_version, entity_key, value_json)
          VALUES (${TodoV1.name}, ${TodoV1.version}, ${key}, ${value})`
        yield* sql`INSERT INTO effect_local_visible_entities (model, model_version, entity_key, value_json)
          VALUES (${TodoV1.name}, ${TodoV1.version}, ${key}, ${value})`
      }
      yield* sql`CREATE TABLE evolution_write_probe (operation TEXT NOT NULL)`
      yield* sql`CREATE TRIGGER probe_canonical_insert AFTER INSERT ON effect_local_client_canonical_entities_data
        BEGIN INSERT INTO evolution_write_probe VALUES ('insert'); END`
      yield* sql`CREATE TRIGGER probe_visible_insert AFTER INSERT ON effect_local_client_visible_entities_data
        BEGIN INSERT INTO evolution_write_probe VALUES ('insert'); END`
      yield* sql`CREATE TRIGGER probe_canonical_delete AFTER DELETE ON effect_local_client_canonical_entities_data
        BEGIN INSERT INTO evolution_write_probe VALUES ('delete'); END`
      yield* sql`CREATE TRIGGER probe_visible_delete AFTER DELETE ON effect_local_client_visible_entities_data
        BEGIN INSERT INTO evolution_write_probe VALUES ('delete'); END`
      const maximumWrites = yield* Ref.make(0)
      const flipped = yield* Deferred.make<void>()
      const ProbeRow = Schema.Struct({ count: Schema.Number })
      const PhaseRow = Schema.Struct({ phase: Schema.String })
      const afterBatch = Effect.gen(function*() {
        const count = (yield* sql<typeof ProbeRow.Type>`SELECT COUNT(*) AS count FROM evolution_write_probe`)[0].count
        yield* Ref.update(maximumWrites, (current) => Math.max(current, count))
        yield* sql`DELETE FROM evolution_write_probe`
        const phase = (yield* sql<typeof PhaseRow.Type>`SELECT phase FROM effect_local_client_evolution`)[0]?.phase
        if (phase === "CleanupCanonical") {
          yield* Deferred.succeed(flipped, undefined)
          yield* Effect.never
        }
      }).pipe(Effect.orDie)
      const runtimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const fiber = yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 1,
        afterBatch
      }).pipe(Effect.provide(runtimeV2), Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(flipped)
      yield* Fiber.interrupt(fiber)

      const active = yield* sql<{ readonly active_schema_generation: number }>`SELECT active_schema_generation
        FROM effect_local_client_meta WHERE singleton = 1`
      assert.strictEqual(active[0].active_schema_generation, source + 1)
      assert.isAtMost(yield* Ref.get(maximumWrites), 2)

      yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 1
      }).pipe(Effect.provide(runtimeV2))
      const remaining = yield* sql<{ readonly count: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_canonical_entities_data WHERE generation = ${source}) +
        (SELECT COUNT(*) FROM effect_local_client_visible_entities_data WHERE generation = ${source}) +
        (SELECT COUNT(*) FROM effect_local_client_receipts_data WHERE generation = ${source}) +
        (SELECT COUNT(*) FROM effect_local_client_pending_data WHERE generation = ${source}) AS count`
      assert.strictEqual(remaining[0].count, 0)
    })).pipe(Effect.provide(database)))

  it.effect("rejects one evolution row larger than the configured byte budget", () =>
    Effect.scoped(Effect.gen(function*() {
      const v1 = yield* buildStore(definitionV1, handlersV1)
      yield* v1.mutate(PutTodoV1, { id: "4", title: "x".repeat(256) })
      const runtimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const error = yield* SchemaEvolution.client({
        definition: definitionV2,
        evolution,
        spaceId,
        clientId,
        batchSize: 10,
        batchBytes: 32
      }).pipe(Effect.provide(runtimeV2), Effect.flip)
      assert.strictEqual(error._tag, "CapacityExceeded")
      if (error._tag === "CapacityExceeded") {
        assert.strictEqual(error.resource, "schema evolution row bytes")
        assert.strictEqual(error.limit, 32)
      }
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
      const sql = yield* SqlClient.SqlClient
      const localV1 = yield* buildStore(definitionV1, handlersV1)
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const accepted = yield* localV1.mutate(PutTodoV1, { id: "4", title: "accepted-old" })
      assert.strictEqual((yield* serverV1.submit(accepted.envelope))._tag, "Accepted")
      const offline = yield* localV1.mutate(PutTodoV1, { id: "5", title: "offline-old" })

      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      yield* serverV2.pull(pullRequest(definitionV2))

      const offlineReceipt = yield* serverV2.submit({
        envelope: offline.envelope,
        schema: definitionV2.schemaIdentity
      })
      assert.strictEqual(offlineReceipt._tag, "Accepted")
      if (offlineReceipt._tag === "Accepted") {
        assert.deepStrictEqual(offlineReceipt.sourceSchema, definitionV2.schemaIdentity)
        assert.deepStrictEqual(offlineReceipt.result, { id: 5, title: "offline-old", done: false })
      }
      const LogRow = Schema.Struct({ entry_json: Schema.String })
      const logRows = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: LogRow,
        execute: () => sql`SELECT entry_json FROM effect_local_authoritative_log ORDER BY server_sequence`
      })(undefined)
      const historical = yield* Codec.decode(Protocol.AcceptedMutation, yield* Codec.parse(logRows[0].entry_json))
      assert.deepStrictEqual(historical.sourceSchema, definitionV1.schemaIdentity)
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
      yield* serverV2.pull(pullRequest(definitionV2))
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
      const sql = yield* SqlClient.SqlClient
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      const submitted = yield* v1Envelope(
        clientId,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000101"),
        1,
        { id: "6", title: "resume-server" }
      )
      yield* serverV1.submit(submitted)
      const reached = yield* Deferred.make<void>()
      const afterBatch = Effect.gen(function*() {
        const progress = (yield* sql<{
          readonly generation: number
          readonly cursor_model: string | null
          readonly target_entity_count: number
          readonly target_entity_bytes: number
        }>`SELECT generation, cursor_model, target_entity_count, target_entity_bytes
          FROM effect_local_server_evolution WHERE space_id = ${spaceId}`)[0]
        if (
          progress === undefined || progress.cursor_model === null ||
          progress.target_entity_count !== 1 || progress.target_entity_bytes === 0
        ) return
        yield* Deferred.succeed(reached, undefined)
        yield* Effect.never
      }).pipe(Effect.orDie)
      const fiber = yield* SchemaEvolution.server({
        definition: definitionV2,
        evolution,
        spaceId,
        batchSize: 1,
        afterBatch
      }).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(reached)
      yield* Fiber.interrupt(fiber)

      const progress = (yield* sql<{
        readonly generation: number
        readonly phase: string
        readonly cursor_model: string | null
        readonly target_entity_count: number
        readonly target_entity_bytes: number
      }>`SELECT generation, phase, cursor_model, target_entity_count, target_entity_bytes
        FROM effect_local_server_evolution WHERE space_id = ${spaceId}`)[0]
      assert.strictEqual(progress.phase, "Entities")
      assert.strictEqual(progress.cursor_model, TodoV1.name)
      assert.strictEqual(progress.target_entity_count, 1)
      assert.isAbove(progress.target_entity_bytes, 0)
      const copied = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_server_entities_data
        WHERE space_id = ${spaceId} AND generation = ${progress.generation}`
      assert.strictEqual(copied[0].count, 1)

      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      yield* serverV2.pull(pullRequest(definitionV2))
      const LogRow = Schema.Struct({ entry_json: Schema.String })
      const log = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: LogRow,
        execute: () => sql`SELECT entry_json FROM effect_local_authoritative_log WHERE server_sequence = 1`
      })(undefined)
      const entry = yield* Codec.decode(Protocol.AcceptedMutation, yield* Codec.parse(log.entry_json))
      assert.deepStrictEqual(entry.sourceSchema, definitionV1.schemaIdentity)
      assert.deepStrictEqual(entry.changes[0], {
        _tag: "Upsert",
        entity: { model: "Todo", modelVersion: TodoV1.version, key: "6" },
        value: { id: "6", title: "resume-server" }
      })
    })).pipe(Effect.provide(database)))

  it.effect("invalidates scoped server views atomically with a schema generation flip", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const serverV1 = yield* buildServer(definitionV1, handlersV1)
      yield* serverV1.submit(
        yield* v1Envelope(
          clientId,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000221"),
          1,
          { id: "7", title: "scoped-view" }
        )
      )
      const scope = Protocol.ReplicationScope.make({ models: [TodoV1.name] })
      const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
      const initial = yield* serverV1.pullAuthorized(
        Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definitionV1.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: null,
          limit: 10
        }),
        null
      )
      if (!("_tag" in initial)) assert.fail("expected bootstrap")
      const bootstrap = yield* serverV1.bootstrapAuthorized(
        Protocol.BootstrapRequest.make({
          spaceId,
          clientId,
          schema: definitionV1.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: initial.manifest.cursor,
          snapshotId: initial.manifest.snapshotId,
          afterOrdinal: -1,
          limit: 10
        }),
        null
      )
      assert.isFalse(bootstrap.hasMore)
      const outstanding = yield* serverV1.pullAuthorized(
        Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definitionV1.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: initial.manifest.cursor,
          limit: 10
        }),
        null
      )
      if ("_tag" in outstanding) assert.fail("expected incremental page")
      for (const suffix of [2, 3]) {
        const scopedClientId = Identity.ClientId.make(
          `cli_00000000-0000-4000-8000-${String(suffix).padStart(12, "0")}`
        )
        const required = yield* serverV1.pullAuthorized(
          Protocol.PullRequest.make({
            spaceId,
            clientId: scopedClientId,
            schema: definitionV1.schemaIdentity,
            scope,
            scopeGeneration,
            cursor: null,
            limit: 10
          }),
          null
        )
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        yield* serverV1.bootstrapAuthorized(
          Protocol.BootstrapRequest.make({
            spaceId,
            clientId: scopedClientId,
            schema: definitionV1.schemaIdentity,
            scope,
            scopeGeneration,
            cursor: required.manifest.cursor,
            snapshotId: required.manifest.snapshotId,
            afterOrdinal: -1,
            limit: 10
          }),
          null
        )
        yield* serverV1.pullAuthorized(
          Protocol.PullRequest.make({
            spaceId,
            clientId: scopedClientId,
            schema: definitionV1.schemaIdentity,
            scope,
            scopeGeneration,
            cursor: required.manifest.cursor,
            limit: 10
          }),
          null
        )
      }

      const flipped = yield* Deferred.make<void>()
      const cleanupPhases = yield* Ref.make<ReadonlyArray<string>>([])
      const cleanupCounts = yield* Ref.make<ReadonlyArray<number>>([])
      const Progress = Schema.Struct({ phase: Schema.String })
      const CountRow = Schema.Struct({ count: Schema.Number })
      const readProgress = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Progress,
        execute: () => sql`SELECT phase FROM effect_local_server_evolution WHERE space_id = ${spaceId}`
      })
      const countScopedRows = SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () =>
          sql`SELECT
          (SELECT COUNT(*) FROM effect_local_server_replication_views WHERE space_id = ${spaceId}) +
          (SELECT COUNT(*) FROM effect_local_server_replication_view_entities WHERE space_id = ${spaceId}) +
          (SELECT COUNT(*) FROM effect_local_server_replication_pages WHERE space_id = ${spaceId}) +
          (SELECT COUNT(*) FROM effect_local_server_scoped_snapshots WHERE space_id = ${spaceId}) +
          (SELECT COUNT(*) FROM effect_local_server_scoped_snapshot_entries WHERE snapshot_id IN (
            SELECT snapshot_id FROM effect_local_server_scoped_snapshots WHERE space_id = ${spaceId}
          )) AS count`
      })
      const afterBatch = Effect.gen(function*() {
        const progress = yield* readProgress(undefined)
        const scopedRows = yield* countScopedRows(undefined)
        yield* Ref.update(cleanupCounts, (counts) => [...counts, scopedRows.count])
        if (progress.phase.startsWith("CleanupScoped")) {
          yield* Ref.update(cleanupPhases, (phases) => [...phases, progress.phase])
        }
        if (progress.phase !== "CleanupEntities") return
        yield* Deferred.succeed(flipped, undefined)
        yield* Effect.never
      }).pipe(Effect.orDie)
      const fiber = yield* SchemaEvolution.server({
        definition: definitionV2,
        evolution,
        spaceId,
        batchSize: 1,
        afterBatch
      }).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(flipped)
      yield* Fiber.interrupt(fiber)
      assert.include(yield* Ref.get(cleanupPhases), "CleanupScopedSnapshotEntries")
      const counts = yield* Ref.get(cleanupCounts)
      assert.isAtLeast(counts[0] ?? 0, 15)
      for (let index = 1; index < counts.length; index++) {
        assert.isAtMost(counts[index - 1] - counts[index], 1)
      }

      const scopedRows = yield* countScopedRows(undefined)
      assert.strictEqual(scopedRows.count, 0)

      yield* SchemaEvolution.server({ definition: definitionV2, evolution, spaceId, batchSize: 1 })
      const serverV2 = yield* buildServer(definitionV2, handlersV2, evolution)
      const result = yield* serverV2.pullAuthorized(
        Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definitionV2.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [TodoV2.name] }),
          scopeGeneration,
          cursor: outstanding.cursor,
          limit: 10
        }),
        null
      )
      if (!("_tag" in result)) assert.fail("expected a replacement bootstrap")
      assert.notStrictEqual(result.manifest.cursor.viewId, outstanding.cursor.viewId)
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
      yield* serverV2.pull(pullRequest(definitionV2))
      const migrated = yield* sql<{
        readonly client_id: string
        readonly local_sequence: number
        readonly mutation_id: string
        readonly source_schema_version: number | null
        readonly source_schema_hash: string | null
        readonly mutation_version: number | null
        readonly mutation_name: string | null
        readonly receipt_json: string
      }>`SELECT client_id, local_sequence, mutation_id, source_schema_version, source_schema_hash,
        mutation_version, mutation_name, receipt_json FROM effect_local_server_receipts ORDER BY mutation_id`
      assert.deepStrictEqual(
        migrated.map((row) => ({
          clientId: row.client_id,
          localSequence: row.local_sequence,
          mutationId: row.mutation_id,
          sourceSchemaVersion: row.source_schema_version,
          sourceSchemaHash: row.source_schema_hash,
          mutationVersion: row.mutation_version,
          mutationName: row.mutation_name
        })),
        [
          {
            clientId,
            localSequence: 1,
            mutationId: first.mutationId,
            sourceSchemaVersion: definitionV1.schemaIdentity.version,
            sourceSchemaHash: definitionV1.schemaIdentity.hash,
            mutationVersion: PutTodoV1.version,
            mutationName: PutTodoV1.name
          },
          {
            clientId: secondClientId,
            localSequence: 1,
            mutationId: second.mutationId,
            sourceSchemaVersion: definitionV1.schemaIdentity.version,
            sourceSchemaHash: definitionV1.schemaIdentity.hash,
            mutationVersion: PutTodoV1.version,
            mutationName: PutTodoV1.name
          }
        ]
      )
      const decoded = yield* Effect.forEach(migrated, (row) =>
        Codec.parse(row.receipt_json).pipe(Effect.flatMap((json) => Codec.decode(Protocol.Receipt, json))))
      assert.deepStrictEqual(
        decoded.map((receipt) => {
          let mutationVersion: Identity.SchemaVersion | null = null
          if (receipt._tag === "Accepted" || receipt._tag === "Rejected") {
            mutationVersion = receipt.mutationVersion
          }
          return {
            tag: receipt._tag,
            clientId: receipt.clientId,
            localSequence: receipt.localSequence,
            mutationId: receipt.mutationId,
            sourceSchema: receipt.sourceSchema,
            mutationVersion
          }
        }),
        [
          {
            tag: "Accepted",
            clientId,
            localSequence: Identity.LocalSequence.make(1),
            mutationId: first.mutationId,
            sourceSchema: definitionV1.schemaIdentity,
            mutationVersion: PutTodoV1.version
          },
          {
            tag: "Accepted",
            clientId: secondClientId,
            localSequence: Identity.LocalSequence.make(1),
            mutationId: second.mutationId,
            sourceSchema: definitionV1.schemaIdentity,
            mutationVersion: PutTodoV1.version
          }
        ]
      )
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
          authorizeRead: ({ principal }) => {
            if (principal === "allowed") return Effect.void
            return Effect.fail("denied")
          }
        }).pipe(Layer.provide(runtime))
      ).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const stale = pullRequest(definitionV1)
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

  it.effect("does not rewrite unchanged server schema metadata during preparation", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const runtime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(handlersV2))
      const server = yield* Layer.build(
        ServerStore.layerTrusted({ ...serverHistory, definition: definitionV2, evolution }).pipe(
          Layer.provide(runtime)
        )
      ).pipe(Effect.map((context) => Context.get(context, ServerStore.ServerStore)))
      yield* sql`CREATE TABLE space_update_probe (count INTEGER NOT NULL)`
      yield* sql`CREATE TRIGGER count_space_updates AFTER UPDATE ON effect_local_server_spaces
        BEGIN INSERT INTO space_update_probe (count) VALUES (1); END`
      const CountRow = Schema.Struct({ count: Schema.Number })
      const countUpdates = SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM space_update_probe`
      })
      const request = pullRequest(definitionV2)

      yield* server.pull(request)
      const afterFirst = (yield* countUpdates(undefined)).count
      yield* server.pull(request)
      const afterSecond = (yield* countUpdates(undefined)).count

      assert.strictEqual(afterSecond - afterFirst, 0)
    })).pipe(Effect.provide(database)))
})
