import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "../src/internal/codec.js"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as SchemaEvolution from "../src/SchemaEvolution.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: ["Todo"] }),
  scope: Protocol.ReplicationScope.make({ models: ["Todo"] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  settlementCapacity: 64,
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
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds" as const,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}

class SchemaPolicyRejectedError extends Schema.TaggedErrorClass<SchemaPolicyRejectedError>(
  "@lucas-barake/effect-local-sql/test/SchemaPolicyRejectedError"
)("SchemaPolicyRejectedError", { reason: Schema.String }) {}

const ReadDeniedError = Schema.TaggedStruct("ReadDeniedError", {})

const expectFailure = <A, E extends { readonly _tag: string },>(result: Result.Result<A, E>): E => {
  if (Result.isFailure(result)) return result.failure
  return assert.fail("expected Effect failure")
}

const TodoV1 = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String }),
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: { readonly title: string }) => todo.title
      }]
    }
  }
})
const PutTodoV1 = Mutation.make("PutTodo", {
  version: 1,
  payload: TodoV1.schema,
  success: TodoV1.schema,
  rejection: SchemaPolicyRejectedError
})
const definitionV1 = Definition.make({ version: 1, models: [TodoV1], mutations: [PutTodoV1] })
const layerHandlersV1 = PutTodoV1.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV1, payload.id, payload).pipe(Effect.as(payload))
)

const TodoV2 = Model.make("Todo", {
  version: 2,
  key: Schema.Number,
  schema: Schema.Struct({ id: Schema.Number, title: Schema.String, done: Schema.Boolean }),
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: { readonly title: string }) => todo.title
      }]
    }
  }
})
const PutTodoV2 = Mutation.make("PutTodo", {
  version: 2,
  payload: TodoV2.schema,
  success: TodoV2.schema,
  rejection: SchemaPolicyRejectedError
})
const definitionV2 = Definition.make({ version: 2, models: [TodoV2], mutations: [PutTodoV2] })
const pullRequest = (
  definition: Definition.Any,
  cursor: Protocol.ReplicationCursor | null = null,
  model: string = TodoV2.name,
  requestedClientId = clientId
): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: requestedClientId,
    schema: definition.schemaIdentity,
    scope: Protocol.ReplicationScope.make({ models: [model] }),
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor,
    limit: 10
  })
const bootstrapRequest = (
  definition: Definition.Any,
  manifest: Protocol.SnapshotManifest,
  model: string = TodoV2.name,
  afterOrdinal = -1,
  limit = 10
): Protocol.BootstrapRequest =>
  Protocol.BootstrapRequest.make({
    spaceId,
    clientId: manifest.clientId,
    schema: definition.schemaIdentity,
    scope: Protocol.ReplicationScope.make({ models: [model] }),
    scopeGeneration: manifest.scopeGeneration,
    cursor: manifest.cursor,
    snapshotId: manifest.snapshotId,
    afterOrdinal,
    limit
  })
const layerHandlersV2 = PutTodoV2.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV2, payload.id, payload).pipe(Effect.as(payload))
)
const layerRejectingHandlersV2 = PutTodoV2.toLayer(() =>
  Effect.fail(new SchemaPolicyRejectedError({ reason: "schema-policy-rejected" }))
)

const TodoV3 = Model.make("Todo", {
  version: 3,
  key: Schema.Number,
  schema: Schema.Struct({ id: Schema.Number, title: Schema.String, done: Schema.Boolean, priority: Schema.Number }),
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: { readonly title: string }) => todo.title
      }]
    }
  }
})
const PutTodoV3 = Mutation.make("PutTodo", {
  version: 3,
  payload: TodoV3.schema,
  success: TodoV3.schema,
  rejection: SchemaPolicyRejectedError
})
const definitionV3 = Definition.make({ version: 3, models: [TodoV3], mutations: [PutTodoV3] })
const layerHandlersV3 = PutTodoV3.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV3, payload.id, payload).pipe(Effect.as(payload))
)
const layerRejectingHandlersV3 = PutTodoV3.toLayer(() =>
  Effect.fail(new SchemaPolicyRejectedError({ reason: "schema-policy-rejected-v3" }))
)

const ExpandedV1 = Model.make("Expanded", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, text: Schema.String })
})
const ExpandedV2 = Model.make("Expanded", {
  version: 2,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, compact: Schema.String })
})
const PutExpanded = Mutation.make("PutExpanded", {
  version: 1,
  payload: ExpandedV2.schema,
  success: Schema.Null,
  rejection: SchemaPolicyRejectedError
})
const expandedDefinitionV1 = Definition.make({ version: 1, models: [ExpandedV1], mutations: [PutExpanded] })
const expandedDefinitionV2 = Definition.make({ version: 2, models: [ExpandedV2], mutations: [PutExpanded] })
const layerExpandedHandlers = PutExpanded.toLayer(({ payload, transaction }) =>
  transaction.set(ExpandedV2, payload.id, payload).pipe(Effect.as(null))
)
const expandingEvolution = Evolution.make({
  current: expandedDefinitionV2,
  steps: [Evolution.step({
    id: "expanded/1-to-2",
    from: expandedDefinitionV1,
    to: expandedDefinitionV2,
    models: [Evolution.model({
      id: "expanded-model/1-to-2",
      from: ExpandedV1,
      to: ExpandedV2,
      value: ({ value }) => ({ id: value.id, compact: value.text }),
      downgradeValue: ({ value }) => ({ id: value.id, text: value.compact.repeat(80) })
    })]
  })]
})

const CompactV1 = Model.make("Large", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, summary: Schema.String })
})
const LargeV2 = Model.make("Large", {
  version: 2,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, text: Schema.String })
})
const PutLarge = Mutation.make("PutLarge", {
  version: 1,
  payload: Schema.Struct({ id: Schema.String }),
  success: Schema.Null,
  rejection: SchemaPolicyRejectedError
})
const compactDefinitionV1 = Definition.make({ version: 1, models: [CompactV1], mutations: [PutLarge] })
const largeDefinitionV2 = Definition.make({ version: 2, models: [LargeV2], mutations: [PutLarge] })
const layerLargeHandlers = PutLarge.toLayer(({ payload, transaction }) =>
  transaction.set(LargeV2, payload.id, { id: payload.id, text: "x".repeat(2_200_000) }).pipe(Effect.as(null))
)
const compactingEvolution = Evolution.make({
  current: largeDefinitionV2,
  steps: [Evolution.step({
    id: "large/1-to-2",
    from: compactDefinitionV1,
    to: largeDefinitionV2,
    models: [Evolution.model({
      id: "large-model/1-to-2",
      from: CompactV1,
      to: LargeV2,
      value: ({ value }) => ({ id: value.id, text: value.summary }),
      downgradeValue: ({ value }) => ({ id: value.id, summary: value.text.slice(0, 8) })
    })]
  })]
})

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
      value: ({ value }) => ({ id: Number(value.id), title: value.title, done: false }),
      downgradeKey: String,
      downgradeValue: ({ value }) => ({ id: String(value.id), title: value.title })
    })],
    mutations: [Evolution.mutation({
      id: "put-todo/1-to-2",
      from: PutTodoV1,
      to: PutTodoV2,
      payload: (payload) => ({ id: Number(payload.id), title: payload.title, done: false }),
      success: (success) => ({ id: Number(success.id), title: success.title, done: false }),
      downgradePayload: ({ id, title }) => ({ id: String(id), title }),
      downgradeSuccess: ({ id, title }) => ({ id: String(id), title })
    })]
  })]
})

const collidingProjectionEvolution = Evolution.make({
  current: definitionV2,
  steps: [Evolution.step({
    id: "definition/1-to-2-colliding-projection",
    from: definitionV1,
    to: definitionV2,
    models: [Evolution.model({
      id: "todo/1-to-2-colliding-projection",
      from: TodoV1,
      to: TodoV2,
      key: Number,
      value: ({ value }) => ({ id: Number(value.id), title: value.title, done: false }),
      downgradeKey: () => "shared",
      downgradeValue: ({ value }) => ({ id: String(value.id), title: value.title })
    })],
    mutations: [Evolution.mutation({
      id: "put-todo/1-to-2-colliding-projection",
      from: PutTodoV1,
      to: PutTodoV2,
      payload: (payload) => ({ id: Number(payload.id), title: payload.title, done: false }),
      success: (success) => ({ id: Number(success.id), title: success.title, done: false }),
      downgradePayload: ({ id, title }) => ({ id: String(id), title }),
      downgradeSuccess: ({ id, title }) => ({ id: String(id), title })
    })]
  })]
})

const forwardOnlyEvolution = Evolution.make({
  current: definitionV2,
  steps: [Evolution.step({
    id: "definition/1-to-2-forward-only",
    from: definitionV1,
    to: definitionV2,
    models: [Evolution.model({
      id: "todo/1-to-2-forward-only",
      from: TodoV1,
      to: TodoV2,
      key: Number,
      value: ({ value }) => ({ id: Number(value.id), title: value.title, done: false })
    })],
    mutations: [Evolution.mutation({
      id: "put-todo/1-to-2-forward-only",
      from: PutTodoV1,
      to: PutTodoV2,
      payload: (payload) => ({ id: Number(payload.id), title: payload.title, done: false }),
      success: (success) => ({ id: Number(success.id), title: success.title, done: false })
    })]
  })]
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
        value: ({ value }) => ({ ...value, priority: 0 }),
        downgradeValue: ({ value }) => ({ id: value.id, title: value.title, done: value.done })
      })],
      mutations: [Evolution.mutation({
        id: "put-todo/2-to-3",
        from: PutTodoV2,
        to: PutTodoV3,
        payload: (payload) => ({ ...payload, priority: 0 }),
        success: (success) => ({ ...success, priority: 0 }),
        downgradePayload: ({ id, title, done }) => ({ id, title, done }),
        downgradeSuccess: ({ id, title, done }) => ({ id, title, done })
      })]
    })
  ]
})

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)
const provideDatabase = Effect.provide(layerDatabase)
const provideNodeFileSystem = Effect.provide(NodeFileSystem.layer)

const buildStore = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  configuredEvolution?: Evolution.Evolution,
  storeClientId: Identity.ClientId = clientId
) => {
  const layerRuntime = MutationRuntime.layer(definition, configuredEvolution).pipe(Layer.provide(handlers))
  let options: LocalStore.Options = {
    ...clientHistory,
    definition,
    spaceId,
    clientId: storeClientId,
    schemaEvolutionBatchSize: 1
  }
  if (configuredEvolution !== undefined) options = { ...options, evolution: configuredEvolution }
  return LocalStore.layer(options).pipe(
    Layer.provide(layerRuntime),
    Layer.build,
    Effect.map(Context.get(LocalStore.Store))
  )
}

const buildServer = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  configuredEvolution?: Evolution.Evolution,
  serverOptions?: Partial<Pick<ServerStore.Options, "acceptedSchemaVersions" | "retainedHistoryEntries">>
) => {
  const layerRuntime = MutationRuntime.layer(definition, configuredEvolution).pipe(Layer.provide(handlers))
  let options: ServerStore.TrustedOptions = {
    ...serverHistory,
    definition,
    schemaEvolutionBatchSize: 1
  }
  if (configuredEvolution !== undefined) options = { ...options, evolution: configuredEvolution }
  if (serverOptions !== undefined) {
    options = { ...options, ...serverOptions }
  }
  return ServerStore.layerTrusted(options).pipe(
    Layer.provide(layerRuntime),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )
}

const buildReplica = <D extends Definition.Any,>(
  definition: D,
  handlers: Layer.Layer<MutationRuntime.Handlers<D>>,
  remote: SyncEngine.Service,
  configuredEvolution?: Evolution.Evolution
) => {
  let options: SqlReplica.Options<D> = {
    ...clientHistory,
    definition,
    clientId,
    initialSpaces: [spaceId],
    schemaEvolutionBatchSize: 1
  }
  if (configuredEvolution !== undefined) options = { ...options, evolution: configuredEvolution }
  return SqlReplica.layer(options).pipe(
    Layer.provide(handlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
    Layer.build,
    Effect.flatMap((context) => Context.get(context, Replica.Replica).space(spaceId))
  )
}

const unavailableSync = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  discard: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  pull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  bootstrap: () => Effect.die("unexpected bootstrap"),
  watch: () => Stream.never
})

const serverSync = (server: ServerStore.Service) =>
  SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: server.submit,
    discard: (request) => server.discard(request, null),
    pull: server.pull,
    bootstrap: server.bootstrap,
    watch: server.watch
  })

const v1Envelope = Effect.fnUntraced(function*(
  envelopeClientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: number,
  payload: typeof TodoV1.schema.Type
) {
  const identity = {
    spaceId,
    clientId: envelopeClientId,
    mutationId,
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: PutTodoV1.name,
    payload,
    digestVersion: 3 as const,
    membershipIncarnation,
    sourceSchema: definitionV1.schemaIdentity,
    mutationVersion: PutTodoV1.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

describe("client schema evolution", () => {
  it.effect(
    "rejects same-name mutation descriptors that are not registered in the definition",
    Effect.fnUntraced(
      function*() {
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const replica = yield* buildReplica(definitionV2, layerHandlersV2, serverSync(server), evolution)
        const counterfeit = Mutation.make("PutTodo", {
          version: 2,
          payload: Schema.String,
          success: Schema.Json,
          rejection: SchemaPolicyRejectedError
        })
        const unknownMutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000990")

        const pendingResult = yield* replica.pendingFor(counterfeit).pipe(Effect.result)
        const pendingError = expectFailure(pendingResult)
        assert.strictEqual(pendingError._tag, "ProtocolInvalid")
        assert.strictEqual(
          (yield* replica.receipt(counterfeit, unknownMutationId).pipe(Effect.flip))._tag,
          "ProtocolInvalid"
        )

        const counterfeitSettlement = yield* replica.settlementsFor(counterfeit).pipe(
          Stream.runHead,
          Effect.result,
          Effect.forkScoped({ startImmediately: true })
        )
        yield* replica.mutate(PutTodoV2, { id: 990, title: "registered", done: false })
        const invalid = yield* Fiber.join(counterfeitSettlement)
        assert.isTrue(Result.isFailure(invalid))
        if (Result.isFailure(invalid)) assert.strictEqual(invalid.failure._tag, "ProtocolInvalid")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "atomically promotes canonical state, receipts, and replayed pending mutations",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const accepted = yield* v1.mutate(PutTodoV1, { id: "1", title: "accepted" })
        yield* pipe(
          Protocol.AcceptedReceipt.make({
            spaceId,
            clientId,
            membershipIncarnation: accepted.envelope.membershipIncarnation,
            mutationId: accepted.envelope.mutationId,
            localSequence: accepted.envelope.localSequence,
            name: PutTodoV1.name,
            sourceSchema: definitionV1.schemaIdentity,
            mutationVersion: PutTodoV1.version,
            serverSequence: Identity.ServerSequence.make(1),
            result: accepted.optimisticResult
          }),
          v1.applyReceipt
        )
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId,
          membershipIncarnation: accepted.envelope.membershipIncarnation,
          mutationId: accepted.envelope.mutationId,
          localSequence: accepted.envelope.localSequence,
          sourceSchema: definitionV1.schemaIdentity,
          digest: accepted.envelope.digest,
          changes: accepted.changes
        })])
        const pending = yield* v1.mutate(PutTodoV1, { id: "2", title: "pending" })

        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 1)),
          (todo) => assert.deepStrictEqual(todo, { id: 1, title: "accepted", done: false })
        )
        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 2)),
          (todo) => assert.deepStrictEqual(todo, { id: 2, title: "pending", done: false })
        )

        const promotedReceipt = Option.getOrThrow(yield* v2.receipt(accepted.envelope.mutationId))
        assert.strictEqual(promotedReceipt._tag, "Accepted")
        if (promotedReceipt._tag === "Accepted") {
          assert.deepStrictEqual(promotedReceipt.sourceSchema, definitionV2.schemaIdentity)
          assert.deepStrictEqual(promotedReceipt.result, { id: 1, title: "accepted", done: false })
        }

        const promotedPending = yield* v2.pendingToSubmit
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
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "preserves AwaitingReceipt across promotion without replaying its optimistic changes",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "90", title: "optimistic" })
        yield* v1.markSubmitting(pending.envelope.mutationId)
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          sourceSchema: definitionV1.schemaIdentity,
          digest: pending.envelope.digest,
          changes: [Protocol.Upsert.make({
            entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "90" },
            value: { id: "90", title: "authoritative" }
          })]
        })])
        yield* sql`UPDATE effect_local_client_pending_data SET submission_state = 'AwaitingReceipt'
        WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`
        const awaiting = (yield* v1.pending)[0]
        assert.strictEqual(awaiting.submissionState, "AwaitingReceipt")
        assert.strictEqual(awaiting.attempts, 1)
        assert.strictEqual(Option.getOrThrow(yield* v1.get(TodoV1, "90")).title, "authoritative")

        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)

        const promoted = (yield* v2.pending)[0]
        assert.strictEqual(promoted.submissionState, "AwaitingReceipt")
        assert.strictEqual(promoted.attempts, 1)
        assert.deepStrictEqual(promoted.changes, [Protocol.Upsert.make({
          entity: { model: TodoV2.name, modelVersion: TodoV2.version, key: 90 },
          value: { id: 90, title: "optimistic", done: false }
        })])
        assert.strictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 90)).title, "authoritative")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "promotes receipt-backed pending without replaying and settles after projection recovery",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "92", title: "receipt-backed" })
        yield* v1.markSubmitting(pending.envelope.mutationId)
        const encodedRejection = yield* Schema.encodeEffect(SchemaPolicyRejectedError)(
          new SchemaPolicyRejectedError({ reason: "server-rejected" })
        )
        const receipt = Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          name: PutTodoV1.name,
          sourceSchema: definitionV1.schemaIdentity,
          mutationVersion: PutTodoV1.version,
          origin: "Mutation" as const,
          rejection: encodedRejection
        })
        yield* v1.persistReceipt(receipt)
        yield* sql`CREATE TRIGGER fail_projection_promotion BEFORE UPDATE OF visible_revision
        ON effect_local_client_spaces
        WHEN NEW.visible_revision <> OLD.visible_revision
        BEGIN SELECT RAISE(ABORT, 'projection failed'); END`

        const failedSettlement = yield* v1.settleReceipts.pipe(Effect.result)
        assert.isTrue(Result.isFailure(failedSettlement))
        const durablePending = (yield* v1.pending)[0]
        assert.strictEqual(durablePending.submissionState, "Submitted")
        assert.strictEqual(durablePending.attempts, 1)
        const receiptIsPresent = Option.isSome(yield* v1.receipt(pending.envelope.mutationId))
        assert.isTrue(receiptIsPresent)

        yield* sql`DROP TRIGGER fail_projection_promotion`
        const rejectingV2 = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const promoted = (yield* rejectingV2.pending)[0]
        assert.strictEqual(promoted.submissionState, "Submitted")
        assert.strictEqual(promoted.attempts, 1)
        assert.deepStrictEqual(yield* rejectingV2.quarantine, [])
        const promotedReceipt = Option.getOrThrow(yield* rejectingV2.receipt(pending.envelope.mutationId))
        assert.strictEqual(promotedReceipt._tag, "Rejected")
        if (promotedReceipt._tag === "Rejected") {
          assert.deepStrictEqual(promotedReceipt.sourceSchema, definitionV2.schemaIdentity)
          assert.strictEqual(promotedReceipt.mutationVersion, PutTodoV2.version)
        }

        const settlementFiber = yield* rejectingV2.settlements.pipe(
          Stream.runHead,
          Effect.forkScoped({ startImmediately: true })
        )
        yield* rejectingV2.settleReceipts
        const delivered = Option.getOrThrow(yield* Fiber.join(settlementFiber))
        assert.strictEqual(delivered.pending.envelope.mutationId, pending.envelope.mutationId)
        assert.deepStrictEqual(yield* rejectingV2.pending, [])
        assert.deepStrictEqual(yield* rejectingV2.quarantine, [])
        const promotedReceiptIsPresent = Option.isSome(yield* rejectingV2.receipt(pending.envelope.mutationId))
        assert.isTrue(promotedReceiptIsPresent)
        const duplicate = yield* rejectingV2.settlements.pipe(
          Stream.runHead,
          Effect.timeoutOption("1 second"),
          Effect.forkScoped({ startImmediately: true })
        )
        yield* rejectingV2.settleReceipts
        yield* TestClock.adjust("1 second")
        const duplicateIsAbsent = Option.isNone(yield* Fiber.join(duplicate))
        assert.isTrue(duplicateIsAbsent)
        assert.deepStrictEqual(yield* rejectingV2.pending, [])
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "replays an old pending envelope after applying a current accepted entry",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.mutate(PutTodoV1, { id: "2", title: "pending" })
        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        yield* v2.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
          membershipIncarnation,
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000201"),
          localSequence: Identity.LocalSequence.make(1),
          sourceSchema: definitionV2.schemaIdentity,
          digest: "1".repeat(64),
          changes: [Protocol.Upsert.make({
            entity: { model: TodoV2.name, modelVersion: TodoV2.version, key: 20 },
            value: { id: 20, title: "remote", done: true }
          })]
        })])

        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 2)),
          (todo) => assert.deepStrictEqual(todo, { id: 2, title: "pending", done: false })
        )
        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 20)),
          (todo) => assert.deepStrictEqual(todo, { id: 20, title: "remote", done: true })
        )
        assert.strictEqual(yield* v2.cursor, 1)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "promotes pending optimistic changes across multiple schema versions",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "3", title: "multi-hop" })
        yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const v3 = yield* buildStore(definitionV3, layerHandlersV3, evolutionV3)

        assert.deepStrictEqual((yield* v3.pendingToSubmit)[0].envelope, pending.envelope)
        pipe(
          Option.getOrThrow(yield* v3.get(TodoV3, 3)),
          (todo) => assert.deepStrictEqual(todo, { id: 3, title: "multi-hop", done: false, priority: 0 })
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects a different client before it can promote the owned database",
    Effect.fnUntraced(
      function*() {
        const owner = yield* buildStore(definitionV1, layerHandlersV1)
        yield* owner.mutate(PutTodoV1, { id: "4", title: "owned" })
        const otherClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")

        const result = yield* buildStore(definitionV2, layerHandlersV2, evolution, otherClientId).pipe(Effect.result)
        const error = expectFailure(result)
        assert.strictEqual(error._tag, "ReplicaIdentityMismatch")
        pipe(
          Option.getOrThrow(yield* owner.get(TodoV1, "4")),
          (todo) => assert.deepStrictEqual(todo, { id: "4", title: "owned" })
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects a later historical key that collides with promoted lineage",
    Effect.fnUntraced(
      function*() {
        const remoteClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: remoteClientId,
          membershipIncarnation,
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000202"),
          localSequence: Identity.LocalSequence.make(1),
          sourceSchema: definitionV1.schemaIdentity,
          digest: "2".repeat(64),
          changes: [Protocol.Upsert.make({
            entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "01" },
            value: { id: "01", title: "original" }
          })]
        })])
        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const collision = Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(2),
          spaceId,
          clientId: remoteClientId,
          membershipIncarnation,
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000203"),
          localSequence: Identity.LocalSequence.make(2),
          sourceSchema: definitionV1.schemaIdentity,
          digest: "3".repeat(64),
          changes: [Protocol.Upsert.make({
            entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "1" },
            value: { id: "1", title: "collision" }
          })]
        })

        const collisionResult = yield* v2.applyEntries([collision]).pipe(Effect.result)
        const collisionError = expectFailure(collisionResult)
        assert.strictEqual(collisionError._tag, "SchemaKeyCollision")
        assert.strictEqual(yield* v2.cursor, 1)
        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 1)),
          (todo) => assert.deepStrictEqual(todo, { id: 1, title: "original", done: false })
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects receipt JSON that conflicts with its durable SQL identity",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "5", title: "receipt" })
        const receipt = Protocol.AcceptedReceipt.make({
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
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
        yield* sql`UPDATE effect_local_client_receipts_data
        SET receipt_json = ${yield* Codec.stringify(conflicting)}
        WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`

        const result = yield* buildStore(definitionV2, layerHandlersV2, evolution).pipe(Effect.result)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "StorageCorrupt")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects log JSON that conflicts with its durable SQL identity",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const remoteClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const entry = Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: remoteClientId,
          membershipIncarnation,
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

        const result = yield* buildStore(definitionV2, layerHandlersV2, evolution).pipe(Effect.result)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "StorageCorrupt")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "resumes committed shadow progress after interruption",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "3", title: "resume" })
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
          membershipIncarnation,
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
          }>`SELECT generation, cursor_model FROM effect_local_client_evolution
          WHERE space_id = ${spaceId}`)[0]
          if (progress === undefined || progress.cursor_model === null) return
          const copied = (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${progress.generation}`)[0]
          if (copied.count !== 1) return
          yield* Deferred.succeed(reached, undefined)
          yield* Effect.never
        }).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
        const layerRuntimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const fiber = yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 1,
          afterBatch
        }).pipe(Effect.provide(layerRuntimeV2), Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(reached)
        yield* Fiber.interrupt(fiber)

        const progress = (yield* sql<{
          readonly generation: number
          readonly phase: string
          readonly cursor_model: string | null
        }>`SELECT generation, phase, cursor_model FROM effect_local_client_evolution
        WHERE space_id = ${spaceId}`)[0]
        assert.strictEqual(progress.phase, "Entities")
        assert.strictEqual(progress.cursor_model, TodoV1.name)
        const copied = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${progress.generation}`
        assert.strictEqual(copied[0].count, 1)

        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        pipe(
          Option.getOrThrow(yield* v2.get(TodoV2, 3)),
          (todo) => assert.deepStrictEqual(todo, { id: 3, title: "resume", done: false })
        )
        assert.strictEqual((yield* v2.pending)[0].envelope.digest, pending.envelope.digest)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "migrates durable retractions before flipping the active client generation",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000220"),
          localSequence: Identity.LocalSequence.make(1),
          membershipIncarnation,
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
        yield* sql`UPDATE effect_local_client_spaces SET replication_view_id = ${viewId},
        replication_view_revision = 4, installed_snapshot_id = ${snapshotId},
        installed_snapshot_sequence = 1, installed_snapshot_terminal_sequence = 1 WHERE space_id = ${spaceId}`
        yield* sql`INSERT INTO effect_local_client_scoped_bootstrap
        (snapshot_id, space_id, client_id, definition_hash, schema_version, schema_hash,
          scope_digest, scope_generation, view_id, view_revision, server_sequence, terminal_sequence,
          entry_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest)
        VALUES (${snapshotId}, ${spaceId}, ${clientId}, ${definitionV1.hash},
          ${definitionV1.schemaIdentity.version}, ${definitionV1.schemaIdentity.hash}, ${"0".repeat(64)},
          1, ${viewId}, 4, 1, 1, 1, 1, ${"0".repeat(64)}, 1, 1, ${"0".repeat(64)})`
        yield* sql`INSERT INTO effect_local_client_scoped_bootstrap_entries
        (space_id, snapshot_id, ordinal, model, entity_key, change_json, entry_bytes)
        VALUES (${spaceId}, ${snapshotId}, 0, ${TodoV1.name}, ${yield* Codec.stringify("3")},
          ${yield* pipe(
          Protocol.Retract.make({
            entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "3" }
          }),
          Codec.stringify
        )}, 1)`

        const reached = yield* Deferred.make<void>()
        const PhaseRow = Schema.Struct({
          phase: Schema.String,
          generation: Schema.Number,
          source_generation: Schema.Number
        })
        const readProgress = SqlSchema.findOne({
          Request: Schema.Void,
          Result: PhaseRow,
          execute: () =>
            sql`SELECT phase, generation, source_generation FROM effect_local_client_evolution
            WHERE space_id = ${spaceId}`
        })
        const afterBatch = Effect.gen(function*() {
          const row = yield* readProgress(undefined)
          if (row.phase !== "Flip") return
          yield* Deferred.succeed(reached, undefined)
          yield* Effect.never
        }).pipe(Effect.catchTags({
          SchemaError: (error) => Effect.die(error),
          SqlError: (error) => Effect.die(error),
          NoSuchElementError: (error) => Effect.die(error)
        }))
        const layerRuntimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const fiber = yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 1,
          afterBatch
        }).pipe(Effect.provide(layerRuntimeV2), Effect.forkChild({ startImmediately: true }))
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
          WHERE space_id = ${spaceId} AND generation = ${generation}
            AND model = ${TodoV2.name} AND model_version = ${TodoV2.version}
            AND entity_key = ${migratedKey}`
        })(progress.generation)
        const targetVisible = yield* SqlSchema.findOne({
          Request: Schema.Number,
          Result: CountRow,
          execute: (generation) =>
            sql`SELECT COUNT(*) AS count FROM effect_local_client_visible_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${generation}
            AND model = ${TodoV2.name} AND entity_key = ${migratedKey}`
        })(progress.generation)
        assert.strictEqual(targetRetractions.count, 1)
        assert.strictEqual(targetVisible.count, 0)

        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const todoIsAbsent = Option.isNone(yield* v2.get(TodoV2, 3))
        assert.isTrue(todoIsAbsent)
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
          (SELECT COUNT(*) FROM effect_local_client_scoped_bootstrap
            WHERE space_id = ${spaceId}) AS staged,
          (SELECT COUNT(*) FROM effect_local_client_retractions
            WHERE space_id = ${spaceId} AND generation = ${progress.source_generation}) AS source_retractions
          FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
        })(undefined)
        assert.strictEqual(final.replication_view_id, null)
        assert.strictEqual(final.replication_view_revision, 0)
        assert.strictEqual(final.staged, 0)
        assert.strictEqual(final.source_retractions, 0)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "bounds generation work and resumes cleanup after the active flip",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        yield* buildStore(definitionV1, layerHandlersV1)
        const source = (yield* sql<{
          readonly active_schema_generation: number
          readonly active_projection_generation: number
        }>`SELECT active_schema_generation, active_projection_generation
        FROM effect_local_client_spaces WHERE space_id = ${spaceId}`)[0]
        for (let index = 1; index <= 3; index++) {
          const key = yield* pipe(String(index), Codec.stringify)
          const value = yield* Codec.stringify({ id: String(index), title: `todo-${index}` })
          yield* sql`INSERT INTO effect_local_client_canonical_entities_data
          (space_id, schema_generation, model, model_version, entity_key, value_json)
          VALUES (${spaceId}, ${source.active_schema_generation}, ${TodoV1.name},
            ${TodoV1.version}, ${key}, ${value})`
          yield* sql`INSERT INTO effect_local_client_visible_entities_data
          (space_id, schema_generation, projection_generation, model, model_version, entity_key, value_json)
          VALUES (${spaceId}, ${source.active_schema_generation}, ${source.active_projection_generation},
            ${TodoV1.name}, ${TodoV1.version}, ${key}, ${value})`
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
          const phase = (yield* sql<typeof PhaseRow.Type>`SELECT phase FROM effect_local_client_evolution
          WHERE space_id = ${spaceId}`)[0]?.phase
          if (phase === "CleanupCanonical") {
            yield* Deferred.succeed(flipped, undefined)
            yield* Effect.never
          }
        }).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
        const layerRuntimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const fiber = yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 1,
          afterBatch
        }).pipe(Effect.provide(layerRuntimeV2), Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(flipped)
        yield* Fiber.interrupt(fiber)

        const active = yield* sql<{ readonly active_schema_generation: number }>`SELECT active_schema_generation
        FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
        assert.strictEqual(active[0].active_schema_generation, source.active_schema_generation + 1)
        assert.isAtMost(yield* Ref.get(maximumWrites), 2)

        yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 1
        }).pipe(Effect.provide(layerRuntimeV2))
        const remaining = yield* sql<{ readonly count: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}) +
        (SELECT COUNT(*) FROM effect_local_client_visible_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}) +
        (SELECT COUNT(*) FROM effect_local_client_receipts_data
          WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}) +
        (SELECT COUNT(*) FROM effect_local_client_pending_data
          WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}) AS count`
        assert.strictEqual(remaining[0].count, 0)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "recovers schema promotion from an interrupted projection replay",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.mutate(PutTodoV1, { id: "7", title: "pending" })
        yield* v1.applyEntries([Protocol.AcceptedMutation.make({
          sequence: Identity.ServerSequence.make(1),
          spaceId,
          clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
          membershipIncarnation,
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000207"),
          localSequence: Identity.LocalSequence.make(1),
          sourceSchema: definitionV1.schemaIdentity,
          digest: "7".repeat(64),
          changes: [Protocol.Upsert.make({
            entity: { model: TodoV1.name, modelVersion: TodoV1.version, key: "8" },
            value: { id: "8", title: "committed" }
          })]
        })])
        const source = (yield* sql<{
          readonly active_schema_generation: number
          readonly active_projection_generation: number
        }>`SELECT active_schema_generation, active_projection_generation
        FROM effect_local_client_spaces WHERE space_id = ${spaceId}`)[0]
        const interruptedProjection = source.active_projection_generation + 1
        yield* sql`INSERT INTO effect_local_client_visible_entities_data
        (space_id, schema_generation, projection_generation, model, model_version, entity_key, value_json)
        SELECT space_id, schema_generation, ${interruptedProjection}, model, model_version, entity_key, value_json
        FROM effect_local_client_visible_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}
          AND projection_generation = ${source.active_projection_generation}
        ORDER BY model, entity_key LIMIT 1`
        yield* sql`UPDATE effect_local_client_spaces
        SET projection_replay_generation = ${interruptedProjection}, projection_replay_cursor = 'pending:0'
        WHERE space_id = ${spaceId}`

        const batches = yield* Ref.make(0)
        const afterBatch = Ref.updateAndGet(batches, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count > 40) return Effect.die("schema evolution cleanup stalled")
            return Effect.void
          })
        )
        const layerRuntimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 1,
          afterBatch
        }).pipe(Effect.provide(layerRuntimeV2))

        const metadata = (yield* sql<{
          readonly projection_replay_generation: number | null
          readonly projection_replay_cursor: string | null
        }>`SELECT projection_replay_generation, projection_replay_cursor
        FROM effect_local_client_spaces WHERE space_id = ${spaceId}`)[0]
        assert.isNull(metadata.projection_replay_generation)
        assert.isNull(metadata.projection_replay_cursor)
        const remaining = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_client_visible_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${source.active_schema_generation}`
        assert.strictEqual(remaining[0].count, 0)

        const v2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        assert.strictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 7)).title, "pending")
        assert.strictEqual(Option.getOrThrow(yield* v2.get(TodoV2, 8)).title, "committed")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects one evolution row larger than the configured byte budget",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.mutate(PutTodoV1, { id: "4", title: "x".repeat(256) })
        const layerRuntimeV2 = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const result = yield* SchemaEvolution.client({
          definition: definitionV2,
          evolution,
          spaceId,
          clientId,
          batchSize: 10,
          batchBytes: 32
        }).pipe(Effect.provide(layerRuntimeV2), Effect.result)
        const error = expectFailure(result)
        assert.strictEqual(error._tag, "CapacityExceeded")
        if (error._tag === "CapacityExceeded") {
          assert.strictEqual(error.resource, "schema evolution row bytes")
          assert.strictEqual(error.limit, 32)
        }
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects unrelated source keys that converge on one target",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        yield* v1.mutate(PutTodoV1, { id: "01", title: "first" })
        yield* v1.mutate(PutTodoV1, { id: "1", title: "second" })

        const result = yield* buildStore(definitionV2, layerHandlersV2, evolution).pipe(Effect.result)
        const error = expectFailure(result)
        assert.strictEqual(error._tag, "SchemaKeyCollision")
        const CountRow = Schema.Struct({ count: Schema.Number })
        const source = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: CountRow,
          execute: () =>
            sql`SELECT COUNT(*) AS count FROM effect_local_client_pending_data AS p
          INNER JOIN effect_local_client_spaces AS s ON s.space_id = p.space_id
            AND s.active_schema_generation = p.schema_generation
          WHERE p.space_id = ${spaceId}`
        })(undefined)
        const published = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: CountRow,
          execute: () =>
            sql`SELECT COUNT(*) AS count FROM effect_local_client_visible_entities_data AS v
          INNER JOIN effect_local_client_spaces AS s ON s.space_id = v.space_id
            AND s.active_schema_generation = v.schema_generation
            AND s.active_projection_generation = v.projection_generation
          WHERE v.space_id = ${spaceId} AND v.model_version = ${TodoV2.version}`
        })(undefined)
        assert.strictEqual(source.count, 2)
        assert.strictEqual(published.count, 0)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "promotes a server space and admits an old offline envelope through the current handler",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const localV1 = yield* buildStore(definitionV1, layerHandlersV1)
        const serverV1 = yield* buildServer(definitionV1, layerHandlersV1)
        yield* sql`INSERT INTO effect_local_server_offline_wake_acknowledgements
          (space_id, client_id, acknowledged_sequence) VALUES (${spaceId}, ${clientId}, 1)`
        const accepted = yield* localV1.mutate(PutTodoV1, { id: "4", title: "accepted-old" })
        assert.strictEqual((yield* serverV1.submit(accepted.envelope))._tag, "Accepted")
        const offline = yield* localV1.mutate(PutTodoV1, { id: "5", title: "offline-old" })

        const serverV2 = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const wakeAcknowledgements = yield* sql<{ readonly acknowledged_sequence: number }>`
          SELECT acknowledged_sequence FROM effect_local_server_offline_wake_acknowledgements
          WHERE space_id = ${spaceId} AND client_id = ${clientId}`
        assert.deepStrictEqual(wakeAcknowledgements, [{ acknowledged_sequence: 1 }])
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
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "starts after quarantining a pending mutation rejected during promotion",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "42", title: "rejected-on-upgrade" })

        const v2 = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const quarantined = yield* v2.quarantine
        assert.strictEqual(quarantined.length, 1)
        assert.strictEqual(quarantined[0].envelope.mutationId, pending.envelope.mutationId)
        const decodeRejection = Schema.decodeUnknownEffect(SchemaPolicyRejectedError)
        const quarantineRejection = yield* decodeRejection(quarantined[0].rejection)
        assert.strictEqual(quarantineRejection.reason, "schema-policy-rejected")
        assert.strictEqual((yield* v2.pending).length, 0)

        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const discarded = yield* server.discard({
          envelope: quarantined[0].envelope,
          schema: definitionV2.schemaIdentity
        }, null)
        assert.strictEqual(discarded._tag, "Rejected")
        if (discarded._tag === "Rejected") assert.strictEqual(discarded.origin, "Quarantine")
        yield* v2.resolveQuarantine(discarded)
        assert.deepStrictEqual(yield* v2.quarantine, [])

        const writableV2 = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const replacement = yield* writableV2.mutate(PutTodoV2, {
          id: 42,
          title: "corrected",
          done: false
        })
        assert.strictEqual(replacement.envelope.localSequence, 2)
        assert.strictEqual(
          (yield* server.submit({
            envelope: replacement.envelope,
            schema: definitionV2.schemaIdentity
          }))._tag,
          "Accepted"
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "keeps quarantine intact when disposition receipt identity or provenance is invalid",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "91", title: "quarantined" })
        const v2 = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const item = Option.getOrThrow(yield* v2.quarantineByMutation(pending.envelope.mutationId))
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const valid = yield* server.discard({ envelope: item.envelope, schema: v2.schema }, null)
        if (valid._tag !== "Rejected") assert.fail("expected rejected quarantine receipt")
        const invalidReceipts = [
          Protocol.RejectedReceipt.make({ ...valid, name: "DifferentMutation" }),
          Protocol.RejectedReceipt.make({
            ...valid,
            sourceSchema: {
              ...definitionV2.schemaIdentity,
              hash: Identity.SchemaHash.make("0".repeat(16))
            }
          }),
          Protocol.RejectedReceipt.make({
            ...valid,
            mutationVersion: Identity.SchemaVersion.make(valid.mutationVersion + 1)
          })
        ]

        for (const receipt of invalidReceipts) {
          const error = yield* v2.resolveQuarantine(receipt).pipe(Effect.flip)
          assert.strictEqual(error._tag, "ProtocolInvalid")
          const quarantineIsPresent = Option.isSome(
            yield* v2.quarantineByMutation(pending.envelope.mutationId)
          )
          assert.isTrue(quarantineIsPresent)
          assert.deepStrictEqual(yield* v2.pending, [])
          const receiptIsAbsent = Option.isNone(yield* v2.receipt(pending.envelope.mutationId))
          assert.isTrue(receiptIsAbsent)
        }
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "keeps quarantine durable when receipt persistence fails during disposition",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const pending = yield* v1.mutate(PutTodoV1, { id: "43", title: "atomic-disposition" })
        const v2 = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const quarantined = yield* v2.quarantine
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const receipt = yield* server.discard({
          envelope: quarantined[0].envelope,
          schema: definitionV2.schemaIdentity
        }, null)

        yield* sql`CREATE TRIGGER fail_quarantine_receipt BEFORE INSERT ON effect_local_client_receipts_data
        BEGIN SELECT RAISE(ABORT, 'receipt persistence failed'); END`
        assert.strictEqual((yield* v2.resolveQuarantine(receipt).pipe(Effect.flip))._tag, "StorageUnavailable")
        yield* sql`DROP TRIGGER fail_quarantine_receipt`

        const reopened = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        assert.deepStrictEqual((yield* reopened.quarantine).map((item) => item.envelope.mutationId), [
          pending.envelope.mutationId
        ])
        assert.deepStrictEqual(yield* reopened.pending, [])
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "keeps a quarantined mutation inspectable when its replacement rejects",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "44", title: "retryable-resubmit" })
        const rejecting = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const item = (yield* rejecting.quarantine)[0]
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const receipt = yield* server.discard({ envelope: item.envelope, schema: definitionV2.schemaIdentity }, null)

        const rejectedResult = yield* rejecting.ensureQuarantineResubmission(original.envelope.mutationId, PutTodoV2, {
          id: 44,
          title: "still-rejected",
          done: false
        }).pipe(Effect.result)
        const rejectedError = expectFailure(rejectedResult)
        assert.strictEqual(rejectedError._tag, "SchemaPolicyRejectedError")
        if (rejectedError._tag === "SchemaPolicyRejectedError") {
          assert.strictEqual(rejectedError.reason, "schema-policy-rejected")
        }
        assert.deepStrictEqual((yield* rejecting.quarantine).map(({ envelope }) => envelope.mutationId), [
          original.envelope.mutationId
        ])

        const writable = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const replacement = yield* writable.ensureQuarantineResubmission(original.envelope.mutationId, PutTodoV2, {
          id: 44,
          title: "corrected",
          done: false
        })
        const retry = yield* writable.ensureQuarantineResubmission(original.envelope.mutationId, PutTodoV2, {
          id: 44,
          title: "corrected",
          done: false
        })
        assert.strictEqual(retry.envelope.mutationId, replacement.envelope.mutationId)
        yield* writable.resolveQuarantine(receipt)
        assert.deepStrictEqual(yield* writable.quarantine, [])
        assert.deepStrictEqual((yield* writable.pending).map(({ envelope }) => envelope.mutationId), [
          replacement.envelope.mutationId
        ])
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "cancels a staged public resubmission when the original is later discarded",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "45", title: "original" })
        yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const stagingStore = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const firstError = yield* buildReplica(definitionV2, layerHandlersV2, unavailableSync, evolution).pipe(
          Effect.flatMap((replica) =>
            replica.resubmitQuarantined(
              original.envelope.mutationId,
              PutTodoV2,
              { id: 45, title: "staged", done: false }
            ).pipe(Effect.flip)
          ),
          Effect.scoped
        )
        assert.isTrue(pipe(firstError, Schema.is(ReplicaError.ServerUnavailable)))

        assert.strictEqual(yield* stagingStore.pendingCount, 1)
        const staged = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ replacement_mutation_id: Identity.MutationId }),
          execute: () =>
            sql`SELECT replacement_mutation_id
            FROM effect_local_client_quarantine_resubmissions
            WHERE original_mutation_id = ${original.envelope.mutationId}`
        })(undefined)
        pipe(
          Option.getOrThrow(yield* stagingStore.get(TodoV2, 45)),
          (todo) => assert.deepStrictEqual(todo, { id: 45, title: "staged", done: false })
        )
        const conflict = yield* buildReplica(definitionV2, layerHandlersV2, unavailableSync, evolution).pipe(
          Effect.flatMap((replica) =>
            replica.resubmitQuarantined(
              original.envelope.mutationId,
              PutTodoV2,
              { id: 45, title: "different", done: false }
            ).pipe(Effect.flip)
          ),
          Effect.scoped
        )
        assert.isTrue(pipe(conflict, Schema.is(ReplicaError.QuarantineResubmissionConflict)))

        const reactivity = yield* Reactivity.Reactivity
        const invalidations = { entity: 0, status: 0, originalReceipt: 0, replacementReceipt: 0 }
        const spaceKey = `effect-local:space:${spaceId}`
        const cancelEntity = reactivity.registerUnsafe(
          [ReactivityKey.entity(spaceId, TodoV2.name, 45)],
          () => invalidations.entity++
        )
        const cancelStatus = reactivity.registerUnsafe([`${spaceKey}:status`], () => invalidations.status++)
        const cancelOriginalReceipt = reactivity.registerUnsafe(
          [`${spaceKey}:receipt:${original.envelope.mutationId}`],
          () => invalidations.originalReceipt++
        )
        const cancelReplacementReceipt = reactivity.registerUnsafe(
          [`${spaceKey}:receipt:${staged.replacement_mutation_id}`],
          () => invalidations.replacementReceipt++
        )
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            cancelEntity()
            cancelStatus()
            cancelOriginalReceipt()
            cancelReplacementReceipt()
          })
        )

        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const replica = yield* buildReplica(definitionV2, layerHandlersV2, serverSync(server), evolution)
        const receipt = yield* replica.discardQuarantined(original.envelope.mutationId)
        assert.strictEqual(receipt._tag, "Rejected")
        assert.deepStrictEqual(yield* replica.quarantine, [])
        assert.deepStrictEqual(yield* stagingStore.pending, [])
        const stagedTodoIsAbsent = Option.isNone(yield* stagingStore.get(TodoV2, 45))
        assert.isTrue(stagedTodoIsAbsent)
        const required = yield* server.pull(pullRequest(definitionV2))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(definitionV2, required.manifest))
        assert.deepStrictEqual(page.entries, [])
        assert.isAtLeast(invalidations.entity, 1)
        assert.isAtLeast(invalidations.status, 1)
        assert.isAtLeast(invalidations.originalReceipt, 1)
        assert.isAtLeast(invalidations.replacementReceipt, 1)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "submits intervening pending mutations before discarding a staged replacement",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "49", title: "original" })
        yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const writable = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        yield* writable.mutate(PutTodoV2, {
          id: 50,
          title: "intervening",
          done: false
        })
        const staged = yield* buildReplica(definitionV2, layerHandlersV2, unavailableSync, evolution).pipe(
          Effect.flatMap((replica) =>
            replica.resubmitQuarantined(original.envelope.mutationId, PutTodoV2, {
              id: 49,
              title: "staged",
              done: false
            }).pipe(Effect.flip)
          ),
          Effect.scoped
        )
        assert.isTrue(pipe(staged, Schema.is(ReplicaError.ServerUnavailable)))

        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const replica = yield* buildReplica(definitionV2, layerHandlersV2, serverSync(server), evolution)
        yield* replica.discardQuarantined(original.envelope.mutationId)

        assert.deepStrictEqual(yield* replica.quarantine, [])
        assert.deepStrictEqual(yield* writable.pending, [])
        const required = yield* server.pull(pullRequest(definitionV2))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(definitionV2, required.manifest))
        pipe(page.entries.map(({ change }) => change), (changes) =>
          assert.deepStrictEqual(changes, [
            Protocol.Upsert.make({
              entity: Protocol.EntityKey.make({ model: TodoV2.name, modelVersion: TodoV2.version, key: 50 }),
              value: { id: 50, title: "intervening", done: false }
            })
          ]))
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "resumes staged replacement cancellation after an intervening submit failure",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "51", title: "original" })
        yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const writable = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const intervening = yield* writable.mutate(PutTodoV2, {
          id: 52,
          title: "intervening",
          done: false
        })
        assert.strictEqual(intervening.envelope.localSequence, 2)
        const staged = yield* buildReplica(definitionV2, layerHandlersV2, unavailableSync, evolution).pipe(
          Effect.flatMap((replica) =>
            replica.resubmitQuarantined(original.envelope.mutationId, PutTodoV2, {
              id: 51,
              title: "staged",
              done: false
            }).pipe(Effect.flip)
          ),
          Effect.scoped
        )
        assert.isTrue(pipe(staged, Schema.is(ReplicaError.ServerUnavailable)))

        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const live = serverSync(server)
        const failSubmit = yield* Ref.make(true)
        const failOnce = SyncEngine.SyncEngine.of({
          ...live,
          submit: Effect.fnUntraced(function*(request) {
            const shouldFail = yield* Ref.get(failSubmit)
            if (shouldFail) return yield* new ReplicaError.ServerUnavailable()
            return yield* live.submit(request)
          })
        })
        const replica = yield* buildReplica(definitionV2, layerHandlersV2, failOnce, evolution)
        const firstError = yield* replica.discardQuarantined(original.envelope.mutationId).pipe(Effect.flip)
        assert.strictEqual(firstError._tag, "ServerUnavailable")
        yield* Ref.set(failSubmit, false)

        yield* Ref.set(failSubmit, false)
        const receipt = yield* replica.discardQuarantined(original.envelope.mutationId)
        assert.strictEqual(receipt._tag, "Rejected")
        assert.deepStrictEqual(yield* replica.quarantine, [])
        assert.deepStrictEqual(yield* writable.pending, [])
        const required = yield* server.pull(pullRequest(definitionV2))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(definitionV2, required.manifest))
        pipe(page.entries.map(({ change }) => change), (changes) =>
          assert.deepStrictEqual(changes, [
            Protocol.Upsert.make({
              entity: Protocol.EntityKey.make({ model: TodoV2.name, modelVersion: TodoV2.version, key: 52 }),
              value: { id: 52, title: "intervening", done: false }
            })
          ]))
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "resubmits a quarantined mutation through the public replica composition",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "48", title: "original" })
        yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const writable = yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const replica = yield* buildReplica(definitionV2, layerHandlersV2, serverSync(server), evolution)

        const result = yield* replica.resubmitQuarantined(original.envelope.mutationId, PutTodoV2, {
          id: 48,
          title: "replacement",
          done: false
        })

        assert.strictEqual(result._tag, "Resubmitted")
        if (result._tag !== "Resubmitted") return
        assert.deepStrictEqual(yield* replica.quarantine, [])
        assert.deepStrictEqual((yield* writable.pending).map(({ envelope }) => envelope.mutationId), [
          result.pending.envelope.mutationId
        ])
        pipe(
          Option.getOrThrow(yield* replica.get(TodoV2, 48)),
          (todo) => assert.deepStrictEqual(todo, { id: 48, title: "replacement", done: false })
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "keeps the original recoverable when its replacement is quarantined by promotion",
    Effect.fnUntraced(
      function*() {
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "46", title: "original" })
        yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        yield* buildStore(definitionV2, layerHandlersV2, evolution)
        const firstError = yield* buildReplica(definitionV2, layerHandlersV2, unavailableSync, evolution).pipe(
          Effect.flatMap((replica) =>
            replica.resubmitQuarantined(original.envelope.mutationId, PutTodoV2, {
              id: 46,
              title: "replacement",
              done: false
            }).pipe(Effect.flip)
          ),
          Effect.scoped
        )
        assert.isTrue(pipe(firstError, Schema.is(ReplicaError.ServerUnavailable)))

        const v3 = yield* buildStore(definitionV3, layerRejectingHandlersV3, evolutionV3)
        const quarantined = yield* v3.quarantine
        assert.strictEqual(quarantined.length, 2)
        const replacement = quarantined.find(({ envelope }) => envelope.mutationId !== original.envelope.mutationId)
        assert.isDefined(replacement)
        if (replacement === undefined) return
        assert.deepStrictEqual(replacement.envelope, {
          ...replacement.envelope,
          name: PutTodoV2.name,
          payload: { id: 46, title: "replacement", done: false },
          mutationVersion: PutTodoV2.version,
          sourceSchema: definitionV2.schemaIdentity
        })
        const rejectedResult = yield* v3.ensureQuarantineResubmission(original.envelope.mutationId, PutTodoV2, {
          id: 46,
          title: "replacement",
          done: false
        }).pipe(Effect.result)
        const rejectedError = expectFailure(rejectedResult)
        assert.strictEqual(rejectedError._tag, "SchemaPolicyRejectedError")
        if (rejectedError._tag === "SchemaPolicyRejectedError") {
          assert.strictEqual(rejectedError.reason, "schema-policy-rejected-v3")
        }
        const replacementQuarantineIsPresent = Option.isSome(
          yield* v3.quarantineByMutation(original.envelope.mutationId)
        )
        assert.isTrue(replacementQuarantineIsPresent)

        const server = yield* buildServer(definitionV3, layerHandlersV3, evolutionV3)
        const currentReplica = yield* buildReplica(definitionV3, layerHandlersV3, serverSync(server), evolutionV3)
        yield* currentReplica.discardQuarantined(original.envelope.mutationId)
        assert.deepStrictEqual(yield* currentReplica.quarantine, [])
        assert.deepStrictEqual(yield* v3.pending, [])
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "does not invalidate quarantine dependencies before the outer disposition commits",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const reactivity = yield* Reactivity.Reactivity
        const v1 = yield* buildStore(definitionV1, layerHandlersV1)
        const original = yield* v1.mutate(PutTodoV1, { id: "47", title: "atomic-invalidation" })
        const v2 = yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
        const item = Option.getOrThrow(yield* v2.quarantineByMutation(original.envelope.mutationId))
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const receipt = yield* server.discard({ envelope: item.envelope, schema: v2.schema }, null)
        let invalidations = 0
        const cancel = reactivity.registerUnsafe([
          `effect-local:space:${spaceId}:receipt:${original.envelope.mutationId}`
        ], () => invalidations++)
        yield* Effect.addFinalizer(() => Effect.sync(cancel))
        yield* sql`CREATE TRIGGER fail_quarantine_delete BEFORE DELETE ON effect_local_client_quarantine
        BEGIN SELECT RAISE(ABORT, 'quarantine delete failed'); END`

        assert.strictEqual((yield* v2.resolveQuarantine(receipt).pipe(Effect.flip))._tag, "StorageUnavailable")
        assert.strictEqual(invalidations, 0)
        const originalQuarantineIsPresent = Option.isSome(
          yield* v2.quarantineByMutation(original.envelope.mutationId)
        )
        assert.isTrue(originalQuarantineIsPresent)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "serves schemas inside the configured window across source schema evolution",
    Effect.fnUntraced(
      function*() {
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution, {
          acceptedSchemaVersions: 1,
          retainedHistoryEntries: 0
        })
        const envelope = yield* v1Envelope(
          clientId,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000160"),
          1,
          { id: "42", title: "mixed-version" }
        )

        const receipt = yield* server.admit({ envelope, schema: definitionV1.schemaIdentity }, "principal")
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag === "Accepted") {
          assert.deepStrictEqual(receipt.sourceSchema, definitionV1.schemaIdentity)
          assert.strictEqual(receipt.mutationVersion, PutTodoV1.version)
          assert.deepStrictEqual(receipt.result, { id: "42", title: "mixed-version" })
        }

        const required = yield* server.pullAuthorized(pullRequest(definitionV1), "principal")
        if (!("_tag" in required)) assert.fail("expected a projected bootstrap manifest")
        assert.deepStrictEqual(required.manifest.schema, definitionV1.schemaIdentity)
        assert.strictEqual(required.manifest.definitionHash, definitionV1.hash)
        assert.deepStrictEqual(required.serverSchema, definitionV2.schemaIdentity)
        const page = yield* server.bootstrapAuthorized(
          bootstrapRequest(definitionV1, required.manifest, TodoV1.name),
          "principal"
        )
        assert.deepStrictEqual(page.manifest, required.manifest)
        pipe(
          page.entries.map(({ entryBytes: _, ...entry }) => entry),
          (entries) =>
            assert.deepStrictEqual(entries, [{
              ordinal: 0,
              change: Protocol.Upsert.make({
                entity: Protocol.EntityKey.make({ model: TodoV1.name, modelVersion: TodoV1.version, key: "42" }),
                value: { id: "42", title: "mixed-version" }
              })
            }])
        )
        assert.isFalse(page.hasMore)

        const serverV3 = yield* buildServer(definitionV3, layerHandlersV3, evolutionV3, { acceptedSchemaVersions: 2 })
        const replacement = yield* serverV3.pullAuthorized(pullRequest(definitionV1), "principal")
        if (!("_tag" in replacement)) assert.fail("expected a replacement bootstrap manifest")
        assert.notStrictEqual(replacement.manifest.snapshotId, required.manifest.snapshotId)
        assert.deepStrictEqual(replacement.serverSchema, definitionV3.schemaIdentity)
        const replacementPage = yield* serverV3.bootstrapAuthorized(
          bootstrapRequest(definitionV1, replacement.manifest, TodoV1.name),
          "principal"
        )
        pipe(
          replacementPage.entries.map(({ entryBytes: _, ...entry }) => entry),
          (entries) =>
            assert.deepStrictEqual(entries, [{
              ordinal: 0,
              change: Protocol.Upsert.make({
                entity: Protocol.EntityKey.make({ model: TodoV1.name, modelVersion: TodoV1.version, key: "42" }),
                value: { id: "42", title: "mixed-version" }
              })
            }])
        )
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects a historical schema for a windowed watch",
    Effect.fnUntraced(
      function*() {
        const server = yield* buildServer(definitionV2, layerHandlersV2, evolution, { acceptedSchemaVersions: 1 })
        const request = Protocol.WatchRequest.make({
          spaceId,
          clientId,
          schema: definitionV1.schemaIdentity,
          scope: Protocol.ReplicationScope.make({
            models: [],
            windows: [Protocol.ReplicationWindow.make({ model: TodoV1.name, index: "byTitle", count: 1 })]
          }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        })
        const outcome = yield* server.watchAuthorized(request, "principal").pipe(Effect.result)
        assert.isTrue(Result.isFailure(outcome))
        if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "ProtocolInvalid")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "rejects an accepted schema window without complete downgrade transforms",
    Effect.fnUntraced(
      function*() {
        const result = yield* buildServer(definitionV2, layerHandlersV2, forwardOnlyEvolution, {
          acceptedSchemaVersions: 1
        }).pipe(Effect.result)
        const error = expectFailure(result)
        assert.strictEqual(error._tag, "InvalidConfiguration")
        if (error._tag === "InvalidConfiguration") assert.strictEqual(error.option, "acceptedSchemaVersions")
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "paginates pull by the projected wire representation",
    Effect.fnUntraced(
      function*() {
        const server = yield* buildServer(expandedDefinitionV2, layerExpandedHandlers, expandingEvolution, {
          acceptedSchemaVersions: 1
        })
        for (let sequence = 1; sequence <= 2; sequence++) {
          const mutationId = Identity.MutationId.make(`mut_00000000-0000-4000-8000-00000000017${sequence}`)
          const identity = {
            spaceId,
            clientId,
            mutationId,
            localSequence: Identity.LocalSequence.make(sequence),
            basis: Identity.ServerSequence.make(sequence - 1),
            name: PutExpanded.name,
            payload: { id: String(sequence), compact: "x".repeat(30_000) },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: expandedDefinitionV2.schemaIdentity,
            mutationVersion: PutExpanded.version
          }
          const envelope = Protocol.MutationEnvelope.make({
            ...identity,
            digest: yield* Protocol.mutationDigest(identity)
          })
          assert.strictEqual(
            (yield* server.submit({ envelope, schema: expandedDefinitionV2.schemaIdentity }))._tag,
            "Accepted"
          )
        }

        const required = yield* server.pull(pullRequest(expandedDefinitionV1, null, ExpandedV1.name))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const first = yield* server.bootstrap(
          bootstrapRequest(expandedDefinitionV1, required.manifest, ExpandedV1.name)
        )
        assert.strictEqual(first.entries.length, 1)
        assert.isTrue(first.hasMore)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(first), Protocol.maximumBatchBytes)

        const second = yield* server.bootstrap(bootstrapRequest(
          expandedDefinitionV1,
          required.manifest,
          ExpandedV1.name,
          first.entries[0].ordinal
        ))
        assert.strictEqual(second.entries.length, 1)
        assert.isFalse(second.hasMore)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "pages by the compact projected representation instead of source entity bytes",
    Effect.fnUntraced(
      function*() {
        const server = yield* buildServer(largeDefinitionV2, layerLargeHandlers, compactingEvolution, {
          acceptedSchemaVersions: 1
        })
        for (let sequence = 1; sequence <= 2; sequence++) {
          const identity = {
            spaceId,
            clientId,
            mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-00000000018${sequence}`),
            localSequence: Identity.LocalSequence.make(sequence),
            basis: Identity.ServerSequence.make(sequence - 1),
            name: PutLarge.name,
            payload: { id: String(sequence) },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: largeDefinitionV2.schemaIdentity,
            mutationVersion: PutLarge.version
          }
          const envelope = Protocol.MutationEnvelope.make({
            ...identity,
            digest: yield* Protocol.mutationDigest(identity)
          })
          assert.strictEqual(
            (yield* server.submit({ envelope, schema: largeDefinitionV2.schemaIdentity }))._tag,
            "Accepted"
          )
        }

        const required = yield* server.pull(pullRequest(compactDefinitionV1, null, CompactV1.name))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(compactDefinitionV1, required.manifest, CompactV1.name))
        assert.strictEqual(page.entries.length, 2)
        assert.isFalse(page.hasMore)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(page), Protocol.maximumBatchBytes)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "reports durable snapshot projection key conflicts as schema collisions",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const server = yield* buildServer(definitionV2, layerHandlersV2, collidingProjectionEvolution, {
          acceptedSchemaVersions: 1,
          retainedHistoryEntries: 0
        })
        for (let sequence = 1; sequence <= 2; sequence++) {
          const identity = {
            spaceId,
            clientId,
            mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-00000000019${sequence}`),
            localSequence: Identity.LocalSequence.make(sequence),
            basis: Identity.ServerSequence.make(sequence - 1),
            name: PutTodoV2.name,
            payload: { id: sequence, title: `collision-${sequence}`, done: false },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: definitionV2.schemaIdentity,
            mutationVersion: PutTodoV2.version
          }
          const envelope = Protocol.MutationEnvelope.make({
            ...identity,
            digest: yield* Protocol.mutationDigest(identity)
          })
          assert.strictEqual((yield* server.submit({ envelope, schema: definitionV2.schemaIdentity }))._tag, "Accepted")
        }
        yield* server.maintain(spaceId)

        const pullResult = yield* pipe(pullRequest(definitionV1), server.pull, Effect.result)
        const error = expectFailure(pullResult)
        assert.strictEqual(error._tag, "SchemaKeyCollision")
        const ProjectionCounts = Schema.Struct({ manifests: Schema.Number, entities: Schema.Number })
        const projectionCounts = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: ProjectionCounts,
          execute: () =>
            sql`SELECT
          (SELECT COUNT(*) FROM effect_local_server_snapshot_projections) AS manifests,
          (SELECT COUNT(*) FROM effect_local_server_snapshot_projection_entities) AS entities`
        })(undefined)
        assert.deepStrictEqual(projectionCounts, { manifests: 0, entities: 0 })
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "resumes a server promotion after interruption",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const serverV1 = yield* buildServer(definitionV1, layerHandlersV1)
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
        }).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
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

        const serverV2 = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        yield* pipe(pullRequest(definitionV2), serverV2.pull)
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
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "invalidates scoped server views atomically with a schema generation flip",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const serverV1 = yield* buildServer(definitionV1, layerHandlersV1)
        const scopedEnvelope = yield* v1Envelope(
          clientId,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000221"),
          1,
          { id: "7", title: "scoped-view" }
        )
        yield* serverV1.submit(scopedEnvelope)
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
        }).pipe(Effect.catchTags({
          SchemaError: (error) => Effect.die(error),
          SqlError: (error) => Effect.die(error),
          NoSuchElementError: (error) => Effect.die(error)
        }))
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
        const serverV2 = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        const replacementRequest = Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definitionV2.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [TodoV2.name] }),
          scopeGeneration,
          cursor: outstanding.cursor,
          limit: 10
        })
        const result = yield* serverV2.pullAuthorized(replacementRequest, null)
        if (!("_tag" in result)) assert.fail("expected a replacement bootstrap")
        assert.notStrictEqual(result.manifest.cursor.viewId, outstanding.cursor.viewId)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "fences scoped writes after the prepared schema generation changes",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/schema-fence.sqlite`
        const persistentDatabase = () =>
          Layer.mergeAll(SqliteClient.layer({ filename }), NodeCrypto.layer, Reactivity.layer)
        const prepared = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        const scopeAuthorizations = yield* Ref.make(0)
        const layerRuntimeV1 = MutationRuntime.layer(definitionV1).pipe(Layer.provide(layerHandlersV1))
        const layerServerLayerV1 = ServerStore.layer({
          ...serverHistory,
          definition: definitionV1,
          schemaEvolutionBatchSize: 1,
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: (input) => {
            if (input._tag === "Entity") return Effect.void
            return Ref.updateAndGet(scopeAuthorizations, (count) => count + 1).pipe(
              Effect.flatMap((count) => {
                if (count !== 2) return Effect.void
                return Deferred.succeed(prepared, undefined).pipe(Effect.andThen(Deferred.await(resume)))
              })
            )
          }
        })
        const serverV1 = yield* layerServerLayerV1.pipe(
          Layer.provide(layerRuntimeV1),
          Layer.provide(persistentDatabase()),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
        const staleRequest = Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definitionV1.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [TodoV1.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null,
          limit: 10
        })
        const stale = yield* serverV1.pullAuthorized(staleRequest, null).pipe(
          Effect.result,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(prepared)
        yield* SchemaEvolution.server({ definition: definitionV2, evolution, spaceId, batchSize: 1 }).pipe(
          Effect.provide(SqliteClient.layer({ filename }))
        )
        yield* Deferred.succeed(resume, undefined)
        const result = yield* Fiber.join(stale)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "SchemaGenerationConflict")
        const CountRow = Schema.Struct({ count: Schema.Number })
        const staleCount = yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return yield* SqlSchema.findOne({
            Request: Schema.Void,
            Result: CountRow,
            execute: () =>
              sql`SELECT
              (SELECT COUNT(*) FROM effect_local_server_replication_views WHERE space_id = ${spaceId}) +
              (SELECT COUNT(*) FROM effect_local_server_scoped_snapshots WHERE space_id = ${spaceId}) AS count`
          })(undefined)
        })
          .pipe(Effect.provide(SqliteClient.layer({ filename })))
        assert.strictEqual(staleCount.count, 0)
      },
      Effect.scoped,
      provideNodeFileSystem
    )
  )

  it.effect(
    "migrates every server receipt when client sequences overlap",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const serverV1 = yield* buildServer(definitionV1, layerHandlersV1)
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

        const serverV2 = yield* buildServer(definitionV2, layerHandlersV2, evolution)
        yield* pipe(pullRequest(definitionV2), serverV2.pull)
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
        const decoded = yield* Effect.forEach(
          migrated,
          (row) => Codec.parse(row.receipt_json).pipe(Effect.flatMap((json) => Codec.decode(Protocol.Receipt, json)))
        )
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
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "authorizes the migrated current mutation view",
    Effect.fnUntraced(
      function*() {
        const serverV1 = yield* buildServer(definitionV1, layerHandlersV1)
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
        const layerRuntime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const server = yield* ServerStore.layer({
          ...serverHistory,
          definition: definitionV2,
          evolution,
          authorizeAccess: () => Effect.void,
          authorizeMutation: ({ mutation }) => Ref.set(authorizedPayload, mutation.payload),
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(layerRuntime),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
        const receipt = yield* server.admit({ envelope: offline, schema: definitionV2.schemaIdentity }, "principal")
        assert.strictEqual(receipt._tag, "Accepted")
        assert.deepStrictEqual(yield* Ref.get(authorizedPayload), { id: 10, title: "authorized", done: false })
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "authorizes reads before schema disclosure or lazy space creation",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const layerRuntime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const server = yield* ServerStore.layer({
          ...serverHistory,
          definition: definitionV2,
          evolution,
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: ({ principal }) => {
            if (principal === "allowed") return Effect.void
            return pipe(ReadDeniedError.make({}), Effect.fail)
          }
        }).pipe(
          Layer.provide(layerRuntime),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
        const stale = pullRequest(definitionV1)
        const deniedResult = yield* server.pullAuthorized(stale, "denied").pipe(Effect.result)
        const deniedError = expectFailure(deniedResult)
        assert.strictEqual(deniedError._tag, "AuthorizationDenied")
        const CountRow = Schema.Struct({ count: Schema.Number })
        const countSpaces = SqlSchema.findOne({
          Request: Schema.Void,
          Result: CountRow,
          execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_server_spaces`
        })
        assert.strictEqual((yield* countSpaces(undefined)).count, 0)
        const staleResult = yield* server.pullAuthorized(stale, "allowed").pipe(Effect.result)
        const staleError = expectFailure(staleResult)
        assert.strictEqual(staleError._tag, "StaleSchema")
        assert.strictEqual((yield* countSpaces(undefined)).count, 0)
      },
      Effect.scoped,
      provideDatabase
    )
  )

  it.effect(
    "does not rewrite unchanged server schema metadata during preparation",
    Effect.fnUntraced(
      function*() {
        const sql = yield* SqlClient.SqlClient
        const layerRuntime = MutationRuntime.layer(definitionV2, evolution).pipe(Layer.provide(layerHandlersV2))
        const server = yield* ServerStore.layerTrusted({
          ...serverHistory,
          definition: definitionV2,
          evolution
        }).pipe(
          Layer.provide(layerRuntime),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
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
      },
      Effect.scoped,
      provideDatabase
    )
  )
})
