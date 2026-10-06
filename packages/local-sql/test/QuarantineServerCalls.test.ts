import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import { constructors, describeExit, within } from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000d001")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000d002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000d001")

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: ["Todo"] }),
  scope: Protocol.ReplicationScope.make({ models: ["Todo"] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
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
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds" as const,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}

class SchemaPolicyRejectedError extends Schema.TaggedError<SchemaPolicyRejectedError>(
  "@lucas-barake/effect-local-sql/test/SchemaPolicyRejectedError"
)("SchemaPolicyRejectedError", { reason: Schema.String }) {}

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

const layerHandlersV2 = PutTodoV2.toLayer(({ payload, transaction }) =>
  transaction.set(TodoV2, payload.id, payload).pipe(Effect.as(payload))
)
const layerRejectingHandlersV2 = PutTodoV2.toLayer(() =>
  Effect.fail(new SchemaPolicyRejectedError({ reason: "schema-policy-rejected" }))
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

const layerDatabase = Layer.mergeAll(
  ConnectionLane.makeLayer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)
const provideDatabase = Effect.provide(layerDatabase)

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
  serverOptions?: Partial<
    Pick<ServerStore.Options, "acceptedSchemaVersions" | "retainedHistoryEntries" | "retainedReceipts">
  >
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

const operations = ["discard", "resubmit"] as const

const rows = constructors.flatMap((constructor) => operations.map((operation) => ({ constructor, operation })))

type Row = typeof rows[number]

const outcomeOf = <A, E extends { readonly _tag: string },>(
  finished: Option.Option<Exit.Exit<Result.Result<A, E>>>
) => {
  if (Option.isNone(finished)) return "never completed"
  if (Exit.isFailure(finished.value)) return "died"
  if (Result.isFailure(finished.value.value)) return finished.value.value.failure._tag
  return "succeeded"
}

const waitingOnTheServer = Effect.fnUntraced(function*(row: Row) {
  const v1 = yield* buildStore(definitionV1, layerHandlersV1)
  const original = yield* v1.mutate(PutTodoV1, { id: "61", title: "original" })
  yield* buildStore(definitionV2, layerRejectingHandlersV2, evolution)
  const server = yield* buildServer(definitionV2, layerHandlersV2, evolution, { acceptedSchemaVersions: 0 })
  const entered = yield* Deferred.make<void>()
  const answered = yield* Deferred.make<void>()
  let credentialGeneration = 0
  let discards = 0
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    credentialGeneration: Effect.sync(() => credentialGeneration),
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => server.admitBatch(request, null),
    discard: (request) =>
      Effect.suspend(() => {
        discards += 1
        return Deferred.succeed(entered, undefined)
      }).pipe(
        Effect.andThen(Deferred.await(answered)),
        Effect.andThen(server.discard(request, null))
      ),
    pull: server.pull,
    bootstrap: server.bootstrap,
    watch: server.watch
  })
  const options: SqlReplica.Options<typeof definitionV2> = {
    ...clientHistory,
    definition: definitionV2,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 3,
    foregroundActiveSpaces: 1,
    schemaEvolutionBatchSize: 1,
    evolution
  }
  const layerServices = Layer.merge(layerHandlersV2, Layer.succeed(SyncEngine.SyncEngine, remote))
  let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
  if (row.constructor === "layerWorkflow") {
    layerReplica = SqlReplica.layerWorkflow(options).pipe(
      Layer.provide(layerServices),
      Layer.provide(WorkflowEngine.layerMemory)
    )
  }
  const replicaScope = yield* Scope.make()
  const replica = Context.get(yield* Layer.buildWithScope(layerReplica, replicaScope), Replica.Replica)
  const space = yield* replica.space(spaceId)
  const other = yield* replica.space(otherSpaceId)
  const mutationId = original.envelope.mutationId
  let operation: Effect.Effect<void, { readonly _tag: string }> = Effect.asVoid(space.discardQuarantined(mutationId))
  if (row.operation === "resubmit") {
    const resubmitted = { id: 61, title: "again", done: false }
    operation = Effect.asVoid(space.resubmitQuarantined(mutationId, PutTodoV2, resubmitted))
  }
  const waiting = yield* operation.pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
  yield* VirtualTime.advanceUntil(Deferred.await(entered))
  return {
    replica,
    replicaScope,
    space,
    other,
    finished: Fiber.join(waiting).pipe(within),
    serverAnswers: Deferred.succeed(answered, undefined),
    replaceCredential: Effect.sync(() => {
      credentialGeneration += 1
    }),
    discards: () => discards
  }
})

const harness = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  VirtualTime.scoped(effect).pipe(provideDatabase)

describe("a quarantine operation whose credential is replaced while the server call is in flight", () => {
  it.effect.each(rows)(
    "sends the call again under the new credential instead of applying the answer ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { discards, finished, replaceCredential, serverAnswers } = yield* waitingOnTheServer(row)

      yield* replaceCredential
      yield* serverAnswers
      const outcome = outcomeOf(yield* finished)

      assert.strictEqual(outcome, "succeeded")
      assert.strictEqual(discards(), 2, "server calls for the one quarantined mutation")
    }, harness)
  )
})

describe("a quarantine operation that waits on a server that does not answer", () => {
  it.effect.each(rows)(
    "does not stop a local read or write on its own space ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { space } = yield* waitingOnTheServer(row)

      const read = yield* within(space.pending)
      const written = yield* space.mutate(PutTodoV2, { id: 7, title: "offline", done: false }).pipe(within)

      assert.strictEqual(describeExit(read), "succeeded")
      assert.strictEqual(describeExit(written), "succeeded")
    }, harness)
  )

  it.effect.each(rows)(
    "gives the only foreground place to another space and takes it back ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { other, space } = yield* waitingOnTheServer(row)

      const elsewhere = yield* within(other.pending)
      const back = yield* within(space.pending)

      assert.strictEqual(describeExit(elsewhere), "succeeded")
      assert.strictEqual(describeExit(back), "succeeded")
    }, harness)
  )

  it.effect.each(rows)(
    "does not hold back a deactivation of its space ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { space } = yield* waitingOnTheServer(row)

      const deactivated = yield* within(space.deactivate)
      const read = yield* within(space.pending)

      assert.strictEqual(describeExit(deactivated), "succeeded")
      assert.strictEqual(describeExit(read), "succeeded")
    }, harness)
  )

  it.effect.each(rows)(
    "finishes once the server answers although its space was deactivated meanwhile ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { finished, serverAnswers, space } = yield* waitingOnTheServer(row)
      yield* VirtualTime.advanceUntil(space.deactivate)

      yield* serverAnswers
      const outcome = outcomeOf(yield* finished)
      const quarantined = yield* VirtualTime.advanceUntil(space.quarantine)

      assert.strictEqual(outcome, "succeeded")
      assert.strictEqual(quarantined.length, 0)
    }, harness)
  )

  it.effect.each(rows)(
    "lets its space be left and then fails with SpaceUnavailable ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { finished, replica, serverAnswers } = yield* waitingOnTheServer(row)

      const left = yield* within(replica.leave(spaceId))
      yield* serverAnswers
      const outcome = outcomeOf(yield* finished)

      assert.strictEqual(describeExit(left), "succeeded")
      assert.strictEqual(outcome, "SpaceUnavailable")
    }, harness)
  )

  it.effect.each(rows)(
    "lets the replica scope close ($operation, $constructor)",
    Effect.fnUntraced(function*(row) {
      const { replicaScope } = yield* waitingOnTheServer(row)

      const closed = yield* within(Scope.close(replicaScope, Exit.void))

      assert.strictEqual(describeExit(closed), "succeeded")
    }, harness)
  )
})
