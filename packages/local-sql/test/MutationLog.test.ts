import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Rows from "../src/internal/rows.js"
import * as LocalStore from "../src/LocalStore.js"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const defaultMembershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const putTodoProvenance = {
  name: Domain.PutTodo.name,
  sourceSchema: Domain.definition.schemaIdentity,
  mutationVersion: Domain.PutTodo.version
}

class TestAuthorizationError extends Schema.TaggedErrorClass<TestAuthorizationError, Schema.JsonObject>(
  "@lucas-barake/effect-local-sql/test/TestAuthorizationError"
)("TestAuthorizationError", { reason: Schema.String }) {
}

const expectedFailure = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function*() {
    const exit = yield* effect.pipe(Effect.exit)
    if (Exit.isSuccess(exit)) assert.fail("expected failure")
    const failure = Cause.findErrorOption(exit.cause)
    if (Option.isNone(failure)) return yield* Effect.failCause(exit.cause)
    return failure.value
  })

const envelope = (
  name: string,
  payload: Schema.Json,
  localSequence: number,
  mutationId: Identity.MutationId,
  membershipIncarnation: Identity.MembershipIncarnation = defaultMembershipIncarnation
) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId,
      mutationId,
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name,
      payload,
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.definition.mutationByName.get(name)?.version ?? Identity.SchemaVersion.make(1)
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const database = () =>
  pipe(
    {
      argument0_0: SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      argument0_1: NodeCrypto.layer,
      argument0_2: Reactivity.layer,
      argument0_3: QueryReactivity.layer
    },
    ({ argument0_0, argument0_1, argument0_2, argument0_3 }) =>
      Layer.mergeAll(argument0_0, argument0_1, argument0_2, argument0_3)
  )

const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  scope,
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
  readAuthorizationRefreshInterval: "1 second" as const,
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
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

const localLayer = (id = clientId) =>
  LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId: id }).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )

const serverLayer = (
  authorizeMutation?: ServerStore.Options["authorizeMutation"]
) => {
  let layer = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition })
  if (authorizeMutation !== undefined) {
    layer = ServerStore.layer({
      definition: Domain.definition,
      ...serverHistory,
      authorizeAccess: () => Effect.void,
      authorizeMutation,
      authorizeRead: () => Effect.void
    })
  }
  return layer.pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )
}

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const directSync = (server: ServerStore.Service) =>
  pipe({
    argument3_0: SyncEngine.SyncEngine,
    argument3_1: SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      submit: server.submit,
      discard: (request) => server.discard(request, null),
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })
  }, ({ argument3_0, argument3_1 }) => Layer.succeed(argument3_0, argument3_1))

const incremental = (result: Protocol.PullResult): Protocol.PullPage => {
  if ("_tag" in result) assert.fail(`Unexpected bootstrap ${result.manifest.snapshotId}`)
  return result
}

const pullRequest = (
  cursor: Protocol.ReplicationCursor | null = null,
  limit = 10,
  requestedClientId = clientId,
  requestedScope = scope
): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: requestedClientId,
    schema: Domain.definition.schemaIdentity,
    scope: requestedScope,
    scopeGeneration,
    cursor,
    limit
  })

const bootstrapRequest = (
  manifest: Protocol.SnapshotManifest,
  afterOrdinal = -1,
  limit = 10,
  requestedScope = scope
): Protocol.BootstrapRequest =>
  Protocol.BootstrapRequest.make({
    spaceId,
    clientId: manifest.clientId,
    schema: Domain.definition.schemaIdentity,
    scope: requestedScope,
    scopeGeneration: manifest.scopeGeneration,
    cursor: manifest.cursor,
    snapshotId: manifest.snapshotId,
    afterOrdinal,
    limit
  })

const watchRequest = (requestedClientId = clientId): Protocol.WatchRequest =>
  Protocol.WatchRequest.make({
    spaceId,
    clientId: requestedClientId,
    schema: Domain.definition.schemaIdentity,
    scope,
    scopeGeneration,
    cursor: null
  })

const installFreshView = (
  local: LocalStore.Service,
  server: ServerStore.Service,
  requestedClientId = clientId,
  requestedScope = scope
) =>
  Effect.gen(function*() {
    const required = yield* pipe(pullRequest(null, 10, requestedClientId, requestedScope), (argument4_0) =>
      server.pull(argument4_0))
    if (!("_tag" in required)) {
      assert.fail("expected bootstrap")
    }
    const page = yield* pipe(bootstrapRequest(required.manifest, -1, 10, requestedScope), (argument5_0) =>
      server.bootstrap(argument5_0))
    yield* local.prepareBootstrap(page.manifest)
    assert.isTrue(yield* local.stageBootstrapPage(page))
    yield* local.installBootstrap(page.manifest)
  })

const authoritativeRows = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Identity.SpaceId,
    Result: Rows.ServerLogRow,
    execute: (requestedSpaceId) =>
      sql`SELECT space_id, server_sequence, client_id, membership_incarnation, local_sequence, mutation_id, digest,
        entry_bytes, entry_json, source_schema_version, source_schema_hash
        FROM effect_local_authoritative_log WHERE space_id = ${requestedSpaceId}
        ORDER BY server_sequence`
  })(spaceId)

const authoritativeLog = (sql: SqlClient.SqlClient) =>
  authoritativeRows(sql).pipe(
    Effect.flatMap(
      Effect.forEach((row) =>
        Schema.decodeUnknownEffect(Schema.fromJsonString(Protocol.AcceptedMutation))(row.entry_json)
      )
    )
  )

const acceptedMutation = (
  pending: Protocol.PendingMutation,
  receipt: Protocol.AcceptedReceipt
): Protocol.AcceptedMutation =>
  Protocol.AcceptedMutation.make({
    sequence: receipt.serverSequence,
    spaceId,
    clientId,
    mutationId: pending.envelope.mutationId,
    localSequence: pending.envelope.localSequence,
    membershipIncarnation: pending.envelope.membershipIncarnation,
    sourceSchema: pending.envelope.sourceSchema,
    digest: pending.envelope.digest,
    changes: pending.changes
  })

const clientServices = (id: Identity.ClientId, server: ServerStore.Service) => {
  const local = localLayer(id)
  const reconciler = Reconciler.layer({ definition: Domain.definition, spaceId, retryDelay: "10 millis" }).pipe(
    Layer.provide(local),
    Layer.provide(directSync(server))
  )
  return Layer.merge(local, reconciler)
}

describe("server reconciled mutation log", () => {
  it.effect("does not scale SQL writes or transactions with watcher fanout", () =>
    pipe(
      Effect.gen(function*() {
        const actualSql = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
          Effect.provide(Reactivity.layer)
        )
        const transactionCalls = yield* Ref.make(0)
        const observedSql = new Proxy(actualSql, {
          get: (target, property, receiver) => {
            if (property !== "withTransaction") return Reflect.get(target, property, receiver)
            return <R, E extends { readonly _tag: string }, A,>(effect: Effect.Effect<A, E, R>) =>
              Ref.update(transactionCalls, (count) => count + 1).pipe(
                Effect.andThen(target.withTransaction(effect))
              )
          }
        })
        const infrastructure = pipe({
          argument10_0: Layer.succeed(SqlClient.SqlClient, observedSql),
          argument10_1: NodeCrypto.layer,
          argument10_2: Reactivity.layer
        }, ({ argument10_0, argument10_1, argument10_2 }) => Layer.mergeAll(argument10_0, argument10_1, argument10_2))
        const server = yield* pipe({
          argument11_0: ServerStore.ServerStore,
          argument11_1: ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(infrastructure)
          )
        }, ({ argument11_0, argument11_1 }) => service(argument11_0, argument11_1))
        yield* observedSql`CREATE TABLE space_update_probe (count INTEGER NOT NULL)`
        yield* observedSql`CREATE TRIGGER count_space_updates AFTER UPDATE ON effect_local_server_spaces
          BEGIN INSERT INTO space_update_probe (count) VALUES (1); END`
        const countUpdates = SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ count: Schema.Number }),
          execute: () => observedSql`SELECT COUNT(*) AS count FROM space_update_probe`
        })
        const submit = (localSequence: number) =>
          pipe(
            {
              argument12_0: Domain.PutTodo.name,
              argument12_1: Domain.todo(`fanout-${localSequence}`),
              argument12_2: localSequence,
              argument12_3: Identity.MutationId.make(
                `mut_00000000-0000-4000-8020-${String(localSequence).padStart(12, "0")}`
              )
            },
            ({ argument12_0, argument12_1, argument12_2, argument12_3 }) =>
              envelope(argument12_0, argument12_1, argument12_2, argument12_3)
          ).pipe(Effect.flatMap(server.submit))

        assert.strictEqual((yield* submit(1))._tag, "Accepted")
        yield* Ref.set(transactionCalls, 0)
        yield* observedSql`DELETE FROM space_update_probe`
        assert.strictEqual((yield* submit(2))._tag, "Accepted")
        const baselineTransactions = yield* Ref.get(transactionCalls)
        const baselineUpdates = (yield* countUpdates(undefined)).count

        const watcherQueues = yield* pipe({
          argument13_0: Array.from({ length: 4 }),
          argument13_1: () => Queue.unbounded<Protocol.Wake>()
        }, ({ argument13_0, argument13_1 }) => Effect.forEach(argument13_0, argument13_1))
        const watchers = yield* Effect.forEach(watcherQueues, (queue) =>
          pipe(watchRequest(), (argument14_0) =>
            server.watch(argument14_0)).pipe(
              Stream.runForEach((wake) => Queue.offer(queue, wake)),
              Effect.forkChild({ startImmediately: true })
            ))
        yield* Effect.forEach(watcherQueues, Queue.take)
        yield* Ref.set(transactionCalls, 0)
        yield* observedSql`DELETE FROM space_update_probe`

        assert.strictEqual((yield* submit(3))._tag, "Accepted")
        const wakes = yield* Effect.forEach(watcherQueues, Queue.take)
        pipe(
          { argument15_0: wakes, argument15_1: Array.from({ length: 4 }, () => ({ spaceId })) },
          ({ argument15_0, argument15_1 }) =>
            assert.deepStrictEqual(argument15_0, argument15_1)
        )
        assert.strictEqual(yield* Ref.get(transactionCalls), baselineTransactions)
        assert.strictEqual((yield* countUpdates(undefined)).count, baselineUpdates)
        yield* Effect.forEach(watchers, Fiber.interrupt)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument8_0) => Effect.scoped(argument8_0)
    ))

  it.effect("caps active sync watchers and releases slots on interruption", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument17_0: ServerStore.ServerStore,
          argument17_1: ServerStore.layerTrusted({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 1
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument17_0, argument17_1 }) => service(argument17_0, argument17_1))
        const ready = yield* Deferred.make<void>()
        const first = yield* pipe(watchRequest(), (argument19_0) => server.watch(argument19_0)).pipe(
          Stream.runForEach(() => Deferred.succeed(ready, undefined)),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(ready)

        const error = yield* pipe(watchRequest(), (argument20_0) => server.watch(argument20_0)).pipe(
          Stream.runHead,
          Effect.flip
        )
        assert.deepStrictEqual(
          error,
          new ReplicaError.CapacityExceeded({ resource: "sync watchers", limit: 1 })
        )
        const rejection = (yield* Metric.snapshot).find((snapshot) =>
          snapshot.id === "effect_local_server_rejection" && snapshot.attributes?.class === "CapacityExceeded"
        )
        assert.strictEqual(rejection?.type, "Counter")
        if (rejection?.type === "Counter") assert.strictEqual(rejection.state.count, 1)

        yield* Fiber.interrupt(first)
        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument22_0) => server.watch(argument22_0)).pipe(Stream.runHead)),
          (argument21_0) => assert.isTrue(argument21_0)
        )
      }).pipe(
        Effect.provide(NodeCrypto.layer),
        Effect.provideService(Metric.MetricRegistry, new Map())
      ),
      (argument16_0) => Effect.scoped(argument16_0)
    ))

  it.effect("keeps watcher admission available when its metric update defects", () => {
    const registry = new Map<string, Metric.Metric.Metadata<any, any>>()
    return pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument24_0: ServerStore.ServerStore,
          argument24_1: ServerStore.layerTrusted({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 2
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument24_0, argument24_1 }) => service(argument24_0, argument24_1))
        const firstReady = yield* Deferred.make<void>()
        const first = yield* pipe(watchRequest(), (argument26_0) => server.watch(argument26_0)).pipe(
          Stream.tap(() => Deferred.succeed(firstReady, undefined)),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(firstReady)

        const metadata = pipe(registry.values(), (argument27_0) => Array.from(argument27_0)).find(
          (entry) => entry.id === "effect_local_server_sync_watcher_count"
        )
        assert.isDefined(metadata)
        const modify = metadata.hooks.modify.bind(metadata.hooks)
        let defectNextIncrement = true
        Object.defineProperty(metadata.hooks, "modify", {
          configurable: true,
          value: (input: number, context: never) => {
            if (input === 1 && defectNextIncrement) {
              defectNextIncrement = false
              assert.fail("metric registry defect")
            }
            return modify(input, context)
          }
        })

        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument29_0) => server.watch(argument29_0)).pipe(Stream.runHead)),
          (argument28_0) => assert.isTrue(argument28_0)
        )
        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument31_0) => server.watch(argument31_0)).pipe(Stream.runHead)),
          (argument30_0) => assert.isTrue(argument30_0)
        )
        yield* Fiber.interrupt(first)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument23_0) => Effect.scoped(argument23_0)
    ).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("returns accepted and retry receipts when admission metrics defect after recording", () => {
    const registry = new Map<string, Metric.Metric.Metadata<any, any>>()
    return pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument33_0: ServerStore.ServerStore,
          argument33_1: ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument33_0, argument33_1 }) => service(argument33_0, argument33_1))
        yield* pipe({
          argument35_0: pipe({
            argument36_0: Metric.counter("effect_local_server_admission", { incremental: true }),
            argument36_1: { outcome: "accepted" }
          }, ({ argument36_0, argument36_1 }) => Metric.withAttributes(argument36_0, argument36_1)),
          argument35_1: 0
        }, ({ argument35_0, argument35_1 }) => Metric.update(argument35_0, argument35_1))
        const metadata = pipe(registry.values(), (argument37_0) => Array.from(argument37_0)).find((entry) =>
          entry.id === "effect_local_server_admission" && entry.attributes?.outcome === "accepted"
        )
        assert.isDefined(metadata)
        const update = metadata.hooks.update.bind(metadata.hooks)
        Object.defineProperty(metadata.hooks, "update", {
          configurable: true,
          value: (input: number, context: never) => {
            update(input, context)
            assert.fail("admission metric defect")
          }
        })
        const submitted = yield* pipe(
          {
            argument38_0: Domain.PutTodo.name,
            argument38_1: Domain.todo("metric-defect-receipt"),
            argument38_2: 1,
            argument38_3: Identity.MutationId.make("mut_00000000-0000-4000-8021-000000000001")
          },
          ({ argument38_0, argument38_1, argument38_2, argument38_3 }) =>
            envelope(argument38_0, argument38_1, argument38_2, argument38_3)
        )

        const accepted = yield* server.submit(submitted)
        const retry = yield* server.submit(submitted)

        assert.strictEqual(accepted._tag, "Accepted")
        assert.deepStrictEqual(retry, accepted)
        const admissions = yield* Metric.snapshot
        const acceptedMetric = admissions.find((snapshot) =>
          snapshot.id === "effect_local_server_admission" && snapshot.attributes?.outcome === "accepted"
        )
        const failedMetric = admissions.find((snapshot) =>
          snapshot.id === "effect_local_server_admission" && snapshot.attributes?.outcome === "failed"
        )
        assert.strictEqual(acceptedMetric?.type, "Counter")
        if (acceptedMetric?.type === "Counter") {
          assert.strictEqual(acceptedMetric.state.count, 2)
        }
        assert.isUndefined(failedMetric)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument32_0) => Effect.scoped(argument32_0)
    ).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("preserves a refresh defect and releases the sync watcher slot", () =>
    pipe(
      Effect.gen(function*() {
        let lookups = 0
        const server = yield* pipe({
          argument40_0: ServerStore.ServerStore,
          argument40_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 1,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () => {
              lookups++
              if (lookups === 1) return Effect.void
              return Effect.die("refresh defect")
            }
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument40_0, argument40_1 }) => service(argument40_0, argument40_1))
        const initial = yield* Deferred.make<void>()
        const watching = yield* pipe(
          { argument42_0: watchRequest(), argument42_1: "reader" },
          ({ argument42_0, argument42_1 }) => server.watchAuthorized(argument42_0, argument42_1)
        ).pipe(
          Effect.flatMap((stream) =>
            stream.pipe(
              Stream.tap(() => Deferred.succeed(initial, undefined)),
              Stream.runDrain
            )
          ),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(initial)
        yield* TestClock.adjust("500 millis")

        const exit = yield* Fiber.await(watching)
        pipe(Exit.isFailure(exit), (argument43_0) => assert.isTrue(argument43_0))
        if (Exit.isFailure(exit)) {
          pipe({
            argument44_0: exit.cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect),
            argument44_1: ["refresh defect"]
          }, ({ argument44_0, argument44_1 }) => assert.deepStrictEqual(argument44_0, argument44_1))
        }
        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument46_0) => server.watch(argument46_0)).pipe(Stream.runHead)),
          (argument45_0) => assert.isTrue(argument45_0)
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument39_0) => Effect.scoped(argument39_0)
    ))

  it.effect("preserves a refresh policy denial and releases the sync watcher slot", () =>
    pipe(
      Effect.gen(function*() {
        let lookups = 0
        const denial = new TestAuthorizationError({ reason: "read revoked" })
        const server = yield* pipe({
          argument48_0: ServerStore.ServerStore,
          argument48_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 1,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () => {
              lookups++
              if (lookups === 1) return Effect.void
              return Effect.fail(denial)
            }
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument48_0, argument48_1 }) => service(argument48_0, argument48_1))
        const initial = yield* Deferred.make<void>()
        const watching = yield* pipe(
          { argument50_0: watchRequest(), argument50_1: "reader" },
          ({ argument50_0, argument50_1 }) => server.watchAuthorized(argument50_0, argument50_1)
        ).pipe(
          Effect.flatMap((stream) =>
            stream.pipe(
              Stream.tap(() => Deferred.succeed(initial, undefined)),
              Stream.runDrain
            )
          ),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(initial)
        yield* TestClock.adjust("500 millis")

        const error = yield* Fiber.join(watching).pipe(expectedFailure)
        const denialJson: Pick<TestAuthorizationError, "_tag" | "reason"> = denial
        assert.deepStrictEqual(error, new ReplicaError.AuthorizationDenied({ reason: { ...denialJson } }))
        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument52_0) => server.watch(argument52_0)).pipe(Stream.runHead)),
          (argument51_0) => assert.isTrue(argument51_0)
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument47_0) => Effect.scoped(argument47_0)
    ))

  it.effect("preserves refresh interruption and releases the sync watcher slot", () =>
    pipe(
      Effect.gen(function*() {
        let lookups = 0
        const server = yield* pipe({
          argument54_0: ServerStore.ServerStore,
          argument54_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 1,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () => {
              lookups++
              if (lookups === 1) return Effect.void
              return Effect.interrupt
            }
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument54_0, argument54_1 }) => service(argument54_0, argument54_1))
        const initial = yield* Deferred.make<void>()
        const watching = yield* pipe(
          { argument56_0: watchRequest(), argument56_1: "reader" },
          ({ argument56_0, argument56_1 }) => server.watchAuthorized(argument56_0, argument56_1)
        ).pipe(
          Effect.flatMap((stream) =>
            stream.pipe(
              Stream.tap(() => Deferred.succeed(initial, undefined)),
              Stream.runDrain
            )
          ),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(initial)
        yield* TestClock.adjust("500 millis")

        const exit = yield* Fiber.await(watching)
        pipe(Exit.isFailure(exit), (argument57_0) => assert.isTrue(argument57_0))
        if (Exit.isFailure(exit)) pipe(Cause.hasInterrupts(exit.cause), (argument58_0) => assert.isTrue(argument58_0))
        pipe(
          Option.isSome(yield* pipe(watchRequest(), (argument60_0) => server.watch(argument60_0)).pipe(Stream.runHead)),
          (argument59_0) => assert.isTrue(argument59_0)
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument53_0) => Effect.scoped(argument53_0)
    ))

  it.effect("rejects excess pending read authorizations with typed capacity", () =>
    pipe(
      Effect.gen(function*() {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const server = yield* pipe({
          argument62_0: ServerStore.ServerStore,
          argument62_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            maximumConcurrentReadAuthorizations: 1,
            maximumPendingReadAuthorizations: 1,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release))
              )
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument62_0, argument62_1 }) => service(argument62_0, argument62_1))
        const pending = yield* pipe(
          { argument65_0: watchRequest(), argument65_1: "first" },
          ({ argument65_0, argument65_1 }) => server.watchAuthorized(argument65_0, argument65_1)
        ).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(entered)

        const error = yield* pipe(
          { argument66_0: watchRequest(), argument66_1: "second" },
          ({ argument66_0, argument66_1 }) => server.watchAuthorized(argument66_0, argument66_1)
        ).pipe(expectedFailure)
        assert.deepStrictEqual(
          error,
          new ReplicaError.CapacityExceeded({ resource: "read authorizations", limit: 1 })
        )
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(pending)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument61_0) => Effect.scoped(argument61_0)
    ))

  it.effect("does not reuse authorization after a caller mutates its principal", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument68_0: ServerStore.ServerStore,
          argument68_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: ({ principal }) => {
              if (
                typeof principal === "object" && principal !== null && !Array.isArray(principal) &&
                "subject" in principal && principal.subject === "allowed"
              ) return Effect.void
              return Effect.fail(new TestAuthorizationError({ reason: "denied" }))
            }
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument68_0, argument68_1 }) => service(argument68_0, argument68_1))
        const principal: { subject: string } = { subject: "allowed" }
        const first = yield* pipe(
          { argument70_0: watchRequest(), argument70_1: principal },
          ({ argument70_0, argument70_1 }) => server.watchAuthorized(argument70_0, argument70_1)
        )
        pipe(Option.isSome(yield* first.pipe(Stream.runHead)), (argument71_0) => assert.isTrue(argument71_0))

        principal.subject = "denied"
        const denied = yield* pipe(
          { argument72_0: watchRequest(), argument72_1: principal },
          ({ argument72_0, argument72_1 }) => server.watchAuthorized(argument72_0, argument72_1)
        ).pipe(expectedFailure)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument67_0) => Effect.scoped(argument67_0)
    ))

  it.effect("does not reveal sync watcher occupancy to unauthorized principals", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument74_0: ServerStore.ServerStore,
          argument74_1: ServerStore.layer({
            ...serverHistory,
            definition: Domain.definition,
            maximumWatchersPerSpace: 1,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: ({ principal }) => {
              if (principal === "allowed") return Effect.void
              return Effect.fail(new TestAuthorizationError({ reason: "denied" }))
            }
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument74_0, argument74_1 }) => service(argument74_0, argument74_1))
        const ready = yield* Deferred.make<void>()
        const firstStream = yield* pipe(
          { argument76_0: watchRequest(), argument76_1: "allowed" },
          ({ argument76_0, argument76_1 }) => server.watchAuthorized(argument76_0, argument76_1)
        )
        const first = yield* firstStream.pipe(
          Stream.runForEach(() => Deferred.succeed(ready, undefined)),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(ready)

        const denied = yield* pipe(
          { argument77_0: watchRequest(), argument77_1: "denied" },
          ({ argument77_0, argument77_1 }) => server.watchAuthorized(argument77_0, argument77_1)
        ).pipe(expectedFailure)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
        yield* Fiber.interrupt(first)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument73_0) => Effect.scoped(argument73_0)
    ))

  it.effect("records server capacity metrics in the active registry", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument79_0: ServerStore.ServerStore,
          argument79_1: ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument79_0, argument79_1 }) => service(argument79_0, argument79_1))
        const wakes = yield* Queue.unbounded<Protocol.Wake>()
        const watcher = yield* pipe(watchRequest(), (argument81_0) => server.watch(argument81_0)).pipe(
          Stream.runForEach((wake) => Queue.offer(wakes, wake)),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(wakes)
        assert.strictEqual(
          (yield* server.submit(
            yield* pipe(
              {
                argument82_0: Domain.PutTodo.name,
                argument82_1: Domain.todo("metric"),
                argument82_2: 1,
                argument82_3: Identity.MutationId.make("mut_00000000-0000-4000-8030-000000000001")
              },
              ({ argument82_0, argument82_1, argument82_2, argument82_3 }) =>
                envelope(argument82_0, argument82_1, argument82_2, argument82_3)
            )
          ))._tag,
          "Accepted"
        )
        yield* Queue.take(wakes)
        yield* server.maintain(spaceId)
        yield* TestClock.adjust("20 millis")

        const snapshots = yield* Metric.snapshot
        const find = (id: string, attributes?: Readonly<Record<string, string>>) =>
          snapshots.find((snapshot) =>
            snapshot.id === id &&
            (attributes === undefined ||
              Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value))
          )
        const watcherCount = find("effect_local_server_sync_watcher_count")
        assert.strictEqual(watcherCount?.type, "Gauge")
        if (watcherCount?.type === "Gauge") assert.strictEqual(watcherCount.state.value, 1)
        const fanout = find("effect_local_server_wake_fanout_duration")
        assert.strictEqual(fanout?.type, "Histogram")
        if (fanout?.type === "Histogram") assert.strictEqual(fanout.state.count, 1)
        const admission = find("effect_local_server_admission", { outcome: "accepted" })
        assert.strictEqual(admission?.type, "Counter")
        if (admission?.type === "Counter") assert.strictEqual(admission.state.count, 1)
        const maintenance = find("effect_local_server_maintenance", { outcome: "completed" })
        assert.strictEqual(maintenance?.type, "Counter")
        if (maintenance?.type === "Counter") assert.strictEqual(maintenance.state.count, 1)
        const historyDepth = find("effect_local_server_history_depth")
        assert.strictEqual(historyDepth?.type, "Gauge")
        if (historyDepth?.type === "Gauge") assert.strictEqual(historyDepth.state.value, 1)
        const receiptDepth = find("effect_local_server_receipt_depth")
        assert.strictEqual(receiptDepth?.type, "Gauge")
        if (receiptDepth?.type === "Gauge") assert.strictEqual(receiptDepth.state.value, 1)
        const historyLimit = find("effect_local_server_history_limit")
        assert.strictEqual(historyLimit?.type, "Gauge")
        if (historyLimit?.type === "Gauge") {
          assert.strictEqual(historyLimit.state.value, serverHistory.maximumHistoryEntries)
        }
        const receiptLimit = find("effect_local_server_receipt_limit")
        assert.strictEqual(receiptLimit?.type, "Gauge")
        if (receiptLimit?.type === "Gauge") {
          assert.strictEqual(receiptLimit.state.value, serverHistory.maximumReceipts)
        }
        yield* Fiber.interrupt(watcher)
        const afterRelease = (yield* Metric.snapshot).find((snapshot) =>
          snapshot.id === "effect_local_server_sync_watcher_count"
        )
        assert.strictEqual(afterRelease?.type, "Gauge")
        if (afterRelease?.type === "Gauge") assert.strictEqual(afterRelease.state.value, 0)
      }).pipe(
        Effect.provide(NodeCrypto.layer),
        Effect.provideService(Metric.MetricRegistry, new Map())
      ),
      (argument78_0) => Effect.scoped(argument78_0)
    ))

  it.effect("classifies malformed visible rows during index maintenance as storage corruption", () =>
    pipe(
      Effect.gen(function*() {
        const clientDatabase = database()
        const live = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(live, clientDatabase), (argument84_0) => Layer.build(argument84_0))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* pipe({
          argument85_0: Domain.PutTodo,
          argument85_1: Domain.todo("corrupt-index-refresh")
        }, ({ argument85_0, argument85_1 }) => local.mutate(argument85_0, argument85_1))
        const meta = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({
            active_schema_generation: Schema.Int,
            active_projection_generation: Schema.Int
          }),
          execute: () =>
            sql`SELECT active_schema_generation, active_projection_generation
          FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
        })(undefined)

        yield* sql`INSERT INTO effect_local_client_canonical_entities_data
        (space_id, schema_generation, model, model_version, entity_key, value_json)
        VALUES (${spaceId}, ${meta.active_schema_generation}, ${Domain.Todo.name}, ${Domain.Todo.version},
          ${Canonical.stringify("corrupt-index-refresh")}, x'00')`
        const malformed = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ value_json: Schema.Unknown }),
          execute: () =>
            sql`SELECT value_json FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = ${meta.active_schema_generation}
          AND model = ${Domain.Todo.name}
          AND entity_key = ${Canonical.stringify("corrupt-index-refresh")}`
        })(undefined)
        assert.instanceOf(malformed.value_json, Uint8Array)
        yield* pipe(
          Protocol.RejectedReceipt.make({
            spaceId,
            clientId,
            mutationId: pending.envelope.mutationId,
            localSequence: pending.envelope.localSequence,
            membershipIncarnation: pending.envelope.membershipIncarnation,
            ...putTodoProvenance,
            origin: "Legacy",
            terminalSequence: Identity.TerminalSequence.make(1),
            rejection: "denied"
          }),
          (argument86_0) => local.persistReceipt(argument86_0)
        )

        const error = yield* local.settleReceipts.pipe(expectedFailure)
        assert.strictEqual(error._tag, "StorageCorrupt")
        assert.strictEqual(yield* local.pendingCount, 1)

        yield* sql`DELETE FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${meta.active_schema_generation}
          AND model = ${Domain.Todo.name} AND entity_key = ${Canonical.stringify("corrupt-index-refresh")}`
        const pull = yield* Stream.toPull(local.settlements)
        yield* local.settleReceipts
        pipe({
          argument87_0: (yield* pull).map((settlement) => settlement.pending.envelope.mutationId),
          argument87_1: [pending.envelope.mutationId]
        }, ({ argument87_0, argument87_1 }) => assert.deepStrictEqual(argument87_0, argument87_1))
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument83_0) => Effect.scoped(argument83_0)
    ))

  it.effect("finishes a committed settlement batch after its caller is interrupted", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe({
          argument89_0: LocalStore.Store,
          argument89_1: LocalStore.layer({
            ...clientHistory,
            settlementCapacity: 1,
            definition: Domain.definition,
            spaceId,
            clientId
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument89_0, argument89_1 }) => service(argument89_0, argument89_1))
        const pending = yield* Effect.forEach(
          ["settlement-a", "settlement-b", "settlement-c", "settlement-d"],
          (id) =>
            pipe(
              { argument91_0: Domain.PutTodo, argument91_1: Domain.todo(id) },
              ({ argument91_0, argument91_1 }) => local.mutate(argument91_0, argument91_1)
            )
        )
        const pull = yield* Stream.toPull(local.settlements)
        const admitted = yield* Deferred.make<void>()
        const settling = yield* pipe(
          pending.map((item) =>
            Protocol.RejectedReceipt.make({
              spaceId,
              clientId,
              mutationId: item.envelope.mutationId,
              localSequence: item.envelope.localSequence,
              membershipIncarnation: item.envelope.membershipIncarnation,
              ...putTodoProvenance,
              origin: "Legacy",
              terminalSequence: Identity.TerminalSequence.make(item.envelope.localSequence),
              rejection: "denied"
            })
          ),
          (argument92_0) => local.applyReceipts(argument92_0)
        ).pipe(
          Effect.tap(() => Deferred.succeed(admitted, undefined)),
          Effect.forkChild({ startImmediately: true })
        )

        assert.isFalse(yield* Deferred.isDone(admitted))
        const first = yield* pull
        const interruption = yield* Fiber.interrupt(settling).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        const remainder = yield* Effect.gen(function*() {
          const values: Array<Replica.MutationSettlement> = []
          while (values.length < 3) values.push(...(yield* pull))
          return values
        }).pipe(Effect.forkChild({ startImmediately: true }))
        const result = yield* Fiber.join(remainder)
        yield* Fiber.join(interruption)

        pipe({
          argument93_0: [...first, ...result].map((settlement) => settlement.pending.envelope.mutationId),
          argument93_1: pending.map((item) => item.envelope.mutationId)
        }, ({ argument93_0, argument93_1 }) => assert.deepStrictEqual(argument93_0, argument93_1))
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument88_0) => Effect.scoped(argument88_0)
    ))

  it.effect("publishes a settlement when interrupted after durable deletion", () =>
    pipe(
      Effect.gen(function*() {
        const invalidateStarted = yield* Deferred.make<void>()
        const releaseInvalidation = yield* Deferred.make<void>()
        const blockInvalidation = yield* Ref.make(false)
        const blockedQueryReactivity = QueryReactivity.QueryReactivity.of({
          retain: () => Effect.succeed(Effect.void),
          record: () => Effect.void,
          affected: () =>
            Effect.gen(function*() {
              if (!(yield* Ref.get(blockInvalidation))) return []
              yield* Deferred.succeed(invalidateStarted, undefined)
              yield* Deferred.await(releaseInvalidation)
              return []
            })
        })
        const local = yield* pipe({
          argument95_0: LocalStore.Store,
          argument95_1: LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(
              Layer.mergeAll(
                SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
                NodeCrypto.layer,
                Reactivity.layer,
                Layer.succeed(QueryReactivity.QueryReactivity, blockedQueryReactivity)
              )
            )
          )
        }, ({ argument95_0, argument95_1 }) => service(argument95_0, argument95_1))
        const pending = yield* pipe(
          { argument98_0: Domain.PutTodo, argument98_1: Domain.todo("commit-to-publish") },
          ({ argument98_0, argument98_1 }) => local.mutate(argument98_0, argument98_1)
        )
        yield* Ref.set(blockInvalidation, true)
        const pull = yield* Stream.toPull(local.settlements)
        const settling = yield* pipe(
          Protocol.RejectedReceipt.make({
            spaceId,
            clientId,
            mutationId: pending.envelope.mutationId,
            localSequence: pending.envelope.localSequence,
            membershipIncarnation: pending.envelope.membershipIncarnation,
            ...putTodoProvenance,
            origin: "Legacy",
            terminalSequence: Identity.TerminalSequence.make(pending.envelope.localSequence),
            rejection: "denied"
          }),
          (argument99_0) => local.applyReceipt(argument99_0)
        ).pipe(Effect.forkChild)

        yield* Deferred.await(invalidateStarted)
        const interruptionStarted = yield* Deferred.make<void>()
        const interruption = yield* Deferred.succeed(interruptionStarted, undefined).pipe(
          Effect.andThen(Fiber.interrupt(settling)),
          Effect.forkChild
        )
        yield* Deferred.await(interruptionStarted)
        yield* Deferred.succeed(releaseInvalidation, undefined)
        yield* Fiber.join(interruption)

        pipe({
          argument101_0: (yield* pull).map((settlement) => settlement.pending.envelope.mutationId),
          argument101_1: [pending.envelope.mutationId]
        }, ({ argument101_0, argument101_1 }) => assert.deepStrictEqual(argument101_0, argument101_1))
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument94_0) => Effect.scoped(argument94_0)
    ))

  it.effect("does not republish a deferred settlement after bootstrap removes its pending row", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8
        }
        const local = yield* pipe(
          { argument103_0: LocalStore.Store, argument103_1: localLayer() },
          ({ argument103_0, argument103_1 }) => service(argument103_0, argument103_1)
        )
        const server = yield* pipe({
          argument104_0: ServerStore.ServerStore,
          argument104_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument104_0, argument104_1 }) => service(argument104_0, argument104_1))
        yield* installFreshView(local, server)
        const pending = yield* pipe(
          { argument106_0: Domain.PutTodo, argument106_1: Domain.todo("bootstrap-deferred") },
          ({ argument106_0, argument106_1 }) => local.mutate(argument106_0, argument106_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        const page = incremental(
          yield* pipe(
            pullRequest((yield* local.replicationState).cursor),
            (argument107_0) => server.pull(argument107_0)
          )
        )
        const pull = yield* Stream.toPull(local.settlements)

        yield* local.persistReceipt(receipt)
        yield* local.applyViewPage({ ...page, hasMore: true })
        yield* server.maintain(spaceId)
        const required = yield* pipe(pullRequest(), (argument108_0) => server.pull(argument108_0))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const bootstrap = yield* pipe(
          bootstrapRequest(required.manifest),
          (argument109_0) => server.bootstrap(argument109_0)
        )
        yield* local.prepareBootstrap(bootstrap.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(bootstrap))
        yield* local.installBootstrap(bootstrap.manifest)

        pipe({
          argument110_0: (yield* pull).map((settlement) => settlement.pending.envelope.mutationId),
          argument110_1: [pending.envelope.mutationId]
        }, ({ argument110_0, argument110_1 }) => assert.deepStrictEqual(argument110_0, argument110_1))
        yield* pipe(
          incremental(
            yield* pipe(pullRequest((yield* local.replicationState).cursor), (argument112_0) =>
              server.pull(argument112_0))
          ),
          (argument111_0) =>
            local.applyViewPage(argument111_0)
        )
        const duplicate = yield* pull.pipe(
          Effect.timeoutOption("1 second"),
          Effect.forkChild({ startImmediately: true })
        )
        yield* TestClock.adjust("1 second")
        pipe(Option.isNone(yield* Fiber.join(duplicate)), (argument113_0) => assert.isTrue(argument113_0))
      }),
      (argument102_0) => Effect.scoped(argument102_0)
    ))

  it.effect("keeps a deferred settlement pending when invalidation preparation fails", () =>
    pipe(
      Effect.gen(function*() {
        const failPreparation = yield* Ref.make(false)
        const queryReactivity = QueryReactivity.QueryReactivity.of({
          retain: () => Effect.succeed(Effect.void),
          record: () => Effect.void,
          affected: () =>
            Effect.gen(function*() {
              if (yield* Ref.getAndSet(failPreparation, false)) {
                return yield* Effect.die("invalidation preparation failed")
              }
              return []
            })
        })
        const local = yield* pipe({
          argument115_0: LocalStore.Store,
          argument115_1: LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(
              Layer.mergeAll(
                SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
                NodeCrypto.layer,
                Reactivity.layer,
                Layer.succeed(QueryReactivity.QueryReactivity, queryReactivity)
              )
            )
          )
        }, ({ argument115_0, argument115_1 }) => service(argument115_0, argument115_1))
        const server = yield* pipe(
          { argument118_0: ServerStore.ServerStore, argument118_1: serverLayer() },
          ({ argument118_0, argument118_1 }) => service(argument118_0, argument118_1)
        )
        yield* installFreshView(local, server)
        const pending = yield* pipe({
          argument119_0: Domain.PutTodo,
          argument119_1: Domain.todo("deferred-invalidation")
        }, ({ argument119_0, argument119_1 }) => local.mutate(argument119_0, argument119_1))
        const receipt = yield* server.submit(pending.envelope)
        const page = incremental(
          yield* pipe(
            pullRequest((yield* local.replicationState).cursor),
            (argument120_0) => server.pull(argument120_0)
          )
        )
        const settlementFiber = yield* local.settlements.pipe(
          Stream.runHead,
          Effect.forkScoped({ startImmediately: true })
        )

        yield* local.persistReceipt(receipt)
        yield* Ref.set(failPreparation, true)
        const failed = yield* local.applyViewPage(page).pipe(Effect.exit)

        assert.strictEqual(failed._tag, "Failure")
        assert.strictEqual(yield* local.pendingCount, 1)

        yield* pipe(
          incremental(
            yield* pipe(pullRequest((yield* local.replicationState).cursor), (argument122_0) =>
              server.pull(argument122_0))
          ),
          (argument121_0) =>
            local.applyViewPage(argument121_0)
        )
        const deliveredOption = yield* Fiber.join(settlementFiber)
        const delivered = Option.getOrThrow(deliveredOption)
        assert.strictEqual(delivered.pending.envelope.mutationId, pending.envelope.mutationId)
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument114_0) => Effect.scoped(argument114_0)
    ))

  it.effect("keeps a quarantine settlement recoverable when final invalidation preparation fails", () =>
    pipe(
      Effect.gen(function*() {
        const affectedCalls = yield* Ref.make(0)
        const queryReactivity = QueryReactivity.QueryReactivity.of({
          retain: () => Effect.succeed(Effect.void),
          record: () => Effect.void,
          affected: () =>
            Effect.gen(function*() {
              const call = yield* Ref.updateAndGet(affectedCalls, (count) => count + 1)
              if (call === 2) return yield* Effect.die("final invalidation preparation failed")
              return []
            })
        })
        const clientDatabase = pipe({
          argument125_0: SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
          argument125_1: NodeCrypto.layer,
          argument125_2: Reactivity.layer,
          argument125_3: Layer.succeed(QueryReactivity.QueryReactivity, queryReactivity)
        }, ({ argument125_0, argument125_1, argument125_2, argument125_3 }) =>
          Layer.mergeAll(argument125_0, argument125_1, argument125_2, argument125_3))
        const live = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(live, clientDatabase), (argument126_0) =>
          Layer.build(argument126_0))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* pipe({
          argument127_0: Domain.PutTodo,
          argument127_1: Domain.todo("quarantine-finalization")
        }, ({ argument127_0, argument127_1 }) =>
          local.mutate(argument127_0, argument127_1))
        yield* Ref.set(affectedCalls, 0)
        yield* pipe(
          Effect.gen(function*() {
            yield* sql`INSERT INTO effect_local_client_quarantine
          (space_id, membership_incarnation, mutation_id, local_sequence, basis, name, payload_json,
            digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
            rejection_json, target_schema_version, target_schema_hash)
          SELECT space_id, membership_incarnation, mutation_id, local_sequence, basis, name, payload_json,
            digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
            ${yield* Canonical.stringifyEffect("schema-policy-rejected")},
            ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash}
          FROM effect_local_client_pending_data WHERE space_id = ${spaceId}
            AND mutation_id = ${pending.envelope.mutationId}`
            yield* sql`DELETE FROM effect_local_client_pending_data
          WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`
          }),
          (argument128_0) => sql.withTransaction(argument128_0)
        )
        const receipt = Protocol.RejectedReceipt.make({
          ...putTodoProvenance,
          spaceId,
          clientId,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          origin: "Quarantine",
          terminalSequence: Identity.TerminalSequence.make(1),
          rejection: "discarded"
        })
        const pull = yield* Stream.toPull(local.settlements)

        const failed = yield* local.resolveQuarantine(receipt).pipe(Effect.exit)

        assert.strictEqual(failed._tag, "Failure")
        pipe(Option.isNone(yield* local.quarantineByMutation(pending.envelope.mutationId)), (argument129_0) =>
          assert.isTrue(argument129_0))
        assert.strictEqual(yield* local.pendingCount, 1)
        pipe(Option.isSome(yield* local.receipt(pending.envelope.mutationId)), (argument130_0) =>
          assert.isTrue(argument130_0))

        yield* local.settleReceipts
        pipe({
          argument131_0: (yield* pull).map((settlement) =>
            settlement.pending.envelope.mutationId
          ),
          argument131_1: [pending.envelope.mutationId]
        }, ({ argument131_0, argument131_1 }) =>
          assert.deepStrictEqual(argument131_0, argument131_1))
        assert.strictEqual(yield* local.pendingCount, 0)
        yield* local.settleReceipts
        const duplicate = yield* pull.pipe(
          Effect.timeoutOption("1 second"),
          Effect.forkChild({ startImmediately: true })
        )
        yield* TestClock.adjust("1 second")
        pipe(Option.isNone(yield* Fiber.join(duplicate)), (argument132_0) =>
          assert.isTrue(argument132_0))
      }),
      (argument124_0) => Effect.scoped(argument124_0)
    ))

  it.effect("replays accepted pending without authoritative coverage while settling a rejection", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument134_0: LocalStore.Store, argument134_1: localLayer() },
          ({ argument134_0, argument134_1 }) => service(argument134_0, argument134_1)
        )
        const server = yield* pipe(
          { argument135_0: ServerStore.ServerStore, argument135_1: serverLayer() },
          ({ argument135_0, argument135_1 }) => service(argument135_0, argument135_1)
        )
        const acceptedPending = yield* pipe({
          argument136_0: Domain.PutTodo,
          argument136_1: Domain.todo("accepted-uncovered")
        }, ({ argument136_0, argument136_1 }) => local.mutate(argument136_0, argument136_1))
        const rejectedPending = yield* pipe({
          argument137_0: Domain.PutTodo,
          argument137_1: Domain.todo("rejected-covered")
        }, ({ argument137_0, argument137_1 }) => local.mutate(argument137_0, argument137_1))
        const accepted = yield* server.submit(acceptedPending.envelope)

        yield* local.persistReceipt(accepted)
        yield* pipe(
          Protocol.RejectedReceipt.make({
            spaceId,
            clientId,
            mutationId: rejectedPending.envelope.mutationId,
            localSequence: rejectedPending.envelope.localSequence,
            membershipIncarnation: rejectedPending.envelope.membershipIncarnation,
            ...putTodoProvenance,
            origin: "Legacy",
            terminalSequence: Identity.TerminalSequence.make(2),
            rejection: "denied"
          }),
          (argument138_0) => local.persistReceipt(argument138_0)
        )
        yield* local.settleReceipts

        pipe({
          argument139_0: Option.getOrThrow(yield* local.get(Domain.Todo, "accepted-uncovered")),
          argument139_1: Domain.todo("accepted-uncovered")
        }, ({ argument139_0, argument139_1 }) => assert.deepStrictEqual(argument139_0, argument139_1))
        pipe(Option.isNone(yield* local.get(Domain.Todo, "rejected-covered")), (argument140_0) =>
          assert.isTrue(argument140_0))
        assert.strictEqual(yield* local.pendingCount, 1)
      }),
      (argument133_0) =>
        Effect.scoped(argument133_0)
    ))

  it.effect("filters, orders, paginates, and streams through a declared secondary index", () =>
    pipe(
      Effect.gen(function*() {
        const sharedDatabase = database()
        const local = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime)
        )
        const queries = QueryExecutor.layer(Domain.definition, spaceId).pipe(Layer.provide(Domain.handlers))
        const context = yield* pipe(Layer.merge(local, queries).pipe(Layer.provide(sharedDatabase)), (argument142_0) =>
          Layer.build(argument142_0))
        const store = Context.get(context, LocalStore.Store)
        const executor = Context.get(context, QueryExecutor.QueryExecutor)
        for (const [id, count] of [["low", 1], ["middle", 3], ["high", 5], ["highest", 7]] as const) {
          yield* store.mutate(Domain.PutTodo, { ...Domain.todo(id), count })
        }

        assert.deepStrictEqual(yield* executor.execute(Domain.ReadCountIndex, { minimum: 3, direction: "asc" }), {
          first: ["middle", "high"],
          second: ["highest"],
          streamed: ["middle", "high", "highest"]
        })
        assert.deepStrictEqual(yield* executor.execute(Domain.ReadCountIndex, { minimum: 3, direction: "desc" }), {
          first: ["highest", "high"],
          second: ["middle"],
          streamed: ["highest", "high", "middle"]
        })
      }),
      (argument141_0) =>
        Effect.scoped(argument141_0)
    ))

  it.effect("falls forward to a covering snapshot when an expired receipt snapshot is retired", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8,
          retainedSnapshots: 1
        }
        const server = yield* pipe({
          argument144_0: ServerStore.ServerStore,
          argument144_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument144_0, argument144_1 }) => service(argument144_0, argument144_1))
        const first = yield* pipe({
          argument146_0: Domain.PutTodo.name,
          argument146_1: Domain.todo("retired-snapshot-1"),
          argument146_2: 1,
          argument146_3: Identity.MutationId.make("mut_00000000-0000-4000-8010-000000000001")
        }, ({ argument146_0, argument146_1, argument146_2, argument146_3 }) =>
          envelope(argument146_0, argument146_1, argument146_2, argument146_3))
        yield* server.submit(first)
        yield* server.maintain(spaceId)
        const expired = yield* server.submit(first)
        if (expired._tag !== "Expired") {
          assert.fail("expected expired receipt")
        }

        yield* server.submit(
          yield* pipe({
            argument147_0: Domain.PutTodo.name,
            argument147_1: Domain.todo("retired-snapshot-2"),
            argument147_2: 2,
            argument147_3: Identity.MutationId.make("mut_00000000-0000-4000-8010-000000000002")
          }, ({ argument147_0, argument147_1, argument147_2, argument147_3 }) =>
            envelope(argument147_0, argument147_1, argument147_2, argument147_3))
        )
        yield* server.maintain(spaceId)

        const required = yield* pipe(pullRequest(), (argument148_0) =>
          server.pull(argument148_0))
        if (!("_tag" in required)) {
          assert.fail("expected bootstrap")
        }
        const page = yield* pipe(
          Protocol.BootstrapRequest.make({
            ...bootstrapRequest(required.manifest),
            snapshotId: expired.snapshotId
          }),
          (argument149_0) =>
            server.bootstrap(argument149_0)
        )
        assert.notStrictEqual(page.manifest.snapshotId, expired.snapshotId)
        assert.isAtLeast(page.manifest.sequence, expired.snapshotSequence)
        assert.isAtLeast(page.manifest.terminalSequenceThrough, expired.terminalSequenceThrough)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument143_0) =>
        Effect.scoped(argument143_0)
    ))

  it.effect("publishes a snapshot before bounding history and receipts", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 1,
          maximumHistoryEntries: 4,
          retainedReceipts: 1,
          maximumReceipts: 4
        }
        const live = ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument151_0) => Layer.build(argument151_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted: Array<Protocol.MutationEnvelope> = []
        for (let sequence = 1; sequence <= 4; sequence++) {
          const item = yield* pipe(
            {
              argument152_0: Domain.PutTodo.name,
              argument152_1: Domain.todo(`bounded-${sequence}`),
              argument152_2: sequence,
              argument152_3: Identity.MutationId.make(
                `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
              )
            },
            ({ argument152_0, argument152_1, argument152_2, argument152_3 }) =>
              envelope(argument152_0, argument152_1, argument152_2, argument152_3)
          )
          submitted.push(item)
          assert.strictEqual((yield* server.submit(item))._tag, "Accepted")
        }

        yield* sql`CREATE TRIGGER require_snapshot_before_history_delete
          BEFORE DELETE ON effect_local_authoritative_log
          WHEN NOT EXISTS (
            SELECT 1 FROM effect_local_server_snapshots WHERE space_id = OLD.space_id
          )
          BEGIN SELECT RAISE(ABORT, 'snapshot required before history delete'); END`
        yield* sql`CREATE TRIGGER require_snapshot_before_receipt_delete
          BEFORE DELETE ON effect_local_server_receipts
          WHEN NOT EXISTS (
            SELECT 1 FROM effect_local_server_snapshots WHERE space_id = OLD.space_id
          )
          BEGIN SELECT RAISE(ABORT, 'snapshot required before receipt delete'); END`

        yield* server.maintain(spaceId)

        const countRows = SqlSchema.findOne({
          Request: Schema.String,
          Result: Schema.Struct({ history: Schema.Int, receipts: Schema.Int, snapshots: Schema.Int }),
          execute: (requestedSpace) =>
            sql`SELECT
            (SELECT COUNT(*) FROM effect_local_authoritative_log WHERE space_id = ${requestedSpace}) AS history,
            (SELECT COUNT(*) FROM effect_local_server_receipts WHERE space_id = ${requestedSpace}) AS receipts,
            (SELECT COUNT(*) FROM effect_local_server_snapshots WHERE space_id = ${requestedSpace}) AS snapshots`
        })
        const counts = yield* countRows(spaceId)
        assert.strictEqual(counts.history, 1)
        assert.strictEqual(counts.receipts, 1)
        assert.strictEqual(counts.snapshots, 1)
        const metrics = yield* Metric.snapshot
        const prunedHistory = metrics.find((snapshot) =>
          snapshot.id === "effect_local_server_pruned" && snapshot.attributes?.resource === "history"
        )
        assert.strictEqual(prunedHistory?.type, "Counter")
        if (prunedHistory?.type === "Counter") assert.strictEqual(prunedHistory.state.count, 3)
        const prunedReceipt = metrics.find((snapshot) =>
          snapshot.id === "effect_local_server_pruned" && snapshot.attributes?.resource === "receipt"
        )
        assert.strictEqual(prunedReceipt?.type, "Counter")
        if (prunedReceipt?.type === "Counter") assert.strictEqual(prunedReceipt.state.count, 3)
        const globalSnapshot = yield* SqlSchema.findOne({
          Request: Identity.SpaceId,
          Result: Schema.Struct({ snapshot_id: Identity.SnapshotId }),
          execute: (requestedSpace) =>
            sql`SELECT snapshot_id FROM effect_local_server_snapshots
              WHERE space_id = ${requestedSpace}`
        })(spaceId)

        const pulled = yield* pipe(pullRequest(), (argument153_0) => server.pull(argument153_0))
        assert.isTrue("_tag" in pulled)
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const page = yield* pipe(bootstrapRequest(pulled.manifest), (argument154_0) => server.bootstrap(argument154_0))
        assert.strictEqual(page.entries.length, 4)
        assert.isFalse(page.hasMore)
        pipe(
          { argument155_0: Protocol.encodedBytes(page), argument155_1: bounded.maximumBootstrapPageBytes },
          ({ argument155_0, argument155_1 }) => assert.isAtMost(argument155_0, argument155_1)
        )

        const expired = yield* server.submit(submitted[0])
        assert.strictEqual(expired._tag, "Expired")
        if (expired._tag === "Expired") {
          assert.strictEqual(expired.snapshotId, globalSnapshot.snapshot_id)
          assert.notStrictEqual(expired.snapshotId, pulled.manifest.snapshotId)
          assert.isAtLeast(expired.terminalSequenceThrough, 3)
        }
      }).pipe(
        Effect.provide(NodeCrypto.layer),
        Effect.provideService(Metric.MetricRegistry, new Map())
      ),
      (argument150_0) => Effect.scoped(argument150_0)
    ))

  it.effect("pages every space during global history maintenance", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({
          ...serverHistory,
          definition: Domain.definition,
          maintenanceSpaceBatchSize: 2
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument157_0) => Layer.build(argument157_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        for (let index = 1; index <= 3; index++) {
          const requestedSpace = Identity.SpaceId.make(
            `spc_00000000-0000-4000-8000-${String(index).padStart(12, "0")}`
          )
          const identity: Omit<Protocol.MutationEnvelope, "digest"> = {
            spaceId: requestedSpace,
            clientId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8002-${String(index).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(1),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutTodo.name,
            payload: Domain.todo(`maintain-${index}`),
            digestVersion: 3,
            membershipIncarnation: defaultMembershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutTodo.version
          }
          yield* pipe(
            Protocol.MutationEnvelope.make({
              ...identity,
              digest: yield* Protocol.mutationDigest(identity)
            }),
            (argument158_0) => server.submit(argument158_0)
          )
        }

        yield* server.maintainAll

        const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_snapshots`
        assert.strictEqual(rows[0].count, 3)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument156_0) => Effect.scoped(argument156_0)
    ))

  it.effect("applies hard history backpressure until maintenance publishes recovery state", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 2,
          retainedReceipts: 1,
          maximumReceipts: 3
        }
        const server = yield* pipe({
          argument160_0: ServerStore.ServerStore,
          argument160_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument160_0, argument160_1 }) => service(argument160_0, argument160_1))
        for (let sequence = 1; sequence <= 2; sequence++) {
          yield* server.submit(
            yield* pipe({
              argument162_0: Domain.PutTodo.name,
              argument162_1: Domain.todo(`capacity-${sequence}`),
              argument162_2: sequence,
              argument162_3: Identity.MutationId.make(
                `mut_00000000-0000-4000-8001-${String(sequence).padStart(12, "0")}`
              )
            }, ({ argument162_0, argument162_1, argument162_2, argument162_3 }) =>
              envelope(argument162_0, argument162_1, argument162_2, argument162_3))
          )
        }
        const third = yield* pipe({
          argument163_0: Domain.PutTodo.name,
          argument163_1: Domain.todo("capacity-3"),
          argument163_2: 3,
          argument163_3: Identity.MutationId.make("mut_00000000-0000-4000-8001-000000000003")
        }, ({ argument163_0, argument163_1, argument163_2, argument163_3 }) =>
          envelope(argument163_0, argument163_1, argument163_2, argument163_3))
        const blocked = yield* server.submit(third).pipe(Effect.flip)
        assert.strictEqual(blocked._tag, "CapacityExceeded")
        yield* server.maintain(spaceId)
        assert.strictEqual((yield* server.submit(third))._tag, "Accepted")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument159_0) =>
        Effect.scoped(argument159_0)
    ))

  it.effect("rejects corrupted retained row counters before admission", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument165_0) => Layer.build(argument165_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        yield* server.submit(
          yield* pipe(
            {
              argument166_0: Domain.PutTodo.name,
              argument166_1: Domain.todo("counter-1"),
              argument166_2: 1,
              argument166_3: Identity.MutationId.make("mut_00000000-0000-4000-8001-100000000001")
            },
            ({ argument166_0, argument166_1, argument166_2, argument166_3 }) =>
              envelope(argument166_0, argument166_1, argument166_2, argument166_3)
          )
        )
        yield* sql`UPDATE effect_local_server_spaces SET retained_history_count = 0
          WHERE space_id = ${spaceId}`

        const error = yield* server.submit(
          yield* pipe(
            {
              argument167_0: Domain.PutTodo.name,
              argument167_1: Domain.todo("counter-2"),
              argument167_2: 2,
              argument167_3: Identity.MutationId.make("mut_00000000-0000-4000-8001-100000000002")
            },
            ({ argument167_0, argument167_1, argument167_2, argument167_3 }) =>
              envelope(argument167_0, argument167_1, argument167_2, argument167_3)
          )
        ).pipe(Effect.flip)

        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument164_0) => Effect.scoped(argument164_0)
    ))

  it.effect("bounds rejected receipts independently from accepted history", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 1,
          retainedReceipts: 0,
          maximumReceipts: 2
        }
        const server = yield* pipe({
          argument169_0: ServerStore.ServerStore,
          argument169_1: ServerStore.layer({
            ...bounded,
            definition: Domain.definition,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.fail(new TestAuthorizationError({ reason: "denied" })),
            authorizeRead: () => Effect.void
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument169_0, argument169_1 }) => service(argument169_0, argument169_1))
        const submitted: Array<Protocol.MutationEnvelope> = []
        for (let sequence = 1; sequence <= 3; sequence++) {
          submitted.push(
            yield* pipe(
              {
                argument171_0: Domain.PutTodo.name,
                argument171_1: Domain.todo(`rejected-${sequence}`),
                argument171_2: sequence,
                argument171_3: Identity.MutationId.make(
                  `mut_00000000-0000-4000-8004-${String(sequence).padStart(12, "0")}`
                )
              },
              ({ argument171_0, argument171_1, argument171_2, argument171_3 }) =>
                envelope(argument171_0, argument171_1, argument171_2, argument171_3)
            )
          )
        }

        assert.strictEqual((yield* server.submit(submitted[0]))._tag, "Rejected")
        assert.strictEqual((yield* server.submit(submitted[1]))._tag, "Rejected")
        const blocked = yield* server.submit(submitted[2]).pipe(Effect.flip)
        assert.strictEqual(blocked._tag, "CapacityExceeded")

        yield* server.maintain(spaceId)

        assert.strictEqual((yield* server.submit(submitted[2]))._tag, "Rejected")
        assert.strictEqual((yield* server.submit(submitted[0]))._tag, "Expired")
        const pulled = yield* pipe(pullRequest(), (argument172_0) => server.pull(argument172_0))
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        assert.strictEqual(pulled.manifest.entityCount, 0)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument168_0) => Effect.scoped(argument168_0)
    ))

  it.effect("terminally rejects state that cannot fit a future snapshot", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const bounded = {
          ...serverHistory,
          maximumSnapshotBytes: 1
        }
        const live = ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument174_0) => Layer.build(argument174_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const item = yield* pipe(
          {
            argument175_0: Domain.PutTodo.name,
            argument175_1: Domain.todo("snapshot-capacity"),
            argument175_2: 1,
            argument175_3: Identity.MutationId.make("mut_00000000-0000-4000-8005-000000000001")
          },
          ({ argument175_0, argument175_1, argument175_2, argument175_3 }) =>
            envelope(argument175_0, argument175_1, argument175_2, argument175_3)
        )

        const receipt = yield* server.submit(item)
        assert.strictEqual(receipt._tag, "Rejected")
        if (receipt._tag !== "Rejected") assert.fail("expected terminal rejection")
        assert.deepStrictEqual(receipt.rejection, {
          _tag: "CapacityExceeded",
          resource: "snapshot bytes",
          limit: 1
        })
        assert.deepStrictEqual(yield* server.submit(item), receipt)

        const count = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ entities: Schema.Int, history: Schema.Int }),
          execute: () =>
            sql`SELECT
              (SELECT COUNT(*) FROM effect_local_server_entities) AS entities,
              (SELECT COUNT(*) FROM effect_local_authoritative_log) AS history`
        })(undefined)
        assert.deepStrictEqual(count, { entities: 0, history: 0 })
        yield* server.maintain(spaceId)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument173_0) => Effect.scoped(argument173_0)
    ))

  it.effect("bootstraps a fresh client without replaying retained history", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 1,
          maximumHistoryEntries: 8,
          retainedReceipts: 1,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument177_0: ServerStore.ServerStore,
          argument177_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument177_0, argument177_1 }) => service(argument177_0, argument177_1))
        for (let sequence = 1; sequence <= 4; sequence++) {
          yield* server.submit(
            yield* pipe(
              {
                argument179_0: Domain.PutTodo.name,
                argument179_1: Domain.todo(`bootstrap-${sequence}`),
                argument179_2: sequence,
                argument179_3: Identity.MutationId.make(
                  `mut_00000000-0000-4000-8002-${String(sequence).padStart(12, "0")}`
                )
              },
              ({ argument179_0, argument179_1, argument179_2, argument179_3 }) =>
                envelope(argument179_0, argument179_1, argument179_2, argument179_3)
            )
          )
        }
        yield* server.maintain(spaceId)

        const freshClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000099")
        const local = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId: freshClientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const reconciler = Reconciler.layerOnePass({ definition: Domain.definition, spaceId, pageSize: 2 }).pipe(
          Layer.provide(local),
          Layer.provide(directSync(server))
        )
        const context = yield* pipe(Layer.merge(local, reconciler), (argument182_0) => Layer.build(argument182_0))
        const store = Context.get(context, LocalStore.Store)
        yield* Context.get(context, Reconciler.Reconciliation).sync

        assert.strictEqual(yield* store.cursor, 4)
        for (let sequence = 1; sequence <= 4; sequence++) {
          pipe({
            argument183_0: Option.getOrThrow(yield* store.get(Domain.Todo, `bootstrap-${sequence}`)),
            argument183_1: Domain.todo(`bootstrap-${sequence}`)
          }, ({ argument183_0, argument183_1 }) => assert.deepStrictEqual(argument183_0, argument183_1))
        }
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument176_0) => Effect.scoped(argument176_0)
    ))

  it.effect("publishes one projection after a multi-page catch up", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe(
          { argument185_0: ServerStore.ServerStore, argument185_1: serverLayer() },
          ({ argument185_0, argument185_1 }) => service(argument185_0, argument185_1)
        )
        for (let sequence = 1; sequence <= 4; sequence++) {
          yield* server.submit(
            yield* pipe(
              {
                argument186_0: Domain.PutTodo.name,
                argument186_1: Domain.todo(`paged-${sequence}`),
                argument186_2: sequence,
                argument186_3: Identity.MutationId.make(
                  `mut_00000000-0000-4000-8004-${String(sequence).padStart(12, "0")}`
                )
              },
              ({ argument186_0, argument186_1, argument186_2, argument186_3 }) =>
                envelope(argument186_0, argument186_1, argument186_2, argument186_3)
            )
          )
        }

        const databaseContext = yield* pipe(database(), (argument187_0) => Layer.build(argument187_0))
        const sql = Context.get(databaseContext, SqlClient.SqlClient)
        const services = pipe(
          {
            argument188_0: Layer.succeed(SqlClient.SqlClient, sql),
            argument188_1: pipe({
              argument189_0: Crypto.Crypto,
              argument189_1: Context.get(databaseContext, Crypto.Crypto)
            }, ({ argument189_0, argument189_1 }) => Layer.succeed(argument189_0, argument189_1)),
            argument188_2: pipe({
              argument190_0: Reactivity.Reactivity,
              argument190_1: Context.get(databaseContext, Reactivity.Reactivity)
            }, ({ argument190_0, argument190_1 }) => Layer.succeed(argument190_0, argument190_1)),
            argument188_3: pipe({
              argument191_0: QueryReactivity.QueryReactivity,
              argument191_1: Context.get(databaseContext, QueryReactivity.QueryReactivity)
            }, ({ argument191_0, argument191_1 }) => Layer.succeed(argument191_0, argument191_1))
          },
          ({ argument188_0, argument188_1, argument188_2, argument188_3 }) =>
            Layer.mergeAll(argument188_0, argument188_1, argument188_2, argument188_3)
        )
        const localContext = yield* pipe(
          LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(services)
          ),
          (argument192_0) => Layer.build(argument192_0)
        )
        const local = Context.get(localContext, LocalStore.Store)
        yield* sql`CREATE TABLE projection_insert_probe (count INTEGER NOT NULL)`
        yield* sql`CREATE TRIGGER projection_insert_probe_trigger
        AFTER INSERT ON effect_local_client_visible_entities_data
        BEGIN INSERT INTO projection_insert_probe VALUES (1); END`
        const reconciliation = yield* pipe({
          argument193_0: Reconciler.Reconciliation,
          argument193_1: Reconciler.layerOnePass({ definition: Domain.definition, spaceId, pageSize: 1 }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(directSync(server))
          )
        }, ({ argument193_0, argument193_1 }) => service(argument193_0, argument193_1))

        yield* reconciliation.sync

        const copied = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM projection_insert_probe`
        assert.strictEqual(copied[0].count, 4)
        assert.strictEqual(yield* local.cursor, 4)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument184_0) => Effect.scoped(argument184_0)
    ))

  it.effect("resumes a durable bootstrap stage after reopening the client database", () =>
    pipe(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/bootstrap-resume.sqlite`
        const persistentDatabase = () =>
          pipe(
            {
              argument197_0: SqliteClient.layer({ filename, disableWAL: true }),
              argument197_1: NodeCrypto.layer,
              argument197_2: Reactivity.layer,
              argument197_3: QueryReactivity.layer
            },
            ({ argument197_0, argument197_1, argument197_2, argument197_3 }) =>
              Layer.mergeAll(argument197_0, argument197_1, argument197_2, argument197_3)
          )
        const makeLocal = () =>
          LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument199_0: ServerStore.ServerStore,
          argument199_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument199_0, argument199_1 }) => service(argument199_0, argument199_1))
        for (let sequence = 1; sequence <= 2; sequence++) {
          yield* server.submit(
            yield* pipe(
              {
                argument201_0: Domain.PutTodo.name,
                argument201_1: Domain.todo(`resume-${sequence}`),
                argument201_2: sequence,
                argument201_3: Identity.MutationId.make(
                  `mut_00000000-0000-4000-8006-${String(sequence).padStart(12, "0")}`
                )
              },
              ({ argument201_0, argument201_1, argument201_2, argument201_3 }) =>
                envelope(argument201_0, argument201_1, argument201_2, argument201_3)
            )
          )
        }
        yield* server.maintain(spaceId)
        const pulled = yield* pipe(pullRequest(), (argument202_0) => server.pull(argument202_0))
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const first = yield* pipe(
          bootstrapRequest(pulled.manifest, -1, 1),
          (argument203_0) => server.bootstrap(argument203_0)
        )
        assert.isTrue(first.hasMore)

        yield* pipe(
          Effect.gen(function*() {
            const local = yield* pipe(
              { argument205_0: LocalStore.Store, argument205_1: makeLocal() },
              ({ argument205_0, argument205_1 }) => service(argument205_0, argument205_1)
            )
            assert.strictEqual(yield* local.prepareBootstrap(first.manifest), -1)
            assert.isFalse(yield* local.stageBootstrapPage(first))
          }),
          (argument204_0) => Effect.scoped(argument204_0)
        )

        yield* pipe(
          Effect.gen(function*() {
            const local = yield* pipe(
              { argument207_0: LocalStore.Store, argument207_1: makeLocal() },
              ({ argument207_0, argument207_1 }) => service(argument207_0, argument207_1)
            )
            assert.strictEqual(yield* local.prepareBootstrap(first.manifest), 0)
            const finalPage = yield* pipe(bootstrapRequest(first.manifest, 0, 1), (argument208_0) =>
              server.bootstrap(argument208_0))
            assert.isTrue(yield* local.stageBootstrapPage(finalPage))
            yield* local.installBootstrap(finalPage.manifest)
            assert.strictEqual(yield* local.cursor, 2)
            pipe({
              argument209_0: Option.getOrThrow(yield* local.get(Domain.Todo, "resume-1")),
              argument209_1: Domain.todo("resume-1")
            }, ({ argument209_0, argument209_1 }) =>
              assert.deepStrictEqual(argument209_0, argument209_1))
            pipe({
              argument210_0: Option.getOrThrow(yield* local.get(Domain.Todo, "resume-2")),
              argument210_1: Domain.todo("resume-2")
            }, ({ argument210_0, argument210_1 }) => assert.deepStrictEqual(argument210_0, argument210_1))
          }),
          (argument206_0) => Effect.scoped(argument206_0)
        )
      }).pipe(Effect.provide(NodeFileSystem.layer), Effect.provide(NodeCrypto.layer)),
      (argument196_0) => Effect.scoped(argument196_0)
    ))

  it.effect("keeps canonical state unchanged when a bootstrap page is corrupt", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 4,
          retainedReceipts: 0,
          maximumReceipts: 4
        }
        const server = yield* pipe({
          argument212_0: ServerStore.ServerStore,
          argument212_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument212_0, argument212_1 }) => service(argument212_0, argument212_1))
        const item = yield* pipe(
          {
            argument214_0: Domain.PutTodo.name,
            argument214_1: Domain.todo("corrupt-bootstrap"),
            argument214_2: 1,
            argument214_3: Identity.MutationId.make("mut_00000000-0000-4000-8003-000000000001")
          },
          ({ argument214_0, argument214_1, argument214_2, argument214_3 }) =>
            envelope(argument214_0, argument214_1, argument214_2, argument214_3)
        )
        yield* server.submit(item)
        yield* server.maintain(spaceId)
        const pulled = yield* pipe(pullRequest(), (argument215_0) => server.pull(argument215_0))
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const page = yield* pipe(bootstrapRequest(pulled.manifest), (argument216_0) => server.bootstrap(argument216_0))
        const local = yield* pipe(
          { argument217_0: LocalStore.Store, argument217_1: localLayer() },
          ({ argument217_0, argument217_1 }) => service(argument217_0, argument217_1)
        )
        yield* local.prepareBootstrap(page.manifest)
        const corrupt = Protocol.BootstrapPage.make({
          ...page,
          entries: page.entries.map((entry) => {
            if (entry.change._tag !== "Upsert") return entry
            return { ...entry, change: { ...entry.change, value: { corrupt: true } } }
          })
        })
        const error = yield* local.stageBootstrapPage(corrupt).pipe(expectedFailure)
        assert.strictEqual(error._tag, "ProtocolInvalid")
        const stalled = yield* pipe(
          Protocol.BootstrapPage.make({
            manifest: page.manifest,
            entries: [],
            hasMore: true,
            serverSchema: page.serverSchema
          }),
          (argument218_0) => local.stageBootstrapPage(argument218_0)
        ).pipe(expectedFailure)
        assert.strictEqual(stalled._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.cursor, 0)
        pipe(
          Option.isNone(yield* local.get(Domain.Todo, "corrupt-bootstrap")),
          (argument219_0) => assert.isTrue(argument219_0)
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument211_0) => Effect.scoped(argument211_0)
    ))

  it.effect("settles an expired pending mutation only after installing its covering snapshot", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument221_0: ServerStore.ServerStore,
          argument221_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument221_0, argument221_1 }) => service(argument221_0, argument221_1))
        const clientDatabase = database()
        const localLayerWithDatabase = LocalStore.layer({
          ...clientHistory,
          retainedReceipts: 1,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const reconciler = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(localLayerWithDatabase),
          Layer.provide(directSync(server))
        )
        const context = yield* pipe(
          Layer.mergeAll(localLayerWithDatabase, reconciler, clientDatabase),
          (argument224_0) => Layer.build(argument224_0)
        )
        const local = Context.get(context, LocalStore.Store)
        const sync = Context.get(context, Reconciler.Reconciliation)
        const sql = Context.get(context, SqlClient.SqlClient)

        yield* pipe(
          { argument225_0: Domain.PutTodo, argument225_1: Domain.todo("expired-pending") },
          ({ argument225_0, argument225_1 }) => local.mutate(argument225_0, argument225_1)
        )
        yield* sync.sync
        const increment = yield* local.mutate(Domain.IncrementTodo, { id: "expired-pending", delta: 1 })
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)
        assert.strictEqual((yield* server.submit(increment.envelope))._tag, "Accepted")
        yield* server.maintain(spaceId)

        const expired = yield* server.submit(increment.envelope)
        assert.strictEqual(expired._tag, "Expired")
        if (expired._tag !== "Expired") assert.fail("expected expired receipt")
        yield* local.persistReceipt(expired)
        yield* local.settleReceipts

        assert.strictEqual(yield* local.pendingCount, 1)
        assert.strictEqual(Option.getOrThrow(yield* local.receipt(increment.envelope.mutationId))._tag, "Expired")
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)

        const state = yield* local.replicationState
        if (state.cursor === null) assert.fail("expected installed replication view")
        const page = yield* server.bootstrap({
          spaceId,
          clientId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: state.cursor,
          snapshotId: expired.snapshotId,
          afterOrdinal: -1,
          limit: 10
        })
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
        yield* local.installBootstrap(page.manifest)

        assert.strictEqual(yield* local.pendingCount, 0)
        const receipt = Option.getOrThrow(yield* local.receipt(increment.envelope.mutationId))
        assert.strictEqual(receipt._tag, "Expired")
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)
        const countRows = SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ receipts: Schema.Int, history: Schema.Int }),
          execute: () =>
            sql`SELECT
              (SELECT COUNT(*) FROM effect_local_client_receipts_data WHERE space_id = ${spaceId}) AS receipts,
              (SELECT COUNT(*) FROM effect_local_server_log WHERE space_id = ${spaceId}) AS history`
        })
        assert.deepStrictEqual(yield* countRows(undefined), { receipts: 1, history: 0 })
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument220_0) => Effect.scoped(argument220_0)
    ))

  it.effect("settles expired rejected history when the cursor already covers its snapshot", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument227_0: ServerStore.ServerStore,
          argument227_1: ServerStore.layer({
            ...bounded,
            definition: Domain.definition,
            authorizeAccess: () => Effect.void,
            authorizeMutation: (input) => {
              if (input.mutation.name === Domain.RenameTodo.name) {
                return Effect.fail(new TestAuthorizationError({ reason: "denied" }))
              }
              return Effect.void
            },
            authorizeRead: () => Effect.void
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument227_0, argument227_1 }) => service(argument227_0, argument227_1))
        const localLive = LocalStore.layer({
          ...clientHistory,
          retainedReceipts: 2,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const reconciliationLive = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(localLive),
          Layer.provide(directSync(server))
        )
        const context = yield* pipe(Layer.merge(localLive, reconciliationLive), (argument231_0) =>
          Layer.build(argument231_0))
        const local = Context.get(context, LocalStore.Store)
        const reconciliation = Context.get(context, Reconciler.Reconciliation)

        yield* pipe(
          { argument232_0: Domain.PutTodo, argument232_1: Domain.todo("terminal-fence") },
          ({ argument232_0, argument232_1 }) =>
            local.mutate(argument232_0, argument232_1)
        )
        yield* reconciliation.sync
        yield* server.maintain(spaceId)
        const firstRequired = yield* pipe(pullRequest(), (argument233_0) => server.pull(argument233_0))
        if (!("_tag" in firstRequired)) assert.fail("expected first snapshot")
        const firstPage = yield* pipe(bootstrapRequest(firstRequired.manifest), (argument234_0) =>
          server.bootstrap(argument234_0))
        yield* local.prepareBootstrap(firstPage.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(firstPage))
        yield* local.installBootstrap(firstPage.manifest)

        const rejected = yield* local.mutate(Domain.RenameTodo, { id: "terminal-fence", title: "rejected-only" })
        assert.strictEqual((yield* server.submit(rejected.envelope))._tag, "Rejected")
        yield* server.maintain(spaceId)

        yield* reconciliation.sync

        assert.strictEqual(yield* local.pendingCount, 0)
        assert.strictEqual(
          Option.getOrThrow(yield* local.receipt(rejected.envelope.mutationId))._tag,
          "Expired"
        )
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "terminal-fence")).title, "first")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument226_0) =>
        Effect.scoped(argument226_0)
    ))

  it.effect("revalidates durable staged entities before snapshot installation", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument236_0: ServerStore.ServerStore,
          argument236_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument236_0, argument236_1 }) => service(argument236_0, argument236_1))
        const clientDatabase = database()
        const localLive = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(
          Layer.merge(localLive, clientDatabase),
          (argument238_0) => Layer.build(argument238_0)
        )
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)

        const initial = yield* pipe({
          argument239_0: Domain.PutTodo,
          argument239_1: Domain.todo("staged-corruption", "old")
        }, ({ argument239_0, argument239_1 }) => local.mutate(argument239_0, argument239_1))
        const initialReceipt = yield* server.submit(initial.envelope)
        yield* local.applyReceipt(initialReceipt)
        const initialRequired = yield* pipe(pullRequest(), (argument240_0) => server.pull(argument240_0))
        if (!("_tag" in initialRequired)) assert.fail("expected initial bootstrap")
        const initialPage = yield* pipe(
          bootstrapRequest(initialRequired.manifest),
          (argument241_0) => server.bootstrap(argument241_0)
        )
        yield* local.prepareBootstrap(initialPage.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(initialPage))
        yield* local.installBootstrap(initialPage.manifest)
        const rename = yield* pipe(
          {
            argument242_0: Domain.RenameTodo.name,
            argument242_1: { id: "staged-corruption", title: "new" },
            argument242_2: 2,
            argument242_3: Identity.MutationId.make("mut_00000000-0000-4000-8007-000000000002"),
            argument242_4: initial.envelope.membershipIncarnation
          },
          ({ argument242_0, argument242_1, argument242_2, argument242_3, argument242_4 }) =>
            envelope(argument242_0, argument242_1, argument242_2, argument242_3, argument242_4)
        )
        yield* server.submit(rename)
        yield* server.maintain(spaceId)
        const required = yield* pipe(pullRequest(), (argument243_0) => server.pull(argument243_0))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* pipe(
          bootstrapRequest(required.manifest),
          (argument244_0) => server.bootstrap(argument244_0)
        )
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
        yield* sql`UPDATE effect_local_client_scoped_bootstrap_entries
          SET entry_bytes = entry_bytes + 1 WHERE ordinal = 0`

        const error = yield* local.installBootstrap(page.manifest).pipe(expectedFailure)

        assert.strictEqual(error._tag, "StorageCorrupt")
        assert.strictEqual(yield* local.cursor, 1)
        assert.strictEqual(
          Option.getOrThrow(yield* local.get(Domain.Todo, "staged-corruption")).title,
          "old"
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument235_0) => Effect.scoped(argument235_0)
    ))

  it.effect("rejects mismatched durable receipt identity during snapshot installation", () =>
    pipe(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* pipe({
          argument246_0: ServerStore.ServerStore,
          argument246_1: ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument246_0, argument246_1 }) => service(argument246_0, argument246_1))
        const clientDatabase = database()
        const localLive = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(
          Layer.merge(localLive, clientDatabase),
          (argument248_0) => Layer.build(argument248_0)
        )
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const first = yield* pipe(
          { argument249_0: Domain.PutTodo, argument249_1: Domain.todo("receipt-a") },
          ({ argument249_0, argument249_1 }) => local.mutate(argument249_0, argument249_1)
        )
        const second = yield* pipe(
          { argument250_0: Domain.PutTodo, argument250_1: Domain.todo("receipt-b") },
          ({ argument250_0, argument250_1 }) => local.mutate(argument250_0, argument250_1)
        )
        yield* pipe(
          Protocol.RejectedReceipt.make({
            spaceId,
            clientId,
            membershipIncarnation: first.envelope.membershipIncarnation,
            mutationId: first.envelope.mutationId,
            localSequence: first.envelope.localSequence,
            ...putTodoProvenance,
            origin: "Legacy",
            terminalSequence: Identity.TerminalSequence.make(1),
            rejection: "denied"
          }),
          (argument251_0) => local.persistReceipt(argument251_0)
        )
        const corrupt = Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          membershipIncarnation: second.envelope.membershipIncarnation,
          mutationId: second.envelope.mutationId,
          localSequence: second.envelope.localSequence,
          ...putTodoProvenance,
          origin: "Legacy",
          terminalSequence: Identity.TerminalSequence.make(1),
          rejection: "denied"
        })
        yield* sql`UPDATE effect_local_client_receipts_data SET receipt_json = ${Canonical.stringify(corrupt)}
          WHERE space_id = ${spaceId} AND mutation_id = ${first.envelope.mutationId}`
        const authoritative = yield* pipe(
          {
            argument252_0: Domain.PutTodo.name,
            argument252_1: Domain.todo("snapshot-receipt-identity"),
            argument252_2: 1,
            argument252_3: Identity.MutationId.make("mut_00000000-0000-4000-8008-000000000001")
          },
          ({ argument252_0, argument252_1, argument252_2, argument252_3 }) =>
            envelope(argument252_0, argument252_1, argument252_2, argument252_3)
        )
        yield* server.submit(authoritative)
        yield* server.maintain(spaceId)
        const required = yield* pipe(pullRequest(), (argument253_0) => server.pull(argument253_0))
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* pipe(
          bootstrapRequest(required.manifest),
          (argument254_0) => server.bootstrap(argument254_0)
        )
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))

        const error = yield* local.installBootstrap(page.manifest).pipe(expectedFailure)

        assert.strictEqual(error._tag, "StorageCorrupt")
        assert.strictEqual(yield* local.pendingCount, 2)
        assert.strictEqual(yield* local.cursor, 0)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument245_0) => Effect.scoped(argument245_0)
    ))

  it.effect("settles a durable legacy rejection before resubmitting pending work", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument256_0: LocalStore.Store, argument256_1: localLayer() },
          ({ argument256_0, argument256_1 }) => service(argument256_0, argument256_1)
        )
        const pending = yield* pipe(
          { argument257_0: Domain.PutTodo, argument257_1: Domain.todo("legacy-rejection") },
          ({ argument257_0, argument257_1 }) => local.mutate(argument257_0, argument257_1)
        )
        yield* pipe(
          Protocol.RejectedReceipt.make({
            spaceId,
            clientId,
            membershipIncarnation: pending.envelope.membershipIncarnation,
            mutationId: pending.envelope.mutationId,
            localSequence: pending.envelope.localSequence,
            ...putTodoProvenance,
            origin: "Legacy",
            rejection: "Rejected"
          }),
          (argument258_0) => local.persistReceipt(argument258_0)
        )
        const submissions = yield* Ref.make(0)
        const server = yield* pipe(
          { argument259_0: ServerStore.ServerStore, argument259_1: serverLayer() },
          ({ argument259_0, argument259_1 }) => service(argument259_0, argument259_1)
        )
        const remote = SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: () => Effect.die("unexpected discard"),
          submit: (submitted) => {
            const rejected = Protocol.RejectedReceipt.make({
              spaceId,
              clientId,
              membershipIncarnation: submitted.envelope.membershipIncarnation,
              mutationId: submitted.envelope.mutationId,
              localSequence: submitted.envelope.localSequence,
              ...putTodoProvenance,
              origin: "Legacy",
              terminalSequence: Identity.TerminalSequence.make(1),
              rejection: "Rejected"
            })
            return Ref.update(submissions, (count) => count + 1).pipe(Effect.as(rejected))
          },
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: server.watch
        })
        const reconciliation = yield* pipe({
          argument261_0: Reconciler.Reconciliation,
          argument261_1: Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
          )
        }, ({ argument261_0, argument261_1 }) => service(argument261_0, argument261_1))

        yield* reconciliation.sync

        assert.strictEqual(yield* Ref.get(submissions), 0)
        assert.strictEqual(yield* local.pendingCount, 0)
        pipe(
          Option.isNone(yield* local.get(Domain.Todo, "legacy-rejection")),
          (argument264_0) => assert.isTrue(argument264_0)
        )
      }),
      (argument255_0) => Effect.scoped(argument255_0)
    ))

  it.effect("rejects inconsistent durable replication scope metadata", () =>
    pipe(
      Effect.gen(function*() {
        const databaseContext = yield* pipe(database(), (argument266_0) => Layer.build(argument266_0))
        const sql = Context.get(databaseContext, SqlClient.SqlClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId, migration }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const services = pipe(
          {
            argument267_0: Layer.succeed(SqlClient.SqlClient, sql),
            argument267_1: pipe({
              argument268_0: Crypto.Crypto,
              argument268_1: Context.get(databaseContext, Crypto.Crypto)
            }, ({ argument268_0, argument268_1 }) => Layer.succeed(argument268_0, argument268_1)),
            argument267_2: pipe({
              argument269_0: Reactivity.Reactivity,
              argument269_1: Context.get(databaseContext, Reactivity.Reactivity)
            }, ({ argument269_0, argument269_1 }) => Layer.succeed(argument269_0, argument269_1)),
            argument267_3: pipe({
              argument270_0: QueryReactivity.QueryReactivity,
              argument270_1: Context.get(databaseContext, QueryReactivity.QueryReactivity)
            }, ({ argument270_0, argument270_1 }) => Layer.succeed(argument270_0, argument270_1))
          },
          ({ argument267_0, argument267_1, argument267_2, argument267_3 }) =>
            Layer.mergeAll(argument267_0, argument267_1, argument267_2, argument267_3)
        )
        const live = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(services)
        )

        const context = yield* Layer.build(live)
        const local = Context.get(context, LocalStore.Store)
        yield* sql`UPDATE effect_local_client_spaces SET desired_scope_digest = ${"0".repeat(64)}
        WHERE space_id = ${spaceId}`
        const error = yield* local.replicationState.pipe(expectedFailure)

        assert.strictEqual(error._tag, "StorageCorrupt")
      }),
      (argument265_0) => Effect.scoped(argument265_0)
    ))

  it.effect("commits optimistically, admits in total order, and reconciles canonical state", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument272_0: LocalStore.Store, argument272_1: localLayer() },
          ({ argument272_0, argument272_1 }) => service(argument272_0, argument272_1)
        )
        const server = yield* pipe(
          { argument273_0: ServerStore.ServerStore, argument273_1: serverLayer() },
          ({ argument273_0, argument273_1 }) => service(argument273_0, argument273_1)
        )

        const pending = yield* pipe(
          { argument274_0: Domain.PutTodo, argument274_1: Domain.todo("1") },
          ({ argument274_0, argument274_1 }) => local.mutate(argument274_0, argument274_1)
        )
        pipe(
          { argument275_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")), argument275_1: Domain.todo("1") },
          ({ argument275_0, argument275_1 }) => assert.deepStrictEqual(argument275_0, argument275_1)
        )
        assert.strictEqual(yield* local.pendingCount, 1)

        const receipt = yield* server.submit(pending.envelope)
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
        assert.strictEqual(receipt.serverSequence, 1)
        yield* local.applyReceipt(receipt)
        yield* installFreshView(local, server)

        assert.strictEqual(yield* local.pendingCount, 0)
        pipe(
          { argument276_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")), argument276_1: Domain.todo("1") },
          ({ argument276_0, argument276_1 }) => assert.deepStrictEqual(argument276_0, argument276_1)
        )
        assert.strictEqual(yield* local.cursor, 1)
        assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
      }),
      (argument271_0) => Effect.scoped(argument271_0)
    ))

  it.effect("rebases pending reads over an incrementally applied page", () =>
    pipe(
      Effect.gen(function*() {
        const clientDatabase = database()
        const live = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(live, clientDatabase), Layer.build)
        const local = Context.get(context, LocalStore.Store)
        const server = yield* pipe(
          { layer: serverLayer(), tag: ServerStore.ServerStore },
          ({ layer, tag }) => service(tag, layer)
        )

        yield* installFreshView(local, server)
        const base = yield* pipe(
          { mutation: Domain.PutTodo, payload: Domain.todo("base") },
          ({ mutation, payload }) => local.mutate(mutation, payload)
        )
        const baseReceipt = yield* server.submit(base.envelope)
        yield* local.applyReceipt(baseReceipt)
        const settledState = yield* local.replicationState
        const settledRequest = pullRequest(settledState.cursor)
        const settledResult = yield* server.pull(settledRequest)
        const settledPage = incremental(settledResult)
        yield* local.applyViewPage(settledPage)

        yield* local.mutate(Domain.IncrementTodo, { id: "base", delta: 1 })
        const optimistic = Option.getOrThrow(yield* local.get(Domain.Todo, "base"))
        assert.strictEqual(optimistic.count, 1)

        const remoteIncarnation = Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000099"
        )
        const remotePayload = { ...Domain.todo("base"), count: 5 }
        const remoteMutationId = Identity.MutationId.make("mut_00000000-0000-4000-8099-000000000001")
        const remote = yield* envelope(
          Domain.PutTodo.name,
          remotePayload,
          1,
          remoteMutationId,
          remoteIncarnation
        )
        yield* server.submit(remote)

        const state = yield* local.replicationState
        const request = pullRequest(state.cursor)
        const result = yield* server.pull(request)
        const page = incremental(result)
        yield* local.applyViewPage(page)

        const rebased = Option.getOrThrow(yield* local.get(Domain.Todo, "base"))
        assert.strictEqual(rebased.count, 6)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (effect) => Effect.scoped(effect)
    ))

  it.effect("publishes a complete projection after bounded replay", () =>
    pipe(
      Effect.gen(function*() {
        const actualSql = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
          Effect.provide(Reactivity.layer)
        )
        const statements: Array<{ readonly sql: string; readonly parameters: ReadonlyArray<unknown> }> = []
        const observedSql = new Proxy(actualSql, {
          apply: (target, thisArgument, argumentsList) => {
            if (Array.isArray(argumentsList[0])) {
              statements.push({
                sql: argumentsList[0].join("?").replace(/\s+/g, " ").trim(),
                parameters: argumentsList.slice(1)
              })
            }
            return Reflect.apply(target, thisArgument, argumentsList)
          }
        })
        const clientDatabase = pipe(
          {
            sqlLayer: Layer.succeed(SqlClient.SqlClient, observedSql),
            cryptoLayer: NodeCrypto.layer,
            reactivityLayer: Reactivity.layer,
            queryReactivityLayer: QueryReactivity.layer
          },
          ({ cryptoLayer, queryReactivityLayer, reactivityLayer, sqlLayer }) =>
            Layer.mergeAll(sqlLayer, cryptoLayer, reactivityLayer, queryReactivityLayer)
        )
        const live = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId,
          projectionReplayBatchSize: 1
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(live, clientDatabase), Layer.build)
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const server = yield* pipe(
          { layer: serverLayer(), tag: ServerStore.ServerStore },
          ({ layer, tag }) => service(tag, layer)
        )

        yield* installFreshView(local, server)

        const pending = yield* pipe(
          {
            ids: Array.from({ length: 12 }, (_, index) => `projection-${index + 1}`),
            mutate: (id: string) =>
              pipe(
                { mutation: Domain.PutTodo, payload: Domain.todo(id) },
                ({ mutation, payload }) => local.mutate(mutation, payload)
              )
          },
          ({ ids, mutate }) => Effect.forEach(ids, mutate)
        )
        const receipt = yield* server.submit(pending[0].envelope)
        const state = yield* local.replicationState
        const page = incremental(
          yield* pipe(pullRequest(state.cursor), (request) => server.pull(request))
        )

        yield* local.applyReceipt(receipt)
        statements.length = 0
        yield* local.applyViewPage(page)

        const identityReads = statements.filter(({ sql: query }) =>
          query.startsWith("SELECT local_sequence, changes_json FROM effect_local_client_pending_data")
        )
        const fullReads = statements.filter(({ parameters, sql: query }) =>
          query.startsWith("SELECT p.membership_incarnation") && query.includes("ORDER BY p.local_sequence LIMIT") &&
          parameters.at(-1) === 1
        )
        assert.isAtLeast(identityReads.length, 13)
        assert.isAtLeast(fullReads.length, 13)
        pipe(
          identityReads.every(({ parameters }) => parameters.at(-1) === 1),
          (allReadsAreBounded) => assert.isTrue(allReadsAreBounded)
        )

        const projection = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({
            active: Schema.Int,
            replay: Schema.NullOr(Schema.Int),
            active_rows: Schema.Int,
            inactive_rows: Schema.Int
          }),
          execute: () =>
            sql`SELECT
          s.active_projection_generation AS active,
          s.projection_replay_generation AS replay,
          SUM(CASE WHEN e.projection_generation = s.active_projection_generation THEN 1 ELSE 0 END) AS active_rows,
          SUM(CASE WHEN e.projection_generation <> s.active_projection_generation THEN 1 ELSE 0 END) AS inactive_rows
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_visible_entities_data AS e ON e.space_id = s.space_id
          WHERE s.space_id = ${spaceId}`
        })(undefined)
        assert.isNull(projection.replay)
        assert.strictEqual(projection.active_rows, 12)
        assert.strictEqual(projection.inactive_rows, 0)
        pipe(
          {
            actual: Option.getOrThrow(yield* local.get(Domain.Todo, "projection-12")),
            expected: Domain.todo("projection-12")
          },
          ({ actual, expected }) => assert.deepStrictEqual(actual, expected)
        )
      }),
      (argument277_0) => Effect.scoped(argument277_0)
    ))

  it.effect("rejects a first submission whose digest does not match its envelope", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument285_0: LocalStore.Store, argument285_1: localLayer() },
          ({ argument285_0, argument285_1 }) => service(argument285_0, argument285_1)
        )
        const server = yield* pipe(
          { argument286_0: ServerStore.ServerStore, argument286_1: serverLayer() },
          ({ argument286_0, argument286_1 }) => service(argument286_0, argument286_1)
        )
        const pending = yield* pipe(
          { argument287_0: Domain.PutTodo, argument287_1: Domain.todo("1") },
          ({ argument287_0, argument287_1 }) => local.mutate(argument287_0, argument287_1)
        )
        const changed = { ...pending.envelope, payload: Domain.todo("1", "tampered") }
        const exit = yield* server.submit(changed).pipe(Effect.exit)
        assert.isTrue(exit._tag === "Failure")
        if (exit._tag === "Failure") {
          const failure = Cause.findErrorOption(exit.cause)
          assert.strictEqual(failure._tag, "Some")
          if (failure._tag === "Some") assert.strictEqual(failure.value._tag, "MutationIdentityConflict")
        }
      }),
      (argument284_0) => Effect.scoped(argument284_0)
    ))

  it.effect("returns the same terminal receipt for an exact retry", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument289_0: LocalStore.Store, argument289_1: localLayer() },
          ({ argument289_0, argument289_1 }) => service(argument289_0, argument289_1)
        )
        const server = yield* pipe(
          { argument290_0: ServerStore.ServerStore, argument290_1: serverLayer() },
          ({ argument290_0, argument290_1 }) => service(argument290_0, argument290_1)
        )
        const pending = yield* pipe(
          { argument291_0: Domain.PutTodo, argument291_1: Domain.todo("1") },
          ({ argument291_0, argument291_1 }) => local.mutate(argument291_0, argument291_1)
        )
        const first = yield* server.submit(pending.envelope)
        const retry = yield* server.submit(pending.envelope)
        assert.deepStrictEqual(retry, first)
        assert.strictEqual(first._tag, "Accepted")
        if (first._tag === "Accepted") assert.strictEqual(first.serverSequence, 1)
      }),
      (argument288_0) => Effect.scoped(argument288_0)
    ))

  it.effect("republishes an accepted retry from its durable authoritative entry", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument293_0: LocalStore.Store, argument293_1: localLayer() },
          ({ argument293_0, argument293_1 }) => service(argument293_0, argument293_1)
        )
        const server = yield* pipe(
          { argument294_0: ServerStore.ServerStore, argument294_1: serverLayer() },
          ({ argument294_0, argument294_1 }) => service(argument294_0, argument294_1)
        )
        const wakes = yield* Queue.unbounded<Protocol.Wake>()
        const watcher = yield* pipe(watchRequest(), (argument295_0) => server.watch(argument295_0)).pipe(
          Stream.runForEach((wake) => Queue.offer(wakes, wake)),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(wakes)
        const pending = yield* pipe(
          { argument296_0: Domain.PutTodo, argument296_1: Domain.todo("retry-wake") },
          ({ argument296_0, argument296_1 }) => local.mutate(argument296_0, argument296_1)
        )
        const first = yield* server.submit(pending.envelope)
        assert.strictEqual(first._tag, "Accepted")
        yield* Queue.take(wakes)

        assert.deepStrictEqual(yield* server.submit(pending.envelope), first)
        pipe(
          { argument297_0: yield* Queue.take(wakes), argument297_1: Protocol.Wake.make({ spaceId }) },
          ({ argument297_0, argument297_1 }) => assert.deepStrictEqual(argument297_0, argument297_1)
        )
        yield* Fiber.interrupt(watcher)
      }),
      (argument292_0) => Effect.scoped(argument292_0)
    ))

  it.effect("reauthorizes an exact retry before returning its durable receipt", () =>
    pipe(
      Effect.gen(function*() {
        const access = yield* Ref.make(true)
        const secured = ServerStore.layer({
          ...serverHistory,
          definition: Domain.definition,
          authorizeAccess: () =>
            Ref.get(access).pipe(
              Effect.flatMap((allowed) => {
                if (allowed) return Effect.void
                return Effect.fail(new TestAuthorizationError({ reason: "revoked" }))
              })
            ),
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const server = yield* service(ServerStore.ServerStore, secured)
        const submitted = yield* pipe(
          {
            argument300_0: Domain.PutTodo.name,
            argument300_1: Domain.todo("1"),
            argument300_2: 1,
            argument300_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000041")
          },
          ({ argument300_0, argument300_1, argument300_2, argument300_3 }) =>
            envelope(argument300_0, argument300_1, argument300_2, argument300_3)
        )
        const request = Protocol.SubmitRequest.make({ envelope: submitted, schema: Domain.definition.schemaIdentity })
        assert.strictEqual((yield* server.admit(request, { subject: "test" }))._tag, "Accepted")
        yield* Ref.set(access, false)

        const error = yield* server.admit(request, { subject: "test" }).pipe(Effect.flip)
        assert.strictEqual(error._tag, "AuthorizationDenied")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument298_0) => Effect.scoped(argument298_0)
    ))

  it.effect("keeps mutation payloads and private results out of the authoritative log", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument302_0: LocalStore.Store, argument302_1: localLayer() },
          ({ argument302_0, argument302_1 }) => service(argument302_0, argument302_1)
        )
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument303_0) => Layer.build(argument303_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* pipe(
          { argument304_0: Domain.PutTodo, argument304_1: Domain.todo("private", "secret") },
          ({ argument304_0, argument304_1 }) => local.mutate(argument304_0, argument304_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        assert.strictEqual(receipt._tag, "Accepted")

        const entry = (yield* authoritativeLog(sql))[0]
        pipe(Object.hasOwn(entry, "envelope"), (argument305_0) => assert.isFalse(argument305_0))
        pipe(Object.hasOwn(entry, "result"), (argument306_0) => assert.isFalse(argument306_0))
      }),
      (argument301_0) => Effect.scoped(argument301_0)
    ))

  it.effect("pulls authoritative entities without materializing private receipt payloads", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument308_0) => Layer.build(argument308_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* pipe({
          argument309_0: Domain.PutTodo.name,
          argument309_1: Domain.todo("public-with-private-receipt"),
          argument309_2: 1,
          argument309_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000029")
        }, ({ argument309_0, argument309_1, argument309_2, argument309_3 }) =>
          envelope(argument309_0, argument309_1, argument309_2, argument309_3))
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts
      SET receipt_json = ${"x".repeat(Protocol.maximumReceiptBytes)}
      WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const entries = yield* authoritativeLog(sql)
        pipe({
          argument310_0: entries.map((entry) =>
            entry.mutationId
          ),
          argument310_1: [submitted.mutationId]
        }, ({ argument310_0, argument310_1 }) => assert.deepStrictEqual(argument310_0, argument310_1))
        const required = yield* pipe(pullRequest(), (argument311_0) => server.pull(argument311_0))
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        const page = yield* pipe(
          bootstrapRequest(required.manifest),
          (argument312_0) => server.bootstrap(argument312_0)
        )
        pipe({
          argument313_0: page.entries.map((entry) => entry.change.entity.key),
          argument313_1: ["public-with-private-receipt"]
        }, ({ argument313_0, argument313_1 }) => assert.deepStrictEqual(argument313_0, argument313_1))
        assert.strictEqual((yield* server.submit(submitted).pipe(Effect.flip))._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument307_0) => Effect.scoped(argument307_0)
    ))

  it.effect("recovers an accepted commit whose private receipt was lost", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument315_0: LocalStore.Store, argument315_1: localLayer() },
          ({ argument315_0, argument315_1 }) => service(argument315_0, argument315_1)
        )
        const server = yield* pipe(
          { argument316_0: ServerStore.ServerStore, argument316_1: serverLayer() },
          ({ argument316_0, argument316_1 }) => service(argument316_0, argument316_1)
        )
        const pending = yield* pipe(
          { argument317_0: Domain.PutTodo, argument317_1: Domain.todo("1") },
          ({ argument317_0, argument317_1 }) => local.mutate(argument317_0, argument317_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        assert.strictEqual(receipt._tag, "Accepted")

        yield* installFreshView(local, server)
        assert.strictEqual(yield* local.pendingCount, 1)
        pipe(
          Option.isNone(yield* local.receipt(pending.envelope.mutationId)),
          (argument318_0) => assert.isTrue(argument318_0)
        )
        pipe(
          { argument319_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")), argument319_1: Domain.todo("1") },
          ({ argument319_0, argument319_1 }) => assert.deepStrictEqual(argument319_0, argument319_1)
        )

        yield* local.applyReceipt(receipt)
        assert.strictEqual(yield* local.pendingCount, 0)
        assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
      }),
      (argument314_0) => Effect.scoped(argument314_0)
    ))

  it.effect("stores matching authoritative entry and SQL identities", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const serverLayerWithDatabase = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition })
          .pipe(
            Layer.provide(runtime),
            Layer.provide(serverDatabase)
          )
        const context = yield* pipe(
          Layer.merge(serverLayerWithDatabase, serverDatabase),
          (argument321_0) => Layer.build(argument321_0)
        )
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* pipe(
          {
            argument322_0: Domain.PutTodo.name,
            argument322_1: Domain.todo("1"),
            argument322_2: 1,
            argument322_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000031")
          },
          ({ argument322_0, argument322_1, argument322_2, argument322_3 }) =>
            envelope(argument322_0, argument322_1, argument322_2, argument322_3)
        )
        yield* server.submit(submitted)
        const row = (yield* authoritativeRows(sql))[0]
        const entry = yield* pipe(
          Schema.fromJsonString(Protocol.AcceptedMutation),
          (argument323_0) => Schema.decodeUnknownEffect(argument323_0)
        )(row.entry_json)
        assert.strictEqual(row.space_id, entry.spaceId)
        assert.strictEqual(row.server_sequence, entry.sequence)
        assert.strictEqual(row.client_id, entry.clientId)
        assert.strictEqual(row.local_sequence, entry.localSequence)
        assert.strictEqual(row.mutation_id, entry.mutationId)
        assert.strictEqual(row.digest, entry.digest)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument320_0) => Effect.scoped(argument320_0)
    ))

  it.effect("stores a terminal rejection without advancing the accepted cursor", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument325_0: LocalStore.Store, argument325_1: localLayer() },
          ({ argument325_0, argument325_1 }) => service(argument325_0, argument325_1)
        )
        const server = yield* pipe({
          argument326_0: ServerStore.ServerStore,
          argument326_1: serverLayer(() => Effect.fail(new TestAuthorizationError({ reason: "denied" })))
        }, ({ argument326_0, argument326_1 }) => service(argument326_0, argument326_1))
        const pending = yield* pipe(
          { argument327_0: Domain.PutTodo, argument327_1: Domain.todo("1") },
          ({ argument327_0, argument327_1 }) => local.mutate(argument327_0, argument327_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        assert.strictEqual(receipt._tag, "Rejected")
        yield* local.applyReceipt(receipt)
        assert.strictEqual(yield* local.pendingCount, 0)
        pipe(Option.isNone(yield* local.get(Domain.Todo, "1")), (argument328_0) => assert.isTrue(argument328_0))
        assert.strictEqual(yield* local.cursor, 0)
      }),
      (argument324_0) => Effect.scoped(argument324_0)
    ))

  it.effect("rolls back server writes performed before a typed rejection", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe(
          { argument330_0: ServerStore.ServerStore, argument330_1: serverLayer() },
          ({ argument330_0, argument330_1 }) => service(argument330_0, argument330_1)
        )
        const rejected = yield* pipe(
          {
            argument331_0: Domain.RejectAfterWrite.name,
            argument331_1: Domain.todo("poison"),
            argument331_2: 1,
            argument331_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000011")
          },
          ({ argument331_0, argument331_1, argument331_2, argument331_3 }) =>
            envelope(argument331_0, argument331_1, argument331_2, argument331_3)
        )
        assert.strictEqual((yield* server.submit(rejected))._tag, "Rejected")

        const increment = yield* pipe(
          {
            argument332_0: Domain.IncrementTodo.name,
            argument332_1: { id: "poison", delta: 1 },
            argument332_2: 2,
            argument332_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000012")
          },
          ({ argument332_0, argument332_1, argument332_2, argument332_3 }) =>
            envelope(argument332_0, argument332_1, argument332_2, argument332_3)
        )
        const receipt = yield* server.submit(increment)
        assert.strictEqual(receipt._tag, "Rejected")
        if (receipt._tag === "Rejected") assert.strictEqual(receipt.rejection, "TodoNotFound")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument329_0) => Effect.scoped(argument329_0)
    ))

  it.effect("terminally rejects an accepted entry that cannot fit in a pull page", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe(
          { argument334_0: ServerStore.ServerStore, argument334_1: serverLayer() },
          ({ argument334_0, argument334_1 }) => service(argument334_0, argument334_1)
        )
        const oversized = yield* pipe(
          {
            argument335_0: Domain.PutHugeTodo.name,
            argument335_1: { id: "huge" },
            argument335_2: 1,
            argument335_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000021")
          },
          ({ argument335_0, argument335_1, argument335_2, argument335_3 }) =>
            envelope(argument335_0, argument335_1, argument335_2, argument335_3)
        )
        assert.strictEqual((yield* server.submit(oversized))._tag, "Rejected")

        const next = yield* pipe(
          {
            argument336_0: Domain.PutTodo.name,
            argument336_1: Domain.todo("next"),
            argument336_2: 2,
            argument336_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000022")
          },
          ({ argument336_0, argument336_1, argument336_2, argument336_3 }) =>
            envelope(argument336_0, argument336_1, argument336_2, argument336_3)
        )
        const receipt = yield* server.submit(next)
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument333_0) => Effect.scoped(argument333_0)
    ))

  it.effect("stores a bounded terminal receipt when a private result exceeds the RPC response limit", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe(
          { argument338_0: ServerStore.ServerStore, argument338_1: serverLayer() },
          ({ argument338_0, argument338_1 }) => service(argument338_0, argument338_1)
        )
        const oversized = yield* pipe({
          argument339_0: Domain.ReturnHugeResult.name,
          argument339_1: null,
          argument339_2: 1,
          argument339_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000023")
        }, ({ argument339_0, argument339_1, argument339_2, argument339_3 }) =>
          envelope(argument339_0, argument339_1, argument339_2, argument339_3))
        const first = yield* server.submit(oversized)
        const retry = yield* server.submit(oversized)

        assert.strictEqual(first._tag, "Rejected")
        assert.deepStrictEqual(retry, first)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(first), Protocol.maximumReceiptBytes)
        if (first._tag === "Rejected") {
          assert.deepStrictEqual(first.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        const next = yield* pipe({
          argument340_0: Domain.PutTodo.name,
          argument340_1: Domain.todo("after-oversized-result"),
          argument340_2: 2,
          argument340_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000030")
        }, ({ argument340_0, argument340_1, argument340_2, argument340_3 }) =>
          envelope(argument340_0, argument340_1, argument340_2, argument340_3))
        const accepted = yield* server.submit(next)
        assert.strictEqual(accepted._tag, "Accepted")
        if (accepted._tag === "Accepted") {
          assert.strictEqual(accepted.serverSequence, 1)
        }
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument337_0) =>
        Effect.scoped(argument337_0)
    ))

  it.effect("bounds an oversized authorization rejection before storing its terminal receipt", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument342_0: ServerStore.ServerStore,
          argument342_1: serverLayer(() =>
            pipe(
              "x".repeat(Protocol.maximumReceiptBytes),
              (argument343_0) => Effect.fail(new TestAuthorizationError({ reason: argument343_0 }))
            )
          )
        }, ({ argument342_0, argument342_1 }) => service(argument342_0, argument342_1))
        const submitted = yield* pipe({
          argument344_0: Domain.PutTodo.name,
          argument344_1: Domain.todo("oversized-authorization"),
          argument344_2: 1,
          argument344_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        }, ({ argument344_0, argument344_1, argument344_2, argument344_3 }) =>
          envelope(argument344_0, argument344_1, argument344_2, argument344_3))
        const receipt = yield* server.submit(submitted)

        assert.strictEqual(receipt._tag, "Rejected")
        assert.isAtMost(yield* Protocol.encodedBytesEffect(receipt), Protocol.maximumReceiptBytes)
        if (receipt._tag === "Rejected") {
          assert.deepStrictEqual(receipt.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        assert.deepStrictEqual(yield* server.submit(submitted), receipt)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument341_0) =>
        Effect.scoped(argument341_0)
    ))

  it.effect("assigns dense authoritative log sequences", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument346_0) => Layer.build(argument346_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        yield* server.submit(
          yield* pipe(
            {
              argument347_0: Domain.PutTodo.name,
              argument347_1: Domain.todo("gap-1"),
              argument347_2: 1,
              argument347_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000024")
            },
            ({ argument347_0, argument347_1, argument347_2, argument347_3 }) =>
              envelope(argument347_0, argument347_1, argument347_2, argument347_3)
          )
        )
        yield* server.submit(
          yield* pipe(
            {
              argument348_0: Domain.PutTodo.name,
              argument348_1: Domain.todo("gap-2"),
              argument348_2: 2,
              argument348_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000025")
            },
            ({ argument348_0, argument348_1, argument348_2, argument348_3 }) =>
              envelope(argument348_0, argument348_1, argument348_2, argument348_3)
          )
        )
        const rows = yield* authoritativeRows(sql)
        pipe(
          { argument349_0: rows.map((row) => row.server_sequence), argument349_1: [1, 2] },
          ({ argument349_0, argument349_1 }) => assert.deepStrictEqual(argument349_0, argument349_1)
        )
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument345_0) => Effect.scoped(argument345_0)
    ))

  it.effect("rejects an exact retry whose durable receipt conflicts with its SQL identity", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument351_0) => Layer.build(argument351_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* pipe({
          argument352_0: Domain.PutTodo.name,
          argument352_1: Domain.todo("retry-corrupt"),
          argument352_2: 1,
          argument352_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000026")
        }, ({ argument352_0, argument352_1, argument352_2, argument352_3 }) =>
          envelope(argument352_0, argument352_1, argument352_2, argument352_3))
        yield* server.submit(submitted)
        const conflictingMutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000027")
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Rejected",
            spaceId,
            clientId,
            mutationId: conflictingMutationId,
            localSequence: submitted.localSequence,
            rejection: "corrupt"
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument350_0) =>
        Effect.scoped(argument350_0)
    ))

  it.effect("rejects an accepted retry whose receipt sequence conflicts with the authoritative log", () =>
    pipe(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* pipe(Layer.merge(live, serverDatabase), (argument354_0) => Layer.build(argument354_0))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* pipe({
          argument355_0: Domain.PutTodo.name,
          argument355_1: Domain.todo("retry-sequence-corrupt"),
          argument355_2: 1,
          argument355_3: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        }, ({ argument355_0, argument355_1, argument355_2, argument355_3 }) =>
          envelope(argument355_0, argument355_1, argument355_2, argument355_3))
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Accepted",
            spaceId,
            clientId,
            mutationId: submitted.mutationId,
            localSequence: submitted.localSequence,
            serverSequence: 2,
            result: Domain.todo("retry-sequence-corrupt")
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument353_0) =>
        Effect.scoped(argument353_0)
    ))

  it.effect("rejects a pending row whose durable digest does not match its reconstructed identity", () =>
    pipe(
      Effect.gen(function*() {
        const clientDatabase = database()
        const live = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(live, clientDatabase), (argument357_0) => Layer.build(argument357_0))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* pipe(
          { argument358_0: Domain.PutTodo, argument358_1: Domain.todo("digest-corrupt") },
          ({ argument358_0, argument358_1 }) => local.mutate(argument358_0, argument358_1)
        )
        yield* sql`UPDATE effect_local_client_pending_data SET digest = ${"0".repeat(64)}
        WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`

        const error = yield* local.pending.pipe(expectedFailure)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument356_0) => Effect.scoped(argument356_0)
    ))

  it.effect("persists each submitted receipt before submitting the next pending mutation", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument360_0: LocalStore.Store, argument360_1: localLayer() },
          ({ argument360_0, argument360_1 }) => service(argument360_0, argument360_1)
        )
        const first = yield* pipe(
          { argument361_0: Domain.PutTodo, argument361_1: Domain.todo("stream-1") },
          ({ argument361_0, argument361_1 }) => local.mutate(argument361_0, argument361_1)
        )
        const second = yield* pipe(
          { argument362_0: Domain.PutTodo, argument362_1: Domain.todo("stream-2") },
          ({ argument362_0, argument362_1 }) => local.mutate(argument362_0, argument362_1)
        )
        let submissions = 0
        const server = yield* pipe(
          { argument363_0: ServerStore.ServerStore, argument363_1: serverLayer() },
          ({ argument363_0, argument363_1 }) => service(argument363_0, argument363_1)
        )
        const remote = SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: () => Effect.die("unexpected discard"),
          submit: ({ envelope: submitted }) =>
            Effect.gen(function*() {
              submissions++
              if (submissions === 2) {
                pipe(
                  Option.isSome(yield* local.receipt(first.envelope.mutationId)),
                  (argument364_0) => assert.isTrue(argument364_0)
                )
              }
              return Protocol.RejectedReceipt.make({
                ...putTodoProvenance,
                spaceId,
                clientId,
                membershipIncarnation: submitted.membershipIncarnation,
                mutationId: submitted.mutationId,
                localSequence: submitted.localSequence,
                origin: "Authorization",
                rejection: "denied"
              })
            }),
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: server.watch
        })
        const reconciler = yield* pipe({
          argument365_0: Reconciler.Reconciler,
          argument365_1: Reconciler.layer({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
          )
        }, ({ argument365_0, argument365_1 }) => service(argument365_0, argument365_1))

        yield* reconciler.sync
        assert.strictEqual(submissions, 2)
        pipe(
          Option.isSome(yield* local.receipt(first.envelope.mutationId)),
          (argument368_0) => assert.isTrue(argument368_0)
        )
        pipe(
          Option.isSome(yield* local.receipt(second.envelope.mutationId)),
          (argument369_0) => assert.isTrue(argument369_0)
        )
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument359_0) => Effect.scoped(argument359_0)
    ))

  it.effect("invalidates the receipt dependency when a terminal receipt is stored", () =>
    pipe(
      Effect.gen(function*() {
        const clientDatabase = database()
        const local = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* pipe(Layer.merge(local, clientDatabase), (argument371_0) => Layer.build(argument371_0))
        const store = Context.get(context, LocalStore.Store)
        const reactivity = Context.get(context, Reactivity.Reactivity)
        const pending = yield* pipe(
          { argument372_0: Domain.PutTodo, argument372_1: Domain.todo("1") },
          ({ argument372_0, argument372_1 }) => store.mutate(argument372_0, argument372_1)
        )
        let invalidations = 0
        const cancel = reactivity.registerUnsafe(
          [`effect-local:space:${spaceId}:receipt:${pending.envelope.mutationId}`],
          () => invalidations++
        )
        yield* Effect.addFinalizer(() => Effect.sync(cancel))
        yield* store.applyReceipt({
          _tag: "Rejected",
          ...putTodoProvenance,
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          origin: "Authorization",
          rejection: "denied"
        })
        assert.strictEqual(invalidations, 1)
      }),
      (argument370_0) => Effect.scoped(argument370_0)
    ))

  it.effect("uses explicit field semantics without metadata on ordinary model values", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument374_0: LocalStore.Store, argument374_1: localLayer() },
          ({ argument374_0, argument374_1 }) => service(argument374_0, argument374_1)
        )
        yield* pipe(
          { argument375_0: Domain.PutTodo, argument375_1: Domain.todo("1") },
          ({ argument375_0, argument375_1 }) => local.mutate(argument375_0, argument375_1)
        )
        yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
        yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
        yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
        const value = Option.getOrThrow(yield* local.get(Domain.Todo, "1"))
        assert.deepStrictEqual(value, { id: "1", title: "first", count: 2, labels: ["a"] })
        pipe(
          { argument376_0: Object.keys(value).some((key) => key.startsWith("$")), argument376_1: false },
          ({ argument376_0, argument376_1 }) => assert.strictEqual(argument376_0, argument376_1)
        )
      }),
      (argument373_0) => Effect.scoped(argument373_0)
    ))

  it.effect("canonicalizes the exact identity covered by the digest", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument378_0: LocalStore.Store, argument378_1: localLayer() },
          ({ argument378_0, argument378_1 }) => service(argument378_0, argument378_1)
        )
        const pending = yield* pipe(
          { argument379_0: Domain.PutTodo, argument379_1: Domain.todo("1") },
          ({ argument379_0, argument379_1 }) => local.mutate(argument379_0, argument379_1)
        )
        const { digest, ...identity } = pending.envelope
        assert.strictEqual(yield* Protocol.mutationDigest(identity), digest)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument377_0) => Effect.scoped(argument377_0)
    ))

  it.effect("refuses a mutation envelope beyond the configured protocol bound", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument381_0: LocalStore.Store, argument381_1: localLayer() },
          ({ argument381_0, argument381_1 }) => service(argument381_0, argument381_1)
        )
        const error = yield* pipe({
          argument382_0: Domain.PutTodo,
          argument382_1: pipe(
            { argument383_0: "1", argument383_1: "x".repeat(Protocol.maximumMutationBytes) },
            ({ argument383_0, argument383_1 }) => Domain.todo(argument383_0, argument383_1)
          )
        }, ({ argument382_0, argument382_1 }) => local.mutate(argument382_0, argument382_1)).pipe(expectedFailure)
        assert.strictEqual(error._tag, "CapacityExceeded")
        assert.strictEqual(yield* local.pendingCount, 0)
      }),
      (argument380_0) => Effect.scoped(argument380_0)
    ))

  it.effect("rejects invalid local and reconciliation configuration during layer construction", () =>
    pipe(
      Effect.gen(function*() {
        const localError = yield* pipe({
          argument385_0: LocalStore.Store,
          argument385_1: LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId,
            maximumPendingMutations: 0
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument385_0, argument385_1 }) => service(argument385_0, argument385_1)).pipe(expectedFailure)
        assert.strictEqual(localError._tag, "InvalidConfiguration")
        if (localError._tag === "InvalidConfiguration") {
          assert.strictEqual(localError.option, "maximumPendingMutations")
        }

        const wakeCapacityError = yield* pipe({
          argument387_0: ServerStore.ServerStore,
          argument387_1: ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition, wakeCapacity: 0 })
            .pipe(
              Layer.provide(runtime),
              Layer.provide(database())
            )
        }, ({ argument387_0, argument387_1 }) => service(argument387_0, argument387_1)).pipe(expectedFailure)
        assert.strictEqual(wakeCapacityError._tag, "InvalidConfiguration")
        if (wakeCapacityError._tag === "InvalidConfiguration") {
          assert.strictEqual(wakeCapacityError.option, "wakeCapacity")
        }

        const pendingReadAuthorizationError = yield* pipe({
          argument389_0: ServerStore.ServerStore,
          argument389_1: ServerStore.layerTrusted({
            ...serverHistory,
            definition: Domain.definition,
            maximumPendingReadAuthorizations: 0
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        }, ({ argument389_0, argument389_1 }) => service(argument389_0, argument389_1)).pipe(expectedFailure)
        assert.strictEqual(pendingReadAuthorizationError._tag, "InvalidConfiguration")
        if (pendingReadAuthorizationError._tag === "InvalidConfiguration") {
          assert.strictEqual(pendingReadAuthorizationError.option, "maximumPendingReadAuthorizations")
        }

        const server = yield* pipe(
          { argument391_0: ServerStore.ServerStore, argument391_1: serverLayer() },
          ({ argument391_0, argument391_1 }) => service(argument391_0, argument391_1)
        )
        const invalidPageSize = Reconciler.layer({ definition: Domain.definition, spaceId, pageSize: 0 }).pipe(
          Layer.provide(localLayer()),
          Layer.provide(directSync(server))
        )
        const pageSizeError = yield* service(Reconciler.Reconciler, invalidPageSize).pipe(expectedFailure)
        assert.strictEqual(pageSizeError._tag, "InvalidConfiguration")
        if (pageSizeError._tag === "InvalidConfiguration") assert.strictEqual(pageSizeError.option, "pageSize")

        const invalidRetryDelay = Reconciler.layer({
          definition: Domain.definition,
          spaceId,
          retryDelay: "0 millis"
        }).pipe(
          Layer.provide(localLayer()),
          Layer.provide(directSync(server))
        )
        const retryDelayError = yield* service(Reconciler.Reconciler, invalidRetryDelay).pipe(expectedFailure)
        assert.strictEqual(retryDelayError._tag, "InvalidConfiguration")
        if (retryDelayError._tag === "InvalidConfiguration") assert.strictEqual(retryDelayError.option, "retryDelay")
      }),
      (argument384_0) => Effect.scoped(argument384_0)
    ))

  it.effect("rolls a rejected optimistic mutation back and replays later pending work", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument397_0: ServerStore.ServerStore,
          argument397_1: serverLayer(({ mutation }) => {
            if (mutation.name === Domain.RenameTodo.name) {
              return Effect.fail(new TestAuthorizationError({ reason: "denied" }))
            }
            return Effect.void
          })
        }, ({ argument397_0, argument397_1 }) => service(argument397_0, argument397_1))
        const services = yield* pipe(clientServices(clientId, server), (argument398_0) => Layer.build(argument398_0))
        const local = Context.get(services, LocalStore.Store)
        const reconciler = Context.get(services, Reconciler.Reconciler)

        yield* pipe(
          { argument399_0: Domain.PutTodo, argument399_1: Domain.todo("1") },
          ({ argument399_0, argument399_1 }) => local.mutate(argument399_0, argument399_1)
        )
        yield* reconciler.sync
        const rename = yield* local.mutate(Domain.RenameTodo, { id: "1", title: "optimistic" })
        yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })

        yield* reconciler.sync
        pipe({
          argument400_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")),
          argument400_1: {
            id: "1",
            title: "first",
            count: 2,
            labels: []
          }
        }, ({ argument400_0, argument400_1 }) => assert.deepStrictEqual(argument400_0, argument400_1))
        assert.strictEqual(Option.getOrThrow(yield* local.receipt(rename.envelope.mutationId))._tag, "Rejected")
        assert.strictEqual(yield* local.pendingCount, 0)
        assert.strictEqual(yield* local.cursor, 2)
      }),
      (argument396_0) => Effect.scoped(argument396_0)
    ))

  it.effect("rejects a client sequence gap without consuming server order", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument402_0: LocalStore.Store, argument402_1: localLayer() },
          ({ argument402_0, argument402_1 }) => service(argument402_0, argument402_1)
        )
        const server = yield* pipe(
          { argument403_0: ServerStore.ServerStore, argument403_1: serverLayer() },
          ({ argument403_0, argument403_1 }) => service(argument403_0, argument403_1)
        )
        const first = yield* pipe(
          { argument404_0: Domain.PutTodo, argument404_1: Domain.todo("1") },
          ({ argument404_0, argument404_1 }) => local.mutate(argument404_0, argument404_1)
        )
        const second = yield* pipe(
          { argument405_0: Domain.PutTodo, argument405_1: Domain.todo("2") },
          ({ argument405_0, argument405_1 }) => local.mutate(argument405_0, argument405_1)
        )
        const error = yield* server.submit(second.envelope).pipe(Effect.flip)
        assert.strictEqual(error._tag, "OutOfOrderMutation")
        if (error._tag === "OutOfOrderMutation") assert.strictEqual(error.expected, 1)
        const receipt = yield* server.submit(first.envelope)
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
      }),
      (argument401_0) => Effect.scoped(argument401_0)
    ))

  it.effect("deduplicates overlapping catch up pages", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument407_0: LocalStore.Store, argument407_1: localLayer() },
          ({ argument407_0, argument407_1 }) => service(argument407_0, argument407_1)
        )
        const server = yield* pipe(
          { argument408_0: ServerStore.ServerStore, argument408_1: serverLayer() },
          ({ argument408_0, argument408_1 }) => service(argument408_0, argument408_1)
        )
        const first = yield* pipe(
          { argument409_0: Domain.PutTodo, argument409_1: Domain.todo("1") },
          ({ argument409_0, argument409_1 }) => local.mutate(argument409_0, argument409_1)
        )
        const receipt = yield* server.submit(first.envelope)
        if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
        const entry = acceptedMutation(first, receipt)
        yield* local.applyEntries([entry, entry])
        assert.strictEqual(yield* local.cursor, 1)
        pipe(
          { argument410_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")), argument410_1: Domain.todo("1") },
          ({ argument410_0, argument410_1 }) => assert.deepStrictEqual(argument410_0, argument410_1)
        )
      }),
      (argument406_0) => Effect.scoped(argument406_0)
    ))

  it.effect("rejects a conflicting duplicate catch up entry", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument412_0: LocalStore.Store, argument412_1: localLayer() },
          ({ argument412_0, argument412_1 }) => service(argument412_0, argument412_1)
        )
        const server = yield* pipe(
          { argument413_0: ServerStore.ServerStore, argument413_1: serverLayer() },
          ({ argument413_0, argument413_1 }) => service(argument413_0, argument413_1)
        )
        const pending = yield* pipe(
          { argument414_0: Domain.PutTodo, argument414_1: Domain.todo("1") },
          ({ argument414_0, argument414_1 }) => local.mutate(argument414_0, argument414_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
        const entry = acceptedMutation(pending, receipt)
        yield* local.applyEntries([entry])

        const error = yield* local.applyEntries([{ ...entry, changes: [] }]).pipe(expectedFailure)
        assert.strictEqual(error._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.cursor, 1)
      }),
      (argument411_0) => Effect.scoped(argument411_0)
    ))

  it.effect("does not settle pending work from a conflicting own-client entry", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument416_0: LocalStore.Store, argument416_1: localLayer() },
          ({ argument416_0, argument416_1 }) => service(argument416_0, argument416_1)
        )
        const server = yield* pipe(
          { argument417_0: ServerStore.ServerStore, argument417_1: serverLayer() },
          ({ argument417_0, argument417_1 }) => service(argument417_0, argument417_1)
        )
        const pending = yield* pipe(
          { argument418_0: Domain.PutTodo, argument418_1: Domain.todo("1") },
          ({ argument418_0, argument418_1 }) => local.mutate(argument418_0, argument418_1)
        )
        const receipt = yield* server.submit(pending.envelope)
        if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
        const entry = acceptedMutation(pending, receipt)
        const error = yield* local.applyEntries([{
          ...entry,
          digest: "0".repeat(64)
        }]).pipe(expectedFailure)

        assert.strictEqual(error._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.pendingCount, 1)
        assert.strictEqual(yield* local.cursor, 0)
      }),
      (argument415_0) => Effect.scoped(argument415_0)
    ))

  it.effect("does not resubscribe after a permanent watch failure", () =>
    pipe(
      Effect.gen(function*() {
        const subscriptions = yield* Ref.make(0)
        const firstSubscribed = yield* Latch.make()
        const server = yield* pipe(
          { argument420_0: ServerStore.ServerStore, argument420_1: serverLayer() },
          ({ argument420_0, argument420_1 }) => service(argument420_0, argument420_1)
        )
        const remote = pipe({
          argument421_0: SyncEngine.SyncEngine,
          argument421_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: server.pull,
            bootstrap: server.bootstrap,
            watch: () =>
              pipe(
                Ref.modify(subscriptions, (count) => [count, count + 1]).pipe(
                  Effect.flatMap((count) => {
                    if (count === 0) {
                      return firstSubscribed.open.pipe(Effect.as(Stream.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "disconnected"
                        })
                      )))
                    }
                    return Effect.succeed(Stream.never)
                  })
                ),
                (argument422_0) => Stream.unwrap(argument422_0)
              )
          })
        }, ({ argument421_0, argument421_1 }) => Layer.succeed(argument421_0, argument421_1))
        const reconciler = Reconciler.layer({
          definition: Domain.definition,
          spaceId,
          retryDelay: "1 second"
        }).pipe(
          Layer.provide(localLayer()),
          Layer.provide(remote)
        )
        yield* service(Reconciler.Reconciler, reconciler)
        yield* firstSubscribed.await
        yield* TestClock.adjust("1 minute")
        assert.strictEqual(yield* Ref.get(subscriptions), 1)
      }),
      (argument419_0) => Effect.scoped(argument419_0)
    ))

  it.effect("does not resubscribe a transient watch while authentication is paused", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument426_0: LocalStore.Store, argument426_1: localLayer() },
          ({ argument426_0, argument426_1 }) => service(argument426_0, argument426_1)
        )
        const subscriptions = yield* Ref.make(0)
        const watchFailed = yield* Deferred.make<void>()
        const releasePull = yield* Deferred.make<void>()
        const credentialWaitStarted = yield* Deferred.make<void>()
        const remote = pipe({
          argument427_0: SyncEngine.SyncEngine,
          argument427_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () =>
              Deferred.succeed(credentialWaitStarted, undefined).pipe(Effect.andThen(Effect.never)),
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: () =>
              Deferred.await(releasePull).pipe(
                Effect.andThen(Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 0 })))
              ),
            bootstrap: () => Effect.die("unexpected bootstrap"),
            watch: () =>
              pipe(
                Ref.updateAndGet(subscriptions, (count) => count + 1).pipe(
                  Effect.flatMap((count) => {
                    if (count === 1) {
                      return Deferred.succeed(watchFailed, undefined).pipe(
                        Effect.as(Stream.fail(new ReplicaError.ServerUnavailable()))
                      )
                    }
                    return Effect.succeed(Stream.never)
                  })
                ),
                (argument429_0) => Stream.unwrap(argument429_0)
              )
          })
        }, ({ argument427_0, argument427_1 }) => Layer.succeed(argument427_0, argument427_1))
        const reconciliationContext = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(remote)
          ),
          (argument431_0) => Layer.build(argument431_0)
        )
        const manager = yield* Reconciler.makeManager().pipe(Effect.provide(remote))
        yield* manager.register({
          spaceId,
          generation: 1,
          definition: Domain.definition,
          local,
          reconciliation: Context.get(reconciliationContext, Reconciler.Reconciliation),
          retryDelay: "1 second"
        })
        yield* Deferred.await(watchFailed)
        yield* Deferred.succeed(releasePull, undefined)
        yield* Deferred.await(credentialWaitStarted)

        yield* TestClock.adjust("1 second")

        assert.strictEqual(yield* Ref.get(subscriptions), 1)
      }),
      (argument425_0) => Effect.scoped(argument425_0)
    ))

  it.effect("keeps an active authentication pause when the in-memory watch fails", () =>
    pipe(
      Effect.gen(function*() {
        const subscriptions = yield* Ref.make(0)
        const watchSubscribed = yield* Deferred.make<void>()
        const releaseWatch = yield* Deferred.make<void>()
        const releasePull = yield* Deferred.make<void>()
        const credentialWaitStarted = yield* Deferred.make<void>()
        const remote = pipe({
          argument434_0: SyncEngine.SyncEngine,
          argument434_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () =>
              Deferred.succeed(credentialWaitStarted, undefined).pipe(Effect.andThen(Effect.never)),
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: () =>
              Deferred.await(releasePull).pipe(
                Effect.andThen(Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 0 })))
              ),
            bootstrap: () => Effect.die("unexpected bootstrap"),
            watch: () =>
              pipe(
                Ref.updateAndGet(subscriptions, (count) => count + 1).pipe(
                  Effect.andThen(Deferred.succeed(watchSubscribed, undefined)),
                  Effect.andThen(Deferred.await(releaseWatch)),
                  Effect.as(Stream.fail(new ReplicaError.ServerUnavailable()))
                ),
                (argument436_0) => Stream.unwrap(argument436_0)
              )
          })
        }, ({ argument434_0, argument434_1 }) => Layer.succeed(argument434_0, argument434_1))
        const scheduler = yield* pipe({
          argument440_0: Reconciler.Reconciler,
          argument440_1: Reconciler.layer({ definition: Domain.definition, spaceId, retryDelay: "1 second" }).pipe(
            Layer.provide(localLayer()),
            Layer.provide(remote)
          )
        }, ({ argument440_0, argument440_1 }) => service(argument440_0, argument440_1))
        yield* Deferred.await(watchSubscribed)
        yield* Deferred.succeed(releasePull, undefined)
        yield* Deferred.await(credentialWaitStarted)
        assert.strictEqual((yield* scheduler.status)._tag, "NeedsAuthentication")

        yield* Deferred.succeed(releaseWatch, undefined)
        yield* TestClock.adjust("1 second")

        assert.strictEqual(yield* Ref.get(subscriptions), 1)
        assert.strictEqual((yield* scheduler.status)._tag, "NeedsAuthentication")
      }),
      (argument433_0) => Effect.scoped(argument433_0)
    ))

  it.effect("ignores a stale watch failure after authentication recovery starts", () =>
    pipe(
      Effect.gen(function*() {
        const watchSubscribed = yield* Deferred.make<void>()
        const releaseWatch = yield* Deferred.make<void>()
        const releaseRejectedPull = yield* Deferred.make<void>()
        const credentialWaitStarted = yield* Deferred.make<void>()
        const credentialChanged = yield* Deferred.make<void>()
        const recoveryPullEntered = yield* Deferred.make<void>()
        const releaseRecoveryPull = yield* Deferred.make<void>()
        const recoverySucceeded = yield* Deferred.make<void>()
        const pulls = yield* Ref.make(0)
        const server = yield* pipe(
          { argument443_0: ServerStore.ServerStore, argument443_1: serverLayer() },
          ({ argument443_0, argument443_1 }) => service(argument443_0, argument443_1)
        )
        const remote = pipe({
          argument444_0: SyncEngine.SyncEngine,
          argument444_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () =>
              Deferred.succeed(credentialWaitStarted, undefined).pipe(
                Effect.andThen(Deferred.await(credentialChanged))
              ),
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: (request) =>
              Ref.updateAndGet(pulls, (count) => count + 1).pipe(
                Effect.flatMap((attempt) => {
                  if (attempt === 1) {
                    return Deferred.await(releaseRejectedPull).pipe(
                      Effect.andThen(Effect.fail(
                        new ReplicaError.CredentialRejected({ credentialGeneration: 0 })
                      ))
                    )
                  }
                  return Deferred.succeed(recoveryPullEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseRecoveryPull)),
                    Effect.andThen(server.pull(request))
                  )
                })
              ),
            bootstrap: server.bootstrap,
            watch: () =>
              pipe(
                Deferred.succeed(watchSubscribed, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseWatch)),
                  Effect.as(Stream.fail(new ReplicaError.ServerUnavailable()))
                ),
                (argument449_0) => Stream.unwrap(argument449_0)
              )
          })
        }, ({ argument444_0, argument444_1 }) => Layer.succeed(argument444_0, argument444_1))
        const local = yield* pipe(
          { argument452_0: LocalStore.Store, argument452_1: localLayer() },
          ({ argument452_0, argument452_1 }) => service(argument452_0, argument452_1)
        )
        const reconciliationContext = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(remote)
          ),
          (argument453_0) => Layer.build(argument453_0)
        )
        const baseReconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
        const reconciliation = Reconciler.Reconciliation.of({
          ...baseReconciliation,
          succeeded: baseReconciliation.succeeded.pipe(
            Effect.andThen(Deferred.succeed(recoverySucceeded, undefined))
          )
        })
        const scheduler = yield* pipe({
          argument456_0: Reconciler.Reconciler,
          argument456_1: Reconciler.layerInMemoryScheduler({
            definition: Domain.definition,
            spaceId,
            retryDelay: "1 second"
          }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
            Layer.provide(remote)
          )
        }, ({ argument456_0, argument456_1 }) => service(argument456_0, argument456_1))
        yield* Deferred.await(watchSubscribed)
        yield* Deferred.succeed(releaseRejectedPull, undefined)
        yield* Deferred.await(credentialWaitStarted)
        yield* Deferred.succeed(credentialChanged, undefined)
        yield* Deferred.await(recoveryPullEntered)
        yield* Deferred.succeed(releaseWatch, undefined)
        yield* Deferred.succeed(releaseRecoveryPull, undefined)
        yield* Deferred.await(recoverySucceeded)

        assert.strictEqual((yield* scheduler.status)._tag, "Online")
      }),
      (argument442_0) => Effect.scoped(argument442_0)
    ))

  it.effect("does not retry a permanently stale reconciliation runtime", () =>
    pipe(
      Effect.gen(function*() {
        const subscriptions = yield* Ref.make(0)
        const pulls = yield* Ref.make(0)
        const subscribed = yield* Deferred.make<void>()
        const stale = new ReplicaError.StaleSchema({
          expectedVersion: 2,
          expectedHash: "expected",
          actualVersion: 1,
          actualHash: "actual"
        })
        const remote = pipe({
          argument460_0: SyncEngine.SyncEngine,
          argument460_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: () => Ref.update(pulls, (count) => count + 1).pipe(Effect.andThen(Effect.fail(stale))),
            bootstrap: () => Effect.fail(stale),
            watch: () =>
              pipe(
                Ref.update(subscriptions, (count) => count + 1).pipe(
                  Effect.andThen(Deferred.succeed(subscribed, undefined)),
                  Effect.as(Stream.fail(stale))
                ),
                (argument462_0) => Stream.unwrap(argument462_0)
              )
          })
        }, ({ argument460_0, argument460_1 }) => Layer.succeed(argument460_0, argument460_1))
        const reconciler = Reconciler.layer({
          definition: Domain.definition,
          spaceId,
          retryDelay: "1 second"
        }).pipe(
          Layer.provide(localLayer()),
          Layer.provide(remote)
        )
        const scheduler = yield* service(Reconciler.Reconciler, reconciler)
        yield* Deferred.await(subscribed)
        yield* Effect.yieldNow
        yield* TestClock.adjust("5 seconds")
        yield* Effect.yieldNow

        assert.strictEqual(yield* Ref.get(subscriptions), 1)
        assert.strictEqual(yield* Ref.get(pulls), 1)
        assert.strictEqual((yield* scheduler.status)._tag, "Failed")
      }),
      (argument459_0) => Effect.scoped(argument459_0)
    ))

  it.effect("clears the schema update status when the server returns to the client schema", () =>
    pipe(
      Effect.gen(function*() {
        const observed = yield* Ref.make<Identity.SchemaIdentity>({
          version: Identity.SchemaVersion.make(Domain.definition.schemaIdentity.version + 1),
          hash: Identity.SchemaHash.make("ffffffffffffffff")
        })
        const server = yield* pipe(
          { argument467_0: ServerStore.ServerStore, argument467_1: serverLayer() },
          ({ argument467_0, argument467_1 }) => service(argument467_0, argument467_1)
        )
        const remote = pipe({
          argument468_0: SyncEngine.SyncEngine,
          argument468_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: (request) =>
              Effect.all([server.pull(request), Ref.get(observed)]).pipe(
                Effect.map(([result, serverSchema]) => ({ ...result, serverSchema }))
              ),
            bootstrap: (request) =>
              Effect.all([server.bootstrap(request), Ref.get(observed)]).pipe(
                Effect.map(([page, serverSchema]) => ({ ...page, serverSchema }))
              ),
            watch: () => Stream.never
          })
        }, ({ argument468_0, argument468_1 }) => Layer.succeed(argument468_0, argument468_1))
        const context = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(localLayer()),
            Layer.provide(remote)
          ),
          (argument469_0) => Layer.build(argument469_0)
        )
        const reconciliation = Context.get(context, Reconciler.Reconciliation)
        yield* reconciliation.sync
        assert.strictEqual((yield* reconciliation.status)._tag, "SchemaUpdateAvailable")

        yield* Ref.set(observed, Domain.definition.schemaIdentity)
        yield* reconciliation.sync
        assert.strictEqual((yield* reconciliation.status)._tag, "Online")
      }),
      (argument466_0) => Effect.scoped(argument466_0)
    ))

  it.effect("keeps authentication status when an earlier sync completes", () =>
    pipe(
      Effect.gen(function*() {
        const pullEntered = yield* Deferred.make<void>()
        const releasePull = yield* Deferred.make<void>()
        const server = yield* pipe(
          { argument472_0: ServerStore.ServerStore, argument472_1: serverLayer() },
          ({ argument472_0, argument472_1 }) => service(argument472_0, argument472_1)
        )
        const remote = pipe({
          argument473_0: SyncEngine.SyncEngine,
          argument473_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: (request) =>
              Deferred.succeed(pullEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releasePull)),
                Effect.andThen(server.pull(request))
              ),
            bootstrap: server.bootstrap,
            watch: () => Stream.never
          })
        }, ({ argument473_0, argument473_1 }) => Layer.succeed(argument473_0, argument473_1))
        const context = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(localLayer()),
            Layer.provide(remote)
          ),
          (argument476_0) => Layer.build(argument476_0)
        )
        const reconciliation = Context.get(context, Reconciler.Reconciliation)
        const syncing = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(pullEntered)
        yield* reconciliation.failed(new ReplicaError.CredentialRejected({ credentialGeneration: 0 }))
        yield* Deferred.succeed(releasePull, undefined)
        yield* Fiber.join(syncing)

        assert.strictEqual((yield* reconciliation.status)._tag, "NeedsAuthentication")
      }),
      (argument471_0) => Effect.scoped(argument471_0)
    ))

  it.effect("keeps authentication status when a transient failure arrives later", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe(
          { argument479_0: ServerStore.ServerStore, argument479_1: serverLayer() },
          ({ argument479_0, argument479_1 }) => service(argument479_0, argument479_1)
        )
        const context = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(localLayer()),
            Layer.provide(Layer.succeed(
              SyncEngine.SyncEngine,
              SyncEngine.SyncEngine.of({
                waitForCredentialChange: () => Effect.never,
                discard: () => Effect.die("unexpected discard"),
                submit: () => Effect.die("unexpected submit"),
                pull: server.pull,
                bootstrap: server.bootstrap,
                watch: () => Stream.never
              })
            ))
          ),
          (argument480_0) => Layer.build(argument480_0)
        )
        const reconciliation = Context.get(context, Reconciler.Reconciliation)
        yield* reconciliation.failed(new ReplicaError.CredentialRejected({ credentialGeneration: 0 }))
        yield* reconciliation.failed(new ReplicaError.ServerUnavailable())

        assert.strictEqual((yield* reconciliation.status)._tag, "NeedsAuthentication")
      }),
      (argument478_0) => Effect.scoped(argument478_0)
    ))

  it.effect("keeps a permanent failure when an earlier sync completes", () =>
    pipe(
      Effect.gen(function*() {
        const pullEntered = yield* Deferred.make<void>()
        const releasePull = yield* Deferred.make<void>()
        const server = yield* pipe(
          { argument485_0: ServerStore.ServerStore, argument485_1: serverLayer() },
          ({ argument485_0, argument485_1 }) => service(argument485_0, argument485_1)
        )
        const remote = pipe({
          argument486_0: SyncEngine.SyncEngine,
          argument486_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.die("unexpected submit"),
            pull: (request) =>
              Deferred.succeed(pullEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releasePull)),
                Effect.andThen(server.pull(request))
              ),
            bootstrap: server.bootstrap,
            watch: () => Stream.never
          })
        }, ({ argument486_0, argument486_1 }) => Layer.succeed(argument486_0, argument486_1))
        const context = yield* pipe(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(localLayer()),
            Layer.provide(remote)
          ),
          (argument489_0) => Layer.build(argument489_0)
        )
        const reconciliation = Context.get(context, Reconciler.Reconciliation)
        const syncing = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(pullEntered)
        yield* reconciliation.watchFailed(new ReplicaError.ProtocolInvalid({ message: "watch stopped" }))
        yield* Deferred.succeed(releasePull, undefined)
        yield* Fiber.join(syncing)

        assert.strictEqual((yield* reconciliation.status)._tag, "Failed")
      }),
      (argument484_0) => Effect.scoped(argument484_0)
    ))

  it.effect("retries pending mutations after an interrupted submit", () =>
    pipe(
      Effect.gen(function*() {
        const firstAttempt = yield* Deferred.make<void>()
        const secondAttempt = yield* Deferred.make<void>()
        let attempts = 0
        const server = yield* pipe(
          { argument492_0: ServerStore.ServerStore, argument492_1: serverLayer() },
          ({ argument492_0, argument492_1 }) => service(argument492_0, argument492_1)
        )
        const remote = pipe({
          argument493_0: SyncEngine.SyncEngine,
          argument493_1: SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: () => Effect.die("unexpected discard"),
            submit: ({ envelope: submitted }) =>
              Effect.suspend(() => {
                attempts++
                if (attempts === 1) {
                  return Deferred.succeed(firstAttempt, undefined).pipe(
                    Effect.andThen(Effect.interrupt)
                  )
                }
                const rejected = Protocol.RejectedReceipt.make({
                  ...putTodoProvenance,
                  spaceId,
                  clientId,
                  membershipIncarnation: submitted.membershipIncarnation,
                  mutationId: submitted.mutationId,
                  localSequence: submitted.localSequence,
                  origin: "Authorization",
                  rejection: "denied"
                })
                return Deferred.succeed(secondAttempt, undefined).pipe(Effect.as(rejected))
              }),
            pull: server.pull,
            bootstrap: server.bootstrap,
            watch: server.watch
          })
        }, ({ argument493_0, argument493_1 }) => Layer.succeed(argument493_0, argument493_1))
        const local = localLayer()
        const reconciler = Reconciler.layer({
          definition: Domain.definition,
          spaceId,
          retryDelay: "1 second"
        }).pipe(
          Layer.provide(local),
          Layer.provide(remote)
        )
        const services = yield* pipe(Layer.merge(local, reconciler), (argument495_0) => Layer.build(argument495_0))
        const store = Context.get(services, LocalStore.Store)
        const scheduler = Context.get(services, Reconciler.Reconciler)
        yield* pipe(
          { argument496_0: Domain.PutTodo, argument496_1: Domain.todo("interrupted") },
          ({ argument496_0, argument496_1 }) => store.mutate(argument496_0, argument496_1)
        )

        yield* scheduler.notify
        yield* Deferred.await(firstAttempt)
        yield* Effect.yieldNow
        yield* scheduler.notify
        yield* TestClock.adjust("1 second")
        yield* Effect.yieldNow

        assert.strictEqual(attempts, 2)
        yield* Deferred.await(secondAttempt)
      }),
      (argument491_0) => Effect.scoped(argument491_0)
    ))

  it.effect("emits a scoped wake when a watch subscription becomes ready", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument498_0: LocalStore.Store, argument498_1: localLayer() },
          ({ argument498_0, argument498_1 }) => service(argument498_0, argument498_1)
        )
        const server = yield* pipe(
          { argument499_0: ServerStore.ServerStore, argument499_1: serverLayer() },
          ({ argument499_0, argument499_1 }) => service(argument499_0, argument499_1)
        )
        const pending = yield* pipe(
          { argument500_0: Domain.PutTodo, argument500_1: Domain.todo("1") },
          ({ argument500_0, argument500_1 }) => local.mutate(argument500_0, argument500_1)
        )
        yield* server.submit(pending.envelope)

        const wake = yield* server.watch({
          spaceId,
          clientId,
          schema: Domain.definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }).pipe(Stream.runHead)
        assert.strictEqual(Option.getOrThrow(wake).spaceId, spaceId)
      }),
      (argument497_0) => Effect.scoped(argument497_0)
    ))

  it.effect("isolates wake backpressure between spaces", () =>
    pipe(
      Effect.gen(function*() {
        const server = yield* pipe({
          argument502_0: ServerStore.ServerStore,
          argument502_1: ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition, wakeCapacity: 1 })
            .pipe(
              Layer.provide(runtime),
              Layer.provide(database())
            )
        }, ({ argument502_0, argument502_1 }) => service(argument502_0, argument502_1))
        const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
        const ready = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let initial = true
        const wake = yield* server.watch({
          spaceId: otherSpaceId,
          clientId,
          schema: Domain.definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }).pipe(
          Stream.tap(() => {
            if (!initial) return Effect.void
            initial = false
            return Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(release)))
          }),
          Stream.drop(1),
          Stream.runHead,
          Effect.forkChild
        )
        yield* Deferred.await(ready)

        const makeForSpace = (
          targetSpaceId: Identity.SpaceId,
          localSequence: number,
          mutationId: Identity.MutationId
        ) =>
          Effect.gen(function*() {
            const identity = {
              spaceId: targetSpaceId,
              clientId,
              mutationId,
              localSequence: Identity.LocalSequence.make(localSequence),
              basis: Identity.ServerSequence.make(0),
              payload: Domain.todo(`${targetSpaceId}:${localSequence}`),
              digestVersion: 3 as const,
              membershipIncarnation: defaultMembershipIncarnation,
              ...putTodoProvenance
            }
            return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
          })
        yield* server.submit(
          yield* pipe(
            {
              argument505_0: otherSpaceId,
              argument505_1: 1,
              argument505_2: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000051")
            },
            ({ argument505_0, argument505_1, argument505_2 }) =>
              makeForSpace(argument505_0, argument505_1, argument505_2)
          )
        )
        for (let index = 1; index <= 3; index++) {
          yield* server.submit(
            yield* pipe(
              {
                argument506_0: spaceId,
                argument506_1: index,
                argument506_2: Identity.MutationId.make(
                  `mut_00000000-0000-4000-8000-${String(index + 51).padStart(12, "0")}`
                )
              },
              ({ argument506_0, argument506_1, argument506_2 }) =>
                makeForSpace(argument506_0, argument506_1, argument506_2)
            )
          )
        }
        yield* Deferred.succeed(release, undefined)
        const result = yield* Fiber.join(wake)
        assert.strictEqual(Option.getOrThrow(result).spaceId, otherSpaceId)
      }).pipe(Effect.provide(NodeCrypto.layer)),
      (argument501_0) => Effect.scoped(argument501_0)
    ))

  it.effect("runs multi-read queries against one committed visible snapshot", () =>
    pipe(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/snapshot.sqlite`
        const Item = Model.make("SnapshotItem", {
          version: 1,
          key: Schema.String,
          schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
        })
        const PutPair = Mutation.make("PutSnapshotPair", {
          version: 1,
          payload: { left: Schema.Number, right: Schema.Number }
        })
        const ReadPair = Query.make("ReadSnapshotPair", {
          success: Schema.Tuple([Schema.Number, Schema.Number])
        })
        class QueryGate extends Context.Service<QueryGate, {
          readonly betweenReads: Effect.Effect<void>
        }>()("test/QueryGate") {}
        const definition = Definition.make({ version: 1, models: [Item], mutations: [PutPair], queries: [ReadPair] })
        const reached = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const writerStarted = yield* Deferred.make<void>()
        const readPairLayer = Effect.gen(function*() {
          const gate = yield* QueryGate
          return ReadPair.toLayer(({ query }) =>
            Effect.gen(function*() {
              const left = Option.getOrThrow(yield* query.get(Item, "left"))
              yield* gate.betweenReads
              const right = Option.getOrThrow(yield* query.get(Item, "right"))
              return [left.value, right.value] as const
            })
          )
        }).pipe(Layer.unwrap)
        const handlers = pipe({
          argument508_0: PutPair.toLayer(({ payload, transaction }) =>
            Effect.gen(function*() {
              if (payload.left === 1) yield* Deferred.succeed(writerStarted, undefined)
              yield* transaction.set(Item, "left", { id: "left", value: payload.left })
              yield* transaction.set(Item, "right", { id: "right", value: payload.right })
            })
          ),
          argument508_1: readPairLayer
        }, ({ argument508_0, argument508_1 }) => Layer.mergeAll(argument508_0, argument508_1))
        const gate = pipe({
          argument510_0: QueryGate,
          argument510_1: QueryGate.of({
            betweenReads: Deferred.succeed(reached, undefined).pipe(
              Effect.andThen(Deferred.await(release))
            )
          })
        }, ({ argument510_0, argument510_1 }) => Layer.succeed(argument510_0, argument510_1))
        const pairDatabase = () =>
          pipe(
            {
              argument512_0: SqliteClient.layer({ filename }),
              argument512_1: NodeCrypto.layer,
              argument512_2: Reactivity.layer,
              argument512_3: QueryReactivity.layer
            },
            ({ argument512_0, argument512_1, argument512_2, argument512_3 }) =>
              Layer.mergeAll(argument512_0, argument512_1, argument512_2, argument512_3)
          )
        const pairRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers), Layer.provide(gate))
        const local = LocalStore.layer({
          ...clientHistory,
          scope: Protocol.ReplicationScope.make({ models: [Item.name] }),
          definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(pairRuntime),
          Layer.provide(pairDatabase())
        )
        const queries = QueryExecutor.layer(definition, spaceId).pipe(
          Layer.provide(handlers),
          Layer.provide(gate),
          Layer.provide(pairDatabase())
        )
        const context = yield* pipe(Layer.merge(local, queries), (argument515_0) => Layer.build(argument515_0))
        const store = Context.get(context, LocalStore.Store)
        const queryExecutor = Context.get(context, QueryExecutor.QueryExecutor)
        yield* store.mutate(PutPair, { left: 0, right: 0 })
        const query = yield* queryExecutor.execute(ReadPair, undefined).pipe(Effect.forkChild)
        yield* Deferred.await(reached)
        const mutation = yield* store.mutate(PutPair, { left: 1, right: 1 }).pipe(Effect.forkChild)
        yield* Deferred.await(writerStarted)
        yield* Fiber.join(mutation)
        yield* Deferred.succeed(release, undefined)
        assert.deepStrictEqual(yield* Fiber.join(query), [0, 0])

        const counterfeit = Query.make("ReadSnapshotPair", { success: Schema.String })
        const error = yield* queryExecutor.execute(counterfeit, undefined).pipe(expectedFailure)
        assert.strictEqual(error._tag, "ProtocolInvalid")
      }),
      (argument507_0) => Effect.scoped(argument507_0)
    ).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("rejects a receipt for another replica without settling pending work", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument517_0: LocalStore.Store, argument517_1: localLayer() },
          ({ argument517_0, argument517_1 }) => service(argument517_0, argument517_1)
        )
        const pending = yield* pipe(
          { argument518_0: Domain.PutTodo, argument518_1: Domain.todo("1") },
          ({ argument518_0, argument518_1 }) => local.mutate(argument518_0, argument518_1)
        )
        const error = yield* local.applyReceipt({
          _tag: "Rejected",
          ...putTodoProvenance,
          spaceId: Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002"),
          clientId,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          origin: "Authorization",
          rejection: { reason: "wrong space" }
        }).pipe(expectedFailure)

        assert.strictEqual(error._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.pendingCount, 1)
      }),
      (argument516_0) => Effect.scoped(argument516_0)
    ))

  it.effect("rejects a conflicting duplicate receipt", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument520_0: LocalStore.Store, argument520_1: localLayer() },
          ({ argument520_0, argument520_1 }) => service(argument520_0, argument520_1)
        )
        const pending = yield* pipe(
          { argument521_0: Domain.PutTodo, argument521_1: Domain.todo("1") },
          ({ argument521_0, argument521_1 }) => local.mutate(argument521_0, argument521_1)
        )
        const receipt: Protocol.AcceptedReceipt = {
          _tag: "Accepted",
          ...putTodoProvenance,
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          serverSequence: Identity.ServerSequence.make(1),
          result: pending.optimisticResult
        }
        yield* local.applyReceipt(receipt)

        const error = yield* local.applyReceipt({ ...receipt, result: { conflicting: true } }).pipe(expectedFailure)
        assert.strictEqual(error._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.pendingCount, 1)
      }),
      (argument519_0) => Effect.scoped(argument519_0)
    ))

  it.effect("converges concurrent clients through server assigned order", () =>
    pipe(
      Effect.gen(function*() {
        const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
        const server = yield* pipe(
          { argument523_0: ServerStore.ServerStore, argument523_1: serverLayer() },
          ({ argument523_0, argument523_1 }) => service(argument523_0, argument523_1)
        )
        const firstContext = yield* pipe(
          clientServices(clientId, server),
          (argument524_0) => Layer.build(argument524_0)
        )
        const secondContext = yield* pipe(
          clientServices(secondClientId, server),
          (argument525_0) => Layer.build(argument525_0)
        )
        const first = Context.get(firstContext, LocalStore.Store)
        const firstReconciler = Context.get(firstContext, Reconciler.Reconciler)
        const second = Context.get(secondContext, LocalStore.Store)
        const secondReconciler = Context.get(secondContext, Reconciler.Reconciler)

        yield* pipe(
          { argument526_0: Domain.PutTodo, argument526_1: Domain.todo("1") },
          ({ argument526_0, argument526_1 }) => first.mutate(argument526_0, argument526_1)
        )
        yield* firstReconciler.sync
        yield* secondReconciler.sync
        yield* first.mutate(Domain.IncrementTodo, { id: "1", delta: 1 })
        yield* second.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
        yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })
        yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })

        assert.strictEqual(Option.getOrThrow(yield* first.get(Domain.Todo, "1")).count, 3)
        assert.strictEqual(Option.getOrThrow(yield* second.get(Domain.Todo, "1")).count, 3)
        assert.strictEqual(yield* first.cursor, 3)
        assert.strictEqual(yield* second.cursor, 3)
      }),
      (argument522_0) => Effect.scoped(argument522_0)
    ))

  it.effect("reconciles an offline mutation queue with linear handler work", () =>
    pipe(
      Effect.gen(function*() {
        const Item = Model.make("ReconciliationWorkItem", {
          version: 1,
          key: Schema.String,
          schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
        })
        const PutItem = Mutation.make("PutReconciliationWorkItem", {
          version: 1,
          payload: Item.schema,
          success: Item.schema
        })
        const workDefinition = Definition.make({ version: 1, models: [Item], mutations: [PutItem] })
        const executions = yield* Ref.make(0)
        const workHandlers = PutItem.toLayer(({ payload, transaction }) =>
          Ref.update(executions, (count) => count + 1).pipe(
            Effect.andThen(transaction.set(Item, payload.id, payload)),
            Effect.as(payload)
          )
        )
        const workRuntime = MutationRuntime.layer(workDefinition).pipe(Layer.provide(workHandlers))
        const authoritativeDatabase = database()
        const authoritativeLayer = ServerStore.layerTrusted({ ...serverHistory, definition: workDefinition }).pipe(
          Layer.provide(workRuntime),
          Layer.provide(authoritativeDatabase)
        )
        const server = yield* service(ServerStore.ServerStore, authoritativeLayer)
        const replicaDatabase = database()
        const workScope = Protocol.ReplicationScope.make({ models: [Item.name] })
        const local = LocalStore.layer({
          ...clientHistory,
          scope: workScope,
          definition: workDefinition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(workRuntime),
          Layer.provide(replicaDatabase)
        )
        const reconciler = Reconciler.layer({ definition: workDefinition, spaceId }).pipe(
          Layer.provide(local),
          Layer.provide(directSync(server))
        )
        const context = yield* pipe(Layer.merge(local, reconciler), (argument530_0) => Layer.build(argument530_0))
        const store = Context.get(context, LocalStore.Store)
        const sync = Context.get(context, Reconciler.Reconciler)
        yield* sync.sync
        yield* Ref.set(executions, 0)

        const mutationCount = 10
        for (let index = 0; index < mutationCount; index++) {
          yield* store.mutate(PutItem, { id: String(index), value: index })
        }
        yield* sync.sync

        assert.strictEqual(yield* store.pendingCount, 0)
        assert.isAtMost(yield* Ref.get(executions), mutationCount * 3)
      }),
      (argument527_0) => Effect.scoped(argument527_0)
    ))

  it.effect("caps catch up pages by encoded bytes as well as entry count", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument532_0: LocalStore.Store, argument532_1: localLayer() },
          ({ argument532_0, argument532_1 }) => service(argument532_0, argument532_1)
        )
        const server = yield* pipe(
          { argument533_0: ServerStore.ServerStore, argument533_1: serverLayer() },
          ({ argument533_0, argument533_1 }) => service(argument533_0, argument533_1)
        )
        yield* installFreshView(local, server)
        for (let index = 0; index < 24; index++) {
          const pending = yield* pipe({
            argument534_0: Domain.PutTodo,
            argument534_1: pipe(
              { argument535_0: String(index), argument535_1: `${index}:${"x".repeat(200_000)}` },
              ({ argument535_0, argument535_1 }) => Domain.todo(argument535_0, argument535_1)
            )
          }, ({ argument534_0, argument534_1 }) => local.mutate(argument534_0, argument534_1))
          yield* server.submit(pending.envelope)
        }
        const state = yield* local.replicationState
        if (state.cursor === null) assert.fail("expected installed replication view")
        const page = incremental(
          yield* pipe(pullRequest(state.cursor, 1_000), (argument536_0) => server.pull(argument536_0))
        )
        pipe(
          { argument537_0: Protocol.encodedBytes(page), argument537_1: Protocol.maximumBatchBytes },
          ({ argument537_0, argument537_1 }) => assert.isAtMost(argument537_0, argument537_1)
        )
        assert.isTrue(page.hasMore)
        assert.isBelow(page.changes.length, 24)
      }),
      (argument531_0) => Effect.scoped(argument531_0)
    ))

  it.effect("restores optimistic state and its pending envelope after restart", () =>
    pipe(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/replica.db`
        const persistentDatabase = () =>
          pipe(
            {
              argument539_0: SqliteClient.layer({ filename, disableWAL: true }),
              argument539_1: NodeCrypto.layer,
              argument539_2: Reactivity.layer,
              argument539_3: QueryReactivity.layer
            },
            ({ argument539_0, argument539_1, argument539_2, argument539_3 }) =>
              Layer.mergeAll(argument539_0, argument539_1, argument539_2, argument539_3)
          )

        const mutationId = yield* pipe(
          Effect.gen(function*() {
            const layer = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
              Layer.provide(runtime),
              Layer.provide(persistentDatabase())
            )
            const local = yield* service(LocalStore.Store, layer)
            const pending = yield* pipe(
              { argument542_0: Domain.PutTodo, argument542_1: Domain.todo("1") },
              ({ argument542_0, argument542_1 }) => local.mutate(argument542_0, argument542_1)
            )
            return pending.envelope.mutationId
          }),
          (argument540_0) => Effect.scoped(argument540_0)
        )

        yield* pipe(
          Effect.gen(function*() {
            const layer = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
              Layer.provide(runtime),
              Layer.provide(persistentDatabase())
            )
            const local = yield* service(LocalStore.Store, layer)
            pipe({
              argument545_0: Option.getOrThrow(yield* local.get(Domain.Todo, "1")),
              argument545_1: Domain.todo("1")
            }, ({ argument545_0, argument545_1 }) => assert.deepStrictEqual(argument545_0, argument545_1))
            const pending = yield* local.pending
            pipe(
              { argument546_0: pending.map((item) => item.envelope.mutationId), argument546_1: [mutationId] },
              ({ argument546_0, argument546_1 }) => assert.deepStrictEqual(argument546_0, argument546_1)
            )
          }),
          (argument543_0) => Effect.scoped(argument543_0)
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)),
      (argument538_0) => Effect.scoped(argument538_0)
    ))

  it.effect("recovers an interrupted durable submission as retrying without incrementing attempts", () =>
    pipe(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/submitting-recovery.db`
        const persistentDatabase = () =>
          pipe(
            {
              argument548_0: SqliteClient.layer({ filename, disableWAL: true }),
              argument548_1: NodeCrypto.layer,
              argument548_2: Reactivity.layer,
              argument548_3: QueryReactivity.layer
            },
            ({ argument548_0, argument548_1, argument548_2, argument548_3 }) =>
              Layer.mergeAll(argument548_0, argument548_1, argument548_2, argument548_3)
          )
        const makeLocal = () =>
          LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )

        const mutationId = yield* pipe(
          Effect.gen(function*() {
            const local = yield* pipe(
              { argument551_0: LocalStore.Store, argument551_1: makeLocal() },
              ({ argument551_0, argument551_1 }) => service(argument551_0, argument551_1)
            )
            const pending = yield* pipe({
              argument552_0: Domain.PutTodo,
              argument552_1: Domain.todo("submitting-recovery")
            }, ({ argument552_0, argument552_1 }) => local.mutate(argument552_0, argument552_1))
            yield* local.markSubmitting(pending.envelope.mutationId)
            const submitting = (yield* local.pending)[0]
            assert.strictEqual(submitting.submissionState, "Submitting")
            assert.strictEqual(submitting.attempts, 1)
            return pending.envelope.mutationId
          }),
          (argument550_0) => Effect.scoped(argument550_0)
        )

        yield* pipe(
          Effect.gen(function*() {
            const local = yield* pipe(
              { argument554_0: LocalStore.Store, argument554_1: makeLocal() },
              ({ argument554_0, argument554_1 }) => service(argument554_0, argument554_1)
            )
            const recovered = (yield* local.pending)[0]
            assert.strictEqual(recovered.envelope.mutationId, mutationId)
            assert.strictEqual(recovered.submissionState, "Retrying")
            assert.strictEqual(recovered.attempts, 1)
          }),
          (argument553_0) => Effect.scoped(argument553_0)
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)),
      (argument547_0) => Effect.scoped(argument547_0)
    ))

  it.effect("restores Submitted and invalidates pending for an identical duplicate receipt", () =>
    pipe(
      Effect.gen(function*() {
        const sharedDatabase = database()
        const localLayerWithDatabase = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(Layer.provide(runtime), Layer.provide(sharedDatabase))
        const context = yield* pipe(Layer.merge(localLayerWithDatabase, sharedDatabase), (argument556_0) =>
          Layer.build(argument556_0))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const reactivity = Context.get(context, Reactivity.Reactivity)
        const pending = yield* pipe(
          { argument557_0: Domain.PutTodo, argument557_1: Domain.todo("duplicate-state") },
          ({ argument557_0, argument557_1 }) =>
            local.mutate(argument557_0, argument557_1)
        )
        const receipt = Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          ...putTodoProvenance,
          origin: "Authorization",
          rejection: "denied"
        })
        yield* local.persistReceipt(receipt)
        yield* sql`UPDATE effect_local_client_pending_data SET submission_state = 'Retrying'
        WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`
        let invalidations = 0
        const cancel = reactivity.registerUnsafe([ReactivityKey.pending(spaceId)], () => invalidations++)
        yield* Effect.addFinalizer(() => Effect.sync(cancel))

        yield* local.persistReceipt(receipt)

        const restored = (yield* local.pending)[0]
        assert.strictEqual(restored.submissionState, "Submitted")
        assert.strictEqual(invalidations, 1)
      }),
      (argument555_0) => Effect.scoped(argument555_0)
    ))

  it.effect("rejects named receipt provenance outside the current definition", () =>
    pipe(
      Effect.gen(function*() {
        const local = yield* pipe(
          { argument559_0: LocalStore.Store, argument559_1: localLayer() },
          ({ argument559_0, argument559_1 }) => service(argument559_0, argument559_1)
        )
        const pending = yield* pipe(
          { argument560_0: Domain.PutTodo, argument560_1: Domain.todo("receipt-provenance") },
          ({ argument560_0, argument560_1 }) => local.mutate(argument560_0, argument560_1)
        )
        const receipt = Protocol.AcceptedReceipt.make({
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          ...putTodoProvenance,
          serverSequence: Identity.ServerSequence.make(1),
          result: pending.optimisticResult
        })
        const wrongSchema = Protocol.AcceptedReceipt.make({
          ...receipt,
          sourceSchema: {
            ...Domain.definition.schemaIdentity,
            hash: pipe("0".repeat(16), (argument561_0) => Identity.SchemaHash.make(argument561_0))
          }
        })
        const wrongVersion = Protocol.AcceptedReceipt.make({
          ...receipt,
          mutationVersion: Identity.SchemaVersion.make(receipt.mutationVersion + 1)
        })

        for (const invalid of [wrongSchema, wrongVersion]) {
          const error = yield* local.persistReceipt(invalid).pipe(expectedFailure)
          assert.strictEqual(error._tag, "ProtocolInvalid")
          pipe(
            Option.isNone(yield* local.receipt(pending.envelope.mutationId)),
            (argument562_0) => assert.isTrue(argument562_0)
          )
          assert.strictEqual((yield* local.pending)[0].submissionState, "Queued")
        }
      }),
      (argument558_0) => Effect.scoped(argument558_0)
    ))
})
