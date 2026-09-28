import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({
  definition: Domain.definition,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerServerDatabase)
)

type Remote = "Reachable" | "Gated" | "Unreachable" | "Denied"

const makeRemote = Effect.fnUntraced(function*() {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const mode = MutableRef.make<Remote>("Reachable")
  const pullEntered = yield* Deferred.make<void>()
  const releasePull = yield* Deferred.make<void>()
  const gate = <A, E extends { readonly _tag: string },>(
    effect: Effect.Effect<A, E | ReplicaError.ServerUnavailable>
  ) =>
    Effect.suspend(() => {
      const current = MutableRef.get(mode)
      if (current === "Unreachable") return Effect.fail(new ReplicaError.ServerUnavailable())
      if (current === "Gated") {
        return Deferred.succeed(pullEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releasePull)),
          Effect.andThen(effect)
        )
      }
      return effect
    })
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => gate(server.admitBatch(request, null)),
    discard: (request) => gate(server.discard(request, null)),
    pull: (request) =>
      Effect.suspend(() => {
        if (MutableRef.get(mode) === "Denied") {
          return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "revoked" }))
        }
        return gate(server.pull(request))
      }),
    bootstrap: (request) => gate(server.bootstrap(request)),
    watch: (request) =>
      Stream.unwrap(Effect.sync(() => {
        if (MutableRef.get(mode) === "Unreachable") return Stream.fail(new ReplicaError.ServerUnavailable())
        return server.watch(request)
      }))
  })
  return { remote, mode, pullEntered, releasePull }
})

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const layerReplica = (
  remote: SyncEngine.Service,
  capacity?: Pick<SqlReplica.Options<typeof Domain.definition>, "maximumActiveSpaces" | "foregroundActiveSpaces">
) =>
  SqlReplica.layer({
    ...capacity,
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceId],
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration,
    retryDelay: "1 minute",
    maximumRetryDelay: "1 minute"
  }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
  )

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const awaitAggregate = (
  reactivity: Reactivity.Reactivity,
  replica: Replica.Service,
  predicate: (aggregate: ReplicaStatus.Aggregate) => boolean
) =>
  reactivity.stream([ReactivityKey.aggregateStatus], replica.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const noCounts = { idle: 0, offline: 0, connecting: 0, online: 0, needsAuthentication: 0, failed: 0 }

describe("space activity status", () => {
  it.effect(
    "reports a remembered space that is not being synchronized as Idle, before activation and after deactivation",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const replica = Context.get(
        yield* Layer.build(layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database)))),
        Replica.Replica
      )
      const space = yield* replica.space(spaceId)
      const idleAggregate = { state: "Idle", spaces: 1, totalPending: 0, counts: { ...noCounts, idle: 1 } }

      assert.deepStrictEqual(yield* space.status, { spaceId, synced: false, _tag: "Idle", pending: 0 })
      assert.deepStrictEqual(yield* replica.status, idleAggregate)

      yield* space.activate
      yield* awaitStatus(reactivity, space, (status) => status._tag === "Online")
      yield* space.deactivate

      assert.deepStrictEqual(yield* space.status, { spaceId, synced: true, _tag: "Idle", pending: 0 })
      assert.deepStrictEqual(yield* replica.status, idleAggregate)
    })
  )

  it.effect(
    "computes the aggregate state from the spaces being synchronized and counts idle spaces apart",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const replica = Context.get(
        yield* Layer.build(layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database)))),
        Replica.Replica
      )
      yield* replica.join(otherSpaceId)
      const space = yield* replica.space(spaceId)
      yield* space.activate

      const online = yield* awaitAggregate(reactivity, replica, (aggregate) => aggregate.counts.online === 1)
      assert.deepStrictEqual(online, {
        state: "Online",
        spaces: 2,
        totalPending: 0,
        counts: { ...noCounts, online: 1, idle: 1 }
      })

      MutableRef.set(remote.mode, "Unreachable")
      yield* space.mutate(Domain.PutTodo, Domain.todo("unreachable"))
      const offline = yield* awaitAggregate(
        reactivity,
        replica,
        (aggregate) => aggregate.totalPending === 1 && aggregate.counts.online === 0
      )
      assert.deepStrictEqual(offline, {
        state: "Offline",
        spaces: 2,
        totalPending: 1,
        counts: { ...noCounts, offline: 1, idle: 1 }
      })
    })
  )

  it.effect(
    "keeps the pending count of a queued idle space while a background sync of another space runs",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      MutableRef.set(remote.mode, "Unreachable")
      const database = yield* Layer.build(layerDatabase)
      const firstScope = yield* Scope.make()
      const first = Context.get(
        yield* Layer.buildWithScope(
          layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database))),
          firstScope
        ),
        Replica.Replica
      )
      yield* first.join(otherSpaceId)
      for (const id of [spaceId, otherSpaceId]) {
        const space = yield* first.space(id)
        yield* space.mutate(Domain.PutTodo, Domain.todo(id))
      }
      yield* Scope.close(firstScope, Exit.void)

      MutableRef.set(remote.mode, "Gated")
      const replica = Context.get(
        yield* Layer.build(
          layerReplica(remote.remote, { maximumActiveSpaces: 2, foregroundActiveSpaces: 1 }).pipe(
            Layer.provide(Layer.succeedContext(database))
          )
        ),
        Replica.Replica
      )
      yield* Deferred.await(remote.pullEntered)
      const queued = yield* replica.space(otherSpaceId)

      assert.strictEqual(yield* queued.activation, "Inactive")
      assert.deepStrictEqual(yield* queued.status, { spaceId: otherSpaceId, synced: false, _tag: "Idle", pending: 1 })
      assert.deepStrictEqual(yield* replica.status, {
        state: "Connecting",
        spaces: 2,
        totalPending: 2,
        counts: { ...noCounts, connecting: 1, idle: 1 }
      })
    })
  )
})

describe("space synced status", () => {
  it.effect(
    "reports synced false until the first sync installs a view and true afterwards",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      MutableRef.set(remote.mode, "Gated")
      const database = yield* Layer.build(layerDatabase)
      const context = yield* Layer.build(
        layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database)))
      )
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.activate
      yield* Deferred.await(remote.pullEntered)

      const before = yield* space.status
      assert.strictEqual(before._tag, "Connecting")
      assert.strictEqual(before.synced, false)

      yield* Deferred.succeed(remote.releasePull, undefined)
      const after = yield* awaitStatus(
        Context.get(database, Reactivity.Reactivity),
        space,
        (status) => status._tag === "Online"
      )
      assert.strictEqual(after.synced, true)
    })
  )

  it.effect(
    "reports a synced space as synced on reopen while the server is unreachable, active or not",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const firstScope = yield* Scope.make()
      const first = yield* Layer.buildWithScope(
        layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database))),
        firstScope
      )
      const firstSpace = yield* Context.get(first, Replica.Replica).space(spaceId)
      yield* firstSpace.activate
      yield* awaitStatus(reactivity, firstSpace, (status) => status._tag === "Online")
      yield* Scope.close(firstScope, Exit.void)

      MutableRef.set(remote.mode, "Unreachable")
      const second = yield* Layer.build(layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database))))
      const space = yield* Context.get(second, Replica.Replica).space(spaceId)
      assert.strictEqual((yield* space.status).synced, true)

      yield* space.activate
      assert.strictEqual((yield* space.status).synced, true)
      const offline = yield* awaitStatus(reactivity, space, (status) => status._tag === "Offline")
      assert.strictEqual(offline.synced, true)

      yield* space.deactivate
      assert.strictEqual((yield* space.status).synced, true)
    })
  )

  it.effect(
    "reports synced false again after leaving and rejoining a space",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const context = yield* Layer.build(
        layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database)))
      )
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* awaitStatus(reactivity, space, (status) => status._tag === "Online" && status.synced)

      MutableRef.set(remote.mode, "Gated")
      yield* replica.leave(spaceId)
      const rejoined = yield* replica.join(spaceId)
      assert.strictEqual((yield* rejoined.status).synced, false)
      yield* rejoined.activate
      yield* Deferred.await(remote.pullEntered)
      assert.strictEqual((yield* rejoined.status).synced, false)
    })
  )

  it.effect(
    "reports synced once a background sync installs the view of an inactive space",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      MutableRef.set(remote.mode, "Unreachable")
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const firstScope = yield* Scope.make()
      const first = yield* Layer.buildWithScope(
        layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database))),
        firstScope
      )
      const firstSpace = yield* Context.get(first, Replica.Replica).space(spaceId)
      yield* firstSpace.activate
      yield* firstSpace.mutate(Domain.PutTodo, Domain.todo("offline"))
      yield* awaitStatus(reactivity, firstSpace, (status) => status._tag === "Offline" && status.pending === 1)
      assert.strictEqual((yield* firstSpace.status).synced, false)
      yield* Scope.close(firstScope, Exit.void)

      MutableRef.set(remote.mode, "Reachable")
      const second = yield* Layer.build(layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database))))
      const space = yield* Context.get(second, Replica.Replica).space(spaceId)
      yield* awaitStatus(reactivity, space, (status) => status.synced)
      yield* reactivity.stream([ReactivityKey.activation(spaceId)], space.activation).pipe(
        Stream.filter((activation) => activation === "Inactive"),
        Stream.runHead
      )
      const inactive = yield* space.status
      assert.strictEqual(inactive.synced, true)
      assert.strictEqual(inactive.pending, 0)
    })
  )

  it.effect(
    "reports synced false once the server revokes read access and the view is cleared",
    Effect.fnUntraced(function*() {
      const remote = yield* makeRemote()
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const context = yield* Layer.build(
        layerReplica(remote.remote).pipe(Layer.provide(Layer.succeedContext(database)))
      )
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.activate
      yield* awaitStatus(reactivity, space, (status) => status._tag === "Online" && status.synced)

      MutableRef.set(remote.mode, "Denied")
      yield* space.mutate(Domain.PutTodo, Domain.todo("after-revocation"))
      yield* awaitStatus(reactivity, space, (status) => !status.synced)
      const failed = yield* awaitStatus(reactivity, space, (status) => status._tag === "Failed")
      assert.strictEqual(failed.synced, false)
    })
  )
})
