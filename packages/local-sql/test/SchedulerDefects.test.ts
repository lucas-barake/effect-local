import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  awaitSpaceStatusWhere,
  type Constructor,
  constructors,
  emptyPage,
  idleRemote,
  makeAttempts,
  viewId
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000a01")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000a02")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000a01")

const claimStatement = "AND attempt_count >= "

const serverUnavailable = Effect.fail(new ReplicaError.ServerUnavailable())

const twoSpaces = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 second"
  })

const withPending = (constructor: Constructor, seeded: ReadonlyArray<Identity.SpaceId>) =>
  twoSpaces(constructor).pipe(Effect.tap((services) => BackgroundReplica.seedPending(services, seeded)))

const captureLogs = () => {
  const entries: Array<Logger.Options<unknown>> = []
  const logger = Logger.make<unknown, void>((entry) => {
    entries.push(entry)
  })
  const defects = () =>
    entries.flatMap((entry) => {
      if (entry.logLevel !== "Error" || !Cause.hasDies(entry.cause)) return []
      return [Cause.squash(entry.cause)]
    })
  return { layerLogs: Logger.layer([logger]), defects }
}

const eventually = (
  services: BackgroundReplica.Services,
  space: Replica.Space,
  matches: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  awaitSpaceStatusWhere(space, services.reactivity, matches).pipe(
    Effect.scoped,
    VirtualTime.advanceUntil,
    Effect.timeoutOption("5 minutes")
  )

const isDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Idle" && status.pending === 0

const isFailed = (status: ReplicaStatus.SpaceStatus) => status._tag === "Failed"

const failureMessage = (status: Option.Option<ReplicaStatus.SpaceStatus>) =>
  status.pipe(
    Option.flatMap((current) => {
      if (current._tag !== "Failed") return Option.none()
      return Option.some(current.message)
    }),
    Option.getOrElse(() => "the space never reported a failure")
  )

describe("background turns that die", () => {
  it.effect.each(constructors)(
    "keeps draining background spaces after two background turns died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      const attempts = yield* makeAttempts
      let undecodable = true
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId !== spaceId) return emptyPage(services.crypto, request)
          if (undecodable) return Effect.andThen(attempts.record, Effect.die("undecodable response"))
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      }))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))
      const otherDrained = yield* eventually(services, other, isDrained)
      yield* space.activate
      yield* VirtualTime.advanceUntil(attempts.reached(2))
      yield* space.deactivate
      yield* VirtualTime.advanceUntil(attempts.reached(3))
      yield* space.activate
      yield* VirtualTime.advanceUntil(attempts.reached(4))
      undecodable = false
      yield* space.deactivate

      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(otherDrained))
      assert.isAbove(attempts.count(), 4)
      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports a background space whose turn died as failed until its next turn drains it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      const attempts = yield* makeAttempts
      const logs = captureLogs()
      let undecodable = true
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId !== spaceId) return emptyPage(services.crypto, request)
          if (undecodable) return Effect.andThen(attempts.record, Effect.die("undecodable response"))
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))

      const failed = yield* eventually(services, space, isFailed)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.strictEqual(failureMessage(failed), "ProtocolInvalid")
      assert.strictEqual(Option.getOrThrow(failed).pending, 1)
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.isTrue(Option.isSome(otherDrained))
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.state, "Failed")
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.deepStrictEqual(logs.defects(), ["undecodable response"])

      yield* space.activate
      yield* VirtualTime.advanceUntil(attempts.reached(2))
      undecodable = false
      yield* space.deactivate

      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(drained))
      assert.strictEqual((yield* replica.status).counts.failed, 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports a background space as failed after a storage statement died in its turn with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId])
      const logs = captureLogs()
      services.dieNext(claimStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)

      const failed = yield* eventually(services, space, isFailed)

      assert.strictEqual(failureMessage(failed), "ProtocolInvalid")
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.deepStrictEqual(logs.defects(), ["injected statement defect"])

      yield* space.activate
      yield* space.deactivate

      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "stops a background turn without reporting a defect when the replica scope closes with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId])
      const attempts = yield* makeAttempts
      const logs = captureLogs()
      const interrupted = yield* Deferred.make<void>()
      const replicaScope = yield* Scope.make()
      yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () =>
          attempts.record.pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))
          )
      })).pipe(Effect.provide(logs.layerLogs), Scope.provide(replicaScope))
      yield* VirtualTime.advanceUntil(attempts.reached(1))

      yield* Scope.close(replicaScope, Exit.void)

      assert.isTrue(yield* Deferred.isDone(interrupted))
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isNone(retried))
      assert.strictEqual(attempts.count(), 1)
      assert.deepStrictEqual(logs.defects(), [])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "keeps retrying other background spaces after the retry scheduler died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      const attempts = yield* makeAttempts
      const logs = captureLogs()
      const releaseOther = yield* Deferred.make<void>()
      let transportWaitDies = true
      let otherPulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForTransportChange: () => {
          if (transportWaitDies) return Effect.die("transport wait died")
          return Effect.never
        },
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId === spaceId) {
            if (attempts.count() === 0) return Effect.andThen(attempts.record, serverUnavailable)
            return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          }
          otherPulls += 1
          if (otherPulls === 1) return Effect.andThen(Deferred.await(releaseOther), serverUnavailable)
          return emptyPage(services.crypto, request)
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))

      const failed = yield* eventually(services, space, isFailed)
      transportWaitDies = false
      yield* Deferred.succeed(releaseOther, undefined)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.isTrue(Option.isSome(otherDrained), "the other space retried and drained")
      assert.strictEqual(failureMessage(failed), "ProtocolInvalid")
      assert.deepStrictEqual(logs.defects(), ["transport wait died"])

      yield* space.activate
      yield* space.deactivate

      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

const foregroundSpaces = Effect.fnUntraced(
  function*(
    services: BackgroundReplica.Services,
    remote: BackgroundReplica.Remote,
    _layerLogs: Layer.Layer<never>
  ) {
    const replica = yield* services.start(remote)
    const space = yield* replica.space(spaceId)
    const other = yield* replica.space(otherSpaceId)
    yield* services.sql`UPDATE effect_local_client_spaces
      SET replication_view_id = ${viewId}, replication_view_revision = 0`
    yield* space.activate
    yield* other.activate
    const online = yield* eventually(services, space, isOnlineDrained)
    const otherOnline = yield* eventually(services, other, isOnlineDrained)
    assert.isTrue(Option.isSome(online) && Option.isSome(otherOnline), "both spaces came online")
    return { replica, space, other }
  },
  (effect, _services, _remote, layerLogs) => Effect.provide(effect, layerLogs)
)

describe("foreground sync that dies", () => {
  it.effect.each(constructors)(
    "reports a foreground space whose sync died as failed and drains it after the next mutation with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      let undecodable = false
      const { other, space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (request.spaceId === spaceId && undecodable) return Effect.die("undecodable response")
            return emptyPage(services.crypto, request)
          }
        }),
        logs.layerLogs
      )
      undecodable = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      const failed = yield* eventually(services, space, isFailed)
      yield* other.mutate(Domain.PutTodo, Domain.todo("other"))
      const otherDrained = yield* eventually(services, other, isOnlineDrained)

      assert.strictEqual(failureMessage(failed), "ProtocolInvalid")
      assert.strictEqual(Option.getOrThrow(failed).pending, 1)
      assert.deepStrictEqual(logs.defects(), ["undecodable response"])
      assert.isTrue(Option.isSome(otherDrained))

      undecodable = false
      yield* space.mutate(Domain.PutTodo, Domain.todo("second"))

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports a foreground space whose watch died as failed and still drains its next mutation with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      const watchDies = yield* Deferred.make<void>()
      const { other, space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            return Deferred.await(watchDies).pipe(Effect.andThen(Effect.die("undecodable wake")), Stream.fromEffect)
          }
        }),
        logs.layerLogs
      )
      yield* Deferred.succeed(watchDies, undefined)

      const failed = yield* eventually(services, space, isFailed)
      yield* other.mutate(Domain.PutTodo, Domain.todo("other"))
      const otherDrained = yield* eventually(services, other, isOnlineDrained)

      assert.strictEqual(failureMessage(failed), "ProtocolInvalid")
      assert.deepStrictEqual(logs.defects(), ["undecodable wake"])
      assert.isTrue(Option.isSome(otherDrained))

      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const layerServer = ServerStore.layerTrusted({ definition: Domain.definition, migration }).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer)
)
const layerClientDatabase = Layer.mergeAll(
  ConnectionLane.makeLayer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const inMemoryScheduler = Effect.fnUntraced(function*(faults: {
  readonly pull: Effect.Effect<void>
  readonly watch: Effect.Effect<void>
}) {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const logs = captureLogs()
  const statuses = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
  const remote = SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => server.admitBatch(request, null),
    pull: (request) => Effect.andThen(faults.pull, server.pull(request)),
    bootstrap: server.bootstrap,
    watch: () => Stream.fromEffect(faults.watch).pipe(Stream.drain)
  })
  const context = yield* Layer.build(
    Reconciler.layer({
      definition: Domain.definition,
      spaceId,
      retryDelay: "1 second",
      maximumRetryDelay: "1 second",
      onStatusChange: (status) => Queue.offer(statuses, status).pipe(Effect.asVoid)
    }).pipe(
      Layer.provideMerge(
        LocalStore.layer({
          definition: Domain.definition,
          spaceId,
          clientId,
          scope,
          retainedReceipts: 256,
          maximumReceipts: 10_000,
          retainedHistoryEntries: 256,
          maximumBootstrapEntities: 10_000,
          maximumBootstrapBytes: 64 * 1024 * 1024,
          maximumBootstrapPageBytes: 4 * 1024 * 1024,
          migration
        }).pipe(Layer.provide(layerRuntime), Layer.provide(layerClientDatabase))
      ),
      Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
      Layer.provide(logs.layerLogs)
    )
  )
  const awaitStatus = (matches: (status: ReplicaStatus.ReplicaStatus) => boolean) =>
    Queue.take(statuses).pipe(
      Effect.repeat({ until: matches }),
      VirtualTime.advanceUntil,
      Effect.timeoutOption("5 minutes")
    )
  const online = yield* awaitStatus((status) => status._tag === "Online")
  assert.isTrue(Option.isSome(online), "the space came online")
  return {
    reconciler: Context.get(context, Reconciler.Reconciler),
    local: Context.get(context, LocalStore.Store),
    awaitStatus,
    logs
  }
})

const reportedFailure = (status: Option.Option<ReplicaStatus.ReplicaStatus>) =>
  status.pipe(
    Option.flatMap((current) => {
      if (current._tag !== "Failed") return Option.none()
      return Option.some(current.message)
    }),
    Option.getOrElse(() => "the space never reported a failure")
  )

describe("in-memory scheduler loops that die", () => {
  it.effect(
    "reports a sync that died as failed and drains the next scheduled sync",
    Effect.fnUntraced(function*() {
      let undecodable = false
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (undecodable) return Effect.die("undecodable response")
          return Effect.void
        }),
        watch: Effect.never
      })
      undecodable = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const failed = yield* awaitStatus((status) => status._tag === "Failed")

      assert.strictEqual(reportedFailure(failed), "ProtocolInvalid")
      assert.deepStrictEqual(logs.defects(), ["undecodable response"])

      undecodable = false
      yield* local.mutate(Domain.PutTodo, Domain.todo("second"))
      yield* reconciler.schedule

      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect(
    "reports a watch that died as failed and still drains the next scheduled sync",
    Effect.fnUntraced(function*() {
      const watchDies = yield* Deferred.make<void>()
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchDies), Effect.die("undecodable wake"))
      })
      yield* Deferred.succeed(watchDies, undefined)

      const failed = yield* awaitStatus((status) => status._tag === "Failed")

      assert.strictEqual(reportedFailure(failed), "ProtocolInvalid")
      assert.deepStrictEqual(logs.defects(), ["undecodable wake"])

      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})
