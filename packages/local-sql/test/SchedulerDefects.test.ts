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
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
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

const twoSpaces = (constructor: Constructor, maximumRetryDelay: Duration.Input = "1 second") =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay
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
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId !== spaceId) return emptyPage(services.crypto, request)
          if (attempts.count() < 2) return Effect.andThen(attempts.record, Effect.die("undecodable response"))
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      }))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)

      const otherDrained = yield* eventually(services, other, isDrained)
      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(otherDrained))
      assert.isAbove(attempts.count(), 2)
      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports a background space whose turn died as failed until a retry drains it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      const attempts = yield* makeAttempts
      const logs = captureLogs()
      const healed = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId !== spaceId) return emptyPage(services.crypto, request)
          if (attempts.count() === 0) return Effect.andThen(attempts.record, Effect.die("undecodable response"))
          return attempts.record.pipe(
            Effect.andThen(Deferred.await(healed)),
            Effect.andThen(emptyPage(services.crypto, request))
          )
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(1))

      const failed = yield* eventually(services, space, isFailed)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
      assert.strictEqual(Option.getOrThrow(failed).pending, 1)
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.isTrue(Option.isSome(otherDrained))
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.state, "Failed")
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.deepStrictEqual(logs.defects(), ["undecodable response"])

      yield* Deferred.succeed(healed, undefined)

      const drained = yield* eventually(services, space, isDrained)

      assert.isTrue(Option.isSome(drained))
      assert.strictEqual((yield* replica.status).counts.failed, 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "retries and drains a background space after a storage statement died in its turn with %s",
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
      const activation = yield* space.activation
      const drained = yield* eventually(services, space, isDrained)

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
      assert.strictEqual(activation, "Inactive")
      assert.deepStrictEqual(logs.defects(), ["injected statement defect"])
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

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
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

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
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

      assert.strictEqual(reportedFailure(failed), "UnexpectedFailure")
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

      assert.strictEqual(reportedFailure(failed), "UnexpectedFailure")
      assert.deepStrictEqual(logs.defects(), ["undecodable wake"])

      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

const membershipPendingCountStatement = "SELECT COUNT(p.mutation_id) AS count"

const settle = (duration: "5 seconds" | "5 minutes" | "10 minutes") =>
  VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

describe("a background turn that dies settles like a typed failure", () => {
  it.effect.each(constructors)(
    "keeps a space idle when queued background work died against the foreground runtime after the foreground reconciled with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      let offline = false
      let pullDies = false
      let workerGate: Deferred.Deferred<void> | undefined
      const workerParked = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.suspend(() => {
          const held = workerGate
          if (held === undefined) return Effect.succeed(0)
          workerGate = undefined
          return Deferred.succeed(workerParked, undefined).pipe(Effect.andThen(Deferred.await(held)), Effect.as(0))
        }),
        submitBatch: (request) => {
          if (offline) return Effect.fail(new ReplicaError.ServerUnavailable())
          return acceptSubmission(request)
        },
        pull: (request) => {
          if (request.spaceId === spaceId && pullDies) {
            pullDies = false
            return Effect.die("undecodable response")
          }
          return emptyPage(services.crypto, request)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const parked = yield* eventually(services, space, (status) => status._tag === "Offline" && status.pending === 1)
      assert.isTrue(Option.isSome(parked), "the mutation stayed pending while the server was unreachable")
      const releaseWorker = yield* Deferred.make<void>()
      workerGate = releaseWorker
      yield* space.deactivate
      yield* VirtualTime.advanceUntil(Deferred.await(workerParked))
      offline = false
      yield* space.activate
      const reconciled = yield* eventually(services, space, isOnlineDrained)
      assert.isTrue(Option.isSome(reconciled), "the foreground reconciled the space")
      yield* settle("5 minutes")

      pullDies = true
      yield* Deferred.succeed(releaseWorker, undefined)
      yield* settle("5 seconds")
      assert.isFalse(pullDies, "the queued background work ran and died")
      const whileActive = yield* space.status
      yield* space.deactivate
      yield* settle("5 minutes")
      const afterDeactivation = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(whileActive._tag, "Online")
      assert.strictEqual(whileActive.pending, 0)
      assert.strictEqual(afterDeactivation._tag, "Idle")
      assert.strictEqual(aggregate.counts.failed, 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "keeps a drained space idle when the pending count after its successful background turn died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const attempts = yield* makeAttempts
      services.dieNext(membershipPendingCountStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => Effect.andThen(attempts.record, emptyPage(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(attempts.reached(2))
      yield* settle("5 minutes")

      const status = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(status.pending, 0)
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(aggregate.counts.failed, 0)
    }, VirtualTime.scoped)
  )
})

describe("turns that die retry with the normal backoff", () => {
  it.effect.each(constructors)(
    "drains a mutation admitted while a foreground sync was running that then died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      const syncing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let dies = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (request.spaceId !== spaceId || !dies) return emptyPage(services.crypto, request)
            dies = false
            return Deferred.succeed(syncing, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(Effect.die("undecodable response"))
            )
          }
        }),
        logs.layerLogs
      )
      dies = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* VirtualTime.advanceUntil(Deferred.await(syncing))
      yield* space.mutate(Domain.PutTodo, Domain.todo("second"))
      yield* Deferred.succeed(release, undefined)

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.deepStrictEqual(logs.defects(), ["undecodable response"])
      assert.isTrue(Option.isSome(drained), "both mutations drained without another trigger")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "spaces the retries of a foreground sync that keeps dying by the backoff with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      const logs = captureLogs()
      let pulls = 0
      let dies = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (request.spaceId !== spaceId || !dies) return emptyPage(services.crypto, request)
            pulls += 1
            return Effect.die("undecodable response")
          }
        }),
        logs.layerLogs
      )
      dies = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      yield* settle("10 minutes")

      assert.isAtLeast(pulls, 6)
      assert.isAtMost(pulls, 20)
      assert.strictEqual(logs.defects().length, pulls)
      assert.strictEqual((yield* space.status)._tag, "Failed")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "spaces the retries of a background sync that keeps dying by the backoff with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const logs = captureLogs()
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => {
          pulls += 1
          return Effect.die("undecodable response")
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)

      yield* settle("10 minutes")

      assert.isAtLeast(pulls, 6)
      assert.isAtMost(pulls, 20)
      assert.strictEqual(logs.defects().length, pulls)
      assert.strictEqual((yield* space.status)._tag, "Failed")
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries a sync that died in the in-memory scheduler without another trigger",
    Effect.fnUntraced(function*() {
      let undecodable = false
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (!undecodable) return Effect.void
          undecodable = false
          return Effect.die("undecodable response")
        }),
        watch: Effect.never
      })
      undecodable = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const failed = yield* awaitStatus((status) => status._tag === "Failed")
      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.strictEqual(reportedFailure(failed), "UnexpectedFailure")
      assert.deepStrictEqual(logs.defects(), ["undecodable response"])
      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

const describeSpaceStatus = (status: Option.Option<ReplicaStatus.SpaceStatus>, fallback: ReplicaStatus.SpaceStatus) => {
  const current = Option.getOrElse(status, () => fallback)
  if (current._tag === "Failed") return `Failed: ${current.message}, pending ${current.pending}`
  return `${current._tag}, pending ${current.pending}`
}

const offlineReplica = Effect.fnUntraced(function*(constructor: Constructor, transportWaitDies: boolean) {
  const services = yield* withPending(constructor, [spaceId, otherSpaceId])
  const bothFailed = yield* Deferred.make<void>()
  const trigger = yield* Deferred.make<void>()
  let offline = true
  let pulls = 0
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    waitForTransportChange: () => {
      if (!transportWaitDies) return Effect.never
      return Effect.andThen(Deferred.await(trigger), Effect.die("transport wait died"))
    },
    submitBatch: acceptSubmission,
    pull: (request) => {
      pulls += 1
      if (!offline) return emptyPage(services.crypto, request)
      let reached = Effect.void
      if (pulls === 2) reached = Deferred.succeed(bothFailed, undefined).pipe(Effect.asVoid)
      return Effect.andThen(reached, serverUnavailable)
    }
  }))
  const space = yield* replica.space(spaceId)
  const other = yield* replica.space(otherSpaceId)
  yield* VirtualTime.advanceUntil(Deferred.await(bothFailed))
  yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 millis"))
  assert.strictEqual(pulls, 2, "each space failed one turn and is waiting in the retry schedule")
  const reconnect = Effect.sync(() => {
    offline = false
  })
  return { services, space, other, reconnect, trigger: Deferred.succeed(trigger, undefined) }
})

describe("background retries held when the retry scheduler dies", () => {
  it.effect.each(constructors)(
    "drains two offline spaces after the server returns when nothing died with %s",
    Effect.fnUntraced(function*(constructor) {
      const { other, reconnect, services, space } = yield* offlineReplica(constructor, false)
      yield* reconnect

      const drained = yield* eventually(services, space, isDrained)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Idle, pending 0")
      assert.strictEqual(describeSpaceStatus(otherDrained, yield* other.status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "drains two offline spaces after the server returns when the retry scheduler died once with %s",
    Effect.fnUntraced(function*(constructor) {
      const { other, reconnect, services, space, trigger } = yield* offlineReplica(constructor, true)
      yield* reconnect
      yield* trigger

      const drained = yield* eventually(services, space, isDrained)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Idle, pending 0")
      assert.strictEqual(describeSpaceStatus(otherDrained, yield* other.status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "spaces retries by the backoff while the transport wait keeps dying with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const logs = captureLogs()
      let pulls = 0
      yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForTransportChange: () => Effect.die("transport wait died"),
        pull: () => {
          pulls += 1
          return serverUnavailable
        }
      })).pipe(Effect.provide(logs.layerLogs))

      yield* settle("10 minutes")

      assert.isAtLeast(pulls, 6)
      assert.isAtMost(pulls, 20)
      assert.isAtMost(logs.defects().length, pulls)
    }, VirtualTime.scoped)
  )
})

const defectWithInterrupt = Cause.combine(Cause.die("undecodable response"), Cause.interrupt())

describe("background turns that end with a defect and an interrupt in one cause", () => {
  it.effect.each(constructors)(
    "keeps draining background spaces after two turns ended with a defect and an interrupt with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      let mixed = true
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (mixed) return Effect.failCause(defectWithInterrupt)
          return emptyPage(services.crypto, request)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* settle("5 seconds")
      mixed = false
      const cycle = yield* Effect.forkChild(Effect.andThen(space.activate, space.deactivate), {
        startImmediately: true
      })

      const drained = yield* eventually(services, space, (status) => status._tag === "Idle" && status.pending === 0)
      yield* Fiber.interrupt(cycle)

      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )
})
