import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
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
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as Configuration from "../src/internal/configuration.js"
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
  const errors = () => entries.filter((entry) => entry.logLevel === "Error").length
  const errorMessages = () =>
    entries.flatMap((entry) => {
      if (entry.logLevel !== "Error") return []
      let message: unknown = entry.message
      if (Array.isArray(message)) message = message[0]
      return [String(message)]
    })
  return { layerLogs: Logger.layer([logger]), defects, errors, errorMessages }
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
const inMemoryScheduler = Effect.fnUntraced(function*(faults: {
  readonly pull: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watch: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly waitForCredentialChange?: Effect.Effect<void>
  readonly waitForTransportChange?: Effect.Effect<void>
  readonly transportGeneration?: Effect.Effect<number>
  readonly everyWatch?: boolean
}) {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const database = yield* Layer.build(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))
  const sql = Context.get(database, SqlClient.SqlClient)
  let dying: string | undefined
  let locked: string | undefined
  const dyingSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const source: unknown = args[0]
      if (!Array.isArray(source)) return Reflect.apply(target, thisArg, args)
      const text = source.join("?")
      if (dying !== undefined && text.includes(dying)) return Effect.die("injected statement defect")
      if (locked !== undefined && text.includes(locked)) {
        const reason = new SqlError.LockTimeoutError({ cause: "injected lock timeout" })
        return Effect.fail(new SqlError.SqlError({ reason }))
      }
      return Reflect.apply(target, thisArg, args)
    }
  })
  const layerDyingSql = Layer.succeedContext(Context.add(database, SqlClient.SqlClient, dyingSql))
  const layerClientDatabase = Layer.mergeAll(
    ConnectionLane.makeLayer().pipe(Layer.provideMerge(layerDyingSql)),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
  const logs = captureLogs()
  const statuses = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
  let subscriptions = 0
  const watchTimes = yield* Queue.unbounded<number>()
  const remote = SyncEngine.SyncEngine.of({
    ...idleRemote,
    waitForCredentialChange: () => faults.waitForCredentialChange ?? Effect.never,
    waitForTransportChange: () => faults.waitForTransportChange ?? Effect.never,
    transportGeneration: faults.transportGeneration ?? Effect.succeed(0),
    submitBatch: (request) => server.admitBatch(request, null),
    pull: (request) => Effect.andThen(faults.pull, server.pull(request)),
    bootstrap: server.bootstrap,
    watch: () => {
      subscriptions += 1
      if (subscriptions > 1 && faults.everyWatch !== true) return Stream.never
      const subscribed = Clock.currentTimeMillis.pipe(Effect.flatMap((now) => Queue.offer(watchTimes, now)))
      return Stream.fromEffect(Effect.andThen(subscribed, faults.watch)).pipe(Stream.drain)
    }
  })
  const context = yield* Layer.build(
    Reconciler.layer({
      definition: Domain.definition,
      spaceId,
      retryDelay: "1 second",
      maximumRetryDelay: "1 minute",
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
  const reconciler = Context.get(context, Reconciler.Reconciler)
  return {
    reconciler,
    local: Context.get(context, LocalStore.Store),
    awaitStatus,
    forgetStatuses: Queue.clear(statuses),
    logs,
    subscriptions: () => subscriptions,
    watchTimes,
    dieOn: (statement: string | undefined) => {
      dying = statement
    },
    lockOn: (statement: string | undefined) => {
      locked = statement
    }
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

const settle = (
  duration: "10 millis" | "5 seconds" | "30 seconds" | "1 minute" | "2 minutes" | "5 minutes" | "10 minutes"
) => VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

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
  it.effect.each(
    [
      ["layer", false],
      ["layer", true],
      ["layerWorkflow", false],
      ["layerWorkflow", true]
    ] as const
  )(
    "drains two offline spaces with %s after the server returns, retry scheduler died once: %s",
    Effect.fnUntraced(function*([constructor, schedulerDies]) {
      const { other, reconnect, services, space, trigger } = yield* offlineReplica(constructor, schedulerDies)
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
      const failed = yield* eventually(services, space, isFailed)
      yield* settle("5 seconds")
      mixed = false

      const drained = yield* eventually(services, space, isDrained)

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Idle, pending 0")
    }, VirtualTime.scoped)
  )
})

const credentialRejected = Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))

const needsAuthentication = (status: ReplicaStatus.ReplicaStatus) => status._tag === "NeedsAuthentication"

const makeCredentialProvider = () => {
  const state = { waits: 0, healsAfter: Number.POSITIVE_INFINITY }
  const waitForChange = () =>
    Effect.suspend(() => {
      state.waits += 1
      if (state.waits > state.healsAfter) return Effect.void
      return Effect.die("credential wait died")
    })
  return { state, waitForChange }
}

describe("credential waits that die", () => {
  it.effect.each(constructors)(
    "waits again without sending the rejected credential after a background credential wait died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const logs = captureLogs()
      const provider = makeCredentialProvider()
      let rejected = true
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: provider.waitForChange,
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (rejected) return credentialRejected
          return emptyPage(services.crypto, request)
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      const paused = yield* eventually(services, space, needsAuthentication)
      assert.isTrue(Option.isSome(paused), "the space asked for a new credential")
      yield* settle("5 minutes")
      const sent = pulls
      const waited = provider.state.waits
      rejected = false
      provider.state.healsAfter = provider.state.waits

      const drained = yield* eventually(services, space, isDrained)

      assert.strictEqual(sent, 1)
      assert.isAbove(waited, 1)
      assert.deepStrictEqual(logs.defects(), ["credential wait died"])
      assert.isTrue(Option.isSome(drained), "the space drained once the provider reported a change")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "waits again without sending the rejected credential after a foreground credential wait died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      const logs = captureLogs()
      const provider = makeCredentialProvider()
      let rejected = false
      let rejections = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          waitForCredentialChange: provider.waitForChange,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!rejected || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            rejections += 1
            return credentialRejected
          }
        }),
        logs.layerLogs
      )
      rejected = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const paused = yield* eventually(services, space, needsAuthentication)
      assert.isTrue(Option.isSome(paused), "the space asked for a new credential")
      yield* settle("5 minutes")
      const sent = rejections
      const waited = provider.state.waits
      rejected = false
      provider.state.healsAfter = provider.state.waits

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(sent, 1)
      assert.isAbove(waited, 1)
      assert.deepStrictEqual(logs.defects(), ["credential wait died"])
      assert.isTrue(Option.isSome(drained), "the space drained once the provider reported a change")
    }, VirtualTime.scoped)
  )

  it.effect(
    "waits again without sending the rejected credential after an in-memory credential wait died",
    Effect.fnUntraced(function*() {
      const provider = makeCredentialProvider()
      let rejected = false
      let rejections = 0
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (!rejected) return Effect.void
          rejections += 1
          return credentialRejected
        }),
        watch: Effect.never,
        waitForCredentialChange: provider.waitForChange()
      })
      rejected = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      const paused = yield* awaitStatus(needsAuthentication)
      assert.isTrue(Option.isSome(paused), "the space asked for a new credential")
      yield* settle("5 minutes")
      const sent = rejections
      const waited = provider.state.waits
      rejected = false
      provider.state.healsAfter = provider.state.waits

      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.strictEqual(sent, 1)
      assert.isAbove(waited, 1)
      assert.deepStrictEqual(logs.defects(), ["credential wait died"])
      assert.isTrue(Option.isSome(drained), "the space drained once the provider reported a change")
    }, VirtualTime.scoped)
  )
})

describe("retry backoffs that die", () => {
  it.effect(
    "retries a foreground sync after its retry backoff died in the transport wait",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let offline = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          waitForTransportChange: () => Effect.die("transport wait died"),
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!offline || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            offline = false
            return serverUnavailable
          }
        }),
        logs.layerLogs
      )
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.deepStrictEqual(logs.defects(), ["transport wait died"])
      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Online, pending 0")
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries an in-memory sync after its retry backoff died in the transport wait",
    Effect.fnUntraced(function*() {
      let offline = false
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (!offline) return Effect.void
          offline = false
          return serverUnavailable
        }),
        watch: Effect.never,
        waitForTransportChange: Effect.die("transport wait died")
      })
      offline = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.deepStrictEqual(logs.defects(), ["transport wait died"])
      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

describe("a leave whose cleanup dies", () => {
  it.effect.each(constructors)(
    "lets a space be joined and left again after its leave cleanup died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      services.dieNext("DELETE FROM effect_local_client_spaces")
      const left = yield* replica.leave(spaceId).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(left) && Cause.hasDies(left.cause), "the leave died")

      const rejoined = yield* replica.join(spaceId).pipe(Effect.exit)
      const leftAgain = yield* replica.leave(spaceId).pipe(Effect.exit)
      const remembered = yield* replica.space(spaceId).pipe(Effect.exit)

      assert.isTrue(Exit.isSuccess(rejoined), "joining again succeeded")
      assert.isTrue(Exit.isSuccess(leftAgain), "leaving again succeeded")
      assert.isTrue(Exit.isFailure(remembered), "the space is no longer joined")
    }, VirtualTime.scoped)
  )
})

describe("watches that die subscribe again", () => {
  it.effect.each(constructors)(
    "spaces the subscriptions of a foreground watch that keeps dying by the backoff with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      const logs = captureLogs()
      let dies = false
      let subscriptions = 0
      const firstWatchEnds = yield* Deferred.make<void>()
      yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            if (!dies) return Stream.fromEffect(Deferred.await(firstWatchEnds)).pipe(Stream.drain)
            subscriptions += 1
            return Stream.die("undecodable wake")
          }
        }),
        logs.layerLogs
      )
      dies = true
      yield* Deferred.succeed(firstWatchEnds, undefined)

      yield* settle("10 minutes")

      assert.isAtLeast(subscriptions, 6)
      assert.isAtMost(subscriptions, 20)
      assert.strictEqual(logs.defects().length, subscriptions)
    }, VirtualTime.scoped)
  )
})

const storePendingCountStatement = "SELECT COUNT(*) AS count FROM effect_local_client_pending_data"
const noStatement = "no statement contains this text"

describe("inner work that ends by interruption only", () => {
  it.effect.each(constructors)(
    "subscribes again without reporting a failure after a foreground watch ended by interruption with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      const watchEnds = yield* Deferred.make<void>()
      let subscriptions = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            subscriptions += 1
            if (subscriptions > 1) return Stream.never
            return Deferred.await(watchEnds).pipe(Effect.andThen(Effect.interrupt), Stream.fromEffect)
          }
        }),
        logs.layerLogs
      )
      yield* Deferred.succeed(watchEnds, undefined)
      yield* settle("5 minutes")

      assert.strictEqual(subscriptions, 2)
      assert.strictEqual(logs.errors(), 0)
      const status = yield* space.status
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
    }, VirtualTime.scoped)
  )

  it.effect(
    "subscribes again without reporting a failure after an in-memory watch ended by interruption",
    Effect.fnUntraced(function*() {
      const watchEnds = yield* Deferred.make<void>()
      const { logs, reconciler, subscriptions } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchEnds), Effect.interrupt)
      })
      yield* Deferred.succeed(watchEnds, undefined)
      yield* settle("5 minutes")

      assert.strictEqual(subscriptions(), 2)
      assert.strictEqual(logs.errors(), 0)
      assert.strictEqual((yield* reconciler.status)._tag, "Online")
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries a foreground sync that ended by interruption as an unreachable server",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let cancelled = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!cancelled || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            cancelled = false
            return Effect.interrupt
          }
        }),
        logs.layerLogs
      )
      cancelled = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      const offline = yield* eventually(services, space, (status) => status._tag === "Offline")
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(offline), "the cancelled sync was reported as an unreachable server")
      assert.isTrue(Option.isSome(drained), "the retry drained the space")
      assert.strictEqual(logs.errors(), 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports nothing and schedules no retry when the replica scope closes while a foreground turn is in flight with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logged: Array<string> = []
      const logger = Logger.make<unknown, void>((entry) => {
        logged.push(entry.logLevel)
      })
      const pulling = yield* Deferred.make<void>()
      const replicaScope = yield* Scope.make()
      let hangs = false
      let notifications = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (request.spaceId !== spaceId || !hangs) return emptyPage(services.crypto, request)
            return Effect.andThen(Deferred.succeed(pulling, undefined), Effect.never)
          }
        }),
        Logger.layer([logger])
      ).pipe(Scope.provide(replicaScope))
      hangs = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* VirtualTime.advanceUntil(Deferred.await(pulling))
      yield* settle("5 seconds")
      logged.length = 0
      services.reactivity.registerUnsafe([ReactivityKey.status(spaceId)], () => {
        notifications += 1
      })

      yield* Scope.close(replicaScope, Exit.void)
      yield* settle("5 minutes")

      assert.deepStrictEqual({ logged, notifications }, { logged: [], notifications: 0 })
    }, VirtualTime.scoped)
  )
})

describe("a watch that recovered", () => {
  it.effect.each(
    [
      ["layer", "died"],
      ["layer", "failed on unavailable storage"],
      ["layerWorkflow", "died"],
      ["layerWorkflow", "failed on unavailable storage"]
    ] as const
  )(
    "reports the space online again with %s after its watch %s once and the next watch stayed open",
    Effect.fnUntraced(function*([constructor, ending]) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      const watchEnds = yield* Deferred.make<void>()
      let ended: Effect.Effect<never, ReplicaError.ReplicaError> = Effect.die("undecodable wake")
      if (ending !== "died") ended = Effect.fail(new ReplicaError.StorageUnavailable({ cause: "injected" }))
      let subscriptions = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            subscriptions += 1
            if (subscriptions > 1) return Stream.never
            return Deferred.await(watchEnds).pipe(Effect.andThen(ended), Stream.fromEffect)
          }
        }),
        logs.layerLogs
      )
      yield* Deferred.succeed(watchEnds, undefined)
      const failed = yield* eventually(services, space, isFailed)

      const recovered = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(failed), "the watch failure was reported")
      assert.strictEqual(subscriptions, 2)
      assert.isTrue(Option.isSome(recovered), "the space reported online again without another trigger")
    }, VirtualTime.scoped)
  )

  it.effect(
    "reports the space online again after an in-memory watch died once and the next watch stayed open",
    Effect.fnUntraced(function*() {
      const watchDies = yield* Deferred.make<void>()
      const { awaitStatus, subscriptions } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchDies), Effect.die("undecodable wake"))
      })
      yield* Deferred.succeed(watchDies, undefined)

      const failed = yield* awaitStatus((status) => status._tag === "Failed")
      const recovered = yield* awaitStatus((status) => status._tag === "Online")

      assert.isTrue(Option.isSome(failed), "the watch death was reported")
      assert.strictEqual(subscriptions(), 2)
      assert.isTrue(Option.isSome(recovered), "the space reported online again without another trigger")
    }, VirtualTime.scoped)
  )
})

describe("a runtime close whose pending count failed", () => {
  it.effect.each(constructors)(
    "reports the replica idle after the release that followed a successful background turn died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId])
      services.dieNext(membershipPendingCountStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      yield* settle("5 minutes")

      const status = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.deepStrictEqual(
        { state: aggregate.state, idle: aggregate.counts.idle, online: aggregate.counts.online },
        { state: "Idle", idle: 2, online: 0 }
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(["die", "typed failure"] as const)(
    "drains the pending mutation of a foreground space after its leave ended with a %s while closing the runtime",
    Effect.fnUntraced(function*(kind) {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let offline = false
      const { replica, space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: (request) => {
            if (offline) return serverUnavailable
            return acceptSubmission(request)
          },
          pull: (request) => emptyPage(services.crypto, request)
        }),
        logs.layerLogs
      )
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const parked = yield* eventually(services, space, (status) => status._tag === "Offline" && status.pending === 1)
      assert.isTrue(Option.isSome(parked), "the mutation stayed pending while the server was unreachable")
      if (kind === "die") services.dieNext(membershipPendingCountStatement)
      else services.lockNext(membershipPendingCountStatement)

      const left = yield* replica.leave(spaceId).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(left), "the leave did not complete")
      offline = false
      yield* settle("5 minutes")

      const status = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      assert.strictEqual(aggregate.totalPending, 0)
    }, VirtualTime.scoped)
  )
})

describe("a leave whose notification died after the membership row was deleted", () => {
  it.effect.each(constructors)(
    "leaves a space that can be joined, activated and left again with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      let throwing = true
      let listChanges = 0
      const unregister = services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        if (throwing) decodeURIComponent("%")
      })
      services.reactivity.registerUnsafe([ReactivityKey.spaces], () => {
        listChanges += 1
      })
      const left = yield* replica.leave(spaceId).pipe(Effect.exit)
      throwing = false
      const announced = listChanges
      const rows = yield* services.sql`SELECT space_id FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      const afterLeave = yield* replica.status
      const listedAfterLeave = yield* replica.spaces

      const rejoined = yield* replica.join(spaceId)
      const activation = yield* rejoined.activate.pipe(Effect.exit)
      const leftAgain = yield* replica.leave(spaceId).pipe(Effect.exit)
      const aggregate = yield* replica.status
      const listed = yield* replica.spaces
      unregister()

      assert.isTrue(Exit.isFailure(left) && Cause.hasDies(left.cause), "the caller received the subscriber defect")
      assert.strictEqual(rows.length, 0, "the membership row was deleted")
      assert.strictEqual(announced, 1, "the change of the space list was announced")
      assert.strictEqual(afterLeave.spaces, 1)
      assert.strictEqual(listedAfterLeave.length, 1)
      assert.isTrue(Exit.isSuccess(activation), "the joined space could be activated")
      assert.isTrue(Exit.isSuccess(leftAgain), "the space could be left again")
      assert.strictEqual(aggregate.spaces, listed.length)
    }, VirtualTime.scoped)
  )
})

const requestReconciliationStatement = "SET requested_generation = "

describe("steps of a managed space that run outside its turn", () => {
  it.effect.each(["die", "typed failure"] as const)(
    "syncs again on its own after the readmission that follows a retry backoff ended with a %s",
    Effect.fnUntraced(function*(kind) {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let unavailable = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (unavailable && request.spaceId === spaceId) {
              return Effect.fail(new ReplicaError.StorageUnavailable({ cause: "injected" }))
            }
            return emptyPage(services.crypto, request)
          }
        }),
        logs.layerLogs
      )
      unavailable = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const failed = yield* eventually(services, space, isFailed)
      assert.isTrue(Option.isSome(failed), "the turn failed and a retry was scheduled")
      if (kind === "die") services.dieNext(requestReconciliationStatement, 3)
      else services.lockNext(requestReconciliationStatement, 3)
      yield* settle("30 seconds")
      services.dieNext(noStatement)
      services.lockNext(noStatement)
      unavailable = false

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Online, pending 0")
    }, VirtualTime.scoped)
  )

  it.effect(
    "syncs the next mutation after the transport generation read at the start of a turn died",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let dies = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          transportGeneration: Effect.suspend(() => {
            if (dies) return Effect.die("generation died")
            return Effect.succeed(0)
          }),
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request)
        }),
        logs.layerLogs
      )
      dies = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const failed = yield* eventually(services, space, isFailed)
      dies = false
      yield* space.mutate(Domain.PutTodo, Domain.todo("second"))

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Online, pending 0")
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries an in-memory turn whose transport generation read died",
    Effect.fnUntraced(function*() {
      let dies = false
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.never,
        transportGeneration: Effect.suspend(() => {
          if (!dies) return Effect.succeed(0)
          dies = false
          return Effect.die("generation died")
        })
      })
      dies = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const failed = yield* awaitStatus((status) => status._tag === "Failed")
      const drained = yield* awaitStatus((status) => status._tag === "Online" && status.pending === 0)

      assert.strictEqual(reportedFailure(failed), "UnexpectedFailure")
      assert.deepStrictEqual(logs.defects(), ["generation died"])
      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )
})

const storageUnavailable = Effect.fail(new ReplicaError.StorageUnavailable({ cause: "injected" }))

describe("a watch whose recovery dies", () => {
  it.effect.each(constructors)(
    "subscribes again and delivers a later wake after the sync requests that follow a failed watch died twice with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      const watchFails = yield* Deferred.make<void>()
      const wakes = yield* Deferred.make<void>()
      let subscriptions = 0
      let pulls = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (request.spaceId === spaceId) pulls += 1
            return emptyPage(services.crypto, request)
          },
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            subscriptions += 1
            if (subscriptions === 1) {
              return Deferred.await(watchFails).pipe(Effect.andThen(storageUnavailable), Stream.fromEffect)
            }
            if (subscriptions > 2) return Stream.never
            const wake = Effect.as(Deferred.await(wakes), Protocol.Wake.make({ spaceId }))
            return Stream.concat(Stream.fromEffect(wake), Stream.never)
          }
        }),
        logs.layerLogs
      )
      services.dieNext(requestReconciliationStatement, 2)
      yield* Deferred.succeed(watchFails, undefined)
      yield* settle("1 minute")
      services.dieNext(noStatement)
      const pulledBeforeWake = pulls
      yield* Deferred.succeed(wakes, undefined)

      const recovered = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(subscriptions, 2)
      assert.isAbove(pulls, pulledBeforeWake)
      assert.isTrue(Option.isSome(recovered), "the space reported online after the wake was synced")
    }, VirtualTime.scoped)
  )

  it.effect(
    "subscribes again after the sync requests that follow a failed in-memory watch died",
    Effect.fnUntraced(function*() {
      const watchFails = yield* Deferred.make<void>()
      const { awaitStatus, dieOn, forgetStatuses, subscriptions } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchFails), storageUnavailable)
      })
      dieOn(requestReconciliationStatement)
      yield* Deferred.succeed(watchFails, undefined)
      yield* settle("1 minute")
      const whileDying = subscriptions()
      dieOn(undefined)
      yield* forgetStatuses

      const recovered = yield* awaitStatus((status) => status._tag === "Online")

      assert.strictEqual(whileDying, 2, "the watch reopened while the sync request kept dying")
      assert.strictEqual(subscriptions(), 2)
      assert.isTrue(Option.isSome(recovered), "the space reported online once storage healed")
    }, VirtualTime.scoped)
  )
})

describe("a failure report whose pending count could not be read", () => {
  it.effect.each(
    [
      ["layer", "died"],
      ["layer", "failed"],
      ["layerWorkflow", "died"],
      ["layerWorkflow", "failed"]
    ] as const
  )(
    "keeps the last known pending count with %s when the count read for the report %s",
    Effect.fnUntraced(function*([constructor, ending]) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      let undecodable = false
      const { replica, space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (undecodable && request.spaceId === spaceId) return Effect.die("undecodable response")
            return emptyPage(services.crypto, request)
          }
        }),
        logs.layerLogs
      )
      undecodable = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      if (ending === "died") services.dieNext(storePendingCountStatement, 1_000_000)
      else services.lockNext(storePendingCountStatement, 1_000_000)
      yield* settle("5 seconds")
      const whileUnreadable = yield* replica.status
      services.dieNext(noStatement)
      services.lockNext(noStatement)
      const stored = yield* services.sql<{ readonly pending: number }>`SELECT COUNT(mutation_id) AS pending
        FROM effect_local_client_pending_data WHERE space_id = ${spaceId}`
      const status = yield* space.status

      const countDefects = logs.errorMessages().filter((message) =>
        message === "Pending count for a failure report died"
      )
      assert.strictEqual(countDefects.length > 0, ending === "died")
      assert.strictEqual(stored[0].pending, 1)
      assert.strictEqual(whileUnreadable.totalPending, stored[0].pending)
      assert.strictEqual(whileUnreadable.counts.failed, 1)
      assert.strictEqual(status._tag, "Failed")
      assert.strictEqual(status.pending, 1)
    }, VirtualTime.scoped)
  )

  it.effect(
    "keeps the pending count the in-memory scheduler last published when the count read for the report died",
    Effect.fnUntraced(function*() {
      let undecodable = false
      const { awaitStatus, dieOn, forgetStatuses, local, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (undecodable) return Effect.die("undecodable response")
          return Effect.void
        }),
        watch: Effect.never
      })
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      undecodable = true
      yield* reconciler.schedule
      const counted = yield* awaitStatus((status) => status._tag === "Failed")
      dieOn(storePendingCountStatement)
      yield* forgetStatuses

      const uncounted = yield* awaitStatus((status) => status._tag === "Failed")

      assert.strictEqual(Option.getOrThrow(counted).pending, 1)
      assert.strictEqual(Option.getOrThrow(uncounted).pending, 1)
    }, VirtualTime.scoped)
  )
})

const secondsBetween = (times: ReadonlyArray<number>) =>
  times.slice(1).map((time, index) => (time - times[index]) / 1000)

const failureWithDefect = Cause.combine(
  Cause.fail(new ReplicaError.StorageUnavailable({ cause: "injected" })),
  Cause.die("finalizer died")
)

describe("the background worker boundary", () => {
  it.effect.each(constructors)(
    "keeps both background workers alive after the release that follows two turns died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId, otherSpaceId])
      const logs = captureLogs()
      services.dieNext(membershipPendingCountStatement, 2)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      yield* settle("5 minutes")
      const released = logs.defects()
      yield* space.mutate(Domain.PutTodo, Domain.todo("later"))
      yield* other.mutate(Domain.PutTodo, Domain.todo("later"))
      yield* space.deactivate
      yield* other.deactivate

      const drained = yield* eventually(services, space, isDrained)
      const otherDrained = yield* eventually(services, other, isDrained)

      assert.deepStrictEqual(released, ["injected statement defect", "injected statement defect"])
      assert.isTrue(Option.isSome(drained), "a worker drained the first space")
      assert.isTrue(Option.isSome(otherDrained), "a worker drained the second space")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "closes the runtime, logs both defects and retries when a turn and its release both died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId])
      const logs = captureLogs()
      let dies = true
      services.dieNext(membershipPendingCountStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (!dies) return emptyPage(services.crypto, request)
          dies = false
          return Effect.die("undecodable response")
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const space = yield* replica.space(spaceId)
      yield* settle("10 millis")

      const failed = yield* eventually(services, space, isFailed)
      const activation = yield* space.activation
      const drained = yield* eventually(services, space, isDrained)

      assert.strictEqual(failureMessage(failed), "UnexpectedFailure")
      assert.strictEqual(activation, "Inactive")
      assert.deepStrictEqual(logs.defects(), ["undecodable response", "injected statement defect"])
      assert.isTrue(Option.isSome(drained), "the retry drained the space")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "never reports a failure when the release after a successful turn died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* withPending(constructor, [spaceId])
      services.dieNext(membershipPendingCountStatement)
      let failedReports = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const watching = yield* Effect.forkChild(
        Effect.gen(function*() {
          const changes = yield* Queue.unbounded<void>()
          services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
            Queue.offerUnsafe(changes, undefined)
          })
          while (true) {
            yield* Queue.take(changes)
            if ((yield* replica.status).counts.failed > 0) failedReports += 1
          }
        }),
        { startImmediately: true }
      )
      const space = yield* replica.space(spaceId)
      yield* settle("5 minutes")
      yield* Fiber.interrupt(watching)

      assert.strictEqual(failedReports, 0)
      assert.strictEqual((yield* space.status)._tag, "Idle")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "reports the typed failure when a foreground sync failed and a defect came with it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      let fails = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!fails || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            return Effect.failCause(failureWithDefect)
          }
        }),
        logs.layerLogs
      )
      fails = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* settle("10 millis")

      const status = yield* space.status
      fails = false

      const reported = Option.some(status)
      assert.strictEqual(failureMessage(reported), "StorageUnavailable")
    }, VirtualTime.scoped)
  )
})

describe("backoffs that keep dying or ending", () => {
  it.effect(
    "spaces the retries of a foreground sync by the backoff while its transport wait keeps dying",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layer", "1 minute")
      const logs = captureLogs()
      let offline = false
      let pulls = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          waitForTransportChange: () => Effect.die("transport wait died"),
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!offline || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            pulls += 1
            return serverUnavailable
          }
        }),
        logs.layerLogs
      )
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      yield* settle("10 minutes")

      assert.isAtLeast(pulls, 6)
      assert.isAtMost(pulls, 20)
    }, VirtualTime.scoped)
  )

  it.effect(
    "doubles the gap between the retries of an in-memory sync while its transport wait keeps dying",
    Effect.fnUntraced(function*() {
      const pullTimes = yield* Queue.unbounded<number>()
      let offline = false
      const { local, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (!offline) return Effect.void
          return Clock.currentTimeMillis.pipe(
            Effect.flatMap((now) => Queue.offer(pullTimes, now)),
            Effect.andThen(serverUnavailable)
          )
        }),
        watch: Effect.never,
        waitForTransportChange: Effect.die("transport wait died")
      })
      offline = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule

      const times = yield* VirtualTime.advanceUntil(Queue.takeN(pullTimes, 9))

      assert.deepStrictEqual(secondsBetween(times), [1, 2, 4, 8, 16, 32, 60, 60])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "spaces the subscriptions of a foreground watch that keeps ending by interruption by the backoff with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor, "1 minute")
      const logs = captureLogs()
      const firstWatchEnds = yield* Deferred.make<void>()
      let ends = false
      let subscriptions = 0
      yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            if (!ends) return Stream.fromEffect(Deferred.await(firstWatchEnds)).pipe(Stream.drain)
            subscriptions += 1
            return Stream.fromEffect(Effect.interrupt)
          }
        }),
        logs.layerLogs
      )
      ends = true
      yield* Deferred.succeed(firstWatchEnds, undefined)

      yield* settle("10 minutes")

      assert.isAtLeast(subscriptions, 6)
      assert.isAtMost(subscriptions, 20)
      assert.strictEqual(logs.errors(), 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(["died", "ended by interruption"] as const)(
    "doubles the gap between the subscriptions of an in-memory watch that keeps having %s",
    Effect.fnUntraced(function*(ending) {
      let ended: Effect.Effect<never> = Effect.die("undecodable wake")
      if (ending !== "died") ended = Effect.interrupt
      const { watchTimes } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: ended,
        everyWatch: true
      })

      const times = yield* VirtualTime.advanceUntil(Queue.takeN(watchTimes, 9))

      assert.deepStrictEqual(secondsBetween(times), [1, 2, 4, 8, 16, 32, 60, 60])
    }, VirtualTime.scoped)
  )
})

describe("a credential wait that ends by interruption", () => {
  it.effect(
    "waits again without logging an error",
    Effect.fnUntraced(function*() {
      let rejected = false
      let waits = 0
      const { awaitStatus, local, logs, reconciler } = yield* inMemoryScheduler({
        pull: Effect.suspend(() => {
          if (rejected) return credentialRejected
          return Effect.void
        }),
        watch: Effect.never,
        waitForCredentialChange: Effect.suspend(() => {
          waits += 1
          return Effect.interrupt
        })
      })
      rejected = true
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      const paused = yield* awaitStatus(needsAuthentication)
      yield* settle("5 minutes")

      assert.isTrue(Option.isSome(paused), "the space asked for a new credential")
      assert.isAbove(waits, 1)
      assert.strictEqual(logs.errors(), 0)
    }, VirtualTime.scoped)
  )
})

const ownerUnavailable = Effect.fail(new ReplicaError.OwnerUnavailable({ reason: "transport" }))

describe("a sync request after a watch failure that fails", () => {
  it.effect(
    "reports the space online again after an in-memory watch failed once and the next watch stayed open",
    Effect.fnUntraced(function*() {
      const watchFails = yield* Deferred.make<void>()
      const { awaitStatus, subscriptions } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchFails), storageUnavailable)
      })
      yield* Deferred.succeed(watchFails, undefined)

      const failed = yield* awaitStatus((status) => status._tag === "Failed")
      const recovered = yield* awaitStatus((status) => status._tag === "Online")

      assert.isTrue(Option.isSome(failed), "the watch failure was reported")
      assert.strictEqual(subscriptions(), 2)
      assert.isTrue(Option.isSome(recovered), "the space reported online again without another trigger")
    }, VirtualTime.scoped)
  )

  it.effect(
    "reports the failure of the in-memory sync request that follows a failed watch",
    Effect.fnUntraced(function*() {
      const watchFails = yield* Deferred.make<void>()
      const { awaitStatus, forgetStatuses, lockOn } = yield* inMemoryScheduler({
        pull: Effect.void,
        watch: Effect.andThen(Deferred.await(watchFails), ownerUnavailable)
      })
      lockOn(requestReconciliationStatement)
      yield* forgetStatuses
      yield* Deferred.succeed(watchFails, undefined)

      const reported = yield* awaitStatus(
        (status) => status._tag === "Failed" && status.message === "StorageUnavailable"
      )

      assert.isTrue(Option.isSome(reported), "the failed sync request was reported")
    }, VirtualTime.scoped)
  )

  it.effect(
    "reports the failure of the workflow sync request that follows a failed watch",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layerWorkflow")
      const logs = captureLogs()
      const watchFails = yield* Deferred.make<void>()
      let subscriptions = 0
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            subscriptions += 1
            if (subscriptions > 1) return Stream.never
            return Deferred.await(watchFails).pipe(Effect.andThen(ownerUnavailable), Stream.fromEffect)
          }
        }),
        logs.layerLogs
      )
      services.lockNext(requestReconciliationStatement, 1_000_000)
      yield* Deferred.succeed(watchFails, undefined)

      const reported = yield* eventually(
        services,
        space,
        (status) => status._tag === "Failed" && status.message === "StorageUnavailable"
      )
      services.lockNext(noStatement)
      const recovered = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(reported), "the failed sync request was reported")
      assert.isTrue(Option.isSome(recovered), "the space reported online on its own once storage healed")
    }, VirtualTime.scoped)
  )
})

describe("a status subscriber that throws while a failure is reported", () => {
  it.effect.each(constructors)(
    "still retries and drains a foreground space with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const logs = captureLogs()
      let throwing = false
      let unavailable = false
      services.reactivity.registerUnsafe([ReactivityKey.status(spaceId)], () => {
        if (throwing) decodeURIComponent("%")
      })
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!unavailable || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            unavailable = false
            throwing = true
            return storageUnavailable
          }
        }),
        logs.layerLogs
      )
      unavailable = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* settle("5 seconds")
      const notified = logs.errorMessages().filter((message) => message === "Failure status notification died")
      throwing = false
      yield* settle("5 minutes")
      const status = yield* space.status

      assert.isAbove(notified.length, 0)
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
    }, VirtualTime.scoped)
  )
})

describe("a readmission whose transport generation read died", () => {
  it.effect(
    "retries the managed space with the backoff",
    Effect.fnUntraced(function*() {
      const services = yield* twoSpaces("layer")
      const logs = captureLogs()
      let unavailable = false
      let generationDies = false
      const { space } = yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          transportGeneration: Effect.suspend(() => {
            if (!generationDies) return Effect.succeed(0)
            generationDies = false
            return Effect.die("generation died")
          }),
          submitBatch: acceptSubmission,
          pull: (request) => {
            if (!unavailable || request.spaceId !== spaceId) return emptyPage(services.crypto, request)
            unavailable = false
            generationDies = true
            return storageUnavailable
          }
        }),
        logs.layerLogs
      )
      unavailable = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))

      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.deepStrictEqual(logs.defects(), ["generation died"])
      assert.strictEqual(describeSpaceStatus(drained, yield* space.status), "Online, pending 0")
    }, VirtualTime.scoped)
  )
})

const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000a03")

const threeSpaces = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId, thirdSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

describe("background work claimed after the foreground took the space over", () => {
  it.effect.each(constructors)(
    "leaves the foreground runtime alone when that work died with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* threeSpaces(constructor)
      yield* BackgroundReplica.seedPending(services, [otherSpaceId, thirdSpaceId])
      const releaseWorkers = yield* Deferred.make<void>()
      const workersBusy = yield* Deferred.make<void>()
      let busy = 0
      let offline = false
      let dies = false
      let died = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => {
          if (offline) return serverUnavailable
          return acceptSubmission(request)
        },
        pull: (request) => {
          if (request.spaceId !== spaceId) {
            busy += 1
            let reached = Effect.void
            if (busy === 2) reached = Deferred.succeed(workersBusy, undefined).pipe(Effect.asVoid)
            return reached.pipe(
              Effect.andThen(Deferred.await(releaseWorkers)),
              Effect.andThen(emptyPage(services.crypto, request))
            )
          }
          if (dies) {
            died += 1
            return Effect.die("undecodable response")
          }
          return emptyPage(services.crypto, request)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(Deferred.await(workersBusy))
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0 WHERE space_id = ${spaceId}`
      yield* space.activate
      const online = yield* eventually(services, space, isOnlineDrained)
      assert.isTrue(Option.isSome(online), "the space came online")
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const parked = yield* eventually(services, space, (status) => status._tag === "Offline" && status.pending === 1)
      assert.isTrue(Option.isSome(parked), "the mutation stayed pending")
      yield* space.deactivate
      offline = false
      yield* space.activate
      const reconciled = yield* eventually(services, space, isOnlineDrained)
      assert.isTrue(Option.isSome(reconciled), "the foreground reconciled")
      yield* settle("30 seconds")

      dies = true
      yield* Deferred.succeed(releaseWorkers, undefined)
      yield* settle("30 seconds")
      const whileActive = yield* space.status
      yield* space.deactivate
      const afterDeactivation = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(whileActive._tag, "Online")
      assert.strictEqual(whileActive.pending, 0)
      assert.deepStrictEqual(
        { backgroundAttempts: died, afterDeactivation: afterDeactivation._tag, failed: aggregate.counts.failed },
        { backgroundAttempts: 1, afterDeactivation: "Idle", failed: 0 }
      )
    }, VirtualTime.scoped)
  )
})

describe("a subscriber that throws while a died background turn is published", () => {
  it.effect.each(constructors)(
    "keeps the background workers running with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* threeSpaces(constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId, otherSpaceId])
      const logs = captureLogs()
      let pullDies = 2
      let untilThrow = 0
      let throws = 0
      services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        if (untilThrow === 0) return
        untilThrow -= 1
        if (untilThrow > 0) return
        throws += 1
        decodeURIComponent("%")
      })
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId !== thirdSpaceId && pullDies > 0) {
            pullDies -= 1
            untilThrow = 2
            return Effect.die("undecodable response")
          }
          return emptyPage(services.crypto, request)
        }
      })).pipe(Effect.provide(logs.layerLogs))
      const third = yield* replica.space(thirdSpaceId)
      yield* settle("5 minutes")
      const settlements = logs.errorMessages().filter((message) => message === "Background turn settlement died")
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* third.activate
      yield* third.mutate(Domain.PutTodo, Domain.todo("third"))
      yield* third.deactivate

      const drained = yield* eventually(services, third, isDrained)

      assert.strictEqual(throws, 2, "the subscriber threw while each died turn was published")
      assert.strictEqual(settlements.length, 2)
      assert.isTrue(Option.isSome(drained), "a worker drained the third space")
    }, VirtualTime.scoped)
  )
})

describe("a sync request that keeps dying after a watch failure", () => {
  it.effect.each(constructors)(
    "subscribes to the watch again and backs off while the request keeps dying with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* threeSpaces(constructor)
      const logs = captureLogs()
      const watchFails = yield* Deferred.make<void>()
      let subscriptions = 0
      yield* foregroundSpaces(
        services,
        SyncEngine.SyncEngine.of({
          ...idleRemote,
          submitBatch: acceptSubmission,
          pull: (request) => emptyPage(services.crypto, request),
          watch: (request) => {
            if (request.spaceId !== spaceId) return Stream.never
            subscriptions += 1
            if (subscriptions > 1) return Stream.never
            return Deferred.await(watchFails).pipe(Effect.andThen(storageUnavailable), Stream.fromEffect)
          }
        }),
        logs.layerLogs
      )
      services.dieNext(requestReconciliationStatement, 1_000_000)
      yield* Deferred.succeed(watchFails, undefined)

      yield* settle("10 minutes")

      assert.strictEqual(subscriptions, 2, "the watch reopened while the sync request kept dying")
      assert.isAbove(logs.errors(), 5)
      assert.isBelow(logs.errors(), 30, "the retry of the dying request was backed off")
    }, VirtualTime.scoped)
  )
})

describe("the watch backoff", () => {
  it.effect(
    "keeps growing while the watch has not opened again",
    Effect.fnUntraced(function*() {
      const backoff = Configuration.makeWatchBackoff({ retryDelayMillis: 1000, maximumRetryDelayMillis: 60_000 })
      yield* backoff.opened
      yield* TestClock.adjust("10 seconds")
      const afterStayingOpen = yield* backoff.closed
      yield* TestClock.adjust("10 seconds")
      const withoutReopening = yield* backoff.closed
      yield* backoff.opened
      yield* TestClock.adjust("10 seconds")
      const afterReopening = yield* backoff.closed

      assert.deepStrictEqual([afterStayingOpen, withoutReopening, afterReopening], [1000, 2000, 1000])
    })
  )
})

const managedSpace = Effect.fnUntraced(function*(readmission: {
  readonly onFirstFailure: Effect.Effect<void>
  readonly transportGeneration: Effect.Effect<number>
  readonly request: Effect.Effect<void, ReplicaError.ReplicaError>
}) {
  const waits: Array<number> = []
  const synced = yield* Queue.unbounded<void>()
  let requested = 0
  let completed = 0
  let syncs = 0
  const remote = SyncEngine.SyncEngine.of({
    ...idleRemote,
    transportGeneration: readmission.transportGeneration,
    waitForTransportChange: (generation) => {
      waits.push(generation)
      return Effect.never
    }
  })
  const manager = yield* Reconciler.makeManager({ concurrency: 1 }).pipe(
    Effect.provideService(SyncEngine.SyncEngine, remote)
  )
  yield* manager.register({
    spaceId,
    generation: 1,
    definition: Domain.definition,
    local: {
      requestReconciliation: Effect.suspend(() => {
        let admitted = readmission.request
        if (requested === 0) admitted = Effect.void
        return Effect.map(admitted, () => {
          requested += 1
          return requested
        })
      }),
      reconciliationGenerations: Effect.sync(() => ({ requested, completed })),
      completeReconciliation: (generation) =>
        Effect.sync(() => {
          completed = generation
        }),
      replicationState: Effect.never
    },
    reconciliation: {
      sync: Effect.suspend(() => {
        syncs += 1
        if (syncs > 1) return Queue.offer(synced, undefined).pipe(Effect.asVoid)
        return Effect.andThen(readmission.onFirstFailure, storageUnavailable)
      }),
      generation: Effect.succeed(0),
      failed: () => Effect.void,
      watchFailed: () => Effect.void,
      succeeded: Effect.void,
      status: Effect.succeed({ _tag: "Connecting", pending: 0 })
    }
  })
  return { waits, synced }
})

describe("the transport generation a managed readmission reads", () => {
  it.effect(
    "is the generation the retry of a readmission that could not reach the server waits on",
    Effect.fnUntraced(function*() {
      let generation = 1
      let unreachable = false
      const { waits } = yield* managedSpace({
        onFirstFailure: Effect.sync(() => {
          generation = 7
          unreachable = true
        }),
        transportGeneration: Effect.sync(() => generation),
        request: Effect.suspend(() => {
          if (unreachable) return serverUnavailable
          return Effect.void
        })
      })

      yield* settle("5 seconds")

      assert.deepStrictEqual(Array.from(new Set(waits)), [7])
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries the space when the read died and the readmission could not reach the server either",
    Effect.fnUntraced(function*() {
      let dies = false
      let unreachable = false
      const { synced } = yield* managedSpace({
        onFirstFailure: Effect.sync(() => {
          dies = true
          unreachable = true
        }),
        transportGeneration: Effect.suspend(() => {
          if (!dies) return Effect.succeed(1)
          dies = false
          return Effect.die("generation died")
        }),
        request: Effect.suspend(() => {
          if (!unreachable) return Effect.void
          unreachable = false
          return serverUnavailable
        })
      })

      const retried = yield* VirtualTime.advanceUntil(Queue.take(synced)).pipe(Effect.timeoutOption("5 minutes"))

      assert.isTrue(Option.isSome(retried), "the space synced again after both failures")
    }, VirtualTime.scoped)
  )
})
