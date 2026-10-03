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
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as VirtualTime from "./fixtures/VirtualTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000901")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000902")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000901")
const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000901")

type Constructor = "layer" | "layerWorkflow"

type Remote = SyncEngine.SyncEngine["Service"]

const idleRemote = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  transportGeneration: Effect.succeed(0),
  waitForTransportChange: () => Effect.never,
  submitBatch: () => Effect.never,
  discard: () => Effect.die("unexpected discard"),
  pull: () => Effect.never,
  bootstrap: () => Effect.die("unexpected bootstrap"),
  watch: () => Stream.never
})

const acceptSubmission: Remote["submitBatch"] = (request) =>
  Effect.succeed(Protocol.SubmitBatchResult.make({
    receipts: request.envelopes.map((envelope) =>
      Protocol.AcceptedReceipt.make({
        ...envelope,
        serverSequence: Identity.ServerSequence.make(1),
        result: Domain.todo(envelope.mutationId, "accepted")
      })
    )
  }))

const emptyPage = (crypto: Crypto.Crypto, request: Parameters<Remote["pull"]>[0]) =>
  Protocol.viewChangesDigest([]).pipe(
    Effect.map((digest) =>
      Protocol.PullPage.make({
        scopeGeneration: request.scopeGeneration,
        cursor: Protocol.ReplicationCursor.make({
          viewId,
          revision: Identity.ReplicationViewRevision.make((request.cursor?.revision ?? 0) + 1)
        }),
        serverSequence: Identity.ServerSequence.make(1),
        changes: [],
        contentBytes: Protocol.encodedBytes([]),
        digest,
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      })
    ),
    Effect.provideService(Crypto.Crypto, crypto)
  )

const makeAttempts = Effect.gen(function*() {
  const reached = [
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>()
  ]
  let count = 0
  const record = Effect.suspend(() => {
    count += 1
    const signal = reached[count - 1]
    if (signal === undefined) return Effect.void
    return Deferred.succeed(signal, undefined).pipe(Effect.asVoid)
  })
  return {
    record,
    count: () => count,
    reached: (attempt: 1 | 2 | 3 | 4) => Deferred.await(reached[attempt - 1])
  }
})

interface Capacity {
  readonly maximumActiveSpaces: number
  readonly foregroundActiveSpaces: number
  readonly initialSpaces: ReadonlyArray<Identity.SpaceId>
}

const singleSpace: Capacity = { maximumActiveSpaces: 4, foregroundActiveSpaces: 2, initialSpaces: [spaceId] }
const oneForegroundSlot: Capacity = {
  maximumActiveSpaces: 2,
  foregroundActiveSpaces: 1,
  initialSpaces: [spaceId, otherSpaceId]
}

const backgroundServices = Effect.fnUntraced(function*(constructor: Constructor, capacity: Capacity) {
  const databaseContext = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    WorkflowEngine.layerMemory
  ).pipe(Layer.build)
  const sql = Context.get(databaseContext, SqlClient.SqlClient)
  let locked: { readonly statement: string; remaining: number } | undefined
  let heldStatement:
    | {
      readonly statement: string
      readonly entered: Deferred.Deferred<void>
      readonly release: Deferred.Deferred<void>
    }
    | undefined
  const failLocked = () => {
    const reason = new SqlError.LockTimeoutError({ cause: "injected lock timeout" })
    return Effect.fail(new SqlError.SqlError({ reason }))
  }
  const lockingSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const source: unknown = args[0]
      if (!Array.isArray(source)) return Reflect.apply(target, thisArg, args)
      const text = source.join("?")
      const held = heldStatement
      if (held !== undefined && text.includes(held.statement)) {
        heldStatement = undefined
        return Deferred.succeed(held.entered, undefined).pipe(
          Effect.andThen(Deferred.await(held.release)),
          Effect.andThen(Effect.suspend(failLocked))
        )
      }
      if (locked === undefined || !text.includes(locked.statement)) return Reflect.apply(target, thisArg, args)
      locked.remaining -= 1
      if (locked.remaining <= 0) locked = undefined
      return failLocked()
    }
  })
  const crypto = Context.get(databaseContext, Crypto.Crypto)
  const reactivity = Context.get(databaseContext, Reactivity.Reactivity)
  const options = {
    definition: Domain.definition,
    clientId,
    initialSpaces: capacity.initialSpaces,
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    maximumActiveSpaces: capacity.maximumActiveSpaces,
    foregroundActiveSpaces: capacity.foregroundActiveSpaces,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    retryDelay: "10 seconds",
    maximumRetryDelay: "10 seconds"
  } satisfies SqlReplica.Options<typeof Domain.definition>
  let heldInvalidation:
    | {
      readonly key: string
      remaining: number
      readonly entered: Deferred.Deferred<void>
      readonly release: Deferred.Deferred<void>
    }
    | undefined
  const gatedReactivity = new Proxy(reactivity, {
    get: (target, property, receiver) => {
      if (property !== "invalidate") return Reflect.get(target, property, receiver)
      return (keys: Parameters<typeof reactivity.invalidate>[0]) => {
        const held = heldInvalidation
        if (held === undefined || !Array.isArray(keys) || !keys.includes(held.key)) {
          return target.invalidate(keys)
        }
        held.remaining -= 1
        if (held.remaining > 0) return target.invalidate(keys)
        heldInvalidation = undefined
        return target.invalidate(keys).pipe(
          Effect.andThen(Deferred.succeed(held.entered, undefined)),
          Effect.andThen(Deferred.await(held.release))
        )
      }
    }
  })
  const lockingContext = databaseContext.pipe(
    Context.add(SqlClient.SqlClient, lockingSql),
    Context.add(Reactivity.Reactivity, gatedReactivity)
  )
  const start = (remote: Remote) => {
    const layerServices = Layer.mergeAll(
      Domain.layerHandlers,
      Layer.succeed(SyncEngine.SyncEngine, remote),
      Layer.succeedContext(lockingContext)
    )
    let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
    if (constructor === "layerWorkflow") {
      layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
    }
    return Layer.build(layerReplica).pipe(Effect.map(Context.get(Replica.Replica)))
  }
  const lockNext = (statement: string, times = 1) => {
    locked = { statement, remaining: times }
  }
  const holdStatement = Effect.fnUntraced(function*(statement: string) {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    heldStatement = { statement, entered, release }
    return { entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  const holdInvalidation = Effect.fnUntraced(function*(key: string) {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const arm = (remaining: number) => {
      heldInvalidation = { key, remaining, entered, release }
    }
    return { arm, entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  const lockRemaining = () => locked?.remaining ?? 0
  return { sql, crypto, reactivity, start, lockNext, lockRemaining, holdStatement, holdInvalidation }
})

const pendingBackgroundSpace = Effect.fnUntraced(function*(constructor: Constructor, capacity: Capacity) {
  const services = yield* backgroundServices(constructor, capacity)
  const seedScope = yield* Scope.make()
  const seedReplica = yield* services.start(idleRemote).pipe(Scope.provide(seedScope))
  const seedSpace = yield* seedReplica.space(spaceId)
  yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("pending"))
  yield* seedSpace.deactivate
  yield* Scope.close(seedScope, Exit.void)
  yield* services.sql`UPDATE effect_local_client_spaces
    SET replication_view_id = ${viewId}, replication_view_revision = 0`
  return services
})

const awaitSpaceStatus = Effect.fnUntraced(function*(
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  tag: ReplicaStatus.ReplicaStatus["_tag"]
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.status(space.spaceId), ReactivityKey.aggregateStatus], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  let status = yield* space.status
  while (status._tag !== tag) {
    yield* Queue.take(changes)
    status = yield* space.status
  }
  return status
})

const serverUnavailable = Effect.fail(new ReplicaError.ServerUnavailable())

const settle = (duration: "1 second" | "5 minutes") =>
  VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

const transientThenDrain = (
  services: { readonly crypto: Crypto.Crypto },
  attempts: { readonly record: Effect.Effect<void>; readonly count: () => number },
  first: Effect.Effect<never, ReplicaError.ReplicaError>
) =>
  SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      if (request.spaceId !== spaceId) return Effect.never
      if (attempts.count() === 0) return Effect.andThen(attempts.record, first)
      return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
    }
  })

const holdOtherSpaceActivating = Effect.fnUntraced(function*(
  services: Effect.Success<ReturnType<typeof backgroundServices>>,
  replica: Replica.Replica["Service"]
) {
  const other = yield* replica.space(otherSpaceId)
  const building = yield* services.holdInvalidation(ReactivityKey.activation(otherSpaceId))
  building.arm(1)
  const activation = yield* Effect.forkChild(other.activate, { startImmediately: true })
  yield* building.entered
  assert.strictEqual(yield* other.activation, "Activating")
  return { release: building.release, activation }
})

describe("background retries survive an abandoned foreground claim", () => {
  it.effect(
    "keeps the scheduled background retry when a foreground activation of the space fails to build",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(
        transientThenDrain(services, attempts, serverUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.strictEqual(attempts.count(), 1)
      const stored = yield* services.sql<{ readonly desired_scope_json: string }>`
        SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = '{"models":["Missing"]}'`

      const activation = yield* Effect.result(space.activate)
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = ${stored[0].desired_scope_json}`

      assert.strictEqual(activation._tag, "Failure")
      assert.strictEqual(yield* space.activation, "Inactive")
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "keeps the scheduled background retry when a foreground activation waiting for capacity is interrupted with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, oneForegroundSlot)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(
        transientThenDrain(services, attempts, serverUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      assert.strictEqual(yield* space.activation, "Inactive")
      const occupant = yield* holdOtherSpaceActivating(services, replica)

      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* Fiber.interrupt(activation)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)

      assert.strictEqual(yield* space.activation, "Inactive")
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "retries a background turn that fails after a foreground activation waiting for capacity was interrupted with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, oneForegroundSlot)
      const attempts = yield* makeAttempts
      const releasePull = yield* Deferred.make<void>()
      const replica = yield* services.start(
        transientThenDrain(
          services,
          attempts,
          Deferred.await(releasePull).pipe(Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable())))
        )
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      const occupant = yield* holdOtherSpaceActivating(services, replica)

      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* Fiber.interrupt(activation)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)
      yield* Deferred.succeed(releasePull, undefined)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "retries a background turn that fails after a foreground activation waiting for its build was interrupted with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, singleSpace)
      const attempts = yield* makeAttempts
      const building = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      building.arm(1)
      const replica = yield* services.start(
        transientThenDrain(services, attempts, serverUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* building.entered
      assert.strictEqual(yield* space.activation, "Activating")

      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* Fiber.interrupt(activation)
      yield* building.release
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )
})

describe("background turns that settle after their space was left", () => {
  it.effect(
    "starts no credential wait when a background turn settles after its space was left",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      let credentialWaits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => {
          credentialWaits += 1
          return Effect.never
        },
        pull: () => {
          bookkeeping.arm(2)
          return Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
        }
      }))
      yield* bookkeeping.entered
      yield* replica.leave(spaceId)
      assert.strictEqual((yield* replica.status).spaces, 0)

      yield* bookkeeping.release
      yield* settle("5 minutes")

      assert.strictEqual(credentialWaits, 0)
    }, Effect.scoped)
  )

  it.effect(
    "retries a rejoined space after a new credential when a turn of its previous membership settles late",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      let foreground = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        pull: () => {
          if (foreground) return Effect.never
          if (attempts.count() === 0) bookkeeping.arm(2)
          if (attempts.count() < 2) {
            return Effect.andThen(
              attempts.record,
              Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
            )
          }
          return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
        }
      }))
      yield* bookkeeping.entered
      yield* replica.leave(spaceId)
      const rejoined = yield* replica.join(spaceId)
      foreground = true
      yield* rejoined.mutate(Domain.PutTodo, Domain.todo("again"))
      foreground = false
      yield* rejoined.deactivate
      yield* attempts.reached(2)
      const paused = yield* awaitSpaceStatus(rejoined, services.reactivity, "NeedsAuthentication")
      assert.strictEqual(paused.pending, 1)

      yield* bookkeeping.release
      yield* settle("1 second")
      yield* Deferred.succeed(credentialChanged, undefined)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )
})

describe("review 225 suspicions that did not reproduce", () => {
  it.effect(
    "keeps waiting for a new credential when leaving the space fails",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        pull: () => {
          if (attempts.count() === 0) {
            return Effect.andThen(
              attempts.record,
              Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
            )
          }
          return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      services.lockNext("DELETE FROM effect_local_client_spaces")

      const left = yield* Effect.result(replica.leave(spaceId))

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual((yield* space.status)._tag, "NeedsAuthentication")
      assert.strictEqual((yield* replica.status).counts.needsAuthentication, 1)
      yield* Deferred.succeed(credentialChanged, undefined)
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "lets the foreground take over a space whose background runtime is still building with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, singleSpace)
      const building = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      building.arm(1)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      yield* building.entered

      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* building.release
      yield* Fiber.join(activation)
      yield* settle("5 minutes")
      yield* space.deactivate
      yield* settle("5 minutes")

      const status = yield* space.status
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 1)
      assert.strictEqual(aggregate.counts.idle, 1)
      assert.strictEqual(aggregate.totalPending, 0)
    }, Effect.scoped)
  )

  it.effect(
    "contributes nothing to the aggregate when a terminally failed turn settles after its space was left",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => {
          bookkeeping.arm(2)
          return Effect.fail(new ReplicaError.ProtocolInvalid({ message: "rejected" }))
        }
      }))
      yield* bookkeeping.entered
      yield* replica.leave(spaceId)
      yield* bookkeeping.release
      yield* settle("5 minutes")

      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 0)
      assert.strictEqual(aggregate.totalPending, 0)
      assert.deepStrictEqual(aggregate.counts, {
        idle: 0,
        offline: 0,
        connecting: 0,
        online: 0,
        needsAuthentication: 0,
        failed: 0
      })
    }, Effect.scoped)
  )

  it.effect(
    "interrupts a pending credential wait when the replica shuts down",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const waitInterrupted = yield* Deferred.make<void>()
      const replicaScope = yield* Scope.make()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Effect.onInterrupt(Effect.never, () => Deferred.succeed(waitInterrupted, undefined)),
        pull: () => Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
      })).pipe(Scope.provide(replicaScope))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      yield* Scope.close(replicaScope, Exit.void)

      assert.isTrue(yield* Deferred.isDone(waitInterrupted))
    }, Effect.scoped)
  )

  it.effect(
    "keeps a terminally failed space failed in both statuses when leaving it fails",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ProtocolInvalid({ message: "no" })))
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      yield* settle("1 second")
      services.lockNext("DELETE FROM effect_local_client_spaces")

      const left = yield* Effect.result(replica.leave(spaceId))
      yield* settle("5 minutes")

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual((yield* space.status)._tag, "Failed")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 1)
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(attempts.count(), 1)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a terminal failure settled during a leave that later fails consistent across both statuses",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => {
          bookkeeping.arm(2)
          return Effect.fail(new ReplicaError.ProtocolInvalid({ message: "no" }))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* bookkeeping.entered
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true })
      )
      yield* removal.entered
      yield* bookkeeping.release
      yield* settle("1 second")
      yield* removal.release
      const left = yield* Fiber.join(leaving)
      yield* settle("5 minutes")

      assert.strictEqual(left._tag, "Failure")
      const status = yield* space.status
      const aggregate = yield* replica.status
      assert.strictEqual(status._tag, "Failed")
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(aggregate.totalPending, status.pending)
    }, Effect.scoped)
  )

  it.effect(
    "spaces consecutive failed retry admissions of a foreground space by the backoff and then drains",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      let failNextPull = false
      const pullTimes: Array<number> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (failNextPull) {
            failNextPull = false
            return serverUnavailable
          }
          return Effect.clockWith((clock) =>
            Effect.sync(() => {
              pullTimes.push(clock.currentTimeMillisUnsafe())
            })
          ).pipe(Effect.andThen(emptyPage(services.crypto, request)))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* settle("5 minutes")
      assert.strictEqual((yield* space.status)._tag, "Online")
      failNextPull = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("second"))
      yield* settle("1 second")
      assert.isFalse(failNextPull)
      const pullsBefore = pullTimes.length
      const failedAt = pullTimes.at(-1) ?? 0
      services.lockNext("SET requested_generation = ?", 3)

      yield* settle("5 minutes")

      assert.strictEqual(services.lockRemaining(), 0)
      const status = yield* space.status
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
      assert.isAbove(pullTimes.length, pullsBefore)
      assert.isAtLeast(pullTimes[pullsBefore] - failedAt, 40_000)
    }, Effect.scoped)
  )

  it.effect(
    "control: the scheduled background retry runs when no foreground activation is attempted",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(
        transientThenDrain(services, attempts, serverUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      assert.strictEqual(yield* space.activation, "Inactive")
      assert.strictEqual(attempts.count(), 1)
      const stored = yield* services.sql<{ readonly desired_scope_json: string }>`
        SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = '{"models":["Missing"]}'`
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = ${stored[0].desired_scope_json}`

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "control: the scheduled background retry runs while another space is activating with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, oneForegroundSlot)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(
        transientThenDrain(services, attempts, serverUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      assert.strictEqual(yield* space.activation, "Inactive")
      const occupant = yield* holdOtherSpaceActivating(services, replica)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "control: a background turn that fails while another space is activating is retried with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, oneForegroundSlot)
      const attempts = yield* makeAttempts
      const releasePull = yield* Deferred.make<void>()
      const replica = yield* services.start(
        transientThenDrain(
          services,
          attempts,
          Deferred.await(releasePull).pipe(Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable())))
        )
      )
      yield* attempts.reached(1)
      const occupant = yield* holdOtherSpaceActivating(services, replica)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)
      yield* Deferred.succeed(releasePull, undefined)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )

  it.effect(
    "control: a rejoined space retries after a new credential when no earlier turn settles late",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      let foreground = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        pull: () => {
          if (foreground) return Effect.never
          if (attempts.count() === 0) bookkeeping.arm(2)
          if (attempts.count() < 2) {
            return Effect.andThen(
              attempts.record,
              Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
            )
          }
          return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
        }
      }))
      yield* bookkeeping.entered
      yield* replica.leave(spaceId)
      const rejoined = yield* replica.join(spaceId)
      foreground = true
      yield* rejoined.mutate(Domain.PutTodo, Domain.todo("again"))
      foreground = false
      yield* rejoined.deactivate
      yield* attempts.reached(2)
      const paused = yield* awaitSpaceStatus(rejoined, services.reactivity, "NeedsAuthentication")
      assert.strictEqual(paused.pending, 1)

      yield* settle("1 second")
      yield* Deferred.succeed(credentialChanged, undefined)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("5 minutes"))
      yield* bookkeeping.release
      assert.isTrue(Option.isSome(retried))
    }, Effect.scoped)
  )
})
