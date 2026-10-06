import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
import * as Scope from "effect/Scope"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  awaitSpaceStatus,
  type Constructor,
  emptyPage,
  idleRemote,
  makeAttempts,
  viewId
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000901")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000902")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000901")

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

const pendingBackgroundSpace = Effect.fnUntraced(function*(
  constructor: Constructor,
  capacity: Capacity,
  seedOther = false
) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    ...capacity,
    retryDelay: "10 seconds",
    maximumRetryDelay: "10 seconds"
  })
  const seeded = [spaceId]
  if (seedOther) seeded.push(otherSpaceId)
  yield* BackgroundReplica.seedPending(services, seeded)
  return services
})

const serverUnavailable = Effect.fail(new ReplicaError.ServerUnavailable())

const settle = (duration: "1 second" | "5 seconds" | "5 minutes") =>
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
  services: BackgroundReplica.Services,
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

const credentialRejected = Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))

const makeCredential = Effect.gen(function*() {
  const changed = yield* Deferred.make<void>()
  const waits = { started: 0, interrupted: 0 }
  let generation = 7
  const rejected = Effect.suspend(() =>
    Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: generation }))
  )
  const waitForChange = (rejectedGeneration: number) => {
    if (rejectedGeneration < generation) return Effect.void
    waits.started += 1
    let wait: Effect.Effect<void> = Deferred.await(changed)
    if (rejectedGeneration === 8) wait = Effect.never
    return Effect.onInterrupt(wait, () =>
      Effect.sync(() => {
        waits.interrupted += 1
      }))
  }
  const change = Effect.suspend(() => {
    generation = 8
    return Deferred.succeed(changed, undefined)
  })
  return { rejected, waitForChange, change, waits, live: () => waits.started - waits.interrupted }
})
const protocolInvalid = Effect.fail(new ReplicaError.ProtocolInvalid({ message: "rejected" }))

const claimDuringFailingTurn = Effect.fnUntraced(function*(
  failure: Effect.Effect<never, ReplicaError.ReplicaError>,
  later: "drain" | "reject"
) {
  const services = yield* pendingBackgroundSpace("layer", oneForegroundSlot)
  const attempts = yield* makeAttempts
  const releasePull = yield* Deferred.make<void>()
  const credential = yield* makeCredential
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    waitForCredentialChange: credential.waitForChange,
    submitBatch: acceptSubmission,
    pull: (request) => {
      if (request.spaceId !== spaceId) return Effect.never
      if (attempts.count() === 0) {
        return attempts.record.pipe(Effect.andThen(Deferred.await(releasePull)), Effect.andThen(failure))
      }
      if (later === "reject") return Effect.andThen(attempts.record, credential.rejected)
      return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
    }
  }))
  const space = yield* replica.space(spaceId)
  yield* attempts.reached(1)
  const occupant = yield* holdOtherSpaceActivating(services, replica)
  const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
  yield* Deferred.succeed(releasePull, undefined)
  yield* settle("1 second")
  return { services, attempts, replica, space, occupant, activation, credential }
})

type Operation = "mutate" | "query" | "get" | "pending"

const operations: ReadonlyArray<Operation> = ["mutate", "query", "get", "pending"]

const operate = (space: Replica.Space, operation: Operation): Effect.Effect<unknown, { readonly _tag: string }> => {
  if (operation === "mutate") return space.mutate(Domain.PutTodo, Domain.todo("other"))
  if (operation === "query") return space.query(Domain.ReadCountIndex, { minimum: 0, direction: "asc" })
  if (operation === "get") return space.get(Domain.Todo, "pending")
  return space.pending
}

const turnTakenDuringLeave = Effect.fnUntraced(function*() {
  const services = yield* pendingBackgroundSpace("layer", singleSpace)
  const attempts = yield* makeAttempts
  let foreground = false
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    pull: () => {
      if (foreground) return Effect.never
      return Effect.andThen(attempts.record, serverUnavailable)
    }
  }))
  const space = yield* replica.space(spaceId)
  yield* attempts.reached(1)
  yield* settle("1 second")
  foreground = true
  yield* space.activate
  yield* settle("1 second")
  foreground = false
  const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
  const leave = Effect.result(replica.leave(spaceId))
  const leaving = yield* space.deactivate.pipe(
    Effect.andThen(leave),
    Effect.provideService(Scheduler.PreventSchedulerYield, true),
    Effect.forkChild({ startImmediately: true })
  )
  yield* removal.entered
  yield* settle("1 second")
  const duringLeave = attempts.count()
  yield* removal.release
  const left = yield* Fiber.join(leaving)
  assert.strictEqual(left._tag, "Failure")
  const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
  return { duringLeave, retried }
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )
})

describe("work that comes due during a leave that later fails", () => {
  it.effect(
    "retries after a new credential that arrived while a leave that later failed was in progress",
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
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true })
      )
      yield* removal.entered

      yield* Deferred.succeed(credentialChanged, undefined)
      yield* settle("1 second")
      yield* removal.release
      const left = yield* Fiber.join(leaving)

      assert.strictEqual(left._tag, "Failure")
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
  )

  it.effect(
    "retries a background space whose retry came due while a leave that later failed was in progress",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
      }))
      yield* attempts.reached(1)
      yield* settle("1 second")
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true })
      )
      yield* removal.entered

      yield* settle("5 minutes")
      yield* removal.release
      const left = yield* Fiber.join(leaving)

      assert.strictEqual(left._tag, "Failure")
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )

  it.effect(
    "interrupts a pending credential wait when the replica shuts down",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const waitStarted = yield* Deferred.make<void>()
      const waitInterrupted = yield* Deferred.make<void>()
      const replicaScope = yield* Scope.make()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Deferred.succeed(waitStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(waitInterrupted, undefined))
          ),
        pull: () => Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
      })).pipe(Scope.provide(replicaScope))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      yield* VirtualTime.advanceUntil(Deferred.await(waitStarted))

      yield* Scope.close(replicaScope, Exit.void)

      assert.isTrue(yield* Deferred.isDone(waitInterrupted))
    }, VirtualTime.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "counts a space that was online as idle when leaving it fails with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        ...singleSpace,
        constructor,
        clientId,
        retryDelay: "10 seconds",
        maximumRetryDelay: "10 seconds"
      })
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      }))
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* awaitSpaceStatus(space, services.reactivity, "Online").pipe(Effect.scoped, VirtualTime.advanceUntil)
      services.lockNext("DELETE FROM effect_local_client_spaces")

      const left = yield* Effect.result(replica.leave(spaceId))
      const status = yield* space.status
      const aggregate = yield* replica.status

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual(status._tag, "Idle")
      assert.deepStrictEqual({ online: aggregate.counts.online, idle: aggregate.counts.idle }, { online: 0, idle: 1 })
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )
})

describe("a background turn rejected by a leave that later fails", () => {
  it.effect(
    "does not report a space as failed because a turn was rejected by a leave that later failed",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", oneForegroundSlot, true)
      const occupantReached = yield* Deferred.make<void>()
      const releaseOccupant = yield* Deferred.make<void>()
      let occupantPulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId === otherSpaceId) return emptyPage(services.crypto, request)
          occupantPulls += 1
          if (occupantPulls > 1) return Effect.never
          return Deferred.succeed(occupantReached, undefined).pipe(
            Effect.andThen(Deferred.await(releaseOccupant)),
            Effect.andThen(serverUnavailable)
          )
        }
      }))
      const space = yield* replica.space(otherSpaceId)
      yield* Deferred.await(occupantReached)
      yield* space.activate
      yield* settle("5 seconds")
      const drained = yield* space.status
      assert.strictEqual(drained._tag, "Online")
      assert.strictEqual(drained.pending, 0)
      const closing = yield* services.holdInvalidation(ReactivityKey.activation(otherSpaceId))
      closing.arm(1)
      const deactivation = yield* Effect.forkChild(space.deactivate, { startImmediately: true })
      yield* closing.entered
      assert.strictEqual(yield* space.activation, "Deactivating")
      yield* Deferred.succeed(releaseOccupant, undefined)
      yield* settle("1 second")
      services.lockNext("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(otherSpaceId).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true })
      )

      yield* closing.release
      yield* Fiber.join(deactivation)
      const left = yield* Fiber.join(leaving)
      yield* settle("5 minutes")

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual(yield* space.activation, "Inactive")
      const status = yield* space.status
      const aggregate = yield* replica.status
      let message = ""
      if (status._tag === "Failed") message = status.message
      assert.deepStrictEqual(
        { tag: status._tag, message, pending: status.pending, failed: aggregate.counts.failed },
        { tag: "Idle", message: "", pending: 0, failed: 0 }
      )
    }, VirtualTime.scoped)
  )

  it.effect(
    "runs a turn that was rejected because its space started leaving once the leave fails",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const turnStarted = yield* Deferred.make<void>()
      const releaseTurn = yield* Deferred.make<void>()
      let generationReads = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.suspend(() => {
          generationReads += 1
          if (generationReads > 1) return Effect.succeed(0)
          return Deferred.succeed(turnStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseTurn)),
            Effect.as(0)
          )
        }),
        pull: () => Effect.andThen(attempts.record, serverUnavailable)
      }))
      const space = yield* replica.space(spaceId)
      yield* Deferred.await(turnStarted)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
      yield* removal.entered
      yield* Deferred.succeed(releaseTurn, undefined)
      yield* settle("1 second")
      yield* removal.release
      const left = yield* Fiber.join(leaving)
      yield* settle("5 minutes")

      assert.strictEqual(left._tag, "Failure")
      const status = yield* space.status
      let message = ""
      if (status._tag === "Failed") message = status.message
      assert.deepStrictEqual(
        { tag: status._tag, message, pending: status.pending, synced: attempts.count() > 0 },
        { tag: "Idle", message: "", pending: 1, synced: true }
      )
    }, VirtualTime.scoped)
  )
})

describe("background outcomes that must not outlive the work that replaced them", () => {
  it.effect(
    "reports a space as idle while the turn requeued by a failed leave has not run yet",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const turnStarted = yield* Deferred.make<void>()
      const releaseTurn = yield* Deferred.make<void>()
      let generationReads = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.suspend(() => {
          generationReads += 1
          if (generationReads > 1) return Effect.never
          return Deferred.succeed(turnStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseTurn)),
            Effect.as(0)
          )
        })
      }))
      const space = yield* replica.space(spaceId)
      yield* Deferred.await(turnStarted)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      const leaving = yield* replica.leave(spaceId).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
      yield* removal.entered
      yield* Deferred.succeed(releaseTurn, undefined)
      yield* settle("1 second")
      yield* removal.release
      const left = yield* Fiber.join(leaving)
      yield* settle("5 seconds")

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual(generationReads, 2)
      const status = yield* space.status
      let message = ""
      if (status._tag === "Failed") message = status.message
      assert.deepStrictEqual(
        { tag: status._tag, message, pending: status.pending },
        { tag: "Idle", message: "", pending: 1 }
      )
    }, VirtualTime.scoped)
  )

  it.effect(
    "drops a scheduled retry when a failed foreground takeover queues the space and that turn fails terminally",
    Effect.fnUntraced(function*() {
      const run = yield* claimDuringFailingTurn(serverUnavailable, "reject")
      assert.strictEqual(yield* run.space.activation, "Active")
      const stored = yield* run.services.sql<{ readonly desired_scope_json: string }>`
        SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      yield* run.services.sql`UPDATE effect_local_client_spaces
        SET desired_scope_json = '{"models":["Missing"]}' WHERE space_id = ${spaceId}`
      yield* run.occupant.release
      yield* Fiber.join(run.occupant.activation)
      const claimed = yield* Fiber.await(run.activation)
      yield* settle("1 second")
      assert.isTrue(Exit.isFailure(claimed))
      assert.strictEqual(yield* run.space.activation, "Inactive")
      assert.strictEqual((yield* run.space.status)._tag, "Failed")
      yield* run.services.sql`UPDATE effect_local_client_spaces
        SET desired_scope_json = ${stored[0].desired_scope_json} WHERE space_id = ${spaceId}`

      yield* settle("5 minutes")

      assert.strictEqual(run.attempts.count(), 1)
      assert.strictEqual((yield* run.space.status)._tag, "Failed")
    }, VirtualTime.scoped)
  )
})

describe("review 225 round 2 suspicions that did not reproduce", () => {
  it.effect(
    "focus 1: clears a credential rejection settled during a foreground claim once the foreground reconciles",
    Effect.fnUntraced(function*() {
      const run = yield* claimDuringFailingTurn(credentialRejected, "drain")
      yield* run.occupant.release
      yield* Fiber.join(run.occupant.activation)
      yield* Fiber.join(run.activation)
      yield* settle("5 minutes")

      const online = yield* run.space.status
      assert.strictEqual(online._tag, "Online")
      assert.strictEqual(online.pending, 0)
      assert.strictEqual(run.credential.live(), 0)
      yield* run.space.deactivate
      yield* settle("5 minutes")
      assert.strictEqual((yield* run.space.status)._tag, "Idle")
      const aggregate = yield* run.replica.status
      assert.strictEqual(aggregate.counts.needsAuthentication, 0)
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.totalPending, 0)
    }, VirtualTime.scoped)
  )

  it.effect(
    "focus 1: clears a terminal failure settled during a foreground claim once the foreground reconciles",
    Effect.fnUntraced(function*() {
      const run = yield* claimDuringFailingTurn(protocolInvalid, "drain")
      yield* run.occupant.release
      yield* Fiber.join(run.occupant.activation)
      yield* Fiber.join(run.activation)
      yield* settle("5 minutes")
      yield* run.space.deactivate
      yield* settle("5 minutes")

      const status = yield* run.space.status
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* run.replica.status
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.counts.idle, 2)
    }, VirtualTime.scoped)
  )

  it.effect(
    "focus 1: keeps one credential wait and a consistent status when the foreground claim is abandoned",
    Effect.fnUntraced(function*() {
      const run = yield* claimDuringFailingTurn(credentialRejected, "reject")
      yield* Fiber.interrupt(run.activation)
      yield* settle("5 seconds")
      yield* run.occupant.release
      yield* Fiber.join(run.occupant.activation)
      yield* settle("5 seconds")

      assert.strictEqual(yield* run.space.activation, "Inactive")
      const status = yield* run.space.status
      assert.strictEqual(status._tag, "NeedsAuthentication")
      assert.strictEqual(status.pending, 1)
      assert.strictEqual(run.credential.live(), 1)
      const aggregate = yield* run.replica.status
      assert.strictEqual(aggregate.counts.needsAuthentication, 1)
      assert.strictEqual(aggregate.totalPending, 1)
      const before = run.attempts.count()
      yield* run.credential.change
      yield* settle("5 seconds")
      assert.strictEqual(run.attempts.count(), before + 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(["released after the turn", "released while the turn holds the runtime"] as const)(
    "focus 2: runs a background turn again when a turn failed on a foreground runtime that is %s",
    Effect.fnUntraced(function*(release) {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      const onForeground = yield* Deferred.make<void>()
      const releaseForegroundPull = yield* Deferred.make<void>()
      let mode: "first" | "foreground" | "onForeground" | "drain" = "first"
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (mode === "first") return Effect.andThen(attempts.record, credentialRejected)
          if (mode === "foreground") return protocolInvalid
          if (mode === "onForeground") {
            mode = "drain"
            return attempts.record.pipe(
              Effect.andThen(Deferred.succeed(onForeground, undefined)),
              Effect.andThen(Deferred.await(releaseForegroundPull)),
              Effect.andThen(serverUnavailable),
              Effect.uninterruptible
            )
          }
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      mode = "foreground"
      yield* space.activate
      yield* settle("1 second")
      assert.strictEqual(yield* space.activation, "Active")
      mode = "onForeground"
      yield* Deferred.succeed(credentialChanged, undefined)
      yield* Deferred.await(onForeground)
      assert.strictEqual(yield* space.activation, "Active")

      if (release === "released after the turn") {
        yield* Deferred.succeed(releaseForegroundPull, undefined)
        yield* settle("1 second")
        assert.strictEqual(attempts.count(), 2)
        yield* space.deactivate
      } else {
        const deactivation = yield* Effect.forkChild(space.deactivate, { startImmediately: true })
        yield* Deferred.succeed(releaseForegroundPull, undefined)
        yield* Fiber.join(deactivation)
      }

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
      yield* settle("5 minutes")
      const status = yield* space.status
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
    }, VirtualTime.scoped)
  )

  it.effect(
    "focus 3: runs a turn that was queued before a leave and taken during it once the leave fails",
    Effect.fnUntraced(function*() {
      const run = yield* turnTakenDuringLeave()
      assert.strictEqual(run.duringLeave, 1)
      assert.isTrue(Option.isSome(run.retried))
    }, VirtualTime.scoped)
  )

  it.effect.each(["completes", "fails"] as const)(
    "focus 4: handles a credential rejection settled around a leave that waits for the turn when the leave %s",
    Effect.fnUntraced(function*(outcome) {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const attempts = yield* makeAttempts
      const credential = yield* makeCredential
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: credential.waitForChange,
        pull: () => {
          if (attempts.count() === 0) bookkeeping.arm(1)
          return Effect.andThen(attempts.record, credential.rejected)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* bookkeeping.entered
      assert.strictEqual(yield* space.activation, "Deactivating")
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", outcome === "completes")
      const leaving = yield* replica.leave(spaceId).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
      yield* bookkeeping.release
      yield* removal.entered
      yield* settle("1 second")
      const startedDuringLeave = credential.waits.started
      yield* removal.release
      const left = yield* Fiber.join(leaving)
      yield* settle("5 seconds")

      assert.isAtMost(startedDuringLeave, 1)
      if (outcome === "completes") {
        assert.strictEqual(left._tag, "Success")
        assert.strictEqual(credential.live(), 0)
        assert.strictEqual((yield* replica.status).spaces, 0)
        yield* credential.change
        yield* settle("5 minutes")
        assert.strictEqual(attempts.count(), 1)
        return
      }
      assert.strictEqual(left._tag, "Failure")
      const status = yield* space.status
      assert.strictEqual(status._tag, "NeedsAuthentication")
      assert.strictEqual(credential.live(), 1)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.needsAuthentication, 1)
      assert.strictEqual(aggregate.totalPending, status.pending)
      const before = attempts.count()
      yield* credential.change
      yield* settle("5 seconds")
      assert.strictEqual(attempts.count(), before + 1)
    }, VirtualTime.scoped)
  )

  it.effect(
    "focus 4: a wait of a left membership is gone and the rejoined membership keeps its own wait",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const credential = yield* makeCredential
      let foreground = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: credential.waitForChange,
        pull: () => {
          if (foreground) return Effect.never
          return Effect.andThen(attempts.record, credential.rejected)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      yield* replica.leave(spaceId)
      assert.strictEqual(credential.live(), 0)
      const rejoined = yield* replica.join(spaceId)
      foreground = true
      yield* rejoined.mutate(Domain.PutTodo, Domain.todo("again"))
      foreground = false
      yield* rejoined.deactivate
      yield* awaitSpaceStatus(rejoined, services.reactivity, "NeedsAuthentication")
      yield* settle("5 seconds")

      assert.strictEqual(credential.live(), 1)
      const before = attempts.count()
      yield* credential.change
      yield* settle("5 seconds")
      assert.strictEqual(attempts.count(), before + 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "focus 5: queues exactly one background turn when leaving a foreground space with pending work fails with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor, singleSpace)
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      let foreground = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        pull: () => {
          if (foreground) return Effect.never
          if (attempts.count() === 0) return Effect.andThen(attempts.record, credentialRejected)
          return Effect.andThen(attempts.record, serverUnavailable)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      foreground = true
      yield* space.activate
      yield* settle("1 second")
      assert.strictEqual(attempts.count(), 1)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces")
      foreground = false
      const leaving = yield* replica.leave(spaceId).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
      yield* removal.entered
      yield* Deferred.succeed(credentialChanged, undefined)
      yield* settle("1 second")
      assert.strictEqual(attempts.count(), 1)
      yield* removal.release
      const left = yield* Fiber.join(leaving)
      yield* settle("5 seconds")

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual(attempts.count(), 2)
      assert.strictEqual(yield* space.activation, "Inactive")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 1)
      assert.strictEqual(aggregate.totalPending, 1)
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
  )

  it.effect(
    "focus 5: a turn queued by a failed leave does not disturb a foreground activation that follows",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      let drain = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (drain) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          return Effect.andThen(attempts.record, serverUnavailable)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      yield* space.activate
      services.lockNext("DELETE FROM effect_local_client_spaces")
      const left = yield* Effect.result(replica.leave(spaceId))
      drain = true
      yield* space.activate
      yield* settle("5 minutes")

      assert.strictEqual(left._tag, "Failure")
      assert.strictEqual(yield* space.activation, "Active")
      const status = yield* space.status
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 1)
      assert.strictEqual(aggregate.counts.online, 1)
      assert.strictEqual(aggregate.totalPending, 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(operations)(
    "focus 6: keeps the scheduled background retry when %s waiting for foreground capacity is interrupted",
    Effect.fnUntraced(function*(operation) {
      const services = yield* pendingBackgroundSpace("layer", oneForegroundSlot)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(transientThenDrain(services, attempts, serverUnavailable))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      const occupant = yield* holdOtherSpaceActivating(services, replica)

      const running = yield* Effect.forkChild(operate(space, operation), { startImmediately: true })
      yield* Fiber.interrupt(running)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)

      assert.strictEqual(yield* space.activation, "Inactive")
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
  )

  it.effect.each(operations)(
    "focus 6: retries a background turn that fails after %s waiting for foreground capacity was interrupted",
    Effect.fnUntraced(function*(operation) {
      const services = yield* pendingBackgroundSpace("layer", oneForegroundSlot)
      const attempts = yield* makeAttempts
      const releasePull = yield* Deferred.make<void>()
      const heldUnavailable = Effect.andThen(Deferred.await(releasePull), serverUnavailable)
      const replica = yield* services.start(
        transientThenDrain(services, attempts, heldUnavailable)
      )
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      const occupant = yield* holdOtherSpaceActivating(services, replica)

      const running = yield* Effect.forkChild(operate(space, operation), { startImmediately: true })
      yield* Fiber.interrupt(running)
      yield* occupant.release
      yield* Fiber.join(occupant.activation)
      yield* Deferred.succeed(releasePull, undefined)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
  )

  it.effect.each(operations)(
    "focus 6: keeps the scheduled background retry when %s fails because the foreground runtime does not build",
    Effect.fnUntraced(function*(operation) {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(transientThenDrain(services, attempts, serverUnavailable))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")
      const stored = yield* services.sql<{ readonly desired_scope_json: string }>`
        SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = '{"models":["Missing"]}'`

      const result = yield* Effect.result(operate(space, operation))
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = ${stored[0].desired_scope_json}`

      assert.strictEqual(result._tag, "Failure")
      assert.strictEqual(attempts.count(), 1)
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("5 minutes"))
      assert.isTrue(Option.isSome(retried))
    }, VirtualTime.scoped)
  )

  it.effect.each(operations)(
    "focus 6: drains pending work in the foreground when %s activates a space with a scheduled background retry",
    Effect.fnUntraced(function*(operation) {
      const services = yield* pendingBackgroundSpace("layer", singleSpace)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(transientThenDrain(services, attempts, serverUnavailable))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* settle("1 second")

      yield* operate(space, operation)
      yield* settle("5 minutes")

      assert.strictEqual(yield* space.activation, "Active")
      const status = yield* space.status
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
    }, VirtualTime.scoped)
  )

  it.effect.each(["no leave", "leave completes"] as const)(
    "control: a turn that waited for a deactivation leaves the space idle with %s",
    Effect.fnUntraced(function*(variant) {
      const services = yield* pendingBackgroundSpace("layer", oneForegroundSlot, true)
      const occupantReached = yield* Deferred.make<void>()
      const releaseOccupant = yield* Deferred.make<void>()
      let occupantPulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (request.spaceId === otherSpaceId) return emptyPage(services.crypto, request)
          occupantPulls += 1
          if (occupantPulls > 1) return Effect.never
          return Deferred.succeed(occupantReached, undefined).pipe(
            Effect.andThen(Deferred.await(releaseOccupant)),
            Effect.andThen(serverUnavailable)
          )
        }
      }))
      const space = yield* replica.space(otherSpaceId)
      yield* Deferred.await(occupantReached)
      yield* space.activate
      yield* settle("5 seconds")
      const closing = yield* services.holdInvalidation(ReactivityKey.activation(otherSpaceId))
      closing.arm(1)
      const deactivation = yield* Effect.forkChild(space.deactivate, { startImmediately: true })
      yield* closing.entered
      yield* Deferred.succeed(releaseOccupant, undefined)
      yield* settle("1 second")
      if (variant === "leave completes") {
        const leaving = yield* replica.leave(otherSpaceId).pipe(
          Effect.result,
          Effect.forkChild({ startImmediately: true })
        )
        yield* closing.release
        yield* Fiber.join(deactivation)
        const left = yield* Fiber.join(leaving)
        yield* settle("5 minutes")
        assert.strictEqual(left._tag, "Success")
        const aggregate = yield* replica.status
        assert.strictEqual(aggregate.spaces, 1)
        assert.strictEqual(aggregate.counts.failed, 0)
        return
      }
      yield* closing.release
      yield* Fiber.join(deactivation)
      yield* settle("5 minutes")
      const status = yield* space.status
      const aggregate = yield* replica.status
      assert.deepStrictEqual(
        { tag: status._tag, pending: status.pending, failed: aggregate.counts.failed, online: aggregate.counts.online },
        { tag: "Idle", pending: 0, failed: 0, online: 0 }
      )
    }, VirtualTime.scoped)
  )
})
