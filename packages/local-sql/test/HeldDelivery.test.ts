import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
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
  viewId
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f11")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f12")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f13")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f11")

const twoSpaces = (constructor: Constructor) =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })

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

const settle = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("5 minutes"))

const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

const installView = (services: BackgroundReplica.Services) =>
  services.sql`UPDATE effect_local_client_spaces SET replication_view_id = ${viewId}, replication_view_revision = 0`

const heldFailureReport = Effect.fnUntraced(function*(constructor: Constructor) {
  const services = yield* twoSpaces(constructor)
  const delivery = yield* services.holdInvalidation(ReactivityKey.status(spaceId))
  let offline = false
  const replicaScope = yield* Scope.make()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => {
      if (!offline) return acceptSubmission(request)
      delivery.arm(1)
      return Effect.fail(new ReplicaError.ServerUnavailable())
    },
    pull: (request) => emptyPage(services.crypto, request)
  })).pipe(Scope.provide(replicaScope))
  yield* installView(services)
  const space = yield* replica.space(spaceId)
  yield* space.activate
  assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
  offline = true
  const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(
    Effect.exit,
    Effect.forkChild({ startImmediately: true })
  )
  yield* VirtualTime.advanceUntil(delivery.entered)
  offline = false
  assert.isTrue(mutated.pollUnsafe() !== undefined, "the mutation had returned, so the failure report is held")
  return { replica, replicaScope, space }
})

describe("a status subscriber of a failed foreground sync that never returns", () => {
  it.effect(
    "does not hold back a deactivation",
    Effect.fnUntraced(function*() {
      const { space } = yield* heldFailureReport("layer")

      const deactivating = yield* space.deactivate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle
      const deactivated = deactivating.pollUnsafe()

      assert.isTrue(deactivated !== undefined && Exit.isSuccess(deactivated), "the deactivation completed")
      assert.strictEqual(yield* space.activation, "Inactive")
    }, VirtualTime.scoped)
  )

  it.effect(
    "does not hold back a leave",
    Effect.fnUntraced(function*() {
      const { replica } = yield* heldFailureReport("layer")

      const leaving = yield* replica.leave(spaceId).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle
      const left = leaving.pollUnsafe()
      const remaining = yield* replica.spaces

      assert.isTrue(left !== undefined && Exit.isSuccess(left), "the leave completed")
      assert.strictEqual(remaining.length, 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not hold back the close of the replica with %s",
    Effect.fnUntraced(function*(constructor) {
      const { replicaScope } = yield* heldFailureReport(constructor)

      const closing = yield* Scope.close(replicaScope, Exit.void).pipe(Effect.forkChild({ startImmediately: true }))
      yield* settle

      assert.isTrue(closing.pollUnsafe() !== undefined, "the close completed")
      yield* Fiber.join(closing)
    }, VirtualTime.scoped)
  )
})

describe("a status subscriber of a failed background turn that never returns", () => {
  it.effect.each(constructors)(
    "does not hold back a foreground activation that takes the space over with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const delivery = yield* services.holdInvalidation(ReactivityKey.status(spaceId))
      let offline = true
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => {
          if (!offline) return acceptSubmission(request)
          delivery.arm(1)
          return Effect.fail(new ReplicaError.ServerUnavailable())
        },
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(delivery.entered)
      offline = false

      const activating = yield* space.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle
      const activated = activating.pollUnsafe()
      const status = yield* space.status

      assert.isTrue(activated !== undefined && Exit.isSuccess(activated), "the activation completed")
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(status.pending, 0)
    }, VirtualTime.scoped)
  )
})

describe("a membership subscriber of a join that never returns", () => {
  it.effect.each(constructors)(
    "does not hold back the interruption of the join or a join that waited for it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      let listedAnnouncements = 0
      let membershipAnnouncements = 0
      services.reactivity.registerUnsafe([ReactivityKey.spaces], () => {
        listedAnnouncements += 1
      })
      services.reactivity.registerUnsafe([ReactivityKey.membership(thirdSpaceId)], () => {
        membershipAnnouncements += 1
      })
      const delivery = yield* services.holdInvalidation(ReactivityKey.membership(thirdSpaceId))
      delivery.arm(1)
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces", true)
      const joining = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(inserting.entered)
      const waiting = yield* replica.join(thirdSpaceId).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* inserting.release
      yield* VirtualTime.advanceUntil(delivery.entered)

      const interrupting = yield* Fiber.interrupt(joining).pipe(Effect.forkChild({ startImmediately: true }))
      yield* settle
      const interrupted = interrupting.pollUnsafe() !== undefined
      const waited = waiting.pollUnsafe()
      const listed = yield* replica.spaces

      assert.isTrue(interrupted, "the join was interrupted while its notification was being delivered")
      assert.isTrue(waited !== undefined && Exit.isSuccess(waited), "the join that waited completed")
      assert.strictEqual(listed.length, 3, "the space was joined")
      assert.strictEqual(listedAnnouncements, 1, "the key the caller did not reach was delivered by the library")
      assert.isAbove(membershipAnnouncements, 0, "the key that was being delivered was delivered")
    }, VirtualTime.scoped)
  )
})
