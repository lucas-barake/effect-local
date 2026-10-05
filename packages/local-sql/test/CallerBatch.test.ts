import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
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

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f31")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f32")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f31")

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

const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

const healthyRemote = (services: BackgroundReplica.Services) =>
  SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => emptyPage(services.crypto, request)
  })

const installView = (services: BackgroundReplica.Services) =>
  services.sql`UPDATE effect_local_client_spaces SET replication_view_id = ${viewId}, replication_view_revision = 0`

const onlineSpace = Effect.fnUntraced(function*(services: BackgroundReplica.Services) {
  const replica = yield* services.start(healthyRemote(services))
  yield* installView(services)
  const space = yield* replica.space(spaceId)
  yield* space.activate
  assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
  return { replica, space }
})

const count = (services: BackgroundReplica.Services, key: string) => {
  let delivered = 0
  services.reactivity.registerUnsafe([key], () => {
    delivered += 1
  })
  return () => delivered
}

describe("notifications raised inside a batch of the caller", () => {
  it.effect.each(constructors)(
    "are delivered once per key when the batch around an activation ends with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const activation = count(services, ReactivityKey.activation(spaceId))

      const insideBatch = yield* space.activate.pipe(
        Effect.andThen(Reactivity.invalidate([ReactivityKey.activation(spaceId)])),
        Effect.map(() => activation()),
        services.reactivity.withBatch,
        Effect.provideService(Reactivity.Reactivity, services.reactivity)
      )

      assert.strictEqual(insideBatch, 0, "nothing was delivered before the batch ended")
      assert.strictEqual(activation(), 1, "the two activation changes and the caller's own were delivered once")
      assert.strictEqual(yield* space.activation, "Active")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "do not reach the activation when a subscriber throws at the end of the batch with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      services.reactivity.registerUnsafe([ReactivityKey.activation(spaceId)], () => {
        decodeURIComponent("%")
      })

      const activated = yield* space.activate.pipe(Effect.exit, services.reactivity.withBatch, Effect.exit)
      const activation = yield* space.activation
      const mutated = yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(Effect.exit)
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Exit.isFailure(activated), "the batch of the caller ended with the subscriber defect")
      assert.strictEqual(activation, "Active")
      assert.isTrue(Exit.isSuccess(mutated), "the next mutation")
      assert.isTrue(Option.isSome(drained), "the mutation synced")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "do not capture the notifications of a runtime that was activated inside the batch with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* services.reactivity.withBatch(space.activate)
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      const pending = count(services, ReactivityKey.pending(spaceId))
      const status = count(services, ReactivityKey.status(spaceId))
      const aggregate = count(services, ReactivityKey.aggregateStatus)
      const entity = count(services, ReactivityKey.entity(spaceId, Domain.Todo.name, "first"))

      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.isTrue(Option.isSome(drained), "the mutation synced")
      assert.isAbove(entity(), 0, "the entity was announced")
      assert.isAbove(pending(), 0, "the pending mutations were announced")
      assert.isAbove(status(), 0, "the space status was announced")
      assert.isAbove(aggregate(), 0, "the aggregate status was announced")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "are delivered when the batch around a mutation ends with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const aggregate = count(services, ReactivityKey.aggregateStatus)
      const pending = count(services, ReactivityKey.pending(spaceId))

      yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(services.reactivity.withBatch)

      assert.isAbove(pending(), 0, "the pending mutations were announced")
      assert.isAbove(aggregate(), 0, "the aggregate status was announced")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "are delivered for a leave that its caller abandoned with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const listed = count(services, ReactivityKey.spaces)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
      const leaving = yield* replica.leave(spaceId).pipe(
        services.reactivity.withBatch,
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(removal.entered)
      yield* Fiber.interrupt(leaving)
      yield* removal.release
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 second"))
      const remaining = yield* replica.spaces

      assert.strictEqual(remaining.length, 1, "the space was left")
      assert.strictEqual(listed(), 1, "the space list was announced")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "include the replication view status of the runtime, which is delivered before the batch ends with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const status = count(services, ReactivityKey.status(spaceId))

      const insideBatch = yield* space.activate.pipe(
        Effect.map(() => status()),
        services.reactivity.withBatch,
        VirtualTime.advanceUntil
      )

      assert.strictEqual(insideBatch, 1)
    }, VirtualTime.scoped)
  )
})
