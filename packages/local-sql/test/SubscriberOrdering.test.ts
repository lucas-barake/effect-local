import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  captureErrors,
  type Constructor,
  constructors,
  count,
  describeExit,
  eventually,
  healthyRemote,
  idleRemote,
  installView,
  isOnlineDrained,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f01")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f02")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f03")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f01")

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

const firstTodo = Domain.todo("first")

const settle = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 second"))

const onlineSpace = Effect.fnUntraced(function*(services: BackgroundReplica.Services) {
  const replica = yield* services.start(healthyRemote(services))
  yield* installView(services)
  const space = yield* replica.space(spaceId)
  yield* space.activate
  assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
  return { replica, space }
})

const activationIs = (space: Replica.Space, expected: Replica.Activation) =>
  space.activation.pipe(
    Effect.map((activation) => activation === expected),
    Effect.catch(() => Effect.succeed(false))
  )

const throwingObserver = () => {
  let completed = 0
  const observe = () => {
    completed += 1
    decodeURIComponent("%")
  }
  return { observe, completed: () => completed }
}

describe("the subscribers of an operation when its caller resumes", () => {
  it.effect.each(constructors)(
    "were notified of the leave of an active space with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { replica } = yield* onlineSpace(services)
      const listed = count(services, ReactivityKey.spaces)
      const membership = count(services, ReactivityKey.membership(spaceId))
      const aggregate = count(services, ReactivityKey.aggregateStatus)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.map(() => ({ listed: listed(), membership: membership(), aggregate: aggregate() > 0 })),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(removal.entered)
      yield* removal.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(leaving))

      assert.deepStrictEqual(observed, { listed: 1, membership: 1, aggregate: true })
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the leave of an inactive space with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const listed = count(services, ReactivityKey.spaces)
      const membership = count(services, ReactivityKey.membership(spaceId))
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.map(() => ({ listed: listed(), membership: membership() })),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(removal.entered)
      yield* removal.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(leaving))

      assert.deepStrictEqual(observed, { listed: 1, membership: 1 })
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the join that a second join waited for with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const membership = count(services, ReactivityKey.membership(thirdSpaceId))
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces", true)
      const joining = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(inserting.entered)
      const waiting = yield* replica.join(thirdSpaceId).pipe(
        Effect.map(() => membership()),
        Effect.forkChild({ startImmediately: true })
      )
      yield* inserting.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(waiting))
      yield* Fiber.join(joining)

      assert.strictEqual(observed, 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the activation that a second activation waited for with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const activation = count(services, ReactivityKey.activation(spaceId))
      const building = yield* services.holdStatement("SELECT desired_scope_json", true)
      const activating = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(building.entered)
      const waiting = yield* space.activate.pipe(
        Effect.map(() => activation()),
        Effect.forkChild({ startImmediately: true })
      )
      yield* building.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(waiting))
      yield* Fiber.join(activating)

      assert.strictEqual(observed, 2)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the failed activation that a second activation waited for with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const activation = count(services, ReactivityKey.activation(spaceId))
      const aggregate = count(services, ReactivityKey.aggregateStatus)
      const building = yield* services.holdStatement("SELECT desired_scope_json")
      const activating = yield* space.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(building.entered)
      const waiting = yield* space.activate.pipe(
        Effect.exit,
        Effect.map(() => ({ activation: activation(), aggregate: aggregate() })),
        Effect.forkChild({ startImmediately: true })
      )
      yield* building.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(waiting))
      const failed = yield* Fiber.join(activating)

      assert.isTrue(Exit.isFailure(failed), "the first activation failed to build")
      assert.deepStrictEqual(observed, { activation: 2, aggregate: 2 })
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the deactivation that an activation waited for with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const activation = count(services, ReactivityKey.activation(spaceId))
      const closing = yield* services.holdInvalidationWhen(
        ReactivityKey.activation(spaceId),
        () => activationIs(space, "Deactivating")
      )
      const deactivating = yield* space.deactivate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(closing.entered)
      const waiting = yield* space.deactivate.pipe(
        Effect.map(() => activation()),
        Effect.forkChild({ startImmediately: true })
      )
      yield* closing.release
      const observed = yield* VirtualTime.advanceUntil(Fiber.join(waiting))
      yield* Fiber.join(deactivating)

      assert.strictEqual(observed, 2)
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "were notified of the commit when its mutation returns with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const pending = count(services, ReactivityKey.pending(spaceId))
      const aggregate = count(services, ReactivityKey.aggregateStatus)

      const observed = yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(
        Effect.map(() => ({ pending: pending(), aggregate: aggregate() }))
      )

      assert.deepStrictEqual(observed, { pending: 1, aggregate: 1 })
    }, VirtualTime.scoped)
  )
})

describe("a caller whose completion callback throws", () => {
  it.effect.each(constructors)(
    "does not keep the subscribers of the space list from learning of its leave with %s",
    Effect.fnUntraced(function*(constructor) {
      const errors = captureErrors()
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const listed = count(services, ReactivityKey.spaces)
      const membership = count(services, ReactivityKey.membership(spaceId))
      const aggregate = count(services, ReactivityKey.aggregateStatus)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
      const callback = throwingObserver()
      const leaving = yield* replica.leave(spaceId).pipe(
        Effect.provide(errors.layerLogs),
        Effect.forkChild({ startImmediately: true })
      )
      leaving.addObserver(callback.observe)
      yield* VirtualTime.advanceUntil(removal.entered)
      yield* removal.release
      yield* settle
      const remaining = yield* replica.spaces

      assert.strictEqual(callback.completed(), 1, "the leave completed and its callback threw")
      assert.strictEqual(remaining.length, 1, "the space was left")
      assert.deepStrictEqual(
        { listed: listed(), membership: membership(), aggregate: aggregate() },
        { listed: 1, membership: 1, aggregate: 1 }
      )
      assert.deepStrictEqual(errors.messages(), ["Completion callback died"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not fail the join it waited for or keep its subscribers from learning of it with %s",
    Effect.fnUntraced(function*(constructor) {
      const errors = captureErrors()
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const listed = count(services, ReactivityKey.spaces)
      const membership = count(services, ReactivityKey.membership(thirdSpaceId))
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces", true)
      const first = yield* replica.join(thirdSpaceId).pipe(
        Effect.exit,
        Effect.provide(errors.layerLogs),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(inserting.entered)
      const callback = throwingObserver()
      const waiting = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      waiting.addObserver(callback.observe)
      yield* inserting.release
      const joined = yield* VirtualTime.advanceUntil(Fiber.join(first))
      const spaces = yield* replica.spaces

      assert.strictEqual(callback.completed(), 1, "the join that waited completed and its callback threw")
      assert.isTrue(Exit.isSuccess(joined), "the first join")
      assert.strictEqual(spaces.length, 3, "the space was joined")
      assert.deepStrictEqual({ listed: listed(), membership: membership() }, { listed: 1, membership: 1 })
      assert.deepStrictEqual(errors.messages(), ["Completion callback died"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not fail the activation it waited for with %s",
    Effect.fnUntraced(function*(constructor) {
      const errors = captureErrors()
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const building = yield* services.holdStatement("SELECT desired_scope_json", true)
      const first = yield* space.activate.pipe(
        Effect.exit,
        Effect.provide(errors.layerLogs),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(building.entered)
      const callback = throwingObserver()
      const waiting = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      waiting.addObserver(callback.observe)
      yield* building.release
      const activated = yield* VirtualTime.advanceUntil(Fiber.join(first))

      assert.strictEqual(callback.completed(), 1, "the activation that waited completed and its callback threw")
      assert.isTrue(Exit.isSuccess(activated), "the first activation")
      assert.strictEqual(yield* space.activation, "Active")
      assert.deepStrictEqual(errors.messages(), ["Completion callback died"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not stop later mutations from committing when it waited for a mutation with %s",
    Effect.fnUntraced(function*(constructor) {
      const errors = captureErrors()
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services)).pipe(Effect.provide(errors.layerLogs))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came online")
      const callback = throwingObserver()
      const mutating = yield* space.mutate(Domain.PutTodo, firstTodo).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      mutating.addObserver(callback.observe)
      yield* settle

      const second = yield* space.mutate(Domain.PutTodo, Domain.todo("second")).pipe(
        Effect.exit,
        Effect.timeoutOption("5 minutes"),
        VirtualTime.advanceUntil
      )
      const drained = yield* eventually(services, space, isOnlineDrained)

      assert.strictEqual(callback.completed(), 1, "the mutation completed and its callback threw")
      assert.isTrue(Option.isSome(second) && Exit.isSuccess(second.value), "the next mutation committed")
      assert.isTrue(Option.isSome(drained), "both mutations synced")
      assert.deepStrictEqual(errors.messages(), ["Completion callback died"])
    }, VirtualTime.scoped)
  )
})

const outcome = <A, E extends { readonly _tag: string },>(fiber: Fiber.Fiber<A, E>) => {
  const exit = fiber.pollUnsafe()
  if (exit === undefined) return "never completed"
  if (Exit.isSuccess(exit)) return "succeeded"
  return "failed"
}

const allSucceeded = { first: "succeeded", throwing: "succeeded", later: "succeeded" }

describe("a waiter whose completion callback throws", () => {
  it.effect.each(constructors)(
    "does not strand a later caller that waited for the same activation with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const building = yield* services.holdStatement("SELECT desired_scope_json", true)
      const first = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(building.entered)
      const throwing = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* building.release
      yield* settle

      assert.deepStrictEqual(
        { first: outcome(first), throwing: outcome(throwing), later: outcome(later) },
        allSucceeded
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not strand a later caller that waited for the same join with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces", true)
      const first = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(inserting.entered)
      const throwing = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* inserting.release
      yield* settle

      assert.deepStrictEqual(
        { first: outcome(first), throwing: outcome(throwing), later: outcome(later) },
        allSucceeded
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not strand a later caller of the same leave with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const removal = yield* services.holdStatement("DELETE FROM effect_local_client_spaces", true)
      const first = yield* replica.leave(spaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(removal.entered)
      const throwing = yield* replica.leave(spaceId).pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* replica.leave(spaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* removal.release
      yield* settle

      assert.deepStrictEqual(
        { first: outcome(first), throwing: outcome(throwing), later: outcome(later) },
        allSucceeded
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not strand a later activation that waited for the same deactivation with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const closing = yield* services.holdInvalidationWhen(
        ReactivityKey.activation(spaceId),
        () => activationIs(space, "Deactivating")
      )
      const first = yield* space.deactivate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(closing.entered)
      const throwing = yield* space.deactivate.pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* space.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* closing.release
      yield* settle

      assert.deepStrictEqual(
        { first: outcome(first), throwing: outcome(throwing), later: outcome(later) },
        allSucceeded
      )
      assert.strictEqual(yield* space.activation, "Active")
    }, VirtualTime.scoped)
  )
})

describe("a space reactivated while its deactivation was still being announced", () => {
  it.effect.each(constructors)(
    "stays counted online with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { replica, space } = yield* onlineSpace(services)
      const delivery = yield* services.holdInvalidationWhen(
        ReactivityKey.activation(spaceId),
        () => activationIs(space, "Inactive")
      )
      const deactivating = yield* space.deactivate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(delivery.entered)
      yield* space.activate
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the space came back online")
      yield* delivery.release
      const deactivated = yield* VirtualTime.advanceUntil(Fiber.join(deactivating))
      yield* settle
      const aggregate = yield* replica.status

      assert.isTrue(Exit.isSuccess(deactivated), "the deactivation")
      assert.strictEqual(yield* space.activation, "Active")
      assert.strictEqual(aggregate.counts.online, 1)
      assert.strictEqual(aggregate.counts.idle, 1)
    }, VirtualTime.scoped)
  )
})

describe("an activation that fails to build", () => {
  it.effect.each(constructors)(
    "announces the space status with each activation change with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const space = yield* replica.space(spaceId)
      const activation = count(services, ReactivityKey.activation(spaceId))
      const status = count(services, ReactivityKey.status(spaceId))
      services.lockNext("SELECT desired_scope_json")

      const activated = yield* within(space.activate)

      assert.strictEqual(describeExit(activated), "failed", "the activation")
      assert.deepStrictEqual({ activation: activation(), status: status() }, { activation: 2, status: 2 })
    }, VirtualTime.scoped)
  )
})

describe("a join that fails", () => {
  it.effect.each(constructors)(
    "releases a join that waited for it with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const inserting = yield* services.holdStatement("INSERT INTO effect_local_client_spaces")
      const first = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(inserting.entered)
      const waiting = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
      yield* inserting.release

      const failed = yield* within(Fiber.join(first))
      const waited = yield* within(Fiber.join(waiting))

      assert.strictEqual(describeExit(failed), "failed", "the first join")
      assert.strictEqual(describeExit(waited), "succeeded", "the join that waited")
      assert.strictEqual((yield* replica.spaces).length, 3)
    }, VirtualTime.scoped)
  )
})

describe("a join that its caller interrupts at any point", () => {
  it.effect.each(constructors)(
    "leaves the space list and the aggregate status in agreement and a later join able to complete with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const replica = yield* services.start(idleRemote)
      const disagreements: Array<string> = []
      const stuck: Array<number> = []
      for (let yields = 0; yields < 20; yields++) {
        const joining = yield* replica.join(thirdSpaceId).pipe(Effect.forkChild({ startImmediately: true }))
        for (let step = 0; step < yields; step++) yield* Effect.yieldNow
        yield* Fiber.interrupt(joining)
        const listed = (yield* replica.spaces).length
        const counted = (yield* replica.status).spaces
        if (listed !== counted) disagreements.push(`after ${yields} yields: ${listed} listed, ${counted} counted`)
        const later = yield* within(replica.join(thirdSpaceId))
        if (describeExit(later) !== "succeeded") stuck.push(yields)
        yield* within(replica.leave(thirdSpaceId))
      }

      assert.deepStrictEqual(disagreements, [])
      assert.deepStrictEqual(stuck, [])
    }, VirtualTime.scoped)
  )
})

describe("the subscribers of a commit", () => {
  it.effect.each(constructors)(
    "learn of the aggregate status before the pending mutations with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const order: Array<string> = []
      services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        order.push("aggregate")
      })
      services.reactivity.registerUnsafe([ReactivityKey.pending(spaceId)], () => {
        order.push("pending")
      })

      yield* space.mutate(Domain.PutTodo, firstTodo)

      assert.deepStrictEqual(order.slice(0, 2), ["aggregate", "pending"])
    }, VirtualTime.scoped)
  )
})

describe("a waiter of a shared signal whose completion callback throws", () => {
  it.effect.each(constructors)(
    "does not strand another activation that waited for the same foreground slot with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [spaceId, otherSpaceId, thirdSpaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      const replica = yield* services.start(healthyRemote(services))
      yield* installView(services)
      const occupant = yield* replica.space(spaceId)
      const other = yield* replica.space(otherSpaceId)
      const third = yield* replica.space(thirdSpaceId)
      const building = yield* services.holdStatement("SELECT desired_scope_json", true)
      const occupying = yield* occupant.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(building.entered)
      const throwing = yield* other.activate.pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* third.activate.pipe(Effect.forkChild({ startImmediately: true }))
      yield* building.release
      yield* settle

      assert.deepStrictEqual(
        { occupying: outcome(occupying), throwing: outcome(throwing), later: outcome(later) },
        { occupying: "succeeded", throwing: "succeeded", later: "succeeded" }
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not strand another reader that waited for the same settlement with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* twoSpaces(constructor)
      const { space } = yield* onlineSpace(services)
      const throwing = yield* Stream.runHead(space.settlements()).pipe(Effect.forkChild({ startImmediately: true }))
      throwing.addObserver(throwingObserver().observe)
      const later = yield* Stream.runHead(space.settlements()).pipe(Effect.forkChild({ startImmediately: true }))
      yield* settle
      yield* space.mutate(Domain.PutTodo, firstTodo)
      assert.isTrue(Option.isSome(yield* eventually(services, space, isOnlineDrained)), "the mutation settled")
      yield* settle

      assert.strictEqual(outcome(throwing), "succeeded", "the reader whose callback throws")
      assert.strictEqual(outcome(later), "succeeded", "the other reader")
    }, VirtualTime.scoped)
  )
})
