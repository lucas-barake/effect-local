import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  type Constructor,
  constructors,
  describeExit,
  emptyPage,
  eventually,
  healthyRemote,
  installView,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f61")
const second = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f62")
const third = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f63")
const fourth = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f64")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f61")

const budgets = [2048, 500, 200, 64] as const

const rows = constructors.flatMap((constructor) => budgets.map((budget) => ({ constructor, budget })))

const activationLimit = 240

const oneForegroundPlace = Effect.fnUntraced(function*(
  constructor: Constructor,
  spaceIds: ReadonlyArray<Identity.SpaceId>
) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: spaceIds,
    maximumActiveSpaces: spaceIds.length + 1,
    foregroundActiveSpaces: 1,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const replica = yield* services.start(healthyRemote(services))
  yield* installView(services)
  const spaces = yield* Effect.forEach(spaceIds, (spaceId) => replica.space(spaceId))
  const runaway = yield* Deferred.make<void>()
  let activations = 0
  for (const spaceId of spaceIds) {
    services.reactivity.registerUnsafe([ReactivityKey.activation(spaceId)], () => {
      activations += 1
      if (activations === activationLimit) Deferred.doneUnsafe(runaway, Exit.void)
    })
  }
  return { services, spaces, runaway: Deferred.await(runaway), activations: () => activations }
})

const settle = VirtualTime.quiet("1 minute")

const busyKey = (spaceId: Identity.SpaceId) => ReactivityKey.entity(spaceId, Domain.Todo.name, "busy")

const twoForegroundPlaces = Effect.fnUntraced(function*(constructor: Constructor) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [first, second, third, fourth],
    maximumActiveSpaces: 5,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const replica = yield* services.start(healthyRemote(services))
  yield* installView(services)
  const spaces = yield* Effect.forEach([first, second, third, fourth], (spaceId) => replica.space(spaceId))
  const hold = (space: Replica.Space) => space.mutate(Domain.PutTodo, Domain.todo("busy")).pipe(Effect.exit)
  return { services, spaces, hold }
})

const settled = <A, E extends { readonly _tag: string },>(fiber: Fiber.Fiber<Exit.Exit<A, E>>) => {
  const polled = fiber.pollUnsafe()
  if (polled === undefined || !Exit.isSuccess(polled)) return "never completed"
  return describeExit(Option.some(polled.value))
}

const outcome = <A, E extends { readonly _tag: string },>(fiber: Fiber.Fiber<A, E>) => {
  const exit = fiber.pollUnsafe()
  if (exit === undefined) return "never completed"
  if (Exit.isSuccess(exit)) return "succeeded"
  return "failed"
}

describe("operations that compete for one foreground place", () => {
  it.effect.each(rows)(
    "all complete when the resident is left while two spaces wait at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { activations, runaway, services, spaces } = yield* oneForegroundPlace(
        row.constructor,
        [first, second, third]
      )
      const [a, b, c] = spaces
      yield* VirtualTime.advanceUntil(a.activate)
      yield* settle
      const before = activations()
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const operation = yield* a.pending.pipe(Effect.asVoid, Effect.forkChild({ startImmediately: true }))
      yield* VirtualTime.advanceUntil(reading.entered)
      const waitingB = yield* b.mutate(Domain.PutTodo, Domain.todo("b")).pipe(
        Effect.asVoid,
        Effect.forkChild({ startImmediately: true })
      )
      const waitingC = yield* c.mutate(Domain.PutTodo, Domain.todo("c")).pipe(
        Effect.asVoid,
        Effect.forkChild({ startImmediately: true })
      )
      const reactivating = yield* a.deactivate.pipe(
        Effect.andThen(a.activate),
        Effect.forkChild({ startImmediately: true })
      )
      yield* reading.release
      const competing = [operation, waitingB, waitingC, reactivating]
      yield* Effect.raceFirst(Fiber.awaitAll(competing), runaway)
      const used = activations() - before

      assert.deepStrictEqual(
        competing.map(outcome),
        ["succeeded", "succeeded", "succeeded", "succeeded"],
        `every competing operation finished within ${used} activation changes`
      )
      assert.isBelow(used, activationLimit, "the spaces did not keep taking the foreground place from each other")
    }, VirtualTime.atBudget)
  )

  it.effect.each(rows)(
    "serves every one of four contending spaces at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { activations, runaway, spaces } = yield* oneForegroundPlace(
        row.constructor,
        [first, second, third, fourth]
      )
      const rounds = ["one", "two", "three"]
      const work = (space: Replica.Space) =>
        Effect.forEach(rounds, (round) => space.mutate(Domain.PutTodo, Domain.todo(round)), { discard: true })
      const contenders = yield* Effect.forEach(spaces, (space) =>
        Effect.forkChild(work(space), {
          startImmediately: true
        }))
      yield* Effect.raceFirst(Fiber.awaitAll(contenders), runaway)

      assert.deepStrictEqual(
        contenders.map(outcome),
        ["succeeded", "succeeded", "succeeded", "succeeded"],
        `every contender finished within ${activations()} activation changes`
      )
      assert.isBelow(activations(), activationLimit, "each operation took the place at most once")
    }, VirtualTime.atBudget)
  )

  it.effect.each(constructors)(
    "serves a waiting space before later operations of the resident with %s",
    Effect.fnUntraced(function*(constructor) {
      const { runaway, services, spaces } = yield* oneForegroundPlace(constructor, [first, second])
      const [a, b] = spaces
      yield* VirtualTime.advanceUntil(a.activate)
      yield* settle
      const finished: Array<string> = []
      const record = (name: string) =>
        Effect.sync(() => {
          finished.push(name)
        })
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const rounds = ["one", "two", "three", "four", "five", "six"]
      const resident = yield* a.pending.pipe(
        Effect.andThen(
          Effect.forEach(rounds, (round) => a.mutate(Domain.PutTodo, Domain.todo(round)), { discard: true })
        ),
        Effect.andThen(record("resident")),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(reading.entered)
      const waiter = yield* b.mutate(Domain.PutTodo, Domain.todo("b")).pipe(
        Effect.andThen(record("waiter")),
        Effect.forkChild({ startImmediately: true })
      )
      yield* reading.release
      yield* Effect.raceFirst(Fiber.awaitAll([resident, waiter]), runaway)

      assert.deepStrictEqual(finished, ["waiter", "resident"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "serves three waiting spaces in the order they arrived with %s",
    Effect.fnUntraced(function*(constructor) {
      const { runaway, services, spaces } = yield* oneForegroundPlace(constructor, [first, second, third, fourth])
      const [resident, ...waiting] = spaces
      yield* VirtualTime.advanceUntil(resident.activate)
      yield* settle
      const finished: Array<number> = []
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const operation = yield* Effect.forkChild(resident.pending, { startImmediately: true })
      yield* VirtualTime.advanceUntil(reading.entered)
      const waiters = yield* Effect.forEach(
        waiting,
        (space, index) =>
          space.mutate(Domain.PutTodo, Domain.todo("waited")).pipe(
            Effect.andThen(Effect.sync(() => finished.push(index))),
            Effect.forkChild({ startImmediately: true })
          )
      )

      yield* reading.release
      yield* Effect.raceFirst(Fiber.awaitAll([operation, ...waiters]), runaway)

      assert.deepStrictEqual(finished, [0, 1, 2])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "does not let a later space take the place the first waiter is still freeing with %s",
    Effect.fnUntraced(function*(constructor) {
      const { runaway, services, spaces } = yield* oneForegroundPlace(constructor, [first, second, third])
      const [resident, head, later] = spaces
      yield* VirtualTime.advanceUntil(resident.activate)
      yield* settle
      const finished: Array<string> = []
      const record = (name: string) =>
        Effect.sync(() => {
          finished.push(name)
        })
      const freeing = yield* services.holdStatement("effect_local_client_pending_data", true)
      const leading = yield* head.mutate(Domain.PutTodo, Domain.todo("head")).pipe(
        Effect.andThen(record("head")),
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(freeing.entered)
      const residentWhileFreed = yield* resident.activation
      const following = yield* later.mutate(Domain.PutTodo, Domain.todo("later")).pipe(
        Effect.andThen(record("later")),
        Effect.forkChild({ startImmediately: true })
      )

      yield* freeing.release
      yield* Effect.raceFirst(Fiber.awaitAll([leading, following]), runaway)

      assert.strictEqual(
        residentWhileFreed,
        "Inactive",
        "the place was free while the first waiter counted pending work"
      )
      assert.deepStrictEqual(finished, ["head", "later"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "lets the next space take a free place while an earlier space with two waiters is still activating with %s",
    Effect.fnUntraced(function*(constructor) {
      const { hold, services, spaces } = yield* twoForegroundPlaces(constructor)
      const [a, b, x, y] = spaces
      const busy = yield* services.holdInvalidationsOf([busyKey(first), busyKey(second)])
      const residents = yield* Effect.forEach(
        [a, b],
        (space) => Effect.forkChild(hold(space), { startImmediately: true })
      )
      yield* settle
      const activating = yield* services.holdInvalidation(ReactivityKey.activation(third))
      activating.arm(1)
      const firstWaiter = yield* x.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      const secondWaiter = yield* x.get(Domain.Todo, "x").pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      const next = yield* y.get(Domain.Todo, "y").pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle

      yield* busy.release
      yield* VirtualTime.advanceUntil(activating.entered)
      yield* settle
      const nextWhileActivating = settled(next)
      yield* activating.release
      yield* VirtualTime.advanceUntil(Fiber.awaitAll([...residents, firstWaiter, secondWaiter, next]))

      assert.strictEqual(busy.entered(), 2)
      assert.strictEqual(
        nextWhileActivating,
        "succeeded",
        "the later space did not wait behind a waiter that needed no place"
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "wakes the next waiter when the first one takes a freed place with %s",
    Effect.fnUntraced(function*(constructor) {
      const { hold, services, spaces } = yield* twoForegroundPlaces(constructor)
      const [a, b, x, y] = spaces
      const busy = yield* services.holdInvalidationsOf([busyKey(first), busyKey(second)])
      const residents = yield* Effect.forEach(
        [a, b],
        (space) => Effect.forkChild(hold(space), { startImmediately: true })
      )
      yield* settle
      const activating = yield* services.holdInvalidation(ReactivityKey.activation(third))
      activating.arm(1)
      const head = yield* x.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      const next = yield* y.get(Domain.Todo, "y").pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle

      yield* busy.release
      yield* VirtualTime.advanceUntil(activating.entered)
      yield* settle
      const nextWhileActivating = settled(next)
      yield* activating.release
      yield* VirtualTime.advanceUntil(Fiber.awaitAll([...residents, head, next]))

      assert.strictEqual(nextWhileActivating, "succeeded", "the second place went to the next waiter at once")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "stops asking a busy resident to go once the waiter got the other place with %s",
    Effect.fnUntraced(function*(constructor) {
      const { hold, services, spaces } = yield* twoForegroundPlaces(constructor)
      const [a, b, x] = spaces
      const other = yield* services.holdInvalidation(busyKey(second))
      other.arm(1)
      yield* VirtualTime.advanceUntil(a.activate)
      const otherResident = yield* Effect.forkChild(hold(b), { startImmediately: true })
      yield* VirtualTime.advanceUntil(other.entered)
      const asked = yield* services.holdInvalidationsOf([busyKey(first)])
      const askedResident = yield* Effect.forkChild(hold(a), { startImmediately: true })
      yield* settle
      const activating = yield* services.holdInvalidation(ReactivityKey.activation(third))
      activating.arm(1)
      const waiter = yield* x.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* settle

      yield* other.release
      yield* VirtualTime.advanceUntil(activating.entered)
      const read = yield* within(a.get(Domain.Todo, "busy"))
      yield* activating.release
      yield* asked.release
      yield* VirtualTime.advanceUntil(Fiber.awaitAll([otherResident, askedResident, waiter]))

      assert.strictEqual(asked.entered(), 1)
      assert.strictEqual(describeExit(read), "succeeded", "the resident that kept its place takes new operations again")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "takes operations again on a busy space whose deactivation was interrupted with %s",
    Effect.fnUntraced(function*(constructor) {
      const { services, spaces } = yield* oneForegroundPlace(constructor, [first, second])
      const [resident] = spaces
      const busy = yield* services.holdInvalidationsOf([busyKey(first)])
      const operation = yield* resident.mutate(Domain.PutTodo, Domain.todo("busy")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* settle
      const deactivating = yield* Effect.forkChild(resident.deactivate, { startImmediately: true })
      yield* settle
      const blocked = yield* resident.get(Domain.Todo, "busy").pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* settle
      const blockedWhileDeactivating = settled(blocked)

      yield* Fiber.interrupt(deactivating)
      yield* settle
      const afterInterruption = settled(blocked)
      yield* busy.release
      yield* VirtualTime.advanceUntil(Fiber.awaitAll([operation, blocked]))

      assert.strictEqual(blockedWhileDeactivating, "never completed", "a pending deactivation admits no new operation")
      assert.strictEqual(afterInterruption, "succeeded", "the withdrawn deactivation admitted the waiting operation")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "holds back later operations of the resident for the second waiter after the first waiter left with %s",
    Effect.fnUntraced(function*(constructor) {
      const { runaway, services, spaces } = yield* oneForegroundPlace(constructor, [first, second, third])
      const [resident, leaving, staying] = spaces
      const busy = yield* services.holdInvalidationsOf([busyKey(first)])
      const operation = yield* resident.mutate(Domain.PutTodo, Domain.todo("busy")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* settle
      const head = yield* Effect.forkChild(leaving.get(Domain.Todo, "x"), { startImmediately: true })
      const waiter = yield* staying.mutate(Domain.PutTodo, Domain.todo("waiter")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* settle
      yield* Fiber.interrupt(head)
      yield* settle
      const later = yield* resident.mutate(Domain.PutTodo, Domain.todo("later")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* settle
      const laterWhileWaiting = settled(later)

      yield* busy.release
      yield* Effect.raceFirst(Fiber.awaitAll([operation, waiter, later]), runaway)

      assert.strictEqual(laterWhileWaiting, "never completed", "the second waiter asked the resident to go")
      assert.deepStrictEqual([settled(waiter), settled(later)], ["succeeded", "succeeded"])
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "interrupts a get that waits for a place held by an operation in flight with %s",
    Effect.fnUntraced(function*(constructor) {
      const { services, spaces } = yield* oneForegroundPlace(constructor, [first, second])
      const [resident, waiting] = spaces
      yield* VirtualTime.advanceUntil(resident.activate)
      yield* settle
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const operation = yield* Effect.forkChild(resident.pending, { startImmediately: true })
      yield* VirtualTime.advanceUntil(reading.entered)

      const blocked = yield* Effect.forkChild(waiting.get(Domain.Todo, "blocked"), { startImmediately: true })
      assert.strictEqual(yield* waiting.activation, "Inactive")
      yield* Fiber.interrupt(blocked)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(blocked)))

      yield* reading.release
      yield* Fiber.join(operation)
      assert.isTrue(Option.isNone(yield* VirtualTime.advanceUntil(resident.get(Domain.Todo, "resident"))))
      assert.isTrue(Option.isNone(yield* VirtualTime.advanceUntil(waiting.get(Domain.Todo, "after"))))
      assert.strictEqual(yield* resident.activation, "Inactive")
      assert.strictEqual(yield* waiting.activation, "Active")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "wakes a get waiting for a place after later waiters withdraw with %s",
    Effect.fnUntraced(function*(constructor) {
      const { services, spaces } = yield* oneForegroundPlace(constructor, [first, second, third, fourth])
      const [resident, waiting, ...later] = spaces
      yield* VirtualTime.advanceUntil(resident.activate)
      yield* settle
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const operation = yield* Effect.forkChild(resident.pending, { startImmediately: true })
      yield* VirtualTime.advanceUntil(reading.entered)

      const blocked = yield* Effect.forkChild(waiting.get(Domain.Todo, "waiting"), { startImmediately: true })
      const withdrawn = yield* Effect.forEach(
        later,
        (space) => Effect.forkChild(space.activate, { startImmediately: true })
      )
      yield* Fiber.interruptAll(withdrawn)
      yield* reading.release
      yield* Fiber.join(operation)

      assert.isTrue(Option.isNone(yield* VirtualTime.advanceUntil(Fiber.join(blocked))))
    }, (effect) => VirtualTime.scoped(effect).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 4)))
  )

  it.effect.each(constructors)(
    "releases the foreground reservation of an interrupted promotion from background with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [first, second],
        maximumActiveSpaces: 3,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      yield* BackgroundReplica.seedPending(services, [second])
      const pulling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...healthyRemote(services),
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== second) return page
          return Deferred.succeed(pulling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(page)
          )
        }
      }))
      const resident = yield* replica.space(first)
      const background = yield* replica.space(second)
      yield* VirtualTime.advanceUntil(Deferred.await(pulling))
      yield* VirtualTime.advanceUntil(resident.activate)
      yield* settle
      const reading = yield* services.holdStatement("effect_local_client_pending_data", true)
      const operation = yield* Effect.forkChild(resident.pending, { startImmediately: true })
      yield* VirtualTime.advanceUntil(reading.entered)

      const promotion = yield* Effect.forkChild(background.activate, { startImmediately: true })
      assert.strictEqual(yield* background.activation, "Active")
      yield* Fiber.interrupt(promotion)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(promotion)))

      yield* reading.release
      yield* Fiber.join(operation)
      yield* Deferred.succeed(answered, undefined)
      const drained = yield* eventually(
        services,
        background,
        (status) => status._tag === "Idle" && status.pending === 0
      )
      assert.isTrue(Option.isSome(drained), "the background turn finished and closed its runtime")
      assert.strictEqual(yield* background.activation, "Inactive")
      assert.strictEqual(yield* resident.activation, "Active")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "runs an operation on a space whose background turn is in flight and leaves the space in the background with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [first, second],
        maximumActiveSpaces: 3,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      yield* BackgroundReplica.seedPending(services, [first])
      const pulling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      let pulls = 0
      let interrupted = false
      let watches = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...healthyRemote(services),
        watch: (request) => {
          if (request.spaceId === first) watches += 1
          return Stream.never
        },
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          pulls += 1
          if (pulls > 1) return page
          return Deferred.succeed(pulling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(page),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted = true
              })
            )
          )
        }
      }))
      const space = yield* replica.space(first)
      yield* VirtualTime.advanceUntil(Deferred.await(pulling))

      const read = yield* within(space.get(Domain.Todo, "pending"))
      const watchesDuringTheTurn = watches
      yield* Deferred.succeed(answered, undefined)
      const drained = yield* eventually(services, space, (status) => status.pending === 0)
      yield* settle

      assert.strictEqual(describeExit(read), "succeeded")
      assert.isFalse(interrupted, "the background turn kept its server call")
      assert.strictEqual(watchesDuringTheTurn, 0)
      assert.isTrue(Option.isSome(drained), "the turn drained the space")
      assert.strictEqual(watches, 0, "a read that ended before the turn did not make the space foreground")
      assert.strictEqual(yield* space.activation, "Inactive", "the drained space was closed like any background space")
    }, VirtualTime.scoped)
  )
})
