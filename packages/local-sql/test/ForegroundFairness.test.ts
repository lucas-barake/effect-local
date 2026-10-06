import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Scheduler from "effect/Scheduler"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import { type Constructor, constructors, healthyRemote, installView } from "./fixtures/BackgroundReplica.js"
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

const settle = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

const outcome = <A, E extends { readonly _tag: string },>(fiber: Fiber.Fiber<A, E>) => {
  const exit = fiber.pollUnsafe()
  if (exit === undefined) return "never completed"
  if (Exit.isSuccess(exit)) return "succeeded"
  return "failed"
}

const atBudget = <A, E extends { readonly _tag: string }, R,>(
  effect: Effect.Effect<A, E, R>,
  row: { readonly budget: number }
) => VirtualTime.scoped(effect).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, row.budget))

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
    }, atBudget)
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
    }, atBudget)
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
})
