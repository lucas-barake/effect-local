import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  constructors,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceIds = Array.from({ length: 20 }, (_, index) => {
  const suffix = String(index + 1).padStart(2, "0")
  return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-00000000f9${suffix}`)
})
const [home, turned, wanted] = spaceIds
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f900")

const quiet = VirtualTime.quiet("1 minute")

describe("a caller on a space whose background turn waits for a background slot", () => {
  it.effect.each(constructors)(
    "is served in the foreground while another space keeps the only slot with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [home, turned, wanted],
        maximumActiveSpaces: 2,
        foregroundActiveSpaces: 1,
        retryDelay: "10 minutes",
        maximumRetryDelay: "10 minutes"
      })
      yield* BackgroundReplica.seedPending(services, [turned, wanted])
      const pulling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      let held = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== turned || held) return page
          held = true
          return Deferred.succeed(pulling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(page)
          )
        }
      }))
      const current = yield* replica.space(home)
      const slotHolder = yield* replica.space(turned)
      const queued = yield* replica.space(wanted)
      yield* VirtualTime.advanceUntil(Deferred.await(pulling))
      const delivery = yield* services.holdInvalidationsOf([ReactivityKey.entity(home, Domain.Todo.name, "held")])
      const writing = yield* current.mutate(Domain.PutTodo, Domain.todo("held")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* quiet
      const reading = yield* queued.get(Domain.Todo, "pending").pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* quiet
      const activating = yield* slotHolder.activate.pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      yield* quiet
      const waitingForThePlace = [delivery.entered(), reading.pollUnsafe() === undefined, yield* slotHolder.activation]

      yield* Deferred.succeed(answered, undefined)
      yield* quiet
      yield* delivery.release
      const outcomes = [
        describeExit(yield* within(Fiber.join(writing))),
        describeExit(yield* within(Fiber.join(reading))),
        describeExit(yield* within(Fiber.join(activating)))
      ]
      yield* quiet

      assert.deepStrictEqual(waitingForThePlace, [1, true, "Active"], "both wait behind the space in use")
      assert.deepStrictEqual(outcomes, ["succeeded", "succeeded", "succeeded"])
      assert.strictEqual(yield* slotHolder.activation, "Active", "the last space asked for holds the foreground place")
      assert.strictEqual((yield* queued.status).pending, 0, "the space that waited for the slot synced after it")
    }, VirtualTime.scoped)
  )
})

const attemptBudgets = [{ budget: 200 }, { budget: 64 }]

describe("a background runtime that only a workflow attempt holds", () => {
  it.effect.each(attemptBudgets)(
    "is closed when the attempt ends although the only worker waits for its slot at a budget of $budget",
    Effect.fnUntraced(function*(_row) {
      const services = yield* BackgroundReplica.services({
        constructor: "layerWorkflow",
        clientId,
        initialSpaces: [home, turned, wanted],
        maximumActiveSpaces: 2,
        foregroundActiveSpaces: 1,
        retryDelay: "10 minutes",
        maximumRetryDelay: "10 minutes"
      })
      const state = { gate: yield* Deferred.make<void>(), free: 0, waiting: 0 }
      const submitted = yield* Deferred.make<void>()
      const accepted = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => {
          if (request.envelopes[0].spaceId !== wanted) return acceptSubmission(request)
          return Deferred.succeed(submitted, undefined).pipe(
            Effect.andThen(Deferred.await(accepted)),
            Effect.andThen(acceptSubmission(request))
          )
        },
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== turned) return page
          return Effect.suspend(() => {
            if (state.free > 0) {
              state.free -= 1
              return page
            }
            state.waiting += 1
            return Deferred.await(state.gate).pipe(
              Effect.ensuring(Effect.sync(() => {
                state.waiting -= 1
              })),
              Effect.andThen(page)
            )
          })
        }
      }))
      yield* installView(services)
      const current = yield* replica.space(home)
      const owned = yield* replica.space(turned)
      const queued = yield* replica.space(wanted)
      const answer = Effect.fnUntraced(function*(free: number) {
        const held = state.gate
        state.gate = yield* Deferred.make<void>()
        state.free = free
        return yield* Deferred.succeed(held, undefined)
      })
      yield* owned.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
      yield* quiet
      yield* current.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
      yield* quiet
      yield* answer(1)
      yield* quiet
      const ownedByTheAttempt = [yield* owned.activation, (yield* owned.status).pending, state.waiting]
      yield* queued.mutate(Domain.PutTodo, Domain.todo("queued")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(submitted))
      yield* current.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
      yield* Deferred.succeed(accepted, undefined)
      yield* quiet
      const whileTheAttemptHolds = [yield* owned.activation, yield* queued.activation, (yield* queued.status).pending]

      yield* answer(1_000)
      yield* quiet

      assert.deepStrictEqual(ownedByTheAttempt, ["Active", 0, 1], "the attempt pulls on the only background runtime")
      assert.deepStrictEqual(whileTheAttemptHolds, ["Active", "Inactive", 1], "the other space waits for the slot")
      assert.deepStrictEqual(
        [yield* owned.activation, (yield* queued.status).pending],
        ["Inactive", 0],
        "the slot went to the space that waited for it"
      )
    }, VirtualTime.atBudget)
  )
})

const errand = (space: Replica.Space, index: number, lap: number) => {
  const put = (round: string) =>
    space.mutate(Domain.PutTodo, Domain.todo(`${index}-${lap}-${round}`)).pipe(Effect.asVoid)
  const pause = Effect.sleep(`${40 + ((index + lap * 4) % 9) * 35} millis`)
  return put("one").pipe(
    Effect.andThen(pause),
    Effect.andThen(space.get(Domain.Todo, `${index}-${lap}-one`)),
    Effect.andThen(pause),
    Effect.andThen(put("two")),
    Effect.andThen(space.deactivate),
    Effect.andThen(pause),
    Effect.andThen(space.pending),
    Effect.andThen(pause),
    Effect.andThen(put("three")),
    Effect.andThen(space.activate),
    Effect.andThen(pause),
    Effect.andThen(space.pending),
    Effect.asVoid
  )
}

const limits = [
  { spaces: 8, maximumActiveSpaces: 2, foregroundActiveSpaces: 1 },
  { spaces: 8, maximumActiveSpaces: 3, foregroundActiveSpaces: 1 },
  { spaces: 8, maximumActiveSpaces: 3, foregroundActiveSpaces: 2 },
  { spaces: 20, maximumActiveSpaces: 16, foregroundActiveSpaces: 4 }
]
const rows = constructors.flatMap((constructor) =>
  limits.flatMap((limit) =>
    [2048, 64].map((budget) => ({
      constructor,
      spaces: limit.spaces,
      maximumActiveSpaces: limit.maximumActiveSpaces,
      foregroundActiveSpaces: limit.foregroundActiveSpaces,
      budget
    }))
  )
)

describe("more spaces in use than may be active at once", () => {
  it.effect.each(rows)(
    "completes every operation with $spaces spaces, $maximumActiveSpaces active, $foregroundActiveSpaces in the foreground, $constructor at a budget of $budget",
    Effect.fnUntraced(function*(row) {
      const used = spaceIds.slice(0, row.spaces)
      const services = yield* BackgroundReplica.services({
        constructor: row.constructor,
        clientId,
        initialSpaces: used,
        maximumActiveSpaces: row.maximumActiveSpaces,
        foregroundActiveSpaces: row.foregroundActiveSpaces,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      const calling = new Map<Identity.SpaceId, number>()
      let mostSpacesCalling = 0
      const answered = <A, E extends { readonly _tag: string },>(
        spaceId: Identity.SpaceId,
        answer: Effect.Effect<A, E>
      ) =>
        Effect.suspend(() => {
          calling.set(spaceId, (calling.get(spaceId) ?? 0) + 1)
          mostSpacesCalling = Math.max(mostSpacesCalling, calling.size)
          return Effect.sleep("300 millis").pipe(
            Effect.andThen(answer),
            Effect.ensuring(Effect.sync(() => {
              const left = (calling.get(spaceId) ?? 1) - 1
              if (left === 0) calling.delete(spaceId)
              else calling.set(spaceId, left)
            }))
          )
        })
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => answered(request.envelopes[0].spaceId, acceptSubmission(request)),
        pull: (request) => answered(request.spaceId, emptyPage(services.crypto, request))
      }))
      yield* installView(services)
      const spaces = yield* Effect.forEach(used, (spaceId) => replica.space(spaceId))
      const fibers = yield* Effect.forEach(spaces, (space, index) =>
        errand(space, index, 0).pipe(
          Effect.andThen(errand(space, index, 1)),
          Effect.forkChild({ startImmediately: true })
        ))
      const finished = yield* within(Fiber.awaitAll(fibers))
      const stuck = fibers.filter((fiber) => fiber.pollUnsafe()?._tag !== "Success").length
      let drained = 0
      for (const space of spaces) {
        if (Option.isSome(yield* eventually(services, space, (status) => status.pending === 0))) drained += 1
      }

      assert.strictEqual(describeExit(finished), "succeeded", `${stuck} of ${row.spaces} callers never finished`)
      assert.strictEqual(stuck, 0)
      assert.strictEqual(drained, row.spaces, "every space synced what its caller wrote")
      assert.isAtMost(mostSpacesCalling, row.maximumActiveSpaces, "spaces with a server call in flight at once")
    }, VirtualTime.atBudget),
    300_000
  )
})
