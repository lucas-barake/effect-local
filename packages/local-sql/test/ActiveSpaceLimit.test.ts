import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  constructors,
  describeExit,
  emptyPage,
  idleRemote,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const home = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f901")
const turned = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f902")
const wanted = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f903")
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
