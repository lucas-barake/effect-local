import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
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

const home = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f800")
const busy = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f801")
const unrelated = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f802")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f800")
const backgroundWorkers = 2

const quiet = VirtualTime.quiet("1 minute")

describe("a space written to while its background turn is in flight", () => {
  it.effect.each(constructors)(
    "keeps one background worker and syncs the write when the turn ends with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [home, busy, unrelated],
        maximumActiveSpaces: backgroundWorkers + 1,
        foregroundActiveSpaces: 1,
        retryDelay: "10 minutes",
        maximumRetryDelay: "10 minutes"
      })
      yield* BackgroundReplica.seedPending(services, [busy])
      const submitting = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      const elsewhere = yield* Deferred.make<void>()
      let held = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: (request) => emptyPage(services.crypto, request),
        submitBatch: (request) => {
          const spaceId = request.envelopes[0].spaceId
          if (spaceId === unrelated) return Effect.andThen(Deferred.await(elsewhere), acceptSubmission(request))
          if (spaceId !== busy || held) return acceptSubmission(request)
          held = true
          return Deferred.succeed(submitting, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(acceptSubmission(request))
          )
        }
      }))
      const written = yield* replica.space(busy)
      const other = yield* replica.space(unrelated)
      yield* VirtualTime.advanceUntil(Deferred.await(submitting))

      const during = yield* written.mutate(Domain.PutTodo, Domain.todo("during")).pipe(within)
      yield* quiet
      const left = yield* other.mutate(Domain.PutTodo, Domain.todo("elsewhere")).pipe(
        Effect.andThen(other.deactivate),
        within
      )
      yield* Deferred.succeed(elsewhere, undefined)
      yield* quiet
      const whileTheCallIsHeld = [(yield* other.status).pending, (yield* written.status).pending]
      yield* Deferred.succeed(answered, undefined)
      yield* quiet

      assert.strictEqual(describeExit(during), "succeeded")
      assert.strictEqual(describeExit(left), "succeeded")
      assert.deepStrictEqual(whileTheCallIsHeld, [0, 2], "the other background worker synced the unrelated space")
      assert.strictEqual((yield* written.status).pending, 0, "the write made during the turn synced after it")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "leaves the write to the foreground runtime when the space was promoted with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [busy, unrelated],
        maximumActiveSpaces: 2,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 second"
      })
      yield* BackgroundReplica.seedPending(services, [busy, unrelated])
      const pulling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      const promoted = yield* Deferred.make<void>()
      const calls = { pulls: 0, submits: 0, elsewhere: 0 }
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== busy) return page
          calls.pulls += 1
          if (calls.pulls !== 2) return page
          return Deferred.succeed(pulling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          )
        },
        submitBatch: (request) => {
          if (request.envelopes[0].spaceId === unrelated) {
            calls.elsewhere += 1
            if (calls.elsewhere === 1) return Effect.fail(new ReplicaError.ServerUnavailable())
            return acceptSubmission(request)
          }
          calls.submits += 1
          if (calls.submits === 1) return acceptSubmission(request)
          return Effect.andThen(Deferred.succeed(promoted, undefined), Effect.never)
        }
      }))
      const written = yield* replica.space(busy)
      const other = yield* replica.space(unrelated)
      yield* VirtualTime.advanceUntil(Deferred.await(pulling))
      const delivery = yield* services.holdInvalidationsOf([ReactivityKey.entity(busy, Domain.Todo.name, "during")])
      const writing = yield* written.mutate(Domain.PutTodo, Domain.todo("during")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* quiet

      yield* Deferred.succeed(answered, undefined)
      yield* quiet
      const whileTheWriteIsHeld = [delivery.entered(), calls.submits, yield* written.activation]
      yield* delivery.release
      yield* VirtualTime.advanceUntil(Fiber.await(writing))
      yield* VirtualTime.advanceUntil(Deferred.await(promoted))
      yield* quiet

      assert.deepStrictEqual(whileTheWriteIsHeld, [1, 1, "Active"], "the write outlasted the turn")
      assert.deepStrictEqual([yield* written.activation, (yield* written.status)._tag], ["Active", "Connecting"])
      assert.strictEqual((yield* other.status).pending, 0, "the background worker was free for the unrelated space")
    }, VirtualTime.scoped)
  )
})
