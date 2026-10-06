import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import { constructors, describeExit, idleRemote, installView, within } from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000e401")

const spaceIds = Array.from({ length: 20 }, (_, index) => {
  const suffix = String(index + 1).padStart(4, "0")
  return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-00000000${suffix}`)
})

const reconciliationConcurrency = 8

describe("server calls left running by spaces that were visited during an outage", () => {
  it.effect.each(constructors)(
    "never outnumber the reconciliation concurrency with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: spaceIds,
        maximumActiveSpaces: spaceIds.length + 1,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      let outstanding = 0
      let most = 0
      const unanswered = Effect.suspend(() => {
        outstanding += 1
        most = Math.max(most, outstanding)
        const ended = Effect.sync(() => {
          outstanding -= 1
        })
        return Effect.ensuring(Effect.never, ended)
      })
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: () => unanswered,
        pull: () => unanswered
      }))
      yield* installView(services)
      const spaces = yield* Effect.forEach(spaceIds, (spaceId) => replica.space(spaceId))
      const visit = (space: (typeof spaces)[number]) =>
        space.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(Effect.andThen(Effect.sleep("100 millis")))

      const visited = yield* Effect.forEach(spaces, visit, { discard: true }).pipe(within)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(describeExit(visited), "succeeded")
      assert.isAtMost(most, reconciliationConcurrency, "server calls outstanding at once")
    }, VirtualTime.scoped)
  )
})
