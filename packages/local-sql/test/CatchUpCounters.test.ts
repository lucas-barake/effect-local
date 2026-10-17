import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  describeExit,
  emptyPage,
  idleRemote,
  installView,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000ff01")
const second = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000ff02")
const third = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000ff03")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000ff00")

const quiet = VirtualTime.quiet("1 minute")

type Ending = "leave" | "deactivate" | "retire"

const endings: ReadonlyArray<{ readonly ending: Ending; readonly maximumActiveSpaces: number }> = [
  { ending: "leave", maximumActiveSpaces: 3 },
  { ending: "deactivate", maximumActiveSpaces: 3 },
  { ending: "retire", maximumActiveSpaces: 3 },
  { ending: "leave", maximumActiveSpaces: 2 },
  { ending: "deactivate", maximumActiveSpaces: 2 },
  { ending: "retire", maximumActiveSpaces: 2 }
]

describe("a catch up pull that ends early", () => {
  it.effect.each(endings)(
    "leaves the turn and slot for the next catch up after $ending with $maximumActiveSpaces active",
    Effect.fnUntraced(function*(row) {
      const services = yield* BackgroundReplica.services({
        constructor: "layerWorkflow",
        clientId,
        initialSpaces: [first, second, third],
        maximumActiveSpaces: row.maximumActiveSpaces,
        foregroundActiveSpaces: 1,
        reconciliationConcurrency: 2,
        retryDelay: "10 minutes",
        maximumRetryDelay: "10 minutes"
      })
      const gates = new Map([[first, yield* Deferred.make<void>()], [second, yield* Deferred.make<void>()]])
      const waiting = new Map<Identity.SpaceId, number>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          const gate = gates.get(request.spaceId)
          if (gate === undefined) return page
          return Effect.suspend(() => {
            waiting.set(request.spaceId, (waiting.get(request.spaceId) ?? 0) + 1)
            return Deferred.await(gate).pipe(
              Effect.ensuring(Effect.sync(() => {
                waiting.set(request.spaceId, (waiting.get(request.spaceId) ?? 1) - 1)
              })),
              Effect.andThen(page)
            )
          })
        }
      }))
      yield* installView(services)
      const a = yield* replica.space(first)
      const b = yield* replica.space(second)
      const c = yield* replica.space(third)
      yield* VirtualTime.advanceUntil(a.activate)
      yield* quiet
      yield* b.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
      yield* quiet
      const catchingUp = [yield* a.activation, waiting.get(first) ?? 0]

      let ended: string
      if (row.ending === "leave") ended = describeExit(yield* within(replica.leave(first)))
      else if (row.ending === "deactivate") ended = describeExit(yield* within(a.deactivate))
      else ended = describeExit(yield* within(a.activate))
      yield* quiet
      if (row.ending === "retire") {
        yield* b.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
        yield* quiet
      }
      const afterTheEnd = waiting.get(first) ?? 0

      yield* Deferred.succeed(gates.get(first)!, undefined)
      yield* quiet
      yield* c.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
      yield* quiet
      const next = [yield* b.activation, waiting.get(second) ?? 0]

      yield* Deferred.succeed(gates.get(second)!, undefined)
      yield* quiet

      assert.deepStrictEqual(catchingUp, ["Active", 1], "the first space catches up in the background")
      assert.strictEqual(ended, "succeeded")
      assert.isAtMost(afterTheEnd, 1)
      assert.deepStrictEqual(next, ["Active", 1], "the next evicted space catches up in the background")
      assert.strictEqual(yield* services.unfinishedWorkflowExecutions(second), 0)
    }, VirtualTime.scoped)
  )
})
