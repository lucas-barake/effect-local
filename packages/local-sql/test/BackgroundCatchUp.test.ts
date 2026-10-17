import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  count,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const visited = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa01")
const elsewhere = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa02")
const loaded = [
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa03"),
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa04"),
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa05")
]
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000fa00")

const quiet = VirtualTime.quiet("1 minute")

interface Limits {
  readonly maximumActiveSpaces: number
  readonly reconciliationConcurrency?: number
  readonly seeded?: ReadonlyArray<Identity.SpaceId>
  readonly held?: ReadonlyArray<Identity.SpaceId>
}

const evicted = Effect.fnUntraced(function*(limits: Limits) {
  const services = yield* BackgroundReplica.services({
    constructor: "layerWorkflow",
    clientId,
    initialSpaces: [visited, elsewhere, ...loaded],
    foregroundActiveSpaces: 1,
    retryDelay: "10 minutes",
    maximumRetryDelay: "10 minutes",
    ...limits
  })
  yield* BackgroundReplica.seedPending(services, limits.seeded ?? [])
  const gates = new Map<Identity.SpaceId, Deferred.Deferred<void>>()
  for (const spaceId of [visited, ...(limits.held ?? [])]) gates.set(spaceId, yield* Deferred.make<void>())
  const pulls = { waiting: 0, interrupted: 0, completed: 0 }
  const firstSubmits = new Set<Identity.SpaceId>()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => {
      const spaceId = request.envelopes[0].spaceId
      if (firstSubmits.has(spaceId) || limits.seeded?.includes(spaceId) === true) return acceptSubmission(request)
      firstSubmits.add(spaceId)
      return Effect.never
    },
    pull: (request) => {
      const page = emptyPage(services.crypto, request)
      const gate = gates.get(request.spaceId)
      if (gate === undefined) return page
      if (request.spaceId !== visited) return Effect.andThen(Deferred.await(gate), page)
      return Effect.suspend(() => {
        pulls.waiting += 1
        return Deferred.await(gate).pipe(
          Effect.andThen(page),
          Effect.tap(() =>
            Effect.sync(() => {
              pulls.completed += 1
            })
          ),
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              pulls.interrupted += 1
            })
          ),
          Effect.ensuring(Effect.sync(() => {
            pulls.waiting -= 1
          }))
        )
      })
    }
  }))
  yield* installView(services)
  const space = yield* replica.space(visited)
  const other = yield* replica.space(elsewhere)
  const open = (spaceId: Identity.SpaceId) =>
    Effect.suspend(() => {
      const gate = gates.get(spaceId)
      if (gate === undefined) return Effect.void
      return Deferred.succeed(gate, undefined).pipe(Effect.asVoid)
    })
  const leaveWithPendingWork = Effect.fnUntraced(function*(spaceId: Identity.SpaceId) {
    const written = yield* replica.space(spaceId)
    yield* written.mutate(Domain.PutTodo, Domain.todo("pending")).pipe(VirtualTime.advanceUntil)
    yield* VirtualTime.advanceUntil(written.deactivate)
    return written
  })
  yield* VirtualTime.advanceUntil(space.activate)
  yield* quiet
  const whileVisited = { ...pulls }
  yield* other.get(Domain.Todo, "here").pipe(VirtualTime.advanceUntil)
  yield* quiet
  return { services, replica, space, other, pulls, whileVisited, open, leaveWithPendingWork }
})

describe("a workflow attempt with nothing to send whose space lost its foreground place", () => {
  it.effect(
    "pulls again on a background runtime and completes there",
    Effect.fnUntraced(function*() {
      const { open, pulls, services, space, whileVisited } = yield* evicted({ maximumActiveSpaces: 3 })
      const inTheBackground = [yield* space.activation, { ...pulls }]

      yield* open(visited)
      yield* quiet

      assert.deepStrictEqual(whileVisited, { waiting: 1, interrupted: 0, completed: 0 })
      assert.deepStrictEqual(inTheBackground, ["Active", { waiting: 1, interrupted: 1, completed: 0 }])
      assert.deepStrictEqual(
        [
          yield* space.activation,
          pulls.waiting,
          pulls.interrupted,
          yield* services.unfinishedWorkflowExecutions(visited)
        ],
        ["Inactive", 0, 1, 0],
        "the execution completed and its runtime was closed"
      )
      assert.isAtLeast(pulls.completed, 1)
    }, VirtualTime.scoped)
  )

  it.effect(
    "gives its turn permit to a space with pending work and pulls again after it",
    Effect.fnUntraced(function*() {
      const { leaveWithPendingWork, open, pulls, services, space } = yield* evicted({
        maximumActiveSpaces: 4,
        reconciliationConcurrency: 2
      })

      const written = yield* leaveWithPendingWork(loaded[0])
      const drained = yield* eventually(services, written, (status) => status.pending === 0)
      yield* quiet
      const afterTheWork = [yield* space.activation, pulls.waiting, pulls.completed]
      const yielded = pulls.interrupted - 1
      yield* open(visited)
      yield* quiet

      assert.isTrue(Option.isSome(drained), "the pending work synced while the catch up pull was never answered")
      assert.deepStrictEqual(afterTheWork, ["Active", 1, 0])
      assert.isAtLeast(yielded, 1, "the catch up pull was ended for the turn with pending work")
      assert.deepStrictEqual([yield* space.activation, yield* services.unfinishedWorkflowExecutions(visited)], [
        "Inactive",
        0
      ])
    }, VirtualTime.scoped)
  )

  it.effect(
    "gives its background slot to a space with pending work and pulls again after it",
    Effect.fnUntraced(function*() {
      const { leaveWithPendingWork, open, pulls, services, space } = yield* evicted({ maximumActiveSpaces: 2 })

      const written = yield* leaveWithPendingWork(loaded[0])
      const drained = yield* eventually(services, written, (status) => status.pending === 0)
      yield* quiet
      const afterTheWork = [yield* space.activation, yield* written.activation, { ...pulls }]
      yield* open(visited)
      yield* quiet

      assert.isTrue(Option.isSome(drained), "the pending work synced while the catch up pull was never answered")
      assert.deepStrictEqual(afterTheWork, ["Active", "Inactive", { waiting: 1, interrupted: 2, completed: 0 }])
      assert.deepStrictEqual([yield* space.activation, yield* services.unfinishedWorkflowExecutions(visited)], [
        "Inactive",
        0
      ])
    }, VirtualTime.scoped)
  )

  it.effect(
    "builds no runtime while background turns are queued",
    Effect.fnUntraced(function*() {
      const { open, pulls, services, replica, space } = yield* evicted({
        maximumActiveSpaces: 5,
        reconciliationConcurrency: 2,
        seeded: loaded,
        held: [loaded[0]]
      })
      const activations = count(services, ReactivityKey.activation(visited))
      const whileTheFirstTurnRuns = [yield* space.activation, { ...pulls }]

      yield* open(loaded[0])
      yield* quiet
      const afterTheTurns = [yield* space.activation, { ...pulls }]
      const pending: Array<number> = []
      for (const spaceId of loaded) pending.push((yield* (yield* replica.space(spaceId)).status).pending)
      yield* open(visited)
      yield* quiet

      assert.deepStrictEqual(whileTheFirstTurnRuns, ["Inactive", { waiting: 0, interrupted: 1, completed: 0 }])
      assert.deepStrictEqual(pending, [0, 0, 0])
      assert.deepStrictEqual(afterTheTurns, ["Active", { waiting: 1, interrupted: 1, completed: 0 }])
      assert.strictEqual(activations(), 4, "one background runtime was built and closed for the catch up")
    }, VirtualTime.scoped)
  )

  it.effect(
    "is not held back by an attempt that waited for a slot when its space was left",
    Effect.fnUntraced(function*() {
      const { leaveWithPendingWork, open, pulls, replica, space } = yield* evicted({
        maximumActiveSpaces: 2,
        seeded: [loaded[0]],
        held: [loaded[0]]
      })
      const whileTheSlotIsTaken = [yield* space.activation, pulls.waiting]

      yield* leaveWithPendingWork(loaded[1])
      yield* quiet
      const left = yield* within(replica.leave(loaded[1]))
      yield* open(loaded[0])
      yield* quiet

      assert.deepStrictEqual(whileTheSlotIsTaken, ["Inactive", 0])
      assert.strictEqual(describeExit(left), "succeeded")
      assert.deepStrictEqual([yield* space.activation, pulls.waiting], ["Active", 1])
    }, VirtualTime.scoped)
  )
})
