import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  type Constructor,
  constructors,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const home = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f700")
const others = [
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f701"),
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f702"),
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f703"),
  Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000f704")
]
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f700")
const backgroundTurnsAtOnce = 1

const quiet = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

const callerKey = (spaceId: Identity.SpaceId) => ReactivityKey.entity(spaceId, Domain.Todo.name, "caller")

const heldBackgroundTurns = Effect.fnUntraced(function*(constructor: Constructor) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [home, ...others],
    maximumActiveSpaces: 6,
    foregroundActiveSpaces: 1,
    reconciliationConcurrency: backgroundTurnsAtOnce + 1,
    retryDelay: "10 minutes",
    maximumRetryDelay: "10 minutes"
  })
  yield* BackgroundReplica.seedPending(services, others)
  const turns = yield* Queue.unbounded<Identity.SpaceId>()
  const answers = new Map<Identity.SpaceId, Deferred.Deferred<void>>()
  for (const spaceId of others) answers.set(spaceId, yield* Deferred.make<void>())
  const pulls = new Map<Identity.SpaceId, number>()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      const page = emptyPage(services.crypto, request)
      const answer = answers.get(request.spaceId)
      const count = (pulls.get(request.spaceId) ?? 0) + 1
      pulls.set(request.spaceId, count)
      if (answer === undefined || count !== 2) return page
      return Queue.offer(turns, request.spaceId).pipe(
        Effect.andThen(Deferred.await(answer)),
        Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
      )
    }
  }))
  const current = yield* replica.space(home)
  yield* VirtualTime.advanceUntil(current.activate)
  const online = yield* eventually(services, current, (status) => status._tag === "Online")
  assert.isTrue(Option.isSome(online), "the space the user is on came online")
  const inFlight = yield* VirtualTime.advanceUntil(Queue.take(turns))
  const endTurn = (spaceId: Identity.SpaceId) =>
    Effect.suspend(() => {
      const answer = answers.get(spaceId)
      if (answer === undefined) return Effect.void
      return Deferred.succeed(answer, undefined).pipe(Effect.asVoid)
    })
  return { services, replica, current, inFlight, endTurn }
})

describe("a caller operation on a space whose background turn is in flight", () => {
  it.effect.each(constructors)(
    "leaves the space the user returned to in the foreground after a read that ended before the turn with %s",
    Effect.fnUntraced(function*(constructor) {
      const { current, endTurn, inFlight, replica } = yield* heldBackgroundTurns(constructor)
      const passing = yield* replica.space(inFlight)

      const glance = yield* within(passing.get(Domain.Todo, "pending"))
      const returned = yield* within(current.get(Domain.Todo, "here"))
      yield* endTurn(inFlight)
      yield* quiet

      assert.strictEqual(describeExit(glance), "succeeded")
      assert.strictEqual(describeExit(returned), "succeeded")
      assert.strictEqual(yield* current.activation, "Active", "the space the user is on kept its place")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "moves the space to the foreground when the operation outlasts the turn with %s",
    Effect.fnUntraced(function*(constructor) {
      const { current, endTurn, inFlight, replica, services } = yield* heldBackgroundTurns(constructor)
      const used = yield* replica.space(inFlight)
      const delivery = yield* services.holdInvalidationsOf([callerKey(inFlight)])
      const writing = yield* used.mutate(Domain.PutTodo, Domain.todo("caller")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* VirtualTime.advanceUntil(
        Effect.suspend(() => {
          if (delivery.entered() === 1) return Effect.void
          return Effect.never
        }).pipe(Effect.timeoutOption("1 minute"))
      )
      const heldWhileTurnRan = delivery.entered()

      yield* endTurn(inFlight)
      yield* quiet
      const whileHeld = [yield* current.activation, yield* used.activation]
      yield* delivery.release
      const written = yield* VirtualTime.advanceUntil(Fiber.join(writing).pipe(Effect.timeoutOption("1 minute")))
      yield* quiet

      assert.strictEqual(heldWhileTurnRan, 1, "the write ran on the runtime of the background turn")
      assert.deepStrictEqual(whileHeld, ["Active", "Active"])
      assert.strictEqual(describeExit(written), "succeeded")
      assert.deepStrictEqual(
        [yield* current.activation, yield* used.activation],
        ["Inactive", "Active"],
        "the space still in use when its turn ended took the foreground place"
      )
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "keeps the spaces in use by callers at once within the foreground places plus the background turns with %s",
    Effect.fnUntraced(function*(constructor) {
      const { endTurn, inFlight, replica, services } = yield* heldBackgroundTurns(constructor)
      const everySpace = [home, inFlight, ...others.filter((spaceId) => spaceId !== inFlight)]
      const delivery = yield* services.holdInvalidationsOf(everySpace.map(callerKey))
      const writes = yield* Effect.forEach(everySpace, (spaceId) =>
        replica.space(spaceId).pipe(
          Effect.flatMap((space) => space.mutate(Domain.PutTodo, Domain.todo("caller"))),
          Effect.exit,
          Effect.forkChild({ startImmediately: true })
        ))
      yield* quiet
      const inUseWhileTurnRan = delivery.entered()

      yield* endTurn(inFlight)
      yield* quiet
      const inUseAfterTurn = delivery.entered()
      yield* delivery.release
      for (const spaceId of others) yield* endTurn(spaceId)
      const written = yield* VirtualTime.advanceUntil(Fiber.joinAll(writes).pipe(Effect.timeoutOption("10 minutes")))

      assert.strictEqual(inUseWhileTurnRan, 1 + backgroundTurnsAtOnce)
      assert.strictEqual(inUseAfterTurn, 1 + backgroundTurnsAtOnce, "the ended turn did not free a place for a third")
      assert.isTrue(Option.isSome(written), "every write completed once the held ones were released")
    }, VirtualTime.scoped)
  )
})
