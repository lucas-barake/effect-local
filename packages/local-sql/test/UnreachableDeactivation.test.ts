import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
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

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f91")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f92")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f91")

const budgets = [2048, 200, 100, 64, 31] as const

const rows = constructors.flatMap((constructor) => budgets.map((budget) => ({ constructor, budget })))

const atBudget = <A, E extends { readonly _tag: string }, R,>(
  effect: Effect.Effect<A, E, R>,
  row: { readonly budget: number }
) => VirtualTime.scoped(effect).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, row.budget))

const unreachableServer = Effect.fnUntraced(function*(
  row: typeof rows[number],
  foregroundActiveSpaces: number
) {
  const services = yield* BackgroundReplica.services({
    constructor: row.constructor,
    clientId,
    initialSpaces: [spaceId, otherSpaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const reachable = yield* Deferred.make<void>()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => Effect.andThen(Deferred.await(reachable), acceptSubmission(request)),
    pull: (request) => Effect.andThen(Deferred.await(reachable), emptyPage(services.crypto, request))
  }))
  yield* installView(services)
  return {
    services,
    space: yield* replica.space(spaceId),
    other: yield* replica.space(otherSpaceId),
    serverReturns: Deferred.succeed(reachable, undefined)
  }
})

describe("a foreground space whose server is unreachable", () => {
  it.effect.each(rows)(
    "deactivates after a mutation and drains it in the background at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { serverReturns, services, space } = yield* unreachableServer(row, 2)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))

      const deactivated = yield* within(space.deactivate)

      assert.strictEqual(describeExit(deactivated), "succeeded")
      yield* serverReturns
      const drained = yield* eventually(services, space, (status) => status.pending === 0)
      assert.isTrue(Option.isSome(drained), "the pending mutation was drained once the server returned")
    }, atBudget)
  )

  it.effect.each(rows)(
    "gives its place to another space after a mutation at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { other, serverReturns, services, space } = yield* unreachableServer(row, 1)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))

      const mutated = yield* other.mutate(Domain.PutTodo, Domain.todo("other")).pipe(within)

      assert.strictEqual(describeExit(mutated), "succeeded")
      yield* serverReturns
      const drained = yield* eventually(services, space, (status) => status.pending === 0)
      assert.isTrue(Option.isSome(drained), "the evicted space was drained once the server returned")
    }, atBudget)
  )
})

describe("a background server call that fails after the foreground took its space over", () => {
  it.effect.each(constructors)(
    "is not answered to the foreground sync with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* BackgroundReplica.services({
        constructor,
        clientId,
        initialSpaces: [spaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      yield* BackgroundReplica.seedPending(services, [spaceId])
      const held = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls > 1) return emptyPage(services.crypto, request)
          return Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(Effect.fail(new ReplicaError.ProtocolInvalid({ message: "rejected in the background" })))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* VirtualTime.advanceUntil(Deferred.await(held))
      const admission = yield* services.holdStatement("SET requested_generation", true)
      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* VirtualTime.advanceUntil(admission.entered)

      yield* Deferred.succeed(answered, undefined)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 millis"))
      yield* admission.release
      yield* VirtualTime.advanceUntil(Fiber.join(activation))
      const online = yield* eventually(services, space, (status) => status._tag === "Online" && status.pending === 0)

      assert.isTrue(Option.isSome(online), "the foreground sync ran against the server and drained the space")
    }, VirtualTime.scoped)
  )
})
