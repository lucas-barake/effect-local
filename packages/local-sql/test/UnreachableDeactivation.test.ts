import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
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
