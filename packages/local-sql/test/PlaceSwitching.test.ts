import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
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
  installView
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000c001")
const second = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000c002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000c001")

const quiet = VirtualTime.quiet("10 minutes")

const visitCounts = [20, 80, 320] as const

const rows = constructors.flatMap((constructor) => visitCounts.map((visits) => ({ constructor, visits })))

const switching = Effect.fnUntraced(function*(
  constructor: Constructor,
  visits: number,
  outage: boolean,
  stay: Duration.Input = "100 millis"
) {
  const services = yield* BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [first, second],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 1,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const reachable = yield* Deferred.make<void>()
  let answer: Effect.Effect<void> = Effect.sleep("300 millis")
  if (outage) answer = Deferred.await(reachable)
  let pulls = 0
  const answeredForFirst: Array<"pull" | "submit" | "interrupted"> = []
  const logged = <A, E extends { readonly _tag: string },>(
    spaceId: Identity.SpaceId,
    call: "pull" | "submit",
    answered: Effect.Effect<A, E>
  ) => {
    if (spaceId !== first) return answered
    return answered.pipe(
      Effect.tap(() => Effect.sync(() => answeredForFirst.push(call))),
      Effect.onInterrupt(() => Effect.sync(() => answeredForFirst.push("interrupted")))
    )
  }
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => {
      const accepted = Effect.andThen(answer, acceptSubmission(request))
      return logged(request.envelopes[0].spaceId, "submit", accepted)
    },
    pull: (request) => {
      if (request.spaceId === first) pulls += 1
      const page = Effect.andThen(answer, emptyPage(services.crypto, request))
      return logged(request.spaceId, "pull", page)
    }
  }))
  yield* installView(services)
  const a = yield* replica.space(first)
  const b = yield* replica.space(second)
  yield* a.mutate(Domain.PutTodo, Domain.todo("pending"))
  const pause = Effect.sleep(stay)
  const visit = b.get(Domain.Todo, "other").pipe(
    Effect.andThen(pause),
    Effect.andThen(a.get(Domain.Todo, "pending")),
    Effect.andThen(pause)
  )
  const visited = yield* Effect.forEach(Array.from({ length: visits }), () => visit, { discard: true }).pipe(
    Effect.exit,
    Effect.timeoutOption("30 minutes"),
    VirtualTime.advanceUntil
  )
  const pendingAtStop = (yield* a.status).pending
  const wholePassesAtStop = answeredForFirst.filter((call, index) =>
    call === "pull" && answeredForFirst[index + 1] === "submit" && answeredForFirst[index + 2] === "pull"
  ).length
  const pullsAtStop = pulls
  const executionsAtStop = services.workflowExecutions(first)
  yield* Deferred.succeed(reachable, undefined)
  const drained = yield* eventually(services, a, (status) =>
    status.pending === 0)
  yield* quiet
  return {
    visited: describeExit(visited),
    drained: Option.isSome(drained),
    pendingAtStop,
    wholePassesAtStop,
    executionsAtStop,
    pullsAfter: pulls - pullsAtStop,
    executionsAfter: services.workflowExecutions(first) - executionsAtStop
  }
})

describe("a user who keeps moving between two spaces that share one foreground place", () => {
  it.effect.each(constructors)(
    "drains the pending mutation while the visits continue and the server answers in 300 millis with %s",
    Effect.fnUntraced(function*(constructor) {
      const result = yield* switching(constructor, 60, false)

      assert.strictEqual(result.visited, "succeeded")
      assert.strictEqual(result.pendingAtStop, 0)
    }, VirtualTime.scoped),
    120_000
  )

  it.effect.each(constructors)(
    "finishes a whole sync while the visits continue when each stay outlasts one server call with %s",
    Effect.fnUntraced(function*(constructor) {
      const result = yield* switching(constructor, 60, false, "400 millis")

      assert.strictEqual(result.visited, "succeeded")
      assert.strictEqual(result.pendingAtStop, 0)
      assert.isAbove(result.wholePassesAtStop, 0, "a pass ended on a pull it had requested itself")
    }, VirtualTime.scoped),
    120_000
  )

  it.effect.each(rows)(
    "leaves a bounded amount of server work behind after $visits visits with $constructor",
    Effect.fnUntraced(function*(row) {
      const result = yield* switching(row.constructor, row.visits, false)

      assert.strictEqual(result.visited, "succeeded")
      assert.isTrue(result.drained, "the mutation is drained once the visits stop")
      assert.isAtMost(result.pullsAfter, 4, "pulls after the visits stopped")
      assert.isAtMost(result.executionsAfter, 1, "executions started after the visits stopped")
    }, VirtualTime.scoped),
    120_000
  )

  it.effect.each(rows)(
    "keeps one execution and a bounded amount of server work through an outage of $visits visits with $constructor",
    Effect.fnUntraced(function*(row) {
      const result = yield* switching(row.constructor, row.visits, true)

      assert.strictEqual(result.visited, "succeeded")
      assert.isTrue(result.drained, "the mutation is drained once the server returns")
      assert.isAtMost(result.executionsAtStop, 1, "executions started during the outage")
      assert.isAtMost(result.pullsAfter, 6, "pulls after the server returned")
      assert.isAtMost(result.executionsAfter, 1, "executions started after the server returned")
    }, VirtualTime.scoped),
    120_000
  )
})
