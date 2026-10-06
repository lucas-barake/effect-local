import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  describeExit,
  emptyPage,
  eventually,
  idleRemote,
  installView,
  isOnlineDrained,
  within
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const parked = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa01")
const other = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000fa02")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000fa01")

const quiet = VirtualTime.quiet("1 minute")

const parkedAttempt = Effect.fnUntraced(function*() {
  const services = yield* BackgroundReplica.services({
    constructor: "layerWorkflow",
    clientId,
    initialSpaces: [parked, other],
    maximumActiveSpaces: 3,
    foregroundActiveSpaces: 1,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const calling = yield* Deferred.make<void>()
  let holding = true
  let pulls = 0
  const replicaScope = yield* Scope.make()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      const page = emptyPage(services.crypto, request)
      if (request.spaceId !== parked) return page
      pulls += 1
      if (!holding) return page
      return Deferred.succeed(calling, undefined).pipe(Effect.andThen(Effect.never))
    }
  })).pipe(Scope.provide(replicaScope))
  yield* installView(services)
  const space = yield* replica.space(parked)
  yield* VirtualTime.advanceUntil(space.activate)
  yield* VirtualTime.advanceUntil(Deferred.await(calling))
  const deactivated = yield* within(space.deactivate)
  yield* quiet
  assert.strictEqual(describeExit(deactivated), "succeeded")
  assert.strictEqual(yield* space.activation, "Inactive")
  assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 1, "the retired execution waits for a runtime")
  return {
    services,
    replica,
    replicaScope,
    pulls: () => pulls,
    answer: Effect.sync(() => {
      holding = false
    })
  }
})

describe("a workflow attempt that waits for its space to be activated again", () => {
  it.effect(
    "ends when the space is left and does not act for a later membership of the same space",
    Effect.fnUntraced(function*() {
      const { answer, pulls, replica, services } = yield* parkedAttempt()

      const left = yield* within(replica.leave(parked))
      yield* quiet
      const runningAfterLeave = yield* services.runningWorkflowExecutions(parked)
      const startedBeforeRejoin = services.workflowExecutions(parked)
      const pullsAfterLeave = pulls()
      const elsewhere = yield* replica.space(other)
      yield* elsewhere.mutate(Domain.PutTodo, Domain.todo("signal")).pipe(VirtualTime.advanceUntil)
      yield* quiet
      const pullsAfterSignal = pulls()
      yield* answer
      const rejoined = yield* replica.join(parked).pipe(VirtualTime.advanceUntil)
      yield* installView(services)
      yield* rejoined.mutate(Domain.PutTodo, Domain.todo("again")).pipe(VirtualTime.advanceUntil)
      const synced = yield* eventually(services, rejoined, isOnlineDrained)
      yield* quiet

      assert.strictEqual(describeExit(left), "succeeded")
      assert.strictEqual(runningAfterLeave, 0, "executions of the space that was left")
      assert.strictEqual(pullsAfterSignal, pullsAfterLeave, "pulls for the removed space after a capacity change")
      assert.isTrue(Option.isSome(synced), "the new membership synced")
      assert.isAbove(services.workflowExecutions(parked), startedBeforeRejoin, "the new membership started its own")
      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0)
    }, VirtualTime.scoped)
  )

  it.effect(
    "ends when the replica is closed",
    Effect.fnUntraced(function*() {
      const { replicaScope, services } = yield* parkedAttempt()

      yield* Scope.close(replicaScope, Exit.void).pipe(VirtualTime.advanceUntil)
      yield* quiet

      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0)
    }, VirtualTime.scoped)
  )

  it.effect(
    "finishes and records its completion when the space is activated again",
    Effect.fnUntraced(function*() {
      const { answer, replica, services } = yield* parkedAttempt()
      const space = yield* replica.space(parked)

      yield* answer
      yield* VirtualTime.advanceUntil(space.activate)
      const online = yield* eventually(services, space, isOnlineDrained)
      yield* quiet
      const generations = yield* services.sql<{ readonly completed: number; readonly requested: number }>`
        SELECT completed_generation AS completed, requested_generation AS requested
        FROM effect_local_client_spaces WHERE space_id = ${parked}`

      assert.isTrue(Option.isSome(online))
      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0, "no execution is left running")
      assert.strictEqual(generations[0].completed, generations[0].requested, "the requested generation is completed")
    }, VirtualTime.scoped)
  )
})

describe("a leave while the workflow engine cannot be polled", () => {
  it.effect(
    "fails with the defect, keeps the space, and finishes once the engine answers for a waiting attempt",
    Effect.fnUntraced(function*() {
      const { replica, services } = yield* parkedAttempt()

      services.setWorkflowStorageDown(true)
      const failed = yield* within(replica.leave(parked))
      yield* quiet
      const runningAfterFailure = yield* services.runningWorkflowExecutions(parked)
      const stillJoined = yield* Effect.exit(replica.space(parked))
      services.setWorkflowStorageDown(false)
      const left = yield* within(replica.leave(parked))
      yield* quiet

      assert.strictEqual(describeExit(failed), "died", "the polling defect reached the caller")
      assert.isTrue(Exit.isSuccess(stillJoined), "the space was not removed")
      assert.strictEqual(runningAfterFailure, 1, "the execution could not be cancelled yet")
      assert.strictEqual(describeExit(left), "succeeded")
      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0, "the retry cancelled the execution")
    }, VirtualTime.scoped)
  )

  it.effect(
    "fails with the defect and finishes once the engine answers for a foreground scheduler",
    Effect.fnUntraced(function*() {
      const services = yield* BackgroundReplica.services({
        constructor: "layerWorkflow",
        clientId,
        initialSpaces: [parked, other],
        maximumActiveSpaces: 3,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      const calling = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== parked) return page
          return Deferred.succeed(calling, undefined).pipe(Effect.andThen(Effect.never))
        }
      }))
      yield* installView(services)
      const space = yield* replica.space(parked)
      yield* VirtualTime.advanceUntil(space.activate)
      yield* VirtualTime.advanceUntil(Deferred.await(calling))

      services.setWorkflowStorageDown(true)
      const failed = yield* within(replica.leave(parked))
      yield* quiet
      const runningAfterFailure = yield* services.runningWorkflowExecutions(parked)
      services.setWorkflowStorageDown(false)
      const left = yield* within(replica.leave(parked))
      yield* quiet

      assert.strictEqual(describeExit(failed), "died", "the polling defect reached the caller")
      assert.strictEqual(runningAfterFailure, 1, "the execution could not be cancelled yet")
      assert.strictEqual(describeExit(left), "succeeded")
      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0, "the retry cancelled the execution")
    }, VirtualTime.scoped)
  )
})

const budgets = [2048, 200, 97, 64, 63, 48, 31, 17].map((budget) => ({ budget }))

describe("a workflow whose sync drained its space in the background", () => {
  it.effect.each(budgets)(
    "records its completion and ends without another activation at a budget of $budget",
    Effect.fnUntraced(function*(_row) {
      const services = yield* BackgroundReplica.services({
        constructor: "layerWorkflow",
        clientId,
        initialSpaces: [parked, other],
        maximumActiveSpaces: 3,
        foregroundActiveSpaces: 1,
        retryDelay: "1 second",
        maximumRetryDelay: "1 minute"
      })
      const calling = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          const page = emptyPage(services.crypto, request)
          if (request.spaceId !== parked) return page
          return Deferred.succeed(calling, undefined).pipe(
            Effect.andThen(Deferred.await(answered)),
            Effect.andThen(page)
          )
        }
      }))
      yield* installView(services)
      const evicted = yield* replica.space(parked)
      const current = yield* replica.space(other)
      yield* evicted.mutate(Domain.PutTodo, Domain.todo("held")).pipe(VirtualTime.advanceUntil)
      yield* VirtualTime.advanceUntil(Deferred.await(calling))
      yield* current.mutate(Domain.PutTodo, Domain.todo("after")).pipe(VirtualTime.advanceUntil)

      yield* Deferred.succeed(answered, undefined)
      const drained = yield* eventually(services, evicted, (status) => status.pending === 0)
      yield* VirtualTime.quiet("10 minutes")
      const generations = yield* services.sql<{ readonly completed: number }>`
        SELECT completed_generation AS completed FROM effect_local_client_spaces WHERE space_id = ${parked}`

      assert.isTrue(Option.isSome(drained))
      assert.strictEqual(yield* evicted.activation, "Inactive", "the space was not activated again")
      assert.strictEqual(yield* services.runningWorkflowExecutions(parked), 0, "no execution is left running")
      assert.isAbove(generations[0].completed, 0, "the generation the workflow synced is recorded as completed")
    }, VirtualTime.atBudget)
  )
})
