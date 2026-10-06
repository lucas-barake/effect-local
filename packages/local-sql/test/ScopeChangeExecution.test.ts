import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
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

const first = Identity.SpaceId.make("spc_00000000-0000-4000-8000-00000000e201")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000e201")

const scopeRows = [
  { constructor: "layer", delay: "0 millis", budget: 31 },
  { constructor: "layer", delay: "350 millis", budget: 31 },
  { constructor: "layerWorkflow", delay: "0 millis", budget: 64 },
  { constructor: "layerWorkflow", delay: "350 millis", budget: 500 },
  { constructor: "layerWorkflow", delay: "650 millis", budget: 31 }
] as const
const rejoinRows = [
  { constructor: "layerWorkflow", delay: "0 millis", budget: 2048 },
  { constructor: "layerWorkflow", delay: "350 millis", budget: 31 }
] as const
type Row = typeof scopeRows[number] | typeof rejoinRows[number]

const syncing = Effect.fnUntraced(function*(row: Row) {
  const services = yield* BackgroundReplica.services({
    constructor: row.constructor,
    clientId,
    initialSpaces: [first],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  const generations: Array<number> = []
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => Effect.andThen(Effect.sleep("300 millis"), acceptSubmission(request)),
    pull: (request) => {
      generations.push(request.scopeGeneration)
      return Effect.andThen(Effect.sleep("300 millis"), emptyPage(services.crypto, request))
    }
  }))
  yield* installView(services)
  const space = yield* replica.space(first)
  yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
  yield* VirtualTime.advanceUntil(Effect.sleep(row.delay))
  return { services, replica, space, generations }
})

describe("a change of what a space replicates while a reconciliation is in flight", () => {
  it.effect.each(scopeRows)(
    "ends reconciled for a new scope set $delay into a sync at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { generations, services, space } = yield* syncing(row)
      const wide = Protocol.ReplicationScope.make({ models: [Domain.Todo.name, Domain.Message.name] })

      const changed = yield* within(space.setScope(wide))
      const settled = yield* eventually(services, space, isOnlineDrained)
      yield* VirtualTime.quiet("10 minutes")
      const status = yield* space.status

      assert.strictEqual(describeExit(changed), "succeeded")
      assert.isTrue(Option.isSome(settled), "the space came online for the new scope")
      assert.strictEqual(status._tag, "Online")
      assert.strictEqual(generations.at(-1), Math.max(...generations), "the last pull asked for the newest scope")
    }, VirtualTime.atBudget)
  )

  it.effect.each(rejoinRows)(
    "ends reconciled after a leave and a join $delay into a sync at a budget of $budget with $constructor",
    Effect.fnUntraced(function*(row) {
      const { replica, services } = yield* syncing(row)

      const left = yield* within(replica.leave(first))
      const rejoined = yield* VirtualTime.advanceUntil(replica.join(first))
      yield* installView(services)
      yield* VirtualTime.advanceUntil(rejoined.activate)
      const settled = yield* eventually(services, rejoined, (status) => status._tag === "Online")
      yield* VirtualTime.quiet("10 minutes")

      assert.strictEqual(describeExit(left), "succeeded")
      assert.isTrue(Option.isSome(settled), "the joined space came online")
      assert.strictEqual((yield* rejoined.status)._tag, "Online")
    }, VirtualTime.atBudget)
  )
})
