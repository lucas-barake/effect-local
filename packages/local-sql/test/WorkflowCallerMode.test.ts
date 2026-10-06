import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import { acceptSubmission, constructors, emptyPage, idleRemote, viewId } from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f71")
const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f72")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000f73")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000f71")

const settle = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 second"))

const entries = ["activate", "mutate", "setScope", "join"] as const

const rows = constructors.flatMap((constructor) => entries.map((entry) => ({ constructor, entry })))

describe("subscribers after a batch of the caller ended", () => {
  it.effect.each(rows)(
    "still learn of every activation and aggregate change when the batch was around $entry with $constructor",
    Effect.fnUntraced(function*(row) {
      const services = yield* BackgroundReplica.services({
        constructor: row.constructor,
        clientId,
        initialSpaces: [spaceId, otherSpaceId],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2,
        retryDelay: "1 second",
        maximumRetryDelay: "10 seconds"
      })
      let offline = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => {
          if (offline) return Effect.fail(new ReplicaError.ServerUnavailable())
          return acceptSubmission(request)
        },
        pull: (request) => {
          if (offline) return Effect.fail(new ReplicaError.ServerUnavailable())
          return emptyPage(services.crypto, request)
        }
      }))
      const installView = services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* installView
      const batch = services.reactivity.withBatch
      let space: Replica.Space
      if (row.entry === "join") {
        space = yield* replica.join(thirdSpaceId).pipe(batch, VirtualTime.advanceUntil)
        yield* installView
      } else {
        space = yield* replica.space(spaceId)
      }
      if (row.entry === "activate") yield* space.activate.pipe(batch, VirtualTime.advanceUntil)
      if (row.entry === "mutate") {
        yield* space.mutate(Domain.PutTodo, Domain.todo("batched")).pipe(batch, VirtualTime.advanceUntil)
      }
      if (row.entry === "setScope") {
        yield* space.setScope(Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })).pipe(
          batch,
          VirtualTime.advanceUntil
        )
      }
      yield* settle

      const describeAggregate = (current: ReplicaStatus.Aggregate) => {
        const counts = current.counts
        return `idle ${counts.idle}, connecting ${counts.connecting}, offline ${counts.offline}, online ${counts.online}, pending ${current.totalPending}`
      }
      let activation = yield* space.activation
      let aggregate = describeAggregate(yield* replica.status)
      const told = { activation: false, aggregate: false }
      services.reactivity.registerUnsafe([ReactivityKey.activation(space.spaceId)], () => {
        told.activation = true
      })
      services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        told.aggregate = true
      })
      const stale: Array<string> = []
      const phase = Effect.fnUntraced(function*(label: string, seconds: number) {
        for (let second = 1; second <= seconds; second++) {
          yield* settle
          const actual = yield* space.activation
          const total = describeAggregate(yield* replica.status)
          if (told.activation) activation = actual
          if (told.aggregate) aggregate = total
          told.activation = false
          told.aggregate = false
          if (activation !== actual) {
            stale.push(`${label} +${second}s: activation subscriber saw ${activation}, it is ${actual}`)
          }
          if (aggregate !== total) {
            stale.push(`${label} +${second}s: aggregate subscriber saw (${aggregate}), it is (${total})`)
          }
        }
      })
      yield* phase("online", 5)
      yield* space.activate.pipe(VirtualTime.advanceUntil)
      yield* phase("activated", 5)
      offline = true
      yield* space.mutate(Domain.PutTodo, Domain.todo("first")).pipe(VirtualTime.advanceUntil)
      yield* phase("offline with a pending mutation", 30)
      yield* space.deactivate.pipe(VirtualTime.advanceUntil)
      yield* phase("deactivated while offline", 90)
      offline = false
      yield* phase("healed", 120)
      const status = yield* space.status

      assert.deepStrictEqual(stale.slice(0, 6), [])
      assert.strictEqual(`${status._tag}, pending ${status.pending}`, "Idle, pending 0")
    }, VirtualTime.scoped)
  )
})
