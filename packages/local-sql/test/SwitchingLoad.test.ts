import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import { acceptSubmission, type Constructor, emptyPage, idleRemote } from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000f401")

const spaceIds = Array.from({ length: 20 }, (_, index) => {
  const suffix = String(index + 1).padStart(4, "0")
  return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-0000f401${suffix}`)
})

const quick = [10, 10, 10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
const queued = [10, 20, 20, 30, 30, 40, 50, 50, 60, 60, 70, 80, 80, 90, 90, 100, 110, 110, 120, 120]

interface Row {
  readonly constructor: Constructor
  readonly concurrency: number
  readonly switching: boolean
  readonly visitedOnMain: number
  readonly sortedOnMain: ReadonlyArray<number>
}

const rows: ReadonlyArray<Row> = [
  { constructor: "layer", concurrency: 8, switching: false, visitedOnMain: 10, sortedOnMain: quick },
  { constructor: "layer", concurrency: 8, switching: true, visitedOnMain: 20, sortedOnMain: quick },
  { constructor: "layer", concurrency: 2, switching: false, visitedOnMain: 20, sortedOnMain: queued },
  {
    constructor: "layer",
    concurrency: 2,
    switching: true,
    visitedOnMain: 130,
    sortedOnMain: [10, 20, 20, 30, 30, 40, 50, 50, 60, 60, 70, 80, 80, 90, 90, 100, 110, 110, 120, 130]
  },
  { constructor: "layerWorkflow", concurrency: 8, switching: false, visitedOnMain: 10, sortedOnMain: quick },
  { constructor: "layerWorkflow", concurrency: 8, switching: true, visitedOnMain: 10, sortedOnMain: quick },
  { constructor: "layerWorkflow", concurrency: 2, switching: false, visitedOnMain: 20, sortedOnMain: queued },
  {
    constructor: "layerWorkflow",
    concurrency: 2,
    switching: true,
    visitedOnMain: 20,
    sortedOnMain: [10, 20, 20, 30, 30, 40, 40, 50, 60, 60, 70, 70, 80, 90, 90, 100, 100, 110, 120, 120]
  }
]

const tenSeconds = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("10 seconds"))

const loaded = Effect.fnUntraced(function*(row: Row) {
  const services = yield* BackgroundReplica.services({
    constructor: row.constructor,
    clientId,
    initialSpaces: spaceIds,
    maximumActiveSpaces: spaceIds.length + 1,
    foregroundActiveSpaces: 1,
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  })
  yield* BackgroundReplica.seedPending(services, spaceIds)
  const digests = new Map<string, Set<string>>()
  const slow = Effect.sleep("2 seconds")
  const remote = SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: (request) => {
      for (const envelope of request.envelopes) {
        const seen = digests.get(envelope.mutationId) ?? new Set<string>()
        seen.add(envelope.digest)
        digests.set(envelope.mutationId, seen)
      }
      return Effect.andThen(slow, acceptSubmission(request))
    },
    pull: (request) => Effect.andThen(slow, emptyPage(services.crypto, request))
  })
  const options = {
    definition: Domain.definition,
    clientId,
    initialSpaces: spaceIds,
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    maximumActiveSpaces: spaceIds.length + 1,
    foregroundActiveSpaces: 1,
    reconciliationConcurrency: row.concurrency,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute"
  } as const
  const engine = yield* Layer.build(WorkflowEngine.layerMemory)
  const layerServices = Layer.mergeAll(
    Domain.layerHandlers,
    Layer.succeed(SyncEngine.SyncEngine, remote),
    Layer.succeed(SqlClient.SqlClient, services.sql),
    Layer.succeed(Reactivity.Reactivity, services.reactivity),
    NodeCrypto.layer,
    Layer.succeedContext(engine)
  )
  let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
  if (row.constructor === "layerWorkflow") {
    layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
  }
  const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
  const spaces = yield* Effect.forEach(spaceIds, (spaceId) => replica.space(spaceId))
  return { spaces, digests }
})

describe("twenty spaces with pending work and a server that answers in two seconds", () => {
  it.effect.each(rows)(
    "drain no later than on main ($constructor, concurrency $concurrency, switching $switching)",
    Effect.fnUntraced(function*(row) {
      const { digests, spaces } = yield* loaded(row)
      const visit = (space: Replica.Space) =>
        space.get(Domain.Todo, "pending").pipe(Effect.andThen(Effect.sleep("500 millis")))
      if (row.switching) {
        const visits = Effect.andThen(visit(spaces[0]), visit(spaces[1]))
        yield* Effect.forkChild(Effect.forever(visits), { startImmediately: true })
      }
      const drainedAt = spaceIds.map(() => 600)
      for (let seconds = 10; seconds <= 150; seconds += 10) {
        yield* tenSeconds
        for (const [index, space] of spaces.entries()) {
          if (drainedAt[index] < 600) continue
          if ((yield* space.status).pending === 0) drainedAt[index] = seconds
        }
      }
      const sorted = drainedAt.toSorted((left, right) => left - right)
      const later = sorted.filter((seconds, index) => seconds > row.sortedOnMain[index])
      const contents = Array.from(digests.values(), (seen) => seen.size)

      assert.deepStrictEqual(later, [], `drained at ${drainedAt.join(",")}, on main ${row.sortedOnMain.join(",")}`)
      assert.isAtMost(Math.max(drainedAt[0], drainedAt[1]), row.visitedOnMain, "the two visited spaces")
      assert.strictEqual(Math.max(...contents), 1, "every mutation was submitted with one content")
    }, VirtualTime.scoped),
    120_000
  )
})
