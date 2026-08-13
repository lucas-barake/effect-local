import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { assert, bench, describe } from "vitest"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "../test/Domain.js"

/* oxlint-disable effect/noAsyncFunction, effect-local/noManualEffectBoundary -- Vitest owns the benchmark boundary. */

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const defaultScope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const scales = [1, 64, 256, 1_000] as const
type Scale = typeof scales[number]
const eagerBaselineAtE999e1c = {
  1: { fibers: 11, watches: 1, heapBytes: 4_451_056, startupMillis: 16.19 },
  64: { fibers: 144, watches: 64, heapBytes: 53_314_776, startupMillis: 860.58 },
  256: { fibers: 528, watches: 256, heapBytes: 86_219_424, startupMillis: 2_792.07 },
  1_000: { fibers: 2_016, watches: 1_000, heapBytes: 224_750_552, startupMillis: 7_396.56 }
} as const

const activeFibers = (snapshots: ReadonlyArray<Metric.Metric.Snapshot>): number => {
  const snapshot = snapshots.find((candidate) => candidate.id === "child_fibers_active")
  assert.isDefined(snapshot)
  assert.strictEqual(snapshot.type, "Gauge")
  if (snapshot.type === "Gauge") return Number(snapshot.state.value)
  return 0
}

const spaces = (count: number): ReadonlyArray<Identity.SpaceId> =>
  Array.from(
    { length: count },
    (_, index) => Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`)
  )

const collectGarbage = () => {
  globalThis.gc?.()
  globalThis.gc?.()
}

const layerReplica = (spaceCount: number, onWatchCount: (change: number) => void) => {
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    discard: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    pull: () => Effect.never,
    bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    watch: () =>
      Effect.acquireRelease(
        Effect.sync(() => {
          onWatchCount(1)
          return Stream.never
        }),
        () => Effect.sync(() => onWatchCount(-1))
      ).pipe(Stream.unwrap)
  })
  return SqlReplica.layer({
    definition: Domain.definition,
    clientId,
    initialSpaces: spaces(spaceCount),
    defaultScope,
    maximumActiveSpaces: 8,
    foregroundActiveSpaces: 4,
    retainedReceipts: 256,
    settlementCapacity: 64,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
    Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Reactivity.layer)
  )
}

const open = (spaceCount: number) => {
  let watches = 0
  const registry = new Map<string, Metric.Metric.Metadata<any, any>>()
  const layer = layerReplica(spaceCount, (change) => {
    watches += change
  }).pipe(
    Layer.provideMerge(Metric.enableRuntimeMetricsLayer),
    Layer.provideMerge(Layer.succeed(Metric.MetricRegistry, registry))
  )
  return { layer, watches: () => watches }
}

const measure = async (spaceCount: Scale) => {
  const environment = open(0)
  return Effect.runPromise(
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      assert.strictEqual((yield* replica.spaces).length, 0)
      collectGarbage()
      const beforeHeap = process.memoryUsage().heapUsed
      const startedAt = yield* Clock.currentTimeMillis
      yield* Effect.forEach(spaces(spaceCount), replica.join, { discard: true })
      assert.strictEqual((yield* replica.spaces).length, spaceCount)
      yield* Effect.yieldNow
      collectGarbage()
      const current = {
        fibers: activeFibers(yield* Metric.snapshot),
        heapBytes: process.memoryUsage().heapUsed - beforeHeap,
        startupMillis: (yield* Clock.currentTimeMillis) - startedAt,
        watches: environment.watches()
      }
      assert.strictEqual(current.watches, 0)
      return current
    }).pipe(Effect.provide(environment.layer), Effect.scoped)
  )
}

// oxlint-disable-next-line effect/noGlobals -- The benchmark runner selects one isolated process per scale.
const requestedScale = Number(process.env.EFFECT_LOCAL_BENCH_SPACES ?? "1000")
let selectedScale: Scale
switch (requestedScale) {
  case 1:
  case 64:
  case 256:
  case 1_000:
    selectedScale = requestedScale
    break
  default:
    assert.fail(`EFFECT_LOCAL_BENCH_SPACES must be one of ${scales.join(", ")}`)
}

describe("remembered space runtime scale", () => {
  bench(`${selectedScale} remembered spaces`, async () => {
    const current = await measure(selectedScale)
    const baseline = eagerBaselineAtE999e1c[selectedScale]
    // oxlint-disable-next-line no-console -- Resource measurements are the benchmark result.
    console.table([{
      spaces: selectedScale,
      baselineFibers: baseline.fibers,
      currentFibers: current.fibers,
      baselineWatches: baseline.watches,
      currentWatches: current.watches,
      baselineHeapMiB: baseline.heapBytes / 1024 / 1024,
      currentHeapMiB: current.heapBytes / 1024 / 1024,
      baselineStartupMillis: baseline.startupMillis,
      currentStartupMillis: current.startupMillis
    }])
  }, { iterations: 1, time: 0, warmupIterations: 0, warmupTime: 0, throws: true })
})
