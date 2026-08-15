/* oxlint-disable effect/noAsyncFunction, effect/noGlobals, effect/noNodeBuiltinImport, effect/noTryCatch, effect-local/noManualEffectBoundary -- Vitest owns the benchmark boundary and filesystem fixture. */

import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { setFlagsFromString } from "node:v8"
import { runInNewContext } from "node:vm"
import { assert, bench, describe } from "vitest"
import * as Codec from "../src/internal/codec.js"
import * as Migrations from "../src/Migrations.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "../test/Domain.js"

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const defaultScope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const scales = [1, 64, 256, 1_000] as const
type Scale = typeof scales[number]
const eagerBaselineAtE999e1c = {
  1: { fibers: 11, watches: 1, heapBytes: 290_472, startupMillis: 24.17 },
  64: { fibers: 144, watches: 64, heapBytes: 3_183_800, startupMillis: 371.21 },
  256: { fibers: 528, watches: 256, heapBytes: 10_100_400, startupMillis: 1_031.43 },
  1_000: { fibers: 2_016, watches: 1_000, heapBytes: 36_418_480, startupMillis: 4_090.48 }
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

setFlagsFromString("--expose_gc")
const exposedGc: NonNullable<typeof globalThis.gc> = runInNewContext("gc")
const collectGarbage = Effect.promise(() => exposedGc({ execution: "async" }))

const layerReplica = (onWatchCount: (change: number) => void, layerServices: Layer.Layer<any>) => {
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
  const options: SqlReplica.Options<typeof Domain.definition> = {
    definition: Domain.definition,
    clientId,
    defaultScope,
    maximumActiveSpaces: 8,
    foregroundActiveSpaces: 4,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }
  return SqlReplica.layer(options).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
    Layer.provide(layerServices)
  )
}

const open = (layerServices: Layer.Layer<any>) => {
  let watches = 0
  const layer = layerReplica((change) => {
    watches += change
  }, layerServices)
  return { layer, watches: () => watches }
}

interface Measurement {
  readonly fibers: number
  readonly heapBytes: number
  readonly startupMillis: number
  readonly watches: number
}

const median = (values: ReadonlyArray<number>): number => values.toSorted((left, right) => left - right)[1]

const measure = async (spaceCount: Scale): Promise<Measurement> => {
  const temporaryRoot = tmpdir()
  const directory = mkdtempSync(join(temporaryRoot, "effect-local-scale-"))
  const filename = join(directory, "replica.sqlite")
  try {
    const samples = await Effect.runPromise(
      Effect.gen(function*() {
        const databaseScope = yield* Scope.make()
        const databaseContext = yield* Layer.mergeAll(
          SqliteClient.layer({ filename, disableWAL: true }),
          NodeCrypto.layer,
          Reactivity.layer
        ).pipe(Layer.buildWithScope(databaseScope))
        const sql = Context.get(databaseContext, SqlClient.SqlClient)
        const crypto = Context.get(databaseContext, Crypto.Crypto)
        const reactivity = Context.get(databaseContext, Reactivity.Reactivity)
        const layerServices = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient, sql),
          Layer.succeed(Crypto.Crypto, crypto),
          Layer.succeed(Reactivity.Reactivity, reactivity)
        )
        yield* Migrations.client({
          definition: Domain.definition,
          clientId,
          migration: { retryDelay: "1 millis", maximumAttempts: 8 }
        }).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        const scopeJson = yield* Codec.stringify(defaultScope)
        const scopeDigest = yield* Protocol.replicationScopeDigest(defaultScope).pipe(
          Effect.provideService(Crypto.Crypto, crypto)
        )
        const seededSpaces = spaces(spaceCount)
        yield* sql.withTransaction(
          Effect.forEach(seededSpaces, (spaceId) =>
            sql`INSERT INTO effect_local_client_spaces
          (space_id, membership_incarnation, definition_hash, schema_version, schema_hash, schema_generation,
            active_schema_generation, active_projection_generation, projection_schema_generation,
            next_local_sequence, server_cursor, visible_revision, requested_generation, completed_generation,
            installed_snapshot_sequence, installed_snapshot_terminal_sequence, desired_scope_json,
            desired_scope_digest, scope_generation)
          VALUES (${spaceId},
            ('inc_' || lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
              substr(lower(hex(randomblob(2))), 2) || '-' ||
              substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' ||
              lower(hex(randomblob(6)))), ${Domain.definition.hash},
            ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash}, 0, 0, 0, 0,
            1, 0, 0, 1, 0, 0, 0, ${scopeJson}, ${scopeDigest}, 1)`, { discard: true })
        )

        const observations: Array<Measurement> = []
        for (let index = 0; index < 3; index += 1) {
          yield* collectGarbage
          const beforeHeap = process.memoryUsage().heapUsed
          const startedAt = performance.now()
          const environment = open(layerServices)
          const replicaScope = yield* Scope.make()
          const context = yield* Layer.buildWithScope(environment.layer, replicaScope)
          const replica = Context.get(context, Replica.Replica)
          assert.lengthOf(yield* replica.spaces, spaceCount)
          yield* collectGarbage
          const snapshots = yield* Metric.snapshot
          observations.push({
            fibers: activeFibers(snapshots),
            heapBytes: process.memoryUsage().heapUsed - beforeHeap,
            startupMillis: performance.now() - startedAt,
            watches: environment.watches()
          })
          yield* Scope.close(replicaScope, Exit.void)
        }
        yield* Scope.close(databaseScope, Exit.void)
        return observations
      }).pipe(
        Metric.enableRuntimeMetrics,
        Effect.provideService(Metric.MetricRegistry, new Map())
      )
    )
    assert.isTrue(samples.every((sample) => sample.watches === 0))
    return {
      fibers: median(samples.map((sample) => sample.fibers)),
      heapBytes: median(samples.map((sample) => sample.heapBytes)),
      startupMillis: median(samples.map((sample) => sample.startupMillis)),
      watches: median(samples.map((sample) => sample.watches))
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
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
