import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { DatabaseSync } from "node:sqlite"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

/* oxlint-disable typescript/no-this-alias, typescript/unbound-method, typescript/no-unsafe-type-assertion, typescript/no-unnecessary-type-assertion, effect/noAs, effect/noTryCatch -- The driver statement recorder patches the `node:sqlite` prototype so query plans can be checked against the real production wiring. */

interface Execution {
  readonly database: DatabaseSync
  readonly sql: string
  readonly params: ReadonlyArray<unknown>
}

const executions: Array<Execution> = []
let recording = false

const originalPrepare = DatabaseSync.prototype.prepare
DatabaseSync.prototype.prepare = function(this: DatabaseSync, sql: string) {
  const database = this
  const statement = originalPrepare.call(this, sql)
  return new Proxy(statement, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver)
      if (typeof value !== "function") return value
      if (property !== "all" && property !== "run" && property !== "iterate") return value.bind(target)
      return (...params: Array<unknown>) => {
        if (recording) executions.push({ database, sql, params })
        return (value as (...args: Array<unknown>) => unknown).apply(target, params)
      }
    }
  })
} as typeof DatabaseSync.prototype.prepare

const keyedTables =
  /effect_local_client_visible_entities_data|effect_local_client_canonical_entities_data|effect_local_srvidx_[0-9a-f]+/

const rangeScan = /^(SEARCH|SCAN) (?<table>[^ ]+)( USING [^(]*\((?<terms>[^)]*)\))?$/

const keyedNames = (sql: string) => {
  const names = new Set<string>()
  const pattern =
    /"?(effect_local_client_visible_entities_data|effect_local_client_canonical_entities_data|effect_local_srvidx_[0-9a-f]+)"?(\s+AS\s+(\w+))?/g
  for (const match of sql.matchAll(pattern)) {
    names.add(match[1])
    if (match[3] !== undefined) names.add(match[3])
  }
  return names
}

const queryPlans = (predicate: (sql: string) => boolean) => {
  const plans: Array<{ readonly execution: Execution; readonly steps: ReadonlyArray<Record<string, unknown>> }> = []
  const errors: Array<string> = []
  for (const execution of executions) {
    if (!predicate(execution.sql)) continue
    try {
      plans.push({
        execution,
        steps: originalPrepare.call(execution.database, `EXPLAIN QUERY PLAN ${execution.sql}`)
          .all(...execution.params as Array<never>)
      })
    } catch (cause) {
      errors.push(`${String(cause)} :: ${execution.sql.replace(/\s+/g, " ").slice(0, 160)}`)
    }
  }
  return { errors, plans }
}

const unindexedKeyLookups = () => {
  const offenders: Array<string> = []
  const inspected = queryPlans((sql) => keyedTables.test(sql) && /entity_key\s*(=|IN)/.test(sql))
  for (const { execution, steps } of inspected.plans) {
    const names = keyedNames(execution.sql)
    for (const step of steps) {
      if (typeof step.detail !== "string") continue
      const matched = rangeScan.exec(step.detail)
      if (matched?.groups === undefined) continue
      if (!names.has(matched.groups.table)) continue
      const terms = matched.groups.terms ?? ""
      if (terms.includes("entity_key")) continue
      offenders.push(`${step.detail} :: ${execution.sql.replace(/\s+/g, " ").slice(0, 160)}`)
    }
  }
  return { errors: inspected.errors, inspected: inspected.plans.length, offenders }
}

const windowSortPlans = () => {
  const inspected = queryPlans((sql) => sql.includes("ROW_NUMBER() OVER"))
  const temporarySorts = inspected.plans.flatMap(({ execution, steps }) =>
    steps.flatMap((step) => {
      if (typeof step.detail !== "string" || !step.detail.includes("USE TEMP B-TREE")) return []
      return [`${step.detail} :: ${execution.sql.replace(/\s+/g, " ").slice(0, 160)}`]
    })
  )
  return { errors: inspected.errors, inspected: inspected.plans.length, temporarySorts }
}

const serverIndexExecutions = () =>
  executions.filter((execution) =>
    /effect_local_srvidx_[0-9a-f]+|effect_local_server_index_partition_log/.test(execution.sql)
  )

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options

let nextSequence = 1
const envelope = (name: string, payload: typeof Protocol.MutationEnvelope.Type["payload"]) =>
  Effect.gen(function*() {
    const localSequence = nextSequence++
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name,
      payload,
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Identity.SchemaVersion.make(1)
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const windowedScope = Protocol.ReplicationScope.make({
  models: [],
  windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 50 })]
})

const layers = () => {
  const database = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
  const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))
  const server = ServerStore.layer({
    definition: Domain.definition,
    wakeCapacity: 16,
    maximumWatchersPerSpace: 1_024,
    maximumConcurrentReadAuthorizations: 64,
    maximumPendingReadAuthorizations: 4_096,
    readAuthorizationCacheCapacity: 4_096,
    retainedHistoryEntries: 1_024,
    maximumHistoryEntries: 100_000,
    retainedReceipts: 256,
    maximumReceipts: 100_000,
    maximumSnapshotEntities: 100_000,
    maximumSnapshotBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
    pruneBatchSize: 1_000,
    retainedSnapshots: 2,
    maintenanceConcurrency: 1,
    maintenanceSpaceBatchSize: 128,
    migration,
    readAuthorizationRefreshInterval: "1 second",
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () => Effect.void
  }).pipe(Layer.provide(runtime), Layer.provide(database))
  const remote = Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const store = yield* ServerStore.ServerStore
      return SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        discard: (request) => store.discard(request, "reader"),
        submit: store.submit,
        pull: (request) => store.pullAuthorized(request, "reader"),
        bootstrap: (request) => store.bootstrapAuthorized(request, "reader"),
        watch: (request) => Stream.unwrap(store.watchAuthorized(request, "reader"))
      })
    })
  ).pipe(Layer.provide(server))
  const local = LocalStore.layer({
    settlementCapacity: 64,
    retainedReceipts: 256,
    maximumReceipts: 100_000,
    retainedHistoryEntries: 1_024,
    maximumBootstrapEntities: 100_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
    migration,
    definition: Domain.definition,
    spaceId,
    clientId: readerId,
    scope: windowedScope
  }).pipe(Layer.provide(runtime), Layer.provide(database))
  const reconciler = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
    Layer.provide(local),
    Layer.provide(remote)
  )
  return Layer.mergeAll(local, reconciler, server, database)
}

describe("entity lookup query plans", () => {
  it.effect("defers server index maintenance until the index is built", () =>
    Effect.gen(function*() {
      nextSequence = 1
      const server = yield* ServerStore.ServerStore
      executions.length = 0
      recording = true
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 200, chats: 10 }))
      recording = false
      assert.strictEqual(serverIndexExecutions().length, 0)
    }).pipe(Effect.provide(layers())), 120_000)

  it.effect("batches maintenance for a built server index", () =>
    Effect.gen(function*() {
      nextSequence = 1
      const server = yield* ServerStore.ServerStore
      const reconciler = yield* Reconciler.Reconciliation
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 200, chats: 10 }))
      yield* reconciler.sync

      executions.length = 0
      recording = true
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 210, chats: 10 }))
      recording = false
      assert.isAtMost(serverIndexExecutions().length, 10)
    }).pipe(Effect.provide(layers())), 120_000)

  it.effect("uses the server scan index for window ordering", () =>
    Effect.gen(function*() {
      nextSequence = 1
      const server = yield* ServerStore.ServerStore
      const reconciler = yield* Reconciler.Reconciliation
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 200, chats: 10 }))
      executions.length = 0
      recording = true
      yield* reconciler.sync
      recording = false
      const plans = windowSortPlans()
      assert.deepStrictEqual(plans.errors, [])
      assert.isAbove(plans.inspected, 0)
      assert.deepStrictEqual(plans.temporarySorts, [])
    }).pipe(Effect.provide(layers())), 120_000)

  it.effect("resolves multi-entity lookups through the entity key index", () =>
    Effect.gen(function*() {
      nextSequence = 1
      const server = yield* ServerStore.ServerStore
      const reconciler = yield* Reconciler.Reconciliation
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 200, chats: 10 }))
      yield* reconciler.sync
      yield* server.submit(yield* envelope(Domain.PutManyMessages.name, { count: 210, chats: 10 }))
      executions.length = 0
      recording = true
      yield* reconciler.sync
      recording = false
      const lookups = unindexedKeyLookups()
      assert.deepStrictEqual(lookups.errors, [])
      assert.isAbove(lookups.inspected, 0)
      assert.deepStrictEqual(lookups.offenders, [])
    }).pipe(Effect.provide(layers())), 120_000)
})
