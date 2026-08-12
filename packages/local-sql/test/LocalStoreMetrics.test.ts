import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Scope from "effect/Scope"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const firstClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const options = {
  scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  settlementCapacity: 64,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}
const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )

const localLayer = (clientId: Identity.ClientId) =>
  LocalStore.layer({ ...options, definition: Domain.definition, spaceId, clientId }).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )

const metricCount = (id: string) =>
  Metric.snapshot.pipe(Effect.map((snapshots) => {
    const metric = snapshots.find((candidate) => candidate.id === id)
    if (metric === undefined || metric.type !== "Counter") {
      return assert.fail(`Counter metric ${id} was not registered`)
    }
    return metric.state.count
  }))

const pendingCount = metricCount("effect_local_client_pending_mutation_count")
const bootstrapInstallCount = metricCount("effect_local_client_bootstrap_install")

describe("LocalStore metrics", () => {
  it.effect("aggregates pending mutations and removes each scoped contribution", () => {
    const registry = new Map()
    return Effect.gen(function*() {
      const firstScope = yield* Scope.make()
      const secondScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(localLayer(firstClientId), firstScope)
      const secondContext = yield* Layer.buildWithScope(localLayer(secondClientId), secondScope)
      const first = Context.get(firstContext, LocalStore.Store)
      const second = Context.get(secondContext, LocalStore.Store)

      assert.strictEqual(yield* pendingCount, 0)
      yield* first.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* second.mutate(Domain.PutTodo, Domain.todo("second"))
      assert.strictEqual(yield* pendingCount, 2)

      yield* Scope.close(firstScope, Exit.void)
      assert.strictEqual(yield* pendingCount, 1)

      const pending = (yield* second.pending)[0]
      yield* second.applyReceipt({
        _tag: "Rejected",
        name: pending.envelope.name,
        sourceSchema: pending.envelope.sourceSchema,
        mutationVersion: pending.envelope.mutationVersion,
        spaceId,
        clientId: secondClientId,
        membershipIncarnation: pending.envelope.membershipIncarnation,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        origin: "Authorization",
        rejection: "denied"
      })
      assert.strictEqual(yield* pendingCount, 0)
      yield* Scope.close(secondScope, Exit.void)
      assert.strictEqual(yield* pendingCount, 0)
    }).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("keeps separately built layers isolated to their construction registries", () => {
    const firstRegistry = new Map()
    const secondRegistry = new Map()
    return Effect.gen(function*() {
      const firstScope = yield* Scope.make()
      const secondScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(localLayer(firstClientId), firstScope).pipe(
        Effect.provideService(Metric.MetricRegistry, firstRegistry)
      )
      const secondContext = yield* Layer.buildWithScope(localLayer(secondClientId), secondScope).pipe(
        Effect.provideService(Metric.MetricRegistry, secondRegistry)
      )
      const first = Context.get(firstContext, LocalStore.Store)
      const second = Context.get(secondContext, LocalStore.Store)

      yield* first.mutate(Domain.PutTodo, Domain.todo("first")).pipe(
        Effect.provideService(Metric.MetricRegistry, firstRegistry)
      )
      assert.strictEqual(
        yield* pendingCount.pipe(Effect.provideService(Metric.MetricRegistry, firstRegistry)),
        1
      )
      assert.strictEqual(
        yield* pendingCount.pipe(Effect.provideService(Metric.MetricRegistry, secondRegistry)),
        0
      )

      yield* second.mutate(Domain.PutTodo, Domain.todo("second")).pipe(
        Effect.provideService(Metric.MetricRegistry, secondRegistry)
      )
      assert.strictEqual(
        yield* pendingCount.pipe(Effect.provideService(Metric.MetricRegistry, firstRegistry)),
        1
      )
      assert.strictEqual(
        yield* pendingCount.pipe(Effect.provideService(Metric.MetricRegistry, secondRegistry)),
        1
      )

      yield* Scope.close(firstScope, Exit.void).pipe(Effect.provideService(Metric.MetricRegistry, firstRegistry))
      yield* Scope.close(secondScope, Exit.void).pipe(Effect.provideService(Metric.MetricRegistry, secondRegistry))
    })
  })

  it.effect("does not fail a committed mutation when pending metric refresh fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const actualSql = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
        Effect.provide(Reactivity.layer)
      )
      let failMetricRefresh = false
      let pendingReads = 0
      const observedSql = new Proxy(actualSql, {
        apply: (target, thisArgument, argumentsList) => {
          const statement = Reflect.apply(target, thisArgument, argumentsList)
          let text = ""
          if (Array.isArray(argumentsList[0])) text = argumentsList[0].join("?")
          if (!failMetricRefresh || !text.includes("COUNT(*) AS count FROM effect_local_client_pending_data")) {
            return statement
          }
          pendingReads++
          if (pendingReads === 1) return statement
          return Effect.die("pending metric refresh failed")
        }
      })
      const infrastructure = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, observedSql),
        NodeCrypto.layer,
        Reactivity.layer,
        QueryReactivity.layer
      )
      const context = yield* Layer.build(
        LocalStore.layer({
          ...options,
          definition: Domain.definition,
          spaceId,
          clientId: firstClientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(infrastructure)
        )
      )
      const store = Context.get(context, LocalStore.Store)
      failMetricRefresh = true

      const mutation = yield* store.mutate(Domain.PutTodo, Domain.todo("committed"))

      assert.strictEqual(mutation.envelope.localSequence, 1)
      assert.strictEqual(pendingReads, 2)
      assert.strictEqual((yield* store.pending).length, 1)
    })))

  it.effect("counts a durable bootstrap install before projection replay", () => {
    const registry = new Map()
    return Effect.gen(function*() {
      const scope = yield* Scope.make()
      const clientDatabase = database()
      const local = LocalStore.layer({
        ...options,
        definition: Domain.definition,
        spaceId,
        clientId: firstClientId
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(clientDatabase)
      )
      const context = yield* Layer.buildWithScope(Layer.merge(local, clientDatabase), scope)
      const store = Context.get(context, LocalStore.Store)
      const sql = Context.get(context, SqlClient.SqlClient)
      yield* store.mutate(Domain.PutTodo, Domain.todo("pending"))
      const manifest = Protocol.SnapshotManifest.make({
        spaceId,
        clientId: firstClientId,
        definitionHash: Domain.definition.hash,
        schema: Domain.definition.schemaIdentity,
        scopeDigest: yield* Protocol.replicationScopeDigest(options.scope).pipe(Effect.provide(NodeCrypto.layer)),
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: {
          viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
          revision: Identity.ReplicationViewRevision.make(0)
        },
        snapshotId: Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001"),
        sequence: Identity.ServerSequence.make(1),
        terminalSequenceThrough: Identity.TerminalSequence.make(0),
        entityCount: 0,
        contentBytes: 0,
        digest: Protocol.initialSnapshotDigest
      })
      yield* store.prepareBootstrap(manifest)
      yield* store.stageBootstrapPage(Protocol.BootstrapPage.make({
        manifest,
        entries: [],
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      }))
      yield* sql`CREATE TRIGGER fail_projection_replay
        BEFORE INSERT ON effect_local_client_visible_entities_data
        BEGIN SELECT RAISE(FAIL, 'projection replay failed'); END`

      const error = yield* store.installBootstrap(manifest).pipe(Effect.flip)

      assert.strictEqual(error._tag, "StorageUnavailable")
      assert.strictEqual(yield* store.cursor, 1)
      assert.strictEqual(yield* bootstrapInstallCount, 1)
      assert.strictEqual(yield* pendingCount, 1)
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(yield* pendingCount, 0)
    }).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })
})
