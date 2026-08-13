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
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Domain from "./Domain.js"

class UnexpectedBootstrapInstallSuccess extends Schema.TaggedErrorClass<UnexpectedBootstrapInstallSuccess>(
  "@lucas-barake/effect-local-sql/test/UnexpectedBootstrapInstallSuccess"
)("UnexpectedBootstrapInstallSuccess", {}) {}

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

const database = () => {
  const sqlite = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  return Layer.mergeAll(
    sqlite,
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
}

const localLayer = (clientId: Identity.ClientId) => {
  return LocalStore.layer({ ...options, definition: Domain.definition, spaceId, clientId }).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )
}

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
      const firstLayer = localLayer(firstClientId)
      const secondLayer = localLayer(secondClientId)
      const firstContext = yield* Layer.buildWithScope(firstLayer, firstScope)
      const secondContext = yield* Layer.buildWithScope(secondLayer, secondScope)
      const first = Context.get(firstContext, LocalStore.Store)
      const second = Context.get(secondContext, LocalStore.Store)

      assert.strictEqual(yield* pendingCount, 0)
      const firstTodo = Domain.todo("first")
      const secondTodo = Domain.todo("second")
      yield* first.mutate(Domain.PutTodo, firstTodo)
      yield* second.mutate(Domain.PutTodo, secondTodo)
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
      const firstLayer = localLayer(firstClientId)
      const secondLayer = localLayer(secondClientId)
      const firstContext = yield* Layer.buildWithScope(firstLayer, firstScope).pipe(
        Effect.provideService(Metric.MetricRegistry, firstRegistry)
      )
      const secondContext = yield* Layer.buildWithScope(secondLayer, secondScope).pipe(
        Effect.provideService(Metric.MetricRegistry, secondRegistry)
      )
      const first = Context.get(firstContext, LocalStore.Store)
      const second = Context.get(secondContext, LocalStore.Store)

      const firstTodo = Domain.todo("first")
      yield* first.mutate(Domain.PutTodo, firstTodo).pipe(
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

      const secondTodo = Domain.todo("second")
      yield* second.mutate(Domain.PutTodo, secondTodo).pipe(
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

  it.effect("counts pending rows once at recovery and then tracks durable row deltas", () => {
    const registry = new Map()
    return Effect.gen(function*() {
      const actualSql = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
        Effect.provide(Reactivity.layer)
      )
      let pendingReads = 0
      const observedSql = new Proxy(actualSql, {
        apply: (target, thisArgument, argumentsList) => {
          const statement = Reflect.apply(target, thisArgument, argumentsList)
          let text = ""
          if (Array.isArray(argumentsList[0])) text = argumentsList[0].join("?")
          if (text.includes("COUNT(*) AS count FROM effect_local_client_pending_data")) {
            pendingReads++
          }
          return statement
        }
      })
      const observedSqlLayer = Layer.succeed(SqlClient.SqlClient, observedSql)
      const infrastructure = Layer.mergeAll(
        observedSqlLayer,
        NodeCrypto.layer,
        Reactivity.layer,
        QueryReactivity.layer
      )
      const context = yield* LocalStore.layer({
        ...options,
        definition: Domain.definition,
        spaceId,
        clientId: firstClientId
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(infrastructure),
        Layer.build
      )
      const store = Context.get(context, LocalStore.Store)

      const mutations = []
      for (let index = 0; index < 4; index++) {
        const todo = Domain.todo(`pending-${index}`)
        const mutation = yield* store.mutate(Domain.PutTodo, todo)
        mutations.push(mutation)
      }

      assert.strictEqual(yield* pendingCount, 4)
      assert.strictEqual(pendingReads, 5)

      for (const mutation of mutations) {
        yield* store.applyReceipt({
          _tag: "Rejected",
          name: mutation.envelope.name,
          sourceSchema: mutation.envelope.sourceSchema,
          mutationVersion: mutation.envelope.mutationVersion,
          spaceId,
          clientId: firstClientId,
          membershipIncarnation: mutation.envelope.membershipIncarnation,
          mutationId: mutation.envelope.mutationId,
          localSequence: mutation.envelope.localSequence,
          origin: "Authorization",
          rejection: "denied"
        })
      }

      assert.strictEqual((yield* store.pending).length, 0)
      assert.strictEqual(yield* pendingCount, 0)
      assert.strictEqual(pendingReads, 5)
    }).pipe(
      Effect.scoped,
      Effect.provideService(Metric.MetricRegistry, registry)
    )
  })

  it.effect("removes an applied pending contribution when its update defects afterward", () => {
    const registry = new Map<string, Metric.Metric.Metadata<any, any>>()
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
      const merged = Layer.merge(local, clientDatabase)
      const context = yield* Layer.buildWithScope(merged, scope)
      const store = Context.get(context, LocalStore.Store)
      const values = registry.values()
      const entries = Array.from(values)
      const metadata = entries.find((entry) => entry.id === "effect_local_client_pending_mutation_count")
      assert.isDefined(metadata)
      const update = metadata.hooks.update.bind(metadata.hooks)
      let defectPendingIncrement = true
      Object.defineProperty(metadata.hooks, "update", {
        configurable: true,
        value: (input: number, metricContext: never) => {
          update(input, metricContext)
          if (defectPendingIncrement && input === 1) {
            defectPendingIncrement = false
            assert.fail("pending metric defect")
          }
        }
      })

      const todo = Domain.todo("pending-metric-defect")
      yield* store.mutate(Domain.PutTodo, todo)
      assert.strictEqual(yield* pendingCount, 1)

      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(yield* pendingCount, 0)
    }).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("counts a durable bootstrap install before projection replay", () => {
    const registry = new Map()
    const registrySet = registry.set.bind(registry)
    let failBootstrapMetric = false
    registry.set = (key, metadata) => {
      if (metadata.id !== "effect_local_client_bootstrap_install") return registrySet(key, metadata)
      const hooks = new Proxy(metadata.hooks, {
        get: (target, property, receiver) => {
          if (property !== "update") return Reflect.get(target, property, receiver)
          return (...args: ReadonlyArray<unknown>) => {
            const result = Reflect.apply(target.update, target, args)
            if (failBootstrapMetric) return assert.fail("bootstrap metric failed")
            return result
          }
        }
      })
      return registrySet(key, { ...metadata, hooks })
    }
    return Effect.gen(function*() {
      const bootstrapInstall = Metric.counter("effect_local_client_bootstrap_install", {
        description: "Durable client bootstrap installations",
        incremental: true
      })
      yield* Metric.update(bootstrapInstall, 0)
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
      const merged = Layer.merge(local, clientDatabase)
      const context = yield* Layer.buildWithScope(merged, scope)
      const store = Context.get(context, LocalStore.Store)
      const sql = Context.get(context, SqlClient.SqlClient)
      const todo = Domain.todo("pending")
      yield* store.mutate(Domain.PutTodo, todo)
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
      const page = Protocol.BootstrapPage.make({
        manifest,
        entries: [],
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      })
      yield* store.stageBootstrapPage(page)
      yield* sql`CREATE TRIGGER fail_projection_replay
        BEFORE INSERT ON effect_local_client_visible_entities_data
        BEGIN SELECT RAISE(FAIL, 'projection replay failed'); END`
      failBootstrapMetric = true

      const unexpectedSuccess = new UnexpectedBootstrapInstallSuccess()
      const error = yield* store.installBootstrap(manifest).pipe(
        Effect.as(unexpectedSuccess),
        Effect.flip
      )

      assert.strictEqual(error._tag, "StorageUnavailable", "bootstrap installation must fail during projection replay")
      assert.strictEqual(yield* store.cursor, 1)
      assert.strictEqual(yield* bootstrapInstallCount, 1)
      assert.strictEqual(yield* pendingCount, 1)
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(yield* pendingCount, 0)
    }).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })
})
