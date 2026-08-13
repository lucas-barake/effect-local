import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Codec from "../src/internal/codec.js"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ReconciliationWorkflow from "../src/ReconciliationWorkflow.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const history = {
  wakeCapacity: 16,
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}
const clientHistory = {
  settlementCapacity: 64,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}

const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true }).pipe((nestedCallValue) =>
  Layer.mergeAll(
    nestedCallValue,
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
)
const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const TestAuthorizationError = Schema.TaggedStruct("TestAuthorizationError", { reason: Schema.String })

const defaultAuthorizeRead: ServerStore.Options["authorizeRead"] = (input) => {
  if (input._tag === "Entity" && input.entity.key === "private" && input.principal !== "owner") {
    return pipe(TestAuthorizationError.make({ reason: "private" }), Effect.fail)
  }
  return Effect.void
}

const makeServer = (
  authorizeRead: ServerStore.Options["authorizeRead"] = defaultAuthorizeRead,
  overrides: Partial<typeof history> = {}
) =>
  ServerStore.layer({
    ...history,
    ...overrides,
    definition: Domain.definition,
    readAuthorizationRefreshInterval: "1 second",
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead
  }).pipe(Layer.provide(runtime), Layer.provide(database))

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const titleOf = (value: unknown): unknown => {
  if (value === null || typeof value !== "object" || Array.isArray(value) || !("title" in value)) return undefined
  return value.title
}

const envelope = (id: string, sequence: number, title = "first") =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(sequence),
      basis: Identity.ServerSequence.make(0),
      name: Domain.PutTodo.name,
      payload: Domain.todo(id, title),
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.PutTodo.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const deleteEnvelope = (id: string, sequence: number) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(sequence),
      basis: Identity.ServerSequence.make(0),
      name: Domain.DeleteTodo.name,
      payload: { id },
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.DeleteTodo.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const putManyEnvelope = (count: number, sequence: number) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(sequence),
      basis: Identity.ServerSequence.make(0),
      name: Domain.PutManyTodos.name,
      payload: { count },
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.PutManyTodos.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const pullRequest = (
  cursor: Protocol.ReplicationCursor | null = null,
  requestedScope = scope,
  generation = 1
): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: readerId,
    schema: Domain.definition.schemaIdentity,
    scope: requestedScope,
    scopeGeneration: Identity.ReplicationScopeGeneration.make(generation),
    cursor,
    limit: 100
  })

const bootstrapRequest = (manifest: Protocol.SnapshotManifest): Protocol.BootstrapRequest =>
  Protocol.BootstrapRequest.make({
    spaceId,
    clientId: readerId,
    schema: Domain.definition.schemaIdentity,
    scope,
    scopeGeneration: manifest.scopeGeneration,
    cursor: manifest.cursor,
    snapshotId: manifest.snapshotId,
    afterOrdinal: -1,
    limit: 100
  })

describe("scoped replication", () => {
  it.effect("bootstraps only scoped entities visible to the principal", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      yield* server.submit(yield* envelope("private", 2))

      const required = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const page = yield* pipe(bootstrapRequest(required.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      assert.isFalse(page.hasMore)
      pipe(
        page.entries.map((entry) =>
          entry.change
        ).map((change) =>
          change.entity.key
        ),
        (nestedCallValue) =>
          assert.deepStrictEqual(
            nestedCallValue,
            ["public"]
          )
      )
      assert.strictEqual(page.manifest.clientId, readerId)
      assert.strictEqual(page.manifest.entityCount, 1)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("binds scoped manifests to the authenticated principal", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("private", 1))
      const reader = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      const owner = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "owner"))
      if (!("_tag" in reader) || !("_tag" in owner)) {
        assert.fail("expected scoped bootstraps")
      }
      assert.notStrictEqual(reader.manifest.snapshotId, owner.manifest.snapshotId)
      assert.strictEqual(reader.manifest.entityCount, 0)
      assert.strictEqual(owner.manifest.entityCount, 1)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("does not disclose prior principal keys when a client changes principals", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("private", 1))
      const owner = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "owner"))
      if (!("_tag" in owner)) {
        assert.fail("expected owner bootstrap")
      }
      yield* pipe(bootstrapRequest(owner.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "owner"))

      const reader = yield* pipe(pullRequest(owner.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in reader)) {
        assert.fail("expected principal rotation bootstrap")
      }
      const page = yield* pipe(bootstrapRequest(reader.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      assert.deepStrictEqual(page.entries, [])
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("widens and narrows a scope through incremental pages", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      const emptyScope = Protocol.ReplicationScope.make({ models: [] })
      const initial = yield* pipe(pullRequest(null, emptyScope, 1), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "owner"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* pipe(
        Protocol.BootstrapRequest.make({
          ...bootstrapRequest(initial.manifest),
          scope: emptyScope
        }),
        (nestedCallValue: Protocol.BootstrapRequest) =>
          server.bootstrapAuthorized(
            nestedCallValue,
            "owner"
          )
      )
      assert.deepStrictEqual(bootstrap.entries, [])

      const widened = yield* pipe(
        pullRequest(initial.manifest.cursor, scope, 2),
        (nestedCallValue: Protocol.PullRequest) =>
          server.pullAuthorized(nestedCallValue, "owner")
      )
      if ("_tag" in widened) assert.fail("scope widening must not bootstrap")
      pipe(
        widened.changes.map((change) => change._tag),
        (nestedCallValue) => assert.deepStrictEqual(nestedCallValue, ["Upsert"])
      )

      const narrowed = yield* pipe(
        pullRequest(widened.cursor, emptyScope, 3),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "owner")
      )
      if ("_tag" in narrowed) {
        assert.fail("scope narrowing must not bootstrap")
      }
      pipe(
        narrowed.changes.map((change) => change._tag),
        (nestedCallValue) => assert.deepStrictEqual(nestedCallValue, ["Retract"])
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("replays an unacknowledged page and retracts authorization revocation", () =>
    Effect.gen(function*() {
      let visible = true
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && !visible) {
          return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1))
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      yield* server.submit(yield* envelope("second", 2))

      const first = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in first) {
        assert.fail("expected incremental page")
      }
      const replay = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      assert.deepStrictEqual(replay, first)

      visible = false
      const rotated = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      assert.isTrue("_tag" in rotated)
      if (!("_tag" in rotated)) {
        assert.fail("unsafe outstanding page must rotate")
      }
      const revokedBootstrap = yield* pipe(
        bootstrapRequest(rotated.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) =>
          server.bootstrapAuthorized(nestedCallValue, "reader")
      )
      assert.deepStrictEqual(revokedBootstrap.entries, [])
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("authorizes only changed and acknowledged entities on a steady pull", () =>
    Effect.gen(function*() {
      let entityAuthorizations = 0
      const server = yield* pipe(
        makeServer((input) => {
          if (input._tag !== "Entity") return Effect.void
          entityAuthorizations++
          if (
            typeof input.entity.key === "string" && input.entity.key.startsWith("hidden-") &&
            input.principal !== "owner"
          ) {
            return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
          }
          return Effect.void
        }),
        (rebaseNestedCallValue1) =>
          service(
            ServerStore.ServerStore,
            rebaseNestedCallValue1
          )
      )
      for (let index = 0; index < 28; index++) {
        yield* server.submit(yield* envelope(`hidden-${index}`, index + 1))
      }
      yield* server.submit(yield* envelope("visible-a", 29))
      yield* server.submit(yield* envelope("visible-b", 30))
      const required = yield* pipe(pullRequest(), (rebaseNestedCallValue2) =>
        server.pullAuthorized(rebaseNestedCallValue2, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* pipe(bootstrapRequest(required.manifest), (rebaseNestedCallValue3) =>
        server.bootstrapAuthorized(rebaseNestedCallValue3, "reader"))
      assert.isFalse(bootstrap.hasMore)
      const settled = yield* pipe(pullRequest(required.manifest.cursor), (rebaseNestedCallValue4) =>
        server.pullAuthorized(rebaseNestedCallValue4, "reader"))
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* pipe(pullRequest(settled.cursor), (rebaseNestedCallValue5) =>
        server.pullAuthorized(rebaseNestedCallValue5, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* envelope("visible-a", 31, "renamed"))
      entityAuthorizations = 0
      const page = yield* pipe(pullRequest(acknowledged.cursor), (rebaseNestedCallValue6) =>
        server.pullAuthorized(rebaseNestedCallValue6, "reader"))
      if ("_tag" in page) {
        assert.fail("expected incremental page")
      }
      pipe(
        page.changes.map((change) =>
          change._tag
        ),
        (rebaseNestedCallValue7) =>
          assert.deepStrictEqual(rebaseNestedCallValue7, ["Upsert"])
      )
      assert.isAtMost(entityAuthorizations, 10)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("delivers a diff larger than the page limit completely across pulls", () =>
    Effect.gen(function*() {
      const server = yield* pipe(makeServer(), (rebaseNestedCallValue8) =>
        service(ServerStore.ServerStore, rebaseNestedCallValue8))
      yield* server.submit(yield* envelope("seed", 1))
      const required = yield* pipe(pullRequest(), (rebaseNestedCallValue9) =>
        server.pullAuthorized(rebaseNestedCallValue9, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(required.manifest), (rebaseNestedCallValue10) =>
        server.bootstrapAuthorized(rebaseNestedCallValue10, "reader"))
      const settled = yield* pipe(pullRequest(required.manifest.cursor), (rebaseNestedCallValue11) =>
        server.pullAuthorized(rebaseNestedCallValue11, "reader"))
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* pipe(pullRequest(settled.cursor), (rebaseNestedCallValue12) =>
        server.pullAuthorized(rebaseNestedCallValue12, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* putManyEnvelope(250, 2))
      let cursor = acknowledged.cursor
      const delivered = new Map<string, string>()
      const pageSizes: Array<number> = []
      for (let round = 0; round < 10; round++) {
        const page = yield* pipe(pullRequest(cursor), (rebaseNestedCallValue13) =>
          server.pullAuthorized(rebaseNestedCallValue13, "reader"))
        if ("_tag" in page) {
          assert.fail("expected incremental page")
        }
        pageSizes.push(page.changes.length)
        assert.isAtMost(page.changes.length, 100)
        for (const change of page.changes) {
          assert.strictEqual(change._tag, "Upsert")
          delivered.set(yield* Codec.stringify(change.entity.key), change._tag)
        }
        cursor = page.cursor
        if (!page.hasMore && page.changes.length === 0) {
          break
        }
      }
      assert.strictEqual(delivered.size, 250)
      assert.deepStrictEqual(pageSizes, [100, 100, 50, 0])
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("delivers a widened scope larger than the page limit completely across pulls", () =>
    Effect.gen(function*() {
      const server = yield* pipe(
        makeServer(),
        (rebaseNestedCallValue14) => service(ServerStore.ServerStore, rebaseNestedCallValue14)
      )
      const empty = Protocol.ReplicationScope.make({ models: [] })
      yield* server.submit(yield* putManyEnvelope(250, 1))
      const required = yield* pipe(
        pullRequest(null, empty, 1),
        (rebaseNestedCallValue15) => server.pullAuthorized(rebaseNestedCallValue15, "reader")
      )
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      yield* pipe(
        Protocol.BootstrapRequest.make({ ...bootstrapRequest(required.manifest), scope: empty }),
        (rebaseNestedCallValue16) =>
          server.bootstrapAuthorized(
            rebaseNestedCallValue16,
            "reader"
          )
      )
      const settled = yield* pipe(
        pullRequest(required.manifest.cursor, empty, 1),
        (rebaseNestedCallValue17) => server.pullAuthorized(rebaseNestedCallValue17, "reader")
      )
      if ("_tag" in settled) assert.fail("expected steady page")
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* pipe(
        pullRequest(settled.cursor, empty, 1),
        (rebaseNestedCallValue18) => server.pullAuthorized(rebaseNestedCallValue18, "reader")
      )
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      let cursor = acknowledged.cursor
      const delivered = new Set<string>()
      const pageSizes: Array<number> = []
      for (let round = 0; round < 10; round++) {
        const page = yield* pipe(
          pullRequest(cursor, scope, 2),
          (rebaseNestedCallValue19) => server.pullAuthorized(rebaseNestedCallValue19, "reader")
        )
        if ("_tag" in page) assert.fail("expected incremental page")
        pageSizes.push(page.changes.length)
        assert.isAtMost(page.changes.length, 100)
        for (const change of page.changes) {
          assert.strictEqual(change._tag, "Upsert")
          delivered.add(yield* Codec.stringify(change.entity.key))
        }
        cursor = page.cursor
        if (!page.hasMore && page.changes.length === 0) break
      }
      assert.strictEqual(delivered.size, 250)
      assert.deepStrictEqual(pageSizes, [100, 100, 50, 0])
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("retracts an acknowledged entity when authorization is revoked without a change", () =>
    Effect.gen(function*() {
      let secondVisible = true
      const server = yield* pipe(
        makeServer((input) => {
          if (input._tag === "Entity" && input.entity.key === "second" && !secondVisible) {
            return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
          }
          return Effect.void
        }),
        (rebaseNestedCallValue20) =>
          service(
            ServerStore.ServerStore,
            rebaseNestedCallValue20
          )
      )
      yield* server.submit(yield* envelope("first", 1))
      yield* server.submit(yield* envelope("second", 2))
      const required = yield* pipe(
        pullRequest(),
        (rebaseNestedCallValue21) => server.pullAuthorized(rebaseNestedCallValue21, "reader")
      )
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      yield* pipe(
        bootstrapRequest(required.manifest),
        (rebaseNestedCallValue22) => server.bootstrapAuthorized(rebaseNestedCallValue22, "reader")
      )
      const settled = yield* pipe(
        pullRequest(required.manifest.cursor),
        (rebaseNestedCallValue23) => server.pullAuthorized(rebaseNestedCallValue23, "reader")
      )
      if ("_tag" in settled) assert.fail("expected steady page")
      const acknowledged = yield* pipe(
        pullRequest(settled.cursor),
        (rebaseNestedCallValue24) => server.pullAuthorized(rebaseNestedCallValue24, "reader")
      )
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      secondVisible = false
      const revoked = yield* pipe(
        pullRequest(acknowledged.cursor),
        (rebaseNestedCallValue25) => server.pullAuthorized(rebaseNestedCallValue25, "reader")
      )
      if ("_tag" in revoked) assert.fail("expected incremental page")
      pipe(
        revoked.changes.map((change) => [change._tag, change.entity.key]),
        (rebaseNestedCallValue26) =>
          assert.deepStrictEqual(
            rebaseNestedCallValue26,
            [["Retract", "second"]]
          )
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("reveals an unchanged entity only after read authorization is invalidated", () =>
    Effect.gen(function*() {
      let hiddenVisible = false
      const server = yield* pipe(
        makeServer((input) => {
          if (input._tag === "Entity" && input.entity.key === "hidden" && !hiddenVisible) {
            return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
          }
          return Effect.void
        }),
        (rebaseNestedCallValue27) =>
          service(
            ServerStore.ServerStore,
            rebaseNestedCallValue27
          )
      )
      yield* server.submit(yield* envelope("open", 1))
      yield* server.submit(yield* envelope("hidden", 2))
      const required = yield* pipe(pullRequest(), (rebaseNestedCallValue28) =>
        server.pullAuthorized(rebaseNestedCallValue28, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(required.manifest), (rebaseNestedCallValue29) =>
        server.bootstrapAuthorized(rebaseNestedCallValue29, "reader"))
      const settled = yield* pipe(pullRequest(required.manifest.cursor), (rebaseNestedCallValue30) =>
        server.pullAuthorized(rebaseNestedCallValue30, "reader"))
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* pipe(pullRequest(settled.cursor), (rebaseNestedCallValue31) =>
        server.pullAuthorized(rebaseNestedCallValue31, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      hiddenVisible = true
      const unchanged = yield* pipe(pullRequest(acknowledged.cursor), (rebaseNestedCallValue32) =>
        server.pullAuthorized(rebaseNestedCallValue32, "reader"))
      if ("_tag" in unchanged) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(unchanged.changes, [])

      yield* server.invalidateReadAuthorization(spaceId)
      const revealed = yield* pipe(pullRequest(unchanged.cursor), (rebaseNestedCallValue33) =>
        server.pullAuthorized(rebaseNestedCallValue33, "reader"))
      if ("_tag" in revealed) {
        assert.fail("expected incremental page")
      }
      pipe(revealed.changes.map((change) => [change._tag, change.entity.key]), (rebaseNestedCallValue34) =>
        assert.deepStrictEqual(
          rebaseNestedCallValue34,
          [["Upsert", "hidden"]]
        ))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("falls back to a full derive when the log suffix is pruned", () =>
    Effect.gen(function*() {
      const server = yield* pipe(
        makeServer(defaultAuthorizeRead, { retainedHistoryEntries: 0 }),
        (rebaseNestedCallValue35) =>
          service(
            ServerStore.ServerStore,
            rebaseNestedCallValue35
          )
      )
      yield* server.submit(yield* envelope("seed", 1))
      const required = yield* pipe(pullRequest(), (rebaseNestedCallValue36) =>
        server.pullAuthorized(rebaseNestedCallValue36, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(required.manifest), (rebaseNestedCallValue37) =>
        server.bootstrapAuthorized(rebaseNestedCallValue37, "reader"))
      const settled = yield* pipe(pullRequest(required.manifest.cursor), (rebaseNestedCallValue38) =>
        server.pullAuthorized(rebaseNestedCallValue38, "reader"))
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* pipe(pullRequest(settled.cursor), (rebaseNestedCallValue39) =>
        server.pullAuthorized(rebaseNestedCallValue39, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* envelope("late-a", 2))
      yield* server.submit(yield* envelope("late-b", 3))
      yield* server.maintain(spaceId)

      const recovered = yield* pipe(pullRequest(acknowledged.cursor), (rebaseNestedCallValue40) =>
        server.pullAuthorized(rebaseNestedCallValue40, "reader"))
      if ("_tag" in recovered) {
        assert.fail("expected incremental page")
      }
      const labels: Array<string> = []
      for (const change of recovered.changes) {
        labels.push(`${change._tag}:${yield* Codec.stringify(change.entity.key)}`)
      }
      pipe(
        labels.toSorted((left, right) =>
          left.localeCompare(right)
        ),
        (rebaseNestedCallValue41) =>
          assert.deepStrictEqual(
            rebaseNestedCallValue41,
            ["Upsert:\"late-a\"", "Upsert:\"late-b\""]
          )
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("replicates a bounded window per chat, pages older history on demand, and evicts it", () =>
    Effect.gen(function*() {
      const server = yield* pipe(
        makeServer(),
        (rebaseNestedCallValue42) => service(ServerStore.ServerStore, rebaseNestedCallValue42)
      )
      const message = (id: string, chatId: string, sentAt: number, sequence: number) =>
        Effect.gen(function*() {
          const identity = {
            spaceId,
            clientId: writerId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(sequence),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutMessage.name,
            payload: { id, chatId, sentAt, body: `body-${id}` },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutMessage.version
          }
          return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
        })
      let sequence = 0
      for (let sentAt = 1; sentAt <= 10; sentAt++) {
        yield* server.submit(yield* message(`a-${sentAt}`, "chat-a", sentAt, ++sequence))
      }
      for (let sentAt = 1; sentAt <= 5; sentAt++) {
        yield* server.submit(yield* message(`b-${sentAt}`, "chat-b", sentAt, ++sequence))
      }
      const windowScope = (partitions?: ReadonlyArray<Protocol.ReplicationWindowPartition>) => {
        let window = Protocol.ReplicationWindow.make({
          model: Domain.Message.name,
          index: "byChat",
          count: 3
        })
        if (partitions !== undefined) window = Protocol.ReplicationWindow.make({ ...window, partitions })
        return Protocol.ReplicationScope.make({ models: [], windows: [window] })
      }
      const keyOf = (change: Protocol.ViewChange) => {
        if (typeof change.entity.key !== "string") assert.fail("expected string message key")
        return change.entity.key
      }
      const keysOf = (changes: ReadonlyArray<Protocol.ViewChange>, tag: string) =>
        changes.filter((change) => change._tag === tag)
          .map(keyOf)
          .toSorted((left, right) => left.localeCompare(right))

      const required = yield* pipe(
        pipe(windowScope(), (rebaseNestedCallValue44) => pullRequest(null, rebaseNestedCallValue44, 1)),
        (rebaseNestedCallValue43) => server.pullAuthorized(rebaseNestedCallValue43, "reader")
      )
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      const bootstrap = yield* pipe(
        Protocol.BootstrapRequest.make({
          ...bootstrapRequest(required.manifest),
          scope: windowScope()
        }),
        (rebaseNestedCallValue45) =>
          server.bootstrapAuthorized(
            rebaseNestedCallValue45,
            "reader"
          )
      )
      assert.isFalse(bootstrap.hasMore)
      pipe(
        bootstrap.entries.map((entry) => keyOf(entry.change)).toSorted((left, right) => left.localeCompare(right)),
        (rebaseNestedCallValue46) =>
          assert.deepStrictEqual(
            rebaseNestedCallValue46,
            ["a-10", "a-8", "a-9", "b-3", "b-4", "b-5"]
          )
      )
      const settled = yield* pipe(
        pipe(
          windowScope(),
          (rebaseNestedCallValue48) => pullRequest(required.manifest.cursor, rebaseNestedCallValue48, 1)
        ),
        (rebaseNestedCallValue47) =>
          server.pullAuthorized(
            rebaseNestedCallValue47,
            "reader"
          )
      )
      if ("_tag" in settled) assert.fail("expected steady page")
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* pipe(
        pipe(windowScope(), (rebaseNestedCallValue50) => pullRequest(settled.cursor, rebaseNestedCallValue50, 1)),
        (rebaseNestedCallValue49) => server.pullAuthorized(rebaseNestedCallValue49, "reader")
      )
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      yield* server.submit(yield* message("a-11", "chat-a", 11, ++sequence))
      const slid = yield* pipe(
        pipe(windowScope(), (rebaseNestedCallValue52) => pullRequest(acknowledged.cursor, rebaseNestedCallValue52, 1)),
        (rebaseNestedCallValue51) => server.pullAuthorized(rebaseNestedCallValue51, "reader")
      )
      if ("_tag" in slid) assert.fail("expected incremental page")
      pipe(
        keysOf(slid.changes, "Upsert"),
        (rebaseNestedCallValue53) => assert.deepStrictEqual(rebaseNestedCallValue53, ["a-11"])
      )
      pipe(
        keysOf(slid.changes, "Retract"),
        (rebaseNestedCallValue54) => assert.deepStrictEqual(rebaseNestedCallValue54, ["a-8"])
      )

      const widened = windowScope([
        Protocol.ReplicationWindowPartition.make({ key: ["chat-a"], count: 6 })
      ])
      const scrolled = yield* pipe(
        pullRequest(slid.cursor, widened, 2),
        (rebaseNestedCallValue55) => server.pullAuthorized(rebaseNestedCallValue55, "reader")
      )
      if ("_tag" in scrolled) assert.fail("expected incremental page after widening")
      pipe(
        keysOf(scrolled.changes, "Upsert"),
        (rebaseNestedCallValue56) => assert.deepStrictEqual(rebaseNestedCallValue56, ["a-6", "a-7", "a-8"])
      )
      pipe(
        keysOf(scrolled.changes, "Retract"),
        (rebaseNestedCallValue57) => assert.deepStrictEqual(rebaseNestedCallValue57, [])
      )

      const evicted = yield* pipe(
        pipe(windowScope(), (rebaseNestedCallValue59) => pullRequest(scrolled.cursor, rebaseNestedCallValue59, 3)),
        (rebaseNestedCallValue58) => server.pullAuthorized(rebaseNestedCallValue58, "reader")
      )
      if ("_tag" in evicted) assert.fail("expected incremental page after narrowing")
      pipe(
        keysOf(evicted.changes, "Upsert"),
        (rebaseNestedCallValue60) => assert.deepStrictEqual(rebaseNestedCallValue60, [])
      )
      pipe(
        keysOf(evicted.changes, "Retract"),
        (rebaseNestedCallValue61) => assert.deepStrictEqual(rebaseNestedCallValue61, ["a-6", "a-7", "a-8"])
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("withholds unauthorized entities from a replication window", () =>
    Effect.gen(function*() {
      let hiddenVisible = false
      const server = yield* pipe(
        makeServer((input) => {
          if (
            input._tag === "Entity" && typeof input.entity.key === "string" &&
            input.entity.key.startsWith("secret") && !hiddenVisible
          ) {
            return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
          }
          return Effect.void
        }),
        (rebaseNestedCallValue62) =>
          service(
            ServerStore.ServerStore,
            rebaseNestedCallValue62
          )
      )
      const message = (id: string, chatId: string, sentAt: number, sequence: number) =>
        Effect.gen(function*() {
          const identity = {
            spaceId,
            clientId: writerId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(sequence),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutMessage.name,
            payload: { id, chatId, sentAt, body: `body-${id}` },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutMessage.version
          }
          return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
        })
      yield* server.submit(yield* message("open-1", "chat-a", 1, 1))
      yield* server.submit(yield* message("secret-2", "chat-a", 2, 2))
      yield* server.submit(yield* message("open-3", "chat-a", 3, 3))
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 3 })]
      })
      const required = yield* pipe(pullRequest(null, windowed, 1), (rebaseNestedCallValue63) =>
        server.pullAuthorized(rebaseNestedCallValue63, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* pipe(
        Protocol.BootstrapRequest.make({ ...bootstrapRequest(required.manifest), scope: windowed }),
        (rebaseNestedCallValue64) =>
          server.bootstrapAuthorized(
            rebaseNestedCallValue64,
            "reader"
          )
      )
      pipe(
        bootstrap.entries.map((entry) => {
          if (typeof entry.change.entity.key !== "string") {
            assert.fail("expected string message key")
          }
          return entry.change.entity.key
        }).toSorted((left, right) =>
          left.localeCompare(right)
        ),
        (rebaseNestedCallValue65) =>
          assert.deepStrictEqual(
            rebaseNestedCallValue65,
            ["open-1", "open-3"]
          )
      )
      const settled = yield* pipe(pullRequest(required.manifest.cursor, windowed, 1), (rebaseNestedCallValue66) =>
        server.pullAuthorized(rebaseNestedCallValue66, "reader"))
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* pipe(pullRequest(settled.cursor, windowed, 1), (rebaseNestedCallValue67) =>
        server.pullAuthorized(rebaseNestedCallValue67, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* message("secret-4", "chat-a", 4, 4))
      const withheld = yield* pipe(pullRequest(acknowledged.cursor, windowed, 1), (rebaseNestedCallValue68) =>
        server.pullAuthorized(rebaseNestedCallValue68, "reader"))
      if ("_tag" in withheld) {
        assert.fail("expected incremental page")
      }
      pipe(withheld.changes.map((change) => [change._tag, change.entity.key]), (rebaseNestedCallValue69) =>
        assert.deepStrictEqual(
          rebaseNestedCallValue69,
          [["Retract", "open-1"]]
        ))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rotates a staged snapshot when an entity leaves its window", () =>
    Effect.gen(function*() {
      const server = yield* pipe(makeServer(), (rebaseNestedCallValue70) =>
        service(ServerStore.ServerStore, rebaseNestedCallValue70))
      const message = (id: string, sentAt: number) =>
        Effect.gen(function*() {
          const identity = {
            spaceId,
            clientId: writerId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8000-${String(sentAt).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(sentAt),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutMessage.name,
            payload: { id, chatId: "chat-a", sentAt, body: id },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutMessage.version
          }
          return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
        })
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 1 })]
      })
      yield* server.submit(yield* message("m-1", 1))
      const required = yield* pipe(pullRequest(null, windowed), (rebaseNestedCallValue71) =>
        server.pullAuthorized(rebaseNestedCallValue71, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* message("m-2", 2))
      const page = yield* pipe(
        Protocol.BootstrapRequest.make({ ...bootstrapRequest(required.manifest), scope: windowed }),
        (rebaseNestedCallValue72) =>
          server.bootstrapAuthorized(
            rebaseNestedCallValue72,
            "reader"
          )
      )
      pipe(
        page.entries.map((entry) =>
          entry.change.entity.key
        ),
        (rebaseNestedCallValue73) =>
          assert.deepStrictEqual(rebaseNestedCallValue73, ["m-2"])
      )
      assert.notStrictEqual(page.manifest.snapshotId, required.manifest.snapshotId)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rotates an outstanding page when an upsert leaves its window", () =>
    Effect.gen(function*() {
      const server = yield* pipe(makeServer(), (rebaseNestedCallValue74) =>
        service(ServerStore.ServerStore, rebaseNestedCallValue74))
      const message = (id: string, sentAt: number) =>
        Effect.gen(function*() {
          const identity = {
            spaceId,
            clientId: writerId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8000-${String(sentAt).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(sentAt),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutMessage.name,
            payload: { id, chatId: "chat-a", sentAt, body: id },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutMessage.version
          }
          return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
        })
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 1 })]
      })
      const required = yield* pipe(pullRequest(null, windowed), (rebaseNestedCallValue75) =>
        server.pullAuthorized(rebaseNestedCallValue75, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }
      yield* pipe(
        Protocol.BootstrapRequest.make({ ...bootstrapRequest(required.manifest), scope: windowed }),
        (rebaseNestedCallValue76) =>
          server.bootstrapAuthorized(
            rebaseNestedCallValue76,
            "reader"
          )
      )
      const steady = yield* pipe(pullRequest(required.manifest.cursor, windowed), (rebaseNestedCallValue77) =>
        server.pullAuthorized(rebaseNestedCallValue77, "reader"))
      if ("_tag" in steady) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* pipe(pullRequest(steady.cursor, windowed), (rebaseNestedCallValue78) =>
        server.pullAuthorized(rebaseNestedCallValue78, "reader"))
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* message("m-1", 1))
      const outstanding = yield* pipe(pullRequest(acknowledged.cursor, windowed), (rebaseNestedCallValue79) =>
        server.pullAuthorized(rebaseNestedCallValue79, "reader"))
      if ("_tag" in outstanding) {
        assert.fail("expected outstanding page")
      }
      yield* server.submit(yield* message("m-2", 2))
      const replacement = yield* pipe(pullRequest(acknowledged.cursor, windowed), (rebaseNestedCallValue80) =>
        server.pullAuthorized(rebaseNestedCallValue80, "reader"))
      assert.isTrue("_tag" in replacement)
      if ("_tag" in replacement) {
        assert.strictEqual(replacement._tag, "BootstrapRequired")
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects a replication window naming an inherited index property", () =>
    Effect.gen(function*() {
      const server = yield* pipe(makeServer(), (rebaseNestedCallValue81) =>
        service(ServerStore.ServerStore, rebaseNestedCallValue81))
      yield* server.submit(yield* envelope("public", 1))
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [
          Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "constructor", count: 1 })
        ]
      })
      const outcome = yield* pipe(pullRequest(null, windowed, 1), (rebaseNestedCallValue82) =>
        server.pullAuthorized(rebaseNestedCallValue82, "reader")).pipe(Effect.result)
      if (outcome._tag !== "Failure") {
        assert.fail("expected a protocol rejection")
      }
      assert.strictEqual(outcome.failure._tag, "ProtocolInvalid")
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects a replication window bound whose type does not match the sort component", () =>
    Effect.gen(function*() {
      const server = yield* pipe(
        makeServer(),
        (rebaseNestedCallValue83) => service(ServerStore.ServerStore, rebaseNestedCallValue83)
      )
      yield* server.submit(yield* envelope("public", 1))
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [
          Protocol.ReplicationWindow.make({
            model: Domain.Message.name,
            index: "byChat",
            count: 1,
            partitions: [
              Protocol.ReplicationWindowPartition.make({
                key: ["chat-a"],
                bounds: Protocol.ReplicationWindowBounds.make({ gt: true })
              })
            ]
          })
        ]
      })
      const outcome = yield* pipe(
        pullRequest(null, windowed, 1),
        (rebaseNestedCallValue84) => server.pullAuthorized(rebaseNestedCallValue84, "reader")
      ).pipe(Effect.result)
      if (outcome._tag !== "Failure") assert.fail("expected a protocol rejection")
      assert.strictEqual(outcome.failure._tag, "ProtocolInvalid")
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("accepts the maximum bounded partition override set", () =>
    Effect.gen(function*() {
      const server = yield* pipe(makeServer(), (rebaseNestedCallValue85) =>
        service(ServerStore.ServerStore, rebaseNestedCallValue85))
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [
          Protocol.ReplicationWindow.make({
            model: Domain.Message.name,
            index: "byChat",
            count: 1,
            partitions: Array.from(
              { length: Protocol.maximumReplicationWindowPartitions },
              (_, index) =>
                Protocol.ReplicationWindowPartition.make({ key: [`chat-${index}`] })
            )
          })
        ]
      })
      const result = yield* pipe(pullRequest(null, windowed, 1), (rebaseNestedCallValue86) =>
        server.pullAuthorized(rebaseNestedCallValue86, "reader"))
      assert.isTrue("_tag" in result)
      if ("_tag" in result) {
        assert.strictEqual(result._tag, "BootstrapRequired")
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("replaces a windowed view when the index layout changes", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/index-layout.sqlite`
      const reorderedMessage = Model.make(Domain.Message.name, {
        version: Domain.Message.version,
        key: Domain.Message.key,
        schema: Domain.Message.schema,
        indexes: {
          byChat: {
            version: 2,
            partition: Domain.Message.indexes.byChat.partition,
            sort: [{
              name: "body",
              affinity: "text",
              schema: Schema.String,
              extract: (message: typeof Domain.Message.schema.Type) => message.body
            }]
          }
        }
      })
      const reorderedDefinition = Definition.make({
        version: Domain.definition.version,
        models: [Domain.Todo, reorderedMessage],
        mutations: Domain.definition.mutations,
        queries: Domain.definition.queries
      })
      assert.deepStrictEqual(reorderedDefinition.schemaIdentity, Domain.definition.schemaIdentity)
      assert.strictEqual(reorderedDefinition.hash, Domain.definition.hash)
      assert.notStrictEqual(reorderedDefinition.indexLayoutHash, Domain.definition.indexLayoutHash)
      const persistentDatabase = () =>
        pipe(SqliteClient.layer({ filename, disableWAL: true }), (rebaseNestedCallValue87) =>
          Layer.mergeAll(
            rebaseNestedCallValue87,
            NodeCrypto.layer,
            Reactivity.layer,
            QueryReactivity.layer
          ))
      const build = (definition: Definition.Any) => {
        return ServerStore.layer({
          ...history,
          definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(Domain.handlers))),
          Layer.provideMerge(persistentDatabase()),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
      }
      const message = (id: string, sentAt: number, body: string) =>
        Effect.gen(function*() {
          const identity = {
            spaceId,
            clientId: writerId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8000-${String(sentAt).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(sentAt),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutMessage.name,
            payload: { id, chatId: "chat-a", sentAt, body },
            digestVersion: 3 as const,
            membershipIncarnation,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutMessage.version
          }
          return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
        })
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 2 })]
      })

      const first = yield* build(Domain.definition)
      yield* first.submit(yield* message("m-1", 1, "z"))
      yield* first.submit(yield* message("m-2", 2, "a"))
      yield* first.submit(yield* message("m-3", 3, "b"))
      const required = yield* pipe(pullRequest(null, windowed), (rebaseNestedCallValue91) =>
        first.pullAuthorized(rebaseNestedCallValue91, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }
      const page = yield* pipe(
        Protocol.BootstrapRequest.make({ ...bootstrapRequest(required.manifest), scope: windowed }),
        (rebaseNestedCallValue92) =>
          first.bootstrapAuthorized(
            rebaseNestedCallValue92,
            "reader"
          )
      )
      pipe(
        page.entries.map((entry) =>
          entry.change.entity.key
        ),
        (rebaseNestedCallValue93) => assert.deepStrictEqual(rebaseNestedCallValue93, ["m-2", "m-3"])
      )

      const second = yield* build(reorderedDefinition)
      const replacement = yield* pipe(pullRequest(required.manifest.cursor, windowed), (rebaseNestedCallValue94) =>
        second.pullAuthorized(
          rebaseNestedCallValue94,
          "reader"
        ))
      assert.isTrue("_tag" in replacement)
      if ("_tag" in replacement) {
        assert.strictEqual(replacement._tag, "BootstrapRequired")
        assert.notStrictEqual(replacement.manifest.cursor.viewId, required.manifest.cursor.viewId)
      }
    }).pipe(Effect.provide(Layer.merge(NodeFileSystem.layer, NodeCrypto.layer))))

  it.effect("emits a periodic revocation hint", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const watcher = yield* pipe(
        Protocol.WatchRequest.make({
          spaceId,
          clientId: readerId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: initial.manifest.cursor
        }),
        (nestedCallValue: Protocol.WatchRequest) =>
          server.watchAuthorized(
            nestedCallValue,
            "reader"
          )
      ).pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.runForEach((wake) =>
              Queue.offer(wakes, wake)
            )
          )
        ),
        Effect.flatMap(Effect.forkChild({ startImmediately: true }))
      )
      assert.deepStrictEqual(yield* Queue.take(wakes), { spaceId })
      const periodicWake = yield* Queue.take(wakes).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Fiber.join(periodicWake), { spaceId })
      yield* Fiber.interrupt(watcher)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("does not disclose another principal's private mutations through watch wakes", () =>
    Effect.gen(function*() {
      const readerChecks = yield* Ref.make(0)
      const firstReaderCheckStarted = yield* Deferred.make<void>()
      const releaseFirstReaderCheck = yield* Deferred.make<void>()
      const secondReaderCheckStarted = yield* Deferred.make<void>()
      const releaseSecondReaderCheck = yield* Deferred.make<void>()
      const wakeCount = yield* Ref.make(0)
      const firstWake = yield* Deferred.make<void>()
      const secondWake = yield* Deferred.make<void>()
      const server = yield* makeServer((input) => {
        if (input._tag !== "Entity") return Effect.void
        if (input.entity.key === "private" && input.principal === "reader") {
          return Effect.gen(function*() {
            const check = yield* Ref.updateAndGet(readerChecks, (count) => count + 1)
            if (check === 1) {
              yield* Deferred.succeed(firstReaderCheckStarted, undefined)
              yield* Deferred.await(releaseFirstReaderCheck)
              return yield* pipe(TestAuthorizationError.make({ reason: "private" }), Effect.fail)
            }
            yield* Deferred.succeed(secondReaderCheckStarted, undefined)
            yield* Deferred.await(releaseSecondReaderCheck)
            return yield* pipe(TestAuthorizationError.make({ reason: "private" }), Effect.fail)
          })
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("private", 1))
      const owner = yield* pipe(
        pullRequest(),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "owner")
      )
      if (!("_tag" in owner)) assert.fail("expected owner bootstrap")
      yield* pipe(
        bootstrapRequest(owner.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) => server.bootstrapAuthorized(nestedCallValue, "owner")
      )

      const watcher = yield* pipe(
        Protocol.WatchRequest.make({
          spaceId,
          clientId: readerId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: owner.manifest.cursor
        }),
        (nestedCallValue: Protocol.WatchRequest) =>
          server.watchAuthorized(
            nestedCallValue,
            "reader"
          )
      ).pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.tap(() =>
              Ref.updateAndGet(wakeCount, (count) => count + 1).pipe(
                Effect.flatMap((count) => {
                  if (count === 1) return Deferred.succeed(firstWake, undefined)
                  return Deferred.succeed(secondWake, undefined)
                })
              )
            ),
            Stream.runDrain
          )
        ),
        Effect.flatMap(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.await(firstWake)
      yield* server.submit(yield* envelope("private", 2, "changed"))
      yield* Deferred.await(firstReaderCheckStarted)
      yield* server.submit(yield* envelope("private", 3, "changed again"))
      yield* Deferred.succeed(releaseFirstReaderCheck, undefined)
      yield* Deferred.await(secondReaderCheckStarted)
      assert.strictEqual(yield* Ref.get(wakeCount), 1)
      yield* Deferred.succeed(releaseSecondReaderCheck, undefined)
      yield* server.submit(yield* envelope("public", 4))
      yield* Deferred.await(secondWake)
      assert.strictEqual(yield* Ref.get(wakeCount), 2)
      yield* Fiber.interrupt(watcher)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("wakes for a maximum-depth bulk mutation", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      const firstWake = yield* Deferred.make<void>()
      const watcher = yield* pipe(
        Protocol.WatchRequest.make({
          spaceId,
          clientId: readerId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: initial.manifest.cursor
        }),
        (nestedCallValue: Protocol.WatchRequest) =>
          server.watchAuthorized(
            nestedCallValue,
            "reader"
          )
      ).pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.tap(() =>
              Deferred.succeed(firstWake, undefined)
            ),
            Stream.take(2),
            Stream.runCollect
          )
        ),
        Effect.flatMap(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.await(firstWake)
      yield* server.submit(yield* putManyEnvelope(999, 1))
      const wakes = yield* Fiber.join(watcher)
      assert.strictEqual(wakes.length, 2)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("returns a retained accepted receipt while a watcher is active", () =>
    Effect.gen(function*() {
      const server = yield* makeServer(defaultAuthorizeRead, { retainedHistoryEntries: 0 }).pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const firstWake = yield* Deferred.make<void>()
      const watcher = yield* pipe(
        Protocol.WatchRequest.make({
          spaceId,
          clientId: readerId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }),
        (nestedCallValue: Protocol.WatchRequest) =>
          server.watchAuthorized(
            nestedCallValue,
            "reader"
          )
      ).pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.tap(() => Deferred.succeed(firstWake, undefined)),
            Stream.runDrain
          )
        ),
        Effect.flatMap(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.await(firstWake)

      const submitted = yield* envelope("public", 1)
      const first = yield* server.submit(submitted)
      yield* server.maintain(spaceId)
      const retry = yield* server.submit(submitted).pipe(Effect.result)

      yield* Fiber.interrupt(watcher)
      assert.strictEqual(first._tag, "Accepted")
      pipe(Result.succeed(first), (nestedCallValue) => assert.deepStrictEqual(retry, nestedCallValue))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("stages a maximum-entry bootstrap page", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* putManyEnvelope(Protocol.maximumBootstrapEntries, 1))
      const required = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const page = yield* pipe(
        Protocol.BootstrapRequest.make({
          ...bootstrapRequest(required.manifest),
          limit: Protocol.maximumBootstrapEntries
        }),
        (nestedCallValue: Protocol.BootstrapRequest) =>
          server.bootstrapAuthorized(
            nestedCallValue,
            "reader"
          )
      )
      assert.strictEqual(page.entries.length, Protocol.maximumBootstrapEntries)
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(page))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("authorizes each bootstrap entity once across all pages", () =>
    Effect.gen(function*() {
      const entityChecks = yield* Ref.make(0)
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* putManyEnvelope(20, 1))
      const required = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* Ref.set(entityChecks, 0)

      let afterOrdinal = -1
      let hasMore = true
      while (hasMore) {
        const page = yield* pipe(
          Protocol.BootstrapRequest.make({
            ...bootstrapRequest(required.manifest),
            afterOrdinal,
            limit: 5
          }),
          (nestedCallValue: Protocol.BootstrapRequest) =>
            server.bootstrapAuthorized(
              nestedCallValue,
              "reader"
            )
        )
        afterOrdinal += page.entries.length
        hasMore = page.hasMore
      }

      assert.strictEqual(yield* Ref.get(entityChecks), 20)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("reuses a matching snapshot when an initial pull is retried", () =>
    Effect.gen(function*() {
      const entityChecks = yield* Ref.make(0)
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* putManyEnvelope(20, 1))
      const first = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in first)) {
        assert.fail("expected scoped bootstrap")
      }
      const checksAfterFirstPull = yield* Ref.get(entityChecks)

      const retry = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in retry)) {
        assert.fail("expected scoped bootstrap retry")
      }

      assert.strictEqual(retry.manifest.snapshotId, first.manifest.snapshotId)
      assert.strictEqual(yield* Ref.get(entityChecks), checksAfterFirstPull)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("denies the whole scope before disclosing a manifest", () =>
    Effect.gen(function*() {
      const server = yield* makeServer((input) => {
        if (input._tag === "Scope") {
          return pipe(TestAuthorizationError.make({ reason: "scope denied" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const denied = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader")).pipe(Effect.result)
      pipe(Result.isFailure(denied), (isFailure) =>
        assert.isTrue(isFailure))
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("advances the server watermark through an empty filtered page and fences stale scope generations", () =>
    Effect.gen(function*() {
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") {
          return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const initial = yield* pipe(
        pullRequest(),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "reader")
      )
      if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
      yield* pipe(
        bootstrapRequest(initial.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) => server.bootstrapAuthorized(nestedCallValue, "reader")
      )
      yield* server.submit(yield* envelope("hidden", 1))

      const empty = yield* pipe(
        pullRequest(initial.manifest.cursor),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "reader")
      )
      if ("_tag" in empty) assert.fail("expected incremental page")
      assert.deepStrictEqual(empty.changes, [])
      assert.strictEqual(empty.serverSequence, 1)

      const stale = yield* pipe(
        pullRequest(empty.cursor, scope, 0),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "reader")
      ).pipe(Effect.result)
      pipe(Result.isFailure(stale), (isFailure) => assert.isTrue(isFailure))
      if (Result.isFailure(stale)) assert.strictEqual(stale.failure._tag, "StaleReplicationScope")
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("evicts a retracted entity without letting pending replay restore it", () =>
    Effect.gen(function*() {
      let visible = true
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && !visible) {
          return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const required = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* pipe(bootstrapRequest(required.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(bootstrap))
      yield* local.installBootstrap(required.manifest)
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))

      yield* local.mutate(Domain.RenameTodo, { id: "public", title: "optimistic" })
      visible = false
      const revoked = yield* pipe(pullRequest(required.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in revoked) {
        assert.fail("expected retraction page")
      }
      yield* local.applyViewPage(revoked)
      pipe(Option.isNone(yield* local.get(Domain.Todo, "public")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects a final view page that regresses the durable server watermark", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const required = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* pipe(bootstrapRequest(required.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(bootstrap))
      yield* local.installBootstrap(required.manifest)
      const before = yield* local.replicationState
      assert.strictEqual(yield* local.cursor, 1)

      const changes: ReadonlyArray<Protocol.ViewChange> = []
      const invalid = Protocol.PullPage.make({
        scopeGeneration: before.scopeGeneration,
        cursor: Protocol.ReplicationCursor.make({
          viewId: before.cursor!.viewId,
          revision: Identity.ReplicationViewRevision.make(before.cursor!.revision + 1)
        }),
        serverSequence: Identity.ServerSequence.make(0),
        changes,
        contentBytes: Protocol.encodedBytes(changes),
        digest: yield* Protocol.viewChangesDigest(changes),
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      })
      const result = yield* local.applyViewPage(invalid).pipe(Effect.result)

      pipe(Result.isFailure(result), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "ProtocolInvalid")
      }
      assert.strictEqual(yield* local.cursor, 1)
      assert.deepStrictEqual(yield* local.replicationState, before)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("keeps a pending-only optimistic entity visible across bootstrap replacement", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* pipe(
        Domain.todo("pending"),
        (nestedCallValue: ReturnType<typeof Domain.todo>) => local.mutate(Domain.PutTodo, nestedCallValue)
      )

      const required = yield* pipe(
        pullRequest(),
        (nestedCallValue: Protocol.PullRequest) => server.pullAuthorized(nestedCallValue, "reader")
      )
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      const page = yield* pipe(
        bootstrapRequest(required.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) => server.bootstrapAuthorized(nestedCallValue, "reader")
      )
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(page))
      yield* local.installBootstrap(required.manifest)

      assert.strictEqual(yield* local.pendingCount, 1)
      pipe(
        Option.getOrThrow(yield* local.get(Domain.Todo, "pending")),
        (nestedCallValue1) =>
          pipe(
            Domain.todo("pending"),
            (nestedCallValue2: ReturnType<typeof Domain.todo>) =>
              assert.deepStrictEqual(nestedCallValue1, nestedCallValue2)
          )
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("installs a replacement bootstrap that retracts a previously visible entity", () =>
    Effect.gen(function*() {
      let visible = true
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && !visible) {
          return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected initial bootstrap")
      }
      const initialPage = yield* pipe(
        bootstrapRequest(initial.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) =>
          server.bootstrapAuthorized(nestedCallValue, "reader")
      )
      yield* local.prepareBootstrap(initial.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(initialPage))
      yield* local.installBootstrap(initial.manifest)
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) => assert.isTrue(nestedCallValue))

      visible = false
      const replacement = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in replacement)) {
        assert.fail("expected replacement bootstrap")
      }
      const replacementPage = yield* pipe(
        bootstrapRequest(replacement.manifest),
        (nestedCallValue: Protocol.BootstrapRequest) =>
          server.bootstrapAuthorized(
            nestedCallValue,
            "reader"
          )
      )
      yield* local.prepareBootstrap(replacementPage.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(replacementPage))
      yield* local.installBootstrap(replacementPage.manifest)

      pipe(Option.isNone(yield* local.get(Domain.Todo, "public")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("bounds replacement snapshots by the current visible entity set", () =>
    Effect.gen(function*() {
      const server = yield* makeServer(defaultAuthorizeRead, { maximumSnapshotEntities: 1 }).pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))

      yield* server.submit(yield* envelope("first", 1))
      const upsert = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in upsert) {
        assert.fail("expected upsert page")
      }
      yield* server.submit(yield* deleteEnvelope("first", 2))
      const deleted = yield* pipe(pullRequest(upsert.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in deleted) {
        assert.fail("expected delete page")
      }
      yield* server.submit(yield* envelope("second", 3))
      const current = yield* pipe(pullRequest(deleted.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in current) {
        assert.fail("expected current page")
      }

      const rotated = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in rotated)) {
        assert.fail("expected replacement bootstrap")
      }
      const page = yield* pipe(bootstrapRequest(rotated.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      assert.strictEqual(page.manifest.entityCount, 1)
      pipe(
        page.entries.map((entry) =>
          entry.change.entity.key
        ),
        (nestedCallValue) =>
          assert.deepStrictEqual(nestedCallValue, ["second"])
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rotates an outstanding page when its persisted value is no longer current", () =>
    Effect.gen(function*() {
      let allowedTitle = "base"
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
          return pipe(TestAuthorizationError.make({ reason: "not current" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1, "base"))
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))

      allowedTitle = "secret-page"
      yield* server.submit(yield* envelope("public", 2, "secret-page"))
      const secret = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in secret) {
        assert.fail("expected outstanding page")
      }

      allowedTitle = "public-current"
      yield* server.submit(yield* envelope("public", 3, "public-current"))
      const retried = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      assert.isTrue("_tag" in retried)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects a changed durable page payload before acknowledging its membership", () =>
    Effect.gen(function*() {
      const context = yield* ServerStore.layer({
        ...history,
        definition: Domain.definition,
        readAuthorizationRefreshInterval: "1 second",
        authorizeAccess: () => Effect.void,
        authorizeMutation: () => Effect.void,
        authorizeRead: () => Effect.void
      }).pipe(Layer.provide(runtime), Layer.provideMerge(database), Layer.build)
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const request = (cursor: Protocol.ReplicationCursor | null) =>
        Protocol.PullRequest.make({ ...pullRequest(cursor), limit: 1 })
      const initial = yield* pipe(request(null), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* envelope("a", 1))
      yield* server.submit(yield* envelope("b", 2))
      const delivered = yield* pipe(request(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in delivered) {
        assert.fail("expected incremental page")
      }
      pipe(
        delivered.changes.map((change) =>
          change.entity.key
        ),
        (nestedCallValue) =>
          assert.deepStrictEqual(nestedCallValue, ["a"])
      )

      const changedPayload = [Protocol.Upsert.make({
        entity: Protocol.EntityKey.make({ model: Domain.Todo.name, modelVersion: Domain.Todo.version, key: "b" }),
        value: Domain.todo("b")
      })]
      yield* sql`UPDATE effect_local_server_replication_pages
          SET changes_json = ${yield* Codec.stringify(changedPayload)}
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`

      const outcome = yield* pipe(request(delivered.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader")).pipe(Effect.result)
      pipe(Result.isFailure(outcome), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects a durable page watermark beyond the authoritative head", () =>
    Effect.gen(function*() {
      const context = yield* ServerStore.layer({
        ...history,
        definition: Domain.definition,
        readAuthorizationRefreshInterval: "1 second",
        authorizeAccess: () => Effect.void,
        authorizeMutation: () => Effect.void,
        authorizeRead: () => Effect.void
      }).pipe(Layer.provide(runtime), Layer.provideMerge(database), Layer.build)
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* envelope("a", 1))
      const delivered = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if ("_tag" in delivered) {
        assert.fail("expected incremental page")
      }
      yield* sql`UPDATE effect_local_server_replication_pages SET server_sequence = 100
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`

      const outcome = yield* pipe(pullRequest(initial.manifest.cursor), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader")).pipe(Effect.result)
      pipe(Result.isFailure(outcome), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects authoritative log metadata that conflicts with its entry", () =>
    Effect.gen(function*() {
      const context = yield* pipe(
        ServerStore.layer({
          ...history,
          definition: Domain.definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(Layer.provide(runtime), Layer.provideMerge(database)),
        (rebaseNestedCallValue96) =>
          Layer.build(
            rebaseNestedCallValue96
          )
      )
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const initial = yield* pipe(pullRequest(), (rebaseNestedCallValue97) =>
        server.pullAuthorized(rebaseNestedCallValue97, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }
      yield* pipe(bootstrapRequest(initial.manifest), (rebaseNestedCallValue98) =>
        server.bootstrapAuthorized(rebaseNestedCallValue98, "reader"))

      yield* server.submit(yield* envelope("corrupt-log", 1))
      yield* sql`UPDATE effect_local_authoritative_log SET client_id = ${readerId}
          WHERE space_id = ${spaceId} AND server_sequence = 1`

      const outcome = yield* pipe(pullRequest(initial.manifest.cursor), (rebaseNestedCallValue99) =>
        server.pullAuthorized(rebaseNestedCallValue99, "reader")).pipe(
          Effect.result
        )
      pipe(Result.isFailure(outcome), (rebaseNestedCallValue100) =>
        assert.isTrue(rebaseNestedCallValue100))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
      const pages = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_replication_pages WHERE space_id = ${spaceId} AND client_id = ${readerId}`
      assert.deepStrictEqual(pages, [{ count: 0 }])
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects corrupt server index catalog object names before cleanup", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/corrupt-index-catalog.sqlite`
      const persistentDatabase = () =>
        pipe(SqliteClient.layer({ filename, disableWAL: true }), (rebaseNestedCallValue101) =>
          Layer.mergeAll(
            rebaseNestedCallValue101,
            NodeCrypto.layer,
            Reactivity.layer,
            QueryReactivity.layer
          ))
      const serverLayer = () => {
        return ServerStore.layer({
          ...history,
          definition: Domain.definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(runtime),
          Layer.provideMerge(persistentDatabase())
        )
      }
      const context = yield* serverLayer().pipe(Layer.build)
      const sql = Context.get(context, SqlClient.SqlClient)
      yield* sql`CREATE TABLE unrelated_user_data (value INTEGER NOT NULL)`
      yield* sql`INSERT INTO unrelated_user_data VALUES (1)`
      yield* sql`INSERT INTO effect_local_server_index_catalog
          (model, index_name, descriptor_hash, table_name, scan_index_name)
          VALUES (${"obsolete"}, ${"obsolete"}, ${"0".repeat(16)},
            ${"unrelated_user_data"}, ${"unrelated_user_data_scan"})`

      const outcome = yield* serverLayer().pipe(Layer.build, Effect.result)
      pipe(Result.isFailure(outcome), (rebaseNestedCallValue105) => assert.isTrue(rebaseNestedCallValue105))
      if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      const rows = yield* sql<{ readonly value: number }>`SELECT value FROM unrelated_user_data`
      assert.deepStrictEqual(rows, [{ value: 1 }])
    }).pipe(Effect.provide(Layer.merge(NodeFileSystem.layer, NodeCrypto.layer))))

  it.effect("rotates a snapshot when its persisted value is no longer current", () =>
    Effect.gen(function*() {
      let allowedTitle = "secret-snapshot"
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
          return pipe(TestAuthorizationError.make({ reason: "not current" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1, "secret-snapshot"))
      const initial = yield* pipe(pullRequest(), (nestedCallValue: Protocol.PullRequest) =>
        server.pullAuthorized(nestedCallValue, "reader"))
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }

      allowedTitle = "public-current"
      yield* server.submit(yield* envelope("public", 2, "public-current"))
      const page = yield* pipe(bootstrapRequest(initial.manifest), (nestedCallValue: Protocol.BootstrapRequest) =>
        server.bootstrapAuthorized(nestedCallValue, "reader"))
      assert.notStrictEqual(page.manifest.snapshotId, initial.manifest.snapshotId)
      const change = page.entries[0]?.change
      if (change?._tag !== "Upsert") {
        assert.fail("expected current upsert")
      }
      pipe(titleOf(change.value), (nestedCallValue) =>
        assert.strictEqual(nestedCallValue, "public-current"))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("reconciles scope changes incrementally through the durable client view", () =>
    Effect.gen(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      const bootstrapCalls = yield* Ref.make(0)
      const remote = pipe(
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: (request) => server.discard(request, "reader"),
          submit: server.submit,
          pull: (request) => server.pullAuthorized(request, "reader"),
          bootstrap: (request) =>
            Ref.update(bootstrapCalls, (count) => count + 1).pipe(
              Effect.andThen(server.bootstrapAuthorized(request, "reader"))
            ),
          watch: (request) => server.watchAuthorized(request, "reader").pipe(Stream.unwrap)
        }),
        Layer.succeed(SyncEngine.SyncEngine)
      )
      const clientDatabase = database
      const localLayer = LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(Layer.provide(runtime), Layer.provide(clientDatabase))
      const reconcilerLayer = Reconciler.layerOnePass({
        definition: Domain.definition,
        spaceId
      }).pipe(Layer.provide(localLayer), Layer.provide(remote))
      const context = yield* Layer.mergeAll(localLayer, reconcilerLayer, clientDatabase).pipe(Layer.build)
      const local = Context.get(context, LocalStore.Store)
      const reconciler = Context.get(context, Reconciler.Reconciliation)

      yield* reconciler.sync
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) => assert.isTrue(nestedCallValue))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

      yield* pipe(Domain.todo("client"), (nestedCallValue: ReturnType<typeof Domain.todo>) =>
        local.mutate(Domain.PutTodo, nestedCallValue))
      yield* reconciler.sync
      assert.strictEqual(yield* local.pendingCount, 0)
      pipe(Option.isSome(yield* local.get(Domain.Todo, "client")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))

      yield* pipe(Protocol.ReplicationScope.make({ models: [] }), (nestedCallValue: Protocol.ReplicationScope) =>
        local.setScope(nestedCallValue))
      yield* reconciler.sync
      pipe(Option.isNone(yield* local.get(Domain.Todo, "public")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

      yield* local.setScope(scope)
      yield* reconciler.sync
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) =>
        assert.isTrue(nestedCallValue))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("keeps readable replicated state when only mutation submission is unauthorized", () =>
    Effect.gen(function*() {
      let accessAllowed = true
      let readAllowed = true
      const server = yield* ServerStore.layer({
        ...history,
        definition: Domain.definition,
        readAuthorizationRefreshInterval: "1 second",
        authorizeAccess: () => {
          if (accessAllowed) return Effect.void
          return pipe(TestAuthorizationError.make({ reason: "write denied" }), Effect.fail)
        },
        authorizeMutation: () => Effect.void,
        authorizeRead: () => {
          if (readAllowed) return Effect.void
          return pipe(TestAuthorizationError.make({ reason: "read denied" }), Effect.fail)
        }
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      const remote = pipe(
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: (request) => server.discard(request, "reader"),
          submit: (request) => server.admit(request, "reader"),
          pull: (request) => server.pullAuthorized(request, "reader"),
          bootstrap: (request) => server.bootstrapAuthorized(request, "reader"),
          watch: (request) => server.watchAuthorized(request, "reader").pipe(Stream.unwrap)
        }),
        Layer.succeed(SyncEngine.SyncEngine)
      )
      const reconciliationLayer = Reconciler.layerOnePass({ definition: Domain.definition, spaceId })
      const reconciliation = yield* reconciliationLayer.pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(remote),
        Layer.build,
        Effect.map(Context.get(Reconciler.Reconciliation))
      )
      yield* reconciliation.sync
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) => assert.isTrue(nestedCallValue))

      yield* pipe(
        Domain.todo("client"),
        (nestedCallValue: ReturnType<typeof Domain.todo>) => local.mutate(Domain.PutTodo, nestedCallValue)
      )
      accessAllowed = false
      const denied = yield* reconciliation.sync.pipe(Effect.result)
      pipe(Result.isFailure(denied), (isFailure) => assert.isTrue(isFailure))
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
      pipe(Option.isSome(yield* local.get(Domain.Todo, "public")), (nestedCallValue) => assert.isTrue(nestedCallValue))

      accessAllowed = true
      readAllowed = false
      const revoked = yield* reconciliation.sync.pipe(Effect.result)
      pipe(Result.isFailure(revoked), (isFailure) => assert.isTrue(isFailure))
      if (Result.isFailure(revoked)) assert.strictEqual(revoked.failure._tag, "AuthorizationDenied")
      pipe(Option.isNone(yield* local.get(Domain.Todo, "public")), (nestedCallValue) => assert.isTrue(nestedCallValue))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("fences a durable workflow created for an older scope generation", () =>
    Effect.gen(function*() {
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      const syncCalls = yield* Ref.make(0)
      const reconciliation = Reconciler.Reconciliation.of({
        sync: Ref.update(syncCalls, (count) => count + 1),
        failed: () => Effect.void,
        watchFailed: () => Effect.void,
        succeeded: Effect.void,
        status: Effect.succeed({ _tag: "Offline", pending: 0 })
      })
      const engine = yield* service(WorkflowEngine.WorkflowEngine, WorkflowEngine.layerMemory)
      const registrationLayer = ReconciliationWorkflow.layerRegistration({
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        retryDelay: "1 millis",
        maximumRetryDelay: "1 millis",
        maximumAttempts: 3
      })
      yield* registrationLayer.pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine)),
        Layer.build
      )
      const generation = yield* local.requestReconciliation
      const initial = yield* local.replicationState
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId: readerId,
        membershipIncarnation: local.membershipIncarnation,
        scope: initial.scope,
        scopeGeneration: initial.scopeGeneration,
        generation
      })
      yield* pipe(Protocol.ReplicationScope.make({ models: [] }), (nestedCallValue: Protocol.ReplicationScope) =>
        local.setScope(nestedCallValue))
      const outcome = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.result
      )
      pipe(Result.isFailure(outcome), (isFailure) =>
        assert.isTrue(isFailure))
      if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StaleReplicationScope")
      assert.strictEqual(yield* Ref.get(syncCalls), 0)
    }).pipe(Effect.provide(NodeCrypto.layer)))
})
