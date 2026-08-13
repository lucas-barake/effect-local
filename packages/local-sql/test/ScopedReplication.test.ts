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

const Database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.Layer
)
const Runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.Handlers))
const provideNodeCrypto = Effect.provide(NodeCrypto.layer)
const provideNode = Effect.provide(Layer.merge(NodeFileSystem.layer, NodeCrypto.layer))

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
  }).pipe(Layer.provide(Runtime), Layer.provide(Database))

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const titleOf = (value: unknown): unknown => {
  if (value === null || typeof value !== "object" || Array.isArray(value) || !("title" in value)) return undefined
  return value.title
}

const envelope = Effect.fnUntraced(function*(id: string, sequence: number, title = "first") {
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

const deleteEnvelope = Effect.fnUntraced(function*(id: string, sequence: number) {
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

const putManyEnvelope = Effect.fnUntraced(function*(count: number, sequence: number) {
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
  it.effect(
    "bootstraps only scoped entities visible to the principal",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      yield* server.submit(yield* envelope("private", 2))

      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const page = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      assert.isFalse(page.hasMore)
      const entityKeys = page.entries.map((entry) => entry.change).map((change) => change.entity.key)
      assert.deepStrictEqual(entityKeys, ["public"])
      assert.strictEqual(page.manifest.clientId, readerId)
      assert.strictEqual(page.manifest.entityCount, 1)
    }, provideNodeCrypto)
  )

  it.effect(
    "binds scoped manifests to the authenticated principal",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("private", 1))
      const reader = yield* server.pullAuthorized(pullRequest(), "reader")
      const owner = yield* server.pullAuthorized(pullRequest(), "owner")
      if (!("_tag" in reader) || !("_tag" in owner)) {
        assert.fail("expected scoped bootstraps")
      }
      assert.notStrictEqual(reader.manifest.snapshotId, owner.manifest.snapshotId)
      assert.strictEqual(reader.manifest.entityCount, 0)
      assert.strictEqual(owner.manifest.entityCount, 1)
    }, provideNodeCrypto)
  )

  it.effect(
    "does not disclose prior principal keys when a client changes principals",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("private", 1))
      const owner = yield* server.pullAuthorized(pullRequest(), "owner")
      if (!("_tag" in owner)) {
        assert.fail("expected owner bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(owner.manifest), "owner")

      const reader = yield* server.pullAuthorized(pullRequest(owner.manifest.cursor), "reader")
      if (!("_tag" in reader)) {
        assert.fail("expected principal rotation bootstrap")
      }
      const page = yield* server.bootstrapAuthorized(bootstrapRequest(reader.manifest), "reader")
      assert.deepStrictEqual(page.entries, [])
    }, provideNodeCrypto)
  )

  it.effect(
    "widens and narrows a scope through incremental pages",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      const emptyScope = Protocol.ReplicationScope.make({ models: [] })
      const initial = yield* server.pullAuthorized(pullRequest(null, emptyScope, 1), "owner")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrapRequestForEmptyScope = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(initial.manifest),
        scope: emptyScope
      })
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequestForEmptyScope, "owner")
      assert.deepStrictEqual(bootstrap.entries, [])

      const widened = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor, scope, 2), "owner")
      if ("_tag" in widened) assert.fail("scope widening must not bootstrap")
      assert.deepStrictEqual(widened.changes.map((change) => change._tag), ["Upsert"])

      const narrowed = yield* server.pullAuthorized(pullRequest(widened.cursor, emptyScope, 3), "owner")
      if ("_tag" in narrowed) {
        assert.fail("scope narrowing must not bootstrap")
      }
      assert.deepStrictEqual(narrowed.changes.map((change) => change._tag), ["Retract"])
    }, provideNodeCrypto)
  )

  it.effect(
    "replays an unacknowledged page and retracts authorization revocation",
    Effect.fnUntraced(function*() {
      let visible = true
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && !visible) {
          return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1))
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      yield* server.submit(yield* envelope("second", 2))

      const first = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      if ("_tag" in first) {
        assert.fail("expected incremental page")
      }
      const replay = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      assert.deepStrictEqual(replay, first)

      visible = false
      const rotated = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      assert.isTrue("_tag" in rotated)
      if (!("_tag" in rotated)) {
        assert.fail("unsafe outstanding page must rotate")
      }
      const revokedBootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(rotated.manifest), "reader")
      assert.deepStrictEqual(revokedBootstrap.entries, [])
    }, provideNodeCrypto)
  )

  it.effect(
    "authorizes only changed and acknowledged entities on a steady pull",
    Effect.fnUntraced(function*() {
      let entityAuthorizations = 0
      const server = yield* service(
        ServerStore.ServerStore,
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
        })
      )
      for (let index = 0; index < 28; index++) {
        yield* server.submit(yield* envelope(`hidden-${index}`, index + 1))
      }
      yield* server.submit(yield* envelope("visible-a", 29))
      yield* server.submit(yield* envelope("visible-b", 30))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      assert.isFalse(bootstrap.hasMore)
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* envelope("visible-a", 31, "renamed"))
      entityAuthorizations = 0
      const page = yield* server.pullAuthorized(pullRequest(acknowledged.cursor), "reader")
      if ("_tag" in page) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(page.changes.map((change) => change._tag), ["Upsert"])
      assert.isAtMost(entityAuthorizations, 10)
    }, provideNodeCrypto)
  )

  it.effect(
    "delivers a diff larger than the page limit completely across pulls",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      yield* server.submit(yield* envelope("seed", 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* putManyEnvelope(250, 2))
      let cursor = acknowledged.cursor
      const delivered = new Map<string, string>()
      const pageSizes: Array<number> = []
      for (let round = 0; round < 10; round++) {
        const page = yield* server.pullAuthorized(pullRequest(cursor), "reader")
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
    }, provideNodeCrypto)
  )

  it.effect(
    "delivers a widened scope larger than the page limit completely across pulls",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      const empty = Protocol.ReplicationScope.make({ models: [] })
      yield* server.submit(yield* putManyEnvelope(250, 1))
      const required = yield* server.pullAuthorized(pullRequest(null, empty, 1), "reader")
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      const bootstrapRequestForEmptyScope = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: empty
      })
      yield* server.bootstrapAuthorized(bootstrapRequestForEmptyScope, "reader")
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor, empty, 1), "reader")
      if ("_tag" in settled) assert.fail("expected steady page")
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor, empty, 1), "reader")
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      let cursor = acknowledged.cursor
      const delivered = new Set<string>()
      const pageSizes: Array<number> = []
      for (let round = 0; round < 10; round++) {
        const page = yield* server.pullAuthorized(pullRequest(cursor, scope, 2), "reader")
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
    }, provideNodeCrypto)
  )

  it.effect(
    "retracts an acknowledged entity when authorization is revoked without a change",
    Effect.fnUntraced(function*() {
      let secondVisible = true
      const server = yield* service(
        ServerStore.ServerStore,
        makeServer((input) => {
          if (input._tag === "Entity" && input.entity.key === "second" && !secondVisible) {
            return pipe(TestAuthorizationError.make({ reason: "revoked" }), Effect.fail)
          }
          return Effect.void
        })
      )
      yield* server.submit(yield* envelope("first", 1))
      yield* server.submit(yield* envelope("second", 2))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in settled) assert.fail("expected steady page")
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor), "reader")
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      secondVisible = false
      const revoked = yield* server.pullAuthorized(pullRequest(acknowledged.cursor), "reader")
      if ("_tag" in revoked) assert.fail("expected incremental page")
      assert.deepStrictEqual(
        revoked.changes.map((change) => [change._tag, change.entity.key]),
        [["Retract", "second"]]
      )
    }, provideNodeCrypto)
  )

  it.effect(
    "reveals an unchanged entity only after read authorization is invalidated",
    Effect.fnUntraced(function*() {
      let hiddenVisible = false
      const server = yield* service(
        ServerStore.ServerStore,
        makeServer((input) => {
          if (input._tag === "Entity" && input.entity.key === "hidden" && !hiddenVisible) {
            return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
          }
          return Effect.void
        })
      )
      yield* server.submit(yield* envelope("open", 1))
      yield* server.submit(yield* envelope("hidden", 2))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      hiddenVisible = true
      const unchanged = yield* server.pullAuthorized(pullRequest(acknowledged.cursor), "reader")
      if ("_tag" in unchanged) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(unchanged.changes, [])

      yield* server.invalidateReadAuthorization(spaceId)
      const revealed = yield* server.pullAuthorized(pullRequest(unchanged.cursor), "reader")
      if ("_tag" in revealed) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(
        revealed.changes.map((change) => [change._tag, change.entity.key]),
        [["Upsert", "hidden"]]
      )
    }, provideNodeCrypto)
  )

  it.effect(
    "falls back to a full derive when the log suffix is pruned",
    Effect.fnUntraced(function*() {
      const server = yield* service(
        ServerStore.ServerStore,
        makeServer(defaultAuthorizeRead, { retainedHistoryEntries: 0 })
      )
      yield* server.submit(yield* envelope("seed", 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* envelope("late-a", 2))
      yield* server.submit(yield* envelope("late-b", 3))
      yield* server.maintain(spaceId)

      const recovered = yield* server.pullAuthorized(pullRequest(acknowledged.cursor), "reader")
      if ("_tag" in recovered) {
        assert.fail("expected incremental page")
      }
      const labels: Array<string> = []
      for (const change of recovered.changes) {
        labels.push(`${change._tag}:${yield* Codec.stringify(change.entity.key)}`)
      }
      assert.deepStrictEqual(
        labels.toSorted((left, right) => left.localeCompare(right)),
        ["Upsert:\"late-a\"", "Upsert:\"late-b\""]
      )
    }, provideNodeCrypto)
  )

  it.effect(
    "replicates a bounded window per chat, pages older history on demand, and evicts it",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      const message = Effect.fnUntraced(function*(id: string, chatId: string, sentAt: number, sequence: number) {
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

      const defaultWindow = windowScope()
      const required = yield* server.pullAuthorized(pullRequest(null, defaultWindow, 1), "reader")
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      const bootstrapRequestForWindow = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: defaultWindow
      })
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequestForWindow, "reader")
      assert.isFalse(bootstrap.hasMore)
      const bootstrapKeys = bootstrap.entries.map((entry) => keyOf(entry.change))
        .toSorted((left, right) => left.localeCompare(right))
      assert.deepStrictEqual(bootstrapKeys, ["a-10", "a-8", "a-9", "b-3", "b-4", "b-5"])
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor, defaultWindow, 1), "reader")
      if ("_tag" in settled) assert.fail("expected steady page")
      assert.deepStrictEqual(settled.changes, [])
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor, defaultWindow, 1), "reader")
      if ("_tag" in acknowledged) assert.fail("expected acknowledged page")

      yield* server.submit(yield* message("a-11", "chat-a", 11, ++sequence))
      const slid = yield* server.pullAuthorized(pullRequest(acknowledged.cursor, defaultWindow, 1), "reader")
      if ("_tag" in slid) assert.fail("expected incremental page")
      assert.deepStrictEqual(keysOf(slid.changes, "Upsert"), ["a-11"])
      assert.deepStrictEqual(keysOf(slid.changes, "Retract"), ["a-8"])

      const widened = windowScope([
        Protocol.ReplicationWindowPartition.make({ key: ["chat-a"], count: 6 })
      ])
      const scrolled = yield* server.pullAuthorized(pullRequest(slid.cursor, widened, 2), "reader")
      if ("_tag" in scrolled) assert.fail("expected incremental page after widening")
      assert.deepStrictEqual(keysOf(scrolled.changes, "Upsert"), ["a-6", "a-7", "a-8"])
      assert.deepStrictEqual(keysOf(scrolled.changes, "Retract"), [])

      const evicted = yield* server.pullAuthorized(pullRequest(scrolled.cursor, defaultWindow, 3), "reader")
      if ("_tag" in evicted) assert.fail("expected incremental page after narrowing")
      assert.deepStrictEqual(keysOf(evicted.changes, "Upsert"), [])
      assert.deepStrictEqual(keysOf(evicted.changes, "Retract"), ["a-6", "a-7", "a-8"])
    }, provideNodeCrypto)
  )

  it.effect(
    "withholds unauthorized entities from a replication window",
    Effect.fnUntraced(function*() {
      let hiddenVisible = false
      const server = yield* service(
        ServerStore.ServerStore,
        makeServer((input) => {
          if (
            input._tag === "Entity" && typeof input.entity.key === "string" &&
            input.entity.key.startsWith("secret") && !hiddenVisible
          ) {
            return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
          }
          return Effect.void
        })
      )
      const message = Effect.fnUntraced(function*(id: string, chatId: string, sentAt: number, sequence: number) {
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
      const required = yield* server.pullAuthorized(pullRequest(null, windowed, 1), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrapRequestForWindow = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: windowed
      })
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequestForWindow, "reader")
      const visibleKeys = bootstrap.entries.map((entry) => {
        if (typeof entry.change.entity.key !== "string") {
          assert.fail("expected string message key")
        }
        return entry.change.entity.key
      }).toSorted((left, right) => left.localeCompare(right))
      assert.deepStrictEqual(visibleKeys, ["open-1", "open-3"])
      const settled = yield* server.pullAuthorized(pullRequest(required.manifest.cursor, windowed, 1), "reader")
      if ("_tag" in settled) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* server.pullAuthorized(pullRequest(settled.cursor, windowed, 1), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* message("secret-4", "chat-a", 4, 4))
      const withheld = yield* server.pullAuthorized(pullRequest(acknowledged.cursor, windowed, 1), "reader")
      if ("_tag" in withheld) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(
        withheld.changes.map((change) => [change._tag, change.entity.key]),
        [["Retract", "open-1"]]
      )
    }, provideNodeCrypto)
  )

  it.effect(
    "rotates a staged snapshot when an entity leaves its window",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      const message = Effect.fnUntraced(function*(id: string, sentAt: number) {
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
      const required = yield* server.pullAuthorized(pullRequest(null, windowed), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* message("m-2", 2))
      const bootstrapRequestForWindow = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: windowed
      })
      const page = yield* server.bootstrapAuthorized(bootstrapRequestForWindow, "reader")
      assert.deepStrictEqual(page.entries.map((entry) => entry.change.entity.key), ["m-2"])
      assert.notStrictEqual(page.manifest.snapshotId, required.manifest.snapshotId)
    }, provideNodeCrypto)
  )

  it.effect(
    "rotates an outstanding page when an upsert leaves its window",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      const message = Effect.fnUntraced(function*(id: string, sentAt: number) {
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
      const required = yield* server.pullAuthorized(pullRequest(null, windowed), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }
      const bootstrapRequestForWindow = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: windowed
      })
      yield* server.bootstrapAuthorized(bootstrapRequestForWindow, "reader")
      const steady = yield* server.pullAuthorized(pullRequest(required.manifest.cursor, windowed), "reader")
      if ("_tag" in steady) {
        assert.fail("expected steady page")
      }
      const acknowledged = yield* server.pullAuthorized(pullRequest(steady.cursor, windowed), "reader")
      if ("_tag" in acknowledged) {
        assert.fail("expected acknowledged page")
      }

      yield* server.submit(yield* message("m-1", 1))
      const outstanding = yield* server.pullAuthorized(pullRequest(acknowledged.cursor, windowed), "reader")
      if ("_tag" in outstanding) {
        assert.fail("expected outstanding page")
      }
      yield* server.submit(yield* message("m-2", 2))
      const replacement = yield* server.pullAuthorized(pullRequest(acknowledged.cursor, windowed), "reader")
      assert.isTrue("_tag" in replacement)
      if ("_tag" in replacement) {
        assert.strictEqual(replacement._tag, "BootstrapRequired")
      }
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects a replication window naming an inherited index property",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      yield* server.submit(yield* envelope("public", 1))
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [
          Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "constructor", count: 1 })
        ]
      })
      const outcome = yield* server.pullAuthorized(pullRequest(null, windowed, 1), "reader").pipe(Effect.result)
      if (outcome._tag !== "Failure") {
        assert.fail("expected a protocol rejection")
      }
      assert.strictEqual(outcome.failure._tag, "ProtocolInvalid")
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects a replication window bound whose type does not match the sort component",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
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
      const outcome = yield* server.pullAuthorized(pullRequest(null, windowed, 1), "reader").pipe(Effect.result)
      if (outcome._tag !== "Failure") assert.fail("expected a protocol rejection")
      assert.strictEqual(outcome.failure._tag, "ProtocolInvalid")
    }, provideNodeCrypto)
  )

  it.effect(
    "accepts the maximum bounded partition override set",
    Effect.fnUntraced(function*() {
      const server = yield* service(ServerStore.ServerStore, makeServer())
      const windowed = Protocol.ReplicationScope.make({
        models: [],
        windows: [
          Protocol.ReplicationWindow.make({
            model: Domain.Message.name,
            index: "byChat",
            count: 1,
            partitions: Array.from(
              { length: Protocol.maximumReplicationWindowPartitions },
              (_, index) => Protocol.ReplicationWindowPartition.make({ key: [`chat-${index}`] })
            )
          })
        ]
      })
      const result = yield* server.pullAuthorized(pullRequest(null, windowed, 1), "reader")
      assert.isTrue("_tag" in result)
      if ("_tag" in result) {
        assert.strictEqual(result._tag, "BootstrapRequired")
      }
    }, provideNodeCrypto)
  )

  it.effect(
    "replaces a windowed view when the index layout changes",
    Effect.fnUntraced(function*() {
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
        Layer.mergeAll(
          SqliteClient.layer({ filename, disableWAL: true }),
          NodeCrypto.layer,
          Reactivity.layer,
          QueryReactivity.Layer
        )
      const build = (definition: Definition.Any) => {
        return ServerStore.layer({
          ...history,
          definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(Domain.Handlers))),
          Layer.provideMerge(persistentDatabase()),
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
      }
      const message = Effect.fnUntraced(function*(id: string, sentAt: number, body: string) {
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
      const required = yield* first.pullAuthorized(pullRequest(null, windowed), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected bootstrap")
      }
      const bootstrapRequestForWindow = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        scope: windowed
      })
      const page = yield* first.bootstrapAuthorized(bootstrapRequestForWindow, "reader")
      assert.deepStrictEqual(page.entries.map((entry) => entry.change.entity.key), ["m-2", "m-3"])

      const second = yield* build(reorderedDefinition)
      const replacement = yield* second.pullAuthorized(
        pullRequest(required.manifest.cursor, windowed),
        "reader"
      )
      assert.isTrue("_tag" in replacement)
      if ("_tag" in replacement) {
        assert.strictEqual(replacement._tag, "BootstrapRequired")
        assert.notStrictEqual(replacement.manifest.cursor.viewId, required.manifest.cursor.viewId)
      }
    }, provideNode)
  )

  it.effect(
    "emits a periodic revocation hint",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const watchRequest = Protocol.WatchRequest.make({
        spaceId,
        clientId: readerId,
        schema: Domain.definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: initial.manifest.cursor
      })
      const watcher = yield* server.watchAuthorized(watchRequest, "reader").pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.runForEach((wake) => Queue.offer(wakes, wake))
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
    }, provideNodeCrypto)
  )

  it.effect(
    "does not disclose another principal's private mutations through watch wakes",
    Effect.fnUntraced(function*() {
      const readerChecks = yield* Ref.make(0)
      const firstReaderCheckStarted = yield* Deferred.make<void>()
      const releaseFirstReaderCheck = yield* Deferred.make<void>()
      const secondReaderCheckStarted = yield* Deferred.make<void>()
      const releaseSecondReaderCheck = yield* Deferred.make<void>()
      const wakeCount = yield* Ref.make(0)
      const firstWake = yield* Deferred.make<void>()
      const secondWake = yield* Deferred.make<void>()
      const authorizeRead = Effect.fnUntraced(function*(input: Parameters<ServerStore.Options["authorizeRead"]>[0]) {
        if (input._tag !== "Entity") return
        if (input.entity.key === "private" && input.principal === "reader") {
          const check = yield* Ref.updateAndGet(readerChecks, (count) => count + 1)
          if (check === 1) {
            yield* Deferred.succeed(firstReaderCheckStarted, undefined)
            yield* Deferred.await(releaseFirstReaderCheck)
            yield* pipe(TestAuthorizationError.make({ reason: "private" }), Effect.fail)
          }
          yield* Deferred.succeed(secondReaderCheckStarted, undefined)
          yield* Deferred.await(releaseSecondReaderCheck)
          yield* pipe(TestAuthorizationError.make({ reason: "private" }), Effect.fail)
        }
      })
      const server = yield* makeServer(authorizeRead).pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("private", 1))
      const owner = yield* server.pullAuthorized(pullRequest(), "owner")
      if (!("_tag" in owner)) assert.fail("expected owner bootstrap")
      yield* server.bootstrapAuthorized(bootstrapRequest(owner.manifest), "owner")

      const watchRequest = Protocol.WatchRequest.make({
        spaceId,
        clientId: readerId,
        schema: Domain.definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: owner.manifest.cursor
      })
      const watcher = yield* server.watchAuthorized(watchRequest, "reader").pipe(
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
    }, provideNodeCrypto)
  )

  it.effect(
    "wakes for a maximum-depth bulk mutation",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      const firstWake = yield* Deferred.make<void>()
      const watchRequest = Protocol.WatchRequest.make({
        spaceId,
        clientId: readerId,
        schema: Domain.definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: initial.manifest.cursor
      })
      const watcher = yield* server.watchAuthorized(watchRequest, "reader").pipe(
        Effect.map((stream) =>
          stream.pipe(
            Stream.tap(() => Deferred.succeed(firstWake, undefined)),
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
    }, provideNodeCrypto)
  )

  it.effect(
    "returns a retained accepted receipt while a watcher is active",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer(defaultAuthorizeRead, { retainedHistoryEntries: 0 }).pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const firstWake = yield* Deferred.make<void>()
      const watchRequest = Protocol.WatchRequest.make({
        spaceId,
        clientId: readerId,
        schema: Domain.definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null
      })
      const watcher = yield* server.watchAuthorized(watchRequest, "reader").pipe(
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
      assert.deepStrictEqual(retry, Result.succeed(first))
    }, provideNodeCrypto)
  )

  it.effect(
    "stages a maximum-entry bootstrap page",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* putManyEnvelope(Protocol.maximumBootstrapEntries, 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const maximumPageRequest = Protocol.BootstrapRequest.make({
        ...bootstrapRequest(required.manifest),
        limit: Protocol.maximumBootstrapEntries
      })
      const page = yield* server.bootstrapAuthorized(maximumPageRequest, "reader")
      assert.strictEqual(page.entries.length, Protocol.maximumBootstrapEntries)
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(page))
    }, provideNodeCrypto)
  )

  it.effect(
    "authorizes each bootstrap entity once across all pages",
    Effect.fnUntraced(function*() {
      const entityChecks = yield* Ref.make(0)
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* putManyEnvelope(20, 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* Ref.set(entityChecks, 0)

      let afterOrdinal = -1
      let hasMore = true
      while (hasMore) {
        const pageRequest = Protocol.BootstrapRequest.make({
          ...bootstrapRequest(required.manifest),
          afterOrdinal,
          limit: 5
        })
        const page = yield* server.bootstrapAuthorized(pageRequest, "reader")
        afterOrdinal += page.entries.length
        hasMore = page.hasMore
      }

      assert.strictEqual(yield* Ref.get(entityChecks), 20)
    }, provideNodeCrypto)
  )

  it.effect(
    "reuses a matching snapshot when an initial pull is retried",
    Effect.fnUntraced(function*() {
      const entityChecks = yield* Ref.make(0)
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* putManyEnvelope(20, 1))
      const first = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in first)) {
        assert.fail("expected scoped bootstrap")
      }
      const checksAfterFirstPull = yield* Ref.get(entityChecks)

      const retry = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in retry)) {
        assert.fail("expected scoped bootstrap retry")
      }

      assert.strictEqual(retry.manifest.snapshotId, first.manifest.snapshotId)
      assert.strictEqual(yield* Ref.get(entityChecks), checksAfterFirstPull)
    }, provideNodeCrypto)
  )

  it.effect(
    "denies the whole scope before disclosing a manifest",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer((input) => {
        if (input._tag === "Scope") {
          return pipe(TestAuthorizationError.make({ reason: "scope denied" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const denied = yield* server.pullAuthorized(pullRequest(), "reader").pipe(Effect.result)
      assert.isTrue(Result.isFailure(denied))
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
    }, provideNodeCrypto)
  )

  it.effect(
    "advances the server watermark through an empty filtered page and fences stale scope generations",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity") {
          return pipe(TestAuthorizationError.make({ reason: "hidden" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      yield* server.submit(yield* envelope("hidden", 1))

      const empty = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      if ("_tag" in empty) assert.fail("expected incremental page")
      assert.deepStrictEqual(empty.changes, [])
      assert.strictEqual(empty.serverSequence, 1)

      const stale = yield* server.pullAuthorized(pullRequest(empty.cursor, scope, 0), "reader").pipe(Effect.result)
      assert.isTrue(Result.isFailure(stale))
      if (Result.isFailure(stale)) assert.strictEqual(stale.failure._tag, "StaleReplicationScope")
    }, provideNodeCrypto)
  )

  it.effect(
    "evicts a retracted entity without letting pending replay restore it",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(bootstrap))
      yield* local.installBootstrap(required.manifest)
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))

      yield* local.mutate(Domain.RenameTodo, { id: "public", title: "optimistic" })
      visible = false
      const revoked = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
      if ("_tag" in revoked) {
        assert.fail("expected retraction page")
      }
      yield* local.applyViewPage(revoked)
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isNone))
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects a final view page that regresses the durable server watermark",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) {
        assert.fail("expected scoped bootstrap")
      }
      const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
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

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "ProtocolInvalid")
      }
      assert.strictEqual(yield* local.cursor, 1)
      assert.deepStrictEqual(yield* local.replicationState, before)
    }, provideNodeCrypto)
  )

  it.effect(
    "keeps a pending-only optimistic entity visible across bootstrap replacement",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* local.mutate(Domain.PutTodo, Domain.todo("pending"))

      const required = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
      const page = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
      yield* local.prepareBootstrap(required.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(page))
      yield* local.installBootstrap(required.manifest)

      assert.strictEqual(yield* local.pendingCount, 1)
      const pending = Option.getOrThrow(yield* local.get(Domain.Todo, "pending"))
      assert.deepStrictEqual(pending, Domain.todo("pending"))
    }, provideNodeCrypto)
  )

  it.effect(
    "installs a replacement bootstrap that retracts a previously visible entity",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      yield* server.submit(yield* envelope("public", 1))
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected initial bootstrap")
      }
      const initialPage = yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      yield* local.prepareBootstrap(initial.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(initialPage))
      yield* local.installBootstrap(initial.manifest)
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))

      visible = false
      const replacement = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in replacement)) {
        assert.fail("expected replacement bootstrap")
      }
      const replacementPage = yield* server.bootstrapAuthorized(
        bootstrapRequest(replacement.manifest),
        "reader"
      )
      yield* local.prepareBootstrap(replacementPage.manifest)
      assert.isTrue(yield* local.stageBootstrapPage(replacementPage))
      yield* local.installBootstrap(replacementPage.manifest)

      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isNone))
    }, provideNodeCrypto)
  )

  it.effect(
    "bounds replacement snapshots by the current visible entity set",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer(defaultAuthorizeRead, { maximumSnapshotEntities: 1 }).pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")

      yield* server.submit(yield* envelope("first", 1))
      const upsert = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      if ("_tag" in upsert) {
        assert.fail("expected upsert page")
      }
      yield* server.submit(yield* deleteEnvelope("first", 2))
      const deleted = yield* server.pullAuthorized(pullRequest(upsert.cursor), "reader")
      if ("_tag" in deleted) {
        assert.fail("expected delete page")
      }
      yield* server.submit(yield* envelope("second", 3))
      const current = yield* server.pullAuthorized(pullRequest(deleted.cursor), "reader")
      if ("_tag" in current) {
        assert.fail("expected current page")
      }

      const rotated = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in rotated)) {
        assert.fail("expected replacement bootstrap")
      }
      const page = yield* server.bootstrapAuthorized(bootstrapRequest(rotated.manifest), "reader")
      assert.strictEqual(page.manifest.entityCount, 1)
      assert.deepStrictEqual(page.entries.map((entry) => entry.change.entity.key), ["second"])
    }, provideNodeCrypto)
  )

  it.effect(
    "rotates an outstanding page when its persisted value is no longer current",
    Effect.fnUntraced(function*() {
      let allowedTitle = "base"
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
          return pipe(TestAuthorizationError.make({ reason: "not current" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1, "base"))
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")

      allowedTitle = "secret-page"
      yield* server.submit(yield* envelope("public", 2, "secret-page"))
      const secret = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      if ("_tag" in secret) {
        assert.fail("expected outstanding page")
      }

      allowedTitle = "public-current"
      yield* server.submit(yield* envelope("public", 3, "public-current"))
      const retried = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      assert.isTrue("_tag" in retried)
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects a changed durable page payload before acknowledging its membership",
    Effect.fnUntraced(function*() {
      const context = yield* ServerStore.layer({
        ...history,
        definition: Domain.definition,
        readAuthorizationRefreshInterval: "1 second",
        authorizeAccess: () => Effect.void,
        authorizeMutation: () => Effect.void,
        authorizeRead: () => Effect.void
      }).pipe(Layer.provide(Runtime), Layer.provideMerge(Database), Layer.build)
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const request = (cursor: Protocol.ReplicationCursor | null) =>
        Protocol.PullRequest.make({ ...pullRequest(cursor), limit: 1 })
      const initial = yield* server.pullAuthorized(request(null), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* envelope("a", 1))
      yield* server.submit(yield* envelope("b", 2))
      const delivered = yield* server.pullAuthorized(request(initial.manifest.cursor), "reader")
      if ("_tag" in delivered) {
        assert.fail("expected incremental page")
      }
      assert.deepStrictEqual(delivered.changes.map((change) => change.entity.key), ["a"])

      const changedPayload = [Protocol.Upsert.make({
        entity: Protocol.EntityKey.make({ model: Domain.Todo.name, modelVersion: Domain.Todo.version, key: "b" }),
        value: Domain.todo("b")
      })]
      yield* sql`UPDATE effect_local_server_replication_pages
          SET changes_json = ${yield* Codec.stringify(changedPayload)}
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`

      const outcome = yield* server.pullAuthorized(request(delivered.cursor), "reader").pipe(Effect.result)
      assert.isTrue(Result.isFailure(outcome))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects a durable page watermark beyond the authoritative head",
    Effect.fnUntraced(function*() {
      const context = yield* ServerStore.layer({
        ...history,
        definition: Domain.definition,
        readAuthorizationRefreshInterval: "1 second",
        authorizeAccess: () => Effect.void,
        authorizeMutation: () => Effect.void,
        authorizeRead: () => Effect.void
      }).pipe(Layer.provide(Runtime), Layer.provideMerge(Database), Layer.build)
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }

      yield* server.submit(yield* envelope("a", 1))
      const delivered = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
      if ("_tag" in delivered) {
        assert.fail("expected incremental page")
      }
      yield* sql`UPDATE effect_local_server_replication_pages SET server_sequence = 100
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`

      const outcome = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader").pipe(Effect.result)
      assert.isTrue(Result.isFailure(outcome))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects authoritative log metadata that conflicts with its entry",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(
        ServerStore.layer({
          ...history,
          definition: Domain.definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(Layer.provide(Runtime), Layer.provideMerge(Database))
      )
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected bootstrap")
      }
      yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")

      yield* server.submit(yield* envelope("corrupt-log", 1))
      yield* sql`UPDATE effect_local_authoritative_log SET client_id = ${readerId}
          WHERE space_id = ${spaceId} AND server_sequence = 1`

      const outcome = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader").pipe(
        Effect.result
      )
      assert.isTrue(Result.isFailure(outcome))
      if (Result.isFailure(outcome)) {
        assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }
      const pages = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_replication_pages WHERE space_id = ${spaceId} AND client_id = ${readerId}`
      assert.deepStrictEqual(pages, [{ count: 0 }])
    }, provideNodeCrypto)
  )

  it.effect(
    "rejects corrupt server index catalog object names before cleanup",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/corrupt-index-catalog.sqlite`
      const persistentDatabase = () =>
        Layer.mergeAll(
          SqliteClient.layer({ filename, disableWAL: true }),
          NodeCrypto.layer,
          Reactivity.layer,
          QueryReactivity.Layer
        )
      const serverLayer = () => {
        return ServerStore.layer({
          ...history,
          definition: Domain.definition,
          readAuthorizationRefreshInterval: "1 second",
          authorizeAccess: () => Effect.void,
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(Runtime),
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
      assert.isTrue(Result.isFailure(outcome))
      if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      const rows = yield* sql<{ readonly value: number }>`SELECT value FROM unrelated_user_data`
      assert.deepStrictEqual(rows, [{ value: 1 }])
    }, provideNode)
  )

  it.effect(
    "rotates a snapshot when its persisted value is no longer current",
    Effect.fnUntraced(function*() {
      let allowedTitle = "secret-snapshot"
      const server = yield* makeServer((input) => {
        if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
          return pipe(TestAuthorizationError.make({ reason: "not current" }), Effect.fail)
        }
        return Effect.void
      }).pipe(Layer.build, Effect.map(Context.get(ServerStore.ServerStore)))
      yield* server.submit(yield* envelope("public", 1, "secret-snapshot"))
      const initial = yield* server.pullAuthorized(pullRequest(), "reader")
      if (!("_tag" in initial)) {
        assert.fail("expected scoped bootstrap")
      }

      allowedTitle = "public-current"
      yield* server.submit(yield* envelope("public", 2, "public-current"))
      const page = yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
      assert.notStrictEqual(page.manifest.snapshotId, initial.manifest.snapshotId)
      const change = page.entries[0]?.change
      if (change?._tag !== "Upsert") {
        assert.fail("expected current upsert")
      }
      assert.strictEqual(titleOf(change.value), "public-current")
    }, provideNodeCrypto)
  )

  it.effect(
    "reconciles scope changes incrementally through the durable client view",
    Effect.fnUntraced(function*() {
      const server = yield* makeServer().pipe(
        Layer.build,
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      yield* server.submit(yield* envelope("public", 1))
      const bootstrapCalls = yield* Ref.make(0)
      const Remote = pipe(
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
      const ClientDatabase = Database
      const LocalLayer = LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(Layer.provide(Runtime), Layer.provide(ClientDatabase))
      const ReconcilerLayer = Reconciler.layerOnePass({
        definition: Domain.definition,
        spaceId
      }).pipe(Layer.provide(LocalLayer), Layer.provide(Remote))
      const context = yield* Layer.mergeAll(LocalLayer, ReconcilerLayer, ClientDatabase).pipe(Layer.build)
      const local = Context.get(context, LocalStore.Store)
      const reconciler = Context.get(context, Reconciler.Reconciliation)

      yield* reconciler.sync
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

      yield* local.mutate(Domain.PutTodo, Domain.todo("client"))
      yield* reconciler.sync
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "client"), Option.isSome))

      yield* local.setScope(Protocol.ReplicationScope.make({ models: [] }))
      yield* reconciler.sync
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isNone))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

      yield* local.setScope(scope)
      yield* reconciler.sync
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))
      assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)
    }, provideNodeCrypto)
  )

  it.effect(
    "keeps readable replicated state when only mutation submission is unauthorized",
    Effect.fnUntraced(function*() {
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
        Layer.provide(Runtime),
        Layer.provide(Database),
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
        Layer.provide(Runtime),
        Layer.provide(Database),
        Layer.build,
        Effect.map(Context.get(LocalStore.Store))
      )
      const Remote = pipe(
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
      const ReconciliationLayer = Reconciler.layerOnePass({ definition: Domain.definition, spaceId })
      const reconciliation = yield* ReconciliationLayer.pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Remote),
        Layer.build,
        Effect.map(Context.get(Reconciler.Reconciliation))
      )
      yield* reconciliation.sync
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))

      yield* local.mutate(Domain.PutTodo, Domain.todo("client"))
      accessAllowed = false
      const denied = yield* reconciliation.sync.pipe(Effect.result)
      assert.isTrue(Result.isFailure(denied))
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isSome))

      accessAllowed = true
      readAllowed = false
      const revoked = yield* reconciliation.sync.pipe(Effect.result)
      assert.isTrue(Result.isFailure(revoked))
      if (Result.isFailure(revoked)) assert.strictEqual(revoked.failure._tag, "AuthorizationDenied")
      assert.isTrue(pipe(yield* local.get(Domain.Todo, "public"), Option.isNone))
    }, provideNodeCrypto)
  )

  it.effect(
    "fences a durable workflow created for an older scope generation",
    Effect.fnUntraced(function*() {
      const local = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        scope
      }).pipe(
        Layer.provide(Runtime),
        Layer.provide(Database),
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
      const RegistrationLayer = ReconciliationWorkflow.layerRegistration({
        definition: Domain.definition,
        spaceId,
        clientId: readerId,
        retryDelay: "1 millis",
        maximumRetryDelay: "1 millis",
        maximumAttempts: 3
      })
      yield* RegistrationLayer.pipe(
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
      yield* local.setScope(Protocol.ReplicationScope.make({ models: [] }))
      const outcome = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.result
      )
      assert.isTrue(Result.isFailure(outcome))
      if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StaleReplicationScope")
      assert.strictEqual(yield* Ref.get(syncCalls), 0)
    }, provideNodeCrypto)
  )
})
