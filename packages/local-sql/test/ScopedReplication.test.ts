import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const history = {
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

const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)
const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const defaultAuthorizeRead: ServerStore.Options["authorizeRead"] = (input) => {
  if (input._tag === "Entity" && input.entity.key === "private" && input.principal !== "owner") {
    return Effect.fail("private")
  }
  return Effect.void
}

const makeServer = (authorizeRead: ServerStore.Options["authorizeRead"] = defaultAuthorizeRead) =>
  ServerStore.layer({
    ...history,
    definition: Domain.definition,
    readAuthorizationRefreshInterval: "1 second",
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead
  }).pipe(Layer.provide(runtime), Layer.provide(database))

const service = <I, S, E, R,>(tag: Context.Service<I, S>, layer: Layer.Layer<I, E, R>) =>
  Layer.build(layer).pipe(Effect.map((context) => Context.get(context, tag)))

const envelope = (id: string, sequence: number) =>
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
      payload: Domain.todo(id),
      digestVersion: 2 as const,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.PutTodo.version
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
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        yield* server.submit(yield* envelope("public", 1))
        yield* server.submit(yield* envelope("private", 2))

        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
        assert.isFalse(page.hasMore)
        assert.deepStrictEqual(
          page.entries.map((entry) => entry.change).map((change) => change.entity.key),
          ["public"]
        )
        assert.strictEqual(page.manifest.clientId, readerId)
        assert.strictEqual(page.manifest.entityCount, 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("binds scoped manifests to the authenticated principal", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        yield* server.submit(yield* envelope("private", 1))
        const reader = yield* server.pullAuthorized(pullRequest(), "reader")
        const owner = yield* server.pullAuthorized(pullRequest(), "owner")
        if (!("_tag" in reader) || !("_tag" in owner)) assert.fail("expected scoped bootstraps")
        assert.notStrictEqual(reader.manifest.snapshotId, owner.manifest.snapshotId)
        assert.strictEqual(reader.manifest.entityCount, 0)
        assert.strictEqual(owner.manifest.entityCount, 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("widens and narrows a scope through incremental pages", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        yield* server.submit(yield* envelope("public", 1))
        const emptyScope = Protocol.ReplicationScope.make({ models: [] })
        const initial = yield* server.pullAuthorized(pullRequest(null, emptyScope, 1), "owner")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        const bootstrap = yield* server.bootstrapAuthorized(
          Protocol.BootstrapRequest.make({
            ...bootstrapRequest(initial.manifest),
            scope: emptyScope
          }),
          "owner"
        )
        assert.deepStrictEqual(bootstrap.entries, [])

        const widened = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor, scope, 2), "owner")
        if ("_tag" in widened) assert.fail("scope widening must not bootstrap")
        assert.deepStrictEqual(widened.changes.map((change) => change._tag), ["Upsert"])

        const narrowed = yield* server.pullAuthorized(pullRequest(widened.cursor, emptyScope, 3), "owner")
        if ("_tag" in narrowed) assert.fail("scope narrowing must not bootstrap")
        assert.deepStrictEqual(narrowed.changes.map((change) => change._tag), ["Retract"])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("replays an unacknowledged page and retracts authorization revocation", () =>
    Effect.scoped(
      Effect.gen(function*() {
        let visible = true
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity" && !visible) return Effect.fail("revoked")
            return Effect.void
          })
        )
        yield* server.submit(yield* envelope("public", 1))
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        yield* server.submit(yield* envelope("second", 2))

        const first = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        if ("_tag" in first) assert.fail("expected incremental page")
        const replay = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        assert.deepStrictEqual(replay, first)

        visible = false
        const rotated = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        assert.isTrue("_tag" in rotated)
        if (!("_tag" in rotated)) assert.fail("unsafe outstanding page must rotate")
        const revokedBootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(rotated.manifest), "reader")
        assert.deepStrictEqual(
          revokedBootstrap.entries.map((entry) => entry.change._tag),
          ["Retract"]
        )
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("does not wake for hidden mutations and emits a periodic revocation hint", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        const firstWake = yield* Deferred.make<void>()
        const watcher = yield* server.watchAuthorized(
          Protocol.WatchRequest.make({
            spaceId,
            clientId: readerId,
            schema: Domain.definition.schemaIdentity,
            scope,
            scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
            cursor: initial.manifest.cursor
          }),
          "reader"
        ).pipe(
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
        yield* server.submit(yield* envelope("private", 1))
        yield* Effect.yieldNow
        assert.isUndefined(watcher.pollUnsafe())

        yield* TestClock.adjust("1 second")
        const wakes = yield* Fiber.join(watcher)
        assert.strictEqual(wakes.length, 2)
        assert.deepStrictEqual(wakes[0], { spaceId })
        assert.deepStrictEqual(wakes[1], { spaceId })
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("denies the whole scope before disclosing a manifest", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Scope") return Effect.fail("scope denied")
            return Effect.void
          })
        )
        const denied = yield* server.pullAuthorized(pullRequest(), "reader").pipe(Effect.flip)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("advances the server watermark through an empty filtered page and fences stale scope generations", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity") return Effect.fail("hidden")
            return Effect.void
          })
        )
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        yield* server.submit(yield* envelope("hidden", 1))

        const empty = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        if ("_tag" in empty) assert.fail("expected incremental page")
        assert.deepStrictEqual(empty.changes, [])
        assert.strictEqual(empty.serverSequence, 1)

        const stale = yield* server.pullAuthorized(pullRequest(empty.cursor, scope, 0), "reader").pipe(Effect.flip)
        assert.strictEqual(stale._tag, "StaleReplicationScope")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))
})
