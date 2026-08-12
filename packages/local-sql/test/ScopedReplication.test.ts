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
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
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

const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)
const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const defaultAuthorizeRead: ServerStore.Options["authorizeRead"] = (input) => {
  if (input._tag === "Entity" && input.entity.key === "private" && input.principal !== "owner") {
    return Effect.fail("private")
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

const service = <I, S, E, R,>(tag: Context.Service<I, S>, layer: Layer.Layer<I, E, R>) =>
  Layer.build(layer).pipe(Effect.map((context) => Context.get(context, tag)))

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

  it.effect("does not disclose prior principal keys when a client changes principals", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        yield* server.submit(yield* envelope("private", 1))
        const owner = yield* server.pullAuthorized(pullRequest(), "owner")
        if (!("_tag" in owner)) assert.fail("expected owner bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(owner.manifest), "owner")

        const reader = yield* server.pullAuthorized(pullRequest(owner.manifest.cursor), "reader")
        if (!("_tag" in reader)) assert.fail("expected principal rotation bootstrap")
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(reader.manifest), "reader")
        assert.deepStrictEqual(page.entries, [])
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
        assert.deepStrictEqual(revokedBootstrap.entries, [])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("emits a periodic revocation hint", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        const wakes = yield* Queue.unbounded<Protocol.Wake>()
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
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("does not disclose another principal's private mutations through watch wakes", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const readerChecks = yield* Ref.make(0)
        const firstReaderCheckStarted = yield* Deferred.make<void>()
        const releaseFirstReaderCheck = yield* Deferred.make<void>()
        const secondReaderCheckStarted = yield* Deferred.make<void>()
        const releaseSecondReaderCheck = yield* Deferred.make<void>()
        const wakeCount = yield* Ref.make(0)
        const firstWake = yield* Deferred.make<void>()
        const secondWake = yield* Deferred.make<void>()
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag !== "Entity") return Effect.void
            if (input.entity.key === "private" && input.principal === "reader") {
              return Effect.gen(function*() {
                const check = yield* Ref.updateAndGet(readerChecks, (count) => count + 1)
                if (check === 1) {
                  yield* Deferred.succeed(firstReaderCheckStarted, undefined)
                  yield* Deferred.await(releaseFirstReaderCheck)
                  return yield* Effect.fail("private")
                }
                yield* Deferred.succeed(secondReaderCheckStarted, undefined)
                yield* Deferred.await(releaseSecondReaderCheck)
                return yield* Effect.fail("private")
              })
            }
            return Effect.void
          })
        )
        yield* server.submit(yield* envelope("private", 1))
        const owner = yield* server.pullAuthorized(pullRequest(), "owner")
        if (!("_tag" in owner)) assert.fail("expected owner bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(owner.manifest), "owner")

        const watcher = yield* server.watchAuthorized(
          Protocol.WatchRequest.make({
            spaceId,
            clientId: readerId,
            schema: Domain.definition.schemaIdentity,
            scope,
            scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
            cursor: owner.manifest.cursor
          }),
          "reader"
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
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("wakes for a maximum-depth bulk mutation", () =>
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
        yield* server.submit(yield* putManyEnvelope(999, 1))
        const wakes = yield* Fiber.join(watcher)
        assert.strictEqual(wakes.length, 2)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("returns a retained accepted receipt while a watcher is active", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer(defaultAuthorizeRead, { retainedHistoryEntries: 0 })
        )
        const firstWake = yield* Deferred.make<void>()
        const watcher = yield* server.watchAuthorized(
          Protocol.WatchRequest.make({
            spaceId,
            clientId: readerId,
            schema: Domain.definition.schemaIdentity,
            scope,
            scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
            cursor: null
          }),
          "reader"
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
        assert.deepStrictEqual(retry, Result.succeed(first))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("stages a maximum-entry bootstrap page", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* server.submit(yield* putManyEnvelope(Protocol.maximumBootstrapEntries, 1))
        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        const page = yield* server.bootstrapAuthorized(
          Protocol.BootstrapRequest.make({
            ...bootstrapRequest(required.manifest),
            limit: Protocol.maximumBootstrapEntries
          }),
          "reader"
        )
        assert.strictEqual(page.entries.length, Protocol.maximumBootstrapEntries)
        yield* local.prepareBootstrap(required.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("authorizes each bootstrap entity once across all pages", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const entityChecks = yield* Ref.make(0)
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
            return Effect.void
          })
        )
        yield* server.submit(yield* putManyEnvelope(20, 1))
        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        yield* Ref.set(entityChecks, 0)

        let afterOrdinal = -1
        let hasMore = true
        while (hasMore) {
          const page = yield* server.bootstrapAuthorized(
            Protocol.BootstrapRequest.make({
              ...bootstrapRequest(required.manifest),
              afterOrdinal,
              limit: 5
            }),
            "reader"
          )
          afterOrdinal += page.entries.length
          hasMore = page.hasMore
        }

        assert.strictEqual(yield* Ref.get(entityChecks), 20)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("reuses a matching snapshot when an initial pull is retried", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const entityChecks = yield* Ref.make(0)
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity") return Ref.update(entityChecks, (count) => count + 1)
            return Effect.void
          })
        )
        yield* server.submit(yield* putManyEnvelope(20, 1))
        const first = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in first)) assert.fail("expected scoped bootstrap")
        const checksAfterFirstPull = yield* Ref.get(entityChecks)

        const retry = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in retry)) assert.fail("expected scoped bootstrap retry")

        assert.strictEqual(retry.manifest.snapshotId, first.manifest.snapshotId)
        assert.strictEqual(yield* Ref.get(entityChecks), checksAfterFirstPull)
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

  it.effect("evicts a retracted entity without letting pending replay restore it", () =>
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
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* server.submit(yield* envelope("public", 1))
        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        const bootstrap = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
        yield* local.prepareBootstrap(required.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(bootstrap))
        yield* local.installBootstrap(required.manifest)
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))

        yield* local.mutate(Domain.RenameTodo, { id: "public", title: "optimistic" })
        visible = false
        const revoked = yield* server.pullAuthorized(pullRequest(required.manifest.cursor), "reader")
        if ("_tag" in revoked) assert.fail("expected retraction page")
        yield* local.applyViewPage(revoked)
        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "public")))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a final view page that regresses the durable server watermark", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* server.submit(yield* envelope("public", 1))
        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
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
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.cursor, 1)
        assert.deepStrictEqual(yield* local.replicationState, before)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("keeps a pending-only optimistic entity visible across bootstrap replacement", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* local.mutate(Domain.PutTodo, Domain.todo("pending"))

        const required = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in required)) assert.fail("expected scoped bootstrap")
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(required.manifest), "reader")
        yield* local.prepareBootstrap(required.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
        yield* local.installBootstrap(required.manifest)

        assert.strictEqual(yield* local.pendingCount, 1)
        assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "pending")), Domain.todo("pending"))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("installs a replacement bootstrap that retracts a previously visible entity", () =>
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
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* server.submit(yield* envelope("public", 1))
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected initial bootstrap")
        const initialPage = yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        yield* local.prepareBootstrap(initial.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(initialPage))
        yield* local.installBootstrap(initial.manifest)
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))

        visible = false
        const replacement = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in replacement)) assert.fail("expected replacement bootstrap")
        const replacementPage = yield* server.bootstrapAuthorized(
          bootstrapRequest(replacement.manifest),
          "reader"
        )
        yield* local.prepareBootstrap(replacementPage.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(replacementPage))
        yield* local.installBootstrap(replacementPage.manifest)

        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "public")))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("bounds replacement snapshots by the current visible entity set", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer(defaultAuthorizeRead, { maximumSnapshotEntities: 1 })
        )
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")

        yield* server.submit(yield* envelope("first", 1))
        const upsert = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        if ("_tag" in upsert) assert.fail("expected upsert page")
        yield* server.submit(yield* deleteEnvelope("first", 2))
        const deleted = yield* server.pullAuthorized(pullRequest(upsert.cursor), "reader")
        if ("_tag" in deleted) assert.fail("expected delete page")
        yield* server.submit(yield* envelope("second", 3))
        const current = yield* server.pullAuthorized(pullRequest(deleted.cursor), "reader")
        if ("_tag" in current) assert.fail("expected current page")

        const rotated = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in rotated)) assert.fail("expected replacement bootstrap")
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(rotated.manifest), "reader")
        assert.strictEqual(page.manifest.entityCount, 1)
        assert.deepStrictEqual(page.entries.map((entry) => entry.change.entity.key), ["second"])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rotates an outstanding page when its persisted value is no longer current", () =>
    Effect.scoped(
      Effect.gen(function*() {
        let allowedTitle = "base"
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
              return Effect.fail("not current")
            }
            return Effect.void
          })
        )
        yield* server.submit(yield* envelope("public", 1, "base"))
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")

        allowedTitle = "secret-page"
        yield* server.submit(yield* envelope("public", 2, "secret-page"))
        const secret = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        if ("_tag" in secret) assert.fail("expected outstanding page")

        allowedTitle = "public-current"
        yield* server.submit(yield* envelope("public", 3, "public-current"))
        const retried = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        assert.isTrue("_tag" in retried)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a changed durable page payload before acknowledging its membership", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const context = yield* Layer.build(
          ServerStore.layer({
            ...history,
            definition: Domain.definition,
            readAuthorizationRefreshInterval: "1 second",
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () => Effect.void
          }).pipe(Layer.provide(runtime), Layer.provideMerge(database))
        )
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const request = (cursor: Protocol.ReplicationCursor | null) =>
          Protocol.PullRequest.make({ ...pullRequest(cursor), limit: 1 })
        const initial = yield* server.pullAuthorized(request(null), "reader")
        if (!("_tag" in initial)) assert.fail("expected bootstrap")

        yield* server.submit(yield* envelope("a", 1))
        yield* server.submit(yield* envelope("b", 2))
        const delivered = yield* server.pullAuthorized(request(initial.manifest.cursor), "reader")
        if ("_tag" in delivered) assert.fail("expected incremental page")
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
        if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a durable page watermark beyond the authoritative head", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const context = yield* Layer.build(
          ServerStore.layer({
            ...history,
            definition: Domain.definition,
            readAuthorizationRefreshInterval: "1 second",
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.void,
            authorizeRead: () => Effect.void
          }).pipe(Layer.provide(runtime), Layer.provideMerge(database))
        )
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected bootstrap")

        yield* server.submit(yield* envelope("a", 1))
        const delivered = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader")
        if ("_tag" in delivered) assert.fail("expected incremental page")
        yield* sql`UPDATE effect_local_server_replication_pages SET server_sequence = 100
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`

        const outcome = yield* server.pullAuthorized(pullRequest(initial.manifest.cursor), "reader").pipe(Effect.result)
        assert.isTrue(Result.isFailure(outcome))
        if (Result.isFailure(outcome)) assert.strictEqual(outcome.failure._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rotates a snapshot when its persisted value is no longer current", () =>
    Effect.scoped(
      Effect.gen(function*() {
        let allowedTitle = "secret-snapshot"
        const server = yield* service(
          ServerStore.ServerStore,
          makeServer((input) => {
            if (input._tag === "Entity" && titleOf(input.value) !== allowedTitle) {
              return Effect.fail("not current")
            }
            return Effect.void
          })
        )
        yield* server.submit(yield* envelope("public", 1, "secret-snapshot"))
        const initial = yield* server.pullAuthorized(pullRequest(), "reader")
        if (!("_tag" in initial)) assert.fail("expected scoped bootstrap")

        allowedTitle = "public-current"
        yield* server.submit(yield* envelope("public", 2, "public-current"))
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(initial.manifest), "reader")
        assert.notStrictEqual(page.manifest.snapshotId, initial.manifest.snapshotId)
        const change = page.entries[0]?.change
        if (change?._tag !== "Upsert") assert.fail("expected current upsert")
        assert.strictEqual(titleOf(change.value), "public-current")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("reconciles scope changes incrementally through the durable client view", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, makeServer())
        yield* server.submit(yield* envelope("public", 1))
        const bootstrapCalls = yield* Ref.make(0)
        const remote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: (request) => server.discard(request, "reader"),
            submit: server.submit,
            pull: (request) => server.pullAuthorized(request, "reader"),
            bootstrap: (request) =>
              Ref.update(bootstrapCalls, (count) => count + 1).pipe(
                Effect.andThen(server.bootstrapAuthorized(request, "reader"))
              ),
            watch: (request) => Stream.unwrap(server.watchAuthorized(request, "reader"))
          })
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
        const context = yield* Layer.build(Layer.mergeAll(localLayer, reconcilerLayer, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const reconciler = Context.get(context, Reconciler.Reconciliation)

        yield* reconciler.sync
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))
        assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

        yield* local.mutate(Domain.PutTodo, Domain.todo("client"))
        yield* reconciler.sync
        assert.strictEqual(yield* local.pendingCount, 0)
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "client")))

        yield* local.setScope(Protocol.ReplicationScope.make({ models: [] }))
        yield* reconciler.sync
        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "public")))
        assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)

        yield* local.setScope(scope)
        yield* reconciler.sync
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))
        assert.strictEqual(yield* Ref.get(bootstrapCalls), 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("keeps readable replicated state when only mutation submission is unauthorized", () =>
    Effect.scoped(
      Effect.gen(function*() {
        let accessAllowed = true
        let readAllowed = true
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layer({
            ...history,
            definition: Domain.definition,
            readAuthorizationRefreshInterval: "1 second",
            authorizeAccess: () => {
              if (accessAllowed) return Effect.void
              return Effect.fail("write denied")
            },
            authorizeMutation: () => Effect.void,
            authorizeRead: () => {
              if (readAllowed) return Effect.void
              return Effect.fail("read denied")
            }
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        yield* server.submit(yield* envelope("public", 1))
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
        )
        const remote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            discard: (request) => server.discard(request, "reader"),
            submit: (request) => server.admit(request, "reader"),
            pull: (request) => server.pullAuthorized(request, "reader"),
            bootstrap: (request) => server.bootstrapAuthorized(request, "reader"),
            watch: (request) => Stream.unwrap(server.watchAuthorized(request, "reader"))
          })
        )
        const reconciliation = yield* service(
          Reconciler.Reconciliation,
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(remote)
          )
        )
        yield* reconciliation.sync
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))

        yield* local.mutate(Domain.PutTodo, Domain.todo("client"))
        accessAllowed = false
        const denied = yield* reconciliation.sync.pipe(Effect.flip)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
        assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "public")))

        accessAllowed = true
        readAllowed = false
        const revoked = yield* reconciliation.sync.pipe(Effect.flip)
        assert.strictEqual(revoked._tag, "AuthorizationDenied")
        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "public")))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("fences a durable workflow created for an older scope generation", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const local = yield* service(
          LocalStore.Store,
          LocalStore.layer({
            ...clientHistory,
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            scope
          }).pipe(Layer.provide(runtime), Layer.provide(database))
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
        yield* Layer.build(
          ReconciliationWorkflow.layerRegistration({
            definition: Domain.definition,
            spaceId,
            clientId: readerId,
            retryDelay: "1 millis",
            maximumRetryDelay: "1 millis",
            maximumAttempts: 3
          }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
            Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine))
          )
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
        const error = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
          Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
          Effect.flip
        )
        assert.strictEqual(error._tag, "StaleReplicationScope")
        assert.strictEqual(yield* Ref.get(syncCalls), 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))
})
