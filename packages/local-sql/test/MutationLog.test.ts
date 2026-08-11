import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Rows from "../src/internal/rows.js"
import * as LocalStore from "../src/LocalStore.js"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const putTodoProvenance = {
  name: Domain.PutTodo.name,
  sourceSchema: Domain.definition.schemaIdentity,
  mutationVersion: Domain.PutTodo.version
}

const envelope = (
  name: string,
  payload: Schema.Json,
  localSequence: number,
  mutationId: Identity.MutationId
) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId,
      mutationId,
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name,
      payload,
      digestVersion: 2 as const,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.definition.mutationByName.get(name)?.version ?? Identity.SchemaVersion.make(1)
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )

const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const clientHistory = {
  scope,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}
const serverHistory = {
  readAuthorizationRefreshInterval: "1 second" as const,
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

const localLayer = (id = clientId) =>
  LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId: id }).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )

const serverLayer = (
  authorizeMutation?: (input: {
    readonly mutation: MutationRuntime.CurrentMutationView
    readonly principal: Schema.Json
  }) => Effect.Effect<void, Schema.Json>
) => {
  let layer = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition })
  if (authorizeMutation !== undefined) {
    layer = ServerStore.layer({
      definition: Domain.definition,
      ...serverHistory,
      authorizeAccess: () => Effect.void,
      authorizeMutation,
      authorizeRead: () => Effect.void
    })
  }
  return layer.pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )
}

const service = <I, S, E, R,>(tag: Context.Service<I, S>, layer: Layer.Layer<I, E, R>) =>
  Layer.build(layer).pipe(Effect.map((context) => Context.get(context, tag)))

const directSync = (server: ServerStore.Service) =>
  Layer.succeed(
    SyncEngine.SyncEngine,
    SyncEngine.SyncEngine.of({
      submit: server.submit,
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })
  )

const incremental = (result: Protocol.PullResult): Protocol.PullPage => {
  if ("_tag" in result) assert.fail(`Unexpected bootstrap ${result.manifest.snapshotId}`)
  return result
}

const pullRequest = (
  cursor: Protocol.ReplicationCursor | null = null,
  limit = 10,
  requestedClientId = clientId,
  requestedScope = scope
): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: requestedClientId,
    schema: Domain.definition.schemaIdentity,
    scope: requestedScope,
    scopeGeneration,
    cursor,
    limit
  })

const bootstrapRequest = (
  manifest: Protocol.SnapshotManifest,
  afterOrdinal = -1,
  limit = 10,
  requestedScope = scope
): Protocol.BootstrapRequest =>
  Protocol.BootstrapRequest.make({
    spaceId,
    clientId: manifest.clientId,
    schema: Domain.definition.schemaIdentity,
    scope: requestedScope,
    scopeGeneration: manifest.scopeGeneration,
    cursor: manifest.cursor,
    snapshotId: manifest.snapshotId,
    afterOrdinal,
    limit
  })

const installFreshView = (
  local: LocalStore.Service,
  server: ServerStore.Service,
  requestedClientId = clientId,
  requestedScope = scope
) =>
  Effect.gen(function*() {
    const required = yield* server.pull(pullRequest(null, 10, requestedClientId, requestedScope))
    if (!("_tag" in required)) assert.fail("expected bootstrap")
    const page = yield* server.bootstrap(bootstrapRequest(required.manifest, -1, 10, requestedScope))
    yield* local.prepareBootstrap(page.manifest)
    assert.isTrue(yield* local.stageBootstrapPage(page))
    yield* local.installBootstrap(page.manifest)
  })

const authoritativeRows = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Identity.SpaceId,
    Result: Rows.ServerLogRow,
    execute: (requestedSpaceId) =>
      sql`SELECT space_id, server_sequence, client_id, local_sequence, mutation_id, digest,
        entry_bytes, entry_json, source_schema_version, source_schema_hash
        FROM effect_local_authoritative_log WHERE space_id = ${requestedSpaceId}
        ORDER BY server_sequence`
  })(spaceId)

const authoritativeLog = (sql: SqlClient.SqlClient) =>
  authoritativeRows(sql).pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) =>
        Schema.decodeUnknownEffect(Schema.fromJsonString(Protocol.AcceptedMutation))(row.entry_json))
    )
  )

const acceptedMutation = (
  pending: Protocol.PendingMutation,
  receipt: Protocol.AcceptedReceipt
): Protocol.AcceptedMutation =>
  Protocol.AcceptedMutation.make({
    sequence: receipt.serverSequence,
    spaceId,
    clientId,
    mutationId: pending.envelope.mutationId,
    localSequence: pending.envelope.localSequence,
    sourceSchema: pending.envelope.sourceSchema,
    digest: pending.envelope.digest,
    changes: pending.changes
  })

const clientServices = (id: Identity.ClientId, server: ServerStore.Service) => {
  const local = localLayer(id)
  const reconciler = Reconciler.layer({ definition: Domain.definition, spaceId, retryDelay: "10 millis" }).pipe(
    Layer.provide(local),
    Layer.provide(directSync(server))
  )
  return Layer.merge(local, reconciler)
}

describe("server reconciled mutation log", () => {
  it.effect("falls forward to a covering snapshot when an expired receipt snapshot is retired", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8,
          retainedSnapshots: 1
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const first = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("retired-snapshot-1"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8010-000000000001")
        )
        yield* server.submit(first)
        yield* server.maintain(spaceId)
        const expired = yield* server.submit(first)
        if (expired._tag !== "Expired") assert.fail("expected expired receipt")

        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("retired-snapshot-2"),
            2,
            Identity.MutationId.make("mut_00000000-0000-4000-8010-000000000002")
          )
        )
        yield* server.maintain(spaceId)

        const required = yield* server.pull(pullRequest())
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(Protocol.BootstrapRequest.make({
          ...bootstrapRequest(required.manifest),
          snapshotId: expired.snapshotId
        }))
        assert.notStrictEqual(page.manifest.snapshotId, expired.snapshotId)
        assert.isAtLeast(page.manifest.sequence, expired.snapshotSequence)
        assert.isAtLeast(page.manifest.terminalSequenceThrough, expired.terminalSequenceThrough)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("publishes a snapshot before bounding history and receipts", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 1,
          maximumHistoryEntries: 4,
          retainedReceipts: 1,
          maximumReceipts: 4
        }
        const live = ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted: Array<Protocol.MutationEnvelope> = []
        for (let sequence = 1; sequence <= 4; sequence++) {
          const item = yield* envelope(
            Domain.PutTodo.name,
            Domain.todo(`bounded-${sequence}`),
            sequence,
            Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`)
          )
          submitted.push(item)
          assert.strictEqual((yield* server.submit(item))._tag, "Accepted")
        }

        yield* sql`CREATE TRIGGER require_snapshot_before_history_delete
          BEFORE DELETE ON effect_local_authoritative_log
          WHEN NOT EXISTS (
            SELECT 1 FROM effect_local_server_snapshots WHERE space_id = OLD.space_id
          )
          BEGIN SELECT RAISE(ABORT, 'snapshot required before history delete'); END`
        yield* sql`CREATE TRIGGER require_snapshot_before_receipt_delete
          BEFORE DELETE ON effect_local_server_receipts
          WHEN NOT EXISTS (
            SELECT 1 FROM effect_local_server_snapshots WHERE space_id = OLD.space_id
          )
          BEGIN SELECT RAISE(ABORT, 'snapshot required before receipt delete'); END`

        yield* server.maintain(spaceId)

        const countRows = SqlSchema.findOne({
          Request: Schema.String,
          Result: Schema.Struct({ history: Schema.Int, receipts: Schema.Int, snapshots: Schema.Int }),
          execute: (requestedSpace) =>
            sql`SELECT
            (SELECT COUNT(*) FROM effect_local_authoritative_log WHERE space_id = ${requestedSpace}) AS history,
            (SELECT COUNT(*) FROM effect_local_server_receipts WHERE space_id = ${requestedSpace}) AS receipts,
            (SELECT COUNT(*) FROM effect_local_server_snapshots WHERE space_id = ${requestedSpace}) AS snapshots`
        })
        const counts = yield* countRows(spaceId)
        assert.strictEqual(counts.history, 1)
        assert.strictEqual(counts.receipts, 1)
        assert.strictEqual(counts.snapshots, 1)
        const globalSnapshot = yield* SqlSchema.findOne({
          Request: Identity.SpaceId,
          Result: Schema.Struct({ snapshot_id: Identity.SnapshotId }),
          execute: (requestedSpace) =>
            sql`SELECT snapshot_id FROM effect_local_server_snapshots
              WHERE space_id = ${requestedSpace}`
        })(spaceId)

        const pulled = yield* server.pull(pullRequest())
        assert.isTrue("_tag" in pulled)
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(pulled.manifest))
        assert.strictEqual(page.entries.length, 4)
        assert.isFalse(page.hasMore)
        assert.isAtMost(Protocol.encodedBytes(page), bounded.maximumBootstrapPageBytes)

        const expired = yield* server.submit(submitted[0])
        assert.strictEqual(expired._tag, "Expired")
        if (expired._tag === "Expired") {
          assert.strictEqual(expired.snapshotId, globalSnapshot.snapshot_id)
          assert.notStrictEqual(expired.snapshotId, pulled.manifest.snapshotId)
          assert.isAtLeast(expired.terminalSequenceThrough, 3)
        }
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("pages every space during global history maintenance", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({
          ...serverHistory,
          definition: Domain.definition,
          maintenanceSpaceBatchSize: 2
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        for (let index = 1; index <= 3; index++) {
          const requestedSpace = Identity.SpaceId.make(
            `spc_00000000-0000-4000-8000-${String(index).padStart(12, "0")}`
          )
          const identity: Omit<Protocol.MutationEnvelope, "digest"> = {
            spaceId: requestedSpace,
            clientId,
            mutationId: Identity.MutationId.make(
              `mut_00000000-0000-4000-8002-${String(index).padStart(12, "0")}`
            ),
            localSequence: Identity.LocalSequence.make(1),
            basis: Identity.ServerSequence.make(0),
            name: Domain.PutTodo.name,
            payload: Domain.todo(`maintain-${index}`),
            digestVersion: 2,
            sourceSchema: Domain.definition.schemaIdentity,
            mutationVersion: Domain.PutTodo.version
          }
          yield* server.submit(Protocol.MutationEnvelope.make({
            ...identity,
            digest: yield* Protocol.mutationDigest(identity)
          }))
        }

        yield* server.maintainAll

        const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_snapshots`
        assert.strictEqual(rows[0].count, 3)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("applies hard history backpressure until maintenance publishes recovery state", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 2,
          retainedReceipts: 1,
          maximumReceipts: 3
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        for (let sequence = 1; sequence <= 2; sequence++) {
          yield* server.submit(
            yield* envelope(
              Domain.PutTodo.name,
              Domain.todo(`capacity-${sequence}`),
              sequence,
              Identity.MutationId.make(`mut_00000000-0000-4000-8001-${String(sequence).padStart(12, "0")}`)
            )
          )
        }
        const third = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("capacity-3"),
          3,
          Identity.MutationId.make("mut_00000000-0000-4000-8001-000000000003")
        )
        const blocked = yield* server.submit(third).pipe(Effect.flip)
        assert.strictEqual(blocked._tag, "CapacityExceeded")
        yield* server.maintain(spaceId)
        assert.strictEqual((yield* server.submit(third))._tag, "Accepted")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects corrupted retained row counters before admission", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("counter-1"),
            1,
            Identity.MutationId.make("mut_00000000-0000-4000-8001-100000000001")
          )
        )
        yield* sql`UPDATE effect_local_server_spaces SET retained_history_count = 0
          WHERE space_id = ${spaceId}`

        const error = yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("counter-2"),
            2,
            Identity.MutationId.make("mut_00000000-0000-4000-8001-100000000002")
          )
        ).pipe(Effect.flip)

        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("bounds rejected receipts independently from accepted history", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 1,
          retainedReceipts: 0,
          maximumReceipts: 2
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layer({
            ...bounded,
            definition: Domain.definition,
            authorizeAccess: () => Effect.void,
            authorizeMutation: () => Effect.fail("denied"),
            authorizeRead: () => Effect.void
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const submitted: Array<Protocol.MutationEnvelope> = []
        for (let sequence = 1; sequence <= 3; sequence++) {
          submitted.push(
            yield* envelope(
              Domain.PutTodo.name,
              Domain.todo(`rejected-${sequence}`),
              sequence,
              Identity.MutationId.make(`mut_00000000-0000-4000-8004-${String(sequence).padStart(12, "0")}`)
            )
          )
        }

        assert.strictEqual((yield* server.submit(submitted[0]))._tag, "Rejected")
        assert.strictEqual((yield* server.submit(submitted[1]))._tag, "Rejected")
        const blocked = yield* server.submit(submitted[2]).pipe(Effect.flip)
        assert.strictEqual(blocked._tag, "CapacityExceeded")

        yield* server.maintain(spaceId)

        assert.strictEqual((yield* server.submit(submitted[2]))._tag, "Rejected")
        assert.strictEqual((yield* server.submit(submitted[0]))._tag, "Expired")
        const pulled = yield* server.pull(pullRequest())
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        assert.strictEqual(pulled.manifest.entityCount, 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("terminally rejects state that cannot fit a future snapshot", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const bounded = {
          ...serverHistory,
          maximumSnapshotBytes: 1
        }
        const live = ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const item = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("snapshot-capacity"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8005-000000000001")
        )

        const receipt = yield* server.submit(item)
        assert.strictEqual(receipt._tag, "Rejected")
        if (receipt._tag !== "Rejected") assert.fail("expected terminal rejection")
        assert.deepStrictEqual(receipt.rejection, {
          _tag: "CapacityExceeded",
          resource: "snapshot bytes",
          limit: 1
        })
        assert.deepStrictEqual(yield* server.submit(item), receipt)

        const count = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ entities: Schema.Int, history: Schema.Int }),
          execute: () =>
            sql`SELECT
              (SELECT COUNT(*) FROM effect_local_server_entities) AS entities,
              (SELECT COUNT(*) FROM effect_local_authoritative_log) AS history`
        })(undefined)
        assert.deepStrictEqual(count, { entities: 0, history: 0 })
        yield* server.maintain(spaceId)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("bootstraps a fresh client without replaying retained history", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 1,
          maximumHistoryEntries: 8,
          retainedReceipts: 1,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        for (let sequence = 1; sequence <= 4; sequence++) {
          yield* server.submit(
            yield* envelope(
              Domain.PutTodo.name,
              Domain.todo(`bootstrap-${sequence}`),
              sequence,
              Identity.MutationId.make(`mut_00000000-0000-4000-8002-${String(sequence).padStart(12, "0")}`)
            )
          )
        }
        yield* server.maintain(spaceId)

        const freshClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000099")
        const local = LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId: freshClientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const reconciler = Reconciler.layerOnePass({ definition: Domain.definition, spaceId, pageSize: 2 }).pipe(
          Layer.provide(local),
          Layer.provide(directSync(server))
        )
        const context = yield* Layer.build(Layer.merge(local, reconciler))
        const store = Context.get(context, LocalStore.Store)
        yield* Context.get(context, Reconciler.Reconciliation).sync

        assert.strictEqual(yield* store.cursor, 4)
        for (let sequence = 1; sequence <= 4; sequence++) {
          assert.deepStrictEqual(
            Option.getOrThrow(yield* store.get(Domain.Todo, `bootstrap-${sequence}`)),
            Domain.todo(`bootstrap-${sequence}`)
          )
        }
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("resumes a durable bootstrap stage after reopening the client database", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/bootstrap-resume.sqlite`
        const persistentDatabase = () =>
          Layer.mergeAll(
            SqliteClient.layer({ filename, disableWAL: true }),
            NodeCrypto.layer,
            Reactivity.layer
          )
        const makeLocal = () =>
          LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        for (let sequence = 1; sequence <= 2; sequence++) {
          yield* server.submit(
            yield* envelope(
              Domain.PutTodo.name,
              Domain.todo(`resume-${sequence}`),
              sequence,
              Identity.MutationId.make(`mut_00000000-0000-4000-8006-${String(sequence).padStart(12, "0")}`)
            )
          )
        }
        yield* server.maintain(spaceId)
        const pulled = yield* server.pull(pullRequest())
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const first = yield* server.bootstrap(bootstrapRequest(pulled.manifest, -1, 1))
        assert.isTrue(first.hasMore)

        yield* Effect.scoped(Effect.gen(function*() {
          const local = yield* service(LocalStore.Store, makeLocal())
          assert.strictEqual(yield* local.prepareBootstrap(first.manifest), -1)
          assert.isFalse(yield* local.stageBootstrapPage(first))
        }))

        yield* Effect.scoped(Effect.gen(function*() {
          const local = yield* service(LocalStore.Store, makeLocal())
          assert.strictEqual(yield* local.prepareBootstrap(first.manifest), 0)
          const finalPage = yield* server.bootstrap(bootstrapRequest(first.manifest, 0, 1))
          assert.isTrue(yield* local.stageBootstrapPage(finalPage))
          yield* local.installBootstrap(finalPage.manifest)
          assert.strictEqual(yield* local.cursor, 2)
          assert.deepStrictEqual(
            Option.getOrThrow(yield* local.get(Domain.Todo, "resume-1")),
            Domain.todo("resume-1")
          )
          assert.deepStrictEqual(
            Option.getOrThrow(yield* local.get(Domain.Todo, "resume-2")),
            Domain.todo("resume-2")
          )
        }))
      }).pipe(Effect.provide(NodeFileSystem.layer), Effect.provide(NodeCrypto.layer))
    ))

  it.effect("keeps canonical state unchanged when a bootstrap page is corrupt", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 4,
          retainedReceipts: 0,
          maximumReceipts: 4
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const item = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("corrupt-bootstrap"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8003-000000000001")
        )
        yield* server.submit(item)
        yield* server.maintain(spaceId)
        const pulled = yield* server.pull(pullRequest())
        if (!("_tag" in pulled)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(pulled.manifest))
        const local = yield* service(LocalStore.Store, localLayer())
        yield* local.prepareBootstrap(page.manifest)
        const corrupt = Protocol.BootstrapPage.make({
          ...page,
          entries: page.entries.map((entry) => {
            if (entry.change._tag !== "Upsert") return entry
            return { ...entry, change: { ...entry.change, value: { corrupt: true } } }
          })
        })
        const error = yield* local.stageBootstrapPage(corrupt).pipe(Effect.flip)
        assert.strictEqual(error._tag, "ProtocolInvalid")
        const stalled = yield* local.stageBootstrapPage(Protocol.BootstrapPage.make({
          manifest: page.manifest,
          entries: [],
          hasMore: true
        })).pipe(Effect.flip)
        assert.strictEqual(stalled._tag, "ProtocolInvalid")
        assert.strictEqual(yield* local.cursor, 0)
        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "corrupt-bootstrap")))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("settles an expired pending mutation only after installing its covering snapshot", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const clientDatabase = database()
        const localLayerWithDatabase = LocalStore.layer({
          ...clientHistory,
          retainedReceipts: 1,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const reconciler = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(localLayerWithDatabase),
          Layer.provide(directSync(server))
        )
        const context = yield* Layer.build(Layer.mergeAll(localLayerWithDatabase, reconciler, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const sync = Context.get(context, Reconciler.Reconciliation)
        const sql = Context.get(context, SqlClient.SqlClient)

        yield* local.mutate(Domain.PutTodo, Domain.todo("expired-pending"))
        yield* sync.sync
        const increment = yield* local.mutate(Domain.IncrementTodo, { id: "expired-pending", delta: 1 })
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)
        assert.strictEqual((yield* server.submit(increment.envelope))._tag, "Accepted")
        yield* server.maintain(spaceId)

        const expired = yield* server.submit(increment.envelope)
        assert.strictEqual(expired._tag, "Expired")
        if (expired._tag !== "Expired") assert.fail("expected expired receipt")
        yield* local.persistReceipt(expired)
        yield* local.settleReceipts

        assert.strictEqual(yield* local.pendingCount, 1)
        assert.strictEqual(Option.getOrThrow(yield* local.receipt(increment.envelope.mutationId))._tag, "Expired")
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)

        const state = yield* local.replicationState
        if (state.cursor === null) assert.fail("expected installed replication view")
        const page = yield* server.bootstrap({
          spaceId,
          clientId,
          schema: Domain.definition.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: state.cursor,
          snapshotId: expired.snapshotId,
          afterOrdinal: -1,
          limit: 10
        })
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
        yield* local.installBootstrap(page.manifest)

        assert.strictEqual(yield* local.pendingCount, 0)
        const receipt = Option.getOrThrow(yield* local.receipt(increment.envelope.mutationId))
        assert.strictEqual(receipt._tag, "Expired")
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "expired-pending")).count, 1)
        const countRows = SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ receipts: Schema.Int, history: Schema.Int }),
          execute: () =>
            sql`SELECT
              (SELECT COUNT(*) FROM effect_local_receipts) AS receipts,
              (SELECT COUNT(*) FROM effect_local_server_log) AS history`
        })
        assert.deepStrictEqual(yield* countRows(undefined), { receipts: 1, history: 0 })
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("settles expired rejected history when the cursor already covers its snapshot", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layer({
            ...bounded,
            definition: Domain.definition,
            authorizeAccess: () => Effect.void,
            authorizeMutation: (input) => {
              if (input.mutation.name === Domain.RenameTodo.name) return Effect.fail("denied")
              return Effect.void
            },
            authorizeRead: () => Effect.void
          }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const localLive = LocalStore.layer({
          ...clientHistory,
          retainedReceipts: 2,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const reconciliationLive = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(localLive),
          Layer.provide(directSync(server))
        )
        const context = yield* Layer.build(Layer.merge(localLive, reconciliationLive))
        const local = Context.get(context, LocalStore.Store)
        const reconciliation = Context.get(context, Reconciler.Reconciliation)

        yield* local.mutate(Domain.PutTodo, Domain.todo("terminal-fence"))
        yield* reconciliation.sync
        yield* server.maintain(spaceId)
        const firstRequired = yield* server.pull(pullRequest())
        if (!("_tag" in firstRequired)) assert.fail("expected first snapshot")
        const firstPage = yield* server.bootstrap(bootstrapRequest(firstRequired.manifest))
        yield* local.prepareBootstrap(firstPage.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(firstPage))
        yield* local.installBootstrap(firstPage.manifest)

        const rejected = yield* local.mutate(Domain.RenameTodo, { id: "terminal-fence", title: "rejected-only" })
        assert.strictEqual((yield* server.submit(rejected.envelope))._tag, "Rejected")
        yield* server.maintain(spaceId)

        yield* reconciliation.sync

        assert.strictEqual(yield* local.pendingCount, 0)
        assert.strictEqual(
          Option.getOrThrow(yield* local.receipt(rejected.envelope.mutationId))._tag,
          "Expired"
        )
        assert.strictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "terminal-fence")).title, "first")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("revalidates durable staged entities before snapshot installation", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const clientDatabase = database()
        const localLive = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* Layer.build(Layer.merge(localLive, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)

        const initial = yield* local.mutate(Domain.PutTodo, Domain.todo("staged-corruption", "old"))
        const initialReceipt = yield* server.submit(initial.envelope)
        yield* local.applyReceipt(initialReceipt)
        const initialRequired = yield* server.pull(pullRequest())
        if (!("_tag" in initialRequired)) assert.fail("expected initial bootstrap")
        const initialPage = yield* server.bootstrap(bootstrapRequest(initialRequired.manifest))
        yield* local.prepareBootstrap(initialPage.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(initialPage))
        yield* local.installBootstrap(initialPage.manifest)
        const rename = yield* envelope(
          Domain.RenameTodo.name,
          { id: "staged-corruption", title: "new" },
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8007-000000000002")
        )
        yield* server.submit(rename)
        yield* server.maintain(spaceId)
        const required = yield* server.pull(pullRequest())
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(required.manifest))
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))
        yield* sql`UPDATE effect_local_client_scoped_bootstrap_entries
          SET entry_bytes = entry_bytes + 1 WHERE ordinal = 0`

        const error = yield* local.installBootstrap(page.manifest).pipe(Effect.flip)

        assert.strictEqual(error._tag, "StorageCorrupt")
        assert.strictEqual(yield* local.cursor, 1)
        assert.strictEqual(
          Option.getOrThrow(yield* local.get(Domain.Todo, "staged-corruption")).title,
          "old"
        )
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects mismatched durable receipt identity during snapshot installation", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const bounded = {
          ...serverHistory,
          retainedHistoryEntries: 0,
          maximumHistoryEntries: 8,
          retainedReceipts: 0,
          maximumReceipts: 8
        }
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...bounded, definition: Domain.definition }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const clientDatabase = database()
        const localLive = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* Layer.build(Layer.merge(localLive, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const first = yield* local.mutate(Domain.PutTodo, Domain.todo("receipt-a"))
        const second = yield* local.mutate(Domain.PutTodo, Domain.todo("receipt-b"))
        yield* local.persistReceipt(Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          mutationId: first.envelope.mutationId,
          localSequence: first.envelope.localSequence,
          ...putTodoProvenance,
          origin: "Mutation",
          terminalSequence: Identity.TerminalSequence.make(1),
          rejection: "denied"
        }))
        const corrupt = Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          mutationId: second.envelope.mutationId,
          localSequence: second.envelope.localSequence,
          ...putTodoProvenance,
          origin: "Mutation",
          terminalSequence: Identity.TerminalSequence.make(1),
          rejection: "denied"
        })
        yield* sql`UPDATE effect_local_receipts SET receipt_json = ${Canonical.stringify(corrupt)}
          WHERE mutation_id = ${first.envelope.mutationId}`
        const authoritative = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("snapshot-receipt-identity"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8008-000000000001")
        )
        yield* server.submit(authoritative)
        yield* server.maintain(spaceId)
        const required = yield* server.pull(pullRequest())
        if (!("_tag" in required)) assert.fail("expected bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(required.manifest))
        yield* local.prepareBootstrap(page.manifest)
        assert.isTrue(yield* local.stageBootstrapPage(page))

        const error = yield* local.installBootstrap(page.manifest).pipe(Effect.flip)

        assert.strictEqual(error._tag, "StorageCorrupt")
        assert.strictEqual(yield* local.pendingCount, 2)
        assert.strictEqual(yield* local.cursor, 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("settles a durable legacy rejection before resubmitting pending work", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const local = yield* service(LocalStore.Store, localLayer())
        const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("legacy-rejection"))
        yield* local.persistReceipt(Protocol.RejectedReceipt.make({
          spaceId,
          clientId,
          mutationId: pending.envelope.mutationId,
          localSequence: pending.envelope.localSequence,
          ...putTodoProvenance,
          origin: "Mutation",
          rejection: "Rejected"
        }))
        const submissions = yield* Ref.make(0)
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const remote = SyncEngine.SyncEngine.of({
          submit: (submitted) =>
            Ref.update(submissions, (count) => count + 1).pipe(
              Effect.as(Protocol.RejectedReceipt.make({
                spaceId,
                clientId,
                mutationId: submitted.envelope.mutationId,
                localSequence: submitted.envelope.localSequence,
                ...putTodoProvenance,
                origin: "Mutation",
                terminalSequence: Identity.TerminalSequence.make(1),
                rejection: "Rejected"
              }))
            ),
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: server.watch
        })
        const reconciliation = yield* service(
          Reconciler.Reconciliation,
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
          )
        )

        yield* reconciliation.sync

        assert.strictEqual(yield* Ref.get(submissions), 0)
        assert.strictEqual(yield* local.pendingCount, 0)
        assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "legacy-rejection")))
      })
    ))

  it.effect("rejects inconsistent durable replication scope metadata", () =>
    Effect.scoped(Effect.gen(function*() {
      const databaseContext = yield* Layer.build(database())
      const sql = Context.get(databaseContext, SqlClient.SqlClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId, migration }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      const services = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, sql),
        Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto)),
        Layer.succeed(Reactivity.Reactivity, Context.get(databaseContext, Reactivity.Reactivity))
      )
      const live = LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(runtime),
        Layer.provide(services)
      )

      const context = yield* Layer.build(live)
      const local = Context.get(context, LocalStore.Store)
      yield* sql`UPDATE effect_local_client_meta SET desired_scope_digest = ${"0".repeat(64)}
        WHERE singleton = 1`
      const error = yield* local.replicationState.pipe(Effect.flip)

      assert.strictEqual(error._tag, "StorageCorrupt")
    })))

  it.effect("commits optimistically, admits in total order, and reconciles canonical state", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())

      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
      assert.strictEqual(yield* local.pendingCount, 1)

      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Accepted")
      if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
      assert.strictEqual(receipt.serverSequence, 1)
      yield* local.applyReceipt(receipt)
      yield* installFreshView(local, server)

      assert.strictEqual(yield* local.pendingCount, 0)
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
      assert.strictEqual(yield* local.cursor, 1)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
    })))

  it.effect("rejects a first submission whose digest does not match its envelope", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const changed = { ...pending.envelope, payload: Domain.todo("1", "tampered") }
      const exit = yield* server.submit(changed).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      if (exit._tag === "Failure") {
        const failure = Cause.findErrorOption(exit.cause)
        assert.strictEqual(failure._tag, "Some")
        if (failure._tag === "Some") assert.strictEqual(failure.value._tag, "MutationIdentityConflict")
      }
    })))

  it.effect("returns the same terminal receipt for an exact retry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const first = yield* server.submit(pending.envelope)
      const retry = yield* server.submit(pending.envelope)
      assert.deepStrictEqual(retry, first)
      assert.strictEqual(first._tag, "Accepted")
      if (first._tag === "Accepted") assert.strictEqual(first.serverSequence, 1)
    })))

  it.effect("reauthorizes an exact retry before returning its durable receipt", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const access = yield* Ref.make(true)
        const secured = ServerStore.layer({
          ...serverHistory,
          definition: Domain.definition,
          authorizeAccess: () =>
            Ref.get(access).pipe(
              Effect.flatMap((allowed) => {
                if (allowed) return Effect.void
                return Effect.fail({ reason: "revoked" })
              })
            ),
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const server = yield* service(ServerStore.ServerStore, secured)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("1"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000041")
        )
        const request = Protocol.SubmitRequest.make({ envelope: submitted, schema: Domain.definition.schemaIdentity })
        assert.strictEqual((yield* server.admit(request, { subject: "test" }))._tag, "Accepted")
        yield* Ref.set(access, false)

        const error = yield* server.admit(request, { subject: "test" }).pipe(Effect.flip)
        assert.strictEqual(error._tag, "AuthorizationDenied")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("keeps mutation payloads and private results out of the authoritative log", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const serverDatabase = database()
      const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
        Layer.provide(runtime),
        Layer.provide(serverDatabase)
      )
      const context = yield* Layer.build(Layer.merge(live, serverDatabase))
      const server = Context.get(context, ServerStore.ServerStore)
      const sql = Context.get(context, SqlClient.SqlClient)
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("private", "secret"))
      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Accepted")

      const entry = (yield* authoritativeLog(sql))[0]
      assert.isFalse(Object.hasOwn(entry, "envelope"))
      assert.isFalse(Object.hasOwn(entry, "result"))
    })))

  it.effect("pulls the public log without materializing private receipt payloads", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("public-with-private-receipt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000029")
        )
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts
      SET receipt_json = ${"x".repeat(Protocol.maximumReceiptBytes)}
      WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const entries = yield* authoritativeLog(sql)
        assert.deepStrictEqual(entries.map((entry) => entry.mutationId), [submitted.mutationId])
        assert.strictEqual((yield* server.submit(submitted).pipe(Effect.flip))._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("recovers an accepted commit whose private receipt was lost", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Accepted")

      yield* installFreshView(local, server)
      assert.strictEqual(yield* local.pendingCount, 1)
      assert.isTrue(Option.isNone(yield* local.receipt(pending.envelope.mutationId)))
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))

      yield* local.applyReceipt(receipt)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
    })))

  it.effect("stores matching authoritative entry and SQL identities", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const serverLayerWithDatabase = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition })
          .pipe(
            Layer.provide(runtime),
            Layer.provide(serverDatabase)
          )
        const context = yield* Layer.build(Layer.merge(serverLayerWithDatabase, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("1"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000031")
        )
        yield* server.submit(submitted)
        const row = (yield* authoritativeRows(sql))[0]
        const entry = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(Protocol.AcceptedMutation)
        )(row.entry_json)
        assert.strictEqual(row.space_id, entry.spaceId)
        assert.strictEqual(row.server_sequence, entry.sequence)
        assert.strictEqual(row.client_id, entry.clientId)
        assert.strictEqual(row.local_sequence, entry.localSequence)
        assert.strictEqual(row.mutation_id, entry.mutationId)
        assert.strictEqual(row.digest, entry.digest)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("stores a terminal rejection without advancing the accepted cursor", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer(() => Effect.fail({ reason: "denied" })))
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Rejected")
      yield* local.applyReceipt(receipt)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "1")))
      assert.strictEqual(yield* local.cursor, 0)
    })))

  it.effect("rolls back server writes performed before a typed rejection", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const rejected = yield* envelope(
          Domain.RejectAfterWrite.name,
          Domain.todo("poison"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000011")
        )
        assert.strictEqual((yield* server.submit(rejected))._tag, "Rejected")

        const increment = yield* envelope(
          Domain.IncrementTodo.name,
          { id: "poison", delta: 1 },
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000012")
        )
        const receipt = yield* server.submit(increment)
        assert.strictEqual(receipt._tag, "Rejected")
        if (receipt._tag === "Rejected") assert.strictEqual(receipt.rejection, "TodoNotFound")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("terminally rejects an accepted entry that cannot fit in a pull page", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const oversized = yield* envelope(
          Domain.PutHugeTodo.name,
          { id: "huge" },
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000021")
        )
        assert.strictEqual((yield* server.submit(oversized))._tag, "Rejected")

        const next = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("next"),
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000022")
        )
        const receipt = yield* server.submit(next)
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("stores a bounded terminal receipt when a private result exceeds the RPC response limit", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const oversized = yield* envelope(
          Domain.ReturnHugeResult.name,
          null,
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000023")
        )
        const first = yield* server.submit(oversized)
        const retry = yield* server.submit(oversized)

        assert.strictEqual(first._tag, "Rejected")
        assert.deepStrictEqual(retry, first)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(first), Protocol.maximumReceiptBytes)
        if (first._tag === "Rejected") {
          assert.deepStrictEqual(first.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        const next = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("after-oversized-result"),
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000030")
        )
        const accepted = yield* server.submit(next)
        assert.strictEqual(accepted._tag, "Accepted")
        if (accepted._tag === "Accepted") assert.strictEqual(accepted.serverSequence, 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("bounds an oversized authorization rejection before storing its terminal receipt", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          serverLayer(() => Effect.fail("x".repeat(Protocol.maximumReceiptBytes)))
        )
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("oversized-authorization"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        )
        const receipt = yield* server.submit(submitted)

        assert.strictEqual(receipt._tag, "Rejected")
        assert.isAtMost(yield* Protocol.encodedBytesEffect(receipt), Protocol.maximumReceiptBytes)
        if (receipt._tag === "Rejected") {
          assert.deepStrictEqual(receipt.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        assert.deepStrictEqual(yield* server.submit(submitted), receipt)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("assigns dense authoritative log sequences", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("gap-1"),
            1,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000024")
          )
        )
        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("gap-2"),
            2,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000025")
          )
        )
        const rows = yield* authoritativeRows(sql)
        assert.deepStrictEqual(rows.map((row) => row.server_sequence), [1, 2])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects an exact retry whose durable receipt conflicts with its SQL identity", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("retry-corrupt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000026")
        )
        yield* server.submit(submitted)
        const conflictingMutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000027")
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Rejected",
            spaceId,
            clientId,
            mutationId: conflictingMutationId,
            localSequence: submitted.localSequence,
            rejection: "corrupt"
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects an accepted retry whose receipt sequence conflicts with the authoritative log", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("retry-sequence-corrupt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        )
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Accepted",
            spaceId,
            clientId,
            mutationId: submitted.mutationId,
            localSequence: submitted.localSequence,
            serverSequence: 2,
            result: Domain.todo("retry-sequence-corrupt")
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a pending row whose durable digest does not match its reconstructed identity", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const clientDatabase = database()
        const live = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("digest-corrupt"))
        yield* sql`UPDATE effect_local_pending SET digest = ${"0".repeat(64)}
        WHERE mutation_id = ${pending.envelope.mutationId}`

        const error = yield* local.pending.pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("persists each submitted receipt before submitting the next pending mutation", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("stream-1"))
      const second = yield* local.mutate(Domain.PutTodo, Domain.todo("stream-2"))
      let submissions = 0
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const remote = SyncEngine.SyncEngine.of({
        submit: ({ envelope: submitted }) =>
          Effect.gen(function*() {
            submissions++
            if (submissions === 2) {
              assert.isTrue(Option.isSome(yield* local.receipt(first.envelope.mutationId)))
            }
            return Protocol.RejectedReceipt.make({
              ...putTodoProvenance,
              spaceId,
              clientId,
              mutationId: submitted.mutationId,
              localSequence: submitted.localSequence,
              origin: "Authorization",
              rejection: "denied"
            })
          }),
        pull: server.pull,
        bootstrap: server.bootstrap,
        watch: server.watch
      })
      const reconciler = yield* service(
        Reconciler.Reconciler,
        Reconciler.layer({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
        )
      )

      yield* reconciler.sync
      assert.strictEqual(submissions, 2)
      assert.isTrue(Option.isSome(yield* local.receipt(first.envelope.mutationId)))
      assert.isTrue(Option.isSome(yield* local.receipt(second.envelope.mutationId)))
      assert.strictEqual(yield* local.pendingCount, 0)
    })))

  it.effect("invalidates the receipt dependency when a terminal receipt is stored", () =>
    Effect.scoped(Effect.gen(function*() {
      const clientDatabase = database()
      const local = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
        Layer.provide(runtime),
        Layer.provide(clientDatabase)
      )
      const context = yield* Layer.build(Layer.merge(local, clientDatabase))
      const store = Context.get(context, LocalStore.Store)
      const reactivity = Context.get(context, Reactivity.Reactivity)
      const pending = yield* store.mutate(Domain.PutTodo, Domain.todo("1"))
      let invalidations = 0
      const cancel = reactivity.registerUnsafe(
        [`effect-local:receipt:${pending.envelope.mutationId}`],
        () => invalidations++
      )
      yield* Effect.addFinalizer(() => Effect.sync(cancel))
      yield* store.applyReceipt({
        _tag: "Rejected",
        ...putTodoProvenance,
        spaceId,
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        origin: "Authorization",
        rejection: "denied"
      })
      assert.strictEqual(invalidations, 1)
    })))

  it.effect("uses explicit field semantics without metadata on ordinary model values", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
      yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
      yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
      const value = Option.getOrThrow(yield* local.get(Domain.Todo, "1"))
      assert.deepStrictEqual(value, { id: "1", title: "first", count: 2, labels: ["a"] })
      assert.strictEqual(Object.keys(value).some((key) => key.startsWith("$")), false)
    })))

  it.effect("canonicalizes the exact identity covered by the digest", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const local = yield* service(LocalStore.Store, localLayer())
        const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
        const { digest, ...identity } = pending.envelope
        assert.strictEqual(yield* Protocol.mutationDigest(identity), digest)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("refuses a mutation envelope beyond the configured protocol bound", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const error = yield* local.mutate(
        Domain.PutTodo,
        Domain.todo(
          "1",
          "x".repeat(Protocol.maximumMutationBytes)
        )
      ).pipe(Effect.flip)
      assert.strictEqual(error._tag, "CapacityExceeded")
      assert.strictEqual(yield* local.pendingCount, 0)
    })))

  it.effect("rejects invalid local and reconciliation configuration during layer construction", () =>
    Effect.scoped(Effect.gen(function*() {
      const localError = yield* service(
        LocalStore.Store,
        LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId,
          maximumPendingMutations: 0
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      ).pipe(Effect.flip)
      assert.strictEqual(localError._tag, "InvalidConfiguration")
      if (localError._tag === "InvalidConfiguration") {
        assert.strictEqual(localError.option, "maximumPendingMutations")
      }

      const wakeCapacityError = yield* service(
        ServerStore.ServerStore,
        ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition, wakeCapacity: 0 }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      ).pipe(Effect.flip)
      assert.strictEqual(wakeCapacityError._tag, "InvalidConfiguration")
      if (wakeCapacityError._tag === "InvalidConfiguration") {
        assert.strictEqual(wakeCapacityError.option, "wakeCapacity")
      }

      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const invalidPageSize = Reconciler.layer({ definition: Domain.definition, spaceId, pageSize: 0 }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(directSync(server))
      )
      const pageSizeError = yield* service(Reconciler.Reconciler, invalidPageSize).pipe(Effect.flip)
      assert.strictEqual(pageSizeError._tag, "InvalidConfiguration")
      if (pageSizeError._tag === "InvalidConfiguration") assert.strictEqual(pageSizeError.option, "pageSize")

      const invalidRetryDelay = Reconciler.layer({
        definition: Domain.definition,
        spaceId,
        retryDelay: "0 millis"
      }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(directSync(server))
      )
      const retryDelayError = yield* service(Reconciler.Reconciler, invalidRetryDelay).pipe(Effect.flip)
      assert.strictEqual(retryDelayError._tag, "InvalidConfiguration")
      if (retryDelayError._tag === "InvalidConfiguration") assert.strictEqual(retryDelayError.option, "retryDelay")
    })))

  it.effect("rolls a rejected optimistic mutation back and replays later pending work", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* service(
        ServerStore.ServerStore,
        serverLayer(({ mutation }) => {
          if (mutation.name === Domain.RenameTodo.name) return Effect.fail({ reason: "denied" })
          return Effect.void
        })
      )
      const services = yield* Layer.build(clientServices(clientId, server))
      const local = Context.get(services, LocalStore.Store)
      const reconciler = Context.get(services, Reconciler.Reconciler)

      yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* reconciler.sync
      const rename = yield* local.mutate(Domain.RenameTodo, { id: "1", title: "optimistic" })
      yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })

      yield* reconciler.sync
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), {
        id: "1",
        title: "first",
        count: 2,
        labels: []
      })
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(rename.envelope.mutationId))._tag, "Rejected")
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(yield* local.cursor, 2)
    })))

  it.effect("rejects a client sequence gap without consuming server order", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const second = yield* local.mutate(Domain.PutTodo, Domain.todo("2"))
      const error = yield* server.submit(second.envelope).pipe(Effect.flip)
      assert.strictEqual(error._tag, "OutOfOrderMutation")
      if (error._tag === "OutOfOrderMutation") assert.strictEqual(error.expected, 1)
      const receipt = yield* server.submit(first.envelope)
      assert.strictEqual(receipt._tag, "Accepted")
      if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
    })))

  it.effect("deduplicates overlapping catch up pages", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(first.envelope)
      if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
      const entry = acceptedMutation(first, receipt)
      yield* local.applyEntries([entry, entry])
      assert.strictEqual(yield* local.cursor, 1)
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
    })))

  it.effect("rejects a conflicting duplicate catch up entry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
      const entry = acceptedMutation(pending, receipt)
      yield* local.applyEntries([entry])

      const error = yield* local.applyEntries([{ ...entry, changes: [] }]).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.cursor, 1)
    })))

  it.effect("does not settle pending work from a conflicting own-client entry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      if (receipt._tag !== "Accepted") assert.fail("expected accepted receipt")
      const entry = acceptedMutation(pending, receipt)
      const error = yield* local.applyEntries([{
        ...entry,
        digest: "0".repeat(64)
      }]).pipe(Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
      assert.strictEqual(yield* local.cursor, 0)
    })))

  it.effect("resubscribes after a watch stream terminates", () =>
    Effect.scoped(Effect.gen(function*() {
      const subscriptions = yield* Ref.make(0)
      const firstSubscribed = yield* Latch.make()
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const remote = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          submit: () => Effect.die("unexpected submit"),
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: () =>
            Stream.unwrap(
              Ref.modify(subscriptions, (count) => [count, count + 1]).pipe(
                Effect.flatMap((count) => {
                  if (count === 0) {
                    return firstSubscribed.open.pipe(Effect.as(Stream.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "disconnected"
                      })
                    )))
                  }
                  return Effect.succeed(Stream.never)
                })
              )
            )
        })
      )
      const reconciler = Reconciler.layer({
        definition: Domain.definition,
        spaceId,
        retryDelay: "1 second"
      }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(remote)
      )
      yield* service(Reconciler.Reconciler, reconciler)
      yield* firstSubscribed.await
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(subscriptions), 2)
    })))

  it.effect("does not retry a permanently stale reconciliation runtime", () =>
    Effect.scoped(Effect.gen(function*() {
      const subscriptions = yield* Ref.make(0)
      const pulls = yield* Ref.make(0)
      const subscribed = yield* Deferred.make<void>()
      const stale = new ReplicaError.StaleSchema({
        expectedVersion: 2,
        expectedHash: "expected",
        actualVersion: 1,
        actualHash: "actual"
      })
      const remote = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          submit: () => Effect.die("unexpected submit"),
          pull: () => Ref.update(pulls, (count) => count + 1).pipe(Effect.andThen(Effect.fail(stale))),
          bootstrap: () => Effect.fail(stale),
          watch: () =>
            Stream.unwrap(
              Ref.update(subscriptions, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(subscribed, undefined)),
                Effect.as(Stream.fail(stale))
              )
            )
        })
      )
      const reconciler = Reconciler.layer({
        definition: Domain.definition,
        spaceId,
        retryDelay: "1 second"
      }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(remote)
      )
      const scheduler = yield* service(Reconciler.Reconciler, reconciler)
      yield* Deferred.await(subscribed)
      yield* Effect.yieldNow
      yield* TestClock.adjust("5 seconds")
      yield* Effect.yieldNow

      assert.strictEqual(yield* Ref.get(subscriptions), 1)
      assert.strictEqual(yield* Ref.get(pulls), 1)
      assert.strictEqual((yield* scheduler.status)._tag, "Failed")
    })))

  it.effect("retries pending mutations after an interrupted submit", () =>
    Effect.scoped(Effect.gen(function*() {
      const firstAttempt = yield* Deferred.make<void>()
      const secondAttempt = yield* Deferred.make<void>()
      let attempts = 0
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const remote = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          submit: ({ envelope: submitted }) =>
            Effect.suspend(() => {
              attempts++
              if (attempts === 1) {
                return Deferred.succeed(firstAttempt, undefined).pipe(
                  Effect.andThen(Effect.interrupt)
                )
              }
              return Deferred.succeed(secondAttempt, undefined).pipe(
                Effect.as(Protocol.RejectedReceipt.make({
                  ...putTodoProvenance,
                  spaceId,
                  clientId,
                  mutationId: submitted.mutationId,
                  localSequence: submitted.localSequence,
                  origin: "Authorization",
                  rejection: "denied"
                }))
              )
            }),
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: server.watch
        })
      )
      const local = localLayer()
      const reconciler = Reconciler.layer({
        definition: Domain.definition,
        spaceId,
        retryDelay: "1 second"
      }).pipe(
        Layer.provide(local),
        Layer.provide(remote)
      )
      const services = yield* Layer.build(Layer.merge(local, reconciler))
      const store = Context.get(services, LocalStore.Store)
      const scheduler = Context.get(services, Reconciler.Reconciler)
      yield* store.mutate(Domain.PutTodo, Domain.todo("interrupted"))

      yield* scheduler.notify
      yield* Deferred.await(firstAttempt)
      yield* Effect.yieldNow
      yield* scheduler.notify
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      assert.strictEqual(attempts, 2)
      yield* Deferred.await(secondAttempt)
    })))

  it.effect("emits a scoped wake when a watch subscription becomes ready", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* server.submit(pending.envelope)

      const wake = yield* server.watch({
        spaceId,
        clientId,
        schema: Domain.definition.schemaIdentity,
        scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null
      }).pipe(Stream.runHead)
      assert.strictEqual(Option.getOrThrow(wake).spaceId, spaceId)
    })))

  it.effect("isolates wake backpressure between spaces", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition, wakeCapacity: 1 }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
        const ready = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let initial = true
        const wake = yield* server.watch({
          spaceId: otherSpaceId,
          clientId,
          schema: Domain.definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }).pipe(
          Stream.tap(() => {
            if (!initial) return Effect.void
            initial = false
            return Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(release)))
          }),
          Stream.drop(1),
          Stream.runHead,
          Effect.forkChild
        )
        yield* Deferred.await(ready)

        const makeForSpace = (
          targetSpaceId: Identity.SpaceId,
          localSequence: number,
          mutationId: Identity.MutationId
        ) =>
          Effect.gen(function*() {
            const identity = {
              spaceId: targetSpaceId,
              clientId,
              mutationId,
              localSequence: Identity.LocalSequence.make(localSequence),
              basis: Identity.ServerSequence.make(0),
              payload: Domain.todo(`${targetSpaceId}:${localSequence}`),
              digestVersion: 2 as const,
              ...putTodoProvenance
            }
            return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
          })
        yield* server.submit(
          yield* makeForSpace(
            otherSpaceId,
            1,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000051")
          )
        )
        for (let index = 1; index <= 3; index++) {
          yield* server.submit(
            yield* makeForSpace(
              spaceId,
              index,
              Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(index + 51).padStart(12, "0")}`)
            )
          )
        }
        yield* Deferred.succeed(release, undefined)
        const result = yield* Fiber.join(wake)
        assert.strictEqual(Option.getOrThrow(result).spaceId, otherSpaceId)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("runs multi-read queries against one committed visible snapshot", () =>
    Effect.scoped(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/snapshot.sqlite`
      const Item = Model.make("SnapshotItem", {
        version: 1,
        key: Schema.String,
        schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
      })
      const PutPair = Mutation.make("PutSnapshotPair", {
        version: 1,
        payload: { left: Schema.Number, right: Schema.Number }
      })
      const ReadPair = Query.make("ReadSnapshotPair", {
        success: Schema.Tuple([Schema.Number, Schema.Number]),
        dependsOn: [Item]
      })
      class QueryGate extends Context.Service<QueryGate, {
        readonly betweenReads: Effect.Effect<void>
      }>()("test/QueryGate") {}
      const definition = Definition.make({ version: 1, models: [Item], mutations: [PutPair], queries: [ReadPair] })
      const reached = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const writerStarted = yield* Deferred.make<void>()
      const handlers = Layer.mergeAll(
        PutPair.toLayer(({ payload, transaction }) =>
          Effect.gen(function*() {
            if (payload.left === 1) yield* Deferred.succeed(writerStarted, undefined)
            yield* transaction.set(Item, "left", { id: "left", value: payload.left })
            yield* transaction.set(Item, "right", { id: "right", value: payload.right })
          })
        ),
        ReadPair.toLayer(Effect.gen(function*() {
          const gate = yield* QueryGate
          return ({ query }) =>
            Effect.gen(function*() {
              const left = Option.getOrThrow(yield* query.get(Item, "left"))
              yield* gate.betweenReads
              const right = Option.getOrThrow(yield* query.get(Item, "right"))
              return [left.value, right.value] as const
            })
        }))
      )
      const gate = Layer.succeed(
        QueryGate,
        QueryGate.of({
          betweenReads: Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(release))
          )
        })
      )
      const pairDatabase = () =>
        Layer.mergeAll(
          SqliteClient.layer({ filename }),
          NodeCrypto.layer,
          Reactivity.layer
        )
      const pairRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers), Layer.provide(gate))
      const local = LocalStore.layer({
        ...clientHistory,
        scope: Protocol.ReplicationScope.make({ models: [Item.name] }),
        definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(pairRuntime),
        Layer.provide(pairDatabase())
      )
      const queries = QueryExecutor.layer(definition).pipe(
        Layer.provide(handlers),
        Layer.provide(gate),
        Layer.provide(pairDatabase())
      )
      const context = yield* Layer.build(Layer.merge(local, queries))
      const store = Context.get(context, LocalStore.Store)
      const queryExecutor = Context.get(context, QueryExecutor.QueryExecutor)
      yield* store.mutate(PutPair, { left: 0, right: 0 })
      const query = yield* queryExecutor.execute(ReadPair, undefined).pipe(Effect.forkChild)
      yield* Deferred.await(reached)
      const mutation = yield* store.mutate(PutPair, { left: 1, right: 1 }).pipe(Effect.forkChild)
      yield* Deferred.await(writerStarted)
      yield* Fiber.join(mutation)
      yield* Deferred.succeed(release, undefined)
      assert.deepStrictEqual(yield* Fiber.join(query), [0, 0])

      const counterfeit = Query.make("ReadSnapshotPair", { success: Schema.String, dependsOn: [Item] })
      const error = yield* queryExecutor.execute(counterfeit, undefined).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
    })).pipe(Effect.provide(NodeFileSystem.layer)))

  it.effect("rejects a receipt for another replica without settling pending work", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const error = yield* local.applyReceipt({
        _tag: "Rejected",
        ...putTodoProvenance,
        spaceId: Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002"),
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        origin: "Authorization",
        rejection: { reason: "wrong space" }
      }).pipe(Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
    })))

  it.effect("rejects a conflicting duplicate receipt", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt: Protocol.AcceptedReceipt = {
        _tag: "Accepted",
        ...putTodoProvenance,
        spaceId,
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        serverSequence: Identity.ServerSequence.make(1),
        result: pending.optimisticResult
      }
      yield* local.applyReceipt(receipt)

      const error = yield* local.applyReceipt({ ...receipt, result: { conflicting: true } }).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
    })))

  it.effect("converges concurrent clients through server assigned order", () =>
    Effect.scoped(Effect.gen(function*() {
      const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const firstContext = yield* Layer.build(clientServices(clientId, server))
      const secondContext = yield* Layer.build(clientServices(secondClientId, server))
      const first = Context.get(firstContext, LocalStore.Store)
      const firstReconciler = Context.get(firstContext, Reconciler.Reconciler)
      const second = Context.get(secondContext, LocalStore.Store)
      const secondReconciler = Context.get(secondContext, Reconciler.Reconciler)

      yield* first.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* firstReconciler.sync
      yield* secondReconciler.sync
      yield* first.mutate(Domain.IncrementTodo, { id: "1", delta: 1 })
      yield* second.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
      yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })
      yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })

      assert.strictEqual(Option.getOrThrow(yield* first.get(Domain.Todo, "1")).count, 3)
      assert.strictEqual(Option.getOrThrow(yield* second.get(Domain.Todo, "1")).count, 3)
      assert.strictEqual(yield* first.cursor, 3)
      assert.strictEqual(yield* second.cursor, 3)
    })))

  it.effect("reconciles an offline mutation queue with linear handler work", () =>
    Effect.scoped(Effect.gen(function*() {
      const Item = Model.make("ReconciliationWorkItem", {
        version: 1,
        key: Schema.String,
        schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
      })
      const PutItem = Mutation.make("PutReconciliationWorkItem", {
        version: 1,
        payload: Item.schema,
        success: Item.schema
      })
      const workDefinition = Definition.make({ version: 1, models: [Item], mutations: [PutItem] })
      const executions = yield* Ref.make(0)
      const workHandlers = PutItem.toLayer(({ payload, transaction }) =>
        Ref.update(executions, (count) => count + 1).pipe(
          Effect.andThen(transaction.set(Item, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const workRuntime = MutationRuntime.layer(workDefinition).pipe(Layer.provide(workHandlers))
      const authoritativeDatabase = database()
      const authoritativeLayer = ServerStore.layerTrusted({ ...serverHistory, definition: workDefinition }).pipe(
        Layer.provide(workRuntime),
        Layer.provide(authoritativeDatabase)
      )
      const server = yield* service(ServerStore.ServerStore, authoritativeLayer)
      const replicaDatabase = database()
      const workScope = Protocol.ReplicationScope.make({ models: [Item.name] })
      const local = LocalStore.layer({
        ...clientHistory,
        scope: workScope,
        definition: workDefinition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(workRuntime),
        Layer.provide(replicaDatabase)
      )
      const reconciler = Reconciler.layer({ definition: workDefinition, spaceId }).pipe(
        Layer.provide(local),
        Layer.provide(directSync(server))
      )
      const context = yield* Layer.build(Layer.merge(local, reconciler))
      const store = Context.get(context, LocalStore.Store)
      const sync = Context.get(context, Reconciler.Reconciler)
      yield* sync.sync
      yield* Ref.set(executions, 0)

      const mutationCount = 10
      for (let index = 0; index < mutationCount; index++) {
        yield* store.mutate(PutItem, { id: String(index), value: index })
      }
      yield* sync.sync

      assert.strictEqual(yield* store.pendingCount, 0)
      assert.isAtMost(yield* Ref.get(executions), mutationCount * 3)
    })))

  it.effect("caps catch up pages by encoded bytes as well as entry count", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      yield* installFreshView(local, server)
      for (let index = 0; index < 24; index++) {
        const pending = yield* local.mutate(
          Domain.PutTodo,
          Domain.todo(
            String(index),
            `${index}:${"x".repeat(200_000)}`
          )
        )
        yield* server.submit(pending.envelope)
      }
      const state = yield* local.replicationState
      if (state.cursor === null) assert.fail("expected installed replication view")
      const page = incremental(yield* server.pull(pullRequest(state.cursor, 1_000)))
      assert.isAtMost(Protocol.encodedBytes(page), Protocol.maximumBatchBytes)
      assert.isTrue(page.hasMore)
      assert.isBelow(page.changes.length, 24)
    })))

  it.effect("restores optimistic state and its pending envelope after restart", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/replica.db`
        const persistentDatabase = () =>
          Layer.mergeAll(
            SqliteClient.layer({ filename, disableWAL: true }),
            NodeCrypto.layer,
            Reactivity.layer
          )

        const mutationId = yield* Effect.scoped(Effect.gen(function*() {
          const layer = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
          const local = yield* service(LocalStore.Store, layer)
          const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
          return pending.envelope.mutationId
        }))

        yield* Effect.scoped(Effect.gen(function*() {
          const layer = LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
          const local = yield* service(LocalStore.Store, layer)
          assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
          const pending = yield* local.pending
          assert.deepStrictEqual(pending.map((item) => item.envelope.mutationId), [mutationId])
        }))
      }).pipe(Effect.provide(NodeFileSystem.layer))
    ))
})
