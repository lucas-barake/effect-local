import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BackupStore from "../src/BackupStore.js"
import * as CheckpointAuthority from "../src/CheckpointAuthority.js"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlProjection from "../src/SqlProjection.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { decodeJson, encodeJson, nativeError } from "./helpers/json.js"

describe("BackupStore", () => {
  const concatenate = (chunks: ReadonlyArray<Uint8Array>) => {
    const bytes = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0))
    let offset = 0
    for (const chunk of chunks) {
      bytes.set(chunk, offset)
      offset += chunk.byteLength
    }
    return bytes
  }
  /**
   * The archive lines of a completed export, parsed. Every lineage assertion below reads the
   * archive itself rather than the database it came from: the documented remedy for a peer refused
   * on a lineage mismatch is to carry an archive to that peer, so a value the archive does not
   * contain is a value the remedy cannot deliver.
   */
  const archiveLinesOf = (chunks: ReadonlyArray<Uint8Array>) =>
    new TextDecoder().decode(concatenate(chunks)).trimEnd().split("\n").map(decodeJson)
  /**
   * Reseals edited archive lines back into archive bytes. Record checksums, the end record roll-up,
   * and the manifest are all recomputed, so a restore rejects the edit under test rather than the
   * framing around it. The manifest loop exists because `declaredBytes` counts the manifest line
   * that carries it, so writing it can change the size it declares.
   */
  const encodeArchive = (lines: Array<any>) =>
    Effect.gen(function*() {
      const manifest = lines[0]!
      const records = lines.slice(1, -1)
      const end = lines.at(-1)!
      for (const record of records) record.checksum = yield* Canonical.digest(record.value)
      end.value.recordCount = records.length
      end.value.recordsChecksum = yield* Canonical.digest(records.map((record) => record.checksum))
      end.checksum = yield* Canonical.digest(end.value)
      manifest.value.recordCount = records.length
      for (let attempt = 0; attempt < 8; attempt++) {
        manifest.checksum = yield* Canonical.digest(manifest.value)
        const encoded = new TextEncoder().encode(`${lines.map(encodeJson).join("\n")}\n`)
        if (manifest.value.declaredBytes === encoded.byteLength) return encoded
        manifest.value.declaredBytes = encoded.byteLength
      }
      manifest.checksum = yield* Canonical.digest(manifest.value)
      return new TextEncoder().encode(`${lines.map(encodeJson).join("\n")}\n`)
    })
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "backup-tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const TaskList = Projection.make("TaskList", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
  })
  const TaskListSql = SqlProjection.make(TaskList, {
    table: "task_list_v1",
    migrations: [{
      id: 1,
      name: "task_list_v1",
      run: (sql, table) =>
        sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
          source_document_id TEXT PRIMARY KEY,
          title TEXT NOT NULL
        )`.pipe(Effect.asVoid)
    }],
    deleteByDocument: (sql, table, documentId) =>
      sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
    insert: (sql, table, row) =>
      sql`INSERT INTO ${sql(table)} (source_document_id, title)
        VALUES (${row.sourceDocumentId}, ${row.title})`.pipe(Effect.asVoid)
  })
  const projectedDefinition = ReplicaDefinition.make({
    name: "projected-backup-tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [TaskList],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1024 * 1024,
    maxChunkBytes: 64 * 1024,
    maxArchiveRecords: 1000,
    maxJsonDepth: 32,
    maxConflictDepth: 64,
    maxConflictNodes: 100_000,
    maxConflictAlternatives: 10_000,
    maxConflictPathSegments: 128,
    maxConflictValueBytes: 16 * 1024 * 1024,
    maxConflictSourceChanges: 100_000,
    maxConflictSourceOperations: 100_000,
    maxConflictSourceBytes: 64 * 1024 * 1024,
    maxSyncMessageBytes: 64 * 1024,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1000,
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1024 * 1024,
    maxPendingBytesPerPeer: 1024 * 1024,
    maxPendingBytesPerReplica: 1024 * 1024,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1000,
    maxPendingChangesPerPeer: 1000,
    maxPendingChangesPerReplica: 1000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 8,
    maxStreamsPerSession: 8,
    maxInFlightPerSession: 32,
    maxQueuedRpc: 128,
    maxQueuedPermits: 128,
    maxActiveRestores: 128,
    maxRestoresPerSession: 32,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Limits = ReplicaLimits.layer(limits)
  const authorityToken = CheckpointAuthority.AuthorizationToken.make(Uint8Array.of(7, 8, 9))
  const issuedByAuthority = (token: CheckpointAuthority.AuthorizationToken) =>
    Encoding.encodeBase64(token) === Encoding.encodeBase64(authorityToken)
  const checkpointAuthority: CheckpointAuthority.Implementation = {
    signManifest: () => Effect.succeed(Option.some(authorityToken)),
    verifyManifest: (claims, token) =>
      Effect.gen(function*() {
        if (issuedByAuthority(token)) return
        yield* Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Invalid backup checkpoint authorization"
            })
          })
        )
      }),
    signTransition: () => Effect.succeed(Option.some(authorityToken)),
    verifyTransition: (claims, token) =>
      Effect.gen(function*() {
        if (issuedByAuthority(token)) return
        yield* Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Invalid backup transition authorization"
            })
          })
        )
      })
  }
  const Live = SqlReplica.servicesLayerWithBindings(definition, { projections: [] }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, Limits))
  )
  const AuthorityLive = SqlReplica.servicesLayerWithBindings(definition, {
    projections: [],
    checkpointAuthority
  }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, Limits))
  )
  const ProjectedLive = SqlReplica.servicesLayerWithBindings(projectedDefinition, {
    projections: [TaskListSql]
  }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, Limits))
  )
  const seedRelayState = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const current = yield* ReplicaGate.ReplicaGate.pipe(Effect.flatMap((gate) => gate.current))
      const relayMessageId = "rly_00000000-0000-4000-8000-000000000001"
      const senderPeerId = "peer_00000000-0000-4000-8000-000000000001"
      const remotePeerId = "peer_00000000-0000-4000-8000-000000000002"
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
        commit_sequence, accepted_at, writer_provenance, relay_sender_tenant_id,
        relay_sender_subject_id, relay_sender_peer_id, relay_message_id,
        relay_outer_envelope_digest, relay_receipt_expires_at, relay_encoded_size
      ) VALUES (
        ${current.incarnation}, ${remotePeerId}, 'sender-epoch', 0, ${documentId},
        ${"a".repeat(64)}, NULL, NULL, NULL, '[]', '[]', 0,
        '2026-07-25T00:00:00.000Z', '[]', 'tenant', 'sender', ${senderPeerId},
        ${relayMessageId}, ${"b".repeat(64)}, '2026-08-02T00:00:00.000Z', 128
      )`
      yield* sql`INSERT INTO effect_local_peer_relay_receipt_usage (
        replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id,
        receipt_count, encoded_bytes
      ) VALUES (${current.incarnation}, 'tenant', 'sender', ${senderPeerId}, 1, 128)`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox (
        replica_id, replica_incarnation, writer_generation, expected_local_tenant_id,
        expected_local_subject_id, expected_local_peer_id, remote_tenant_id,
        remote_subject_id, remote_peer_id, relay_peer_id, relay_message_id,
        outer_envelope_digest, protocol_version, payload_version, sender_connection_epoch,
        sender_sequence, document_id, document_type, writer_provenance, message_hash,
        payload, encoded_size, created_at, retry_deadline, next_attempt_at
      ) VALUES (
        ${current.replicaId}, ${current.incarnation}, ${current.writerGeneration},
        'tenant', 'local', ${senderPeerId},
        'tenant', 'remote', ${remotePeerId}, ${senderPeerId}, ${relayMessageId},
        ${"b".repeat(64)}, 1, 1, 'sender-epoch', 0, ${documentId}, ${Task.name}, '[]',
        ${"a".repeat(64)}, ${Uint8Array.of(1)}, 1, '2026-07-25T00:00:00.000Z',
        '2026-08-01T00:00:00.000Z', '2026-07-25T00:00:00.000Z'
      )`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_remote_usage (
        replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id,
        message_count, encoded_bytes
      ) VALUES (${current.incarnation}, 'tenant', 'remote', ${remotePeerId}, 1, 1)`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_replica_usage (
        replica_incarnation, message_count, encoded_bytes
      ) VALUES (${current.incarnation}, 1, 1)`
    })
  const smallArchiveRecords = 8
  const SmallLimits = ReplicaLimits.layer({ ...limits, maxArchiveRecords: smallArchiveRecords })
  const SmallLive = SqlReplica.servicesLayerWithBindings(definition, { projections: [] }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, SmallLimits))
  )
  const CommitRename = Mutation.make("CommitRename", {
    document: Task,
    payload: Schema.String,
    success: Schema.String
  })
  const mutatedDefinition = ReplicaDefinition.make({
    name: "mutated-backup-tasks",
    documents: DocumentSet.make(Task),
    mutations: [CommitRename],
    projections: [],
    queries: []
  })
  const MutatedHandlers = CommitRename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return payload
  })
  const MutatedLive = SqlReplica.servicesLayerWithBindings(mutatedDefinition, { projections: [] }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, Limits, MutatedHandlers))
  )

  it.effect("exports and restores canonical history as projection ready when no projections are registered", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "before" })
      yield* sql`UPDATE effect_local_changes
        SET writer_schema_version = 7, writer_definition_hash = 'historical-definition'
        WHERE document_id = ${documentId}`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "after"
      })
      const changed = yield* store.persist(Task, documentId, created, staged)
      InternalAutomerge.free(changed.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "before")
      assert.strictEqual(restored.snapshot.projection, "Ready")
      assert.strictEqual(restored.historyChanges, created.historyChanges)
      assert.strictEqual(restored.historyOperations, created.historyOperations)
      assert.strictEqual(restored.historyBytes, created.historyBytes)
      assert.deepStrictEqual(
        yield* sql<{
          readonly writer_definition_hash: string
          readonly writer_schema_version: number
        }>`SELECT writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE document_id = ${documentId}`,
        [{ writer_definition_hash: "historical-definition", writer_schema_version: 7 }]
      )
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("retains checkpoint writer provenance after pruned history is restored", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_changes
        SET writer_schema_version = 7, writer_definition_hash = 'historical-definition'
        WHERE document_id = ${documentId}`
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      const latest = yield* compaction.compact(Task, documentId)
      assert.strictEqual(yield* compaction.prune(documentId), 1)

      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const checkpoints = yield* sql<{ readonly writer_provenance: string }>`
        SELECT writer_provenance FROM effect_local_checkpoints
        WHERE checkpoint_hash = ${latest.checkpoint.checkpointHash}
      `
      assert.deepStrictEqual(
        Schema.decodeSync(Schema.fromJsonString(Schema.Unknown))(checkpoints[0].writer_provenance),
        latest.checkpoint.writerProvenance
      )
      assert.isTrue(latest.checkpoint.writerProvenance.some((entry) =>
        entry.writerSchemaVersion === 7 && entry.writerDefinitionHash === "historical-definition"
      ))
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "two")
      assert.isNull(restored.historyChanges)
      assert.isNull(restored.historyOperations)
      assert.isNull(restored.historyBytes)

      InternalAutomerge.free(restored.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("restores legacy format one checkpoints without explicit writer provenance", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "legacy" })
      yield* compaction.compact(Task, documentId)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const checkpoint = lines.find((line) => line.kind === "Checkpoint")!
      delete checkpoint.value.writer_provenance
      const change = lines.find((line) => line.kind === "Change")!
      change.value.writer_definition_hash = "local"
      const archive = yield* encodeArchive(lines)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "legacy")
      const provenance = yield* sql<{ readonly writer_definition_hash: string }>`
        SELECT writer_definition_hash FROM effect_local_changes WHERE document_id = ${documentId}
      `
      assert.deepStrictEqual(provenance, [{ writer_definition_hash: definition.hash }])
      const restoredCheckpoints = yield* sql<{ readonly writer_provenance: string }>`
        SELECT writer_provenance FROM effect_local_checkpoints WHERE document_id = ${documentId}
      `
      assert.deepStrictEqual(decodeJson(restoredCheckpoints[0].writer_provenance), [{
        changeHash: change.value.change_hash,
        writerDefinitionHash: definition.hash,
        writerSchemaVersion: change.value.writer_schema_version
      }])
      InternalAutomerge.free(restored.automerge)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("recomputes complete checkpoint backed history instead of trusting archive counters", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "measured" })
      yield* compaction.compact(Task, documentId)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const document = lines.find((line) => line.kind === "Document")!
      document.value.history_changes = null
      document.value.history_operations = "malformed"
      delete document.value.history_bytes
      const archive = yield* encodeArchive(lines)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.historyChanges, created.historyChanges)
      assert.strictEqual(restored.historyOperations, created.historyOperations)
      assert.strictEqual(restored.historyBytes, created.historyBytes)
      const staged = yield* store.stage(restored, (draft) => {
        draft.title = "persisted"
      })
      const persisted = yield* store.persist(Task, documentId, restored, staged)
      assert.strictEqual(persisted.snapshot.value.title, "persisted")
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(restored.automerge)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("keeps checkpoint backed history unmeasured when a retained dependency is missing", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      yield* compaction.compact(Task, documentId)
      yield* sql`DELETE FROM effect_local_changes
        WHERE document_id = ${documentId}
          AND change_hash != ${persisted.materializedHeads[0]}`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "two")
      assert.isNull(restored.historyChanges)
      assert.isNull(restored.historyOperations)
      assert.isNull(restored.historyBytes)
      InternalAutomerge.free(restored.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects an extra disconnected applied head", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const disconnectedId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "connected" })
      yield* compaction.compact(Task, documentId)
      const disconnected = yield* store.create(Task, disconnectedId, { title: "disconnected" })
      yield* sql`UPDATE effect_local_changes
        SET document_id = ${documentId}
        WHERE document_id = ${disconnectedId}`
      yield* sql`DELETE FROM effect_local_documents WHERE document_id = ${disconnectedId}`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      const error = yield* Effect.flip(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      InternalAutomerge.free(disconnected.automerge)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("preserves a rewritten document's lineage across export and restore", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "rewritten" })
      InternalAutomerge.free(created.automerge)
      const lineage = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("rewrite-history")
      )
      assert.notStrictEqual(lineage, Identity.genesisLineage)

      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      assert.strictEqual(lines.find((line) => line.kind === "Document").value.lineage, lineage)
      assert.strictEqual(lines.find((line) => line.kind === "Checkpoint").value.lineage, lineage)

      // `replace` deletes every canonical row before it inserts, so nothing that survives below was
      // carried over from local state: it all came out of the archive.
      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      assert.deepStrictEqual(
        yield* sql<{ readonly lineage: string }>`
          SELECT lineage FROM effect_local_documents WHERE document_id = ${documentId}`,
        [{ lineage }]
      )
      assert.deepStrictEqual(
        yield* sql<{ readonly lineage: string }>`
          SELECT lineage FROM effect_local_checkpoints WHERE document_id = ${documentId}`,
        [{ lineage }]
      )
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "rewritten")
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("round trips compact checkpoint provenance and a multi hop lineage transition chain", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "transitioned" })
      InternalAutomerge.free(created.automerge)
      const firstLineage = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("backup-transition-one")
      )
      const secondLineage = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("backup-transition-two")
      )
      const checkpoints = yield* sql<{
        readonly checkpoint_hash: string
        readonly heads: string
        readonly lineage: string
      }>`SELECT checkpoint_hash, heads, lineage FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      const checkpoint = checkpoints[0]
      const compactProvenance = {
        _tag: "Compact",
        checkpointHash: checkpoint.checkpoint_hash,
        lineage: checkpoint.lineage,
        heads: decodeJson(checkpoint.heads),
        base: { _tag: "Bootstrap" },
        schemaVersion: Task.version,
        writerDefinitionHash: definition.hash,
        authorization: Encoding.encodeBase64(authorityToken)
      }
      yield* sql`UPDATE effect_local_checkpoints
        SET writer_provenance = ${encodeJson(compactProvenance)}
        WHERE checkpoint_hash = ${checkpoint.checkpoint_hash}`
      const transitionsBefore = yield* sql<{
        readonly authorization: Uint8Array | null
        readonly checkpoint_hash: string
        readonly created_at: string
        readonly document_id: string
        readonly heads: string
        readonly lineage: string
        readonly prior_checkpoint_hash: string
        readonly prior_heads: string
        readonly prior_lineage: string
        readonly prior_snapshot: Uint8Array
        readonly schema_version: number
        readonly writer_definition_hash: string
      }>`SELECT * FROM effect_local_lineage_transitions ORDER BY created_at, lineage`
      assert.strictEqual(transitionsBefore.length, 2)
      const firstTransition = transitionsBefore.find((transition) => transition.lineage === firstLineage)!
      const secondTransition = transitionsBefore.find((transition) => transition.lineage === secondLineage)!
      assert.strictEqual(firstTransition.prior_lineage, Identity.genesisLineage)
      assert.strictEqual(secondTransition.prior_lineage, firstLineage)

      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      assert.strictEqual(lines.filter((line) => line.kind === "Transition").length, 2)
      const archivedCheckpoint = lines.find((line) => line.kind === "Checkpoint")!
      assert.deepStrictEqual(archivedCheckpoint.value.writer_provenance, compactProvenance)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const transitionsAfter = yield* sql<typeof transitionsBefore[number]>`
        SELECT * FROM effect_local_lineage_transitions ORDER BY created_at, lineage
      `
      assert.deepStrictEqual(transitionsAfter, transitionsBefore)
      const restoredCheckpoint = yield* sql<{ readonly writer_provenance: string }>`
        SELECT writer_provenance FROM effect_local_checkpoints WHERE checkpoint_hash = ${checkpoint.checkpoint_hash}
      `
      assert.deepStrictEqual(decodeJson(restoredCheckpoint[0].writer_provenance), compactProvenance)
      assert.deepStrictEqual(
        yield* sql<{ readonly lineage: string }>`
          SELECT lineage FROM effect_local_documents WHERE document_id = ${documentId}`,
        [{ lineage: secondLineage }]
      )
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "transitioned")
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("rejects invalid compact checkpoint authorization before replacement", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const archived = yield* store.create(Task, documentId, { title: "archived" })
      InternalAutomerge.free(archived.automerge)
      yield* compaction.compact(Task, documentId)
      const checkpoint = (yield* sql<{
        readonly checkpoint_hash: string
        readonly heads: string
        readonly lineage: string
      }>`SELECT checkpoint_hash, heads, lineage FROM effect_local_checkpoints WHERE document_id = ${documentId}`)[0]
      yield* sql`UPDATE effect_local_checkpoints SET writer_provenance = ${
        encodeJson({
          _tag: "Compact",
          checkpointHash: checkpoint.checkpoint_hash,
          lineage: checkpoint.lineage,
          heads: decodeJson(checkpoint.heads),
          base: { _tag: "Bootstrap" },
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: Encoding.encodeBase64(Uint8Array.of(8))
        })
      } WHERE checkpoint_hash = ${checkpoint.checkpoint_hash}`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const current = yield* store.load(Task, documentId)
      const staged = yield* store.stage(current, (draft) => {
        draft.title = "preserved"
      })
      const preserved = yield* store.persist(Task, documentId, current, staged)
      InternalAutomerge.free(preserved.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(current.automerge)

      const result = yield* Effect.result(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure.reason._tag, "CheckpointRejected")
      const after = yield* store.load(Task, documentId)
      assert.strictEqual(after.snapshot.value.title, "preserved")
      assert.deepStrictEqual(yield* sql`SELECT installation_id FROM effect_local_backup_installations`, [])
      InternalAutomerge.free(after.automerge)
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("rejects invalid transition authorization before document installation", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "transition" })
      InternalAutomerge.free(created.automerge)
      yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("invalid-backup-transition-authorization")
      )
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const transition = lines.find((line) => line.kind === "Transition")!
      transition.value.authorization = Encoding.encodeBase64(Uint8Array.of(8))
      const archive = yield* encodeArchive(lines)
      yield* sql`DELETE FROM effect_local_documents WHERE document_id = ${documentId}`

      const result = yield* Effect.result(backups.installDocument(Task, {
        installationId: yield* Identity.makeBackupInstallationId,
        documentId,
        source: Stream.make(archive),
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure.reason._tag, "CheckpointRejected")
      assert.deepStrictEqual(yield* sql`SELECT document_id FROM effect_local_documents`, [])
      assert.deepStrictEqual(yield* sql`SELECT installation_id FROM effect_local_backup_installations`, [])
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("removes preexisting lineage transitions during replacement", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "replacement" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      yield* sql`INSERT INTO effect_local_lineage_transitions ${
        sql.insert({
          document_id: documentId,
          prior_lineage: Identity.genesisLineage,
          prior_checkpoint_hash: "1".repeat(64),
          prior_heads: "[]",
          prior_snapshot: Uint8Array.of(1),
          lineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000099"),
          checkpoint_hash: "2".repeat(64),
          heads: "[]",
          schema_version: Task.version,
          writer_definition_hash: definition.hash,
          authorization: null,
          created_at: "2026-08-09T00:00:00.000Z"
        })
      }`

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_local_lineage_transitions`, [])
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("rejects conflicting lineage transition forks", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "forked" })
      InternalAutomerge.free(created.automerge)
      yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("backup-fork-one")
      )
      yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("backup-fork-two")
      )
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const transitions = lines.filter((line) => line.kind === "Transition")
      assert.strictEqual(transitions.length, 2)
      transitions[1]!.value.prior_lineage = transitions[0]!.value.prior_lineage
      const archive = yield* encodeArchive(lines)

      const error = yield* Effect.flip(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(error.reason._tag, "BackupInvalid")
    }).pipe(Effect.provide(AuthorityLive)))

  it.effect("rejects checkpoint and change provenance conflicts during restore", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      yield* compaction.compact(Task, documentId)
      yield* sql`UPDATE effect_local_changes
        SET writer_definition_hash = 'conflicting-definition'
        WHERE document_id = ${documentId}`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      yield* sql`UPDATE effect_local_changes
        SET writer_definition_hash = ${definition.hash}
        WHERE document_id = ${documentId}`

      const restored = yield* Effect.result(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.isTrue(Result.isFailure(restored))
      if (Result.isFailure(restored)) {
        assert.strictEqual(restored.failure.reason._tag, "BackupInvalid")
      }
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      InternalAutomerge.free(preserved.automerge)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rebuilds registered projections from restored canonical documents", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const projections = yield* ProjectionStore.ProjectionStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "before" })
      yield* projections.replaceDocument(Task, created.snapshot, created.commitSequence, "Fresh")
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "after"
      })
      const changed = yield* store.persist(Task, documentId, created, staged)
      yield* projections.replaceDocument(Task, changed.snapshot, changed.commitSequence, "Fresh")
      InternalAutomerge.free(changed.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: projectedDefinition.hash
      })
      const rows = yield* sql`SELECT source_document_id, title FROM task_list_v1`
      assert.deepStrictEqual(rows, [{ source_document_id: documentId, title: "before" }])
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.projection, "Ready")
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(ProjectedLive)))

  it.effect("restores canonical and projection state across insert batch boundaries", () =>
    Effect.scoped(Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const archivedPermit = yield* Effect.scoped(gate.shared)
      const checkpointHashes: Array<string> = []
      const commandIds: Array<Identity.CommandId> = []
      const expected = new Map<Identity.DocumentId, string>()
      for (let index = 0; index < 51; index++) {
        const documentId = yield* Identity.makeDocumentId
        const commandId = yield* Identity.makeCommandId
        const title = `task-${index}`
        const encoded = yield* Document.encode(Task, documentId, { title })
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: archivedPermit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded
        })
        const outcome = yield* executor.create(Task, {
          commandId,
          documentId,
          permit: archivedPermit,
          requestHash,
          value: { title }
        })
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
        const compacted = yield* compaction.compact(Task, documentId)
        assert.isTrue(compacted.published)
        checkpointHashes.push(compacted.checkpoint.checkpointHash)
        commandIds.push(commandId)
        expected.set(documentId, title)
      }
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: projectedDefinition.hash
      })

      const documents = yield* sql<{
        readonly document_id: string
        readonly projection_status: string
      }>`SELECT document_id, projection_status FROM effect_local_documents ORDER BY document_id`
      const projected = yield* sql<{
        readonly source_document_id: string
        readonly title: string
      }>`SELECT source_document_id, title FROM task_list_v1 ORDER BY source_document_id`
      const restoredCheckpoints = yield* sql<{ readonly checkpoint_hash: string }>`
        SELECT checkpoint_hash FROM effect_local_checkpoints ORDER BY checkpoint_hash
      `
      const restoredReceipts = yield* sql<{ readonly command_id: string }>`
        SELECT command_id FROM effect_local_command_receipts ORDER BY command_id
      `
      assert.strictEqual(documents.length, 51)
      assert.isTrue(documents.every((row) => row.projection_status === "Ready"))
      assert.strictEqual(projected.length, 51)
      for (const row of projected) {
        assert.strictEqual(row.title, expected.get(Identity.DocumentId.make(row.source_document_id)))
      }
      assert.deepStrictEqual(
        restoredCheckpoints.map((row) => row.checkpoint_hash),
        checkpointHashes.toSorted()
      )
      assert.deepStrictEqual(
        restoredReceipts.map((row) => row.command_id),
        commandIds.toSorted()
      )
      const representativeCommand = commandIds[0]
      const representativeDocument = [...expected.keys()][0]
      assert.deepStrictEqual(
        yield* executor.lookupCreate(representativeCommand, archivedPermit),
        CommandOutcome.durablyCommitted(representativeCommand, representativeDocument)
      )
      const currentPermit = yield* gate.current
      assert.deepStrictEqual(
        yield* executor.lookupCreate(representativeCommand, currentPermit),
        CommandOutcome.unknown(representativeCommand)
      )
    })).pipe(Effect.provide(ProjectedLive)))

  it.effect("retires cluster request and reply state during restore", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "relay state" })
      InternalAutomerge.free(created.automerge)
      yield* seedRelayState(documentId)
      yield* sql`CREATE TABLE ${sql(`${ClusterStorage.messagePrefix}_messages`)} (id INTEGER PRIMARY KEY)`
      yield* sql`CREATE TABLE ${sql(`${ClusterStorage.messagePrefix}_replies`)} (id INTEGER PRIMARY KEY)`
      yield* sql`INSERT INTO ${sql(`${ClusterStorage.messagePrefix}_messages`)} (id) VALUES (1)`
      yield* sql`INSERT INTO ${sql(`${ClusterStorage.messagePrefix}_replies`)} (id) VALUES (1)`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const rows = yield* sql<{
        readonly deleteTokens: number
        readonly messages: number
        readonly outbox: number
        readonly outboxRemoteUsage: number
        readonly outboxReplicaUsage: number
        readonly receipts: number
        readonly receiptUsage: number
        readonly replies: number
      }>`SELECT
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}) AS messages,
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)}) AS replies,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox) AS outbox,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_remote_usage) AS outboxRemoteUsage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_replica_usage) AS outboxReplicaUsage,
        (SELECT COUNT(*) FROM effect_local_peer_receipts WHERE relay_message_id IS NOT NULL) AS receipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS receiptUsage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`
      assert.deepStrictEqual(rows[0], {
        deleteTokens: 0,
        messages: 0,
        outbox: 0,
        outboxRemoteUsage: 0,
        outboxReplicaUsage: 0,
        receipts: 0,
        receiptUsage: 0,
        replies: 0
      })
    }).pipe(Effect.provide(Live)))

  it.effect("reports invalid local rows as storage corruption during export", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_documents SET schema_version = 1.5 WHERE document_id = ${documentId}`
      const result = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(
        Stream.runCollect,
        Effect.result
      )
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
        if (result.failure.reason._tag === "StorageCorrupt") {
          assert.isTrue(Schema.is(Schema.Error())(result.failure.reason.cause))
        }
      }
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rolls back restore when its exclusive permit becomes stale", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "archive" })
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "preserved"
      })
      const current = yield* store.persist(Task, documentId, created, staged)
      yield* seedRelayState(documentId)
      yield* sql`CREATE TRIGGER fence_restore
        AFTER DELETE ON effect_local_documents
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const result = yield* Effect.exit(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(result._tag, "Failure")
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      const installations = yield* sql<{ readonly installation_id: string }>`
        SELECT installation_id FROM effect_local_backup_installations
      `
      assert.strictEqual(installations.length, 0)
      const relayRows = yield* sql<{
        readonly deleteTokens: number
        readonly outbox: number
        readonly outboxRemoteUsage: number
        readonly outboxReplicaUsage: number
        readonly receipts: number
        readonly receiptUsage: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox) AS outbox,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_remote_usage) AS outboxRemoteUsage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_replica_usage) AS outboxReplicaUsage,
        (SELECT COUNT(*) FROM effect_local_peer_receipts WHERE relay_message_id IS NOT NULL) AS receipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS receiptUsage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`
      assert.deepStrictEqual(relayRows, [{
        deleteTokens: 0,
        outbox: 1,
        outboxRemoteUsage: 1,
        outboxReplicaUsage: 1,
        receipts: 1,
        receiptUsage: 1
      }])
      InternalAutomerge.free(preserved.automerge)
      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("no-ops a retried restore that reuses an installed installation id", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const archivedId = yield* Identity.makeDocumentId
      const archived = yield* store.create(Task, archivedId, { title: "archived" })
      InternalAutomerge.free(archived.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const restoreArchive = (installationId: Identity.BackupInstallationId) =>
        backups.restore({
          source: Stream.fromIterable(chunks),
          mode: "replace",
          maxBytes: limits.maxBackupBytes,
          expectedDefinitionHash: definition.hash,
          installationId
        })
      const installationId = yield* Identity.makeBackupInstallationId
      yield* restoreArchive(installationId)
      const interveningId = yield* Identity.makeDocumentId
      const intervening = yield* store.create(Task, interveningId, { title: "intervening" })
      InternalAutomerge.free(intervening.automerge)

      yield* restoreArchive(installationId)

      const preserved = yield* store.load(Task, interveningId)
      assert.strictEqual(preserved.snapshot.value.title, "intervening")
      InternalAutomerge.free(preserved.automerge)
      const installations = yield* sql<{ readonly installation_id: string; readonly mode: string }>`
        SELECT installation_id, mode FROM effect_local_backup_installations
      `
      assert.deepStrictEqual(installations, [{ installation_id: installationId, mode: "replace" }])

      yield* restoreArchive(yield* Identity.makeBackupInstallationId)
      const erased = yield* Effect.flip(store.load(Task, interveningId))
      assert.strictEqual(erased.reason._tag, "DocumentNotFound")
    }).pipe(Effect.provide(Live)))

  it.effect("keeps rewrite idempotency reachable after retrying one backup installation", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "archived" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const installationId = yield* Identity.makeBackupInstallationId
      const restore = backups.restore({
        installationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      yield* restore
      const operationId = Compaction.OperationId.make("rewrite-after-restore")
      const firstLineage = yield* compaction.rewriteHistory(Task, documentId, operationId)
      const permitBeforeRetry = yield* gate.current
      const checkpointBeforeRetry = yield* sql<{
        readonly bytes: Uint8Array
        readonly checkpoint_hash: string
        readonly commit_sequence: number
        readonly lineage: string
      }>`SELECT bytes, checkpoint_hash, commit_sequence, lineage
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`

      yield* restore
      const permitAfterRetry = yield* gate.current
      const replayed = yield* compaction.rewriteHistory(Task, documentId, operationId)
      const checkpointAfterReplay = yield* sql<{
        readonly bytes: Uint8Array
        readonly checkpoint_hash: string
        readonly commit_sequence: number
        readonly lineage: string
      }>`SELECT bytes, checkpoint_hash, commit_sequence, lineage
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      const markers = yield* sql<{
        readonly document_id: string
        readonly lineage: string
        readonly operation_id: string
        readonly replica_incarnation: number
      }>`SELECT replica_incarnation, operation_id, document_id, lineage
        FROM effect_local_history_rewrites ORDER BY replica_incarnation`

      assert.deepStrictEqual(markers, [{
        replica_incarnation: permitBeforeRetry.incarnation,
        operation_id: operationId,
        document_id: documentId,
        lineage: firstLineage
      }])
      assert.deepStrictEqual(permitAfterRetry, permitBeforeRetry)
      assert.strictEqual(replayed, firstLineage)
      assert.deepStrictEqual(checkpointAfterReplay, checkpointBeforeRetry)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects an installation id reused for a different restore request", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "archived" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const installationId = yield* Identity.makeBackupInstallationId
      yield* backups.restore({
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash,
        installationId
      })

      const error = yield* Effect.flip(backups.restore({
        source: Stream.fromIterable(chunks),
        mode: "clone",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash,
        installationId
      }))
      assert.strictEqual(error.reason._tag, "BackupInvalid")
      const sql = yield* SqlClient.SqlClient
      const installations = yield* sql<{ readonly installation_id: string; readonly mode: string }>`
        SELECT installation_id, mode FROM effect_local_backup_installations
      `
      assert.deepStrictEqual(installations, [{ installation_id: installationId, mode: "replace" }])
    }).pipe(Effect.provide(Live)))

  it.effect("rolls back the claimed generation when checksum-valid records fail insertion", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const document = lines.find((line) => line.kind === "Document")!
      lines.splice(-1, 0, { ...document, value: { ...document.value } })
      const archive = yield* encodeArchive(lines)
      const before = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`

      const restored = yield* Effect.result(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.isTrue(Result.isFailure(restored))
      if (Result.isFailure(restored)) {
        assert.strictEqual(restored.failure.reason._tag, "BackupInvalid")
        if (restored.failure.reason._tag === "BackupInvalid") {
          assert.isTrue(Schema.is(Schema.Error())(restored.failure.reason.cause))
        }
      }
      const after = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(after, before)
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      InternalAutomerge.free(preserved.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects checksum-valid corrupt canonical history without replacing the replica", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const change = lines.find((line) => line.kind === "Change")!
      change.value.bytes = change.value.bytes.replace(/[^=]/g, "A")
      const archive = yield* encodeArchive(lines)

      const restored = yield* Effect.exit(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(restored._tag, "Failure")
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      InternalAutomerge.free(preserved.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("reports invalid archive document ids as BackupInvalid", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      for (const record of lines.slice(1, -1)) {
        if ("document_id" in record.value) record.value.document_id = "invalid-document-id"
      }
      const archive = yield* encodeArchive(lines)

      const result = yield* Effect.result(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure.reason._tag, "BackupInvalid")
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      InternalAutomerge.free(preserved.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects malformed and oversized archives without modifying the replica", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const archive = concatenate(chunks)
      const sourceLines = new TextDecoder().decode(archive).trimEnd().split("\n")

      const declaredLines = sourceLines.map(decodeJson)
      declaredLines[0]!.value.declaredBytes += 1
      declaredLines[0]!.checksum = yield* Canonical.digest(declaredLines[0]!.value)
      const declaredSize = new TextEncoder().encode(`${declaredLines.map(encodeJson).join("\n")}\n`)

      const checksumLines = sourceLines.map(decodeJson)
      checksumLines[1]!.checksum = "invalid"
      const checksum = new TextEncoder().encode(`${checksumLines.map(encodeJson).join("\n")}\n`)

      const malformed = new TextEncoder().encode(`${sourceLines[0]}${sourceLines.slice(1).join("\n")}\n`)
      const before = yield* sql<{
        readonly changes: number
        readonly commit_sequence: number
        readonly documents: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
        (SELECT COUNT(*) FROM effect_local_documents) AS documents`
      const cases: ReadonlyArray<{
        readonly maxBytes: number
        readonly name: string
        readonly source: ReadonlyArray<Uint8Array>
      }> = [
        { name: "malformed framing", source: [malformed], maxBytes: limits.maxBackupBytes },
        { name: "declared size", source: [declaredSize], maxBytes: limits.maxBackupBytes },
        { name: "checksum", source: [checksum], maxBytes: limits.maxBackupBytes },
        {
          name: "per chunk limit",
          source: [new Uint8Array(limits.maxChunkBytes + 1)],
          maxBytes: limits.maxBackupBytes
        },
        { name: "total limit", source: [archive], maxBytes: archive.byteLength - 1 }
      ]

      for (const testCase of cases) {
        const result = yield* Effect.exit(backups.restore({
          installationId: yield* Identity.makeBackupInstallationId,
          source: Stream.fromIterable(testCase.source),
          mode: "replace",
          maxBytes: testCase.maxBytes,
          expectedDefinitionHash: definition.hash
        }))
        assert.strictEqual(result._tag, "Failure", testCase.name)
        const after = yield* sql<{
          readonly changes: number
          readonly commit_sequence: number
          readonly documents: number
        }>`SELECT
          (SELECT COUNT(*) FROM effect_local_changes) AS changes,
          (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
          (SELECT COUNT(*) FROM effect_local_documents) AS documents`
        assert.deepStrictEqual(after, before, testCase.name)
        const preserved = yield* store.load(Task, documentId)
        assert.strictEqual(preserved.snapshot.value.title, "preserved", testCase.name)
        InternalAutomerge.free(preserved.automerge)
      }
    }).pipe(Effect.provide(Live)))

  it.effect("bounds export chunks to maxChunkBytes and restores records larger than one chunk", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      let seed = 7
      const characters: Array<string> = []
      for (let index = 0; index < 40_000; index++) {
        seed = (seed * 48271) % 2147483647
        characters.push(String.fromCharCode(0xc0 + (seed % 0x300)))
      }
      const title = characters.join("")
      const created = yield* store.create(Task, documentId, { title })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      for (const chunk of chunks) {
        assert.isAtMost(chunk.byteLength, limits.maxChunkBytes)
      }
      const changeRecord = new TextDecoder().decode(concatenate(chunks)).trimEnd().split("\n")
        .find((line) => decodeJson(line).kind === "Change")!
      assert.isAbove(new TextEncoder().encode(changeRecord).byteLength, limits.maxChunkBytes)
      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, title)
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects maxBytes values that are not positive integers", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "kept" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      for (const maxBytes of [Number.NaN, Number.POSITIVE_INFINITY, 0, -1, 1.5]) {
        const exported = yield* Effect.flip(backups.export({ maxBytes }).pipe(Stream.runCollect))
        assert.strictEqual(exported.reason._tag, "BackupInvalid", `export maxBytes=${maxBytes}`)
        const restored = yield* Effect.flip(backups.restore({
          installationId: yield* Identity.makeBackupInstallationId,
          source: Stream.fromIterable(chunks),
          mode: "replace",
          maxBytes,
          expectedDefinitionHash: definition.hash
        }))
        assert.strictEqual(restored.reason._tag, "BackupInvalid", `restore maxBytes=${maxBytes}`)
      }
    }).pipe(Effect.provide(Live)))

  it.effect("rejects archives beyond the record limit while streaming", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const line = new TextEncoder().encode(`{"kind":"Padding","checksum":"0","value":0}\n`)
      const error = yield* Effect.flip(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(
          Array.from({ length: limits.maxArchiveRecords + 10 }, () => line)
        ).pipe(
          Stream.concat(Stream.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: nativeError("source tail was pulled")
              })
            })
          ))
        ),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, limits.maxArchiveRecords)
      }
    }).pipe(Effect.provide(Live)))

  it.effect("rejects a caller limit above the owner cap before reading", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const result = yield* Effect.exit(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.never,
        mode: "replace",
        maxBytes: limits.maxBackupBytes + 1,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("rejects an export after another writer fences the local replica", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const sql = yield* SqlClient.SqlClient
      const nextReplicaId = yield* Identity.makeReplicaId
      yield* sql`UPDATE effect_local_metadata
        SET replica_id = ${nextReplicaId},
            replica_incarnation = replica_incarnation + 1,
            writer_generation = writer_generation + 1
        WHERE singleton = 1`

      const result = yield* Effect.exit(
        backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      )

      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("rejects archive JSON deeper than the configured limit", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const lines = archiveLinesOf(
        yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      )
      let padding: unknown = "leaf"
      for (let depth = 0; depth <= limits.maxJsonDepth; depth++) padding = { value: padding }
      lines[0]!.value.padding = padding
      const archive = yield* encodeArchive(lines)
      const result = yield* Effect.exit(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(archive),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("exports the current identity after clone and replace restore", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const source = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const manifest = (chunks: ReadonlyArray<Uint8Array>) => archiveLinesOf(chunks)[0]!.value
      const initial = manifest(source)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(source),
        mode: "clone",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const cloned = manifest(yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect))
      assert.notStrictEqual(cloned.replicaId, initial.replicaId)
      assert.strictEqual(cloned.incarnation, initial.incarnation + 1)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(source),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const replaced = manifest(yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect))
      assert.strictEqual(replaced.replicaId, initial.replicaId)
      assert.strictEqual(replaced.incarnation, cloned.incarnation + 1)
    }).pipe(Effect.provide(Live)))

  it.effect("orphans archived receipts when restoring onto a replica with a lower incarnation", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const backups = yield* BackupStore.BackupStore
        const executor = yield* CommandExecutor.CommandExecutor
        const gate = yield* ReplicaGate.ReplicaGate
        yield* gate.claim(() => Effect.void)
        const permit = yield* gate.current
        const documentId = yield* Identity.makeDocumentId
        const commandId = yield* Identity.makeCommandId
        const encoded = yield* Document.encode(Task, documentId, { title: "archived" })
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded
        })
        const outcome = yield* executor.create(Task, {
          commandId,
          documentId,
          permit,
          requestHash,
          value: { title: "archived" }
        })
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
        const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, commandId, incarnation: permit.incarnation }
      }).pipe(Effect.provide(Live))

      yield* Effect.gen(function*() {
        const backups = yield* BackupStore.BackupStore
        const executor = yield* CommandExecutor.CommandExecutor
        const gate = yield* ReplicaGate.ReplicaGate
        yield* backups.restore({
          installationId: yield* Identity.makeBackupInstallationId,
          source: Stream.fromIterable(archive.chunks),
          mode: "replace",
          maxBytes: limits.maxBackupBytes,
          expectedDefinitionHash: definition.hash
        })
        const permit = yield* gate.current
        assert.deepStrictEqual(
          yield* executor.lookupCreate(archive.commandId, permit),
          CommandOutcome.unknown(archive.commandId)
        )
        assert.isAbove(permit.incarnation, archive.incarnation)
      }).pipe(Effect.provide(Live))
    }))

  it.effect("rejects archived receipts recorded above the manifest incarnation", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const permit = yield* gate.current
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "archived" })
      const requestHash = yield* CommandExecutor.createRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId,
        encoded
      })
      yield* executor.create(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        value: { title: "archived" }
      })
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const lines = archiveLinesOf(chunks)
      const receipt = lines.find((line) => line.kind === "Receipt")!
      receipt.value.replica_incarnation = lines[0]!.value.incarnation + 5
      const tampered = yield* encodeArchive(lines)
      const before = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.make(tampered),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      const after = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(after, before)
      const receipts = yield* sql<{ readonly replica_incarnation: number }>`
        SELECT replica_incarnation FROM effect_local_command_receipts
      `
      assert.deepStrictEqual(receipts, [{ replica_incarnation: permit.incarnation }])
    }).pipe(Effect.provide(Live)))

  it.effect("uses the Crypto service captured by the BackupStore layer for clone restores", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const source = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: Identity.BackupInstallationId.make("bak_9e8d7c6b-5a4f-4e3d-8c2b-1a0f9e8d7c6b"),
        source: Stream.fromIterable(source),
        mode: "clone",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
    }).pipe(Effect.provide(Live)))

  it.effect("prunes every receipt restored from an archive at a superseded incarnation", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const archivedPermit = yield* gate.current
      const archived: Array<{ readonly commandId: Identity.CommandId; readonly documentId: Identity.DocumentId }> = []
      for (let index = 0; index < 3; index++) {
        const documentId = yield* Identity.makeDocumentId
        const commandId = yield* Identity.makeCommandId
        const title = `archived-${index}`
        const encoded = yield* Document.encode(Task, documentId, { title })
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: archivedPermit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded
        })
        const outcome = yield* executor.create(Task, {
          commandId,
          documentId,
          permit: archivedPermit,
          requestHash,
          value: { title }
        })
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
        archived.push({ commandId, documentId })
      }
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      // Restore reinserts every archived receipt at its original incarnation and then raises metadata
      // past the manifest, so the whole restored set is superseded and the table empties completely.
      const restoredPermit = yield* gate.current
      assert.isAbove(restoredPermit.incarnation, archivedPermit.incarnation)
      const restoredReceipts = yield* sql<{
        readonly command_id: string
        readonly replica_incarnation: number
      }>`SELECT command_id, replica_incarnation FROM effect_local_command_receipts ORDER BY command_id`
      assert.deepStrictEqual(
        restoredReceipts,
        archived.map((entry) => ({
          command_id: entry.commandId,
          replica_incarnation: archivedPermit.incarnation
        })).toSorted((left, right) => {
          if (left.command_id < right.command_id) return -1
          return 1
        })
      )

      assert.strictEqual(yield* compaction.pruneCommandReceipts, archived.length)

      const remaining = yield* sql<{ readonly command_id: string }>`
        SELECT command_id FROM effect_local_command_receipts
      `
      assert.deepStrictEqual(remaining, [])

      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "after-prune" })
      const requestHash = yield* CommandExecutor.createRequestHash({
        incarnation: restoredPermit.incarnation,
        commandId,
        document: Task,
        documentId,
        encoded
      })
      const outcome = yield* executor.create(Task, {
        commandId,
        documentId,
        permit: restoredPermit,
        requestHash,
        value: { title: "after-prune" }
      })
      assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
      assert.deepStrictEqual(
        yield* executor.lookupCreate(commandId, restoredPermit),
        CommandOutcome.durablyCommitted(commandId, documentId)
      )
      assert.deepStrictEqual(
        yield* sql<{
          readonly command_id: string
          readonly replica_incarnation: number
        }>`SELECT command_id, replica_incarnation FROM effect_local_command_receipts`,
        [{ command_id: commandId, replica_incarnation: restoredPermit.incarnation }]
      )
    }).pipe(Effect.provide(Live)))

  it.effect("stops counting superseded receipts toward the archive record limit", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      yield* gate.claim(() => Effect.void)
      const permit = yield* gate.current
      const supersededIncarnation = permit.incarnation - 1
      assert.isAtLeast(supersededIncarnation, 0)
      // Each live command contributes three records to `record_count`: one document, one change, and
      // one receipt. Two live commands are six records, four receipt-only rows take the total to ten,
      // and the stack caps the archive at eight, so the record guard fires and the byte guard does not.
      const liveCommandIds: Array<Identity.CommandId> = []
      for (let index = 0; index < 2; index++) {
        const documentId = yield* Identity.makeDocumentId
        const commandId = yield* Identity.makeCommandId
        const title = `live-${index}`
        const encoded = yield* Document.encode(Task, documentId, { title })
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded
        })
        const outcome = yield* executor.create(Task, {
          commandId,
          documentId,
          permit,
          requestHash,
          value: { title }
        })
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
        liveCommandIds.push(commandId)
      }
      const supersededCommandIds: Array<Identity.CommandId> = []
      for (let index = 0; index < 4; index++) {
        const commandId = yield* Identity.makeCommandId
        const documentId = yield* Identity.makeDocumentId
        yield* sql`INSERT INTO effect_local_command_receipts (
          replica_incarnation, command_id, request_hash, mutation_name, result,
          document_id, heads, commit_sequence
        ) VALUES (
          ${supersededIncarnation}, ${commandId}, ${`hash-${index}`}, ${"$create"},
          ${new TextEncoder().encode("{}")}, ${documentId}, ${"[]"}, ${0}
        )`
        supersededCommandIds.push(commandId)
      }
      const sizing = yield* sql<{
        readonly changes: number
        readonly checkpoints: number
        readonly documents: number
        readonly receipts: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_checkpoints) AS checkpoints,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts`
      assert.deepStrictEqual(sizing, [{ changes: 2, checkpoints: 0, documents: 2, receipts: 6 }])
      const recordCount = sizing[0].documents + sizing[0].changes + sizing[0].checkpoints + sizing[0].receipts

      const error = yield* Effect.flip(
        backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      )
      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, smallArchiveRecords)
        assert.strictEqual(error.reason.observed, recordCount)
      }

      assert.strictEqual(yield* compaction.pruneCommandReceipts, supersededCommandIds.length)

      const remaining = yield* sql<{
        readonly command_id: string
        readonly replica_incarnation: number
      }>`SELECT command_id, replica_incarnation FROM effect_local_command_receipts ORDER BY command_id`
      assert.deepStrictEqual(
        remaining,
        liveCommandIds.map((commandId) => ({
          command_id: commandId,
          replica_incarnation: permit.incarnation
        })).toSorted((left, right) => {
          if (left.command_id < right.command_id) return -1
          return 1
        })
      )

      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      assert.isAbove(chunks.length, 0)
    }).pipe(Effect.provide(SmallLive)))

  // `restore` recomputes the metadata watermark as the maximum `commit_sequence` over every archived
  // record, receipts included, and every restored receipt is superseded the moment the restore raises
  // the incarnation. The first prune therefore empties the table, and the next archive has to recover
  // the same watermark without it. Each replica gets its own `:memory:` database because `Effect.provide`
  // builds `MutatedLive` once per call.
  it.effect("preserves the commit sequence watermark across a restore, a receipt prune, and a re-export", () =>
    Effect.gen(function*() {
      const source = yield* Effect.gen(function*() {
        const backups = yield* BackupStore.BackupStore
        const compaction = yield* Compaction.Compaction
        const executor = yield* CommandExecutor.CommandExecutor
        const gate = yield* ReplicaGate.ReplicaGate
        const sql = yield* SqlClient.SqlClient
        const permit = yield* gate.current
        const documentId = yield* Identity.makeDocumentId
        const createCommandId = yield* Identity.makeCommandId
        const encoded = yield* Document.encode(Task, documentId, { title: "one" })
        const createHash = yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId: createCommandId,
          document: Task,
          documentId,
          encoded
        })
        const created = yield* executor.create(Task, {
          commandId: createCommandId,
          documentId,
          permit,
          requestHash: createHash,
          value: { title: "one" }
        })
        assert.deepStrictEqual(created, CommandOutcome.durablyCommitted(createCommandId, documentId))
        for (const title of ["two", "three"]) {
          const commandId = yield* Identity.makeCommandId
          const requestHash = yield* CommandExecutor.mutationRequestHash({
            incarnation: permit.incarnation,
            commandId,
            documentId,
            mutation: CommitRename,
            payload: title
          })
          const outcome = yield* executor.mutate(CommitRename, {
            commandId,
            documentId,
            payload: title,
            permit,
            requestHash
          })
          assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, title))
          assert.isTrue((yield* compaction.compact(Task, documentId)).published)
        }
        // Pruning applied changes is what makes a receipt a plausible sole carrier of the watermark:
        // it removes the change rows the sequence would otherwise still be recoverable from.
        assert.isAbove(yield* compaction.prune(documentId), 0)
        const watermark = yield* sql<{ readonly commit_sequence: number }>`
          SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1
        `
        const commitSequence = watermark[0].commit_sequence
        assert.isAbove(commitSequence, 0)
        const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, commitSequence }
      }).pipe(Effect.provide(MutatedLive))

      const republished = yield* Effect.gen(function*() {
        const backups = yield* BackupStore.BackupStore
        const compaction = yield* Compaction.Compaction
        const sql = yield* SqlClient.SqlClient
        yield* backups.restore({
          installationId: yield* Identity.makeBackupInstallationId,
          source: Stream.fromIterable(source.chunks),
          mode: "replace",
          maxBytes: limits.maxBackupBytes,
          expectedDefinitionHash: mutatedDefinition.hash
        })
        assert.deepStrictEqual(
          yield* sql<{ readonly commit_sequence: number }>`
            SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1
          `,
          [{ commit_sequence: source.commitSequence }]
        )
        assert.isAbove(yield* compaction.pruneCommandReceipts, 0)
        assert.deepStrictEqual(
          yield* sql<{ readonly command_id: string }>`SELECT command_id FROM effect_local_command_receipts`,
          []
        )
        return yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      }).pipe(Effect.provide(MutatedLive))

      yield* Effect.gen(function*() {
        const backups = yield* BackupStore.BackupStore
        const sql = yield* SqlClient.SqlClient
        yield* backups.restore({
          installationId: yield* Identity.makeBackupInstallationId,
          source: Stream.fromIterable(republished),
          mode: "replace",
          maxBytes: limits.maxBackupBytes,
          expectedDefinitionHash: mutatedDefinition.hash
        })
        assert.deepStrictEqual(
          yield* sql<{ readonly commit_sequence: number }>`
            SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1
          `,
          [{ commit_sequence: source.commitSequence }]
        )
      }).pipe(Effect.provide(MutatedLive))
    }))

  it.effect("reports an unknown outcome for a stale permit once superseded receipts are pruned", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const archivedPermit = yield* Effect.scoped(gate.shared)
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "archived" })
      const requestHash = yield* CommandExecutor.createRequestHash({
        incarnation: archivedPermit.incarnation,
        commandId,
        document: Task,
        documentId,
        encoded
      })
      const outcome = yield* executor.create(Task, {
        commandId,
        documentId,
        permit: archivedPermit,
        requestHash,
        value: { title: "archived" }
      })
      assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        installationId: yield* Identity.makeBackupInstallationId,
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      // A restored receipt still answers a lookup made with the permit it was written under, which is
      // the contract the archived-permit lookup above relies on. Pruning narrows that deliberately.
      assert.deepStrictEqual(
        yield* executor.lookupCreate(commandId, archivedPermit),
        CommandOutcome.durablyCommitted(commandId, documentId)
      )

      assert.strictEqual(yield* compaction.pruneCommandReceipts, 1)

      assert.deepStrictEqual(
        yield* executor.lookupCreate(commandId, archivedPermit),
        CommandOutcome.unknown(commandId)
      )
    }).pipe(Effect.provide(Live)))
})
