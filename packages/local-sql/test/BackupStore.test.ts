import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BackupStore from "../src/BackupStore.js"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlProjection from "../src/SqlProjection.js"

describe("BackupStore", () => {
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
      checksum: "task-list-v1",
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
    maxQueuedRpc: 128
  }
  const Database = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Base))
  const Store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Base, Gate)))
  const Projections = ProjectionStore.layer([]).pipe(Layer.provide(Base))
  const Limits = ReplicaLimits.layer(limits)
  const Backup = BackupStore.layer(definition).pipe(Layer.provide(Layer.mergeAll(Base, Gate, Limits, Projections)))
  const Live = Layer.mergeAll(Base, Gate, Store, Limits, Projections, Backup)
  const ProjectedBootstrap = ReplicaBootstrap.layer(projectedDefinition).pipe(Layer.provide(Database))
  const ProjectedBase = Layer.merge(Database, ProjectedBootstrap)
  const ProjectedGate = ReplicaGate.layer.pipe(Layer.provide(ProjectedBase))
  const ProjectedStore = DocumentStore.layer.pipe(Layer.provide(Layer.merge(ProjectedBase, ProjectedGate)))
  const ProjectedProjections = ProjectionStore.layer([TaskListSql]).pipe(
    Layer.provide(Layer.merge(ProjectedBase, TaskListSql.layer))
  )
  const ProjectedBackup = BackupStore.layer(projectedDefinition).pipe(
    Layer.provide(Layer.mergeAll(ProjectedBase, ProjectedGate, Limits, ProjectedProjections))
  )
  const ProjectedLive = Layer.mergeAll(
    ProjectedBase,
    ProjectedGate,
    ProjectedStore,
    ProjectedProjections,
    Limits,
    ProjectedBackup
  )
  const ProjectedRecovery = Recovery.layer.pipe(Layer.provide(ProjectedLive))
  const ProjectedCompaction = Compaction.layer.pipe(
    Layer.provide(Layer.merge(ProjectedLive, ProjectedRecovery))
  )
  const ProjectedExecutor = CommandExecutor.layer(projectedDefinition).pipe(Layer.provide(ProjectedLive))
  const ProjectedBatchLive = Layer.mergeAll(
    ProjectedLive,
    ProjectedRecovery,
    ProjectedCompaction,
    ProjectedExecutor
  )

  it.effect("exports and restores canonical history as projection ready when no projections are registered", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = Identity.makeDocumentId()
      const created = yield* store.create(Task, documentId, { title: "before" })
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "after"
      })
      const changed = yield* store.persist(Task, documentId, created, staged)
      InternalAutomerge.free(changed.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
      yield* backups.restore({
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const restored = yield* store.load(Task, documentId)
      assert.strictEqual(restored.snapshot.value.title, "before")
      assert.strictEqual(restored.snapshot.projection, "Ready")
      InternalAutomerge.free(restored.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rebuilds registered projections from restored canonical documents", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const projections = yield* ProjectionStore.ProjectionStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const created = yield* store.create(Task, documentId, { title: "before" })
      yield* projections.replaceDocument(Task, created.snapshot, created.commitSequence)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "after"
      })
      const changed = yield* store.persist(Task, documentId, created, staged)
      yield* projections.replaceDocument(Task, changed.snapshot, changed.commitSequence)
      InternalAutomerge.free(changed.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
      yield* backups.restore({
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
        const documentId = Identity.makeDocumentId()
        const commandId = Identity.makeCommandId()
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
      const representativeCommand = commandIds[0]!
      const representativeDocument = [...expected.keys()][0]!
      assert.deepStrictEqual(
        yield* executor.lookupCreate(representativeCommand, archivedPermit),
        CommandOutcome.durablyCommitted(representativeCommand, representativeDocument)
      )
      const currentPermit = yield* gate.current
      assert.deepStrictEqual(
        yield* executor.lookupCreate(representativeCommand, currentPermit),
        CommandOutcome.unknown(representativeCommand)
      )
    })).pipe(Effect.provide(ProjectedBatchLive)))

  it.effect("retires cluster request and reply state during restore", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE ${sql(`${ClusterStorage.messagePrefix}_messages`)} (id INTEGER PRIMARY KEY)`
      yield* sql`CREATE TABLE ${sql(`${ClusterStorage.messagePrefix}_replies`)} (id INTEGER PRIMARY KEY)`
      yield* sql`INSERT INTO ${sql(`${ClusterStorage.messagePrefix}_messages`)} (id) VALUES (1)`
      yield* sql`INSERT INTO ${sql(`${ClusterStorage.messagePrefix}_replies`)} (id) VALUES (1)`
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)

      yield* backups.restore({
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })

      const rows = yield* sql<{ readonly messages: number; readonly replies: number }>`SELECT
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}) AS messages,
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)}) AS replies`
      assert.deepStrictEqual(rows[0], { messages: 0, replies: 0 })
    }).pipe(Effect.provide(Live)))

  it.effect("rolls back restore when its exclusive permit becomes stale", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const created = yield* store.create(Task, documentId, { title: "archive" })
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "preserved"
      })
      const current = yield* store.persist(Task, documentId, created, staged)
      yield* sql`CREATE TRIGGER fence_restore
        AFTER DELETE ON effect_local_documents
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const result = yield* Effect.exit(backups.restore({
        source: Stream.fromIterable(chunks),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(result._tag, "Failure")
      const preserved = yield* store.load(Task, documentId)
      assert.strictEqual(preserved.snapshot.value.title, "preserved")
      InternalAutomerge.free(preserved.automerge)
      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Live)))

  it.effect("rejects checksum-valid corrupt canonical history without replacing the replica", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const documentId = Identity.makeDocumentId()
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const bytes = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0))
      let offset = 0
      for (const chunk of chunks) {
        bytes.set(chunk, offset)
        offset += chunk.byteLength
      }
      const lines = new TextDecoder().decode(bytes).trimEnd().split("\n")
        .map((line) => JSON.parse(line))
      const change = lines.find((line) => line.kind === "Change")!
      change.value.bytes = change.value.bytes.replace(/[^=]/g, "A")
      change.checksum = yield* Canonical.digest(change.value)
      const end = lines.at(-1)!
      end.value.recordsChecksum = yield* Canonical.digest(lines.slice(1, -1).map((line) => line.checksum))
      end.checksum = yield* Canonical.digest(end.value)
      const archive = new TextEncoder().encode(`${lines.map((line) => JSON.stringify(line)).join("\n")}\n`)

      const restored = yield* Effect.exit(backups.restore({
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

  it.effect("rejects malformed and oversized archives without modifying the replica", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const created = yield* store.create(Task, documentId, { title: "preserved" })
      InternalAutomerge.free(created.automerge)
      const chunks = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
      const archive = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0))
      let offset = 0
      for (const chunk of chunks) {
        archive.set(chunk, offset)
        offset += chunk.byteLength
      }
      const sourceLines = new TextDecoder().decode(archive).trimEnd().split("\n")

      const declaredLines = sourceLines.map((line) => JSON.parse(line))
      declaredLines[0]!.value.declaredBytes += 1
      declaredLines[0]!.checksum = yield* Canonical.digest(declaredLines[0]!.value)
      const declaredSize = new TextEncoder().encode(`${declaredLines.map((line) => JSON.stringify(line)).join("\n")}\n`)

      const checksumLines = sourceLines.map((line) => JSON.parse(line))
      checksumLines[1]!.checksum = "invalid"
      const checksum = new TextEncoder().encode(`${checksumLines.map((line) => JSON.stringify(line)).join("\n")}\n`)

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

  it.effect("rejects a caller limit above the owner cap before reading", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const result = yield* Effect.exit(backups.restore({
        source: Stream.never,
        mode: "replace",
        maxBytes: limits.maxBackupBytes + 1,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("rejects archive JSON deeper than the configured limit", () =>
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const archiveText = yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(
        Stream.decodeText(),
        Stream.runFold(() => "", (text, chunk) => text + chunk)
      )
      const lines = archiveText.trimEnd()
        .split("\n")
        .map((line) => JSON.parse(line))
      let padding: unknown = "leaf"
      for (let depth = 0; depth <= limits.maxJsonDepth; depth++) padding = { value: padding }
      lines[0]!.value.padding = padding
      for (let attempt = 0; attempt < 8; attempt++) {
        lines[0]!.checksum = yield* Canonical.digest(lines[0]!.value)
        const bytes = new TextEncoder().encode(`${lines.map((line) => JSON.stringify(line)).join("\n")}\n`)
        if (lines[0]!.value.declaredBytes === bytes.byteLength) break
        lines[0]!.value.declaredBytes = bytes.byteLength
      }
      lines[0]!.checksum = yield* Canonical.digest(lines[0]!.value)
      const archive = new TextEncoder().encode(`${lines.map((line) => JSON.stringify(line)).join("\n")}\n`)
      const result = yield* Effect.exit(backups.restore({
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
      const manifest = (chunks: ReadonlyArray<Uint8Array>) =>
        JSON.parse(new TextDecoder().decode(Uint8Array.from(chunks.flatMap((chunk) => [...chunk]))).split("\n")[0]!)
          .value as { readonly replicaId: string; readonly incarnation: number }
      const initial = manifest(source)

      yield* backups.restore({
        source: Stream.fromIterable(source),
        mode: "clone",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const cloned = manifest(yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect))
      assert.notStrictEqual(cloned.replicaId, initial.replicaId)
      assert.strictEqual(cloned.incarnation, initial.incarnation + 1)

      yield* backups.restore({
        source: Stream.fromIterable(source),
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: definition.hash
      })
      const replaced = manifest(yield* backups.export({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect))
      assert.strictEqual(replaced.replicaId, initial.replicaId)
      assert.strictEqual(replaced.incarnation, cloned.incarnation + 1)
    }).pipe(Effect.provide(Live)))
})
