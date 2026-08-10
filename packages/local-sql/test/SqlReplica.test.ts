import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Backup from "@lucas-barake/effect-local/Backup"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Pull from "effect/Pull"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import type * as SqlStatement from "effect/unstable/sql/Statement"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as Compaction from "../src/Compaction.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as WriterProvenance from "../src/internal/writerProvenance.js"
import * as PeerRelayClientRuntime from "../src/PeerRelayClientRuntime.js"
import * as PeerRelayOutboxLimits from "../src/PeerRelayOutboxLimits.js"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaOperationScheduler from "../src/ReplicaOperationScheduler.js"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"
import * as SqlProjection from "../src/SqlProjection.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { nativeError } from "./TestErrors.js"

describe("SqlReplica", () => {
  const CheckpointProvenanceJson = Schema.Array(Schema.Struct({
    changeHash: WriterProvenance.ChangeHash,
    writerSchemaVersion: WriterProvenance.WriterSchemaVersion,
    writerDefinitionHash: WriterProvenance.WriterDefinitionHash
  }))
  const CompactCheckpointProvenanceJson = Schema.Struct({
    checkpointHash: WriterProvenance.ChangeHash,
    lineage: Identity.DocumentLineage,
    heads: Schema.Array(Schema.String),
    base: Schema.TaggedUnion({
      Bootstrap: {},
      Heads: { baseHeads: Schema.Array(Schema.String) }
    }),
    schemaVersion: WriterProvenance.WriterSchemaVersion,
    writerDefinitionHash: WriterProvenance.WriterDefinitionHash,
    authorization: Schema.String
  })
  const ManifestValue = Schema.Struct({
    formatVersion: Backup.FormatVersion,
    definitionHash: Schema.String,
    replicaId: Identity.ReplicaId,
    incarnation: Identity.ReplicaIncarnation,
    createdAt: Schema.String,
    recordCount: Schema.mutableKey(Schema.Int),
    declaredBytes: Schema.mutableKey(Schema.Int)
  })
  const DocumentValue = Schema.Struct({
    document_id: Identity.DocumentId,
    document_type: Schema.String,
    schema_version: Schema.mutableKey(Schema.Int),
    observed_versions: Schema.String,
    materialized_heads: Schema.String,
    accepted_heads: Schema.mutableKey(Schema.String),
    tombstone: Schema.Int,
    projection_status: Schema.String,
    checkpoint_hash: Schema.NullOr(Schema.String),
    lineage: Identity.DocumentLineage,
    history_changes: Schema.optionalKey(Schema.Unknown),
    history_operations: Schema.optionalKey(Schema.Unknown),
    history_bytes: Schema.optionalKey(Schema.Unknown)
  })
  const ChangeValue = Schema.Struct({
    change_hash: Schema.mutableKey(Schema.String),
    document_id: Schema.String,
    document_type: Schema.String,
    writer_schema_version: WriterProvenance.WriterSchemaVersion,
    writer_definition_hash: WriterProvenance.WriterDefinitionHash,
    actor: Schema.String,
    sequence: Schema.Int,
    dependencies: Schema.String,
    bytes: Schema.String,
    applied: Schema.mutableKey(Schema.Int),
    peer_id: Schema.NullOr(Schema.String),
    accepted_at: Schema.mutableKey(Schema.String),
    commit_sequence: Schema.Int
  })
  const CheckpointValue = Schema.Struct({
    checkpoint_hash: Schema.String,
    document_id: Identity.DocumentId,
    heads: Schema.String,
    bytes: Schema.String,
    checksum: Schema.String,
    commit_sequence: Schema.Int,
    verified: Schema.Int,
    writer_provenance: Schema.optionalKey(
      Schema.Union([CheckpointProvenanceJson, CompactCheckpointProvenanceJson])
    ),
    lineage: Identity.DocumentLineage
  })
  const TransitionValue = Schema.Struct({
    authorization: Schema.NullOr(Schema.String),
    checkpoint_hash: WriterProvenance.ChangeHash,
    created_at: Schema.String,
    document_id: Identity.DocumentId,
    heads: Schema.String,
    lineage: Identity.DocumentLineage,
    prior_checkpoint_hash: WriterProvenance.ChangeHash,
    prior_heads: Schema.String,
    prior_lineage: Identity.DocumentLineage,
    prior_snapshot: Schema.String,
    schema_version: WriterProvenance.WriterSchemaVersion,
    writer_definition_hash: WriterProvenance.WriterDefinitionHash
  })
  const ReceiptValue = Schema.Struct({
    replica_incarnation: Identity.ReplicaIncarnation,
    command_id: Schema.String,
    request_hash: Schema.String,
    mutation_name: Schema.String,
    result: Schema.String,
    document_id: Schema.String,
    heads: Schema.String,
    commit_sequence: Schema.Int
  })
  const EndValue = Schema.Struct({
    recordCount: Schema.mutableKey(Schema.Int),
    recordsChecksum: Schema.mutableKey(Schema.String)
  })
  const ArchiveLine = Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("Manifest"),
      checksum: Schema.mutableKey(Schema.String),
      value: ManifestValue
    }),
    Schema.Struct({
      kind: Schema.Literal("Document"),
      checksum: Schema.mutableKey(Schema.String),
      value: DocumentValue
    }),
    Schema.Struct({ kind: Schema.Literal("Change"), checksum: Schema.mutableKey(Schema.String), value: ChangeValue }),
    Schema.Struct({
      kind: Schema.Literal("Checkpoint"),
      checksum: Schema.mutableKey(Schema.String),
      value: CheckpointValue
    }),
    Schema.Struct({
      kind: Schema.Literal("Transition"),
      checksum: Schema.mutableKey(Schema.String),
      value: TransitionValue
    }),
    Schema.Struct({ kind: Schema.Literal("Receipt"), checksum: Schema.mutableKey(Schema.String), value: ReceiptValue }),
    Schema.Struct({ kind: Schema.Literal("End"), checksum: Schema.mutableKey(Schema.String), value: EndValue })
  ])
  const ArchiveLineJson = Schema.fromJsonString(ArchiveLine)
  const JsonString = Schema.fromJsonString(Schema.Json)
  const encodeLine = (line: ArchiveLine) => Schema.encodeSync(ArchiveLineJson)(line)
  const concatenate = (chunks: ReadonlyArray<Uint8Array>) => {
    const length = chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
    const result = new Uint8Array(length)
    let offset = 0
    for (const chunk of chunks) {
      result.set(chunk, offset)
      offset += chunk.byteLength
    }
    return result
  }
  const archiveLines = (chunks: ReadonlyArray<Uint8Array>) =>
    new TextDecoder().decode(concatenate(chunks)).trimEnd().split("\n").map((line) =>
      Schema.decodeSync(ArchiveLineJson)(line)
    )
  type ArchiveLine = ReturnType<typeof archiveLines>[number]
  const encodeArchive = (lines: Array<ArchiveLine>) =>
    Effect.gen(function*() {
      const manifest = lines[0]
      const records = lines.slice(1, -1)
      const end = lines.at(-1)
      if (!manifest || !end || manifest.kind !== "Manifest" || end.kind !== "End") {
        return yield* Effect.die("Malformed archive fixture")
      }
      for (const record of records) record.checksum = yield* Canonical.digest(record.value)
      end.value.recordCount = records.length
      end.value.recordsChecksum = yield* Canonical.digest(records.map((record) => record.checksum))
      end.checksum = yield* Canonical.digest(end.value)
      manifest.value.recordCount = records.length
      for (let attempt = 0; attempt < 8; attempt++) {
        manifest.checksum = yield* Canonical.digest(manifest.value)
        const encoded = new TextEncoder().encode(
          `${lines.map(encodeLine).join("\n")}\n`
        )
        if (manifest.value.declaredBytes === encoded.byteLength) return encoded
        manifest.value.declaredBytes = encoded.byteLength
      }
      manifest.checksum = yield* Canonical.digest(manifest.value)
      return new TextEncoder().encode(
        `${lines.map(encodeLine).join("\n")}\n`
      )
    })
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
  const Noop = Mutation.make("Noop", { document: Task })
  const TaskTitle = Projection.make("TaskTitle", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) =>
      Match.value(snapshot.tombstone).pipe(
        Match.when(true, () => []),
        Match.orElse(() => [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }])
      )
  })
  const TaskTitleSql = SqlProjection.make(TaskTitle, {
    table: "task_title_v1",
    migrations: [{
      id: 1,
      name: "task_title_v1",
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
  const definition = ReplicaDefinition.make({
    name: "sql-replica",
    documents: DocumentSet.make(Task),
    mutations: [Rename, Noop],
    projections: [],
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
  const Handler = Layer.merge(
    Rename.toLayer(({ draft, payload }) => {
      draft.title = payload
      return undefined
    }),
    Noop.toLayer(() => undefined)
  )
  const Limits = ReplicaLimits.layer(limits)
  const Live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )
  /** The direct replica stack, over a caller supplied database so a test can instrument the client. */
  const directServices = (database: typeof Database, replicaLimits: typeof Limits = Limits) =>
    SqlReplica.layerFromServices(definition).pipe(
      Layer.provideMerge(
        SqlReplica.servicesLayerWithBindings(definition, { projections: [] })
      ),
      Layer.provideMerge(Layer.merge(database, replicaLimits)),
      Layer.provide(Handler)
    )
  const DirectLive = directServices(Database)
  const SlowHealthLive = SqlReplica.layerWithBindings(definition, {
    health: { sampleInterval: "5 seconds" },
    projections: []
  }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )
  const projectedDefinition = ReplicaDefinition.make({
    name: "projected-sql-replica",
    documents: DocumentSet.make(Task),
    mutations: [Rename, Noop],
    projections: [TaskTitle],
    queries: []
  })
  const ProjectedLive = SqlReplica.layerWithBindings(
    projectedDefinition,
    { projections: [TaskTitleSql] }
  ).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )
  const PriorityLimits = ReplicaLimits.layer({ ...limits, maxQueuedRpc: 1 })
  const PriorityLive = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, PriorityLimits))
  )
  const PriorityDirectLive = SqlReplica.layerFromServices(definition).pipe(
    Layer.provideMerge(SqlReplica.servicesLayerWithBindings(definition, { projections: [] })),
    Layer.provide(Layer.mergeAll(Database, Handler, PriorityLimits))
  )

  const makeSqlClient = (
    sql: SqlClient.SqlClient,
    overrides: Record<string, unknown> = {},
    onTemplate?: (strings: TemplateStringsArray) => void
  ): SqlClient.SqlClient => {
    function query<A extends object = SqlConnection.Row,>(
      strings: TemplateStringsArray,
      ...args: Array<unknown>
    ): SqlStatement.Statement<A>
    function query(value: string): SqlStatement.Identifier
    function query<A extends object = SqlConnection.Row,>(
      strings: TemplateStringsArray | string,
      ...args: Array<unknown>
    ): SqlStatement.Statement<A> | SqlStatement.Identifier {
      if (typeof strings === "string") return sql(strings)
      onTemplate?.(strings)
      return sql<A>(strings, ...args)
    }
    return Object.assign(query, sql, overrides)
  }

  const assertInteractivePriority = Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
    const documentId = yield* Identity.makeDocumentId
    const holderEntered = yield* Deferred.make<void>()
    const holderRelease = yield* Deferred.make<void>()
    const completed = yield* Queue.unbounded<string>()
    const holder = yield* Effect.forkChild(
      Effect.scoped(Effect.gen(function*() {
        yield* scheduler.background
        yield* Deferred.succeed(holderEntered, undefined)
        yield* Deferred.await(holderRelease)
      })),
      { startImmediately: true }
    )
    yield* Deferred.await(holderEntered)
    const background = yield* Effect.forkChild(
      Effect.scoped(
        scheduler.background.pipe(Effect.andThen(Queue.offer(completed, "background")))
      ),
      { startImmediately: true }
    )
    const interactive = yield* Effect.forkChild(
      Effect.exit(replica.get(Task, documentId)).pipe(
        Effect.tap(() => Queue.offer(completed, "interactive"))
      ),
      { startImmediately: true }
    )
    const overflow = yield* Effect.forkChild(Effect.exit(Effect.scoped(scheduler.interactive)), {
      startImmediately: true
    })
    // Polled rather than joined first: the point is that admission was refused outright instead of
    // parking, which only a fiber that has already finished can show.
    assert.isDefined(overflow.pollUnsafe())
    assert.isTrue(Exit.isFailure(yield* Fiber.join(overflow)))
    yield* Deferred.succeed(holderRelease, undefined)
    assert.strictEqual(yield* Queue.take(completed), "interactive")
    assert.strictEqual(yield* Queue.take(completed), "background")
    // The interactive operation was admitted and executed: it reaches the store and fails on the
    // absent document, not on refused admission.
    const outcome = yield* Fiber.join(interactive)
    assert.isTrue(Exit.isFailure(outcome))
    if (Exit.isFailure(outcome)) {
      const failure = Cause.findErrorOption(outcome.cause)
      assert.strictEqual(failure._tag, "Some")
      if (failure._tag === "Some") {
        assert.strictEqual(failure.value.reason._tag, "DocumentNotFound")
      }
    }
    yield* Fiber.join(holder)
    yield* Fiber.join(background)
  })

  it.effect("prioritizes interactive work through both replica constructors", () =>
    Effect.gen(function*() {
      yield* assertInteractivePriority.pipe(Effect.provide(PriorityLive))
      yield* assertInteractivePriority.pipe(Effect.provide(PriorityDirectLive))
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer), TestClock.withLive))

  // Real deadline on Fiber.await, as in GateGrantBoundary.test.ts: a strand here must name the
  // assertion that broke instead of surfacing as a bare suite timeout.
  it.live("grants background admission while a backup export stream is open", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const pull = yield* Stream.toPull(replica.exportBackup({ maxBytes: limits.maxBackupBytes }))
      yield* pull
      const background = yield* Effect.forkChild(Effect.scoped(scheduler.background), {
        startImmediately: true
      })
      const exit = yield* Fiber.await(background).pipe(Effect.timeoutOption("2 seconds"))
      assert.isTrue(
        Option.isSome(exit),
        "inbound sync admission stalled for the whole lifetime of an open backup export stream"
      )
    }).pipe(Effect.scoped, Effect.provide(Live), Effect.provide(NodeCrypto.layer)), 30_000)

  it.effect("ends an export cleanly when the admission queue fills between pulls", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const pull = yield* Stream.toPull(replica.exportBackup({ maxBytes: limits.maxBackupBytes }))
      yield* pull
      const holding = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* Effect.forkChild(
        Effect.scoped(Effect.gen(function*() {
          yield* scheduler.background
          yield* Deferred.succeed(holding, undefined)
          yield* Deferred.await(release)
        })),
        { startImmediately: true }
      )
      yield* Deferred.await(holding)
      const queued = yield* Effect.forkChild(Effect.scoped(scheduler.interactive), { startImmediately: true })
      yield* scheduler.reservationChanges.pipe(
        Stream.filter((counts) => counts.interactive >= 1),
        Stream.runHead
      )
      const terminal = yield* Effect.exit(pull)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(holder)
      yield* Fiber.interrupt(queued)
      // The archive was fully emitted by the first pull, so the closing pull has no work left to
      // admit. It must end the stream, not discard a complete backup because the queue was full.
      assert.isTrue(
        Exit.isFailure(terminal) && Pull.isDoneCause(terminal.cause),
        "a complete export was thrown away by the terminal pull's admission refusal"
      )
    }).pipe(Effect.scoped, Effect.provide(PriorityLive), Effect.provide(NodeCrypto.layer), TestClock.withLive))

  it("rejects duplicate bindings for one projection", () => {
    assert.throws(
      () => SqlReplica.layerWithBindings(projectedDefinition, { projections: [TaskTitleSql, TaskTitleSql] }),
      /exactly one SQL binding/
    )
  })

  it("rejects duplicate bindings for one projection through the relay constructor too", () => {
    assert.throws(
      () => SqlReplica.layerRelayWithBindings(projectedDefinition, { projections: [TaskTitleSql, TaskTitleSql] }),
      /exactly one SQL binding/
    )
  })

  it("validates command delivery publisher options through every public constructor", () => {
    const options = {
      projections: [],
      deliveryPublisher: {
        eventCapacity: 0,
        publishInterval: "1 second"
      }
    } satisfies SqlReplica.Options<[]>
    for (
      const build of [
        () => SqlReplica.layer(definition, options),
        () => SqlReplica.layerRelay(definition, options),
        () => SqlReplica.layerWithBindings(definition, options),
        () => SqlReplica.layerRelayWithBindings(definition, options)
      ]
    ) {
      assert.throws(build, /positive integer/)
    }
  })

  // The reason `layerRelayWithBindings` exists. A deployment with projections that reaches for the
  // only bindings constructor there was got a direct `PeerSync`, and found out at the point it
  // built a service it did not think it was choosing.
  for (
    const flavour of [
      {
        name: "direct",
        build: () => SqlReplica.layerWithBindings(projectedDefinition, { projections: [TaskTitleSql] }),
        relayRuntimeBuilds: false
      },
      {
        name: "relay",
        build: () => SqlReplica.layerRelayWithBindings(projectedDefinition, { projections: [TaskTitleSql] }),
        relayRuntimeBuilds: true
      }
    ]
  ) {
    it.effect(`${flavour.name} bindings constructor: relay client runtime builds = ${flavour.relayRuntimeBuilds}`, () =>
      Effect.gen(function*() {
        const exit = yield* Effect.exit(Effect.scoped(Layer.build(
          PeerRelayClientRuntime.layerSql.pipe(
            Layer.provide(flavour.build()),
            Layer.provide(Layer.mergeAll(
              Database,
              Handler,
              Limits,
              PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults),
              PeerRelayOutboxLimits.layer(PeerRelayOutboxLimits.defaults)
            ))
          )
        )))
        if (flavour.relayRuntimeBuilds) {
          assert.strictEqual(exit._tag, "Success")
          return
        }
        // Named rather than merely "it failed": the direct stack has to be rejected for being the
        // wrong `PeerSync`, not for some unrelated wiring fault that would mask a real regression.
        if (!Exit.isFailure(exit)) {
          assert.fail("the direct stack must not build a relay runtime")
          return
        }
        const failure = Cause.findErrorOption(exit.cause)
        if (Option.isNone(failure)) {
          assert.fail("expected a typed failure")
          return
        }
        const error = failure.value
        if (error._tag !== "ReplicaError") {
          assert.fail(`expected a ReplicaError, got ${error._tag}`)
          return
        }
        const reason = error.reason
        if (reason._tag !== "ProtocolMismatch") {
          assert.fail(`expected a ProtocolMismatch, got ${reason._tag}`)
          return
        }
        assert.strictEqual(reason.expected, "relay enabled PeerSync")
      }))
  }

  it.effect("forwards the configured health sampling interval", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      yield* replica.status.pipe(
        Stream.runForEach((status) => Queue.offer(seen, status)),
        Effect.forkChild
      )
      assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Queue.poll(seen), Option.none())
      yield* TestClock.adjust("4 seconds")
      assert.deepStrictEqual(yield* Queue.take(seen), {
        _tag: "ReadOnly",
        reason: "Another writer generation owns this replica"
      })
    }).pipe(Effect.provide(SlowHealthLive), Effect.provide(Database)))

  it.effect("creates, reads, mutates, tombstones, and resolves receipts", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      assert.ok(yield* ReplicaWorkflow.CompactionWorkflow)
      const unknownCommandId = yield* Identity.makeCommandId
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(unknownCommandId))._tag,
        "UnknownCommand"
      )
      const createCommandId = yield* Identity.makeCommandId
      const created = yield* replica.create(Task, { commandId: createCommandId, value: { title: "one" } })
      const documentId = created
      const createdDelivery = yield* replica.lookupCommandDelivery(createCommandId)
      assert.strictEqual(createdDelivery._tag, "TrackedCommand")
      if (createdDelivery._tag === "TrackedCommand") {
        assert.isAbove(createdDelivery.localChangeCount, 0)
        assert.deepStrictEqual(createdDelivery.destinations, [])
      }
      assert.deepStrictEqual(
        yield* replica.create(Task, { commandId: createCommandId, value: { title: "one" } }),
        created
      )
      assert.strictEqual(
        (yield* Effect.flip(replica.create(Task, { commandId: createCommandId, value: { title: "different" } })))
          .reason._tag,
        "CommandIdConflict"
      )
      const concurrentCommandId = yield* Identity.makeCommandId
      const concurrent = yield* Effect.all([
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } }),
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } })
      ], { concurrency: "unbounded" })
      assert.deepStrictEqual(concurrent[0], concurrent[1])
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "one" })
      const mutationCommandId = yield* Identity.makeCommandId
      assert.deepStrictEqual(
        yield* replica.mutate(Rename, { commandId: mutationCommandId, documentId, payload: "two" }),
        undefined
      )
      const mutationDelivery = yield* replica.lookupCommandDelivery(mutationCommandId)
      assert.strictEqual(mutationDelivery._tag, "TrackedCommand")
      if (mutationDelivery._tag === "TrackedCommand") {
        assert.isAbove(mutationDelivery.localChangeCount, 0)
      }
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      const noopCommandId = yield* Identity.makeCommandId
      assert.deepStrictEqual(
        yield* replica.mutate(Noop, { commandId: noopCommandId, documentId }),
        undefined
      )
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(noopCommandId))._tag,
        "NoChangesToDeliver"
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      assert.deepStrictEqual(
        yield* replica.lookupMutation(Rename, mutationCommandId),
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      const deleteCommandId = yield* Identity.makeCommandId
      yield* replica.delete(Task, { commandId: deleteCommandId, documentId })
      const deleteDelivery = yield* replica.lookupCommandDelivery(deleteCommandId)
      assert.strictEqual(deleteDelivery._tag, "TrackedCommand")
      if (deleteDelivery._tag === "TrackedCommand") {
        assert.isAbove(deleteDelivery.localChangeCount, 0)
      }
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      assert.deepStrictEqual(
        yield* replica.lookupDelete(Task, deleteCommandId),
        CommandOutcome.durablyCommitted(deleteCommandId, undefined)
      )
      const portableCreated = yield* replica.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "portable" }
      })
      const exported = yield* replica.exportDocument(Task, portableCreated)
      const importCommandId = yield* Identity.makeCommandId
      const imported = yield* replica.importDocument(Task, {
        commandId: importCommandId,
        value: exported
      })
      const importedSnapshot = yield* replica.get(Task, imported)
      const sourceSnapshot = yield* replica.get(Task, portableCreated)
      assert.notStrictEqual(imported, portableCreated)
      assert.deepStrictEqual(importedSnapshot.value, { title: "portable" })
      assert.notStrictEqual(importedSnapshot.heads[0], sourceSnapshot.heads[0])
      assert.deepStrictEqual(
        yield* replica.importDocument(Task, { commandId: importCommandId, value: exported }),
        imported
      )
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{
        readonly changes: number
        readonly clusterMessages: number
        readonly documents: number
        readonly receipts: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}) AS clusterMessages,
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts`
      assert.deepStrictEqual(rows[0], { changes: 6, clusterMessages: 8, documents: 4, receipts: 7 })
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("does not publish after a durable stale resolution rejection", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "one" }
      })
      const inspected = yield* replica.inspectConflicts(Task, documentId)
      assert.deepStrictEqual(inspected.conflicts, [])
      const commandId = yield* Identity.makeCommandId
      const resolution: Conflict.Resolution = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "DeleteValue" }
      }
      yield* sql`UPDATE effect_local_commit_outbox
        SET published = 0, invalidation_keys = 'invalid-json'
        WHERE commit_sequence = (
          SELECT MAX(commit_sequence) FROM effect_local_commit_outbox
        )`

      const stale = yield* Effect.flip(
        replica.resolveConflict(Task, { commandId, documentId, resolution })
      )
      if (stale._tag !== "StaleConflictResolution") {
        assert.fail(`Expected StaleConflictResolution, got ${stale._tag}`)
      }
      assert.deepStrictEqual(
        yield* replica.lookupConflictResolution(Task, { commandId, documentId, resolution }),
        CommandOutcome.rejected(commandId, stale)
      )
      const pending = yield* sql<{ readonly published: number }>`
        SELECT published FROM effect_local_commit_outbox ORDER BY commit_sequence DESC LIMIT 1
      `
      assert.deepStrictEqual(pending[0], { published: 0 })
    }).pipe(Effect.provide(DirectLive), Effect.provide(NodeCrypto.layer), TestClock.withLive))

  it.effect("exposes a durable outcome after commit acknowledgement is lost", () =>
    Effect.gen(function*() {
      let loseCommitAcknowledgement = false
      let reservedConnections = 0
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const instrumentedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return makeSqlClient(sql, {
            reserve: sql.reserve.pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  reservedConnections++
                })
              ),
              Effect.map((connection) =>
                Object.assign({}, connection, {
                  executeUnprepared: (
                    statement: string,
                    params: ReadonlyArray<unknown>,
                    transformRows: Parameters<typeof connection.executeUnprepared>[2]
                  ) => {
                    const executed = connection.executeUnprepared(statement, params, transformRows)
                    if (statement !== "COMMIT" || !loseCommitAcknowledgement) return executed
                    return executed.pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          loseCommitAcknowledgement = false
                        })
                      ),
                      Effect.andThen(Effect.fail(
                        new SqlError.SqlError({
                          reason: new SqlError.ConnectionError({
                            cause: nativeError("commit acknowledgement lost"),
                            message: "commit acknowledgement lost",
                            operation: "commit"
                          })
                        })
                      ))
                    )
                  }
                })
              )
            )
          })
        })
      ).pipe(Layer.provide(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const services = directServices(database)

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "one" }
        })
        const commandId = yield* Identity.makeCommandId
        assert.isAbove(reservedConnections, 0)
        loseCommitAcknowledgement = true

        const exit = yield* Effect.exit(replica.mutate(Rename, {
          commandId,
          documentId,
          payload: "two"
        }))

        if (!Exit.isFailure(exit)) {
          assert.fail("expected the commit acknowledgement to be lost")
          return
        }
        assert.isFalse(Cause.hasDies(exit.cause))
        assert.strictEqual(exit.cause.reasons.length, 1)
        const failure = Cause.findErrorOption(exit.cause)
        if (Option.isNone(failure)) {
          assert.fail("expected a typed failure")
          return
        }
        const unknown = failure.value
        assert.strictEqual(unknown.reason._tag, "CommandOutcomeUnknown")
        if (unknown.reason._tag === "CommandOutcomeUnknown") {
          assert.strictEqual(unknown.reason.commandId, commandId)
        }
        assert.deepStrictEqual(
          yield* replica.lookupMutation(Rename, commandId),
          CommandOutcome.durablyCommitted(commandId, undefined)
        )
        assert.strictEqual((yield* replica.get(Task, documentId)).value.title, "two")
      }).pipe(Effect.scoped, Effect.provide(services), Effect.provide(NodeCrypto.layer))
    }))
  it.effect("rolls a command back when delivery source persistence fails", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const commandId = yield* Identity.makeCommandId
      yield* sql`CREATE TRIGGER fail_command_delivery_source
        BEFORE INSERT ON effect_local_command_delivery_sources
        BEGIN
          SELECT RAISE(ABORT, 'forced command delivery source failure');
        END`

      const result = yield* Effect.exit(
        replica.create(Task, { commandId, value: { title: "must roll back" } })
      )
      assert.strictEqual(result._tag, "Failure")
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(commandId))._tag,
        "UnknownCommand"
      )
      const rows = yield* sql`SELECT
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_command_delivery_sources) AS sources,
        (SELECT COUNT(*) FROM effect_local_command_delivery_changes) AS delivery_changes,
        (SELECT COUNT(*) FROM effect_local_command_delivery_events) AS delivery_events`
      assert.deepStrictEqual(rows, [{
        documents: 0,
        changes: 0,
        receipts: 0,
        sources: 0,
        delivery_changes: 0,
        delivery_events: 0
      }])
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("refreshes command delivery subscribers after restore clears custody evidence", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const commandId = yield* Identity.makeCommandId
        yield* replica.create(Task, { commandId, value: { title: "backed up" } })
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        const pull = yield* replica.commandDeliveryChanges(commandId).pipe(Stream.toPull)

        const before = yield* pull
        assert.strictEqual(before[0]?._tag, "TrackedCommand")

        yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup)
        })

        const after = yield* pull
        assert.strictEqual(after[0]?._tag, "UnknownCommand")
      }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive)
    ))

  it.effect("provides nonempty projection bindings", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{ readonly sourceDocumentId: string; readonly title: string }>`SELECT
        source_document_id AS sourceDocumentId,
        title
      FROM task_title_v1`
      assert.deepStrictEqual(rows, [{ sourceDocumentId: created, title: "projected" }])
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("installs and publishes one missing archive document without changing existing replica state", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const archive = yield* Effect.gen(function*() {
          const replica = yield* Replica.Replica
          const selectedId = yield* replica.create(Task, {
            commandId: yield* Identity.makeCommandId,
            value: { title: "selected" }
          })
          const omittedId = yield* replica.create(Task, {
            commandId: yield* Identity.makeCommandId,
            value: { title: "omitted" }
          })
          const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
          return { chunks, omittedId, selectedId }
        }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))

        const replica = yield* Replica.Replica
        const gate = yield* ReplicaGate.ReplicaGate
        const existingId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "existing" }
        })
        yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId: existingId,
          payload: "local write"
        })
        const identityBefore = yield* gate.current
        const publisher = yield* CommitPublisher.CommitPublisher
        const subscription = yield* publisher.subscribe
        const commitFiber = yield* Stream.runHead(subscription.events).pipe(Effect.forkChild)

        yield* replica.installBackupDocument(Task, {
          documentId: archive.selectedId,
          expectedDefinitionHash: projectedDefinition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          source: Stream.fromIterable(archive.chunks)
        })

        assert.deepStrictEqual(Option.getOrThrow(yield* Fiber.join(commitFiber)), {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(subscription.watermark + 1),
          documentId: archive.selectedId,
          keys: [
            Task.name,
            ReplicaDefinition.documentInstanceKey(Task.name, archive.selectedId),
            TaskTitle.name
          ],
          refreshGeneration: subscription.refreshGeneration
        })

        assert.deepStrictEqual(yield* gate.current, identityBefore)
        assert.strictEqual((yield* replica.get(Task, existingId)).value.title, "local write")
        assert.strictEqual((yield* replica.get(Task, archive.selectedId)).value.title, "selected")
        const omitted = yield* Effect.flip(replica.get(Task, archive.omittedId))
        assert.strictEqual(omitted.reason._tag, "DocumentNotFound")
        const sql = yield* SqlClient.SqlClient
        assert.deepStrictEqual(
          yield* sql`SELECT source_document_id, title FROM task_title_v1 ORDER BY source_document_id`,
          [
            { source_document_id: existingId, title: "local write" },
            { source_document_id: archive.selectedId, title: "selected" }
          ].toSorted((left, right) => String(left.source_document_id).localeCompare(String(right.source_document_id)))
        )
      }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive)
    ))

  it.effect("makes an exact selective installation retry a no-op and rejects document collisions", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "archived" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))

      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const installationId = yield* Identity.makeBackupInstallationId
      const install = (nextInstallationId: Identity.BackupInstallationId) =>
        replica.installBackupDocument(Task, {
          documentId: archive.documentId,
          expectedDefinitionHash: projectedDefinition.hash,
          installationId: nextInstallationId,
          maxBytes: limits.maxBackupBytes,
          source: Stream.fromIterable(archive.chunks)
        })

      yield* install(installationId)
      const afterInstall = yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`
      yield* install(installationId)
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        afterInstall
      )
      assert.deepStrictEqual(
        yield* sql`SELECT mode, document_id FROM effect_local_backup_installations`,
        [{ mode: "document", document_id: archive.documentId }]
      )

      const collision = yield* Effect.flip(install(yield* Identity.makeBackupInstallationId))
      assert.strictEqual(collision.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        afterInstall
      )
      assert.strictEqual((yield* replica.get(Task, archive.documentId)).value.title, "archived")
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("rejects a selective archive with duplicate records outside the selected document", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const selectedId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "selected" }
        })
        const omittedId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "omitted" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, omittedId, selectedId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))
      const lines = archiveLines(archive.chunks)
      const source = lines.find((line) => line.kind === "Document" && line.value.document_id === archive.omittedId)
      if (source === undefined) return yield* Effect.die("Missing omitted document archive record")
      const duplicate = structuredClone(source)
      lines.splice(-1, 0, duplicate)
      const tampered = yield* encodeArchive(lines)
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const before = yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(replica.installBackupDocument(Task, {
        documentId: archive.selectedId,
        expectedDefinitionHash: projectedDefinition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(tampered)
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        before
      )
      return yield* Effect.void
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("reports malformed selective archive history as BackupInvalid without mutation", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "archived" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))
      const lines = archiveLines(archive.chunks)
      lines.find((line) => line.kind === "Document")!.value.accepted_heads = "not-json"
      const tampered = yield* encodeArchive(lines)
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const before = yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(replica.installBackupDocument(Task, {
        documentId: archive.documentId,
        expectedDefinitionHash: projectedDefinition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(tampered)
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        before
      )
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("validates canonical metadata for documents omitted from selective installation", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const selectedId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "selected" }
        })
        const omittedId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "omitted" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, omittedId, selectedId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))
      const lines = archiveLines(archive.chunks)
      const documentLine = lines.find(
        (line) => line.kind === "Document" && line.value.document_id === archive.omittedId
      )
      if (documentLine === undefined || documentLine.kind !== "Document") {
        return yield* Effect.die("Missing omitted document archive record")
      }
      documentLine.value.accepted_heads = "not-json"
      const tampered = yield* encodeArchive(lines)
      const replica = yield* Replica.Replica

      const error = yield* Effect.flip(replica.installBackupDocument(Task, {
        documentId: archive.selectedId,
        expectedDefinitionHash: projectedDefinition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(tampered)
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      return yield* Effect.void
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("reports an unsupported selective archive version as BackupInvalid", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "archived" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))
      const lines = archiveLines(archive.chunks)
      lines.find((line) => line.kind === "Document")!.value.schema_version = 999
      const tampered = yield* encodeArchive(lines)
      const replica = yield* Replica.Replica

      const error = yield* Effect.flip(replica.installBackupDocument(Task, {
        documentId: archive.documentId,
        expectedDefinitionHash: projectedDefinition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(tampered)
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("rejects installation id reuse for different archive contents", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "archived" }
        })
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))
      const lines = archiveLines(archive.chunks)
      const manifest = lines[0]
      if (!manifest || manifest.kind !== "Manifest") return yield* Effect.die("Malformed archive fixture")
      const manifestChecksum = manifest.checksum
      const change = lines.find((line) => line.kind === "Change")
      if (!change || change.kind !== "Change") return yield* Effect.die("Malformed archive fixture")
      change.value.accepted_at = "2026-01-01T00:00:00.000Z"
      const changed = yield* encodeArchive(lines)
      assert.strictEqual(manifest.checksum, manifestChecksum)
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const installationId = yield* Identity.makeBackupInstallationId
      const install = (source: Stream.Stream<Uint8Array>) =>
        replica.installBackupDocument(Task, {
          documentId: archive.documentId,
          expectedDefinitionHash: projectedDefinition.hash,
          installationId,
          maxBytes: limits.maxBackupBytes,
          source
        })
      yield* install(Stream.fromIterable(archive.chunks))
      const before = yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(install(Stream.make(changed)))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        before
      )
      return yield* Effect.void
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("keeps a selectively installed pruned checkpoint document writable", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const compaction = yield* Compaction.Compaction
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "created" }
        })
        yield* compaction.compact(Task, documentId)
        yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId,
          payload: "after checkpoint"
        })
        yield* compaction.compact(Task, documentId)
        assert.isAbove(yield* compaction.prune(documentId), 0)
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(DirectLive)))

      const replica = yield* Replica.Replica
      yield* replica.installBackupDocument(Task, {
        documentId: archive.documentId,
        expectedDefinitionHash: definition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.fromIterable(archive.chunks)
      })
      yield* replica.mutate(Rename, {
        commandId: yield* Identity.makeCommandId,
        documentId: archive.documentId,
        payload: "still writable"
      })
      assert.strictEqual((yield* replica.get(Task, archive.documentId)).value.title, "still writable")
    }).pipe(Effect.provide(DirectLive), Effect.provide(Database), TestClock.withLive))

  it.effect("rejects pending rows already materialized by the selected checkpoint", () =>
    Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const compaction = yield* Compaction.Compaction
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "checkpointed" }
        })
        yield* compaction.compact(Task, documentId)
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(DirectLive)))
      const lines = archiveLines(archive.chunks)
      const checkpoint = lines.find((line) => line.kind === "Checkpoint")
      if (
        !checkpoint ||
        checkpoint.kind !== "Checkpoint" ||
        !Array.isArray(checkpoint.value.writer_provenance)
      ) return yield* Effect.die("Malformed checkpoint archive fixture")
      const covered = new Set(checkpoint.value.writer_provenance.map((entry) => entry.changeHash))
      const changeLine = lines.find(
        (line) => line.kind === "Change" && covered.has(line.value.change_hash)
      )
      if (changeLine === undefined || changeLine.kind !== "Change") {
        return yield* Effect.die("Missing covered change archive record")
      }
      changeLine.value.applied = 0
      const tampered = yield* encodeArchive(lines)
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const before = yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(replica.installBackupDocument(Task, {
        documentId: archive.documentId,
        expectedDefinitionHash: definition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(tampered)
      }))

      assert.strictEqual(error.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(
        yield* sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        before
      )
      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_local_changes`, [])
      return yield* Effect.void
    }).pipe(Effect.provide(DirectLive), Effect.provide(Database), TestClock.withLive))

  it.effect("batches selective change insertion", () => {
    let changeInsertStatements = 0
    const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
    const countingDatabase = Layer.effect(
      SqlClient.SqlClient,
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        return makeSqlClient(sql, {}, (strings) => {
          if (strings[0].includes("INSERT INTO effect_local_changes")) {
            changeInsertStatements++
          }
        })
      })
    ).pipe(Layer.provide(baseDatabase))
    const CountingDatabase = Layer.merge(countingDatabase, NodeCrypto.layer)
    const CountingLive = SqlReplica.layerWithBindings(projectedDefinition, { projections: [TaskTitleSql] }).pipe(
      Layer.provide(Layer.mergeAll(CountingDatabase, Handler, Limits))
    )
    return Effect.gen(function*() {
      const archive = yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "change 0" }
        })
        for (let index = 1; index < 51; index++) {
          yield* replica.mutate(Rename, {
            commandId: yield* Identity.makeCommandId,
            documentId,
            payload: `change ${index}`
          })
        }
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        return { chunks, documentId }
      }).pipe(Effect.provide(Layer.fresh(ProjectedLive)))

      const replica = yield* Replica.Replica
      yield* replica.installBackupDocument(Task, {
        documentId: archive.documentId,
        expectedDefinitionHash: projectedDefinition.hash,
        installationId: yield* Identity.makeBackupInstallationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.fromIterable(archive.chunks)
      })

      assert.isBelow(changeInsertStatements, 51)
    }).pipe(Effect.provide(CountingLive), Effect.provide(Database), TestClock.withLive)
  })

  // Deleting a document replaces its projection rows with none, driven by the
  // tombstone the commit reports. Nothing else in the suite exercises a delete
  // against a replica that actually has projection bindings.
  it.effect("clears projection rows when a projected document is deleted", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
      const documentId = created
      const projected = () =>
        sql<{ readonly sourceDocumentId: string; readonly title: string }>`SELECT
          source_document_id AS sourceDocumentId, title FROM task_title_v1`
      assert.deepStrictEqual(yield* projected(), [{ sourceDocumentId: documentId, title: "projected" }])

      yield* replica.delete(Task, { commandId: yield* Identity.makeCommandId, documentId })

      assert.deepStrictEqual(yield* projected(), [])
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      const keys = yield* sql<{ readonly invalidation_keys: string }>`
        SELECT invalidation_keys FROM effect_local_commit_outbox ORDER BY commit_sequence DESC LIMIT 1
      `
      assert.deepStrictEqual(Schema.decodeSync(JsonString)(keys[0].invalidation_keys), [
        Task.name,
        ReplicaDefinition.documentInstanceKey(Task.name, documentId),
        TaskTitle.name
      ])
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("rejects importing a document whose portable definition does not match", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "source" }
      })
      const exported = yield* replica.exportDocument(Task, created)
      const wrongName = yield* Effect.flip(replica.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { ...exported, documentName: "Other" }
      }))
      assert.strictEqual(wrongName.reason._tag, "BackupInvalid")
      const wrongVersion = yield* Effect.flip(replica.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { ...exported, schemaVersion: 999 }
      }))
      assert.strictEqual(wrongVersion.reason._tag, "BackupInvalid")
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("sheds replica operations while a restore holds the writer", () =>
    Effect.gen(function*() {
      const committed = yield* Deferred.make<void>()
      const ingesting = yield* Deferred.make<void>()
      const continueIngest = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const instrumentedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return makeSqlClient(sql, {
            withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
              Effect.serviceOption(sql.transactionService).pipe(
                Effect.flatMap((transaction) =>
                  sql.withTransaction(effect).pipe(
                    Effect.tap(() =>
                      Match.value(armed && Option.isNone(transaction)).pipe(
                        Match.when(true, () =>
                          Deferred.succeed(committed, undefined).pipe(Effect.andThen(release.await))),
                        Match.orElse(() =>
                          Effect.void
                        )
                      )
                    )
                  )
                )
              )
          })
        })
      ).pipe(Layer.provideMerge(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const services = directServices(database, ReplicaLimits.layer({ ...limits, maxQueuedPermits: 1 }))

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        const backup = Array.from(
          yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        )
        assert.isAtLeast(backup.length, 2)
        armed = true
        const restore = yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup).pipe(
            Stream.mapEffect((chunk, index) =>
              Match.value(index).pipe(
                Match.when(1, () =>
                  Deferred.succeed(ingesting, undefined).pipe(
                    Effect.andThen(Deferred.await(continueIngest)),
                    Effect.as(chunk)
                  )),
                Match.orElse(() => Effect.succeed(chunk))
              )
            )
          )
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(ingesting)
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({
            _tag: "Restoring",
            processedBytes: backup[0].byteLength
          })
        )
        yield* Deferred.succeed(continueIngest, undefined)
        yield* Deferred.await(committed)
        armed = false
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({
            _tag: "ReadOnly",
            reason: "A backup restore is installing"
          })
        )

        const queued = yield* Effect.forkChild(replica.get(Task, created))
        yield* Effect.yieldNow

        const shed = yield* Effect.flip(replica.get(Task, created))
        assert.strictEqual(shed.reason._tag, "QuotaExceeded")
        if (shed.reason._tag === "QuotaExceeded") {
          assert.strictEqual(shed.reason.resource, "queued permits")
          assert.strictEqual(shed.reason.limit, 1)
        }

        yield* release.open
        yield* Fiber.join(restore)
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
        )
        assert.strictEqual((yield* Fiber.join(queued)).value.title, "before")
      }).pipe(Effect.scoped, Effect.provide(services))
    }).pipe(TestClock.withLive))

  it.effect("invalidates reactive consumers when interruption arrives after a restore commits", () =>
    Effect.gen(function*() {
      const committed = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const instrumentedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          const instrumented = makeSqlClient(sql, {
            withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
              Effect.serviceOption(sql.transactionService).pipe(
                Effect.flatMap((transaction) =>
                  sql.withTransaction(effect).pipe(
                    Effect.tap(() =>
                      Match.value(armed && Option.isNone(transaction)).pipe(
                        Match.when(true, () =>
                          Deferred.succeed(committed, undefined).pipe(Effect.andThen(release.await))),
                        Match.orElse(() =>
                          Effect.void
                        )
                      )
                    )
                  )
                )
              )
          })
          return instrumented
        })
      ).pipe(Layer.provideMerge(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const services = directServices(database)

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const reactivity = yield* Reactivity.Reactivity
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId: created,
          payload: "after"
        })
        let invalidated = false
        const cancel = reactivity.registerUnsafe([Task.name], () => {
          invalidated = true
        })
        yield* Effect.addFinalizer(() => Effect.sync(cancel))
        armed = true
        const restore = yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup)
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(committed)
        const interrupt = yield* Fiber.interrupt(restore).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Effect.yieldNow
        yield* release.open
        yield* Fiber.join(interrupt)

        assert.strictEqual((yield* replica.get(Task, created)).value.title, "before")
        assert.isTrue(invalidated)
      }).pipe(Effect.scoped, Effect.provide(services))
    }))

  it.effect("publishes a commit whose mutate fiber was interrupted right after the transaction committed", () =>
    Effect.gen(function*() {
      const publishing = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const probedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return new Proxy(sql, {
            apply(target, thisArg, args) {
              const statement = Reflect.apply(target, thisArg, args)
              const strings = args[0]
              if (!Array.isArray(strings) || !armed) return statement
              const query = strings.join("?").replace(/\s+/g, " ").trim()
              if (!query.includes("FROM effect_local_commit_outbox WHERE published = 0")) return statement
              armed = false
              return Deferred.succeed(publishing, undefined).pipe(
                Effect.andThen(release.await),
                Effect.andThen(statement)
              )
            }
          })
        })
      ).pipe(Layer.provide(baseDatabase))
      const database = Layer.merge(probedDatabase, NodeCrypto.layer)
      const services = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
        Layer.provideMerge(Layer.mergeAll(database, Handler, Limits))
      )

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        const publisher = yield* CommitPublisher.CommitPublisher
        yield* Effect.addFinalizer(() => release.open)
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        const subscription = yield* publisher.subscribe
        armed = true
        const mutate = yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId,
          payload: "after"
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(publishing)
        yield* Fiber.interrupt(mutate)
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(mutate)))
        assert.strictEqual((yield* replica.get(Task, documentId)).value.title, "after")
        const pending = yield* sql`SELECT commit_sequence FROM effect_local_commit_outbox
          WHERE document_id = ${documentId} AND published = 0`
        assert.strictEqual(pending.length, 1)

        yield* TestClock.adjust("1 second")
        const event = yield* subscription.events.pipe(
          Stream.filter((candidate) => candidate._tag === "Commit" && candidate.documentId === documentId),
          Stream.runHead
        )
        assert.isTrue(Option.isSome(event))
        const remaining = yield* sql`SELECT commit_sequence FROM effect_local_commit_outbox
          WHERE document_id = ${documentId} AND published = 0`
        assert.strictEqual(remaining.length, 0)
      }).pipe(Effect.scoped, Effect.provide(services))
    }))
  // Called per test: `Effect.provide` shares a built layer between calls, and each status test needs
  // its own database.
  const statusServices = () =>
    directServices(Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    ))

  it.effect("reports unpublished commits as pending commands", () =>
    Effect.gen(function*() {
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_commit_outbox
          (commit_sequence, document_id, invalidation_keys, published)
          VALUES (9001, 'doc_00000000-0000-4000-8000-000000000001', '["Task"]', 0)`
        yield* sql`INSERT INTO effect_local_commit_outbox
          (commit_sequence, document_id, invalidation_keys, published)
          VALUES (9002, 'doc_00000000-0000-4000-8000-000000000002', '["Task"]', 0)`
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 2 })
        )
        yield* replica.flush
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
        )
      }).pipe(Effect.scoped, Effect.provide(statusServices()))
    }))

  it.effect("keeps the status stream open", () =>
    Effect.gen(function*() {
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const ended = yield* Deferred.make<void>()
        const consumer = yield* replica.status.pipe(
          Stream.runDrain,
          Effect.andThen(Deferred.succeed(ended, undefined)),
          Effect.forkChild
        )
        yield* Effect.yieldNow
        assert.isTrue(Option.isNone(yield* Deferred.poll(ended)))
        yield* Fiber.interrupt(consumer)
      }).pipe(Effect.scoped, Effect.provide(statusServices()))
    }))
  it.effect("reports pending commands through the sharded replica layer", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_commit_outbox
        (commit_sequence, document_id, invalidation_keys, published)
        VALUES (9101, 'doc_00000000-0000-4000-8000-000000000011', '["Task"]', 0)`
      yield* sql`INSERT INTO effect_local_commit_outbox
        (commit_sequence, document_id, invalidation_keys, published)
        VALUES (9102, 'doc_00000000-0000-4000-8000-000000000012', '["Task"]', 0)`
      assert.deepStrictEqual(
        yield* Stream.runHead(replica.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 2 })
      )
      yield* replica.flush
      assert.deepStrictEqual(
        yield* Stream.runHead(replica.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
      )
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))
})
