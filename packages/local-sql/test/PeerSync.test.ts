import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import type * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as CheckpointAuthority from "../src/CheckpointAuthority.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as SyncChunks from "../src/internal/syncChunks.js"
import * as WriterProvenance from "../src/internal/writerProvenance.js"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"
import * as PeerSync from "../src/PeerSync.js"
import * as PeerSyncEnvelope from "../src/PeerSyncEnvelope.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("PeerSync", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 64,
    maxConflictDepth: 64,
    maxConflictNodes: 100_000,
    maxConflictAlternatives: 10_000,
    maxConflictPathSegments: 128,
    maxConflictValueBytes: 16 * 1024 * 1024,
    maxConflictSourceChanges: 100_000,
    maxConflictSourceOperations: 100_000,
    maxConflictSourceBytes: 64 * 1024 * 1024,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 1_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 1_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 10,
    maxStreamsPerSession: 10,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 100,
    maxQueuedPermits: 100,
    maxActiveRestores: 100,
    maxRestoresPerSession: 1,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.mergeAll(Database, Bootstrap, CheckpointAuthority.layerRejectAll)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)), Layer.provide(Base))
  const Limits = ReplicaLimits.layer(limits)
  const Infrastructure = Layer.mergeAll(Base, Gate, Limits)
  const StoreService = DocumentStore.layer.pipe(Layer.provide(Infrastructure))
  const Projections = ProjectionStore.layer([]).pipe(Layer.provide(Base))
  const Services = Layer.mergeAll(Infrastructure, StoreService, Projections)
  const SyncService = PeerSync.layer.pipe(Layer.provide(Services))
  const TestLayer = Layer.merge(Services, SyncService)
  const RelayServices = Layer.merge(
    Services,
    PeerRelayReceiptLimits.layer({
      ...PeerRelayReceiptLimits.defaults,
      pruneBatchSize: 1
    })
  )
  const RelaySyncService = PeerSync.layerRelay.pipe(Layer.provide(RelayServices))
  const RelayTestLayer = Layer.merge(RelayServices, RelaySyncService)
  const TightRelayServices = Layer.merge(
    Services,
    PeerRelayReceiptLimits.layer({
      ...PeerRelayReceiptLimits.defaults,
      maxEncodedBytesPerRemote: 256,
      maxEncodedBytesPerReplica: 256
    })
  )
  const TightRelaySyncService = PeerSync.layerRelay.pipe(Layer.provide(TightRelayServices))
  const TightRelayTestLayer = Layer.merge(TightRelayServices, TightRelaySyncService)
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Services))
  const CompactionService = Compaction.layer.pipe(
    Layer.provide(Layer.merge(Services, RecoveryService))
  )
  const CompactionLayer = Layer.mergeAll(TestLayer, RecoveryService, CompactionService)
  const StrictLimits = ReplicaLimits.layer({ ...limits, maxSyncOperationsPerMessage: 1 })
  const StrictInfrastructure = Layer.mergeAll(Base, Gate, StrictLimits)
  const StrictStoreService = DocumentStore.layer.pipe(Layer.provide(StrictInfrastructure))
  const StrictServices = Layer.mergeAll(StrictInfrastructure, StrictStoreService, Projections)
  const StrictSyncService = PeerSync.layer.pipe(Layer.provide(StrictServices))
  const StrictLayer = Layer.merge(StrictServices, StrictSyncService)
  const sourceLayer = (
    sourceLimits: ReplicaLimits.Values,
    filename = ":memory:"
  ) =>
    SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
      Layer.provideMerge([
        SqliteClient.layer({ filename, disableWAL: true }),
        NodeCrypto.layer,
        ReplicaLimits.layer(sourceLimits)
      ])
    )
  const EdgeLimits = ReplicaLimits.layer({
    ...limits,
    maxPendingDependencyEdgesPerDocument: 100,
    maxPendingDependencyEdgesPerPeer: 100,
    maxPendingDependencyEdgesPerReplica: 100,
    maxSyncDependencyEdgesPerMessage: 100,
    maxSyncOperationsPerMessage: 10_000
  })
  const EdgeInfrastructure = Layer.mergeAll(Base, Gate, EdgeLimits)
  const EdgeStoreService = DocumentStore.layer.pipe(Layer.provide(EdgeInfrastructure))
  const EdgeServices = Layer.mergeAll(EdgeInfrastructure, EdgeStoreService, Projections)
  const EdgeSyncService = PeerSync.layer.pipe(Layer.provide(EdgeServices))
  const EdgeLayer = Layer.merge(EdgeServices, EdgeSyncService)
  const ReceiptLimits = ReplicaLimits.layer({ ...limits, maxPendingChangesPerPeer: 2 })
  const ReceiptInfrastructure = Layer.mergeAll(Base, Gate, ReceiptLimits)
  const ReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(ReceiptInfrastructure))
  const ReceiptServices = Layer.mergeAll(ReceiptInfrastructure, ReceiptStoreService, Projections)
  const ReceiptSyncService = PeerSync.layer.pipe(Layer.provide(ReceiptServices))
  const ReceiptLayer = Layer.merge(ReceiptServices, ReceiptSyncService)
  const DocumentReceiptLimits = ReplicaLimits.layer({ ...limits, maxPendingChangesPerDocument: 2 })
  const DocumentReceiptInfrastructure = Layer.mergeAll(Base, Gate, DocumentReceiptLimits)
  const DocumentReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(DocumentReceiptInfrastructure))
  const DocumentReceiptServices = Layer.mergeAll(
    DocumentReceiptInfrastructure,
    DocumentReceiptStoreService,
    Projections
  )
  const DocumentReceiptSyncService = PeerSync.layer.pipe(Layer.provide(DocumentReceiptServices))
  const DocumentReceiptLayer = Layer.merge(DocumentReceiptServices, DocumentReceiptSyncService)
  const ReplicaReceiptLimits = ReplicaLimits.layer({ ...limits, maxPendingChangesPerReplica: 2 })
  const ReplicaReceiptInfrastructure = Layer.mergeAll(Base, Gate, ReplicaReceiptLimits)
  const ReplicaReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(ReplicaReceiptInfrastructure))
  const ReplicaReceiptServices = Layer.mergeAll(ReplicaReceiptInfrastructure, ReplicaReceiptStoreService, Projections)
  const ReplicaReceiptSyncService = PeerSync.layer.pipe(Layer.provide(ReplicaReceiptServices))
  const ReplicaReceiptLayer = Layer.merge(ReplicaReceiptServices, ReplicaReceiptSyncService)
  const PendingReceiptLimits = ReplicaLimits.layer({ ...limits, maxPendingChangesPerPeer: 1 })
  const PendingReceiptInfrastructure = Layer.mergeAll(Base, Gate, PendingReceiptLimits)
  const PendingReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(PendingReceiptInfrastructure))
  const PendingReceiptServices = Layer.mergeAll(PendingReceiptInfrastructure, PendingReceiptStoreService, Projections)
  const PendingReceiptSyncService = PeerSync.layer.pipe(Layer.provide(PendingReceiptServices))
  const PendingReceiptLayer = Layer.merge(PendingReceiptServices, PendingReceiptSyncService)
  const CapacityReceiptLimits = ReplicaLimits.layer({
    ...limits,
    maxPendingChangesPerDocument: 1,
    maxPendingChangesPerPeer: 1,
    maxPendingChangesPerReplica: 1
  })
  const CapacityReceiptInfrastructure = Layer.mergeAll(Base, Gate, CapacityReceiptLimits)
  const CapacityReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(CapacityReceiptInfrastructure))
  const CapacityReceiptServices = Layer.mergeAll(
    CapacityReceiptInfrastructure,
    CapacityReceiptStoreService,
    Projections
  )
  const CapacityReceiptSyncService = PeerSync.layer.pipe(Layer.provide(CapacityReceiptServices))
  const CapacityReceiptLayer = Layer.merge(CapacityReceiptServices, CapacityReceiptSyncService)
  const provenanceFor = (
    message: Uint8Array,
    writerSchemaVersion = Task.version,
    writerDefinitionHash = definition.hash
  ) => {
    const hashes = new Set(
      SyncChunks.decodeSyncChanges(Automerge.decodeSyncMessage(message).changes)
        .map((change) => change.hash)
    )
    return [...hashes].toSorted().map((changeHash) => ({
      changeHash,
      writerSchemaVersion,
      writerDefinitionHash
    }))
  }

  const durableFootprint = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      return yield* sql<{
        readonly accepted_heads: string
        readonly applied_changes: number
        readonly checkpoint_hash: string | null
        readonly checkpoints: number
        readonly commit_outbox: number
        readonly commit_sequence: number
        readonly history_bytes: number | null
        readonly history_changes: number | null
        readonly history_operations: number | null
        readonly materialized_heads: string
        readonly peer_changes: number
        readonly peer_outbox: number
        readonly pending_changes: number
        readonly receipts: number
      }>`SELECT
        document.accepted_heads,
        document.checkpoint_hash,
        document.history_bytes,
        document.history_changes,
        document.history_operations,
        document.materialized_heads,
        metadata.commit_sequence,
        (SELECT COUNT(*) FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 1) AS applied_changes,
        (SELECT COUNT(*) FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0) AS pending_changes,
        (SELECT COUNT(*) FROM effect_local_changes
          WHERE document_id = ${documentId} AND peer_id IS NOT NULL) AS peer_changes,
        (SELECT COUNT(*) FROM effect_local_checkpoints
          WHERE document_id = ${documentId}) AS checkpoints,
        (SELECT COUNT(*) FROM effect_local_commit_outbox
          WHERE document_id = ${documentId}) AS commit_outbox,
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE document_id = ${documentId}) AS receipts,
        (SELECT COUNT(*) FROM effect_local_peer_outbox
          WHERE document_id = ${documentId}) AS peer_outbox
      FROM effect_local_documents AS document
      CROSS JOIN effect_local_metadata AS metadata
      WHERE document.document_id = ${documentId}`
    })

  type CounterState = "measured" | "unmeasured" | "mixed" | "mismatched"

  const seedSourceReceive = (filename: string, counterState: CounterState = "measured") =>
    Effect.scoped(
      Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "one", labels: [] }
        })
        const rows = yield* sql<{ readonly bytes: Uint8Array }>`SELECT bytes
          FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 1
          ORDER BY commit_sequence, actor, sequence`
        const empty = InternalAutomerge.empty<{ title: string; labels: Array<string> }>("1".repeat(32))
        let durable = InternalAutomerge.replay(empty, rows.map((row) => row.bytes))
        const durableHeads = InternalAutomerge.heads(durable)
        let remote = Automerge.change(
          Automerge.clone(durable, { actor: "2".repeat(32) }),
          (draft) => {
            draft.value.title = "remote"
          }
        )
        const remoteChanges = Automerge.getChangesSince(remote, [...durableHeads])
        assert.strictEqual(remoteChanges.length, 1)
        const decoded = Automerge.decodeChange(remoteChanges[0]!)
        let durableState = Automerge.initSyncState()
        let remoteState = Automerge.initSyncState()
        let targetInput:
          | {
            readonly message: Uint8Array
            readonly writerProvenance: ReturnType<typeof provenanceFor>
          }
          | undefined
        let quiesced = false
        for (let round = 0; round < 10; round++) {
          const [nextDurableState, durableMessage] = Automerge.generateSyncMessage(durable, durableState)
          const [nextRemoteState, remoteMessage] = Automerge.generateSyncMessage(remote, remoteState)
          durableState = nextDurableState
          remoteState = nextRemoteState
          if (durableMessage !== null) {
            ;[remote, remoteState] = Automerge.receiveSyncMessage(remote, remoteState, durableMessage)
          }
          if (remoteMessage !== null) {
            const writerProvenance = provenanceFor(remoteMessage, Task.version, definition.hash)
            if (writerProvenance.some((entry) => entry.changeHash === decoded.hash)) {
              targetInput = { message: remoteMessage, writerProvenance }
            }
            ;[durable, durableState] = Automerge.receiveSyncMessage(durable, durableState, remoteMessage)
          }
          if (durableMessage === null && remoteMessage === null) {
            quiesced = true
            break
          }
        }
        assert.isTrue(quiesced)
        assert.isDefined(targetInput)
        const counters = yield* sql<{
          readonly history_bytes: number
          readonly history_changes: number
          readonly history_operations: number
        }>`SELECT history_bytes, history_changes, history_operations
          FROM effect_local_documents WHERE document_id = ${documentId}`
        const current = counters[0]!
        const boundary = {
          bytes: current.history_bytes + remoteChanges[0]!.byteLength,
          changes: current.history_changes + 1,
          operations: current.history_operations + decoded.ops.length
        }
        if (counterState === "unmeasured") {
          yield* sql`UPDATE effect_local_documents SET
            history_bytes = NULL,
            history_changes = NULL,
            history_operations = NULL
            WHERE document_id = ${documentId}`
        } else if (counterState === "mixed") {
          yield* sql`UPDATE effect_local_documents SET history_operations = NULL
            WHERE document_id = ${documentId}`
        } else if (counterState === "mismatched") {
          yield* sql`UPDATE effect_local_documents SET
            history_operations = history_operations + 1
            WHERE document_id = ${documentId}`
        }
        InternalAutomerge.free(remote)
        InternalAutomerge.free(durable)
        return {
          boundary,
          documentId,
          input: {
            lineage: Identity.genesisLineage,
            message: targetInput!.message,
            writerProvenance: targetInput!.writerProvenance
          }
        }
      }).pipe(Effect.provide(sourceLayer(limits, filename)))
    )

  it.effect("does not issue a stale session during an exclusive claim", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sync = yield* PeerSync.PeerSync
      const claimAcquired = yield* Deferred.make<void>()
      const releaseClaim = yield* Deferred.make<void>()
      const claim = yield* gate.claim(() =>
        Deferred.succeed(claimAcquired, undefined).pipe(Effect.andThen(Deferred.await(releaseClaim)))
      ).pipe(Effect.forkChild)

      yield* Deferred.await(claimAcquired)
      const opening = yield* sync.open(yield* Identity.makePeerId).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseClaim, undefined)
      yield* Fiber.join(claim)

      const session = yield* Fiber.join(opening)
      assert.strictEqual(session.replicaIncarnation, (yield* gate.current).incarnation)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("opens a session from an existing shared scope", () =>
    Effect.scoped(Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sync = yield* PeerSync.PeerSync
      const permit = yield* gate.shared
      const session = yield* sync.open(yield* Identity.makePeerId)
      assert.strictEqual(session.replicaIncarnation, permit.incarnation)
    })).pipe(Effect.provide(TestLayer)))

  it.effect("exposes relay receipt maintenance only from the relay layer", () =>
    Effect.gen(function*() {
      assert.isUndefined((yield* PeerSync.PeerSync).pruneRelayReceipts)
    }).pipe(Effect.provide(TestLayer)).pipe(
      Effect.andThen(
        Effect.gen(function*() {
          assert.isDefined((yield* PeerSync.PeerSync).pruneRelayReceipts)
        }).pipe(Effect.provide(RelayTestLayer))
      )
    ))

  it.effect("scopes relay deduplication by sender without colliding with direct receipts and prunes usage", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const senderOne = yield* Identity.makePeerId
      const senderTwo = yield* Identity.makePeerId
      const relayPeerId = yield* Identity.makePeerId
      const relayMessageId = yield* Identity.makeRelayMessageId
      const sessionOne = yield* sync.open(senderOne)
      const sessionTwo = yield* sync.open(senderTwo)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "1".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "relayed"
        }
      )
      const generated = Automerge.generateSyncMessage(remote, Automerge.initSyncState())
      assert.isNotNull(generated[1])
      const message = generated[1]!
      const messageHash = yield* Canonical.digest(message)
      const receiptExpiresAt = new Date(
        (yield* Clock.currentTimeMillis) + PeerRelayReceiptLimits.defaults.receiptRetentionMillis
      ).toISOString()
      const relay = (
        senderPeerId: Identity.PeerId,
        senderSubjectId: string,
        outerEnvelopeDigest: string
      ): PeerSync.RelayReceipt => ({
        relayMessageId,
        relayPeerId,
        senderTenantId: "tenant",
        senderSubjectId,
        senderPeerId,
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(7),
        messageHash,
        outerEnvelopeDigest,
        receiptExpiresAt,
        encodedSize: message.byteLength
      })
      const input = {
        remoteConnectionEpoch: "shared-epoch",
        receiveSequence: 0,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      }

      const firstRelay = yield* sync.receive(Task, documentId, sessionOne, {
        ...input,
        relay: relay(senderOne, "sender-one", "a".repeat(64))
      })
      assert.isFalse(firstRelay.duplicate)

      const direct = yield* sync.receive(Task, documentId, sessionOne, input)
      assert.isFalse(direct.duplicate)

      const secondRelay = yield* sync.receive(Task, documentId, sessionTwo, {
        ...input,
        relay: relay(senderTwo, "sender-two", "b".repeat(64))
      })
      assert.isFalse(secondRelay.duplicate)

      const duplicate = yield* sync.receive(Task, documentId, sessionOne, {
        ...input,
        relay: {
          ...relay(senderOne, "sender-one", "a".repeat(64)),
          encodedSize: message.byteLength + 10_000
        }
      })
      assert.isTrue(duplicate.duplicate)
      const originalCharge = yield* sql<{
        readonly pendingMessage: Uint8Array | null
        readonly reply: Uint8Array | null
        readonly retainedBytes: number
        readonly writerProvenance: string
      }>`SELECT
        pending_message AS pendingMessage,
        reply,
        relay_encoded_size AS retainedBytes,
        writer_provenance AS writerProvenance
      FROM effect_local_peer_receipts
      WHERE relay_sender_peer_id = ${senderOne}
        AND relay_message_id = ${relayMessageId}`
      assert.strictEqual(originalCharge.length, 1)
      assert.strictEqual(
        originalCharge[0]!.retainedBytes,
        message.byteLength +
          (originalCharge[0]!.reply?.byteLength ?? 0) +
          (originalCharge[0]!.pendingMessage?.byteLength ?? 0) +
          new TextEncoder().encode(originalCharge[0]!.writerProvenance).byteLength
      )

      const beforePrune = yield* sql<{
        readonly deleteTokens: number
        readonly directReceipts: number
        readonly relayBytes: number
        readonly relayReceipts: number
        readonly retainedBytes: number
        readonly usageRows: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NULL) AS directReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NOT NULL) AS relayReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows,
        (SELECT COALESCE(SUM(encoded_bytes), 0)
          FROM effect_local_peer_relay_receipt_usage) AS relayBytes,
        (SELECT COALESCE(SUM(relay_encoded_size), 0)
          FROM effect_local_peer_receipts WHERE relay_message_id IS NOT NULL) AS retainedBytes,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`
      assert.deepStrictEqual(beforePrune.map(({ relayBytes: _, retainedBytes: __, ...row }) => row), [{
        deleteTokens: 0,
        directReceipts: 1,
        relayReceipts: 2,
        usageRows: 2
      }])
      assert.strictEqual(beforePrune[0]!.relayBytes, beforePrune[0]!.retainedBytes)
      assert.isAbove(beforePrune[0]!.relayBytes, message.byteLength * 2)

      yield* sql`UPDATE effect_local_peer_receipts
        SET relay_receipt_expires_at = '1970-01-01T00:00:00.000Z'
        WHERE relay_message_id IS NOT NULL`
      yield* sql`CREATE TRIGGER fail_relay_receipt_prune
        BEFORE DELETE ON effect_local_peer_receipts
        WHEN OLD.relay_message_id IS NOT NULL
        BEGIN
          SELECT RAISE(ABORT, 'Injected relay receipt prune failure');
        END`
      assert.strictEqual((yield* Effect.exit(sync.pruneRelayReceipts!))._tag, "Failure")
      yield* sql`DROP TRIGGER fail_relay_receipt_prune`
      assert.deepStrictEqual(
        yield* sql`SELECT
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE relay_message_id IS NOT NULL) AS relayReceipts,
          (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows,
          (SELECT COALESCE(SUM(encoded_bytes), 0)
            FROM effect_local_peer_relay_receipt_usage) AS relayBytes,
          (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`,
        [{
          deleteTokens: 0,
          relayBytes: beforePrune[0]!.relayBytes,
          relayReceipts: 2,
          usageRows: 2
        }]
      )
      assert.strictEqual(yield* sync.pruneRelayReceipts!, 1)

      const afterFirstBatch = yield* sql<{
        readonly deleteTokens: number
        readonly directReceipts: number
        readonly relayReceipts: number
        readonly usageRows: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NULL) AS directReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NOT NULL) AS relayReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`
      assert.deepStrictEqual(afterFirstBatch, [{
        deleteTokens: 0,
        directReceipts: 1,
        relayReceipts: 1,
        usageRows: 1
      }])
      assert.strictEqual(yield* sync.pruneRelayReceipts!, 1)

      const afterPrune = yield* sql<{
        readonly deleteTokens: number
        readonly directReceipts: number
        readonly relayReceipts: number
        readonly usageRows: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NULL) AS directReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE relay_message_id IS NOT NULL) AS relayReceipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_delete_tokens) AS deleteTokens`
      assert.deepStrictEqual(afterPrune, [{
        deleteTokens: 0,
        directReceipts: 1,
        relayReceipts: 0,
        usageRows: 0
      }])
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(RelayTestLayer)))

  it.effect("charges retained relay replies before committing receipt quota usage", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const senderPeerId = yield* Identity.makePeerId
      const relayPeerId = yield* Identity.makePeerId
      const created = yield* store.create(Task, documentId, {
        title: "x".repeat(8_000),
        labels: []
      })
      const remote = Automerge.init<InternalAutomerge.Root<{ title: string; labels: Array<string> }>>()
      const generated = Automerge.generateSyncMessage(remote, Automerge.initSyncState())
      assert.isNotNull(generated[1])
      const message = generated[1]!
      assert.isBelow(message.byteLength, 256)
      const before = yield* sql<{
        readonly changes: number
        readonly commitOutbox: number
        readonly receipts: number
        readonly usageRows: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_commit_outbox) AS commitOutbox,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows`
      const session = yield* sync.open(senderPeerId)
      const messageHash = yield* Canonical.digest(message)
      const error = yield* Effect.flip(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "sender-epoch",
        receiveSequence: 0,
        message,
        lineage: Identity.genesisLineage,
        writerProvenance: [],
        relay: {
          relayMessageId: yield* Identity.makeRelayMessageId,
          relayPeerId,
          senderTenantId: "tenant",
          senderSubjectId: "sender",
          senderPeerId,
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
          messageHash,
          outerEnvelopeDigest: "a".repeat(64),
          receiptExpiresAt: new Date(
            (yield* Clock.currentTimeMillis) + PeerRelayReceiptLimits.defaults.receiptRetentionMillis
          ).toISOString(),
          encodedSize: message.byteLength
        }
      }))
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      if (error.reason._tag === "QuotaExceeded") {
        assert.strictEqual(error.reason.resource, "relay receipt bytes per remote")
      }
      const after = yield* sql<{
        readonly changes: number
        readonly commitOutbox: number
        readonly receipts: number
        readonly usageRows: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_commit_outbox) AS commitOutbox,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS usageRows`
      assert.deepStrictEqual(after, before)
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, {
        title: "x".repeat(8_000),
        labels: []
      })
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TightRelayTestLayer)))

  it.effect("persists inbound application and exact retransmission replies", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const session = yield* sync.open(peerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "1".repeat(32) }), (draft) => {
        const value = draft.value as { title: string; labels: Array<string> }
        value.title = "remote"
        value.labels.push("synced")
      })
      let remoteState = Automerge.initSyncState()
      const generated = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = generated[0]
      assert.isNotNull(generated[1])
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: generated[1]!,
        writerProvenance: provenanceFor(generated[1]!, Task.version, definition.hash)
      })
      const durableReply = yield* sql<{ readonly outbox: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_outbox WHERE status = 'Pending') AS outbox,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(durableReply, [{ outbox: 0, receipts: 1 }])
      if (received.reply !== null) {
        const next = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
        remoteState = next[1]
        const outbound = yield* sync.enqueue(session, received.reply)
        yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
      }
      const nextGenerated = Automerge.generateSyncMessage(remote, remoteState)
      if (nextGenerated[1] !== null) {
        yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 1,
          lineage: Identity.genesisLineage,
          message: nextGenerated[1],
          writerProvenance: provenanceFor(nextGenerated[1], Task.version, definition.hash)
        })
      }
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "remote", labels: ["synced"] })
      const appliedChanges = yield* sql<{ readonly bytes: Uint8Array }>`
        SELECT bytes FROM effect_local_changes
        WHERE document_id = ${documentId} AND applied = 1
      `
      assert.strictEqual(reloaded.historyChanges, appliedChanges.length)
      assert.strictEqual(
        reloaded.historyOperations,
        appliedChanges.reduce((total, change) => total + Automerge.decodeChange(change.bytes).ops.length, 0)
      )
      assert.strictEqual(
        reloaded.historyBytes,
        appliedChanges.reduce((total, change) => total + change.bytes.byteLength, 0)
      )
      const historyBytes = reloaded.historyBytes
      const historyChanges = reloaded.historyChanges
      const historyOperations = reloaded.historyOperations
      assert.isNotNull(historyBytes)
      assert.isNotNull(historyChanges)
      assert.isNotNull(historyOperations)
      const duplicate = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: generated[1]!,
        writerProvenance: provenanceFor(generated[1]!, Task.version, definition.hash)
      })
      assert.isTrue(duplicate.duplicate)
      assert.deepStrictEqual(duplicate.reply?.message, received.reply?.message)
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`
      assert.strictEqual(rows[0]?.count, 2)
      const afterDuplicate = yield* sql<{
        readonly history_bytes: number
        readonly history_changes: number
        readonly history_operations: number
      }>`SELECT history_bytes, history_changes, history_operations
        FROM effect_local_documents WHERE document_id = ${documentId}`
      assert.deepStrictEqual(afterDuplicate, [{
        history_bytes: historyBytes,
        history_changes: historyChanges,
        history_operations: historyOperations
      }])
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("enqueues a reply that batches several changes for a stale peer", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })

      // A first peer contributes a deep shared history, then two more changes on top of it, so
      // this replica ends up holding exactly two changes a stale copy of that history lacks.
      let remote = Automerge.clone(created.automerge, { actor: "1".repeat(32) })
      for (let index = 0; index < 8; index++) {
        remote = Automerge.change(remote, (draft) => {
          const value = draft.value as { title: string; labels: Array<string> }
          value.labels.push(`base-${index}`)
        })
      }
      const firstPeer = yield* Identity.makePeerId
      const firstSession = yield* sync.open(firstPeer)
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      const drain = Effect.gen(function*() {
        while (sequence < 20) {
          const [nextState, message] = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = nextState
          if (message === null) return
          const received = yield* sync.receive(Task, documentId, firstSession, {
            remoteConnectionEpoch: firstSession.connectionEpoch,
            receiveSequence: sequence++,
            lineage: Identity.genesisLineage,
            message,
            writerProvenance: provenanceFor(message)
          })
          if (received.reply !== null) {
            const advanced = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
            remote = advanced[0]
            remoteState = advanced[1]
          }
        }
      })
      yield* drain
      const staleDocument = Automerge.clone(remote, { actor: "2".repeat(32) })
      for (const label of ["from-remote-1", "from-remote-2"]) {
        remote = Automerge.change(remote, (draft) => {
          const value = draft.value as { title: string; labels: Array<string> }
          value.labels.push(label)
        })
      }
      yield* drain

      // A peer holding the shared history but not the two newest changes announces itself. The
      // reply must carry exactly those two changes, and automerge's v2 sync protocol
      // concatenates them into one chunk - the shape every reply takes when the peer is more
      // than one change behind, which is any peer that was offline while this replica kept
      // committing. (A peer missing MORE than a third of the history gets a whole-document
      // chunk instead, which decodes standalone.)
      const stalePeer = yield* Identity.makePeerId
      const staleSession = yield* sync.open(stalePeer)
      const [, announce] = Automerge.generateSyncMessage(staleDocument, Automerge.initSyncState())
      assert.isNotNull(announce)
      const received = yield* sync.receive(Task, documentId, staleSession, {
        remoteConnectionEpoch: staleSession.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: announce!,
        writerProvenance: provenanceFor(announce!)
      })
      assert.isNotNull(received.reply)
      const outbound = yield* sync.enqueue(staleSession, received.reply!)
      assert.isAtLeast(
        outbound.writerProvenance.length,
        2,
        "the batched reply's provenance covers every change it carries"
      )
      Automerge.free(remote)
      Automerge.free(staleDocument)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects cumulative source changes, operations and bytes atomically and recovers", () =>
    Effect.gen(function*() {
      for (
        const quota of [
          {
            key: "maxConflictSourceChanges",
            resource: "conflict source changes",
            value: (boundary: { readonly changes: number }) => boundary.changes
          },
          {
            key: "maxConflictSourceOperations",
            resource: "conflict source operations",
            value: (boundary: { readonly operations: number }) => boundary.operations
          },
          {
            key: "maxConflictSourceBytes",
            resource: "conflict source bytes",
            value: (boundary: { readonly bytes: number }) => boundary.bytes
          }
        ] as const
      ) {
        const filename = join(
          tmpdir(),
          `effect-local-peer-source-${quota.key}-${globalThis.crypto.randomUUID()}.sqlite`
        )
        yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
        const seeded = yield* seedSourceReceive(filename)
        const limit = quota.value(seeded.boundary) - 1
        const rejectingLimits = { ...limits, [quota.key]: limit }
        yield* Effect.scoped(
          Effect.gen(function*() {
            const sync = yield* PeerSync.PeerSync
            const before = yield* durableFootprint(seeded.documentId)
            const session = yield* sync.open(yield* Identity.makePeerId)
            const result = yield* Effect.result(sync.receive(Task, seeded.documentId, session, {
              ...seeded.input,
              remoteConnectionEpoch: session.connectionEpoch,
              receiveSequence: 0
            }))
            assert.isTrue(Result.isFailure(result))
            if (Result.isFailure(result)) {
              assert.strictEqual(result.failure.reason._tag, "QuotaExceeded")
              if (result.failure.reason._tag === "QuotaExceeded") {
                assert.strictEqual(result.failure.reason.resource, quota.resource)
                assert.strictEqual(result.failure.reason.limit, limit)
              }
            }
            assert.deepStrictEqual(yield* durableFootprint(seeded.documentId), before)
          }).pipe(Effect.provide(sourceLayer(rejectingLimits, filename)))
        )
        yield* Effect.scoped(
          Effect.gen(function*() {
            const replica = yield* Replica.Replica
            const sync = yield* PeerSync.PeerSync
            const session = yield* sync.open(yield* Identity.makePeerId)
            const received = yield* sync.receive(Task, seeded.documentId, session, {
              ...seeded.input,
              remoteConnectionEpoch: session.connectionEpoch,
              receiveSequence: 0
            })
            assert.isFalse(received.duplicate)
            assert.strictEqual((yield* replica.get(Task, seeded.documentId)).value.title, "remote")
          }).pipe(Effect.provide(sourceLayer(limits, filename)))
        )
      }
    }))

  it.effect("accepts a peer change exactly at every cumulative source boundary", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-peer-source-boundary-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
      const seeded = yield* seedSourceReceive(filename)
      const boundaryLimits = {
        ...limits,
        maxConflictSourceBytes: seeded.boundary.bytes,
        maxConflictSourceChanges: seeded.boundary.changes,
        maxConflictSourceOperations: seeded.boundary.operations
      }
      yield* Effect.scoped(
        Effect.gen(function*() {
          const sync = yield* PeerSync.PeerSync
          const session = yield* sync.open(yield* Identity.makePeerId)
          const received = yield* sync.receive(Task, seeded.documentId, session, {
            ...seeded.input,
            remoteConnectionEpoch: session.connectionEpoch,
            receiveSequence: 0
          })
          assert.isFalse(received.duplicate)
          const footprint = yield* durableFootprint(seeded.documentId)
          assert.strictEqual(footprint[0]?.history_bytes, seeded.boundary.bytes)
          assert.strictEqual(footprint[0]?.history_changes, seeded.boundary.changes)
          assert.strictEqual(footprint[0]?.history_operations, seeded.boundary.operations)
        }).pipe(Effect.provide(sourceLayer(boundaryLimits, filename)))
      )
    }))

  it.effect("rejects unmeasured and corrupt history counters without durable effects", () =>
    Effect.gen(function*() {
      for (
        const counterCase of [
          {
            expected: "QuotaExceeded",
            state: "unmeasured",
            resource: "unmeasured conflict source history"
          },
          { expected: "StorageCorrupt", state: "mixed" },
          { expected: "StorageCorrupt", state: "mismatched" }
        ] as const
      ) {
        const filename = join(
          tmpdir(),
          `effect-local-peer-source-${counterCase.state}-${globalThis.crypto.randomUUID()}.sqlite`
        )
        yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
        const seeded = yield* seedSourceReceive(filename, counterCase.state)
        yield* Effect.scoped(
          Effect.gen(function*() {
            const sync = yield* PeerSync.PeerSync
            const before = yield* durableFootprint(seeded.documentId)
            const session = yield* sync.open(yield* Identity.makePeerId)
            const result = yield* Effect.result(sync.receive(Task, seeded.documentId, session, {
              ...seeded.input,
              remoteConnectionEpoch: session.connectionEpoch,
              receiveSequence: 0
            }))
            assert.isTrue(Result.isFailure(result))
            if (Result.isFailure(result)) {
              assert.strictEqual(result.failure.reason._tag, counterCase.expected)
              if (
                counterCase.expected === "QuotaExceeded" &&
                result.failure.reason._tag === "QuotaExceeded"
              ) {
                assert.strictEqual(result.failure.reason.resource, counterCase.resource)
              }
            }
            assert.deepStrictEqual(yield* durableFootprint(seeded.documentId), before)
          }).pipe(Effect.provide(sourceLayer(limits, filename)))
        )
      }
    }))

  it.effect("interrupts a blocked receive without durable effects and accepts a retry", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-peer-source-interrupt-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
      const seeded = yield* seedSourceReceive(filename)
      yield* Effect.scoped(
        Effect.gen(function*() {
          const replica = yield* Replica.Replica
          const sync = yield* PeerSync.PeerSync
          const sql = yield* SqlClient.SqlClient
          const session = yield* sync.open(yield* Identity.makePeerId)
          const input = {
            ...seeded.input,
            remoteConnectionEpoch: session.connectionEpoch,
            receiveSequence: 0
          }
          const before = yield* durableFootprint(seeded.documentId)
          const locked = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const blocker = yield* sql.withTransaction(
            Deferred.succeed(locked, undefined).pipe(
              Effect.andThen(Deferred.await(release))
            )
          ).pipe(Effect.forkChild({ startImmediately: true }))
          yield* Deferred.await(locked)
          const receiving = yield* sync.receive(Task, seeded.documentId, session, input).pipe(
            Effect.forkChild({ startImmediately: true })
          )
          assert.isUndefined(receiving.pollUnsafe())
          const interrupting = yield* Fiber.interrupt(receiving).pipe(
            Effect.forkChild({ startImmediately: true })
          )
          yield* Effect.yieldNow
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(blocker)
          yield* Fiber.join(interrupting)
          const interrupted = yield* Fiber.await(receiving)
          assert.strictEqual(interrupted._tag, "Failure")
          if (interrupted._tag === "Failure") {
            assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause))
          }
          assert.deepStrictEqual(yield* durableFootprint(seeded.documentId), before)
          assert.isFalse((yield* sync.receive(Task, seeded.documentId, session, input)).duplicate)
          assert.strictEqual((yield* replica.get(Task, seeded.documentId)).value.title, "remote")
        }).pipe(Effect.provide(sourceLayer(limits, filename)))
      )
    }))

  it.effect("persists the wire declared writer provenance for an inbound change, not the receiver's own", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "7".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "from another build"
      })
      const remoteChangeBytes = Automerge.getChangesSince(remote, [...created.materializedHeads])
      assert.strictEqual(remoteChangeBytes.length, 1)
      const remoteChangeHash = Automerge.decodeChange(remoteChangeBytes[0]!).hash
      const writerSchemaVersion = Task.version + 41
      const writerDefinitionHash = "a-different-peers-build-hash"
      let remoteState = Automerge.initSyncState()
      const firstGenerated = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = firstGenerated[0]
      assert.isNotNull(firstGenerated[1])
      const first = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: firstGenerated[1]!,
        writerProvenance: provenanceFor(firstGenerated[1]!).map((entry) =>
          entry.changeHash === remoteChangeHash
            ? { changeHash: entry.changeHash, writerSchemaVersion, writerDefinitionHash }
            : entry
        )
      })
      if (first.reply !== null) {
        remoteState = Automerge.receiveSyncMessage(remote, remoteState, first.reply.message)[1]
      }
      const secondGenerated = Automerge.generateSyncMessage(remote, remoteState)
      if (secondGenerated[1] !== null) {
        yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 1,
          lineage: Identity.genesisLineage,
          message: secondGenerated[1],
          writerProvenance: provenanceFor(secondGenerated[1]).map((entry) =>
            entry.changeHash === remoteChangeHash
              ? { changeHash: entry.changeHash, writerSchemaVersion, writerDefinitionHash }
              : entry
          )
        })
      }
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "from another build", labels: [] })
      const rows = yield* sql<{
        readonly writer_schema_version: number
        readonly writer_definition_hash: string
      }>`SELECT writer_schema_version, writer_definition_hash FROM effect_local_changes
        WHERE change_hash = ${remoteChangeHash}`
      assert.deepStrictEqual(rows, [{
        writer_schema_version: writerSchemaVersion,
        writer_definition_hash: writerDefinitionHash
      }])
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("preserves each change writer provenance in a relayed sync message", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const origin = Automerge.change(
        Automerge.clone(created.automerge, { actor: "a".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "origin"
        }
      )
      const relayed = Automerge.change(
        Automerge.clone(origin, { actor: "b".repeat(32) }),
        (draft) => {
          ;(draft.value as unknown as { labels: Array<string> }).labels.push("relay")
        }
      )
      const originHash = Automerge.decodeChange(
        Automerge.getChangesSince(origin, [...created.materializedHeads])[0]!
      ).hash
      const relayHash = Automerge.decodeChange(
        Automerge.getChangesSince(relayed, Automerge.getHeads(origin))[0]!
      ).hash
      const originWriter = {
        writerSchemaVersion: Task.version + 10,
        writerDefinitionHash: "origin-definition"
      }
      const relayWriter = {
        writerSchemaVersion: Task.version + 20,
        writerDefinitionHash: "relay-definition"
      }
      let remote = Automerge.clone(relayed)
      let remoteState = Automerge.initSyncState()
      let receiveSequence = 0
      for (let round = 0; round < 4; round++) {
        const generated = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = generated[0]
        if (generated[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: "relay",
          receiveSequence: receiveSequence++,
          lineage: Identity.genesisLineage,
          message: generated[1],
          writerProvenance: provenanceFor(generated[1]).map((entry) => {
            if (entry.changeHash === originHash) return Object.assign({ changeHash: entry.changeHash }, originWriter)
            if (entry.changeHash === relayHash) return Object.assign({ changeHash: entry.changeHash }, relayWriter)
            return entry
          })
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      const rows = yield* sql<{
        readonly change_hash: string
        readonly writer_schema_version: number
        readonly writer_definition_hash: string
      }>`SELECT change_hash, writer_schema_version, writer_definition_hash
        FROM effect_local_changes
        WHERE change_hash = ${originHash} OR change_hash = ${relayHash}`
      const byHash = new Map(rows.map((row) => [row.change_hash, row]))
      assert.deepInclude(byHash.get(originHash), {
        writer_schema_version: originWriter.writerSchemaVersion,
        writer_definition_hash: originWriter.writerDefinitionHash
      })
      assert.deepInclude(byHash.get(relayHash), {
        writer_schema_version: relayWriter.writerSchemaVersion,
        writer_definition_hash: relayWriter.writerDefinitionHash
      })
      InternalAutomerge.free(origin)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(relayed)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("sends exact writer provenance after compaction prunes change rows", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      yield* compaction.compact(Task, documentId)
      assert.strictEqual(yield* compaction.prune(documentId), 1)

      const remote = Automerge.init<InternalAutomerge.Root<{ title: string; labels: Array<string> }>>()
      const handshake = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const session = yield* sync.open(yield* Identity.makePeerId)
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "fresh-peer",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: handshake,
        writerProvenance: []
      })
      assert.isNotNull(received.reply)
      const outbound = yield* sync.enqueue(session, received.reply!)
      assert.deepStrictEqual(
        outbound.writerProvenance,
        provenanceFor(outbound.message, Task.version, definition.hash)
      )

      InternalAutomerge.free(remote)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(CompactionLayer)))

  it.effect("rejects invalid writer provenance at the direct receive boundary", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "c".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "invalid"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const remoteHash = Automerge.decodeChange(
        Automerge.getChangesSince(remote, [...created.materializedHeads])[0]!
      ).hash
      const before = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      for (
        const provenance of [
          { writerSchemaVersion: 0, writerDefinitionHash: definition.hash },
          { writerSchemaVersion: 1.5, writerDefinitionHash: definition.hash },
          { writerSchemaVersion: Number.MAX_SAFE_INTEGER + 1, writerDefinitionHash: definition.hash },
          { writerSchemaVersion: Task.version, writerDefinitionHash: "" },
          { writerSchemaVersion: Task.version, writerDefinitionHash: "x".repeat(257) },
          { writerSchemaVersion: Task.version, writerDefinitionHash: "界".repeat(256) },
          { writerSchemaVersion: Task.version, writerDefinitionHash: "\0".repeat(256) }
        ]
      ) {
        const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: [{
            changeHash: remoteHash,
            writerSchemaVersion: provenance.writerSchemaVersion,
            writerDefinitionHash: provenance.writerDefinitionHash
          }]
        }))
        assert.strictEqual(exit._tag, "Failure", JSON.stringify(provenance))
        if (exit._tag === "Failure") {
          assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exit.cause)).reason._tag, "ProtocolMismatch")
        }
      }
      const after = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(after, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("does not write a checkpoint for a sync message that does not advance heads", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })

      // The remote is an exact clone, so everything it sends back is already applied locally and
      // the canonical heads cannot move. The message still carries changes, which is what makes
      // this distinguishable from a heads-only message.
      const remote = Automerge.clone(created.automerge, { actor: "b".repeat(32) })
      const emptyPeer = Automerge.init<InternalAutomerge.Root<{ title: string; labels: Array<string> }>>()
      const localHandshake = Automerge.generateSyncMessage(emptyPeer, Automerge.initSyncState())[1]!
      const receivedHandshake = Automerge.receiveSyncMessage(remote, Automerge.initSyncState(), localHandshake)
      const message = Automerge.generateSyncMessage(receivedHandshake[0], receivedHandshake[1])[1]!
      assert.isAbove(Automerge.decodeSyncMessage(message).changes.length, 0)

      const beforeCheckpoints = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      assert.strictEqual(beforeCheckpoints[0]!.count, 0)

      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "no-transition",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })

      const after = yield* sql<{
        readonly checkpoints: number
        readonly checkpoint_hash: string | null
        readonly receipts: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_checkpoints WHERE document_id = ${documentId}) AS checkpoints,
        (SELECT checkpoint_hash FROM effect_local_documents WHERE document_id = ${documentId}) AS checkpoint_hash,
        (SELECT COUNT(*) FROM effect_local_peer_receipts WHERE document_id = ${documentId}) AS receipts`
      assert.strictEqual(after[0]!.checkpoints, 0)
      assert.strictEqual(after[0]!.checkpoint_hash, null)
      // The message is still fully processed: the receipt is durable and a reply was produced.
      assert.strictEqual(after[0]!.receipts, 1)
      assert.isNotNull(received.reply)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects missing, duplicate, unrelated, and excess provenance mappings", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "b".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "mapped"
        }
      )
      const emptyPeer = Automerge.init<InternalAutomerge.Root<{ title: string; labels: Array<string> }>>()
      const localHandshake = Automerge.generateSyncMessage(emptyPeer, Automerge.initSyncState())[1]!
      const receivedHandshake = Automerge.receiveSyncMessage(
        remote,
        Automerge.initSyncState(),
        localHandshake
      )
      remote = receivedHandshake[0]
      const message = Automerge.generateSyncMessage(remote, receivedHandshake[1])[1]!
      const valid = provenanceFor(message, Task.version, definition.hash)
      assert.isAbove(valid.length, 0)
      const usedHashes = new Set(valid.map((entry) => entry.changeHash))
      const unrelatedHash = ["a", "b", "c"].map((value) => value.repeat(64))
        .find((changeHash) => !usedHashes.has(changeHash))!
      const unrelated = {
        ...valid[0]!,
        changeHash: unrelatedHash
      }
      const before = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      for (
        const provenance of [
          valid.slice(1),
          [...valid, valid[0]!],
          [unrelated, ...valid.slice(1)],
          [...valid, unrelated]
        ]
      ) {
        const result = yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: "mapping",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenance
        }))
        assert.strictEqual(result._tag, "Failure")
        if (result._tag === "Failure") {
          assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(result.cause)).reason._tag, "ProtocolMismatch")
        }
      }
      const after = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(after, before)
      InternalAutomerge.free(emptyPeer)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects a retransmission that changes writer provenance", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "d".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "remote"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "remote",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      const conflict = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "remote",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: [{
          changeHash: "a".repeat(64),
          writerSchemaVersion: Task.version + 1,
          writerDefinitionHash: "different-definition"
        }]
      }))
      assert.strictEqual(conflict._tag, "Failure")
      if (conflict._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(conflict.cause)).reason._tag, "ProtocolMismatch")
      }
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("keeps locally authored changes stamped with their exact definition", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const rows = yield* sql<{
        readonly writer_schema_version: number
        readonly writer_definition_hash: string
      }>`SELECT writer_schema_version, writer_definition_hash FROM effect_local_changes
        WHERE document_id = ${documentId}`
      assert.isAtLeast(rows.length, 1)
      for (const row of rows) {
        assert.strictEqual(row.writer_schema_version, Task.version)
        assert.strictEqual(row.writer_definition_hash, definition.hash)
      }
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rebinds a durable reply to each local connection epoch", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const firstSession = yield* sync.open(peerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "9".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "remote"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const first = yield* sync.receive(Task, documentId, firstSession, {
        remoteConnectionEpoch: "stable-remote-epoch",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      assert.isNotNull(first.reply)
      const firstOutbound = yield* sync.enqueue(firstSession, first.reply!)
      const secondSession = yield* sync.open(peerId)
      const replayed = yield* sync.receive(Task, documentId, secondSession, {
        remoteConnectionEpoch: "stable-remote-epoch",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      assert.isTrue(replayed.duplicate)
      assert.isNotNull(replayed.reply)
      const secondOutbound = yield* sync.enqueue(secondSession, replayed.reply!)
      assert.notStrictEqual(firstSession.connectionEpoch, secondSession.connectionEpoch)
      assert.strictEqual(firstOutbound.sendSequence, 0)
      assert.strictEqual(secondOutbound.sendSequence, 0)
      const rows = yield* sql<{ readonly connection_epoch: string; readonly send_sequence: number }>`
        SELECT connection_epoch, send_sequence FROM effect_local_peer_outbox ORDER BY connection_epoch
      `
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows.map((row) => row.send_sequence), [0, 0])
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects connection sequence reuse for another document", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const firstDocumentId = yield* Identity.makeDocumentId
      const secondDocumentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const first = yield* store.create(Task, firstDocumentId, { title: "one", labels: [] })
      const second = yield* store.create(Task, secondDocumentId, { title: "two", labels: [] })
      const remote = Automerge.change(Automerge.clone(first.automerge, { actor: "8".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "remote"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      yield* sync.receive(Task, firstDocumentId, session, {
        remoteConnectionEpoch: "remote-epoch",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      const reused = yield* Effect.exit(sync.receive(Task, secondDocumentId, session, {
        remoteConnectionEpoch: "remote-epoch",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      }))
      assert.strictEqual(reused._tag, "Failure")
      if (reused._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(reused.cause)).reason._tag, "ProtocolMismatch")
      }
      InternalAutomerge.free(remote)
      InternalAutomerge.free(first.automerge)
      InternalAutomerge.free(second.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects altered sequence retransmissions without durable mutation", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "2".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      const before = yield* sql<{ readonly receipts: number; readonly changes: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes`
      const altered = Uint8Array.from(message)
      altered[altered.length - 1] = altered[altered.length - 1]! ^ 1
      const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: altered,
        writerProvenance: provenanceFor(altered, Task.version, definition.hash)
      }))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exit.cause)).reason._tag, "ProtocolMismatch")
      }
      const after = yield* sql<{ readonly receipts: number; readonly changes: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes`
      assert.deepStrictEqual(after, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects a valid message from a stale incarnation without durable mutation", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "preserved", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "7".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "rejected"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const before = yield* sql<{
        readonly accepted_heads: string
        readonly changes: number
        readonly checkpoints: number
        readonly commit_sequence: number
        readonly materialized_heads: string
        readonly receipts: number
      }>`SELECT
        (SELECT accepted_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS accepted_heads,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_checkpoints) AS checkpoints,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
        (SELECT materialized_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS materialized_heads,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      yield* gate.claim(() => Effect.void)

      assert.strictEqual(
        (yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        })))._tag,
        "Failure"
      )
      const after = yield* sql<{
        readonly accepted_heads: string
        readonly changes: number
        readonly checkpoints: number
        readonly commit_sequence: number
        readonly materialized_heads: string
        readonly receipts: number
      }>`SELECT
        (SELECT accepted_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS accepted_heads,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_checkpoints) AS checkpoints,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
        (SELECT materialized_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS materialized_heads,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(after, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("keeps accepting resolved sync receipts past the peer receipt quota", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "6".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      for (const sequence of [0, 1, 2, 3]) {
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const outbound = yield* sync.enqueue(session, received.reply)
          yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
        }
      }
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE peer_id = ${session.peerId} AND connection_epoch = ${session.connectionEpoch}`
      assert.strictEqual(rows[0]?.count, 4)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(ReceiptLayer)))

  it.effect("keeps accepting resolved sync receipts past the document receipt quota", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "6".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      for (const sequence of [0, 1, 2, 3]) {
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const outbound = yield* sync.enqueue(session, received.reply)
          yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
        }
      }
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE document_id = ${documentId}`
      assert.strictEqual(rows[0]?.count, 4)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(DocumentReceiptLayer)))

  it.effect("keeps accepting resolved sync receipts past the replica receipt quota", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "6".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      for (const sequence of [0, 1, 2, 3]) {
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const outbound = yield* sync.enqueue(session, received.reply)
          yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
        }
      }
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`
      assert.strictEqual(rows[0]?.count, 4)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(ReplicaReceiptLayer)))

  it.effect("accepts a resolved receipt when genuinely pending receipts exhaust the quota", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "6".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      for (const sequence of [0, 1]) {
        yield* sql`INSERT INTO effect_local_peer_receipts (
          replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
          message_hash, reply, reply_hash, pending_message, heads,
          accepted_heads, commit_sequence, accepted_at
        ) VALUES (
          ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, ${sequence}, ${documentId},
          ${`pending-${sequence}`}, NULL, NULL, ${message}, '[]', ${JSON.stringify(["a".repeat(64)])}, 0,
          ${new Date(0).toISOString()}
        )`
      }
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 2,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      assert.isFalse(received.duplicate)
      const rows = yield* sql<{ readonly pending: number; readonly total: number }>`SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN pending_message IS NOT NULL THEN 1 END) AS pending
        FROM effect_local_peer_receipts
        WHERE peer_id = ${session.peerId} AND connection_epoch = ${session.connectionEpoch}`
      assert.deepStrictEqual(rows, [{ pending: 2, total: 3 }])
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(ReceiptLayer)))

  it.effect("rejects a further real pending receive once the peer receipt quota is genuinely exhausted", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "7".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      for (let round = 0; round < 4; round++) {
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        if (outbound[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1],
          writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "first"
      })
      const first = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = first[0]
      assert.isNotNull(first[1])
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as unknown as { labels: Array<string> }).labels.push("second")
      })
      const second = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(second[1])
      const pendingSequence = sequence++
      const pending = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: pendingSequence,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: provenanceFor(second[1]!, Task.version, definition.hash)
      })
      assert.isFalse(sameHeadsForTest(pending.heads, Automerge.getHeads(remote)))
      const pendingRow = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE peer_id = ${session.peerId} AND connection_epoch = ${session.connectionEpoch}
          AND receive_sequence = ${pendingSequence} AND pending_message IS NOT NULL`
      assert.strictEqual(pendingRow[0]?.count, 1)
      const beforeRejected = yield* sql<{
        readonly acceptedHeads: string
        readonly changes: number
        readonly commitSequence: number
        readonly materializedHeads: string
        readonly receipts: number
      }>`SELECT
        (SELECT accepted_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS acceptedHeads,
        (SELECT COUNT(*) FROM effect_local_changes WHERE document_id = ${documentId}) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commitSequence,
        (SELECT materialized_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS materializedHeads,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      const exhausted = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: provenanceFor(second[1]!, Task.version, definition.hash)
      }))
      assert.strictEqual(exhausted._tag, "Failure")
      if (exhausted._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exhausted.cause)).reason._tag, "QuotaExceeded")
      }
      const afterRejected = yield* sql<{
        readonly acceptedHeads: string
        readonly changes: number
        readonly commitSequence: number
        readonly materializedHeads: string
        readonly receipts: number
      }>`SELECT
        (SELECT accepted_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS acceptedHeads,
        (SELECT COUNT(*) FROM effect_local_changes WHERE document_id = ${documentId}) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commitSequence,
        (SELECT materialized_heads FROM effect_local_documents WHERE document_id = ${documentId}) AS materializedHeads,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(afterRejected, beforeRejected)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(PendingReceiptLayer)))

  it.effect("replays and resolves a pending receipt while every receipt quota is at capacity", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "8".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      for (let round = 0; round < 4; round++) {
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        if (outbound[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1],
          writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "first"
      })
      const first = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = first[0]
      assert.isNotNull(first[1])
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as unknown as { labels: Array<string> }).labels.push("second")
      })
      const second = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(second[1])
      const pendingSequence = sequence++
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: pendingSequence,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: provenanceFor(second[1]!, Task.version, definition.hash)
      })
      const beforeRecovery = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE replica_incarnation = ${session.replicaIncarnation}
          AND document_id = ${documentId} AND pending_message IS NOT NULL`
      assert.strictEqual(beforeRecovery[0]?.count, 1)
      const replay = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: pendingSequence,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: provenanceFor(second[1]!, Task.version, definition.hash)
      })
      assert.isTrue(replay.duplicate)

      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: first[1]!,
        writerProvenance: provenanceFor(first[1]!, Task.version, definition.hash)
      })

      const afterRecovery = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE replica_incarnation = ${session.replicaIncarnation}
          AND document_id = ${documentId} AND pending_message IS NOT NULL`
      assert.strictEqual(afterRecovery[0]?.count, 0)
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "first", labels: ["second"] })
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(CapacityReceiptLayer)))

  it.effect("rejects conflicting provenance for a change held by another pending receipt", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "7".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      for (let round = 0; round < 4; round++) {
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        if (outbound[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1],
          writerProvenance: provenanceFor(outbound[1])
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "dependency"
      })
      const dependency = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = dependency[0]
      assert.isNotNull(dependency[1])
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as unknown as { labels: Array<string> }).labels.push("dependent")
      })
      const dependent = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(dependent[1])
      const provenance = provenanceFor(dependent[1]!)
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: dependent[1]!,
        writerProvenance: provenance
      })

      const conflict = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence,
        lineage: Identity.genesisLineage,
        message: dependent[1]!,
        writerProvenance: provenance.map((entry) => ({
          changeHash: entry.changeHash,
          writerSchemaVersion: entry.writerSchemaVersion,
          writerDefinitionHash: "conflicting-pending-definition"
        }))
      }))
      assert.strictEqual(conflict._tag, "Failure")
      if (conflict._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(conflict.cause)).reason._tag, "ProtocolMismatch")
      }
      assert.deepStrictEqual(
        yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_peer_receipts
          WHERE document_id = ${documentId} AND pending_message IS NOT NULL`,
        [{ count: 1 }]
      )
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("accepts resolved input after the pending peer quota is lowered below durable usage", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const session = yield* sync.open(yield* Identity.makePeerId)
      const pendingDocumentCleanup = new Array<() => void>()
      const makePending = (actor: string, sequenceBase: number) =>
        Effect.gen(function*() {
          const documentId = yield* Identity.makeDocumentId
          const created = yield* store.create(Task, documentId, { title: actor, labels: [] })
          let remote = Automerge.clone(created.automerge, { actor: actor.repeat(32) })
          let remoteState = Automerge.initSyncState()
          let sequence = sequenceBase
          for (let round = 0; round < 4; round++) {
            const outbound = Automerge.generateSyncMessage(remote, remoteState)
            remoteState = outbound[0]
            if (outbound[1] === null) break
            const received = yield* sync.receive(Task, documentId, session, {
              remoteConnectionEpoch: `remote-${actor}`,
              receiveSequence: sequence++,
              lineage: Identity.genesisLineage,
              message: outbound[1],
              writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
            })
            if (received.reply !== null) {
              const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
              remote = applied[0]
              remoteState = applied[1]
            }
          }
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as { title: string }).title = `${actor}-first`
          })
          const dependency = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = dependency[0]
          assert.isNotNull(dependency[1])
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as unknown as { labels: Array<string> }).labels.push(`${actor}-second`)
          })
          const dependent = Automerge.generateSyncMessage(remote, remoteState)
          assert.isNotNull(dependent[1])
          yield* sync.receive(Task, documentId, session, {
            remoteConnectionEpoch: `remote-${actor}`,
            receiveSequence: sequence,
            lineage: Identity.genesisLineage,
            message: dependent[1]!,
            writerProvenance: provenanceFor(dependent[1]!, Task.version, definition.hash)
          })
          pendingDocumentCleanup.push(() => {
            InternalAutomerge.free(remote)
            InternalAutomerge.free(created.automerge)
          })
        })
      yield* makePending("e", 100)
      yield* makePending("f", 200)
      assert.deepStrictEqual(
        yield* sql<{ readonly pending: number }>`SELECT COUNT(*) AS pending
          FROM effect_local_peer_receipts
          WHERE peer_id = ${session.peerId} AND pending_message IS NOT NULL`,
        [{ pending: 2 }]
      )

      const resolvedDocumentId = yield* Identity.makeDocumentId
      const resolvedCreated = yield* store.create(Task, resolvedDocumentId, { title: "resolved", labels: [] })
      const resolvedRemote = Automerge.change(
        Automerge.clone(resolvedCreated.automerge, { actor: "9".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "accepted"
        }
      )
      const resolvedMessage = Automerge.generateSyncMessage(resolvedRemote, Automerge.initSyncState())[1]!
      const restartedExit = yield* Effect.scoped(
        Effect.gen(function*() {
          const restarted = yield* PeerSync.PeerSync
          return yield* Effect.exit(restarted.receive(Task, resolvedDocumentId, session, {
            remoteConnectionEpoch: "resolved-remote",
            receiveSequence: 0,
            lineage: Identity.genesisLineage,
            message: resolvedMessage,
            writerProvenance: provenanceFor(resolvedMessage, Task.version, definition.hash)
          }))
        }).pipe(
          Effect.provide(
            Layer.fresh(PeerSync.layer).pipe(
              Layer.provide(ReplicaLimits.layer({ ...limits, maxPendingChangesPerPeer: 1 }))
            )
          )
        )
      )
      assert.strictEqual(restartedExit._tag, "Success")
      for (const cleanup of pendingDocumentCleanup) cleanup()
      InternalAutomerge.free(resolvedRemote)
      InternalAutomerge.free(resolvedCreated.automerge)
    }).pipe(Effect.provide(ReceiptLayer)))

  it.effect("admits one concurrent pending receipt and rolls back the rejected admission", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const session = yield* sync.open(yield* Identity.makePeerId)
      const prepare = (actor: string, sequenceBase: number) =>
        Effect.gen(function*() {
          const documentId = yield* Identity.makeDocumentId
          const remoteConnectionEpoch = `remote-${actor}`
          const created = yield* store.create(Task, documentId, { title: actor, labels: [] })
          let remote = Automerge.clone(created.automerge, { actor: actor.repeat(32) })
          let remoteState = Automerge.initSyncState()
          let sequence = sequenceBase
          for (let round = 0; round < 4; round++) {
            const outbound = Automerge.generateSyncMessage(remote, remoteState)
            remoteState = outbound[0]
            if (outbound[1] === null) break
            const received = yield* sync.receive(Task, documentId, session, {
              remoteConnectionEpoch,
              receiveSequence: sequence++,
              lineage: Identity.genesisLineage,
              message: outbound[1],
              writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
            })
            if (received.reply !== null) {
              const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
              remote = applied[0]
              remoteState = applied[1]
            }
          }
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as { title: string }).title = `${actor}-first`
          })
          const dependency = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = dependency[0]
          assert.isNotNull(dependency[1])
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as unknown as { labels: Array<string> }).labels.push(`${actor}-second`)
          })
          const dependent = Automerge.generateSyncMessage(remote, remoteState)
          assert.isNotNull(dependent[1])
          return {
            created,
            dependency: dependency[1]!,
            dependent: dependent[1]!,
            documentId,
            remote,
            remoteConnectionEpoch,
            sequence
          }
        })
      const prepared = [
        yield* prepare("c", 100),
        yield* prepare("d", 200)
      ]
      const attempts = yield* Effect.all(
        prepared.map((item) =>
          Effect.exit(sync.receive(Task, item.documentId, session, {
            remoteConnectionEpoch: item.remoteConnectionEpoch,
            receiveSequence: item.sequence,
            lineage: Identity.genesisLineage,
            message: item.dependent,
            writerProvenance: provenanceFor(item.dependent, Task.version, definition.hash)
          }))
        ),
        { concurrency: "unbounded" }
      )
      const winnerIndex = attempts.findIndex((exit) => exit._tag === "Success")
      const loserIndex = attempts.findIndex((exit) => exit._tag === "Failure")
      assert.isAtLeast(winnerIndex, 0)
      assert.isAtLeast(loserIndex, 0)
      if (attempts[loserIndex]!._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(attempts[loserIndex]!.cause))
        assert.strictEqual(error.reason._tag, "QuotaExceeded")
        if (error.reason._tag === "QuotaExceeded") assert.strictEqual(error.reason.resource, "peer sync receipts")
      }
      assert.deepStrictEqual(
        yield* sql<{ readonly pending: number }>`SELECT COUNT(*) AS pending
          FROM effect_local_peer_receipts WHERE pending_message IS NOT NULL`,
        [{ pending: 1 }]
      )

      const winner = prepared[winnerIndex]!
      yield* sync.receive(Task, winner.documentId, session, {
        remoteConnectionEpoch: winner.remoteConnectionEpoch,
        receiveSequence: winner.sequence + 1,
        lineage: Identity.genesisLineage,
        message: winner.dependency,
        writerProvenance: provenanceFor(winner.dependency, Task.version, definition.hash)
      })
      const loser = prepared[loserIndex]!
      const retried = yield* sync.receive(Task, loser.documentId, session, {
        remoteConnectionEpoch: loser.remoteConnectionEpoch,
        receiveSequence: loser.sequence,
        lineage: Identity.genesisLineage,
        message: loser.dependent,
        writerProvenance: provenanceFor(loser.dependent, Task.version, definition.hash)
      })
      assert.isFalse(retried.duplicate)
      assert.deepStrictEqual(
        yield* sql<{ readonly pending: number }>`SELECT COUNT(*) AS pending
          FROM effect_local_peer_receipts WHERE pending_message IS NOT NULL`,
        [{ pending: 1 }]
      )
      for (const item of prepared) {
        InternalAutomerge.free(item.remote)
        InternalAutomerge.free(item.created.automerge)
      }
    }).pipe(Effect.provide(CapacityReceiptLayer)))

  it.effect("rejects pending receipts above the document quota across peers", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const prepare = (session: PeerSync.Session, actor: string, sequenceBase: number) =>
        Effect.gen(function*() {
          const remoteConnectionEpoch = `remote-${actor}`
          let remote = Automerge.clone(created.automerge, { actor: actor.repeat(32) })
          let remoteState = Automerge.initSyncState()
          let sequence = sequenceBase
          for (let round = 0; round < 4; round++) {
            const outbound = Automerge.generateSyncMessage(remote, remoteState)
            remoteState = outbound[0]
            if (outbound[1] === null) break
            const received = yield* sync.receive(Task, documentId, session, {
              remoteConnectionEpoch,
              receiveSequence: sequence++,
              lineage: Identity.genesisLineage,
              message: outbound[1],
              writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
            })
            if (received.reply !== null) {
              const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
              remote = applied[0]
              remoteState = applied[1]
            }
          }
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as { title: string }).title = `${actor}-first`
          })
          const dependency = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = dependency[0]
          assert.isNotNull(dependency[1])
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as unknown as { labels: Array<string> }).labels.push(`${actor}-second`)
          })
          const dependent = Automerge.generateSyncMessage(remote, remoteState)
          assert.isNotNull(dependent[1])
          return { dependent: dependent[1]!, remote, remoteConnectionEpoch, sequence }
        })
      const firstSession = yield* sync.open(yield* Identity.makePeerId)
      const secondSession = yield* sync.open(yield* Identity.makePeerId)
      const first = yield* prepare(firstSession, "e", 300)
      const second = yield* prepare(secondSession, "f", 400)

      yield* sync.receive(Task, documentId, firstSession, {
        remoteConnectionEpoch: first.remoteConnectionEpoch,
        receiveSequence: first.sequence,
        lineage: Identity.genesisLineage,
        message: first.dependent,
        writerProvenance: provenanceFor(first.dependent, Task.version, definition.hash)
      })
      const rejected = yield* Effect.exit(sync.receive(Task, documentId, secondSession, {
        remoteConnectionEpoch: second.remoteConnectionEpoch,
        receiveSequence: second.sequence,
        lineage: Identity.genesisLineage,
        message: second.dependent,
        writerProvenance: provenanceFor(second.dependent, Task.version, definition.hash)
      }))
      assert.strictEqual(rejected._tag, "Failure")
      if (rejected._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(rejected.cause))
        assert.strictEqual(error.reason._tag, "QuotaExceeded")
        if (error.reason._tag === "QuotaExceeded") {
          assert.strictEqual(error.reason.resource, "document sync receipts")
        }
      }
      InternalAutomerge.free(first.remote)
      InternalAutomerge.free(second.remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(CapacityReceiptLayer)))

  it.effect("rejects pending receipts above the replica quota while isolating incarnations", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const prepare = (session: PeerSync.Session, actor: string, sequenceBase: number) =>
        Effect.gen(function*() {
          const documentId = yield* Identity.makeDocumentId
          const remoteConnectionEpoch = `remote-${actor}`
          const created = yield* store.create(Task, documentId, { title: actor, labels: [] })
          let remote = Automerge.clone(created.automerge, { actor: actor.repeat(32) })
          let remoteState = Automerge.initSyncState()
          let sequence = sequenceBase
          for (let round = 0; round < 4; round++) {
            const outbound = Automerge.generateSyncMessage(remote, remoteState)
            remoteState = outbound[0]
            if (outbound[1] === null) break
            const received = yield* sync.receive(Task, documentId, session, {
              remoteConnectionEpoch,
              receiveSequence: sequence++,
              lineage: Identity.genesisLineage,
              message: outbound[1],
              writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
            })
            if (received.reply !== null) {
              const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
              remote = applied[0]
              remoteState = applied[1]
            }
          }
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as { title: string }).title = `${actor}-first`
          })
          const dependency = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = dependency[0]
          assert.isNotNull(dependency[1])
          remote = Automerge.change(remote, (draft) => {
            ;(draft.value as unknown as { labels: Array<string> }).labels.push(`${actor}-second`)
          })
          const dependent = Automerge.generateSyncMessage(remote, remoteState)
          assert.isNotNull(dependent[1])
          return { created, dependent: dependent[1]!, documentId, remote, remoteConnectionEpoch, sequence }
        })
      const firstSession = yield* sync.open(yield* Identity.makePeerId)
      const secondSession = yield* sync.open(yield* Identity.makePeerId)
      const first = yield* prepare(firstSession, "1", 500)
      const second = yield* prepare(secondSession, "2", 600)
      const otherIncarnationDocument = yield* Identity.makeDocumentId
      const otherIncarnationCreated = yield* store.create(
        Task,
        otherIncarnationDocument,
        { title: "retired", labels: [] }
      )
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads,
        accepted_heads, commit_sequence, accepted_at
      ) VALUES (
        ${firstSession.replicaIncarnation + 1}, ${firstSession.peerId}, 'retired', 0, ${otherIncarnationDocument},
        'other-incarnation', NULL, NULL, ${new Uint8Array([1])}, '[]', ${JSON.stringify(["f".repeat(64)])}, 0,
        ${new Date(0).toISOString()}
      )`

      yield* sync.receive(Task, first.documentId, firstSession, {
        remoteConnectionEpoch: first.remoteConnectionEpoch,
        receiveSequence: first.sequence,
        lineage: Identity.genesisLineage,
        message: first.dependent,
        writerProvenance: provenanceFor(first.dependent, Task.version, definition.hash)
      })
      const rejected = yield* Effect.exit(sync.receive(Task, second.documentId, secondSession, {
        remoteConnectionEpoch: second.remoteConnectionEpoch,
        receiveSequence: second.sequence,
        lineage: Identity.genesisLineage,
        message: second.dependent,
        writerProvenance: provenanceFor(second.dependent, Task.version, definition.hash)
      }))
      assert.strictEqual(rejected._tag, "Failure")
      if (rejected._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(rejected.cause))
        assert.strictEqual(error.reason._tag, "QuotaExceeded")
        if (error.reason._tag === "QuotaExceeded") {
          assert.strictEqual(error.reason.resource, "replica sync receipts")
        }
      }
      for (const item of [first, second]) {
        InternalAutomerge.free(item.remote)
        InternalAutomerge.free(item.created.automerge)
      }
      InternalAutomerge.free(otherIncarnationCreated.automerge)
    }).pipe(Effect.provide(CapacityReceiptLayer)))

  it.effect("bounds input before Automerge receive and resets connection state", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const exit = yield* Effect.exit(
        sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message: new Uint8Array(limits.maxSyncMessageBytes + 1),
          writerProvenance: []
        })
      )
      assert.strictEqual(exit._tag, "Failure")
      assert.strictEqual(
        (yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message: new Uint8Array([1, 2, 3]),
          writerProvenance: []
        })))._tag,
        "Failure"
      )
      const receipts = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`
      assert.strictEqual(receipts[0]?.count, 0)
      const first = yield* sync.generate(Task, documentId, session, { lineageAware: true })
      assert.isNotNull(first.outbound)
      const blocked = yield* sync.generate(Task, documentId, session, { lineageAware: true })
      assert.isTrue(blocked.dirty)
      assert.isNull(blocked.outbound)
      assert.isFalse(yield* sync.markSent(session, first.outbound!.sendSequence, "stale"))
      yield* sync.reset(session)
      assert.deepStrictEqual(yield* sync.pending(session), [])
      const retired = yield* sql<{ readonly outbox: number; readonly receipts: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
      assert.deepStrictEqual(retired, [{ outbox: 0, receipts: 0 }])
      const reconnected = yield* sync.open(session.peerId)
      const restarted = yield* sync.generate(Task, documentId, reconnected, { lineageAware: true })
      assert.isNotNull(restarted.outbound)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("preserves valid durable outbox and receipts when the sync service restarts", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.init<InternalAutomerge.Root<{ title: string; labels: Array<string> }>>()
      const handshake = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "remote-restart",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: handshake,
        writerProvenance: []
      })
      assert.isNotNull(received.reply)
      const generated = yield* sync.enqueue(session, received.reply!)
      assert.isAbove(generated.writerProvenance.length, 0)
      assert.deepStrictEqual(
        generated.writerProvenance,
        provenanceFor(generated.message, Task.version, definition.hash)
      )
      yield* Effect.scoped(
        Effect.gen(function*() {
          const restarted = yield* PeerSync.PeerSync
          const pending = yield* restarted.pending(session)
          assert.strictEqual(pending.length, 1)
          assert.strictEqual(pending[0]?.messageHash, generated.messageHash)
          assert.deepStrictEqual(pending[0]?.writerProvenance, generated.writerProvenance)
          const rows = yield* sql<{ readonly outbox: number; readonly receipts: number }>`SELECT
            (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
            (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
          assert.deepStrictEqual(rows, [{ outbox: 1, receipts: 1 }])
        }).pipe(Effect.provide(PeerSync.layer))
      )
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("serializes one document across sessions without blocking independent documents", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const firstDocumentId = yield* Identity.makeDocumentId
      const secondDocumentId = yield* Identity.makeDocumentId
      const firstCreated = yield* store.create(Task, firstDocumentId, { title: "one", labels: [] })
      const secondCreated = yield* store.create(Task, secondDocumentId, { title: "two", labels: [] })
      const firstRemote = Automerge.change(
        Automerge.clone(firstCreated.automerge, { actor: "a".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "first remote"
        }
      )
      const secondRemote = Automerge.change(
        Automerge.clone(secondCreated.automerge, { actor: "b".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "second remote"
        }
      )
      const firstMessage = Automerge.generateSyncMessage(firstRemote, Automerge.initSyncState())[1]!
      const secondMessage = Automerge.generateSyncMessage(secondRemote, Automerge.initSyncState())[1]!
      const firstStarted = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const secondStarted = yield* Deferred.make<void>()
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            documentId === firstDocumentId
              ? Deferred.succeed(firstStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirst)),
                Effect.andThen(store.load(document, documentId))
              )
              : Deferred.succeed(secondStarted, undefined).pipe(
                Effect.andThen(store.load(document, documentId))
              )
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const firstSession = yield* sync.open(yield* Identity.makePeerId)
        const secondSession = yield* sync.open(yield* Identity.makePeerId)
        const firstInput = {
          remoteConnectionEpoch: "first remote",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message: firstMessage,
          writerProvenance: provenanceFor(firstMessage)
        }
        const first = yield* sync.receive(Task, firstDocumentId, firstSession, firstInput).pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)
        const same = yield* sync.receive(Task, firstDocumentId, secondSession, firstInput).pipe(Effect.forkChild)
        const independent = yield* sync.receive(Task, secondDocumentId, secondSession, {
          remoteConnectionEpoch: "second remote",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message: secondMessage,
          writerProvenance: provenanceFor(secondMessage, Task.version, definition.hash)
        }).pipe(Effect.forkChild)
        yield* Deferred.await(secondStarted)
        assert.isUndefined(same.pollUnsafe())
        assert.isFalse((yield* Fiber.join(independent)).duplicate)
        yield* Deferred.succeed(releaseFirst, undefined)
        assert.isFalse((yield* Fiber.join(first)).duplicate)
        const serialized = yield* Fiber.join(same)
        assert.isFalse(serialized.duplicate)
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, blockingStore))
        )),
        Effect.ensuring(Deferred.succeed(releaseFirst, undefined))
      )
      InternalAutomerge.free(firstCreated.automerge)
      InternalAutomerge.free(secondCreated.automerge)
      InternalAutomerge.free(firstRemote)
      InternalAutomerge.free(secondRemote)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects in-flight work across reset without retiring the session", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "c".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "remote"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(store.load(document, documentId))
            )
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const input = {
          remoteConnectionEpoch: "remote",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message)
        }
        const inFlight = yield* Effect.exit(sync.receive(Task, documentId, session, input)).pipe(Effect.forkChild)
        yield* Deferred.await(started)
        yield* sync.reset(session)
        yield* Deferred.succeed(release, undefined)
        const interrupted = yield* Fiber.join(inFlight)
        assert.strictEqual(interrupted._tag, "Failure")
        if (interrupted._tag === "Failure") {
          assert.strictEqual(
            Option.getOrThrow(Cause.findErrorOption(interrupted.cause)).reason._tag,
            "ProtocolMismatch"
          )
        }
        assert.deepStrictEqual(
          yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`,
          [{ count: 0 }]
        )
        assert.isFalse((yield* sync.receive(Task, documentId, session, input)).duplicate)
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, blockingStore))
        )),
        Effect.ensuring(Deferred.succeed(release, undefined))
      )
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(remote)
    }).pipe(Effect.provide(Services)))

  it.effect("does not deadlock reset behind a claim while generation holds a read permit", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const gate = yield* ReplicaGate.ReplicaGate
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const generateStarted = yield* Deferred.make<void>()
      const releaseGenerate = yield* Deferred.make<void>()
      const claimAcquired = yield* Deferred.make<void>()
      const releaseClaim = yield* Deferred.make<void>()
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            Deferred.succeed(generateStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseGenerate)),
              Effect.andThen(store.load(document, documentId))
            )
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const generating = yield* sync.generate(Task, documentId, session, { lineageAware: true }).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(generateStarted)
        const claiming = yield* gate.claim(() =>
          Deferred.succeed(claimAcquired, undefined).pipe(
            Effect.andThen(Deferred.await(releaseClaim))
          )
        ).pipe(Effect.forkChild({ startImmediately: true }))
        const resetting = yield* sync.reset(session).pipe(Effect.forkChild({ startImmediately: true }))
        const acquired = yield* Deferred.await(claimAcquired).pipe(
          Effect.timeout("1 second"),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.succeed(releaseGenerate, undefined)
        yield* Effect.yieldNow
        yield* TestClock.adjust("1 second")
        yield* Fiber.join(acquired)
        yield* Deferred.succeed(releaseClaim, undefined)
        yield* Fiber.join(claiming)
        yield* Fiber.join(generating)
        yield* Fiber.join(resetting)
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, blockingStore))
        )),
        Effect.ensuring(
          Deferred.succeed(releaseGenerate, undefined).pipe(
            Effect.andThen(Deferred.succeed(releaseClaim, undefined)),
            Effect.asVoid
          )
        )
      )
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects operation-heavy messages without persisting the rejected transition", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.change(Automerge.clone(created.automerge, { actor: "4".repeat(32) }), (draft) => {
        const value = draft.value as unknown as { title: string; labels: Array<string> }
        value.title = "complex"
        value.labels.push("one", "two")
      })
      let remoteState = Automerge.initSyncState()
      let rejected = false
      for (let sequence = 0; sequence < 4; sequence++) {
        const generated = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = generated[0]
        if (generated[1] === null) break
        const before = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
          (SELECT COUNT(*) FROM effect_local_changes) AS changes,
          (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
        const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence,
          lineage: Identity.genesisLineage,
          message: generated[1],
          writerProvenance: provenanceFor(generated[1], Task.version, definition.hash)
        }))
        if (exit._tag === "Failure") {
          const after = yield* sql<{ readonly changes: number; readonly receipts: number }>`SELECT
            (SELECT COUNT(*) FROM effect_local_changes) AS changes,
            (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
          assert.deepStrictEqual(after, before)
          rejected = true
          break
        }
        if (exit.value.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, exit.value.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      assert.isTrue(rejected)
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "one", labels: [] })
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(StrictLayer)))

  it.effect("fences sessions opened before the replica identity changes", () =>
    Effect.gen(function*() {
      const sync = yield* PeerSync.PeerSync
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const session = yield* sync.open(yield* Identity.makePeerId)
      yield* sql`UPDATE effect_local_metadata SET
        replica_incarnation = replica_incarnation + 1,
        writer_generation = writer_generation + 1
        WHERE singleton = 1`
      const current = yield* gate.refresh
      assert.notStrictEqual(current.incarnation, session.replicaIncarnation)
      const exit = yield* Effect.exit(sync.pending(session))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exit.cause)).reason._tag, "ProtocolMismatch")
      }
      const fresh = yield* sync.open(session.peerId)
      assert.strictEqual(fresh.replicaIncarnation, current.incarnation)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("quarantines expired pending sync data without deleting applied changes", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(Automerge.clone(created.automerge, { actor: "5".repeat(32) }), (draft) => {
        ;(draft.value as { title: string }).title = "fresh"
      })
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const messageHash = yield* Canonical.digest(message)
      const appliedBefore = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_changes WHERE document_id = ${documentId} AND applied = 1`
      yield* sql`INSERT INTO effect_local_changes (
        change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
        actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
      ) VALUES (
        'expired-change', ${documentId}, ${Task.name}, ${Task.version}, ${definition.hash},
        ${"f".repeat(32)}, 100, '[]', ${new Uint8Array([9])}, 0, ${session.peerId},
        ${new Date(0).toISOString()}, 0
      )`
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads,
        accepted_heads, commit_sequence, accepted_at
      ) VALUES (
        ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, 0, ${documentId},
        ${messageHash}, NULL, NULL, ${message}, '[]', '[]', 0,
        ${new Date(0).toISOString()}
      )`
      yield* TestClock.setTime(limits.maxPendingAgeMillis + 1)
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      })
      assert.isFalse(received.duplicate)
      const remaining = yield* sql<{
        readonly applied: number
        readonly expiredChanges: number
        readonly expiredReceipts: number
        readonly quarantined: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes WHERE document_id = ${documentId} AND applied = 1) AS applied,
        (SELECT COUNT(*) FROM effect_local_changes WHERE change_hash = 'expired-change') AS expiredChanges,
        (SELECT COUNT(*) FROM effect_local_peer_receipts
          WHERE receive_sequence = 0 AND accepted_at = ${new Date(0).toISOString()}) AS expiredReceipts,
        (SELECT COUNT(*) FROM effect_local_quarantine WHERE reason LIKE 'Expired pending sync%') AS quarantined`
      assert.isAtLeast(remaining[0]!.applied, appliedBefore[0]!.count)
      assert.strictEqual(remaining[0]!.expiredChanges, 0)
      assert.strictEqual(remaining[0]!.expiredReceipts, 0)
      assert.strictEqual(remaining[0]!.quarantined, 2)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("materializes reordered dependencies and reconnects with a fresh sync state", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const session = yield* sync.open(peerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "3".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      for (let round = 0; round < 4; round++) {
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        if (outbound[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1],
          writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "two"
      })
      const dependencyHash = Automerge.decodeChange(
        Automerge.getChangesSince(remote, [...created.materializedHeads]).at(-1)!
      ).hash
      const first = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = first[0]
      assert.isNotNull(first[1])
      const dependencyProvenance = provenanceFor(first[1]!, Task.version, definition.hash).map((entry) =>
        entry.changeHash === dependencyHash
          ? Object.assign({}, entry, {
            writerSchemaVersion: 7,
            writerDefinitionHash: "dependency-definition"
          })
          : entry
      )
      const dependencyHeads = Automerge.getHeads(remote)
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as unknown as { labels: Array<string> }).labels.push("after")
      })
      const dependentHash = Automerge.decodeChange(
        Automerge.getChangesSince(remote, dependencyHeads).at(-1)!
      ).hash
      const second = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(second[1])
      const dependentProvenance = provenanceFor(second[1]!, Task.version, definition.hash).map((entry) => {
        if (entry.changeHash === dependencyHash) {
          return Object.assign({}, entry, {
            writerSchemaVersion: 7,
            writerDefinitionHash: "dependency-definition"
          })
        }
        if (entry.changeHash === dependentHash) {
          return Object.assign({}, entry, {
            writerSchemaVersion: 8,
            writerDefinitionHash: "dependent-definition"
          })
        }
        return entry
      })
      const pending = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: dependentProvenance
      })
      assert.isFalse(sameHeadsForTest(pending.heads, Automerge.getHeads(remote)))
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: first[1]!,
        writerProvenance: dependencyProvenance
      })
      const converged = yield* store.load(Task, documentId)
      assert.deepStrictEqual(converged.snapshot.value, { title: "two", labels: ["after"] })
      assert.strictEqual(converged.snapshot.projection, "Ready")
      const provenance = yield* sql<{
        readonly change_hash: string
        readonly writer_definition_hash: string
        readonly writer_schema_version: number
      }>`SELECT change_hash, writer_definition_hash, writer_schema_version
        FROM effect_local_changes WHERE document_id = ${documentId}`
      const byHash = new Map(provenance.map((entry) => [entry.change_hash, entry]))
      for (const entry of [...dependencyProvenance, ...dependentProvenance]) {
        assert.deepInclude(byHash.get(entry.changeHash), {
          writer_definition_hash: entry.writerDefinitionHash,
          writer_schema_version: entry.writerSchemaVersion
        })
      }
      yield* sync.reset(session)
      const reconnected = yield* sync.open(peerId)
      assert.notStrictEqual(reconnected.connectionEpoch, session.connectionEpoch)
      const restarted = yield* sync.generate(Task, documentId, reconnected, { lineageAware: true })
      assert.isNotNull(restarted.outbound)
      InternalAutomerge.free(converged.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("counts decoded dependency edges and operations independently from message bytes", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "6".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let sequence = 0
      for (let round = 0; round < 4; round++) {
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        if (outbound[1] === null) break
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1],
          writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
        })
        if (received.reply !== null) {
          const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
          remote = applied[0]
          remoteState = applied[1]
        }
      }
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "first"
      })
      const first = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = first[0]
      assert.isNotNull(first[1])
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as { title: string }).title = "x".repeat(2048)
      })
      const second = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(second[1])
      const decoded = Automerge.decodeSyncMessage(second[1]!)
      assert.isAbove(second[1]!.byteLength, 100)
      assert.isAtMost(
        decoded.changes.reduce((total, bytes) => total + Automerge.decodeChange(bytes).deps.length, 0),
        100
      )
      assert.isAtMost(
        decoded.changes.reduce((total, bytes) => total + Automerge.decodeChange(bytes).ops.length, 0),
        10_000
      )
      const pending = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: second[1]!,
        writerProvenance: provenanceFor(second[1]!, Task.version, definition.hash)
      })
      assert.isFalse(sameHeadsForTest(pending.heads, Automerge.getHeads(remote)))
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        lineage: Identity.genesisLineage,
        message: first[1]!,
        writerProvenance: provenanceFor(first[1]!, Task.version, definition.hash)
      })
      const converged = yield* store.load(Task, documentId)
      assert.strictEqual(converged.snapshot.value.title, "x".repeat(2048))
      InternalAutomerge.free(converged.automerge)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(EdgeLayer)))

  it.effect("reports startup storage failures through the typed error channel", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-peer-sync-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
      const database = Layer.merge(SqliteClient.layer({ filename, disableWAL: true }), NodeCrypto.layer)
      yield* Effect.scoped(
        Effect.gen(function*() {
          yield* ReplicaBootstrap.make(definition)
          const sql = yield* SqlClient.SqlClient
          yield* sql`DROP TABLE effect_local_peer_outbox`
        }).pipe(Effect.provide(database))
      )
      const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
      const base = Layer.mergeAll(database, bootstrap, CheckpointAuthority.layerRejectAll)
      const gate = ReplicaGate.layer.pipe(Layer.provide(Limits), Layer.provide(base))
      const infrastructure = Layer.mergeAll(base, gate, Limits)
      const store = DocumentStore.layer.pipe(Layer.provide(infrastructure))
      const projections = ProjectionStore.layer([]).pipe(Layer.provide(database))
      const services = Layer.mergeAll(infrastructure, store, projections)
      const sync = PeerSync.layer.pipe(Layer.provide(services))
      const result = yield* Effect.result(Effect.scoped(
        Effect.gen(function*() {
          yield* PeerSync.PeerSync
        }).pipe(Effect.provide(sync))
      ))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "StorageUnavailable")
      }
    }))

  it.effect("retries peer recovery without dropping a concurrent local or remote commit", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      let remote = Automerge.clone(created.automerge, { actor: "d".repeat(32) })
      let remoteState = Automerge.initSyncState()
      let receiveSequence = 0
      const loaded = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let blockNextLoad = false
      let blocked = false
      let recoveryLoads = 0
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            store.load(document, documentId).pipe(
              Effect.tap(() =>
                Effect.suspend(() => {
                  if (!blockNextLoad) return Effect.void
                  recoveryLoads++
                  if (blocked) return Effect.void
                  blocked = true
                  return Deferred.succeed(loaded, undefined).pipe(
                    Effect.andThen(Deferred.await(release))
                  )
                })
              )
            )
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        for (let round = 0; round < 4; round++) {
          const outbound = Automerge.generateSyncMessage(remote, remoteState)
          remoteState = outbound[0]
          if (outbound[1] === null) break
          const received = yield* sync.receive(Task, documentId, session, {
            remoteConnectionEpoch: "remote",
            receiveSequence: receiveSequence++,
            lineage: Identity.genesisLineage,
            message: outbound[1],
            writerProvenance: provenanceFor(outbound[1], Task.version, definition.hash)
          })
          if (received.reply !== null) {
            const applied = Automerge.receiveSyncMessage(remote, remoteState, received.reply.message)
            remote = applied[0]
            remoteState = applied[1]
          }
        }
        remote = Automerge.change(remote, (draft) => {
          ;(draft.value as unknown as { labels: Array<string> }).labels.push("remote")
        })
        const outbound = Automerge.generateSyncMessage(remote, remoteState)
        remoteState = outbound[0]
        assert.isNotNull(outbound[1])
        assert.isAbove(Automerge.decodeSyncMessage(outbound[1]!).changes.length, 0)
        blockNextLoad = true
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: "remote",
          receiveSequence: receiveSequence++,
          lineage: Identity.genesisLineage,
          message: outbound[1]!,
          writerProvenance: provenanceFor(outbound[1]!, Task.version, definition.hash)
        }).pipe(Effect.forkChild)
        yield* Deferred.await(loaded)
        const staged = yield* store.stage(created, (draft) => {
          draft.labels.push("local")
        })
        const persisted = yield* store.persist(Task, documentId, created, staged)
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(received)
        const reloaded = yield* store.load(Task, documentId)
        assert.strictEqual(recoveryLoads, 2)
        assert.strictEqual(reloaded.snapshot.value.title, "one")
        assert.deepStrictEqual(reloaded.snapshot.value.labels.toSorted(), ["local", "remote"])
        InternalAutomerge.free(reloaded.automerge)
        InternalAutomerge.free(persisted.automerge)
        InternalAutomerge.free(staged)
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, blockingStore))
        )),
        Effect.ensuring(Deferred.succeed(release, undefined))
      )
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(remote)
    }).pipe(Effect.provide(Services)))

  it.effect("fails bounded peer recovery conflicts without partial durable peer state", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "e".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "remote"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      let conflicts = 0
      const conflictingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            store.load(document, documentId).pipe(
              Effect.tap((durable) =>
                Effect.gen(function*() {
                  conflicts++
                  const staged = yield* store.stage(durable, (draft) => {
                    ;(draft as unknown as { labels: Array<string> }).labels.push(`conflict-${conflicts}`)
                  })
                  const persisted = yield* store.persist(document, documentId, durable, staged).pipe(
                    Effect.ensuring(Effect.sync(() => InternalAutomerge.free(staged)))
                  )
                  InternalAutomerge.free(persisted.automerge)
                })
              )
            )
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const result = yield* Effect.result(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: "remote",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        }))
        assert.strictEqual(conflicts, 9)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure._tag, "ReplicaError")
          assert.strictEqual(result.failure.reason._tag, "StorageUnavailable")
          if (result.failure.reason._tag === "StorageUnavailable") {
            assert.isTrue(Schema.is(Schema.Error())(result.failure.reason.cause))
            if (!Schema.is(Schema.Error())(result.failure.reason.cause)) return
            assert.strictEqual(result.failure.reason.cause.message, "Document remained busy while applying peer sync")
          }
        }
        const rows = yield* sql<{
          readonly outbox: number
          readonly peerChanges: number
          readonly receipts: number
        }>`SELECT
          (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
          (SELECT COUNT(*) FROM effect_local_changes WHERE peer_id = ${session.peerId}) AS peerChanges,
          (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
        assert.deepStrictEqual(rows, [{ outbox: 0, peerChanges: 0, receipts: 0 }])
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, conflictingStore))
        ))
      )
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(remote)
    }).pipe(Effect.provide(Services)))

  it.effect("fences a peer recovery retry after the session resets", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "f".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "remote"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const firstAttemptClosed = yield* Deferred.make<void>()
      const resetComplete = yield* Deferred.make<void>()
      let recoveryLoads = 0
      const conflictingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            store.load(document, documentId).pipe(
              Effect.tap((durable) =>
                Effect.suspend(() => {
                  recoveryLoads++
                  if (recoveryLoads !== 1) return Effect.void
                  return Effect.gen(function*() {
                    const staged = yield* store.stage(durable, (draft) => {
                      ;(draft as unknown as { labels: Array<string> }).labels.push("conflict")
                    })
                    const persisted = yield* store.persist(document, documentId, durable, staged).pipe(
                      Effect.ensuring(Effect.sync(() => InternalAutomerge.free(staged)))
                    )
                    InternalAutomerge.free(persisted.automerge)
                    yield* Effect.addFinalizer(() =>
                      Deferred.succeed(firstAttemptClosed, undefined).pipe(
                        Effect.andThen(Deferred.await(resetComplete))
                      )
                    )
                  })
                })
              )
            ) as never
          return load
        }
      })
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const received = yield* Effect.result(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: "remote",
          receiveSequence: 0,
          lineage: Identity.genesisLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        })).pipe(Effect.forkChild)
        yield* Deferred.await(firstAttemptClosed)
        yield* sync.reset(session)
        yield* Deferred.succeed(resetComplete, undefined)
        const result = yield* Fiber.join(received)
        assert.strictEqual(recoveryLoads, 1)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure._tag, "ReplicaError")
          assert.strictEqual(result.failure.reason._tag, "ProtocolMismatch")
        }
        const rows = yield* sql<{
          readonly outbox: number
          readonly peerChanges: number
          readonly receipts: number
        }>`SELECT
          (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
          (SELECT COUNT(*) FROM effect_local_changes WHERE peer_id = ${session.peerId}) AS peerChanges,
          (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
        assert.deepStrictEqual(rows, [{ outbox: 0, peerChanges: 0, receipts: 0 }])
      }).pipe(
        Effect.provide(PeerSync.layer.pipe(
          Layer.provide(Layer.succeed(DocumentStore.DocumentStore, conflictingStore))
        )),
        Effect.ensuring(Deferred.succeed(resetComplete, undefined))
      )
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(remote)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects mismatched durable pending change metadata before replay", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const corrupted = Automerge.change(
        Automerge.clone(created.automerge, { actor: "d".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "corrupted"
        }
      )

      yield* Effect.gen(function*() {
        const changeBytes = Automerge.getChangesSince(
          corrupted,
          [...created.materializedHeads]
        )
        assert.strictEqual(changeBytes.length, 1)
        const decoded = Automerge.decodeChange(changeBytes[0]!)
        const storedHash = "0".repeat(64)
        assert.notStrictEqual(storedHash, decoded.hash)

        yield* sql`INSERT INTO effect_local_changes (
          change_hash, document_id, document_type, writer_schema_version,
          writer_definition_hash, actor, sequence, dependencies, bytes,
          applied, peer_id, accepted_at, commit_sequence
        ) VALUES (
          ${storedHash}, ${documentId}, ${Task.name}, ${Task.version},
          ${definition.hash}, ${decoded.actor}, ${decoded.seq},
          ${JSON.stringify(decoded.deps)}, ${changeBytes[0]!},
          0, ${session.peerId}, '9999-12-31T23:59:59.999Z',
          ${created.commitSequence}
        )`

        const before = yield* sql<{
          readonly materialized_heads: string
          readonly receipts: number
        }>`SELECT
          materialized_heads,
          (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts
          FROM effect_local_documents
          WHERE document_id = ${documentId}`

        const generated = Automerge.generateSyncMessage(
          created.automerge,
          Automerge.initSyncState()
        )
        assert.isNotNull(generated[1])

        const result = yield* Effect.result(
          sync.receive(Task, documentId, session, {
            remoteConnectionEpoch: "remote",
            receiveSequence: 0,
            lineage: Identity.genesisLineage,
            message: generated[1]!,
            writerProvenance: provenanceFor(generated[1]!, Task.version, definition.hash)
          })
        )

        const after = yield* sql<{
          readonly materialized_heads: string
          readonly receipts: number
        }>`SELECT
          materialized_heads,
          (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts
          FROM effect_local_documents
          WHERE document_id = ${documentId}`

        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
        }
        assert.deepStrictEqual(after, before)
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            InternalAutomerge.free(corrupted)
            InternalAutomerge.free(created.automerge)
          })
        )
      )
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rechecks the document outbox after a concurrent enqueue", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, {
        title: "one",
        labels: []
      })
      const loadStarted = yield* Deferred.make<void>()
      const releaseLoad = yield* Deferred.make<void>()
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, documentId) =>
            Deferred.succeed(loadStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseLoad)),
              Effect.andThen(store.load(document, documentId))
            )
          return load
        }
      })

      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const syncMessage = Automerge.generateSyncMessage(
          created.automerge,
          Automerge.initSyncState()
        )[1]
        assert.isNotNull(syncMessage)
        const messageHash = yield* Canonical.digest(syncMessage!)

        const generating = yield* sync.generate(
          Task,
          documentId,
          session,
          { lineageAware: true }
        ).pipe(Effect.forkChild)

        yield* Deferred.await(loadStarted)

        const enqueued = yield* sync.enqueue(session, {
          documentId,
          message: syncMessage!,
          messageHash,
          heads: created.materializedHeads
        })

        yield* Deferred.succeed(releaseLoad, undefined)
        const generated = yield* Fiber.join(generating)
        const pending = yield* sync.pending(session)

        assert.isNull(generated.outbound)
        assert.isTrue(generated.dirty)
        assert.deepStrictEqual(
          pending.map((outbound) => outbound.sendSequence),
          [enqueued.sendSequence]
        )
      }).pipe(
        Effect.provide(
          PeerSync.layer.pipe(
            Layer.provide(
              Layer.succeed(DocumentStore.DocumentStore, blockingStore)
            )
          )
        ),
        Effect.ensuring(Deferred.succeed(releaseLoad, undefined)),
        Effect.ensuring(
          Effect.sync(() => InternalAutomerge.free(created.automerge))
        )
      )
    }).pipe(Effect.provide(Services)))

  const supersededLineage = Identity.DocumentLineage.make("lin_11111111-1111-4111-8111-111111111111")

  // Moves only the durable lineage marker. `Compaction.rewriteHistory` is the production writer of
  // this column, but it refuses to run while the document holds a peer receipt or a pending outbox
  // row and deletes both when it does run -- and those are precisely the rows these tests need to
  // survive the lineage change. Setting the marker alone leaves every path under test, `receive`,
  // `generate`, `pending` and the whole `PeerSync` layer, the real production composition.
  const supersedeLineage = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_documents SET lineage = ${supersededLineage}
        WHERE document_id = ${documentId}`
    })

  const durableSnapshot = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    return yield* sql<{
      readonly changes: number
      readonly checkpoints: number
      readonly commit_outbox: number
      readonly commit_sequence: number
      readonly documents: string
      readonly outbox: number
      readonly quarantine: number
      readonly receipts: number
      readonly relay_receipt_usage: number
      readonly transitions: number
    }>`SELECT
      (SELECT COUNT(*) FROM effect_local_changes) AS changes,
      (SELECT COUNT(*) FROM effect_local_checkpoints) AS checkpoints,
      (SELECT COUNT(*) FROM effect_local_commit_outbox) AS commit_outbox,
      (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
      (SELECT group_concat(materialized_heads || '|' || accepted_heads || '|' || lineage)
        FROM effect_local_documents) AS documents,
      (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
      (SELECT COUNT(*) FROM effect_local_quarantine) AS quarantine,
      (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
      (SELECT COUNT(*) FROM effect_local_peer_relay_receipt_usage) AS relay_receipt_usage,
      (SELECT COUNT(*) FROM effect_local_lineage_transitions) AS transitions`
  })

  const lineageFailure = (exit: Exit.Exit<unknown, ReplicaError.ReplicaError>) => {
    assert.strictEqual(exit._tag, "Failure")
    if (exit._tag !== "Failure") throw new TypeError("Expected a failure")
    return Option.getOrThrow(Cause.findErrorOption(exit.cause)).reason
  }

  it.effect("refuses a sync message from a peer on a superseded lineage without durable mutation", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "kept", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "5".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "resurrected"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      yield* supersedeLineage(documentId)
      const before = yield* durableSnapshot

      const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      }))

      const reason = lineageFailure(exit)
      assert.strictEqual(reason._tag, "DocumentLineageChanged")
      if (reason._tag === "DocumentLineageChanged") {
        assert.strictEqual(reason.documentId, documentId)
        assert.strictEqual(reason.localLineage, supersededLineage)
        assert.strictEqual(reason.remoteLineage, Identity.genesisLineage)
      }
      assert.deepStrictEqual(yield* durableSnapshot, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("refuses a retransmitted sync message whose cached receipt predates the rewrite", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "6".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "two"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const input = {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message,
        writerProvenance: provenanceFor(message, Task.version, definition.hash)
      }
      // The receipt is written under the genesis lineage, and a retransmission is normally answered
      // from it without the message being looked at again.
      const accepted = yield* sync.receive(Task, documentId, session, input)
      assert.isFalse(accepted.duplicate)
      yield* supersedeLineage(documentId)
      const before = yield* durableSnapshot

      const exit = yield* Effect.exit(sync.receive(Task, documentId, session, input))

      const reason = lineageFailure(exit)
      assert.strictEqual(reason._tag, "DocumentLineageChanged")
      assert.deepStrictEqual(yield* durableSnapshot, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("rejects hostile lineage values at the direct receive boundary", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "kept", labels: [] })
      const remote = Automerge.change(
        Automerge.clone(created.automerge, { actor: "8".repeat(32) }),
        (draft) => {
          ;(draft.value as { title: string }).title = "hostile"
        }
      )
      const message = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      yield* supersedeLineage(documentId)
      const before = yield* durableSnapshot

      for (
        const lineage of [
          "",
          "x".repeat(257),
          "界".repeat(256),
          "\0".repeat(256),
          supersededLineage.toUpperCase(),
          `${supersededLineage} `,
          `lin_${"0".repeat(36)}`
        ]
      ) {
        const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          lineage: lineage as Identity.DocumentLineage,
          message,
          writerProvenance: provenanceFor(message, Task.version, definition.hash)
        }))
        const reason = lineageFailure(exit)
        assert.isTrue(
          reason._tag === "DocumentLineageChanged" || reason._tag === "ProtocolMismatch",
          `unexpected reason ${reason._tag} for ${JSON.stringify(lineage)}`
        )
      }

      assert.deepStrictEqual(yield* durableSnapshot, before)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("refuses to generate a rewritten document toward a peer that is not lineage aware", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "rewritten", labels: [] })
      yield* supersedeLineage(documentId)
      const before = yield* durableSnapshot

      const exit = yield* Effect.exit(
        sync.generate(Task, documentId, session, { lineageAware: false })
      )

      const reason = lineageFailure(exit)
      assert.strictEqual(reason._tag, "DocumentLineageChanged")
      if (reason._tag === "DocumentLineageChanged") {
        assert.strictEqual(reason.documentId, documentId)
        assert.strictEqual(reason.localLineage, supersededLineage)
      }
      assert.deepStrictEqual(yield* durableSnapshot, before)

      const generated = yield* sync.generate(Task, documentId, session, { lineageAware: true })
      assert.isNotNull(generated.outbound)
      assert.strictEqual(generated.outbound?.lineage, supersededLineage)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("keeps the lineage a queued outbox row was generated under", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "queued", labels: [] })
      const generated = yield* sync.generate(Task, documentId, session, { lineageAware: true })
      assert.isNotNull(generated.outbound)
      assert.strictEqual(generated.outbound?.lineage, Identity.genesisLineage)

      yield* supersedeLineage(documentId)

      const pending = yield* sync.pending(session)
      assert.strictEqual(pending.length, 1)
      assert.strictEqual(pending[0]?.lineage, Identity.genesisLineage)
      const rows = yield* sql<{ readonly lineage: string }>`SELECT lineage FROM effect_local_peer_outbox
        WHERE document_id = ${documentId}`
      assert.deepStrictEqual(rows.map((row) => row.lineage), [Identity.genesisLineage])
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("drops only the invalidated document's sync state", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const rewritten = yield* Identity.makeDocumentId
      const untouched = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const first = yield* store.create(Task, rewritten, { title: "first", labels: [] })
      const second = yield* store.create(Task, untouched, { title: "second", labels: [] })
      const initial = yield* sync.generate(Task, rewritten, session, { lineageAware: true })
      assert.isNotNull(initial.outbound)
      yield* sync.markSent(session, initial.outbound!.sendSequence, initial.outbound!.messageHash)
      yield* sync.generate(Task, untouched, session, { lineageAware: true }).pipe(
        Effect.flatMap((result) => sync.markSent(session, result.outbound!.sendSequence, result.outbound!.messageHash))
      )
      // With the sync state kept, the next generate has nothing left to say about either document.
      assert.isNull((yield* sync.generate(Task, rewritten, session, { lineageAware: true })).outbound)
      assert.isNull((yield* sync.generate(Task, untouched, session, { lineageAware: true })).outbound)

      yield* sync.invalidateDocument(rewritten)

      const regenerated = yield* sync.generate(Task, rewritten, session, { lineageAware: true })
      assert.isNotNull(regenerated.outbound)
      assert.isNull((yield* sync.generate(Task, untouched, session, { lineageAware: true })).outbound)
      InternalAutomerge.free(first.automerge)
      InternalAutomerge.free(second.automerge)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("acquires the document sync lock before a rewrite can commit", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const loadStarted = yield* Deferred.make<void>()
      const releaseLoad = yield* Deferred.make<void>()
      const rewriteCommitted = yield* Deferred.make<void>()
      const blockingStore = new Proxy(store, {
        get(target, property, receiver) {
          if (property !== "load") return Reflect.get(target, property, receiver)
          const load: typeof store.load = (document, requestedId) =>
            store.load(document, requestedId).pipe(
              Effect.tap(() =>
                requestedId === documentId
                  ? Deferred.succeed(loadStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseLoad))
                  )
                  : Effect.void
              )
            )
          return load
        }
      })

      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const created = yield* blockingStore.create(Task, documentId, { title: "kept", labels: [] })
        InternalAutomerge.free(created.automerge)

        const generating = yield* sync.generate(
          Task,
          documentId,
          session,
          { lineageAware: true }
        ).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(loadStarted)

        // `rewriteCommitted` stands at the destructive SQL commit boundary. PeerSync must acquire
        // the same document lock as generate and receive before that boundary can be crossed.
        const rewriting = yield* sync.withDocumentInvalidation(
          documentId,
          Deferred.succeed(rewriteCommitted, undefined)
        ).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        for (let index = 0; index < 10; index++) yield* Effect.yieldNow

        assert.isFalse(
          yield* Deferred.isDone(rewriteCommitted),
          "rewrite committed before acquiring the peer document lock"
        )

        yield* Deferred.succeed(releaseLoad, undefined)
        yield* Fiber.join(generating)
        yield* Fiber.join(rewriting)
      }).pipe(
        Effect.provide(
          PeerSync.layer.pipe(
            Layer.provide(Layer.succeed(DocumentStore.DocumentStore, blockingStore))
          )
        ),
        Effect.ensuring(Deferred.succeed(releaseLoad, undefined))
      )
    }).pipe(Effect.provide(Services)))

  it.effect("clears document sync state when interruption arrives during commit", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const session = yield* sync.open(yield* Identity.makePeerId)
      const created = yield* store.create(Task, documentId, { title: "kept", labels: [] })
      const initial = yield* sync.generate(Task, documentId, session, { lineageAware: true })
      assert.isNotNull(initial.outbound)
      yield* sync.markSent(session, initial.outbound!.sendSequence, initial.outbound!.messageHash)
      assert.isNull((yield* sync.generate(Task, documentId, session, { lineageAware: true })).outbound)

      const commitStarted = yield* Deferred.make<void>()
      const releaseCommit = yield* Deferred.make<void>()
      const transaction = Effect.uninterruptibleMask((restore) =>
        restore(Effect.void).pipe(
          Effect.exit,
          Effect.flatMap((exit) =>
            Deferred.succeed(commitStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCommit)),
              Effect.flatMap(() => exit)
            )
          )
        )
      )
      const maintenance = yield* sync.withDocumentInvalidation(documentId, transaction).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(commitStarted)
      const interrupting = yield* Fiber.interrupt(maintenance).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseCommit, undefined)
      yield* Fiber.join(interrupting)

      assert.isNotNull((yield* sync.generate(Task, documentId, session, { lineageAware: true })).outbound)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(TestLayer)))

  const checkpointToken = new Uint8Array([17, 29, 43])
  const checkpointAuthority: CheckpointAuthority.Implementation = {
    signManifest: () => Effect.succeed(Option.some(checkpointToken)),
    verifyManifest: (claims, token) =>
      Equal.equals(token, checkpointToken)
        ? Effect.void
        : Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Invalid deterministic checkpoint token"
            })
          })
        ),
    signTransition: () => Effect.succeed(Option.some(checkpointToken)),
    verifyTransition: (claims, token) =>
      Equal.equals(token, checkpointToken)
        ? Effect.void
        : Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Invalid deterministic transition token"
            })
          })
        )
  }
  const CheckpointLimits = { ...limits, maxSyncChangesPerMessage: 2 }
  const checkpointSourceLayer = (
    checkpointAuthority?: CheckpointAuthority.Implementation
  ) => {
    const services = SqlReplica.servicesLayerWithBindings(definition, {
      projections: [],
      ...(checkpointAuthority === undefined ? {} : { checkpointAuthority })
    }).pipe(
      Layer.provideMerge([
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        ReplicaLimits.layer(CheckpointLimits)
      ])
    )
    return Layer.merge(services, PeerSync.layer.pipe(Layer.provide(services)))
  }
  const checkpointRelayLayer = (receiptLimits: PeerRelayReceiptLimits.Values) => {
    const database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
    const base = Layer.mergeAll(
      database,
      bootstrap,
      CheckpointAuthority.layer(checkpointAuthority)
    )
    const limitsLayer = ReplicaLimits.layer(CheckpointLimits)
    const gate = ReplicaGate.layer.pipe(Layer.provide(limitsLayer), Layer.provide(base))
    const infrastructure = Layer.mergeAll(base, gate, limitsLayer)
    const store = DocumentStore.layer.pipe(Layer.provide(infrastructure))
    const projections = ProjectionStore.layer([]).pipe(Layer.provide(base))
    const services = Layer.mergeAll(
      infrastructure,
      store,
      projections,
      PeerRelayReceiptLimits.layer(receiptLimits)
    )
    const peerSync = PeerSync.layerRelay.pipe(Layer.provide(services))
    return Layer.merge(services, peerSync)
  }

  const seedHistoryPastChangeCap = Effect.gen(function*() {
    const store = yield* DocumentStore.DocumentStore
    const documentId = yield* Identity.makeDocumentId
    let stored = yield* store.create(Task, documentId, { title: "write-0", labels: [] })
    for (let index = 1; index <= 3; index++) {
      const staged = yield* store.stage(stored, (draft) => {
        draft.title = `write-${index}`
      })
      const next = yield* store.persist(Task, documentId, stored, staged)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(stored.automerge)
      stored = next
    }
    return { documentId, stored }
  })

  const checkpointOf = (documentId: Identity.DocumentId, automerge: InternalAutomerge.AnyDocument) =>
    Effect.gen(function*() {
      const snapshot = InternalAutomerge.save(automerge)
      return {
        automerge,
        snapshot,
        heads: InternalAutomerge.heads(automerge),
        checkpointHash: yield* Canonical.digest({ documentId, bytes: snapshot })
      }
    })

  const coldCheckpointFromSource = Effect.gen(function*() {
    const store = yield* DocumentStore.DocumentStore
    const sync = yield* PeerSync.PeerSync
    const { documentId, stored } = yield* seedHistoryPastChangeCap
    const session = yield* sync.open(yield* Identity.makePeerId)
    const generated = yield* sync.generate(Task, documentId, session, {
      lineageAware: true,
      checkpointTransfer: true
    })
    assert.isNotNull(generated.outbound)
    assert.deepStrictEqual(generated.outbound!.message, new Uint8Array())
    assert.deepStrictEqual(generated.outbound!.writerProvenance, [])
    assert.isDefined(generated.outbound!.checkpointTransfer)
    yield* sync.markSent(session, generated.outbound!.sendSequence, generated.outbound!.messageHash)
    const staged = yield* store.stage(stored, (draft) => {
      draft.title = "after-checkpoint-send"
    })
    const advanced = yield* store.persist(Task, documentId, stored, staged)
    const next = yield* sync.generate(Task, documentId, session, {
      lineageAware: true,
      checkpointTransfer: true
    })
    assert.isNotNull(next.outbound)
    assert.isAbove(next.outbound!.message.byteLength, 0)
    assert.isUndefined(next.outbound!.checkpointTransfer)
    InternalAutomerge.free(advanced.automerge)
    InternalAutomerge.free(staged)
    InternalAutomerge.free(stored.automerge)
    return { documentId, outbound: generated.outbound! }
  }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority)))

  it.effect("converges an oversized cold sync through a compact checkpoint and replays its receipt", () =>
    Effect.gen(function*() {
      const generated = yield* coldCheckpointFromSource
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const store = yield* DocumentStore.DocumentStore
        const sql = yield* SqlClient.SqlClient
        const session = yield* sync.open(yield* Identity.makePeerId)
        const input = {
          remoteConnectionEpoch: "checkpoint-source",
          receiveSequence: 0,
          lineage: generated.outbound.lineage,
          message: generated.outbound.message,
          writerProvenance: generated.outbound.writerProvenance,
          checkpointTransfer: generated.outbound.checkpointTransfer!
        }
        const received = yield* sync.receive(Task, generated.documentId, session, input)
        const duplicate = yield* sync.receive(Task, generated.documentId, session, input)
        const decodedTransfer = yield* PeerSyncEnvelope.decodeCheckpointTransfer(
          generated.outbound.checkpointTransfer!,
          CheckpointLimits.maxSyncMessageBytes
        )
        const replacement = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
          ...decodedTransfer,
          manifest: {
            ...decodedTransfer.manifest,
            base: { _tag: "Heads", baseHeads: received.heads }
          }
        }, CheckpointLimits.maxSyncMessageBytes)
        yield* sql`UPDATE effect_local_peer_receipts SET pending_message = ${new Uint8Array([1])}
          WHERE document_id = ${generated.documentId}`
        const rejected = yield* Effect.flip(sync.receive(Task, generated.documentId, session, {
          ...input,
          receiveSequence: 1,
          checkpointTransfer: replacement
        }))
        const loaded = yield* store.load(Task, generated.documentId)
        const rows = yield* sql<{
          readonly receipts: number
          readonly writer_provenance: string
        }>`SELECT writer_provenance,
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE document_id = ${generated.documentId}) AS receipts
          FROM effect_local_checkpoints WHERE document_id = ${generated.documentId}`

        assert.isFalse(received.duplicate)
        assert.isTrue(duplicate.duplicate)
        assert.strictEqual(rejected.reason._tag, "CheckpointRejected")
        assert.strictEqual(loaded.snapshot.value.title, "write-3")
        assert.strictEqual(rows[0]?.receipts, 1)
        const provenance = Schema.decodeSync(WriterProvenance.StoredCheckpointProvenance)(
          rows[0]!.writer_provenance
        )
        assert.isTrue(WriterProvenance.isCompactCheckpoint(provenance))
        InternalAutomerge.free(loaded.automerge)
      }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority)))
    }))

  it.effect("keeps the legacy cold announcement when checkpoint transfer is not advertised", () =>
    Effect.gen(function*() {
      const sync = yield* PeerSync.PeerSync
      const { documentId, stored } = yield* seedHistoryPastChangeCap
      const session = yield* sync.open(yield* Identity.makePeerId)
      const result = yield* sync.generate(Task, documentId, session, {
        lineageAware: true
      })
      assert.isNotNull(result.outbound)
      assert.isAbove(result.outbound!.message.byteLength, 0)
      assert.isUndefined(result.outbound!.checkpointTransfer)
      InternalAutomerge.free(stored.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority))))

  it.effect("falls back to the ordinary cold announcement when checkpoint signing is unavailable", () =>
    Effect.gen(function*() {
      const sync = yield* PeerSync.PeerSync
      const { documentId, stored } = yield* seedHistoryPastChangeCap
      const session = yield* sync.open(yield* Identity.makePeerId)
      const result = yield* sync.generate(Task, documentId, session, {
        lineageAware: true,
        checkpointTransfer: true
      })

      assert.isNotNull(result.outbound)
      assert.isAbove(result.outbound!.message.byteLength, 0)
      assert.isUndefined(result.outbound!.checkpointTransfer)
      InternalAutomerge.free(stored.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer())))

  it.effect("rejects a checkpoint with invalid authorization without durable mutation", () =>
    Effect.gen(function*() {
      const generated = yield* coldCheckpointFromSource
      const decoded = yield* PeerSyncEnvelope.decodeCheckpointTransfer(
        generated.outbound.checkpointTransfer!,
        CheckpointLimits.maxSyncMessageBytes
      )
      const hostile = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
        ...decoded,
        manifest: { ...decoded.manifest, authorization: new Uint8Array([99]) }
      }, CheckpointLimits.maxSyncMessageBytes)
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const before = yield* durableSnapshot
        const failure = yield* Effect.flip(sync.receive(Task, generated.documentId, session, {
          remoteConnectionEpoch: "hostile-checkpoint",
          receiveSequence: 0,
          lineage: generated.outbound.lineage,
          message: new Uint8Array(),
          writerProvenance: [],
          checkpointTransfer: hostile
        }))

        assert.strictEqual(failure.reason._tag, "CheckpointRejected")
        assert.deepStrictEqual(yield* durableSnapshot, before)
      }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority)))
    }))

  it.effect("accepts and idempotently replays a relayed checkpoint receipt with usage", () =>
    Effect.gen(function*() {
      const generated = yield* coldCheckpointFromSource
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const sql = yield* SqlClient.SqlClient
        const senderPeerId = yield* Identity.makePeerId
        const session = yield* sync.open(senderPeerId)
        const messageHash = yield* Canonical.digest(new Uint8Array())
        const relay: PeerSync.RelayReceipt = {
          relayMessageId: yield* Identity.makeRelayMessageId,
          relayPeerId: yield* Identity.makePeerId,
          senderTenantId: "checkpoint-tenant",
          senderSubjectId: "checkpoint-subject",
          senderPeerId,
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(7),
          messageHash,
          outerEnvelopeDigest: "a".repeat(64),
          receiptExpiresAt: new Date(
            (yield* Clock.currentTimeMillis) + PeerRelayReceiptLimits.defaults.receiptRetentionMillis
          ).toISOString(),
          encodedSize: generated.outbound.checkpointTransfer!.byteLength + 128
        }
        const input = {
          remoteConnectionEpoch: "relay-checkpoint",
          receiveSequence: 0,
          lineage: generated.outbound.lineage,
          message: new Uint8Array(),
          writerProvenance: [] as const,
          checkpointTransfer: generated.outbound.checkpointTransfer!,
          relay
        }
        const received = yield* sync.receive(Task, generated.documentId, session, input)
        const duplicate = yield* sync.receive(Task, generated.documentId, session, input)
        const rows = yield* sql<{
          readonly checkpoint_transfer: Uint8Array
          readonly relay_outer_envelope_digest: string
          readonly usage_bytes: number
          readonly usage_count: number
        }>`SELECT checkpoint_transfer, relay_outer_envelope_digest,
          (SELECT encoded_bytes FROM effect_local_peer_relay_receipt_usage) AS usage_bytes,
          (SELECT receipt_count FROM effect_local_peer_relay_receipt_usage) AS usage_count
          FROM effect_local_peer_receipts WHERE relay_message_id = ${relay.relayMessageId}`

        assert.isFalse(received.duplicate)
        assert.isTrue(duplicate.duplicate)
        assert.deepStrictEqual(rows[0]?.checkpoint_transfer, input.checkpointTransfer)
        assert.strictEqual(rows[0]?.relay_outer_envelope_digest, relay.outerEnvelopeDigest)
        assert.strictEqual(rows[0]?.usage_count, 1)
        assert.isAbove(rows[0]?.usage_bytes ?? 0, relay.encodedSize)
      }).pipe(Effect.provide(checkpointRelayLayer(PeerRelayReceiptLimits.defaults)))
    }))

  it.effect("rolls back a relayed checkpoint when retained receipt bytes exceed quota", () =>
    Effect.gen(function*() {
      const generated = yield* coldCheckpointFromSource
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const senderPeerId = yield* Identity.makePeerId
        const session = yield* sync.open(senderPeerId)
        const relay: PeerSync.RelayReceipt = {
          relayMessageId: yield* Identity.makeRelayMessageId,
          relayPeerId: yield* Identity.makePeerId,
          senderTenantId: "quota-tenant",
          senderSubjectId: "quota-subject",
          senderPeerId,
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(8),
          messageHash: yield* Canonical.digest(new Uint8Array()),
          outerEnvelopeDigest: "b".repeat(64),
          receiptExpiresAt: new Date(
            (yield* Clock.currentTimeMillis) + PeerRelayReceiptLimits.defaults.receiptRetentionMillis
          ).toISOString(),
          encodedSize: 1
        }
        const before = yield* durableSnapshot
        const failure = yield* Effect.flip(sync.receive(Task, generated.documentId, session, {
          remoteConnectionEpoch: "relay-checkpoint-quota",
          receiveSequence: 0,
          lineage: generated.outbound.lineage,
          message: new Uint8Array(),
          writerProvenance: [],
          checkpointTransfer: generated.outbound.checkpointTransfer!,
          relay
        }))

        assert.strictEqual(failure.reason._tag, "QuotaExceeded")
        if (failure.reason._tag === "QuotaExceeded") {
          assert.strictEqual(failure.reason.resource, "relay receipt bytes per remote")
        }
        assert.deepStrictEqual(yield* durableSnapshot, before)
      }).pipe(Effect.provide(checkpointRelayLayer({
        ...PeerRelayReceiptLimits.defaults,
        maxEncodedBytesPerRemote: 1,
        maxEncodedBytesPerReplica: 1
      })))
    }))

  it.effect("rejects an in-flight checkpoint install after its session resets", () =>
    Effect.gen(function*() {
      const generated = yield* coldCheckpointFromSource
      const verificationStarted = yield* Deferred.make<void>()
      const releaseVerification = yield* Deferred.make<void>()
      const blockingAuthority: CheckpointAuthority.Implementation = {
        ...checkpointAuthority,
        verifyManifest: (claims, token) =>
          Deferred.succeed(verificationStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseVerification)),
            Effect.andThen(checkpointAuthority.verifyManifest(claims, token))
          )
      }
      yield* Effect.gen(function*() {
        const sync = yield* PeerSync.PeerSync
        const session = yield* sync.open(yield* Identity.makePeerId)
        const before = yield* durableSnapshot
        const installing = yield* Effect.exit(
          sync.receive(Task, generated.documentId, session, {
            remoteConnectionEpoch: "checkpoint-reset",
            receiveSequence: 0,
            lineage: generated.outbound.lineage,
            message: new Uint8Array(),
            writerProvenance: [] as const,
            checkpointTransfer: generated.outbound.checkpointTransfer!
          })
        ).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(verificationStarted)
        yield* sync.reset(session)
        yield* Deferred.succeed(releaseVerification, undefined)
        const exit = yield* Fiber.join(installing)

        assert.strictEqual(exit._tag, "Failure")
        assert.deepStrictEqual(yield* durableSnapshot, before)
      }).pipe(
        Effect.provide(checkpointSourceLayer(blockingAuthority)),
        Effect.ensuring(Deferred.succeed(releaseVerification, undefined))
      )
    }))

  it.effect("installs a same-lineage bootstrap checkpoint over an existing contained prefix", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const local = yield* store.create(Task, documentId, { title: "prefix", labels: [] })
      const current = yield* checkpointOf(
        documentId,
        Automerge.change(
          Automerge.clone(local.automerge, { actor: "3".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "same-lineage-current"
          }
        )
      )
      const transfer = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
        snapshot: current.snapshot,
        manifest: {
          purpose: CheckpointAuthority.manifestPurpose,
          documentId,
          lineage: Identity.genesisLineage,
          checkpointHash: current.checkpointHash,
          heads: current.heads,
          base: { _tag: "Bootstrap" },
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        },
        transitions: []
      }, CheckpointLimits.maxSyncMessageBytes)
      const session = yield* sync.open(yield* Identity.makePeerId)
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "same-lineage-bootstrap",
        receiveSequence: 0,
        lineage: Identity.genesisLineage,
        message: new Uint8Array(),
        writerProvenance: [],
        checkpointTransfer: transfer
      })
      const loaded = yield* store.load(Task, documentId)
      assert.strictEqual(loaded.snapshot.value.title, "same-lineage-current")
      assert.deepStrictEqual(loaded.materializedHeads, current.heads)
      const staged = yield* store.stage(loaded, (draft) => {
        draft.title = "after-checkpoint-receive"
      })
      const advanced = yield* store.persist(Task, documentId, loaded, staged)
      const outbound = yield* sync.generate(Task, documentId, session, {
        lineageAware: true,
        checkpointTransfer: true
      })
      assert.isNotNull(outbound.outbound)
      assert.isAbove(outbound.outbound!.message.byteLength, 0)
      assert.isUndefined(outbound.outbound!.checkpointTransfer)
      InternalAutomerge.free(advanced.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(loaded.automerge)
      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(local.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority))))

  it.effect("syncs ordinary edits in both directions after installing a compact checkpoint", () =>
    Effect.scoped(Effect.gen(function*() {
      const openPeer = Effect.gen(function*() {
        const context = yield* Layer.build(checkpointSourceLayer(checkpointAuthority))
        const sync = Context.get(context, PeerSync.PeerSync)
        return {
          context,
          store: Context.get(context, DocumentStore.DocumentStore),
          sync,
          session: yield* sync.open(yield* Identity.makePeerId)
        }
      })
      const sender = yield* openPeer
      const recipient = yield* openPeer

      const { documentId, stored } = yield* seedHistoryPastChangeCap.pipe(
        Effect.provide(sender.context)
      )
      InternalAutomerge.free(stored.automerge)

      type Side = typeof sender
      type Packet = { readonly from: Side; readonly outbound: PeerSync.Outbound; readonly to: Side }
      const pending: Array<Packet> = []
      const enqueueGenerated = Effect.fnUntraced(function*(from: Side, to: Side) {
        const generated = yield* from.sync.generate(Task, documentId, from.session, {
          lineageAware: true,
          checkpointTransfer: true
        })
        if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
        return generated
      })
      const drain = Effect.fnUntraced(function*() {
        for (let round = 0; round < 32; round++) {
          while (pending.length > 0) {
            const packet = pending.shift()!
            const received = yield* packet.to.sync.receive(Task, documentId, packet.to.session, {
              remoteConnectionEpoch: packet.from.session.connectionEpoch,
              receiveSequence: packet.outbound.sendSequence,
              lineage: packet.outbound.lineage,
              message: packet.outbound.message,
              writerProvenance: packet.outbound.writerProvenance,
              ...(packet.outbound.checkpointTransfer === undefined
                ? {}
                : { checkpointTransfer: packet.outbound.checkpointTransfer })
            })
            yield* packet.from.sync.markSent(
              packet.from.session,
              packet.outbound.sendSequence,
              packet.outbound.messageHash
            )
            if (received.reply !== null) {
              pending.push({
                from: packet.to,
                outbound: yield* packet.to.sync.enqueue(packet.to.session, received.reply),
                to: packet.from
              })
            }
          }
          const [fromSender, fromRecipient] = yield* Effect.all([
            enqueueGenerated(sender, recipient),
            enqueueGenerated(recipient, sender)
          ])
          if (
            pending.length === 0 && fromSender.outbound === null && fromRecipient.outbound === null &&
            !fromSender.dirty && !fromRecipient.dirty
          ) return
        }
        return yield* Effect.die("post-checkpoint peer sync did not reach quiescence")
      })

      const checkpoint = yield* enqueueGenerated(sender, recipient)
      assert.isNotNull(checkpoint.outbound)
      assert.isDefined(checkpoint.outbound!.checkpointTransfer)
      yield* drain()

      const recipientStored = yield* recipient.store.load(Task, documentId)
      const recipientStaged = yield* recipient.store.stage(recipientStored, (draft) => {
        draft.title = "recipient-edit"
      })
      const recipientAdvanced = yield* recipient.store.persist(
        Task,
        documentId,
        recipientStored,
        recipientStaged
      )
      InternalAutomerge.free(recipientAdvanced.automerge)
      InternalAutomerge.free(recipientStaged)
      InternalAutomerge.free(recipientStored.automerge)
      yield* drain()

      const senderStored = yield* sender.store.load(Task, documentId)
      assert.strictEqual(senderStored.snapshot.value.title, "recipient-edit")
      const senderStaged = yield* sender.store.stage(senderStored, (draft) => {
        draft.title = "sender-edit"
      })
      const senderAdvanced = yield* sender.store.persist(Task, documentId, senderStored, senderStaged)
      InternalAutomerge.free(senderAdvanced.automerge)
      InternalAutomerge.free(senderStaged)
      InternalAutomerge.free(senderStored.automerge)
      yield* drain()

      const converged = yield* recipient.store.load(Task, documentId)
      assert.strictEqual(converged.snapshot.value.title, "sender-edit")
      InternalAutomerge.free(converged.automerge)
    })).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects generation when the retained lineage chain exceeds the wire bound", () =>
    Effect.gen(function*() {
      const sync = yield* PeerSync.PeerSync
      const sql = yield* SqlClient.SqlClient
      const { documentId, stored } = yield* seedHistoryPastChangeCap
      const snapshot = InternalAutomerge.save(stored.automerge)
      const checkpointHash = yield* Canonical.digest({ documentId, bytes: snapshot })
      const encodedHeads = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)))(
        stored.materializedHeads
      )
      const lineages = Array.from({ length: PeerSyncEnvelope.maximumCheckpointTransitions + 1 }, (_, index) =>
        Identity.DocumentLineage.make(
          `lin_${(index + 1).toString(16).padStart(8, "0")}-0000-4000-8000-${
            (index + 1).toString(16).padStart(12, "0")
          }`
        ))
      for (const [index, lineage] of lineages.entries()) {
        yield* sql`INSERT INTO effect_local_lineage_transitions (
          document_id, prior_lineage, prior_checkpoint_hash, prior_heads, prior_snapshot,
          lineage, checkpoint_hash, heads, schema_version, writer_definition_hash,
          authorization, created_at
        ) VALUES (
          ${documentId}, ${index === 0 ? Identity.genesisLineage : lineages[index - 1]!},
          ${checkpointHash}, ${encodedHeads}, ${snapshot}, ${lineage}, ${checkpointHash}, ${encodedHeads},
          ${Task.version}, ${definition.hash}, ${checkpointToken}, ${new Date(0).toISOString()}
        )`
      }
      yield* sql`UPDATE effect_local_documents SET lineage = ${lineages.at(-1)!}
        WHERE document_id = ${documentId}`
      const session = yield* sync.open(yield* Identity.makePeerId)
      const failure = yield* Effect.flip(sync.generate(Task, documentId, session, {
        lineageAware: true,
        checkpointTransfer: true
      }))

      assert.strictEqual(failure.reason._tag, "QuotaExceeded")
      InternalAutomerge.free(stored.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority))))

  const firstRewrittenLineage = Identity.DocumentLineage.make(
    "lin_aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  )
  const secondRewrittenLineage = Identity.DocumentLineage.make(
    "lin_bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  )

  it.effect("installs a signed rewrite when the receiver is a strict prefix of the transition snapshot", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const local = yield* store.create(Task, documentId, { title: "local-prefix", labels: [] })
      const prior = yield* checkpointOf(
        documentId,
        Automerge.change(
          Automerge.clone(local.automerge, { actor: "a".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "before-rewrite"
          }
        )
      )
      const anchor = yield* checkpointOf(
        documentId,
        InternalAutomerge.reroot(InternalAutomerge.value(prior.automerge), false, "b".repeat(32))
      )
      const current = yield* checkpointOf(
        documentId,
        Automerge.change(
          Automerge.clone(anchor.automerge, { actor: "c".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "after-rewrite-mutation"
          }
        )
      )
      assert.notStrictEqual(current.checkpointHash, anchor.checkpointHash)
      const transfer = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
        snapshot: current.snapshot,
        manifest: {
          purpose: CheckpointAuthority.manifestPurpose,
          documentId,
          lineage: firstRewrittenLineage,
          checkpointHash: current.checkpointHash,
          heads: current.heads,
          base: { _tag: "Heads", baseHeads: local.materializedHeads },
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        },
        transitions: [{
          purpose: CheckpointAuthority.transitionPurpose,
          documentId,
          priorLineage: Identity.genesisLineage,
          priorCheckpointHash: prior.checkpointHash,
          priorHeads: prior.heads,
          priorSnapshot: prior.snapshot,
          resultingLineage: firstRewrittenLineage,
          anchorCheckpointHash: anchor.checkpointHash,
          resultingHeads: anchor.heads,
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        }]
      }, CheckpointLimits.maxSyncMessageBytes)
      const session = yield* sync.open(yield* Identity.makePeerId)
      const received = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "prefix-transition",
        receiveSequence: 0,
        lineage: firstRewrittenLineage,
        message: new Uint8Array(),
        writerProvenance: [],
        checkpointTransfer: transfer
      })
      const loaded = yield* store.load(Task, documentId)

      assert.deepStrictEqual(received.heads, current.heads)
      assert.strictEqual(loaded.snapshot.value.title, "after-rewrite-mutation")
      assert.deepStrictEqual(loaded.materializedHeads, current.heads)

      InternalAutomerge.free(loaded.automerge)
      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(anchor.automerge)
      InternalAutomerge.free(prior.automerge)
      InternalAutomerge.free(local.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority))))

  it.effect("rejects signed hops with mismatched intermediate heads without durable mutation", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sync = yield* PeerSync.PeerSync
      const documentId = yield* Identity.makeDocumentId
      const local = yield* store.create(Task, documentId, { title: "local-prefix", labels: [] })
      const firstPrior = yield* checkpointOf(
        documentId,
        Automerge.change(
          Automerge.clone(local.automerge, { actor: "d".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "first-prior"
          }
        )
      )
      const firstAnchor = yield* checkpointOf(
        documentId,
        InternalAutomerge.reroot(InternalAutomerge.value(firstPrior.automerge), false, "e".repeat(32))
      )
      const mismatchedPrior = yield* checkpointOf(
        documentId,
        InternalAutomerge.reroot({ title: "mismatched-intermediate", labels: [] }, false, "f".repeat(32))
      )
      assert.isFalse(sameHeadsForTest(firstAnchor.heads, mismatchedPrior.heads))
      const secondAnchor = yield* checkpointOf(
        documentId,
        InternalAutomerge.reroot(InternalAutomerge.value(mismatchedPrior.automerge), false, "1".repeat(32))
      )
      const current = yield* checkpointOf(
        documentId,
        Automerge.change(
          Automerge.clone(secondAnchor.automerge, { actor: "2".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "post-rewrite-mutation"
          }
        )
      )
      assert.notStrictEqual(current.checkpointHash, secondAnchor.checkpointHash)
      const transfer = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
        snapshot: current.snapshot,
        manifest: {
          purpose: CheckpointAuthority.manifestPurpose,
          documentId,
          lineage: secondRewrittenLineage,
          checkpointHash: current.checkpointHash,
          heads: current.heads,
          base: { _tag: "Heads", baseHeads: local.materializedHeads },
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        },
        transitions: [{
          purpose: CheckpointAuthority.transitionPurpose,
          documentId,
          priorLineage: Identity.genesisLineage,
          priorCheckpointHash: firstPrior.checkpointHash,
          priorHeads: firstPrior.heads,
          priorSnapshot: firstPrior.snapshot,
          resultingLineage: firstRewrittenLineage,
          anchorCheckpointHash: firstAnchor.checkpointHash,
          resultingHeads: firstAnchor.heads,
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        }, {
          purpose: CheckpointAuthority.transitionPurpose,
          documentId,
          priorLineage: firstRewrittenLineage,
          priorCheckpointHash: mismatchedPrior.checkpointHash,
          priorHeads: mismatchedPrior.heads,
          priorSnapshot: mismatchedPrior.snapshot,
          resultingLineage: secondRewrittenLineage,
          anchorCheckpointHash: secondAnchor.checkpointHash,
          resultingHeads: secondAnchor.heads,
          schemaVersion: Task.version,
          writerDefinitionHash: definition.hash,
          authorization: checkpointToken
        }]
      }, CheckpointLimits.maxSyncMessageBytes)
      const session = yield* sync.open(yield* Identity.makePeerId)
      const before = yield* durableSnapshot
      const failure = yield* Effect.flip(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: "mismatched-transition",
        receiveSequence: 0,
        lineage: secondRewrittenLineage,
        message: new Uint8Array(),
        writerProvenance: [],
        checkpointTransfer: transfer
      }))

      assert.strictEqual(failure.reason._tag, "CheckpointRejected")
      assert.deepStrictEqual(yield* durableSnapshot, before)

      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(secondAnchor.automerge)
      InternalAutomerge.free(mismatchedPrior.automerge)
      InternalAutomerge.free(firstAnchor.automerge)
      InternalAutomerge.free(firstPrior.automerge)
      InternalAutomerge.free(local.automerge)
    }).pipe(Effect.provide(checkpointSourceLayer(checkpointAuthority))))
})

const sameHeadsForTest = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  JSON.stringify([...left].toSorted()) === JSON.stringify([...right].toSorted())
