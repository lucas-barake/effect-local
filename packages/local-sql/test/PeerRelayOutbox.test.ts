import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as PeerRelayOutbox from "../src/PeerRelayOutbox.js"
import * as PeerRelayOutboxLimits from "../src/PeerRelayOutboxLimits.js"
import * as PeerSyncEnvelope from "../src/PeerSyncEnvelope.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("PeerRelayOutbox", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "relay-outbox",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const replicaLimits: ReplicaLimits.Values = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 64,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 10_000,
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
    maxActiveRestores: 10,
    maxRestoresPerSession: 1,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const outboxLimits: PeerRelayOutboxLimits.Values = {
    ...PeerRelayOutboxLimits.defaults,
    maxMessagesPerRemote: 2,
    maxMessagesPerReplica: 3,
    maxEncodedBytesPerRemote: 1_000_000,
    maxEncodedBytesPerReplica: 2_000_000,
    maxRetryHorizonMillis: 60_000,
    pruneBatchSize: 10
  }
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
  const lineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000005")
  const endpoint: PeerRelayOutbox.Endpoint = {
    expectedLocal: {
      tenantId: "tenant-a",
      subjectId: "sender-a",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
    },
    remote: {
      tenantId: "tenant-a",
      subjectId: "recipient-a",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
    },
    relayPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
  }

  const layer = (
    filename: string,
    limits: PeerRelayOutboxLimits.Values = outboxLimits
  ) => {
    const Database = Layer.merge(
      SqliteClient.layer({ filename, disableWAL: true }),
      NodeCrypto.layer
    )
    const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
    const Base = Layer.merge(Database, Bootstrap)
    const ReplicaLimitLayer = ReplicaLimits.layer(replicaLimits)
    const Gate = ReplicaGate.layer.pipe(
      Layer.provide(ReplicaLimitLayer),
      Layer.provide(Base)
    )
    const Infrastructure = Layer.mergeAll(
      Base,
      Gate,
      ReplicaLimitLayer,
      PeerRelayOutboxLimits.layer(limits)
    )
    const Outbox = PeerRelayOutbox.layerSql.pipe(Layer.provide(Infrastructure))
    return Layer.merge(Infrastructure, Outbox)
  }

  const insertDocument = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`INSERT OR IGNORE INTO effect_local_documents (
      document_id, document_type, schema_version, observed_versions,
      materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
    ) VALUES (${documentId}, 'Task', 1, '[1]', '[]', '[]', 0, 'Ready', NULL)`
  })

  const makePayload = (sequence: number) =>
    Effect.gen(function*() {
      let source = Automerge.from(
        { value: { title: "one" }, tombstone: false },
        { actor: "a".repeat(32) }
      )
      const remote = Automerge.init()
      const handshake = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
      const received = Automerge.receiveSyncMessage(source, Automerge.initSyncState(), handshake)
      source = received[0]
      const message = Automerge.generateSyncMessage(source, received[1])[1]!
      const changes = Automerge.getAllChanges(source).map(Automerge.decodeChange)
      const envelope = PeerSyncEnvelope.SyncEnvelope.make({
        connectionEpoch: "epoch-1",
        sequence,
        documentId,
        documentType: "Task",
        messageHash: yield* Canonical.digest(message),
        message,
        lineage,
        writerProvenance: changes.map((change) => ({
          changeHash: change.hash,
          writerSchemaVersion: 1,
          writerDefinitionHash: definition.hash
        }))
      })
      Automerge.free(source)
      Automerge.free(remote)
      return yield* PeerSyncEnvelope.encodeSyncEnvelope(envelope)
    })

  it.effect("reuses one stable admission after time advances without incrementing quota", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const payload = yield* makePayload(1)
      const first = yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 30_000 })
      const decoded = yield* PeerSyncEnvelope.decodeSyncEnvelope(payload, replicaLimits)
      assert.strictEqual(decoded.lineage, lineage)
      assert.strictEqual(
        first.outerEnvelopeDigest,
        yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
          domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
          version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
          expectedLocal: first.expectedLocal,
          remote: first.remote,
          relayPeerId: first.relayPeerId,
          relayMessageId: first.relayMessageId,
          protocolVersion: first.protocolVersion,
          payloadVersion: first.payloadVersion,
          senderReplicaIncarnation: first.replicaIncarnation,
          senderConnectionEpoch: first.senderConnectionEpoch,
          senderSequence: first.senderSequence,
          document: first.document,
          lineage: decoded.lineage,
          writerProvenance: first.writerProvenance,
          messageHash: first.messageHash,
          payload: first.payload
        })
      )
      yield* TestClock.adjust("10 seconds")
      const duplicate = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      assert.strictEqual(duplicate.relayMessageId, first.relayMessageId)
      assert.strictEqual(duplicate.createdAt, first.createdAt)
      assert.strictEqual(duplicate.retryDeadline, first.retryDeadline)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 1, encodedBytes: payload.byteLength },
        replica: { messageCount: 1, encodedBytes: payload.byteLength }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("rejects source collisions and concurrent quota growth without changing usage", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const firstPayload = yield* makePayload(1)
      yield* outbox.admit({ ...endpoint, payload: firstPayload, retryHorizonMillis: 30_000 })
      const collision = yield* Effect.exit(outbox.admit({
        ...endpoint,
        expectedLocal: { ...endpoint.expectedLocal, subjectId: "other-sender" },
        payload: firstPayload,
        retryHorizonMillis: 30_000
      }))
      assert.strictEqual(collision._tag, "Failure")
      const secondPayload = yield* makePayload(2)
      yield* outbox.admit({ ...endpoint, payload: secondPayload, retryHorizonMillis: 30_000 })
      const thirdPayload = yield* makePayload(3)
      const quota = yield* Effect.exit(outbox.admit({
        ...endpoint,
        payload: thirdPayload,
        retryHorizonMillis: 30_000
      }))
      assert.strictEqual(quota._tag, "Failure")
      const usage = yield* outbox.usage(endpoint)
      assert.strictEqual(usage.remote.messageCount, 2)
      assert.strictEqual(usage.replica.messageCount, 2)
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("prunes expired rows and deletes zero usage rows", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const sql = yield* SqlClient.SqlClient
      const payload = yield* makePayload(1)
      yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 1_000 })
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* outbox.pruneExpired, 1)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 0, encodedBytes: 0 },
        replica: { messageCount: 0, encodedBytes: 0 }
      })
      const usageRows = yield* sql`SELECT * FROM effect_local_peer_relay_outbox_remote_usage`
      assert.strictEqual(usageRows.length, 0)
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("fails closed when a durable retry is scheduled at its deadline", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const sql = yield* SqlClient.SqlClient
      const payload = yield* makePayload(1)
      yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 1_000 })
      yield* sql`UPDATE effect_local_peer_relay_outbox
        SET next_attempt_at = retry_deadline`
      yield* TestClock.adjust("1 second")
      const exit = yield* Effect.exit(outbox.pruneExpired)
      assert.strictEqual(exit._tag, "Failure")
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 1, encodedBytes: payload.byteLength },
        replica: { messageCount: 1, encodedBytes: payload.byteLength }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("rejects document deletion while pending custody owns the payload and quota", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const sql = yield* SqlClient.SqlClient
      const payload = yield* makePayload(1)
      const entry = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      assert.strictEqual(
        (yield* Effect.exit(sql`DELETE FROM effect_local_documents
          WHERE document_id = ${documentId}`))._tag,
        "Failure"
      )
      const replay = yield* outbox.dueForEndpoint({ ...endpoint, maximum: 2 })
      assert.strictEqual(replay.length, 1)
      assert.strictEqual(replay[0]!.relayMessageId, entry.relayMessageId)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 1, encodedBytes: payload.byteLength },
        replica: { messageCount: 1, encodedBytes: payload.byteLength }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("fences every replay operation after the replica incarnation changes", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const payload = yield* makePayload(1)
      const before = yield* gate.current
      yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 30_000 })
      yield* gate.claim(() => Effect.void)
      assert.strictEqual(
        (yield* Effect.exit(outbox.validateReplicaIncarnation(before.incarnation)))._tag,
        "Failure"
      )
      assert.deepStrictEqual(yield* outbox.dueForEndpoint({ ...endpoint, maximum: 2 }), [])
      assert.strictEqual(yield* outbox.maximumPendingHorizon(endpoint), null)
      const rows = yield* sql`SELECT replica_incarnation
        FROM effect_local_peer_relay_outbox`
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0]!.replica_incarnation, before.incarnation)
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("replays and retires a pending row after writer generation changes on restart", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-local-relay-outbox-"))),
      (directory) =>
        Effect.gen(function*() {
          const filename = join(directory, "replica.sqlite")
          const first = yield* Effect.scoped(
            Effect.gen(function*() {
              yield* insertDocument
              const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
              const gate = yield* ReplicaGate.ReplicaGate
              const payload = yield* makePayload(1)
              const entry = yield* outbox.admit({
                ...endpoint,
                payload,
                retryHorizonMillis: 30_000
              })
              return { entry, permit: yield* gate.current }
            }).pipe(Effect.provide(layer(filename)))
          )

          yield* Effect.scoped(
            Effect.gen(function*() {
              const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
              const gate = yield* ReplicaGate.ReplicaGate
              const permit = yield* gate.current
              assert.strictEqual(permit.replicaId, first.permit.replicaId)
              assert.strictEqual(permit.incarnation, first.permit.incarnation)
              assert.isAbove(permit.writerGeneration, first.permit.writerGeneration)
              const replay = yield* outbox.dueForEndpoint({ ...endpoint, maximum: 2 })
              assert.strictEqual(replay.length, 1)
              assert.strictEqual(replay[0]!.relayMessageId, first.entry.relayMessageId)
              yield* outbox.markCustody({
                relayMessageId: replay[0]!.relayMessageId,
                outerEnvelopeDigest: replay[0]!.outerEnvelopeDigest
              })
              assert.deepStrictEqual(
                yield* outbox.dueForEndpoint({
                  ...endpoint,
                  maximum: 2
                }),
                []
              )
            }).pipe(Effect.provide(layer(filename)))
          )
        }),
      (directory) => Effect.sync(() => rmSync(directory, { recursive: true, force: true }))
    ))
})
