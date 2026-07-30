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
import * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as Tracer from "effect/Tracer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
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
    const DeliveryStore = CommandDeliveryStore.layer.pipe(Layer.provide(Infrastructure))
    const Outbox = PeerRelayOutbox.layerSql.pipe(Layer.provide(Infrastructure))
    return Layer.mergeAll(Infrastructure, DeliveryStore, Outbox)
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

  const trackCommand = (
    commandId: Identity.CommandId,
    payload: Uint8Array
  ) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.current
      const envelope = yield* PeerSyncEnvelope.decodeSyncEnvelope(payload, replicaLimits)
      yield* sql`INSERT INTO effect_local_command_receipts (
        replica_incarnation, command_id, request_hash, mutation_name, result,
        document_id, heads, commit_sequence
      ) VALUES (
        ${permit.incarnation}, ${commandId}, ${`request-${commandId}`}, '$create',
        ${new TextEncoder().encode(commandId)}, ${documentId}, '[]', 1
      )`
      yield* sql`INSERT INTO effect_local_command_delivery_sources (
        replica_incarnation, command_id, document_id
      ) VALUES (${permit.incarnation}, ${commandId}, ${documentId})`
      for (const provenance of envelope.writerProvenance) {
        yield* sql`INSERT INTO effect_local_command_delivery_changes (
          replica_incarnation, command_id, change_hash
        ) VALUES (${permit.incarnation}, ${commandId}, ${provenance.changeHash})`
      }
      return envelope.writerProvenance.length
    })

  it.effect("derives provenance change hashes once per admitted message across replays", () => {
    const counter = { digests: 0 }
    const Database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
    const Base = Layer.merge(Database, Bootstrap)
    const ReplicaLimitLayer = ReplicaLimits.layer(replicaLimits)
    const Gate = ReplicaGate.layer.pipe(
      Layer.provide(ReplicaLimitLayer),
      Layer.provide(Base)
    )
    const CountingCrypto = Layer.effect(
      Crypto.Crypto,
      Effect.gen(function*() {
        const crypto = yield* Crypto.Crypto
        return Crypto.Crypto.of({
          ...crypto,
          digest: (algorithm, data) =>
            Effect.andThen(
              Effect.sync(() => {
                counter.digests++
              }),
              crypto.digest(algorithm, data)
            )
        })
      })
    ).pipe(Layer.provide(NodeCrypto.layer))
    const Infrastructure = Layer.mergeAll(
      Base,
      Gate,
      ReplicaLimitLayer,
      PeerRelayOutboxLimits.layer(outboxLimits),
      CountingCrypto
    )
    const DeliveryStore = CommandDeliveryStore.layer.pipe(Layer.provide(Infrastructure))
    const Outbox = PeerRelayOutbox.layerSql.pipe(Layer.provide(Infrastructure))
    const testLayer = Layer.mergeAll(Infrastructure, DeliveryStore, Outbox)

    return Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const payload = yield* makePayload(41)
      const entry = yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 30_000 })
      assert.strictEqual(entry._tag, "PendingRelayCustody")
      counter.digests = 0
      const first = yield* outbox.dueForEndpoint({ ...endpoint, maximum: 2 })
      assert.strictEqual(first.length, 1)
      const firstRoundDigests = counter.digests
      counter.digests = 0
      const second = yield* outbox.dueForEndpoint({ ...endpoint, maximum: 2 })
      assert.strictEqual(second.length, 1)
      // validateRow re-verifies the stored payload digest on every round. The payload decode
      // and message hash verification behind provenance derivation runs once per message.
      assert.strictEqual(firstRoundDigests, 2)
      assert.strictEqual(counter.digests, 2)
    }).pipe(Effect.provide(testLayer))
  })

  it.effect("reuses one stable admission after time advances without incrementing quota", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const payload = yield* makePayload(1)
      const first = yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 30_000 })
      assert.strictEqual(first._tag, "PendingRelayCustody")
      if (first._tag !== "PendingRelayCustody") return
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
      assert.strictEqual(duplicate._tag, "PendingRelayCustody")
      if (duplicate._tag !== "PendingRelayCustody") return
      assert.strictEqual(duplicate.relayMessageId, first.relayMessageId)
      assert.strictEqual(duplicate.createdAt, first.createdAt)
      assert.strictEqual(duplicate.retryDeadline, first.retryDeadline)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 1, encodedBytes: payload.byteLength },
        replica: { messageCount: 1, encodedBytes: payload.byteLength }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("reports relay custody for the exact command changes", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000021")
      const payload = yield* makePayload(21)
      const localChangeCount = yield* trackCommand(commandId, payload)
      const entry = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })

      const pending = yield* deliveries.lookup(commandId)
      assert.strictEqual(pending._tag, "TrackedCommand")
      if (pending._tag !== "TrackedCommand") return
      assert.strictEqual(pending.localChangeCount, localChangeCount)
      assert.strictEqual(pending.destinations.length, 1)
      assert.strictEqual(pending.destinations[0]?.state._tag, "PendingRelayCustody")

      yield* outbox.markCustody({
        relayMessageId: entry.relayMessageId,
        outerEnvelopeDigest: entry.outerEnvelopeDigest
      })
      const accepted = yield* deliveries.lookup(commandId)
      assert.strictEqual(accepted._tag, "TrackedCommand")
      if (accepted._tag !== "TrackedCommand") return
      assert.strictEqual(accepted.destinations[0]?.state._tag, "RelayCustodyAccepted")
      assert.strictEqual(
        accepted.destinations[0]?.state.acceptedChangeCount,
        localChangeCount
      )
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("returns every durable destination without silently truncating evidence", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000028")
      const payload = yield* makePayload(28)
      yield* trackCommand(commandId, payload)
      const envelope = yield* PeerSyncEnvelope.decodeSyncEnvelope(payload, replicaLimits)
      const changeHash = envelope.writerProvenance[0]!.changeHash
      const permit = yield* gate.current
      const messages = Array.from({ length: 257 }, (_, index) => {
        const suffix = (index + 1).toString(16).padStart(12, "0")
        return {
          replica_id: permit.replicaId,
          replica_incarnation: permit.incarnation,
          expected_local_tenant_id: endpoint.expectedLocal.tenantId,
          expected_local_subject_id: endpoint.expectedLocal.subjectId,
          expected_local_peer_id: endpoint.expectedLocal.peerId,
          remote_tenant_id: endpoint.remote.tenantId,
          remote_subject_id: endpoint.remote.subjectId,
          remote_peer_id: `peer_00000000-0000-4000-8000-${suffix}`,
          relay_peer_id: endpoint.relayPeerId,
          relay_message_id: `rly_00000000-0000-4000-8000-${suffix}`,
          outer_envelope_digest: "a".repeat(64),
          sender_connection_epoch: "destination-coverage",
          sender_sequence: index,
          document_id: documentId,
          created_at: "2026-01-01T00:00:00.000Z",
          retry_deadline: "2026-01-02T00:00:00.000Z",
          relay_custody_accepted_at: "2026-01-01T00:00:01.000Z",
          sender_custody_unconfirmed_at: null
        }
      })
      for (let offset = 0; offset < messages.length; offset += 50) {
        const batch = messages.slice(offset, offset + 50)
        yield* sql`INSERT INTO effect_local_peer_relay_delivery_messages ${sql.insert(batch)}`
        yield* sql`INSERT INTO effect_local_peer_relay_delivery_changes ${
          sql.insert(batch.map((message) => ({
            replica_incarnation: permit.incarnation,
            relay_message_id: message.relay_message_id,
            change_hash: changeHash
          })))
        }`
      }

      const delivery = yield* deliveries.lookup(commandId)
      assert.strictEqual(delivery._tag, "TrackedCommand")
      if (delivery._tag !== "TrackedCommand") return
      assert.strictEqual(delivery.destinations.length, 257)
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("lets a late relay ack replace an expired unconfirmed state", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000022")
      const payload = yield* makePayload(22)
      const localChangeCount = yield* trackCommand(commandId, payload)
      const entry = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 1_000
      })

      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* outbox.pruneExpired, 1)
      const expired = yield* deliveries.lookup(commandId)
      assert.strictEqual(expired._tag, "TrackedCommand")
      if (expired._tag !== "TrackedCommand") return
      assert.strictEqual(
        expired.destinations[0]?.state._tag,
        "RelayCustodyUnconfirmedAtDeadline"
      )

      yield* outbox.markCustody({
        relayMessageId: entry.relayMessageId,
        outerEnvelopeDigest: entry.outerEnvelopeDigest
      })
      const accepted = yield* deliveries.lookup(commandId)
      assert.strictEqual(accepted._tag, "TrackedCommand")
      if (accepted._tag !== "TrackedCommand") return
      const state = accepted.destinations[0]?.state
      assert.strictEqual(state?._tag, "RelayCustodyAccepted")
      if (state?._tag !== "RelayCustodyAccepted") return
      assert.strictEqual(state.acceptedChangeCount, localChangeCount)
      assert.isDefined(state.senderCustodyUnconfirmedAt)
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("completes a retried source after relay custody was already accepted", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const payload = yield* makePayload(23)
      const first = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      assert.strictEqual(first._tag, "PendingRelayCustody")
      yield* outbox.markCustody({
        relayMessageId: first.relayMessageId,
        outerEnvelopeDigest: first.outerEnvelopeDigest
      })

      const retried = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      assert.strictEqual(retried._tag, "RelayCustodyAccepted")
      assert.strictEqual(retried.relayMessageId, first.relayMessageId)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 0, encodedBytes: 0 },
        replica: { messageCount: 0, encodedBytes: 0 }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("completes a retried source after its custody deadline expired", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const payload = yield* makePayload(28)
      const first = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 1_000
      })
      assert.strictEqual(first._tag, "PendingRelayCustody")
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* outbox.pruneExpired, 1)

      const retried = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 1_000
      })
      assert.strictEqual(retried._tag, "RelayCustodyUnconfirmedAtDeadline")
      assert.strictEqual(retried.relayMessageId, first.relayMessageId)
      assert.deepStrictEqual(yield* outbox.usage(endpoint), {
        remote: { messageCount: 0, encodedBytes: 0 },
        replica: { messageCount: 0, encodedBytes: 0 }
      })
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("aggregates the latest unconfirmed observation and deadline independently", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000024")
      const firstPayload = yield* makePayload(24)
      const secondPayload = yield* makePayload(25)
      yield* trackCommand(commandId, firstPayload)
      const first = yield* outbox.admit({
        ...endpoint,
        payload: firstPayload,
        retryHorizonMillis: 20_000
      })
      assert.strictEqual(first._tag, "PendingRelayCustody")
      if (first._tag !== "PendingRelayCustody") return
      yield* outbox.admit({
        ...endpoint,
        payload: secondPayload,
        retryHorizonMillis: 10_000
      })

      yield* TestClock.adjust("20 seconds")
      assert.strictEqual(yield* outbox.pruneExpired, 1)
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* outbox.pruneExpired, 1)

      const delivery = yield* deliveries.lookup(commandId)
      assert.strictEqual(delivery._tag, "TrackedCommand")
      if (delivery._tag !== "TrackedCommand") return
      const state = delivery.destinations[0]?.state
      assert.strictEqual(state?._tag, "RelayCustodyUnconfirmedAtDeadline")
      if (state?._tag !== "RelayCustodyUnconfirmedAtDeadline") return
      assert.strictEqual(DateTime.formatIso(state.deadline), first.retryDeadline)
    }).pipe(
      Effect.provide(layer(":memory:", {
        ...outboxLimits,
        pruneBatchSize: 1
      }))
    ))

  it.effect("fails closed when replayed delivery evidence conflicts with the payload", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const sql = yield* SqlClient.SqlClient
      const payload = yield* makePayload(26)
      yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      yield* sql`UPDATE effect_local_peer_relay_delivery_changes
        SET change_hash = ${"f".repeat(64)}`

      const error = yield* Effect.flip(outbox.dueForEndpoint({
        ...endpoint,
        maximum: 1
      }))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("rejects incomplete writer provenance before durable admission", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const sql = yield* SqlClient.SqlClient
      const valid = yield* makePayload(29)
      const envelope = yield* PeerSyncEnvelope.decodeSyncEnvelope(valid, replicaLimits)
      const payload = yield* PeerSyncEnvelope.encodeSyncEnvelope(
        PeerSyncEnvelope.SyncEnvelope.make({
          ...envelope,
          writerProvenance: []
        })
      )

      const error = yield* Effect.flip(outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      }))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      const rows = yield* sql`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox) AS outbox,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_remote_usage) AS remote_usage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_outbox_replica_usage) AS replica_usage,
        (SELECT COUNT(*) FROM effect_local_peer_relay_delivery_messages) AS messages,
        (SELECT COUNT(*) FROM effect_local_peer_relay_delivery_changes) AS changes`
      assert.deepStrictEqual(rows, [{
        outbox: 0,
        remote_usage: 0,
        replica_usage: 0,
        messages: 0,
        changes: 0
      }])
    }).pipe(Effect.provide(layer(":memory:"))))

  it.effect("rolls acceptance back when its durable event cannot be written", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const sql = yield* SqlClient.SqlClient
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000027")
      const payload = yield* makePayload(27)
      yield* trackCommand(commandId, payload)
      const entry = yield* outbox.admit({
        ...endpoint,
        payload,
        retryHorizonMillis: 30_000
      })
      assert.strictEqual(entry._tag, "PendingRelayCustody")
      if (entry._tag !== "PendingRelayCustody") return
      yield* sql`CREATE TRIGGER fail_delivery_event
        BEFORE INSERT ON effect_local_command_delivery_events
        BEGIN
          SELECT RAISE(ABORT, 'forced delivery event failure');
        END`

      const exit = yield* Effect.exit(deliveries.markAccepted(
        entry.replicaIncarnation,
        entry.relayMessageId,
        entry.outerEnvelopeDigest,
        "2026-01-01T00:00:00.000Z"
      ))
      assert.strictEqual(exit._tag, "Failure")
      assert.deepStrictEqual(
        yield* sql`SELECT relay_custody_accepted_at
          FROM effect_local_peer_relay_delivery_messages
          WHERE relay_message_id = ${entry.relayMessageId}`,
        [{ relay_custody_accepted_at: null }]
      )
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

  it.effect("loads replay rows in bounded batches instead of one query per row", () =>
    Effect.gen(function*() {
      yield* insertDocument
      const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
      for (let sequence = 1; sequence <= 8; sequence++) {
        yield* outbox.admit({
          ...endpoint,
          payload: yield* makePayload(sequence),
          retryHorizonMillis: 30_000
        })
      }

      const spans: Array<Tracer.NativeSpan> = []
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options)
          if (options.name === "sql.execute") spans.push(span)
          return span
        }
      })
      const entries = yield* outbox.dueForEndpoint({ ...endpoint, maximum: 8 }).pipe(
        Effect.provideService(Tracer.Tracer, tracer)
      )

      assert.strictEqual(entries.length, 8)
      // The span budget is the N+1 guard: one metadata read, one batched row read, and one
      // batched evidence read per delivery table. One query per replayed row would need 3 * 8.
      assert.isAtMost(spans.length, 4)
    }).pipe(Effect.provide(layer(":memory:", {
      ...outboxLimits,
      maxMessagesPerRemote: 8,
      maxMessagesPerReplica: 8
    }))))

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
