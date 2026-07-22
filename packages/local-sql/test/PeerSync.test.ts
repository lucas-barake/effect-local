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
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
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
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

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
    maxQueuedRpc: 100
  }
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Base))
  const Limits = ReplicaLimits.layer(limits)
  const Infrastructure = Layer.mergeAll(Base, Gate, Limits)
  const StoreService = DocumentStore.layer.pipe(Layer.provide(Infrastructure))
  const Services = Layer.merge(Infrastructure, StoreService)
  const SyncService = PeerSync.layer.pipe(Layer.provide(Services))
  const TestLayer = Layer.merge(Services, SyncService)
  const StrictLimits = ReplicaLimits.layer({ ...limits, maxSyncOperationsPerMessage: 1 })
  const StrictInfrastructure = Layer.mergeAll(Base, Gate, StrictLimits)
  const StrictStoreService = DocumentStore.layer.pipe(Layer.provide(StrictInfrastructure))
  const StrictServices = Layer.merge(StrictInfrastructure, StrictStoreService)
  const StrictSyncService = PeerSync.layer.pipe(Layer.provide(StrictServices))
  const StrictLayer = Layer.merge(StrictServices, StrictSyncService)
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
  const EdgeServices = Layer.merge(EdgeInfrastructure, EdgeStoreService)
  const EdgeSyncService = PeerSync.layer.pipe(Layer.provide(EdgeServices))
  const EdgeLayer = Layer.merge(EdgeServices, EdgeSyncService)
  const ReceiptLimits = ReplicaLimits.layer({ ...limits, maxPendingChangesPerPeer: 2 })
  const ReceiptInfrastructure = Layer.mergeAll(Base, Gate, ReceiptLimits)
  const ReceiptStoreService = DocumentStore.layer.pipe(Layer.provide(ReceiptInfrastructure))
  const ReceiptServices = Layer.merge(ReceiptInfrastructure, ReceiptStoreService)
  const ReceiptSyncService = PeerSync.layer.pipe(Layer.provide(ReceiptServices))
  const ReceiptLayer = Layer.merge(ReceiptServices, ReceiptSyncService)

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
        message: generated[1]!
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
          message: nextGenerated[1]
        })
      }
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "remote", labels: ["synced"] })
      const duplicate = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        message: generated[1]!
      })
      assert.isTrue(duplicate.duplicate)
      assert.deepStrictEqual(duplicate.reply?.message, received.reply?.message)
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`
      assert.strictEqual(rows[0]?.count, 2)
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(remote)
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
        message
      })
      assert.isNotNull(first.reply)
      const firstOutbound = yield* sync.enqueue(firstSession, first.reply!)
      const secondSession = yield* sync.open(peerId)
      const replayed = yield* sync.receive(Task, documentId, secondSession, {
        remoteConnectionEpoch: "stable-remote-epoch",
        receiveSequence: 0,
        message
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
        message
      })
      const reused = yield* Effect.exit(sync.receive(Task, secondDocumentId, session, {
        remoteConnectionEpoch: "remote-epoch",
        receiveSequence: 0,
        message
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
        message
      })
      const before = yield* sql<{ readonly receipts: number; readonly changes: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes`
      const altered = Uint8Array.from(message)
      altered[altered.length - 1] = altered[altered.length - 1]! ^ 1
      const exit = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 0,
        message: altered
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
          message
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

  it.effect("retains replay receipts and rejects a session that exhausts its receipt quota", () =>
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
        const received = yield* sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: sequence,
          message
        })
        if (received.reply !== null) {
          const outbound = yield* sync.enqueue(session, received.reply)
          yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
        }
      }
      const exhausted = yield* Effect.exit(sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: 2,
        message
      }))
      assert.strictEqual(exhausted._tag, "Failure")
      if (exhausted._tag === "Failure") {
        assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exhausted.cause)).reason._tag, "QuotaExceeded")
      }
      const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_peer_receipts
        WHERE peer_id = ${session.peerId} AND connection_epoch = ${session.connectionEpoch}`
      assert.strictEqual(rows[0]?.count, 2)
      InternalAutomerge.free(remote)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(ReceiptLayer)))

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
          message: new Uint8Array(limits.maxSyncMessageBytes + 1)
        })
      )
      assert.strictEqual(exit._tag, "Failure")
      assert.strictEqual(
        (yield* Effect.exit(sync.receive(Task, documentId, session, {
          remoteConnectionEpoch: session.connectionEpoch,
          receiveSequence: 0,
          message: new Uint8Array([1, 2, 3])
        })))._tag,
        "Failure"
      )
      const receipts = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_peer_receipts`
      assert.strictEqual(receipts[0]?.count, 0)
      const first = yield* sync.generate(Task, documentId, session)
      assert.isNotNull(first.outbound)
      const blocked = yield* sync.generate(Task, documentId, session)
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
      const restarted = yield* sync.generate(Task, documentId, reconnected)
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
      const generated = yield* sync.generate(Task, documentId, session)
      assert.isNotNull(generated.outbound)
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads,
        accepted_heads, commit_sequence, accepted_at
      ) VALUES (
        ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, 0, ${documentId},
        'receipt', NULL, NULL, NULL, ${JSON.stringify(created.materializedHeads)},
        ${JSON.stringify(created.acceptedHeads)}, ${created.commitSequence}, ${new Date(0).toISOString()}
      )`
      yield* Effect.scoped(
        Effect.gen(function*() {
          const restarted = yield* PeerSync.PeerSync
          const pending = yield* restarted.pending(session)
          assert.strictEqual(pending.length, 1)
          assert.strictEqual(pending[0]?.messageHash, generated.outbound?.messageHash)
          const rows = yield* sql<{ readonly outbox: number; readonly receipts: number }>`SELECT
            (SELECT COUNT(*) FROM effect_local_peer_outbox) AS outbox,
            (SELECT COUNT(*) FROM effect_local_peer_receipts) AS receipts`
          assert.deepStrictEqual(rows, [{ outbox: 1, receipts: 1 }])
        }).pipe(Effect.provide(PeerSync.layer))
      )
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
          message: firstMessage
        }
        const first = yield* sync.receive(Task, firstDocumentId, firstSession, firstInput).pipe(Effect.forkChild)
        yield* Deferred.await(firstStarted)
        const same = yield* sync.receive(Task, firstDocumentId, secondSession, firstInput).pipe(Effect.forkChild)
        const independent = yield* sync.receive(Task, secondDocumentId, secondSession, {
          remoteConnectionEpoch: "second remote",
          receiveSequence: 0,
          message: secondMessage
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
        const input = { remoteConnectionEpoch: "remote", receiveSequence: 0, message }
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
          message: generated[1]
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
        message
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
          message: outbound[1]
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
      const first = Automerge.generateSyncMessage(remote, remoteState)
      remoteState = first[0]
      assert.isNotNull(first[1])
      remote = Automerge.change(remote, (draft) => {
        ;(draft.value as unknown as { labels: Array<string> }).labels.push("after")
      })
      const second = Automerge.generateSyncMessage(remote, remoteState)
      assert.isNotNull(second[1])
      const pending = yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        message: second[1]!
      })
      assert.isFalse(sameHeadsForTest(pending.heads, Automerge.getHeads(remote)))
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        message: first[1]!
      })
      const converged = yield* store.load(Task, documentId)
      assert.deepStrictEqual(converged.snapshot.value, { title: "two", labels: ["after"] })
      assert.strictEqual(converged.snapshot.projection, "Blocked")
      yield* sync.reset(session)
      const reconnected = yield* sync.open(peerId)
      assert.notStrictEqual(reconnected.connectionEpoch, session.connectionEpoch)
      const restarted = yield* sync.generate(Task, documentId, reconnected)
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
          message: outbound[1]
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
        message: second[1]!
      })
      assert.isFalse(sameHeadsForTest(pending.heads, Automerge.getHeads(remote)))
      yield* sync.receive(Task, documentId, session, {
        remoteConnectionEpoch: session.connectionEpoch,
        receiveSequence: sequence++,
        message: first[1]!
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
      const base = Layer.merge(database, bootstrap)
      const gate = ReplicaGate.layer.pipe(Layer.provide(base))
      const infrastructure = Layer.mergeAll(base, gate, Limits)
      const store = DocumentStore.layer.pipe(Layer.provide(infrastructure))
      const services = Layer.merge(infrastructure, store)
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
            message: outbound[1]
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
          message: outbound[1]!
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
          message
        }))
        assert.strictEqual(conflicts, 9)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure._tag, "ReplicaError")
          assert.strictEqual(result.failure.reason._tag, "StorageUnavailable")
          if (result.failure.reason._tag === "StorageUnavailable") {
            assert.strictEqual(result.failure.reason.cause._tag, "SqlCause")
            if (result.failure.reason.cause._tag === "SqlCause") {
              assert.strictEqual(result.failure.reason.cause.code, "CONCURRENT_DOCUMENT_WRITE")
            }
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
          message
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
})

const sameHeadsForTest = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  JSON.stringify([...left].toSorted()) === JSON.stringify([...right].toSorted())
