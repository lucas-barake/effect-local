import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as PeerSession from "../src/PeerSession.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

it.layer(NodeCrypto.layer)("PeerSession coverage", (it) => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const limits = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 32
  } satisfies ReplicaLimits.Values
  const permit = {
    replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001"),
    incarnation: Identity.ReplicaIncarnation.make(1),
    writerGeneration: Identity.WriterGeneration.make(2),
    definitionHash: "hash"
  }
  const gate = ReplicaGate.ReplicaGate.of({
    current: Effect.succeed(permit),
    shared: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
    claim: (use) => use(permit),
    refresh: Effect.succeed(permit),
    validate: () => Effect.void
  })
  const result = {
    reply: null,
    heads: [],
    acceptedHeads: [],
    commitSequence: Identity.CommitSequence.make(1),
    observedByPeer: true,
    durableConfirmation: false as const,
    duplicate: false
  }
  const makeSync = (incarnation: Identity.ReplicaIncarnation) =>
    PeerSync.PeerSync.of({
      open: (id) => Effect.succeed({ peerId: id, connectionEpoch: "local-epoch", replicaIncarnation: incarnation }),
      reset: () => Effect.void,
      generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
      receive: () => Effect.succeed(result),
      enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0, writerProvenance: [] }),
      pending: () => Effect.succeed([]),
      markSent: () => Effect.succeed(true)
    })
  const makeScopedTransport = (peerId: Identity.PeerId, closed: Ref.Ref<number>) =>
    PeerTransport.PeerTransport.of({
      capabilities: { storeAndForward: false },
      connect: () =>
        Effect.acquireRelease(
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false } as const,
            receive: Stream.never,
            send: () => Effect.void,
            close: Ref.update(closed, (count) => count + 1)
          }),
          () => Ref.update(closed, (count) => count + 1)
        )
    })
  const publisher = CommitPublisher.CommitPublisher.of({
    publishPending: Effect.succeed(0),
    invalidate: () => Effect.void,
    subscribe: Effect.succeed({
      watermark: Identity.CommitSequence.make(0),
      refreshGeneration: 0,
      events: Stream.never
    })
  })
  const provide = <A, E, R,>(
    effect: Effect.Effect<A, E, R>,
    sync: PeerSync.PeerSync["Service"],
    transport: PeerTransport.PeerTransport["Service"]
  ) =>
    effect.pipe(
      Effect.provideService(PeerTransport.PeerTransport, transport),
      Effect.provideService(PeerSync.PeerSync, sync),
      Effect.provideService(ReplicaGate.ReplicaGate, gate),
      Effect.provideService(CommitPublisher.CommitPublisher, publisher),
      Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
    )

  it.effect("rejects duplicate selected documents", () =>
    Effect.gen(function*() {
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const closed = yield* Ref.make(0)
      const sync = makeSync(permit.incarnation)
      const transport = makeScopedTransport(peerId, closed)
      const exit = yield* Effect.exit(Effect.scoped(
        provide(
          PeerSession.makeTestClient(
            { peerId, documents: [{ document: Task, documentId }, { document: Task, documentId }] },
            () => Effect.die("unexpected entity request")
          ),
          sync,
          transport
        )
      ))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(exit.cause))
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        if (error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(error.reason.expected, "unique selected documents")
          assert.strictEqual(error.reason.observed, "2")
        }
      }
      assert.strictEqual(yield* Ref.get(closed), 0)
    }))

  it.effect("fails when the opened session incarnation does not match the gate permit", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const closed = yield* Ref.make(0)
      const sync = makeSync(Identity.ReplicaIncarnation.make(2))
      const transport = makeScopedTransport(peerId, closed)
      const exit = yield* Effect.exit(Effect.scoped(
        provide(
          PeerSession.makeTestClient({ peerId, documents: [] }, () => Effect.die("unexpected entity request")),
          sync,
          transport
        )
      ))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(exit.cause))
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        if (error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(error.reason.expected, "1")
          assert.strictEqual(error.reason.observed, "2")
        }
      }
      assert.isAbove(yield* Ref.get(closed), 0)
    }))

  it.effect("fails the session when marking an unselected document dirty", () =>
    Effect.scoped(Effect.gen(function*() {
      const documentId = yield* Identity.makeDocumentId
      const otherDocumentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const closed = yield* Ref.make(0)
      const sync = makeSync(permit.incarnation)
      const transport = makeScopedTransport(peerId, closed)
      const session = yield* provide(
        PeerSession.makeTestClient({ peerId, documents: [{ document: Task, documentId }] }, () =>
          Effect.die("unexpected entity request")),
        sync,
        transport
      )
      const exit = yield* Effect.exit(session.markDirty(otherDocumentId))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const error = Option.getOrThrow(Cause.findErrorOption(exit.cause))
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        if (error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(error.reason.expected, "selected document")
          assert.strictEqual(error.reason.observed, otherDocumentId)
        }
      }
      const supervised = session as PeerSession.SupervisedPeerSession
      const disconnect = yield* Effect.exit(supervised.awaitDisconnect)
      assert.strictEqual(disconnect._tag, "Failure")
      const flushed = yield* Effect.exit(session.flush)
      assert.strictEqual(flushed._tag, "Failure")
    })))
})
