import * as Automerge from "@automerge/automerge"
import * as NodeCrypto from "@effect/platform-node/NodeCrypto"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as PeerSyncEnvelope from "../src/PeerSyncEnvelope.js"

const limits: PeerSyncEnvelope.SyncEnvelopeLimits = {
  maxSyncMessageBytes: 64 * 1_024,
  maxSyncChangesPerMessage: 100,
  maxSyncDependencyEdgesPerMessage: 1_000,
  maxSyncOperationsPerMessage: 10_000
}

const rewrittenLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000011")

const makeSyncEnvelope = Effect.gen(function*() {
  let source = Automerge.from(
    { value: { title: "one" }, tombstone: false },
    { actor: "a".repeat(32) }
  )
  const remote = Automerge.init()
  const handshake = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
  const received = Automerge.receiveSyncMessage(source, Automerge.initSyncState(), handshake)
  source = received[0]
  const message = Automerge.generateSyncMessage(source, received[1])[1]!
  const changes = Automerge.getAllChanges(source).map((bytes) => Automerge.decodeChange(bytes))
  const writerProvenance = changes.map((change) => ({
    changeHash: change.hash,
    writerSchemaVersion: 1,
    writerDefinitionHash: "definition-1"
  }))
  const envelope = PeerSyncEnvelope.SyncEnvelope.make({
    connectionEpoch: "epoch-1",
    sequence: 1,
    documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
    documentType: "Task",
    messageHash: yield* Canonical.digest(message),
    message,
    lineage: rewrittenLineage,
    writerProvenance
  })
  Automerge.free(source)
  Automerge.free(remote)
  return envelope
})

describe("PeerSyncEnvelope", () => {
  it.effect("round trips and validates a bounded Automerge sync envelope", () =>
    Effect.gen(function*() {
      const envelope = yield* makeSyncEnvelope
      const bytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(envelope)
      const decoded = yield* PeerSyncEnvelope.decodeSyncEnvelope(bytes, limits)
      assert.deepStrictEqual(decoded, envelope)
      assert.strictEqual(decoded.lineage, rewrittenLineage)
      assert.deepStrictEqual(PeerSyncEnvelope.syncEnvelopeDocument(decoded), {
        documentId: envelope.documentId,
        documentType: "Task"
      })
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("accepts an opaque standard V2 Document sync envelope without semantic expansion", () =>
    Effect.gen(function*() {
      const envelope = yield* makeSyncEnvelope
      const chunks = Automerge.decodeSyncMessage(envelope.message).changes
      assert.strictEqual(chunks.length, 1)
      assert.throws(() => Automerge.decodeChange(chunks[0]!))
      const document = Automerge.load(chunks[0]!)
      try {
        assert.strictEqual(Automerge.getAllChanges(document).length, 1)
      } finally {
        Automerge.free(document)
      }
      const bytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(envelope)
      const decoded = yield* PeerSyncEnvelope.decodeSyncEnvelope(bytes, {
        ...limits,
        maxSyncOperationsPerMessage: 0
      })
      assert.deepStrictEqual(decoded, envelope)
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.validateSyncEnvelope({
          ...envelope,
          writerProvenance: []
        }, limits),
        {
          ...envelope,
          writerProvenance: []
        }
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects oversized, malformed, conflicting, and excess-provenance payloads", () =>
    Effect.gen(function*() {
      const envelope = yield* makeSyncEnvelope
      const bytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(envelope)
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.decodeSyncEnvelope(bytes, {
          ...limits,
          maxSyncMessageBytes: 1
        })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.decodeSyncEnvelope(Uint8Array.of(1, 2, 3), limits)))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.validateSyncEnvelope({
          ...envelope,
          messageHash: "f".repeat(64)
        }, limits)))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.validateSyncEnvelope({
          ...envelope,
          writerProvenance: Array.from(
            { length: limits.maxSyncChangesPerMessage + 1 },
            () => envelope.writerProvenance[0]!
          )
        }, limits)))._tag,
        "Failure"
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))
})

describe("RelayOuterEnvelope", () => {
  const base = PeerSyncEnvelope.RelayOuterEnvelope.make({
    domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
    version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
    expectedLocal: {
      tenantId: "tenant-a",
      subjectId: "subject-a",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
    },
    remote: {
      tenantId: "tenant-a",
      subjectId: "subject-b",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
    },
    relayPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003"),
    relayMessageId: Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000004"),
    protocolVersion: 3,
    payloadVersion: 1,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(2),
    senderConnectionEpoch: "epoch-7",
    senderSequence: 11,
    document: {
      documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000005"),
      documentType: "Task"
    },
    lineage: rewrittenLineage,
    writerProvenance: [
      {
        changeHash: "b".repeat(64),
        writerSchemaVersion: 2,
        writerDefinitionHash: "definition-b"
      },
      {
        changeHash: "a".repeat(64),
        writerSchemaVersion: 1,
        writerDefinitionHash: "definition-a"
      }
    ],
    messageHash: "c".repeat(64),
    payload: Uint8Array.of(1, 2, 3, 4)
  })

  it.effect("has one stable canonical byte and digest vector", () =>
    Effect.gen(function*() {
      const relayAdmission = PeerSyncEnvelope.RelayOuterEnvelope.make({
        ...base,
        expectedLocal: { ...base.expectedLocal },
        remote: { ...base.remote },
        document: { ...base.document },
        writerProvenance: [...base.writerProvenance],
        payload: base.payload.slice()
      })
      const recipientReplay = PeerSyncEnvelope.RelayOuterEnvelope.make({
        ...base,
        expectedLocal: { ...base.expectedLocal },
        remote: { ...base.remote },
        document: { ...base.document },
        writerProvenance: base.writerProvenance.toReversed()
      })
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.encodeRelayOuterEnvelope(base),
        yield* PeerSyncEnvelope.encodeRelayOuterEnvelope(relayAdmission)
      )
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.encodeRelayOuterEnvelope(base),
        yield* PeerSyncEnvelope.encodeRelayOuterEnvelope(recipientReplay)
      )
      const digest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(base)
      assert.strictEqual(digest, "bf6947413c2a682c4a99718e168954df367d445fdbb5524050df97f39cfe1db0")
      assert.strictEqual(
        yield* PeerSyncEnvelope.digestRelayOuterEnvelope(relayAdmission),
        digest
      )
      assert.strictEqual(
        yield* PeerSyncEnvelope.digestRelayOuterEnvelope(recipientReplay),
        digest
      )
      assert.deepStrictEqual(PeerSyncEnvelope.relayOuterEnvelopeDocument(base), base.document)
      assert.strictEqual(
        (yield* Effect.exit(
          Schema.decodeUnknownEffect(PeerSyncEnvelope.RelayOuterEnvelope)({
            ...base,
            domain: "unrelated-domain"
          })
        ))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(
          Schema.decodeUnknownEffect(PeerSyncEnvelope.RelayOuterEnvelope)({
            ...base,
            version: 2
          })
        ))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(
          Schema.decodeUnknownEffect(PeerSyncEnvelope.RelayOuterEnvelope)({
            ...base,
            protocolVersion: 4
          })
        ))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(
          Schema.decodeUnknownEffect(PeerSyncEnvelope.RelayOuterEnvelope)({
            ...base,
            payloadVersion: 2
          })
        ))._tag,
        "Failure"
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("binds every mutable outer envelope field into the digest", () =>
    Effect.gen(function*() {
      const expected = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(base)
      const mutations: ReadonlyArray<PeerSyncEnvelope.RelayOuterEnvelope> = [
        { ...base, expectedLocal: { ...base.expectedLocal, tenantId: "tenant-b" } },
        { ...base, expectedLocal: { ...base.expectedLocal, subjectId: "subject-z" } },
        {
          ...base,
          expectedLocal: {
            ...base.expectedLocal,
            peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000006")
          }
        },
        { ...base, remote: { ...base.remote, tenantId: "tenant-b" } },
        { ...base, remote: { ...base.remote, subjectId: "subject-c" } },
        {
          ...base,
          remote: {
            ...base.remote,
            peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000007")
          }
        },
        {
          ...base,
          relayPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000008")
        },
        {
          ...base,
          relayMessageId: Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000009")
        },
        { ...base, senderReplicaIncarnation: Identity.ReplicaIncarnation.make(3) },
        { ...base, senderConnectionEpoch: "epoch-8" },
        { ...base, senderSequence: 12 },
        { ...base, document: { ...base.document, documentType: "Note" } },
        {
          ...base,
          document: {
            ...base.document,
            documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000010")
          }
        },
        {
          ...base,
          lineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000012")
        },
        {
          ...base,
          writerProvenance: [{
            ...base.writerProvenance[0]!,
            writerSchemaVersion: 3
          }, base.writerProvenance[1]!]
        },
        {
          ...base,
          writerProvenance: [{
            ...base.writerProvenance[0]!,
            changeHash: "e".repeat(64)
          }, base.writerProvenance[1]!]
        },
        {
          ...base,
          writerProvenance: [{
            ...base.writerProvenance[0]!,
            writerDefinitionHash: "definition-c"
          }, base.writerProvenance[1]!]
        },
        { ...base, messageHash: "d".repeat(64) },
        { ...base, payload: Uint8Array.of(1, 2, 3, 5) }
      ]
      for (const mutation of mutations) {
        assert.notStrictEqual(yield* PeerSyncEnvelope.digestRelayOuterEnvelope(mutation), expected)
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))
})
