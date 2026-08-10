import * as Automerge from "@automerge/automerge"
import * as NodeCrypto from "@effect/platform-node/NodeCrypto"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as CheckpointAuthority from "../src/CheckpointAuthority.js"
import * as PeerSyncEnvelope from "../src/PeerSyncEnvelope.js"

const limits: PeerSyncEnvelope.SyncEnvelopeLimits = {
  maxSyncMessageBytes: 64 * 1_024,
  maxSyncChangesPerMessage: 100,
  maxSyncDependencyEdgesPerMessage: 1_000,
  maxSyncOperationsPerMessage: 10_000
}

const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const rewrittenLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000011")
const priorLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000012")

const checkpointTransfer = PeerSyncEnvelope.CheckpointTransfer.make({
  snapshot: Uint8Array.of(1, 2, 3),
  manifest: {
    purpose: CheckpointAuthority.manifestPurpose,
    documentId,
    lineage: rewrittenLineage,
    checkpointHash: "a".repeat(64),
    heads: Conflict.Heads.make(["z", "a", "z"]),
    base: { _tag: "Heads", baseHeads: Conflict.Heads.make(["d", "b", "d"]) },
    schemaVersion: 1,
    writerDefinitionHash: "definition-1",
    authorization: Uint8Array.of(4, 5)
  },
  transitions: [{
    purpose: CheckpointAuthority.transitionPurpose,
    documentId,
    priorLineage,
    priorCheckpointHash: "b".repeat(64),
    priorHeads: Conflict.Heads.make(["c", "a", "c"]),
    resultingLineage: rewrittenLineage,
    anchorCheckpointHash: "a".repeat(64),
    resultingHeads: Conflict.Heads.make(["z", "a", "z"]),
    schemaVersion: 1,
    writerDefinitionHash: "definition-1",
    priorSnapshot: Uint8Array.of(8, 9),
    authorization: Uint8Array.of(6, 7)
  }]
})

const encodedCheckpointTransfer = PeerSyncEnvelope.encodeCheckpointTransfer(
  checkpointTransfer,
  limits.maxSyncMessageBytes
)

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
    documentId,
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
  it.effect("round trips checkpoint transfers with canonical heads", () =>
    Effect.gen(function*() {
      const bytes = yield* encodedCheckpointTransfer
      const decoded = yield* PeerSyncEnvelope.decodeCheckpointTransfer(
        bytes,
        limits.maxSyncMessageBytes
      )
      assert.deepStrictEqual(decoded.manifest.heads, ["a", "z"])
      assert.deepStrictEqual(decoded.manifest.base, { _tag: "Heads", baseHeads: ["b", "d"] })
      assert.deepStrictEqual(decoded.transitions[0].priorHeads, ["a", "c"])
      assert.deepStrictEqual(decoded.transitions[0].resultingHeads, ["a", "z"])
      assert.deepStrictEqual(decoded.snapshot, checkpointTransfer.snapshot)
      assert.deepStrictEqual(decoded.transitions[0].priorSnapshot, Uint8Array.of(8, 9))
    }))

  it.effect("preserves legacy envelopes without a checkpoint transfer", () =>
    Effect.gen(function*() {
      const envelope = yield* makeSyncEnvelope
      const bytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(envelope)
      const json = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(new TextDecoder().decode(bytes))
      assert.isFalse(Object.hasOwn(json, "checkpointTransfer"))
      const decoded = yield* PeerSyncEnvelope.decodeSyncEnvelope(bytes, limits)
      assert.isFalse(Object.hasOwn(decoded, "checkpointTransfer"))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("admits maximum ordinary and checkpoint payload envelopes", () =>
    Effect.gen(function*() {
      const maximumHeader = {
        connectionEpoch: "x".repeat(256),
        sequence: Number.MAX_SAFE_INTEGER,
        documentId,
        documentType: "x".repeat(256),
        lineage: rewrittenLineage
      }
      const ordinaryMessage = new Uint8Array(limits.maxSyncMessageBytes)
      const ordinaryEnvelope = PeerSyncEnvelope.SyncEnvelope.make({
        ...maximumHeader,
        messageHash: yield* Canonical.digest(ordinaryMessage),
        message: ordinaryMessage,
        writerProvenance: Array.from(
          { length: limits.maxSyncChangesPerMessage },
          (_, index) => ({
            changeHash: index.toString(16).padStart(64, "0"),
            writerSchemaVersion: Number.MAX_SAFE_INTEGER,
            writerDefinitionHash: "x".repeat(256)
          })
        )
      })
      const ordinaryBytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(ordinaryEnvelope)
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.decodeSyncEnvelope(ordinaryBytes, limits),
        ordinaryEnvelope
      )

      const emptyMessage = new Uint8Array()
      const checkpointEnvelope = PeerSyncEnvelope.SyncEnvelope.make({
        ...maximumHeader,
        messageHash: yield* Canonical.digest(emptyMessage),
        message: emptyMessage,
        writerProvenance: [],
        checkpointTransfer: new Uint8Array(limits.maxSyncMessageBytes)
      })
      const checkpointBytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(checkpointEnvelope)
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.decodeSyncEnvelope(checkpointBytes, limits),
        checkpointEnvelope
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects one byte over the mutually exclusive envelope admission bound", () =>
    Effect.gen(function*() {
      const ordinaryPayloadBudget = limits.maxSyncMessageBytes * 2 + limits.maxSyncChangesPerMessage * 512
      const checkpointPayloadBudget = Math.ceil(limits.maxSyncMessageBytes / 3) * 4
      const expectedAdmissionBound = Math.max(
        ordinaryPayloadBudget,
        checkpointPayloadBudget
      ) + 4_096
      const error = yield* Effect.flip(
        PeerSyncEnvelope.decodeSyncEnvelope(
          new Uint8Array(expectedAdmissionBound + 1),
          limits
        )
      )
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.observed, "oversized sync envelope")
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("bounds checkpoint transfer bytes, transitions, and authorization tokens", () =>
    Effect.gen(function*() {
      const encoded = yield* encodedCheckpointTransfer
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.encodeCheckpointTransfer(
          checkpointTransfer,
          encoded.byteLength - 1
        )))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(PeerSyncEnvelope.decodeCheckpointTransfer(
          encoded,
          encoded.byteLength - 1
        )))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(
          Schema.decodeUnknownEffect(PeerSyncEnvelope.CheckpointTransfer)({
            ...checkpointTransfer,
            transitions: Array.from(
              { length: PeerSyncEnvelope.maximumCheckpointTransitions + 1 },
              () => checkpointTransfer.transitions[0]
            )
          })
        ))._tag,
        "Failure"
      )
      for (
        const authorization of [
          new Uint8Array(),
          new Uint8Array(CheckpointAuthority.maximumAuthorizationTokenBytes + 1)
        ]
      ) {
        assert.strictEqual(
          (yield* Effect.exit(
            Schema.decodeUnknownEffect(PeerSyncEnvelope.CheckpointTransfer)({
              ...checkpointTransfer,
              manifest: { ...checkpointTransfer.manifest, authorization }
            })
          ))._tag,
          "Failure"
        )
      }
      const encodedJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(new TextDecoder().decode(encoded))
      for (
        const malformed of [
          {
            ...encodedJson,
            manifest: { ...encodedJson.manifest, authorization: "not base64!" }
          },
          {
            ...encodedJson,
            transitions: [{ ...encodedJson.transitions[0], authorization: "not base64!" }]
          }
        ]
      ) {
        assert.strictEqual(
          (yield* Effect.exit(PeerSyncEnvelope.decodeCheckpointTransfer(
            new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(malformed)),
            limits.maxSyncMessageBytes
          )))._tag,
          "Failure"
        )
      }
    }))

  it.effect("requires checkpoint envelopes to carry only the hash-bound empty message", () =>
    Effect.gen(function*() {
      const transferBytes = yield* encodedCheckpointTransfer
      const emptyMessage = new Uint8Array()
      const base = PeerSyncEnvelope.SyncEnvelope.make({
        connectionEpoch: "epoch-1",
        sequence: 2,
        documentId,
        documentType: "Task",
        messageHash: yield* Canonical.digest(emptyMessage),
        message: emptyMessage,
        lineage: rewrittenLineage,
        writerProvenance: [],
        checkpointTransfer: transferBytes
      })
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.validateSyncEnvelope(base, limits),
        base
      )
      const maximumTransferEnvelope = {
        ...base,
        checkpointTransfer: new Uint8Array(limits.maxSyncMessageBytes)
      }
      const maximumTransferEnvelopeBytes = yield* PeerSyncEnvelope.encodeSyncEnvelope(
        maximumTransferEnvelope
      )
      assert.isAtMost(
        maximumTransferEnvelopeBytes.byteLength,
        PeerSyncEnvelope.maximumSyncEnvelopeBytes(
          limits.maxSyncMessageBytes,
          limits.maxSyncChangesPerMessage
        )
      )
      assert.deepStrictEqual(
        yield* PeerSyncEnvelope.decodeSyncEnvelope(maximumTransferEnvelopeBytes, limits),
        maximumTransferEnvelope
      )
      for (
        const invalid of [
          { ...base, message: Uint8Array.of(1) },
          {
            ...base,
            writerProvenance: [{
              changeHash: "c".repeat(64),
              writerSchemaVersion: 1,
              writerDefinitionHash: "definition-1"
            }]
          },
          { ...base, messageHash: "f".repeat(64) },
          {
            ...base,
            checkpointTransfer: new Uint8Array(limits.maxSyncMessageBytes + 1)
          }
        ]
      ) {
        assert.strictEqual(
          (yield* Effect.exit(PeerSyncEnvelope.validateSyncEnvelope(invalid, limits)))._tag,
          "Failure"
        )
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

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
      assert.throws(() => Automerge.decodeChange(chunks[0]))
      const document = Automerge.load(chunks[0])
      yield* Effect.acquireUseRelease(
        Effect.succeed(document),
        (owned) => Effect.sync(() => assert.strictEqual(Automerge.getAllChanges(owned).length, 1)),
        (owned) => Effect.sync(() => Automerge.free(owned))
      )
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
            () => envelope.writerProvenance[0]
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
    protocolVersion: PeerSyncEnvelope.relayProtocolVersion,
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
      assert.strictEqual(digest, "126090efed19a37f5b4844c5964fcaa35cd0dea5a352c73f0477a6b843caee1e")
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
            ...base.writerProvenance[0],
            writerSchemaVersion: 3
          }, base.writerProvenance[1]]
        },
        {
          ...base,
          writerProvenance: [{
            ...base.writerProvenance[0],
            changeHash: "e".repeat(64)
          }, base.writerProvenance[1]]
        },
        {
          ...base,
          writerProvenance: [{
            ...base.writerProvenance[0],
            writerDefinitionHash: "definition-c"
          }, base.writerProvenance[1]]
        },
        { ...base, messageHash: "d".repeat(64) },
        { ...base, payload: Uint8Array.of(1, 2, 3, 5) }
      ]
      for (const mutation of mutations) {
        assert.notStrictEqual(yield* PeerSyncEnvelope.digestRelayOuterEnvelope(mutation), expected)
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))
})
