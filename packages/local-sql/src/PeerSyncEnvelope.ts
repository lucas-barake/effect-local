import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as WriterProvenance from "./internal/writerProvenance.js"

export const syncEnvelopeVersion = 1
export const relayOuterEnvelopeVersion = 1
export const relayOuterEnvelopeDomain = "effect-local/relay-outer-envelope"
export const relayProtocolVersion = 3
export const maximumWriterProvenanceEntries = 1_000
export const maximumRelayPayloadBytes = 4 * 1_024 * 1_024

const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256))
const MessageHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export const SyncEnvelope = Schema.Struct({
  connectionEpoch: BoundedName,
  sequence: Sequence,
  documentId: Identity.DocumentId,
  documentType: BoundedName,
  messageHash: MessageHash,
  message: Schema.Uint8ArrayFromBase64,
  writerProvenance: WriterProvenance.ChangeProvenances
})
export type SyncEnvelope = typeof SyncEnvelope.Type

export interface SyncEnvelopeLimits
  extends Pick<
    ReplicaLimits.Values,
    | "maxSyncMessageBytes"
    | "maxSyncChangesPerMessage"
    | "maxSyncDependencyEdgesPerMessage"
    | "maxSyncOperationsPerMessage"
  >
{}

export const maximumSyncEnvelopeBytes = (
  maxSyncMessageBytes: number,
  maxSyncChangesPerMessage: number
): number => maxSyncMessageBytes * 2 + maxSyncChangesPerMessage * 512 + 4_096

const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(SyncEnvelope))

const protocolMismatch = (expected: string, observed: string): ReplicaError.ReplicaError =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({ expected, observed })
  })

export const encodeSyncEnvelope = (
  envelope: SyncEnvelope
): Effect.Effect<Uint8Array, ReplicaError.ReplicaError> =>
  Schema.encodeEffect(SyncEnvelopeJson)({
    ...envelope,
    writerProvenance: WriterProvenance.canonicalize(envelope.writerProvenance)
  }).pipe(
    Effect.map((value) => new TextEncoder().encode(value)),
    Effect.mapError(() => protocolMismatch("encodable sync envelope", "invalid sync envelope"))
  )

const decodeSyncChanges = (message: Uint8Array): ReadonlyArray<Automerge.DecodedChange> => {
  const changes = new Map<string, Automerge.DecodedChange>()
  for (const chunk of Automerge.decodeSyncMessage(message).changes) {
    try {
      const change = Automerge.decodeChange(chunk)
      changes.set(change.hash, change)
    } catch {
      const document = Automerge.load(chunk)
      try {
        for (const bytes of Automerge.getAllChanges(document)) {
          const change = Automerge.decodeChange(bytes)
          changes.set(change.hash, change)
        }
      } finally {
        Automerge.free(document)
      }
    }
  }
  return [...changes.values()]
}

export const validateSyncEnvelope = (
  envelope: SyncEnvelope,
  limits: SyncEnvelopeLimits
): Effect.Effect<SyncEnvelope, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Effect.gen(function*() {
    if (envelope.message.byteLength > limits.maxSyncMessageBytes) {
      return yield* protocolMismatch(
        `sync message at most ${limits.maxSyncMessageBytes} bytes`,
        "oversized sync message"
      )
    }
    if (envelope.writerProvenance.length > limits.maxSyncChangesPerMessage) {
      return yield* protocolMismatch(
        `at most ${limits.maxSyncChangesPerMessage} writer provenance entries`,
        "excess writer provenance"
      )
    }
    const changes = yield* Effect.try({
      try: () => decodeSyncChanges(envelope.message),
      catch: () => protocolMismatch("valid Automerge sync message", "invalid Automerge sync message")
    })
    if (changes.length > limits.maxSyncChangesPerMessage) {
      return yield* protocolMismatch(
        `at most ${limits.maxSyncChangesPerMessage} sync changes`,
        "excess sync changes"
      )
    }
    const dependencyEdges = changes.reduce((total, change) => total + change.deps.length, 0)
    if (dependencyEdges > limits.maxSyncDependencyEdgesPerMessage) {
      return yield* protocolMismatch(
        `at most ${limits.maxSyncDependencyEdgesPerMessage} sync dependency edges`,
        "excess sync dependency edges"
      )
    }
    const operations = changes.reduce((total, change) => total + change.ops.length, 0)
    if (operations > limits.maxSyncOperationsPerMessage) {
      return yield* protocolMismatch(
        `at most ${limits.maxSyncOperationsPerMessage} sync operations`,
        "excess sync operations"
      )
    }
    yield* Effect.try({
      try: () => WriterProvenance.validateExact(
        changes.map((change) => change.hash),
        envelope.writerProvenance
      ),
      catch: () => protocolMismatch(
        "one canonical writer provenance entry per sync change",
        "invalid writer provenance"
      )
    })
    const messageHash = yield* Canonical.digest(envelope.message)
    if (messageHash !== envelope.messageHash) {
      return yield* protocolMismatch("matching sync message hash", "conflicting sync message hash")
    }
    return {
      ...envelope,
      writerProvenance: WriterProvenance.canonicalize(envelope.writerProvenance)
    }
  })

export const decodeSyncEnvelope = (
  bytes: Uint8Array,
  limits: SyncEnvelopeLimits
): Effect.Effect<SyncEnvelope, ReplicaError.ReplicaError, Crypto.Crypto> => {
  const maximumBytes = maximumSyncEnvelopeBytes(
    limits.maxSyncMessageBytes,
    limits.maxSyncChangesPerMessage
  )
  if (bytes.byteLength > maximumBytes) {
    return Effect.fail(
      protocolMismatch(`sync envelope at most ${maximumBytes} bytes`, "oversized sync envelope")
    )
  }
  return Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
    Effect.mapError(() => protocolMismatch("sync envelope", "invalid sync envelope")),
    Effect.flatMap((envelope) => validateSyncEnvelope(envelope, limits))
  )
}

export const syncEnvelopeDocument = (
  envelope: Pick<SyncEnvelope, "documentId" | "documentType">
): { readonly documentId: Identity.DocumentId; readonly documentType: string } => ({
  documentId: envelope.documentId,
  documentType: envelope.documentType
})

export const RelayPeerPrincipal = Schema.Struct({
  tenantId: BoundedName,
  subjectId: BoundedName,
  peerId: Identity.PeerId
})
export type RelayPeerPrincipal = typeof RelayPeerPrincipal.Type

export const RelayOuterEnvelope = Schema.Struct({
  domain: Schema.Literal(relayOuterEnvelopeDomain),
  version: Schema.Literal(relayOuterEnvelopeVersion),
  expectedLocal: RelayPeerPrincipal,
  remote: RelayPeerPrincipal,
  relayPeerId: Identity.PeerId,
  relayMessageId: Identity.RelayMessageId,
  protocolVersion: Schema.Literal(relayProtocolVersion),
  payloadVersion: Schema.Literal(syncEnvelopeVersion),
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  senderConnectionEpoch: BoundedName,
  senderSequence: Sequence,
  document: Schema.Struct({
    documentId: Identity.DocumentId,
    documentType: BoundedName
  }),
  writerProvenance: WriterProvenance.ChangeProvenances.check(
    Schema.isMaxLength(maximumWriterProvenanceEntries)
  ),
  messageHash: MessageHash,
  payload: Schema.Uint8Array.check(
    Schema.makeFilter(
      (payload) => payload.byteLength <= maximumRelayPayloadBytes,
      { expected: `Uint8Array with at most ${maximumRelayPayloadBytes} bytes` }
    )
  )
})
export type RelayOuterEnvelope = typeof RelayOuterEnvelope.Type

export const canonicalizeRelayOuterEnvelope = (
  envelope: RelayOuterEnvelope
): RelayOuterEnvelope => ({
  ...envelope,
  writerProvenance: WriterProvenance.canonicalize(envelope.writerProvenance)
})

export const encodeRelayOuterEnvelope = (
  envelope: RelayOuterEnvelope
): Effect.Effect<Uint8Array, ReplicaError.ReplicaError> =>
  Schema.encodeEffect(RelayOuterEnvelope)(canonicalizeRelayOuterEnvelope(envelope)).pipe(
    Effect.map((encoded) => new TextEncoder().encode(Canonical.stringify(encoded))),
    Effect.mapError(() => protocolMismatch("encodable relay outer envelope", "invalid relay outer envelope"))
  )

export const digestRelayOuterEnvelope = (
  envelope: RelayOuterEnvelope
): Effect.Effect<string, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Schema.encodeEffect(RelayOuterEnvelope)(canonicalizeRelayOuterEnvelope(envelope)).pipe(
    Effect.mapError(() => protocolMismatch("encodable relay outer envelope", "invalid relay outer envelope")),
    Effect.flatMap(Canonical.digest)
  )

export const relayOuterEnvelopeDocument = (
  envelope: Pick<RelayOuterEnvelope, "document">
): RelayOuterEnvelope["document"] => envelope.document
