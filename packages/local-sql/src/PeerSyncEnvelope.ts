import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as CheckpointAuthority from "./CheckpointAuthority.js"
import * as WriterProvenance from "./internal/writerProvenance.js"

export const syncEnvelopeVersion = 1
export const relayOuterEnvelopeVersion = 1
export const relayOuterEnvelopeDomain = "effect-local/relay-outer-envelope"
export const relayProtocolVersion = 1
export const maximumWriterProvenanceEntries = 1_000
export const maximumRelayPayloadBytes = 4 * 1_024 * 1_024
export const maximumCheckpointTransitions = 32

const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256))
const MessageHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

const CheckpointManifest = Schema.Struct({
  ...CheckpointAuthority.ManifestClaims.fields,
  authorization: CheckpointAuthority.AuthorizationToken
})

const CheckpointTransition = Schema.Struct({
  ...CheckpointAuthority.TransitionClaims.fields,
  priorSnapshot: Schema.Uint8Array,
  authorization: CheckpointAuthority.AuthorizationToken
})
export const CheckpointTransfer = Schema.Struct({
  snapshot: Schema.Uint8Array,
  manifest: CheckpointManifest,
  transitions: Schema.Array(CheckpointTransition).check(
    Schema.isMaxLength(maximumCheckpointTransitions)
  )
})
export type CheckpointTransfer = typeof CheckpointTransfer.Type

const CheckpointTransferJson = Schema.fromJsonString(Schema.toCodecJson(CheckpointTransfer))

export const encodeCheckpointTransfer = (
  transfer: CheckpointTransfer,
  maxSyncMessageBytes: number
): Effect.Effect<Uint8Array, ReplicaError.ReplicaError> =>
  Schema.encodeEffect(CheckpointTransferJson)(transfer).pipe(
    Effect.map((value) => new TextEncoder().encode(value)),
    Effect.flatMap((bytes) =>
      (() => {
        if (bytes.byteLength <= maxSyncMessageBytes) return (Effect.succeed(bytes))
        return (Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `checkpoint transfer at most ${maxSyncMessageBytes} bytes`,
              observed: "oversized checkpoint transfer"
            })
          })
        ))
      })()
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "encodable checkpoint transfer",
            observed: "invalid checkpoint transfer"
          })
        })
      ))
  )

export const decodeCheckpointTransfer = (
  bytes: Uint8Array,
  maxSyncMessageBytes: number
): Effect.Effect<CheckpointTransfer, ReplicaError.ReplicaError> => {
  if (bytes.byteLength > maxSyncMessageBytes) {
    return Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `checkpoint transfer at most ${maxSyncMessageBytes} bytes`,
          observed: "oversized checkpoint transfer"
        })
      })
    )
  }
  return Schema.decodeUnknownEffect(CheckpointTransferJson)(new TextDecoder().decode(bytes)).pipe(
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "checkpoint transfer",
            observed: "invalid checkpoint transfer"
          })
        })
      ))
  )
}

export const SyncEnvelope = Schema.Struct({
  connectionEpoch: BoundedName,
  sequence: Sequence,
  documentId: Identity.DocumentId,
  documentType: BoundedName,
  messageHash: MessageHash,
  message: Schema.Uint8ArrayFromBase64,
  lineage: Identity.DocumentLineage,
  writerProvenance: WriterProvenance.ChangeProvenances,
  checkpointTransfer: Schema.optionalKey(Schema.Uint8ArrayFromBase64)
})
export type SyncEnvelope = typeof SyncEnvelope.Type

export interface SyncEnvelopeLimits extends
  Pick<
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
): number =>
  Math.max(
    maxSyncMessageBytes * 2 + maxSyncChangesPerMessage * 512,
    Math.ceil(maxSyncMessageBytes / 3) * 4
  ) +
  4_096

const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(SyncEnvelope))

export const encodeSyncEnvelope = (
  envelope: SyncEnvelope
): Effect.Effect<Uint8Array, ReplicaError.ReplicaError> =>
  Schema.encodeEffect(SyncEnvelopeJson)({
    ...envelope,
    writerProvenance: WriterProvenance.canonicalize(envelope.writerProvenance)
  }).pipe(
    Effect.map((value) => new TextEncoder().encode(value)),
    Effect.catchTag(
      "SchemaError",
      () =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "encodable sync envelope",
              observed: "invalid sync envelope"
            })
          })
        )
    )
  )

export const validateSyncEnvelope = (
  envelope: SyncEnvelope,
  limits: SyncEnvelopeLimits
): Effect.Effect<SyncEnvelope, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Effect.gen(function*() {
    if (envelope.message.byteLength > limits.maxSyncMessageBytes) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `sync message at most ${limits.maxSyncMessageBytes} bytes`,
          observed: "oversized sync message"
        })
      })
    }
    if (envelope.writerProvenance.length > limits.maxSyncChangesPerMessage) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `at most ${limits.maxSyncChangesPerMessage} writer provenance entries`,
          observed: "excess writer provenance"
        })
      })
    }
    if (
      envelope.checkpointTransfer !== undefined &&
      envelope.checkpointTransfer.byteLength > limits.maxSyncMessageBytes
    ) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `checkpoint transfer at most ${limits.maxSyncMessageBytes} bytes`,
          observed: "oversized checkpoint transfer"
        })
      })
    }
    if (
      envelope.checkpointTransfer !== undefined &&
      (envelope.message.byteLength !== 0 || envelope.writerProvenance.length !== 0)
    ) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "empty sync message and writer provenance for checkpoint transfer",
          observed: "checkpoint transfer mixed with ordinary sync content"
        })
      })
    }
    const messageHash = yield* Canonical.digest(envelope.message)
    if (messageHash !== envelope.messageHash) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "matching sync message hash",
          observed: "conflicting sync message hash"
        })
      })
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
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `sync envelope at most ${maximumBytes} bytes`,
          observed: "oversized sync envelope"
        })
      })
    )
  }
  return Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
    Effect.catchTag(
      "SchemaError",
      () =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "sync envelope",
              observed: "invalid sync envelope"
            })
          })
        )
    ),
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
  lineage: Identity.DocumentLineage,
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
    Effect.catchTag(
      "SchemaError",
      () =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "encodable relay outer envelope",
              observed: "invalid relay outer envelope"
            })
          })
        )
    )
  )

export const digestRelayOuterEnvelope = (
  envelope: RelayOuterEnvelope
): Effect.Effect<string, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Schema.encodeEffect(RelayOuterEnvelope)(canonicalizeRelayOuterEnvelope(envelope)).pipe(
    Effect.catchTag(
      "SchemaError",
      () =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "encodable relay outer envelope",
              observed: "invalid relay outer envelope"
            })
          })
        )
    ),
    Effect.flatMap(Canonical.digest)
  )

export const relayOuterEnvelopeDocument = (
  envelope: Pick<RelayOuterEnvelope, "document">
): RelayOuterEnvelope["document"] => envelope.document
