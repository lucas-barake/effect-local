import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"
import * as Identity from "./Identity.js"
import type * as ReplicaError from "./ReplicaError.js"

export const maximumMutationBytes = 256 * 1024
export const maximumBatchEntries = 1_000
export const maximumBatchBytes = 4 * 1024 * 1024
export const maximumReceiptBytes = 4 * 1024 * 1024
export const maximumPresenceBytes = 16 * 1024
export const maximumPresenceTtlMillis = 60_000

export const encodedBytes = (value: unknown): number => new TextEncoder().encode(Canonical.stringify(value)).byteLength

export const encodedBytesEffect = (
  value: unknown
): Effect.Effect<number, ReplicaError.CanonicalEncodeError> =>
  Canonical.stringifyEffect(value).pipe(Effect.map((encoded) => new TextEncoder().encode(encoded).byteLength))

export const MutationDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type MutationDigest = typeof MutationDigest.Type

export const MutationDigestVersion = Schema.Literals([1, 2])
export type MutationDigestVersion = typeof MutationDigestVersion.Type

const MutationIdentity = {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  basis: Identity.ServerSequence,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  payload: Schema.Json
}

export const MutationEnvelope = Schema.Struct({
  ...MutationIdentity,
  digestVersion: MutationDigestVersion,
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  digest: MutationDigest
})
export type MutationEnvelope = typeof MutationEnvelope.Type

export const mutationDigestInput = (envelope: Omit<MutationEnvelope, "digest">): unknown => {
  const identity = {
    spaceId: envelope.spaceId,
    clientId: envelope.clientId,
    mutationId: envelope.mutationId,
    localSequence: envelope.localSequence,
    basis: envelope.basis,
    name: envelope.name,
    payload: envelope.payload
  }
  if (envelope.digestVersion === 1) return identity
  return {
    ...identity,
    digestVersion: envelope.digestVersion,
    sourceSchema: envelope.sourceSchema,
    mutationVersion: envelope.mutationVersion
  }
}

export const mutationDigest = (envelope: Omit<MutationEnvelope, "digest">) =>
  Canonical.digest(mutationDigestInput(envelope))

export const EntityKey = Schema.Struct({
  model: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  modelVersion: Identity.SchemaVersion,
  key: Schema.Json
})
export type EntityKey = typeof EntityKey.Type

export const Upsert = Schema.TaggedStruct("Upsert", {
  entity: EntityKey,
  value: Schema.Json
})
export const Delete = Schema.TaggedStruct("Delete", { entity: EntityKey })
export const EntityChange = Schema.Union([Upsert, Delete])
export type EntityChange = typeof EntityChange.Type

const ReceiptIdentity = {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence
}

export const AcceptedReceipt = Schema.TaggedStruct("Accepted", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  serverSequence: Identity.ServerSequence,
  result: Schema.Json
})
export type AcceptedReceipt = typeof AcceptedReceipt.Type

export const RejectionOrigin = Schema.Literals(["Mutation", "Authorization", "Capacity", "Legacy"])
export type RejectionOrigin = typeof RejectionOrigin.Type

export const RejectedReceipt = Schema.TaggedStruct("Rejected", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  origin: RejectionOrigin,
  rejection: Schema.Json
})
export type RejectedReceipt = typeof RejectedReceipt.Type

export const LegacyReceipt = Schema.TaggedStruct("Legacy", {
  ...ReceiptIdentity,
  sourceSchema: Identity.SchemaIdentity,
  outcome: Schema.Literals(["Accepted", "Rejected"]),
  serverSequence: Schema.NullOr(Identity.ServerSequence),
  body: Schema.Json
})
export type LegacyReceipt = typeof LegacyReceipt.Type

export const Receipt = Schema.Union([AcceptedReceipt, RejectedReceipt, LegacyReceipt])
export type Receipt = typeof Receipt.Type

export const AcceptedMutation = Schema.Struct({
  sequence: Identity.ServerSequence,
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  sourceSchema: Identity.SchemaIdentity,
  digest: MutationDigest,
  changes: Schema.Array(EntityChange)
})
export type AcceptedMutation = typeof AcceptedMutation.Type

export const PullRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  after: Identity.ServerSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBatchEntries))
})
export type PullRequest = typeof PullRequest.Type

export const PullPage = Schema.Struct({
  entries: Schema.Array(AcceptedMutation).check(Schema.isMaxLength(maximumBatchEntries)),
  hasMore: Schema.Boolean
})
export type PullPage = typeof PullPage.Type

export const Wake = Schema.Struct({
  spaceId: Identity.SpaceId,
  sequence: Identity.ServerSequence
})
export type Wake = typeof Wake.Type

export const PresenceUpdate = Schema.Struct({
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  value: Schema.Json,
  ttlMillis: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(maximumPresenceTtlMillis)
  )
})
export type PresenceUpdate = typeof PresenceUpdate.Type

export const PendingMutation = Schema.Struct({
  envelope: MutationEnvelope,
  optimisticResult: Schema.Json,
  changes: Schema.Array(EntityChange)
})
export type PendingMutation = typeof PendingMutation.Type
