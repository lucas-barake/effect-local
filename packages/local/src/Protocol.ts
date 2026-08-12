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
export const maximumBootstrapEntries = 1_000

export const ProtocolVersion = Schema.Int.check(Schema.isGreaterThan(0))
export type ProtocolVersion = typeof ProtocolVersion.Type
export const currentProtocolVersion = ProtocolVersion.make(1)
export const NegotiateRequest = Schema.Struct({
  supportedVersions: Schema.Array(ProtocolVersion).check(Schema.isMinLength(1))
})
export type NegotiateRequest = typeof NegotiateRequest.Type
export const NegotiatedProtocol = Schema.Struct({ version: ProtocolVersion })
export type NegotiatedProtocol = typeof NegotiatedProtocol.Type

export const encodedBytes = (value: unknown): number => new TextEncoder().encode(Canonical.stringify(value)).byteLength

export const encodedBytesEffect = (
  value: unknown
): Effect.Effect<number, ReplicaError.CanonicalEncodeError> =>
  Canonical.stringifyEffect(value).pipe(Effect.map((encoded) => new TextEncoder().encode(encoded).byteLength))

export const MutationDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type MutationDigest = typeof MutationDigest.Type

export const MutationDigestVersion = Schema.Literal(3)
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
  membershipIncarnation: Identity.MembershipIncarnation,
  digest: MutationDigest
})
export type MutationEnvelope = typeof MutationEnvelope.Type

export type MutationDigestIdentity = Omit<MutationEnvelope, "digest">

export const SubmitRequest = Schema.Struct({
  envelope: MutationEnvelope,
  schema: Identity.SchemaIdentity
})
export type SubmitRequest = typeof SubmitRequest.Type
export const VersionedSubmitRequest = Schema.Struct({
  ...SubmitRequest.fields,
  protocolVersion: ProtocolVersion
})

export const DiscardRequest = Schema.Struct({
  envelope: MutationEnvelope,
  schema: Identity.SchemaIdentity
})
export type DiscardRequest = typeof DiscardRequest.Type
export const VersionedDiscardRequest = Schema.Struct({
  ...DiscardRequest.fields,
  protocolVersion: ProtocolVersion
})

export const mutationDigestInput = (envelope: MutationDigestIdentity): unknown => {
  return {
    spaceId: envelope.spaceId,
    clientId: envelope.clientId,
    mutationId: envelope.mutationId,
    localSequence: envelope.localSequence,
    basis: envelope.basis,
    name: envelope.name,
    payload: envelope.payload,
    digestVersion: envelope.digestVersion,
    sourceSchema: envelope.sourceSchema,
    mutationVersion: envelope.mutationVersion,
    membershipIncarnation: envelope.membershipIncarnation
  }
}

export const mutationDigest = (envelope: MutationDigestIdentity) => Canonical.digest(mutationDigestInput(envelope))

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
  localSequence: Identity.LocalSequence,
  membershipIncarnation: Identity.MembershipIncarnation
}

export const AcceptedReceipt = Schema.TaggedStruct("Accepted", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  serverSequence: Identity.ServerSequence,
  terminalSequence: Schema.optionalKey(Identity.TerminalSequence),
  result: Schema.Json
})
export type AcceptedReceipt = typeof AcceptedReceipt.Type

export const RejectionOrigin = Schema.Literals(["Mutation", "Authorization", "Capacity", "Legacy", "Quarantine"])
export type RejectionOrigin = typeof RejectionOrigin.Type

export const RejectedReceipt = Schema.TaggedStruct("Rejected", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  origin: RejectionOrigin,
  terminalSequence: Schema.optionalKey(Identity.TerminalSequence),
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

export const ExpiredReceipt = Schema.TaggedStruct("Expired", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sourceSchema: Identity.SchemaIdentity,
  mutationVersion: Identity.SchemaVersion,
  snapshotId: Identity.SnapshotId,
  snapshotSequence: Identity.ServerSequence,
  terminalSequenceThrough: Identity.TerminalSequence
})
export type ExpiredReceipt = typeof ExpiredReceipt.Type

export const Receipt = Schema.Union([AcceptedReceipt, RejectedReceipt, LegacyReceipt, ExpiredReceipt])
export type Receipt = typeof Receipt.Type

export const AcceptedMutation = Schema.Struct({
  sequence: Identity.ServerSequence,
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  membershipIncarnation: Identity.MembershipIncarnation,
  sourceSchema: Identity.SchemaIdentity,
  digest: MutationDigest,
  changes: Schema.Array(EntityChange)
})
export type AcceptedMutation = typeof AcceptedMutation.Type

export const PullRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  schema: Identity.SchemaIdentity,
  after: Identity.ServerSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBatchEntries))
})
export type PullRequest = typeof PullRequest.Type
export const VersionedPullRequest = Schema.Struct({
  ...PullRequest.fields,
  protocolVersion: ProtocolVersion
})

export const WatchRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  schema: Identity.SchemaIdentity
})
export type WatchRequest = typeof WatchRequest.Type
export const VersionedWatchRequest = Schema.Struct({
  ...WatchRequest.fields,
  protocolVersion: ProtocolVersion
})

export const PullPage = Schema.Struct({
  entries: Schema.Array(AcceptedMutation).check(Schema.isMaxLength(maximumBatchEntries)),
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
})
export type PullPage = typeof PullPage.Type

export const SnapshotDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type SnapshotDigest = typeof SnapshotDigest.Type
export const initialSnapshotDigest = SnapshotDigest.make("0".repeat(64))

export const SnapshotManifest = Schema.Struct({
  spaceId: Identity.SpaceId,
  definitionHash: Schema.String,
  schema: Identity.SchemaIdentity,
  snapshotId: Identity.SnapshotId,
  sequence: Identity.ServerSequence,
  terminalSequenceThrough: Identity.TerminalSequence,
  entityCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  contentBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  digest: SnapshotDigest
})
export type SnapshotManifest = typeof SnapshotManifest.Type

export const SnapshotEntity = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  model: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  modelVersion: Identity.SchemaVersion,
  key: Schema.Json,
  value: Schema.Json,
  entityBytes: Schema.Int.check(Schema.isGreaterThan(0))
})
export type SnapshotEntity = typeof SnapshotEntity.Type

export const BootstrapRequired = Schema.TaggedStruct("BootstrapRequired", {
  manifest: SnapshotManifest,
  serverSchema: Identity.SchemaIdentity
})
export type BootstrapRequired = typeof BootstrapRequired.Type

export const PullResult = Schema.Union([PullPage, BootstrapRequired])
export type PullResult = typeof PullResult.Type

export const BootstrapRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  schema: Identity.SchemaIdentity,
  snapshotId: Identity.SnapshotId,
  afterOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1)),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBootstrapEntries))
})
export type BootstrapRequest = typeof BootstrapRequest.Type
export const VersionedBootstrapRequest = Schema.Struct({
  ...BootstrapRequest.fields,
  protocolVersion: ProtocolVersion
})

export const BootstrapPage = Schema.Struct({
  manifest: SnapshotManifest,
  entities: Schema.Array(SnapshotEntity).check(Schema.isMaxLength(maximumBootstrapEntries)),
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
})
export type BootstrapPage = typeof BootstrapPage.Type

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
export const VersionedPresenceUpdate = Schema.Struct({
  ...PresenceUpdate.fields,
  protocolVersion: ProtocolVersion
})

export const VersionedWatchPresenceRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  protocolVersion: ProtocolVersion
})

export const PendingMutation = Schema.Struct({
  envelope: MutationEnvelope,
  optimisticResult: Schema.Json,
  changes: Schema.Array(EntityChange)
})
export type PendingMutation = typeof PendingMutation.Type
