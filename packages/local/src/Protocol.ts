import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"
import type * as Definition from "./Definition.js"
import * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"

export const maximumMutationBytes = 256 * 1024
export const maximumBatchEntries = 1_000
export const maximumBatchBytes = 4 * 1024 * 1024
export const maximumReceiptBytes = 4 * 1024 * 1024
export const maximumPresenceBytes = 16 * 1024
export const maximumPresenceTtlMillis = 60_000
export const maximumBootstrapEntries = 1_000

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

export const SubmitRequest = Schema.Struct({
  envelope: MutationEnvelope,
  schema: Identity.SchemaIdentity
})
export type SubmitRequest = typeof SubmitRequest.Type

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

export const Retract = Schema.TaggedStruct("Retract", { entity: EntityKey })
export const ViewChange = Schema.Union([Upsert, Delete, Retract])
export type ViewChange = typeof ViewChange.Type

const ReplicationModelName = Schema.NonEmptyString.check(Schema.isMaxLength(256))

export const ReplicationScope = Schema.Struct({
  models: Schema.Array(ReplicationModelName).check(Schema.isUnique())
})
export type ReplicationScope = typeof ReplicationScope.Type

export const normalizeReplicationScope = (scope: ReplicationScope): ReplicationScope =>
  ReplicationScope.make({ models: scope.models.toSorted() })

export const validateReplicationScope = (
  definition: Definition.Any,
  scope: ReplicationScope
): Effect.Effect<ReplicationScope, ReplicaError.ProtocolInvalid> => {
  const normalized = normalizeReplicationScope(scope)
  for (const model of normalized.models) {
    if (!definition.modelByName.has(model)) {
      return Effect.fail(new ReplicaError.ProtocolInvalid({ message: `Unknown replication model: ${model}` }))
    }
  }
  return Effect.succeed(normalized)
}

export const ReplicationCursor = Schema.Struct({
  viewId: Identity.ReplicationViewId,
  revision: Identity.ReplicationViewRevision
})
export type ReplicationCursor = typeof ReplicationCursor.Type

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
  terminalSequence: Schema.optionalKey(Identity.TerminalSequence),
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
  sourceSchema: Identity.SchemaIdentity,
  digest: MutationDigest,
  changes: Schema.Array(EntityChange)
})
export type AcceptedMutation = typeof AcceptedMutation.Type

export const PullRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  schema: Identity.SchemaIdentity,
  scope: ReplicationScope,
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: Schema.NullOr(ReplicationCursor),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBatchEntries))
})
export type PullRequest = typeof PullRequest.Type

export const WatchRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  schema: Identity.SchemaIdentity,
  scope: ReplicationScope,
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: Schema.NullOr(ReplicationCursor)
})
export type WatchRequest = typeof WatchRequest.Type

export const PullPage = Schema.Struct({
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: ReplicationCursor,
  serverSequence: Identity.ServerSequence,
  changes: Schema.Array(ViewChange).check(Schema.isMaxLength(maximumBatchEntries)),
  contentBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  digest: MutationDigest,
  hasMore: Schema.Boolean
})
export type PullPage = typeof PullPage.Type

export const SnapshotDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type SnapshotDigest = typeof SnapshotDigest.Type
export const initialSnapshotDigest = SnapshotDigest.make("0".repeat(64))

export const SnapshotManifest = Schema.Struct({
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  definitionHash: Schema.String,
  schema: Identity.SchemaIdentity,
  scopeDigest: MutationDigest,
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: ReplicationCursor,
  snapshotId: Identity.SnapshotId,
  sequence: Identity.ServerSequence,
  terminalSequenceThrough: Identity.TerminalSequence,
  entityCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  contentBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  digest: SnapshotDigest
})
export type SnapshotManifest = typeof SnapshotManifest.Type

export const SnapshotEntry = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  change: ViewChange,
  entryBytes: Schema.Int.check(Schema.isGreaterThan(0))
})
export type SnapshotEntry = typeof SnapshotEntry.Type

export const BootstrapRequired = Schema.TaggedStruct("BootstrapRequired", {
  manifest: SnapshotManifest
})
export type BootstrapRequired = typeof BootstrapRequired.Type

export const PullResult = Schema.Union([PullPage, BootstrapRequired])
export type PullResult = typeof PullResult.Type

export const BootstrapRequest = Schema.Struct({
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  schema: Identity.SchemaIdentity,
  scope: ReplicationScope,
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: ReplicationCursor,
  snapshotId: Identity.SnapshotId,
  afterOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1)),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBootstrapEntries))
})
export type BootstrapRequest = typeof BootstrapRequest.Type

export const BootstrapPage = Schema.Struct({
  manifest: SnapshotManifest,
  entries: Schema.Array(SnapshotEntry).check(Schema.isMaxLength(maximumBootstrapEntries)),
  hasMore: Schema.Boolean
})
export type BootstrapPage = typeof BootstrapPage.Type

export const Wake = Schema.Struct({
  spaceId: Identity.SpaceId
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
