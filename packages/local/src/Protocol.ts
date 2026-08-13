import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"
import type * as Definition from "./Definition.js"
import * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"
import type * as SecondaryIndex from "./SecondaryIndex.js"

export const maximumMutationBytes = 256 * 1024
export const maximumBatchEntries = 1_000
export const maximumBatchBytes = 4 * 1024 * 1024
export const maximumReceiptBytes = 4 * 1024 * 1024
export const maximumPresenceBytes = 16 * 1024
export const maximumReplicationScopeBytes = 4 * 1024 * 1024
export const maximumReplicationWindows = 1_000
export const maximumReplicationWindowPartitions = 1_000
export const maximumPresenceTtlMillis = 60_000
export const maximumBootstrapEntries = 1_000

const greaterThanZero = Schema.isGreaterThan(0)
const greaterThanOrEqualToZero = Schema.isGreaterThanOrEqualTo(0)
const greaterThanOrEqualToNegativeOne = Schema.isGreaterThanOrEqualTo(-1)
const minimumLengthOne = Schema.isMinLength(1)
const maximumLength256 = Schema.isMaxLength(256)
const unique = Schema.isUnique()
const digestPattern = Schema.isPattern(/^[0-9a-f]{64}$/)
const maximumBatchLength = Schema.isMaxLength(maximumBatchEntries)
const maximumBootstrapLength = Schema.isMaxLength(maximumBootstrapEntries)
const maximumReplicationWindowsLength = Schema.isMaxLength(maximumReplicationWindows)
const maximumReplicationWindowPartitionsLength = Schema.isMaxLength(maximumReplicationWindowPartitions)
const maximumBatchLimit = Schema.isLessThanOrEqualTo(maximumBatchEntries)
const maximumBootstrapLimit = Schema.isLessThanOrEqualTo(maximumBootstrapEntries)
const maximumPresenceTtl = Schema.isLessThanOrEqualTo(maximumPresenceTtlMillis)

export const ProtocolVersion = Schema.Int.check(greaterThanZero)
export type ProtocolVersion = typeof ProtocolVersion.Type
export const currentProtocolVersion = ProtocolVersion.make(1)
const withProtocolVersion = Schema.fieldsAssign({ protocolVersion: ProtocolVersion })
export const NegotiateRequest = Schema.Struct({
  supportedVersions: Schema.Array(ProtocolVersion).check(minimumLengthOne)
})
export type NegotiateRequest = typeof NegotiateRequest.Type
export const NegotiatedProtocol = Schema.Struct({ version: ProtocolVersion })
export type NegotiatedProtocol = typeof NegotiatedProtocol.Type

export const encodedBytes = (value: unknown): number => {
  const encoded = Canonical.stringify(value)
  return new TextEncoder().encode(encoded).byteLength
}

export const encodedBytesEffect = (
  value: unknown
): Effect.Effect<number, ReplicaError.CanonicalEncodeError> =>
  Canonical.stringifyEffect(value).pipe(Effect.map((encoded) => new TextEncoder().encode(encoded).byteLength))

export const MutationDigest = Schema.String.check(digestPattern)
export type MutationDigest = typeof MutationDigest.Type

export const MutationDigestVersion = Schema.Literal(3)
export type MutationDigestVersion = typeof MutationDigestVersion.Type

const MutationIdentity = {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  basis: Identity.ServerSequence,
  name: Schema.NonEmptyString.check(maximumLength256),
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
export const VersionedSubmitRequest = SubmitRequest.pipe(withProtocolVersion)

export const DiscardRequest = Schema.Struct({
  envelope: MutationEnvelope,
  schema: Identity.SchemaIdentity
})
export type DiscardRequest = typeof DiscardRequest.Type
export const VersionedDiscardRequest = DiscardRequest.pipe(withProtocolVersion)

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

export const mutationDigest = (envelope: MutationDigestIdentity) => {
  const input = mutationDigestInput(envelope)
  return Canonical.digest(input)
}

export const EntityKey = Schema.Struct({
  model: Schema.NonEmptyString.check(maximumLength256),
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

const ReplicationModelName = Schema.NonEmptyString.check(maximumLength256)

const WindowComponentValue = Schema.Union([Schema.String, Schema.Number, Schema.Boolean])
export type WindowComponentValue = typeof WindowComponentValue.Type

export const ReplicationWindowBounds = Schema.Struct({
  gt: Schema.optionalKey(WindowComponentValue),
  gte: Schema.optionalKey(WindowComponentValue),
  lt: Schema.optionalKey(WindowComponentValue),
  lte: Schema.optionalKey(WindowComponentValue)
})
export type ReplicationWindowBounds = typeof ReplicationWindowBounds.Type

export const ReplicationWindowPartition = Schema.Struct({
  key: Schema.Array(WindowComponentValue),
  count: Schema.Int.check(greaterThanZero).pipe(Schema.optionalKey),
  bounds: Schema.optionalKey(ReplicationWindowBounds)
})
export type ReplicationWindowPartition = typeof ReplicationWindowPartition.Type

export const ReplicationWindow = Schema.Struct({
  model: ReplicationModelName,
  index: Schema.NonEmptyString.check(maximumLength256),
  count: Schema.Int.check(greaterThanZero),
  partitions: Schema.Array(ReplicationWindowPartition).check(maximumReplicationWindowPartitionsLength).pipe(
    Schema.optionalKey
  )
})
export type ReplicationWindow = typeof ReplicationWindow.Type

export const ReplicationScope = Schema.Struct({
  models: Schema.Array(ReplicationModelName).check(unique),
  windows: Schema.Array(ReplicationWindow).check(maximumReplicationWindowsLength).pipe(Schema.optionalKey)
})
export type ReplicationScope = typeof ReplicationScope.Type

export const replicationScopeDigest = (scope: ReplicationScope) =>
  Canonical.digest({ format: 1, scope }).pipe(Effect.map((value) => MutationDigest.make(value)))

const comparePartitionKeys = (left: ReplicationWindowPartition, right: ReplicationWindowPartition) => {
  const leftKey = Canonical.stringify(left.key)
  const rightKey = Canonical.stringify(right.key)
  if (leftKey < rightKey) return -1
  if (leftKey > rightKey) return 1
  return 0
}

export const normalizeReplicationScope = (scope: ReplicationScope): ReplicationScope => {
  if (scope.windows === undefined || scope.windows.length === 0) {
    return ReplicationScope.make({ models: scope.models.toSorted() })
  }
  const windows = scope.windows.map((window) => {
    if (window.partitions === undefined) return window
    const partitions = window.partitions.toSorted(comparePartitionKeys)
    return ReplicationWindow.make({ ...window, partitions })
  }).toSorted((left, right) => {
    if (left.model < right.model) return -1
    if (left.model > right.model) return 1
    if (left.index < right.index) return -1
    if (left.index > right.index) return 1
    return 0
  })
  return ReplicationScope.make({ models: scope.models.toSorted(), windows })
}

const affinityMatches = (
  affinity: "text" | "real" | "integer",
  value: WindowComponentValue
): boolean => {
  if (affinity === "text") return typeof value === "string"
  if (affinity === "real") return typeof value === "number"
  return typeof value === "boolean" || (typeof value === "number" && Number.isSafeInteger(value))
}

const canonicalPartitionValue = (value: WindowComponentValue): string | number => {
  if (value === true) return 1
  if (value === false) return 0
  return value
}

export const validateReplicationScope = (
  definition: Definition.Any,
  scope: ReplicationScope
): Effect.Effect<ReplicationScope, ReplicaError.ProtocolInvalid> =>
  Effect.gen(function*() {
    const scopeBytes = yield* encodedBytesEffect(scope).pipe(
      Effect.mapError((cause) =>
        new ReplicaError.ProtocolInvalid({ message: "Replication scope cannot be canonically encoded", cause })
      )
    )
    if (scopeBytes > maximumReplicationScopeBytes) {
      return yield* new ReplicaError.ProtocolInvalid({
        message: `Replication scope exceeds ${maximumReplicationScopeBytes} encoded bytes`
      })
    }
    const normalized = normalizeReplicationScope(scope)
    for (const model of normalized.models) {
      if (!definition.modelByName.has(model)) {
        return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown replication model: ${model}` })
      }
    }
    if (normalized.windows === undefined) return normalized
    const seen = new Set<string>()
    let partitionCount = 0
    for (const window of normalized.windows) {
      const label = `${window.model}/${window.index}`
      const identity = Canonical.stringify([window.model, window.index])
      if (normalized.models.includes(window.model)) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: `Model ${window.model} cannot be both fully replicated and windowed`
        })
      }
      if (seen.has(identity)) {
        return yield* new ReplicaError.ProtocolInvalid({ message: `Duplicate replication window: ${label}` })
      }
      seen.add(identity)
      const model = definition.modelByName.get(window.model)
      if (model === undefined) {
        return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown replication model: ${window.model}` })
      }
      let index: SecondaryIndex.Any | undefined
      if (Object.hasOwn(model.indexes, window.index)) index = model.indexes[window.index]
      if (index === undefined) {
        return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown replication window index: ${label}` })
      }
      if (index.sort.length === 0) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: `Replication window index ${label} has no sort component`
        })
      }
      partitionCount += window.partitions?.length ?? 0
      if (partitionCount > maximumReplicationWindowPartitions) {
        return yield* new ReplicaError.ProtocolInvalid({
          message: `Replication scope exceeds ${maximumReplicationWindowPartitions} partition overrides`
        })
      }
      const seenPartitions = new Set<string>()
      for (const partition of window.partitions ?? []) {
        if (partition.key.length !== index.partition.length) {
          return yield* new ReplicaError.ProtocolInvalid({
            message: `Replication window partition for ${label} expects ${index.partition.length} key components`
          })
        }
        for (let component = 0; component < partition.key.length; component++) {
          const input = partition.key[component]
          const descriptor = index.partition[component]
          if (!affinityMatches(descriptor.affinity, input)) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Replication window partition key for ${label} does not match the index component types`
            })
          }
          const decoded = yield* Schema.decodeUnknownEffect(descriptor.schema)(input).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ProtocolInvalid({
                message: `Replication window partition key for ${label} does not match the component schema`,
                cause
              })
            )
          )
          const encoded = yield* Schema.encodeEffect(descriptor.schema)(decoded).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ProtocolInvalid({
                message: `Replication window partition key for ${label} cannot be canonically encoded`,
                cause
              })
            )
          )
          if (!Object.is(encoded, input)) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Replication window partition key for ${label} is not the canonical encoding`
            })
          }
        }
        const partitionKey = partition.key.map(canonicalPartitionValue)
        const partitionIdentity = Canonical.stringify(partitionKey)
        if (seenPartitions.has(partitionIdentity)) {
          return yield* new ReplicaError.ProtocolInvalid({
            message: `Duplicate replication window partition for ${label}`
          })
        }
        seenPartitions.add(partitionIdentity)
        if (partition.bounds !== undefined) {
          for (
            const bound of [
              partition.bounds.gt,
              partition.bounds.gte,
              partition.bounds.lt,
              partition.bounds.lte
            ]
          ) {
            if (bound === undefined) continue
            const leading = index.sort[0]
            if (!affinityMatches(leading.affinity, bound)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Replication window bounds for ${label} do not match the leading sort component type`
              })
            }
            const decoded = yield* Schema.decodeUnknownEffect(leading.schema)(bound).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ProtocolInvalid({
                  message: `Replication window bounds for ${label} do not match the component schema`,
                  cause
                })
              )
            )
            const encoded = yield* Schema.encodeEffect(leading.schema)(decoded).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ProtocolInvalid({
                  message: `Replication window bound for ${label} cannot be canonically encoded`,
                  cause
                })
              )
            )
            if (!Object.is(encoded, bound)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Replication window bound for ${label} is not the canonical encoding`
              })
            }
          }
        }
      }
    }
    return normalized
  })

export const replicationScopeCoversModel = (scope: ReplicationScope, model: string): boolean =>
  scope.models.includes(model) ||
  (scope.windows !== undefined && scope.windows.some((window) => window.model === model))

export const ReplicationCursor = Schema.Struct({
  viewId: Identity.ReplicationViewId,
  revision: Identity.ReplicationViewRevision
})
export type ReplicationCursor = typeof ReplicationCursor.Type

const ReceiptIdentity = {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  mutationId: Identity.MutationId,
  localSequence: Identity.LocalSequence,
  membershipIncarnation: Identity.MembershipIncarnation
}

export const AcceptedReceipt = Schema.TaggedStruct("Accepted", {
  ...ReceiptIdentity,
  name: Schema.NonEmptyString.check(maximumLength256),
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
  name: Schema.NonEmptyString.check(maximumLength256),
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
  name: Schema.NonEmptyString.check(maximumLength256),
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

const ReplicationRequestContext = {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  schema: Identity.SchemaIdentity,
  scope: ReplicationScope,
  scopeGeneration: Identity.ReplicationScopeGeneration
}

export const PullRequest = Schema.Struct({
  ...ReplicationRequestContext,
  cursor: Schema.NullOr(ReplicationCursor),
  limit: Schema.Int.check(greaterThanZero, maximumBatchLimit)
})
export type PullRequest = typeof PullRequest.Type
export const VersionedPullRequest = PullRequest.pipe(withProtocolVersion)

export const WatchRequest = Schema.Struct({
  ...ReplicationRequestContext,
  cursor: Schema.NullOr(ReplicationCursor)
})
export type WatchRequest = typeof WatchRequest.Type
export const VersionedWatchRequest = WatchRequest.pipe(withProtocolVersion)

export const PullPage = Schema.Struct({
  scopeGeneration: Identity.ReplicationScopeGeneration,
  cursor: ReplicationCursor,
  serverSequence: Identity.ServerSequence,
  changes: Schema.Array(ViewChange).check(maximumBatchLength),
  contentBytes: Schema.Int.check(greaterThanOrEqualToZero),
  digest: MutationDigest,
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
})
export type PullPage = typeof PullPage.Type

export const viewChangesDigest = (changes: ReadonlyArray<ViewChange>) =>
  Canonical.digest({ format: 1, changes }).pipe(Effect.map((value) => MutationDigest.make(value)))

export const SnapshotDigest = Schema.String.check(digestPattern)
export type SnapshotDigest = typeof SnapshotDigest.Type
const initialSnapshotDigestValue = "0".repeat(64)
export const initialSnapshotDigest = SnapshotDigest.make(initialSnapshotDigestValue)

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
  entityCount: Schema.Int.check(greaterThanOrEqualToZero),
  contentBytes: Schema.Int.check(greaterThanOrEqualToZero),
  digest: SnapshotDigest
})
export type SnapshotManifest = typeof SnapshotManifest.Type

export const SnapshotEntry = Schema.Struct({
  ordinal: Schema.Int.check(greaterThanOrEqualToZero),
  change: ViewChange,
  entryBytes: Schema.Int.check(greaterThanZero)
})
export type SnapshotEntry = typeof SnapshotEntry.Type

export const snapshotEntryDigest = (previous: SnapshotDigest, entry: SnapshotEntry) =>
  Canonical.digest({ previous, entry }).pipe(Effect.map((value) => SnapshotDigest.make(value)))

export const SnapshotEntity = Schema.Struct({
  ordinal: Schema.Int.check(greaterThanOrEqualToZero),
  model: Schema.NonEmptyString.check(maximumLength256),
  modelVersion: Identity.SchemaVersion,
  key: Schema.Json,
  value: Schema.Json,
  entityBytes: Schema.Int.check(greaterThanZero)
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
  ...ReplicationRequestContext,
  cursor: ReplicationCursor,
  snapshotId: Identity.SnapshotId,
  afterOrdinal: Schema.Int.check(greaterThanOrEqualToNegativeOne),
  limit: Schema.Int.check(greaterThanZero, maximumBootstrapLimit)
})
export type BootstrapRequest = typeof BootstrapRequest.Type
export const VersionedBootstrapRequest = BootstrapRequest.pipe(withProtocolVersion)

export const BootstrapPage = Schema.Struct({
  manifest: SnapshotManifest,
  entries: Schema.Array(SnapshotEntry).check(maximumBootstrapLength),
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
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
  ttlMillis: Schema.Int.check(greaterThanZero, maximumPresenceTtl)
})
export type PresenceUpdate = typeof PresenceUpdate.Type
export const VersionedPresenceUpdate = PresenceUpdate.pipe(withProtocolVersion)

export const VersionedWatchPresenceRequest = Schema.Struct({ spaceId: Identity.SpaceId }).pipe(withProtocolVersion)

export const SubmissionState = Schema.Literals([
  "Queued",
  "Submitting",
  "Retrying",
  "Submitted",
  "AwaitingReceipt"
])
export type SubmissionState = typeof SubmissionState.Type

export const PendingMutation = Schema.Struct({
  envelope: MutationEnvelope,
  optimisticResult: Schema.Json,
  changes: Schema.Array(EntityChange),
  submissionState: SubmissionState,
  attempts: Schema.Int.check(greaterThanOrEqualToZero)
})
export type PendingMutation = typeof PendingMutation.Type
