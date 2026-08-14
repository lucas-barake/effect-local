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
export const maximumRpcFrameBytes = maximumBatchBytes + 64 * 1024
export const maximumEphemeralSnapshotBytes = maximumBatchBytes
export const maximumReceiptBytes = 4 * 1024 * 1024
export const maximumEphemeralPayloadBytes = 16 * 1024
export const maximumEphemeralChannelLength = 256
export const maximumEphemeralKeyLength = 256
export const maximumReplicationScopeBytes = 4 * 1024 * 1024
export const maximumReplicationWindows = 1_000
export const maximumReplicationWindowPartitions = 1_000
export const maximumEphemeralMemberTtlMillis = 60_000
export const maximumEphemeralEventTtlMillis = 60_000
export const maximumEphemeralStateTtlMillis = 7 * 24 * 60 * 60 * 1_000
export const maximumBootstrapEntries = 1_000

export const ProtocolVersion = Schema.Int.check(Schema.isGreaterThan(0))
export type ProtocolVersion = typeof ProtocolVersion.Type
export const currentProtocolVersion = ProtocolVersion.make(1)
const withProtocolVersion = Schema.fieldsAssign({ protocolVersion: ProtocolVersion })
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

export const Retract = Schema.TaggedStruct("Retract", { entity: EntityKey })
export const ViewChange = Schema.Union([Upsert, Delete, Retract])
export type ViewChange = typeof ViewChange.Type

const ReplicationModelName = Schema.NonEmptyString.check(Schema.isMaxLength(256))

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
  count: Schema.Int.check(Schema.isGreaterThan(0)).pipe(Schema.optionalKey),
  bounds: Schema.optionalKey(ReplicationWindowBounds)
})
export type ReplicationWindowPartition = typeof ReplicationWindowPartition.Type

export const ReplicationWindow = Schema.Struct({
  model: ReplicationModelName,
  index: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  count: Schema.Int.check(Schema.isGreaterThan(0)),
  partitions: Schema.Array(ReplicationWindowPartition).check(
    Schema.isMaxLength(maximumReplicationWindowPartitions)
  ).pipe(Schema.optionalKey)
})
export type ReplicationWindow = typeof ReplicationWindow.Type

export const ReplicationScope = Schema.Struct({
  models: Schema.Array(ReplicationModelName).check(Schema.isUnique()),
  windows: Schema.Array(ReplicationWindow).check(Schema.isMaxLength(maximumReplicationWindows)).pipe(
    Schema.optionalKey
  )
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

export const validateReplicationScope = Effect.fnUntraced(function*(
  definition: Definition.Any,
  scope: ReplicationScope
): Effect.fn.Return<ReplicationScope, ReplicaError.ProtocolInvalid> {
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
      const partitionIdentity = Canonical.stringify(partition.key.map(canonicalPartitionValue))
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
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBatchEntries))
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
  changes: Schema.Array(ViewChange).check(Schema.isMaxLength(maximumBatchEntries)),
  contentBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  digest: MutationDigest,
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
})
export type PullPage = typeof PullPage.Type

export const viewChangesDigest = (changes: ReadonlyArray<ViewChange>) =>
  Canonical.digest({ format: 1, changes }).pipe(Effect.map((value) => MutationDigest.make(value)))

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

export const snapshotEntryDigest = (previous: SnapshotDigest, entry: SnapshotEntry) =>
  Canonical.digest({ previous, entry }).pipe(Effect.map((value) => SnapshotDigest.make(value)))

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
  ...ReplicationRequestContext,
  cursor: ReplicationCursor,
  snapshotId: Identity.SnapshotId,
  afterOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1)),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximumBootstrapEntries))
})
export type BootstrapRequest = typeof BootstrapRequest.Type
export const VersionedBootstrapRequest = BootstrapRequest.pipe(withProtocolVersion)

export const BootstrapPage = Schema.Struct({
  manifest: SnapshotManifest,
  entries: Schema.Array(SnapshotEntry).check(Schema.isMaxLength(maximumBootstrapEntries)),
  hasMore: Schema.Boolean,
  serverSchema: Identity.SchemaIdentity
})
export type BootstrapPage = typeof BootstrapPage.Type

export const Wake = Schema.Struct({
  spaceId: Identity.SpaceId
})
export type Wake = typeof Wake.Type

export const EphemeralMember = Schema.Struct({
  clientId: Identity.ClientId,
  membershipIncarnation: Identity.MembershipIncarnation
})
export type EphemeralMember = typeof EphemeralMember.Type

export const EphemeralChannel = Schema.NonEmptyString.check(
  Schema.isMaxLength(maximumEphemeralChannelLength)
)
export type EphemeralChannel = typeof EphemeralChannel.Type

export const EphemeralKey = Schema.NonEmptyString.check(Schema.isMaxLength(maximumEphemeralKeyLength))
export type EphemeralKey = typeof EphemeralKey.Type

const EphemeralRequestIdentity = {
  spaceId: Identity.SpaceId,
  member: EphemeralMember
}

export const EphemeralJoinRequest = Schema.Struct({
  ...EphemeralRequestIdentity,
  value: Schema.Json,
  ttlMillis: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(maximumEphemeralMemberTtlMillis)
  )
})
export type EphemeralJoinRequest = typeof EphemeralJoinRequest.Type
export const VersionedEphemeralJoinRequest = EphemeralJoinRequest.pipe(withProtocolVersion)

export const EphemeralEventRequest = Schema.TaggedStruct("Event", {
  ...EphemeralRequestIdentity,
  channel: EphemeralChannel,
  value: Schema.Json,
  ttlMillis: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(maximumEphemeralEventTtlMillis)
  )
})
export type EphemeralEventRequest = typeof EphemeralEventRequest.Type

export const EphemeralClearEventRequest = Schema.TaggedStruct("ClearEvent", {
  ...EphemeralRequestIdentity,
  channel: EphemeralChannel
})
export type EphemeralClearEventRequest = typeof EphemeralClearEventRequest.Type

export const EphemeralSetStateRequest = Schema.TaggedStruct("SetState", {
  ...EphemeralRequestIdentity,
  channel: EphemeralChannel,
  key: EphemeralKey,
  value: Schema.Json,
  ttlMillis: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(maximumEphemeralStateTtlMillis)
  )
})
export type EphemeralSetStateRequest = typeof EphemeralSetStateRequest.Type

export const EphemeralRemoveStateRequest = Schema.TaggedStruct("RemoveState", {
  ...EphemeralRequestIdentity,
  channel: EphemeralChannel,
  key: EphemeralKey
})
export type EphemeralRemoveStateRequest = typeof EphemeralRemoveStateRequest.Type

export const EphemeralUpdateMemberRequest = Schema.TaggedStruct("UpdateMember", {
  ...EphemeralRequestIdentity,
  value: Schema.Json
})
export type EphemeralUpdateMemberRequest = typeof EphemeralUpdateMemberRequest.Type

export const EphemeralPublishRequest = Schema.Union([
  EphemeralEventRequest,
  EphemeralClearEventRequest,
  EphemeralSetStateRequest,
  EphemeralRemoveStateRequest,
  EphemeralUpdateMemberRequest
])
export type EphemeralPublishRequest = typeof EphemeralPublishRequest.Type
export const VersionedEphemeralPublishRequest = Schema.Struct({
  request: EphemeralPublishRequest,
  sessionToken: Identity.EphemeralSessionToken
}).pipe(withProtocolVersion)

export const EphemeralHeartbeatRequest = Schema.Struct(EphemeralRequestIdentity)
export type EphemeralHeartbeatRequest = typeof EphemeralHeartbeatRequest.Type
export const VersionedEphemeralHeartbeatRequest = Schema.Struct({
  ...EphemeralRequestIdentity,
  sessionToken: Identity.EphemeralSessionToken
}).pipe(withProtocolVersion)

const EphemeralEntryIdentity = {
  member: EphemeralMember,
  expiresAtMillis: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
}

export const EphemeralMemberEntry = Schema.Struct({
  ...EphemeralEntryIdentity,
  value: Schema.Json
})
export type EphemeralMemberEntry = typeof EphemeralMemberEntry.Type

export const EphemeralStateEntry = Schema.Struct({
  ...EphemeralEntryIdentity,
  channel: EphemeralChannel,
  key: EphemeralKey,
  value: Schema.Json
})
export type EphemeralStateEntry = typeof EphemeralStateEntry.Type

export const EphemeralEventEntry = Schema.Struct({
  ...EphemeralEntryIdentity,
  channel: EphemeralChannel,
  value: Schema.Json
})
export type EphemeralEventEntry = typeof EphemeralEventEntry.Type

const EphemeralDeltaIdentity = {
  spaceId: Identity.SpaceId,
  revision: Identity.EphemeralRevision
}

export const EphemeralSnapshot = Schema.TaggedStruct("Snapshot", {
  ...EphemeralDeltaIdentity,
  members: Schema.Array(EphemeralMemberEntry),
  states: Schema.Array(EphemeralStateEntry)
})
export type EphemeralSnapshot = typeof EphemeralSnapshot.Type

export const EphemeralMemberUpserted = Schema.TaggedStruct("MemberUpserted", {
  ...EphemeralDeltaIdentity,
  entry: EphemeralMemberEntry
})
export type EphemeralMemberUpserted = typeof EphemeralMemberUpserted.Type

export const EphemeralMemberLeft = Schema.TaggedStruct("MemberLeft", {
  ...EphemeralDeltaIdentity,
  member: EphemeralMember
})
export type EphemeralMemberLeft = typeof EphemeralMemberLeft.Type

export const EphemeralStateSet = Schema.TaggedStruct("StateSet", {
  ...EphemeralDeltaIdentity,
  entry: EphemeralStateEntry
})
export type EphemeralStateSet = typeof EphemeralStateSet.Type

export const EphemeralStateRemoved = Schema.TaggedStruct("StateRemoved", {
  ...EphemeralDeltaIdentity,
  member: EphemeralMember,
  channel: EphemeralChannel,
  key: EphemeralKey
})
export type EphemeralStateRemoved = typeof EphemeralStateRemoved.Type

export const EphemeralEvent = Schema.TaggedStruct("Event", {
  ...EphemeralDeltaIdentity,
  entry: EphemeralEventEntry
})
export type EphemeralEvent = typeof EphemeralEvent.Type

export const EphemeralEventCleared = Schema.TaggedStruct("EventCleared", {
  ...EphemeralDeltaIdentity,
  member: EphemeralMember,
  channel: EphemeralChannel
})
export type EphemeralEventCleared = typeof EphemeralEventCleared.Type

export const EphemeralMessage = Schema.Union([
  EphemeralSnapshot,
  EphemeralMemberUpserted,
  EphemeralMemberLeft,
  EphemeralStateSet,
  EphemeralStateRemoved,
  EphemeralEvent,
  EphemeralEventCleared
])
export type EphemeralMessage = typeof EphemeralMessage.Type

export const EphemeralSessionStarted = Schema.TaggedStruct("SessionStarted", {
  spaceId: Identity.SpaceId,
  member: EphemeralMember,
  sessionToken: Identity.EphemeralSessionToken,
  leaseMillis: Schema.Int.check(Schema.isGreaterThan(0))
})
export type EphemeralSessionStarted = typeof EphemeralSessionStarted.Type

export const EphemeralJoinMessage = Schema.Union([EphemeralSessionStarted, EphemeralMessage])
export type EphemeralJoinMessage = typeof EphemeralJoinMessage.Type

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
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})
export type PendingMutation = typeof PendingMutation.Type
