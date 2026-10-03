import * as Schema from "effect/Schema"

const optionalDefect = Schema.optionalKey(Schema.Defect())
const nonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const optionalNonNegativeInt = Schema.optionalKey(nonNegativeInt)
const positiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export class CanonicalEncodeError extends Schema.TaggedError<CanonicalEncodeError>(
  "@lucas-barake/effect-local/CanonicalEncodeError"
)("CanonicalEncodeError", { cause: Schema.Defect() }) {}

export class StorageUnavailable extends Schema.TaggedError<StorageUnavailable>(
  "@lucas-barake/effect-local/StorageUnavailable"
)("StorageUnavailable", { cause: Schema.Defect() }) {}

export class StorageCorrupt extends Schema.TaggedError<StorageCorrupt>(
  "@lucas-barake/effect-local/StorageCorrupt"
)("StorageCorrupt", { message: Schema.String, cause: optionalDefect }) {}

/**
 * The consumer's own query statement failed: bad SQL, a missing table or column, or an unbindable
 * argument. Distinct from `StorageUnavailable` because retrying cannot help - the statement, not
 * the database, is wrong. Transient engine failures (busy, locked, connection) stay
 * `StorageUnavailable`.
 */
export class QueryFailed extends Schema.TaggedError<QueryFailed>(
  "@lucas-barake/effect-local/QueryFailed"
)("QueryFailed", { message: Schema.String, cause: optionalDefect }) {}

export class DefinitionMismatch extends Schema.TaggedError<DefinitionMismatch>(
  "@lucas-barake/effect-local/DefinitionMismatch"
)("DefinitionMismatch", { expected: Schema.String, actual: Schema.String }) {}

export class StaleSchema extends Schema.TaggedError<StaleSchema>(
  "@lucas-barake/effect-local/StaleSchema"
)("StaleSchema", {
  expectedVersion: Schema.Number,
  expectedHash: Schema.String,
  actualVersion: Schema.Number,
  actualHash: Schema.String
}) {}

export class SchemaGenerationConflict extends Schema.TaggedError<SchemaGenerationConflict>(
  "@lucas-barake/effect-local/SchemaGenerationConflict"
)("SchemaGenerationConflict", { expected: Schema.Number, actual: Schema.Number }) {}

export class SchemaEvolutionUnsupported extends Schema.TaggedError<SchemaEvolutionUnsupported>(
  "@lucas-barake/effect-local/SchemaEvolutionUnsupported"
)("SchemaEvolutionUnsupported", {
  sourceVersion: Schema.Number,
  sourceHash: Schema.String,
  targetVersion: Schema.Number,
  targetHash: Schema.String
}) {}

export class SchemaEvolutionFailed extends Schema.TaggedError<SchemaEvolutionFailed>(
  "@lucas-barake/effect-local/SchemaEvolutionFailed"
)("SchemaEvolutionFailed", {
  stepId: Schema.NullOr(Schema.String),
  componentKind: Schema.Literals(["Model", "Mutation"]),
  componentName: Schema.String,
  part: Schema.Literals(["Key", "Value", "Payload", "Success", "Rejection"]),
  fromVersion: Schema.Number,
  toVersion: Schema.Number,
  cause: Schema.Defect()
}) {}

export class StorageMigrationMismatch extends Schema.TaggedError<StorageMigrationMismatch>(
  "@lucas-barake/effect-local/StorageMigrationMismatch"
)("StorageMigrationMismatch", { catalog: Schema.String, message: Schema.String }) {}

export class StorageMigrationPending extends Schema.TaggedError<StorageMigrationPending>(
  "@lucas-barake/effect-local/StorageMigrationPending"
)("StorageMigrationPending", { catalog: Schema.String, message: Schema.String }) {}

export class SchemaKeyCollision extends Schema.TaggedError<SchemaKeyCollision>(
  "@lucas-barake/effect-local/SchemaKeyCollision"
)("SchemaKeyCollision", { model: Schema.String, key: Schema.String }) {}

export class PendingMutationEvolutionRejected extends Schema.TaggedError<PendingMutationEvolutionRejected>(
  "@lucas-barake/effect-local/PendingMutationEvolutionRejected"
)("PendingMutationEvolutionRejected", { mutationId: Schema.String, rejection: Schema.Json }) {}

export class ReplicaIdentityMismatch extends Schema.TaggedError<ReplicaIdentityMismatch>(
  "@lucas-barake/effect-local/ReplicaIdentityMismatch"
)("ReplicaIdentityMismatch", {
  expectedClientId: Schema.String,
  actualClientId: Schema.String
}) {}

export class SpaceNotJoined extends Schema.TaggedError<SpaceNotJoined>(
  "@lucas-barake/effect-local/SpaceNotJoined"
)("SpaceNotJoined", { spaceId: Schema.String }) {}

export class SpaceUnavailable extends Schema.TaggedError<SpaceUnavailable>(
  "@lucas-barake/effect-local/SpaceUnavailable"
)("SpaceUnavailable", { spaceId: Schema.String }) {}

export class EphemeralSessionUnavailable extends Schema.TaggedError<EphemeralSessionUnavailable>(
  "@lucas-barake/effect-local/EphemeralSessionUnavailable"
)("EphemeralSessionUnavailable", {
  spaceId: Schema.String,
  clientId: Schema.String,
  membershipIncarnation: Schema.String
}) {}

export class MutationIdentityConflict extends Schema.TaggedError<MutationIdentityConflict>(
  "@lucas-barake/effect-local/MutationIdentityConflict"
)("MutationIdentityConflict", { mutationId: Schema.String }) {}

export class QuarantineResubmissionConflict extends Schema.TaggedError<QuarantineResubmissionConflict>(
  "@lucas-barake/effect-local/QuarantineResubmissionConflict"
)("QuarantineResubmissionConflict", { mutationId: Schema.String }) {}

export class OutOfOrderMutation extends Schema.TaggedError<OutOfOrderMutation>(
  "@lucas-barake/effect-local/OutOfOrderMutation"
)("OutOfOrderMutation", { expected: Schema.Number, actual: Schema.Number }) {}

export class CursorGap extends Schema.TaggedError<CursorGap>(
  "@lucas-barake/effect-local/CursorGap"
)("CursorGap", { expected: Schema.Number, actual: Schema.Number }) {}

export class SettlementReplayTruncated extends Schema.TaggedError<SettlementReplayTruncated>(
  "@lucas-barake/effect-local/SettlementReplayTruncated"
)("SettlementReplayTruncated", { requested: Schema.Number, oldestAvailable: Schema.Number }) {}

export class StaleReplicationScope extends Schema.TaggedError<StaleReplicationScope>(
  "@lucas-barake/effect-local/StaleReplicationScope"
)("StaleReplicationScope", { expected: Schema.Number, actual: Schema.Number }) {}

export class SnapshotUnavailable extends Schema.TaggedError<SnapshotUnavailable>(
  "@lucas-barake/effect-local/SnapshotUnavailable"
)("SnapshotUnavailable", { snapshotId: Schema.String }) {}

export class CapacityExceeded extends Schema.TaggedError<CapacityExceeded>(
  "@lucas-barake/effect-local/CapacityExceeded"
)("CapacityExceeded", { resource: Schema.String, limit: Schema.Number }) {}

export class InvalidConfiguration extends Schema.TaggedError<InvalidConfiguration>(
  "@lucas-barake/effect-local/InvalidConfiguration"
)("InvalidConfiguration", { option: Schema.String, message: Schema.String }) {}

export class UnknownCommitOutcome extends Schema.TaggedError<UnknownCommitOutcome>(
  "@lucas-barake/effect-local/UnknownCommitOutcome"
)("UnknownCommitOutcome", { mutationId: Schema.String, cause: Schema.Defect() }) {}

export class ProtocolInvalid extends Schema.TaggedError<ProtocolInvalid>(
  "@lucas-barake/effect-local/ProtocolInvalid"
)("ProtocolInvalid", { message: Schema.String, cause: optionalDefect }) {}

export class UpgradeRequired extends Schema.TaggedError<UpgradeRequired>(
  "@lucas-barake/effect-local/UpgradeRequired"
)("UpgradeRequired", {
  clientVersions: Schema.Array(Schema.Int),
  serverVersions: Schema.Array(Schema.Int)
}) {}

export class ProtocolVersionRejected extends Schema.TaggedError<ProtocolVersionRejected>(
  "@lucas-barake/effect-local/ProtocolVersionRejected"
)("ProtocolVersionRejected", {
  version: Schema.Int,
  serverVersions: Schema.Array(Schema.Int)
}) {}

export class ServerUnavailable extends Schema.TaggedError<ServerUnavailable>(
  "@lucas-barake/effect-local/ServerUnavailable"
)("ServerUnavailable", {}) {}

export class CredentialRejected extends Schema.TaggedError<CredentialRejected>(
  "@lucas-barake/effect-local/CredentialRejected"
)("CredentialRejected", {
  credentialGeneration: optionalNonNegativeInt
}) {}

export class AuthenticatorUnavailable extends Schema.TaggedError<AuthenticatorUnavailable>(
  "@lucas-barake/effect-local/AuthenticatorUnavailable"
)("AuthenticatorUnavailable", {}) {}

export class OperationTimeout extends Schema.TaggedError<OperationTimeout>(
  "@lucas-barake/effect-local/OperationTimeout"
)("OperationTimeout", {
  operation: Schema.String,
  timeoutMillis: positiveInt
}) {}

export class AuthorizationDenied extends Schema.TaggedError<AuthorizationDenied>(
  "@lucas-barake/effect-local/AuthorizationDenied"
)("AuthorizationDenied", { reason: Schema.Json }) {}

export class OwnerUnavailable extends Schema.TaggedError<OwnerUnavailable>(
  "@lucas-barake/effect-local/OwnerUnavailable"
)("OwnerUnavailable", {
  reason: Schema.Literals(["transport", "takeover", "promotion-failed"])
}) {}

export class BuildSuperseded extends Schema.TaggedError<BuildSuperseded>(
  "@lucas-barake/effect-local/BuildSuperseded"
)("BuildSuperseded", {
  version: positiveInt,
  supersedingVersion: positiveInt
}) {}

export const StorageError = Schema.Union([StorageUnavailable, StorageCorrupt, CanonicalEncodeError])
export type StorageError = typeof StorageError.Type

export const QueryError = Schema.Union([StorageUnavailable, StorageCorrupt, CanonicalEncodeError, QueryFailed])
export type QueryError = typeof QueryError.Type

export const ReplicaError = Schema.Union([
  StorageUnavailable,
  StorageCorrupt,
  CanonicalEncodeError,
  DefinitionMismatch,
  StaleSchema,
  SchemaGenerationConflict,
  SchemaEvolutionUnsupported,
  SchemaEvolutionFailed,
  StorageMigrationMismatch,
  StorageMigrationPending,
  SchemaKeyCollision,
  PendingMutationEvolutionRejected,
  ReplicaIdentityMismatch,
  SpaceNotJoined,
  SpaceUnavailable,
  EphemeralSessionUnavailable,
  MutationIdentityConflict,
  QuarantineResubmissionConflict,
  OutOfOrderMutation,
  CursorGap,
  SettlementReplayTruncated,
  StaleReplicationScope,
  SnapshotUnavailable,
  CapacityExceeded,
  InvalidConfiguration,
  UnknownCommitOutcome,
  ProtocolInvalid,
  UpgradeRequired,
  ProtocolVersionRejected,
  ServerUnavailable,
  CredentialRejected,
  AuthenticatorUnavailable,
  OperationTimeout,
  AuthorizationDenied,
  OwnerUnavailable,
  BuildSuperseded
])
export type ReplicaError = typeof ReplicaError.Type
