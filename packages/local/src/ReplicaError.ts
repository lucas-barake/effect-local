import * as Schema from "effect/Schema"

export class CanonicalEncodeError extends Schema.TaggedErrorClass<CanonicalEncodeError>(
  "@lucas-barake/effect-local/CanonicalEncodeError"
)("CanonicalEncodeError", { cause: Schema.Defect() }) {}

export class StorageUnavailable extends Schema.TaggedErrorClass<StorageUnavailable>(
  "@lucas-barake/effect-local/StorageUnavailable"
)("StorageUnavailable", { cause: Schema.Defect() }) {}

export class StorageCorrupt extends Schema.TaggedErrorClass<StorageCorrupt>(
  "@lucas-barake/effect-local/StorageCorrupt"
)("StorageCorrupt", { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}

export class DefinitionMismatch extends Schema.TaggedErrorClass<DefinitionMismatch>(
  "@lucas-barake/effect-local/DefinitionMismatch"
)("DefinitionMismatch", { expected: Schema.String, actual: Schema.String }) {}

export class StaleSchema extends Schema.TaggedErrorClass<StaleSchema>(
  "@lucas-barake/effect-local/StaleSchema"
)("StaleSchema", {
  expectedVersion: Schema.Number,
  expectedHash: Schema.String,
  actualVersion: Schema.Number,
  actualHash: Schema.String
}) {}

export class SchemaGenerationConflict extends Schema.TaggedErrorClass<SchemaGenerationConflict>(
  "@lucas-barake/effect-local/SchemaGenerationConflict"
)("SchemaGenerationConflict", { expected: Schema.Number, actual: Schema.Number }) {}

export class SchemaEvolutionUnsupported extends Schema.TaggedErrorClass<SchemaEvolutionUnsupported>(
  "@lucas-barake/effect-local/SchemaEvolutionUnsupported"
)("SchemaEvolutionUnsupported", {
  sourceVersion: Schema.Number,
  sourceHash: Schema.String,
  targetVersion: Schema.Number,
  targetHash: Schema.String
}) {}

export class SchemaEvolutionFailed extends Schema.TaggedErrorClass<SchemaEvolutionFailed>(
  "@lucas-barake/effect-local/SchemaEvolutionFailed"
)("SchemaEvolutionFailed", {
  stepId: Schema.String,
  componentKind: Schema.Literals(["Model", "Mutation"]),
  componentName: Schema.String,
  part: Schema.Literals(["Key", "Value", "Payload", "Success", "Rejection"]),
  fromVersion: Schema.Number,
  toVersion: Schema.Number,
  cause: Schema.Defect()
}) {}

export class StorageMigrationMismatch extends Schema.TaggedErrorClass<StorageMigrationMismatch>(
  "@lucas-barake/effect-local/StorageMigrationMismatch"
)("StorageMigrationMismatch", { catalog: Schema.String, message: Schema.String }) {}

export class SchemaKeyCollision extends Schema.TaggedErrorClass<SchemaKeyCollision>(
  "@lucas-barake/effect-local/SchemaKeyCollision"
)("SchemaKeyCollision", { model: Schema.String, key: Schema.String }) {}

export class PendingMutationEvolutionRejected extends Schema.TaggedErrorClass<PendingMutationEvolutionRejected>(
  "@lucas-barake/effect-local/PendingMutationEvolutionRejected"
)("PendingMutationEvolutionRejected", { mutationId: Schema.String, rejection: Schema.Json }) {}

export class ReplicaIdentityMismatch extends Schema.TaggedErrorClass<ReplicaIdentityMismatch>(
  "@lucas-barake/effect-local/ReplicaIdentityMismatch"
)("ReplicaIdentityMismatch", {
  expectedClientId: Schema.String,
  actualClientId: Schema.String
}) {}

export class SpaceNotJoined extends Schema.TaggedErrorClass<SpaceNotJoined>(
  "@lucas-barake/effect-local/SpaceNotJoined"
)("SpaceNotJoined", { spaceId: Schema.String }) {}

export class SpaceUnavailable extends Schema.TaggedErrorClass<SpaceUnavailable>(
  "@lucas-barake/effect-local/SpaceUnavailable"
)("SpaceUnavailable", { spaceId: Schema.String }) {}

export class MutationIdentityConflict extends Schema.TaggedErrorClass<MutationIdentityConflict>(
  "@lucas-barake/effect-local/MutationIdentityConflict"
)("MutationIdentityConflict", { mutationId: Schema.String }) {}

export class QuarantineResubmissionConflict extends Schema.TaggedErrorClass<QuarantineResubmissionConflict>(
  "@lucas-barake/effect-local/QuarantineResubmissionConflict"
)("QuarantineResubmissionConflict", { mutationId: Schema.String }) {}

export class OutOfOrderMutation extends Schema.TaggedErrorClass<OutOfOrderMutation>(
  "@lucas-barake/effect-local/OutOfOrderMutation"
)("OutOfOrderMutation", { expected: Schema.Number, actual: Schema.Number }) {}

export class CursorGap extends Schema.TaggedErrorClass<CursorGap>(
  "@lucas-barake/effect-local/CursorGap"
)("CursorGap", { expected: Schema.Number, actual: Schema.Number }) {}

export class SnapshotUnavailable extends Schema.TaggedErrorClass<SnapshotUnavailable>(
  "@lucas-barake/effect-local/SnapshotUnavailable"
)("SnapshotUnavailable", { snapshotId: Schema.String }) {}

export class CapacityExceeded extends Schema.TaggedErrorClass<CapacityExceeded>(
  "@lucas-barake/effect-local/CapacityExceeded"
)("CapacityExceeded", { resource: Schema.String, limit: Schema.Number }) {}

export class InvalidConfiguration extends Schema.TaggedErrorClass<InvalidConfiguration>(
  "@lucas-barake/effect-local/InvalidConfiguration"
)("InvalidConfiguration", { option: Schema.String, message: Schema.String }) {}

export class UnknownCommitOutcome extends Schema.TaggedErrorClass<UnknownCommitOutcome>(
  "@lucas-barake/effect-local/UnknownCommitOutcome"
)("UnknownCommitOutcome", { mutationId: Schema.String, cause: Schema.Defect() }) {}

export class ProtocolInvalid extends Schema.TaggedErrorClass<ProtocolInvalid>(
  "@lucas-barake/effect-local/ProtocolInvalid"
)("ProtocolInvalid", { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}

export class UpgradeRequired extends Schema.TaggedErrorClass<UpgradeRequired>(
  "@lucas-barake/effect-local/UpgradeRequired"
)("UpgradeRequired", {
  clientVersions: Schema.Array(Schema.Int),
  serverVersions: Schema.Array(Schema.Int)
}) {}

export class ProtocolVersionRejected extends Schema.TaggedErrorClass<ProtocolVersionRejected>(
  "@lucas-barake/effect-local/ProtocolVersionRejected"
)("ProtocolVersionRejected", {
  version: Schema.Int,
  serverVersions: Schema.Array(Schema.Int)
}) {}

export class ServerUnavailable extends Schema.TaggedErrorClass<ServerUnavailable>(
  "@lucas-barake/effect-local/ServerUnavailable"
)("ServerUnavailable", {}) {}

export class AuthorizationDenied extends Schema.TaggedErrorClass<AuthorizationDenied>(
  "@lucas-barake/effect-local/AuthorizationDenied"
)("AuthorizationDenied", { reason: Schema.Json }) {}

export const StorageError = Schema.Union([StorageUnavailable, StorageCorrupt, CanonicalEncodeError])
export type StorageError = typeof StorageError.Type

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
  SchemaKeyCollision,
  PendingMutationEvolutionRejected,
  ReplicaIdentityMismatch,
  SpaceNotJoined,
  SpaceUnavailable,
  MutationIdentityConflict,
  QuarantineResubmissionConflict,
  OutOfOrderMutation,
  CursorGap,
  SnapshotUnavailable,
  CapacityExceeded,
  InvalidConfiguration,
  UnknownCommitOutcome,
  ProtocolInvalid,
  UpgradeRequired,
  ProtocolVersionRejected,
  ServerUnavailable,
  AuthorizationDenied
])
export type ReplicaError = typeof ReplicaError.Type
