import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Quarantine from "@lucas-barake/effect-local/Quarantine"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Schema from "effect/Schema"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"

export class WireMutationRejection extends Schema.TaggedErrorClass<WireMutationRejection>(
  "@lucas-barake/effect-local-browser/WireMutationRejection"
)("WireMutationRejection", {
  name: Schema.String,
  rejection: Schema.Json
}) {}

export class WireQueryError extends Schema.TaggedErrorClass<WireQueryError>(
  "@lucas-barake/effect-local-browser/WireQueryError"
)("WireQueryError", {
  name: Schema.String,
  error: Schema.Json
}) {}

export class WireEphemeralEncodeError extends Schema.TaggedErrorClass<WireEphemeralEncodeError>(
  "@lucas-barake/effect-local-browser/WireEphemeralEncodeError"
)("WireEphemeralEncodeError", {
  name: Schema.String
}) {}

export class WireEphemeralDecodeError extends Schema.TaggedErrorClass<WireEphemeralDecodeError>(
  "@lucas-barake/effect-local-browser/WireEphemeralDecodeError"
)("WireEphemeralDecodeError", {
  name: Schema.String
}) {}

export class WireUnknownDefinition extends Schema.TaggedErrorClass<WireUnknownDefinition>(
  "@lucas-barake/effect-local-browser/WireUnknownDefinition"
)("WireUnknownDefinition", {
  kind: Schema.Literals(["model", "mutation", "query", "ephemeral"]),
  name: Schema.String
}) {}

export class WireUnknownSession extends Schema.TaggedErrorClass<WireUnknownSession>(
  "@lucas-barake/effect-local-browser/WireUnknownSession"
)("WireUnknownSession", {
  handle: Schema.String
}) {}

const MutateError = Schema.Union([
  ReplicaError.ReplicaError,
  WireMutationRejection,
  WireUnknownDefinition
])

const QueryError = Schema.Union([
  ReplicaError.ReplicaError,
  WireQueryError,
  WireUnknownDefinition
])

const ReplicaOrUnknown = Schema.Union([ReplicaError.ReplicaError, WireUnknownDefinition])

const EphemeralError = Schema.Union([
  ReplicaError.ReplicaError,
  WireEphemeralEncodeError,
  WireUnknownDefinition,
  WireUnknownSession
])

export const SettlementStart = Schema.Union([
  Schema.Literals(["live", "acknowledged"]),
  Schema.Int
])

export const WireSettlement = Schema.Struct({
  sequence: Identity.SettlementSequence,
  pending: Protocol.PendingMutation,
  receipt: Protocol.Receipt
})
export type WireSettlement = typeof WireSettlement.Type

export class Join extends Rpc.make("Join", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class Leave extends Rpc.make("Leave", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class Spaces extends Rpc.make("Spaces", {
  payload: {},
  success: Schema.Array(Identity.SpaceId),
  error: ReplicaError.ReplicaError
}) {}

export class AggregateStatus extends Rpc.make("AggregateStatus", {
  payload: {},
  success: ReplicaStatus.Aggregate,
  error: ReplicaError.ReplicaError
}) {}

export class SpaceScope extends Rpc.make("SpaceScope", {
  payload: { spaceId: Identity.SpaceId },
  success: Protocol.ReplicationScope,
  error: ReplicaError.ReplicaError
}) {}

export class SetScope extends Rpc.make("SetScope", {
  payload: { spaceId: Identity.SpaceId, scope: Protocol.ReplicationScope },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class Activation extends Rpc.make("Activation", {
  payload: { spaceId: Identity.SpaceId },
  success: Replica.Activation,
  error: ReplicaError.ReplicaError
}) {}

export class Activate extends Rpc.make("Activate", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class Deactivate extends Rpc.make("Deactivate", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class SpaceStatus extends Rpc.make("SpaceStatus", {
  payload: { spaceId: Identity.SpaceId },
  success: ReplicaStatus.SpaceStatus,
  error: ReplicaError.ReplicaError
}) {}

export class Mutate extends Rpc.make("Mutate", {
  payload: { spaceId: Identity.SpaceId, name: Schema.String, payload: Schema.Json },
  success: Protocol.PendingMutation,
  error: MutateError
}) {}

export class GetEntity extends Rpc.make("GetEntity", {
  payload: { spaceId: Identity.SpaceId, name: Schema.String, key: Schema.Json },
  success: Schema.Option(Schema.Json),
  error: ReplicaOrUnknown
}) {}

export class Query extends Rpc.make("Query", {
  payload: { spaceId: Identity.SpaceId, name: Schema.String, payload: Schema.Json },
  success: Schema.Json,
  error: QueryError
}) {}

export class ReceiptOf extends Rpc.make("ReceiptOf", {
  payload: { spaceId: Identity.SpaceId, name: Schema.String, mutationId: Identity.MutationId },
  success: Schema.Option(Protocol.Receipt),
  error: ReplicaOrUnknown
}) {}

export class Pending extends Rpc.make("Pending", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Array(Protocol.PendingMutation),
  error: ReplicaError.ReplicaError
}) {}

export class PendingFor extends Rpc.make("PendingFor", {
  payload: { spaceId: Identity.SpaceId, name: Schema.String },
  success: Schema.Array(Protocol.PendingMutation),
  error: ReplicaOrUnknown
}) {}

export class Settlements extends Rpc.make("Settlements", {
  payload: {
    spaceId: Identity.SpaceId,
    from: Schema.optional(SettlementStart),
    name: Schema.optional(Schema.String)
  },
  success: WireSettlement,
  error: ReplicaOrUnknown,
  stream: true
}) {}

export class AcknowledgeSettlements extends Rpc.make("AcknowledgeSettlements", {
  payload: { spaceId: Identity.SpaceId, sequence: Schema.Int },
  success: Schema.Void,
  error: ReplicaError.ReplicaError
}) {}

export class QuarantineList extends Rpc.make("QuarantineList", {
  payload: { spaceId: Identity.SpaceId },
  success: Schema.Array(Quarantine.QuarantinedMutation),
  error: ReplicaError.ReplicaError
}) {}

export class DiscardQuarantined extends Rpc.make("DiscardQuarantined", {
  payload: { spaceId: Identity.SpaceId, mutationId: Identity.MutationId },
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError
}) {}

export class ResubmitQuarantined extends Rpc.make("ResubmitQuarantined", {
  payload: {
    spaceId: Identity.SpaceId,
    mutationId: Identity.MutationId,
    name: Schema.String,
    payload: Schema.Json
  },
  success: Quarantine.ResubmitResult,
  error: MutateError
}) {}

export class Retain extends Rpc.make("Retain", {
  payload: { key: Schema.String },
  success: Schema.Void,
  error: Schema.Never
}) {}

export class Release extends Rpc.make("Release", {
  payload: { key: Schema.String },
  success: Schema.Void,
  error: Schema.Never
}) {}

export class EphemeralOpen extends Rpc.make("EphemeralOpen", {
  payload: {
    handle: Schema.String,
    name: Schema.String,
    spaceId: Identity.SpaceId,
    member: Protocol.EphemeralMember,
    value: Schema.Json,
    ttlMillis: Schema.Int
  },
  success: Schema.Void,
  error: EphemeralError
}) {}

export class EphemeralClose extends Rpc.make("EphemeralClose", {
  payload: { handle: Schema.String },
  success: Schema.Void,
  error: Schema.Never
}) {}

export class EphemeralUpdateMember extends Rpc.make("EphemeralUpdateMember", {
  payload: { handle: Schema.String, value: Schema.Json },
  success: Schema.Void,
  error: EphemeralError
}) {}

export const EphemeralEventFrame = Schema.Struct({
  member: Protocol.EphemeralMember,
  payload: Schema.Json
})

export class EphemeralEvents extends Rpc.make("EphemeralEvents", {
  payload: { handle: Schema.String, name: Schema.String },
  success: EphemeralEventFrame,
  error: EphemeralError,
  stream: true
}) {}

export const EphemeralStateFrame = Schema.Struct({
  member: Protocol.EphemeralMember,
  key: Schema.Json,
  value: Schema.Json,
  expiresAtMillis: Schema.Number
})

export class EphemeralState extends Rpc.make("EphemeralState", {
  payload: { handle: Schema.String, name: Schema.String },
  success: Schema.Array(EphemeralStateFrame),
  error: EphemeralError,
  stream: true
}) {}

export const EphemeralMemberFrame = Schema.Struct({
  member: Protocol.EphemeralMember,
  value: Schema.Json,
  expiresAtMillis: Schema.Number
})

export class EphemeralMembers extends Rpc.make("EphemeralMembers", {
  payload: { handle: Schema.String },
  success: Schema.Array(EphemeralMemberFrame),
  error: EphemeralError,
  stream: true
}) {}

export class EphemeralPublishEvent extends Rpc.make("EphemeralPublishEvent", {
  payload: {
    name: Schema.String,
    spaceId: Identity.SpaceId,
    member: Protocol.EphemeralMember,
    payload: Schema.Json,
    ttlMillis: Schema.Int
  },
  success: Schema.Void,
  error: EphemeralError
}) {}

export class EphemeralPublishState extends Rpc.make("EphemeralPublishState", {
  payload: {
    name: Schema.String,
    spaceId: Identity.SpaceId,
    member: Protocol.EphemeralMember,
    key: Schema.Json,
    payload: Schema.Json,
    ttlMillis: Schema.Int
  },
  success: Schema.Void,
  error: EphemeralError
}) {}

export class EphemeralClear extends Rpc.make("EphemeralClear", {
  payload: {
    name: Schema.String,
    spaceId: Identity.SpaceId,
    member: Protocol.EphemeralMember
  },
  success: Schema.Void,
  error: EphemeralError
}) {}

export class EphemeralRemove extends Rpc.make("EphemeralRemove", {
  payload: {
    name: Schema.String,
    spaceId: Identity.SpaceId,
    member: Protocol.EphemeralMember,
    key: Schema.Json
  },
  success: Schema.Void,
  error: EphemeralError
}) {}

export const ReplicaRpcs = RpcGroup.make(
  Join,
  Leave,
  Spaces,
  AggregateStatus,
  SpaceScope,
  SetScope,
  Activation,
  Activate,
  Deactivate,
  SpaceStatus,
  Mutate,
  GetEntity,
  Query,
  ReceiptOf,
  Pending,
  PendingFor,
  Settlements,
  AcknowledgeSettlements,
  QuarantineList,
  DiscardQuarantined,
  ResubmitQuarantined,
  Retain,
  Release,
  EphemeralOpen,
  EphemeralClose,
  EphemeralUpdateMember,
  EphemeralEvents,
  EphemeralState,
  EphemeralMembers,
  EphemeralPublishEvent,
  EphemeralPublishState,
  EphemeralClear,
  EphemeralRemove
)
