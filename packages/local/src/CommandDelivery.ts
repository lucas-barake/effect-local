import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export const PendingRelayCustody = Schema.TaggedStruct("PendingRelayCustody", {
  acceptedChangeCount: Count,
  pendingChangeCount: Count,
  firstPendingAt: Schema.DateTimeUtcFromString,
  retryDeadline: Schema.DateTimeUtcFromString
})
export type PendingRelayCustody = typeof PendingRelayCustody.Type

export const RelayCustodyAccepted = Schema.TaggedStruct("RelayCustodyAccepted", {
  acceptedChangeCount: Count,
  acceptedAt: Schema.DateTimeUtcFromString,
  senderCustodyUnconfirmedAt: Schema.optional(Schema.DateTimeUtcFromString)
})
export type RelayCustodyAccepted = typeof RelayCustodyAccepted.Type

export const RelayCustodyUnconfirmedAtDeadline = Schema.TaggedStruct(
  "RelayCustodyUnconfirmedAtDeadline",
  {
    acceptedChangeCount: Count,
    unconfirmedChangeCount: Count,
    deadline: Schema.DateTimeUtcFromString,
    observedAt: Schema.DateTimeUtcFromString
  }
)
export type RelayCustodyUnconfirmedAtDeadline = typeof RelayCustodyUnconfirmedAtDeadline.Type

export const DestinationState = Schema.Union([
  PendingRelayCustody,
  RelayCustodyAccepted,
  RelayCustodyUnconfirmedAtDeadline
])
export type DestinationState = typeof DestinationState.Type

export const Destination = Schema.Struct({
  relayPeerId: Identity.PeerId,
  remotePeerId: Identity.PeerId,
  state: DestinationState
})
export type Destination = typeof Destination.Type

export const UnknownCommand = Schema.TaggedStruct("UnknownCommand", {
  commandId: Identity.CommandId
})
export type UnknownCommand = typeof UnknownCommand.Type

export const UntrackedCommand = Schema.TaggedStruct("UntrackedCommand", {
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId
})
export type UntrackedCommand = typeof UntrackedCommand.Type

export const NoChangesToDeliver = Schema.TaggedStruct("NoChangesToDeliver", {
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId
})
export type NoChangesToDeliver = typeof NoChangesToDeliver.Type

export const TrackedCommand = Schema.TaggedStruct("TrackedCommand", {
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId,
  localChangeCount: Count,
  destinations: Schema.Array(Destination)
})
export type TrackedCommand = typeof TrackedCommand.Type

export const CommandDelivery = Schema.Union([
  UnknownCommand,
  UntrackedCommand,
  NoChangesToDeliver,
  TrackedCommand
])
export type CommandDelivery = typeof CommandDelivery.Type

export const unknown = (commandId: Identity.CommandId): UnknownCommand => ({
  _tag: "UnknownCommand",
  commandId
})

export const untracked = (
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId
): UntrackedCommand => ({
  _tag: "UntrackedCommand",
  commandId,
  documentId
})

export const noChanges = (
  commandId: Identity.CommandId,
  documentId: Identity.DocumentId
): NoChangesToDeliver => ({
  _tag: "NoChangesToDeliver",
  commandId,
  documentId
})

export const isRelayCustodyAccepted = (
  delivery: CommandDelivery,
  relayPeerId: Identity.PeerId,
  remotePeerId: Identity.PeerId
): boolean =>
  delivery._tag === "TrackedCommand" &&
  delivery.destinations.some((destination) =>
    destination.relayPeerId === relayPeerId &&
    destination.remotePeerId === remotePeerId &&
    destination.state._tag === "RelayCustodyAccepted" &&
    destination.state.acceptedChangeCount === delivery.localChangeCount
  )
