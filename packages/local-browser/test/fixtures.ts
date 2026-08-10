import * as CommandDeliveryPublisher from "@lucas-barake/effect-local-sql/CommandDeliveryPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Query from "@lucas-barake/effect-local/Query"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"

export const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

export class ReadError extends Schema.TaggedErrorClass<ReadError>()("ReadError", {
  filter: Schema.String
}) {}
export class RenameError extends Schema.TaggedErrorClass<RenameError>()("RenameError", {}) {}

export const Rename = Mutation.make("Rename", {
  document: Task,
  payload: { title: Schema.String },
  success: Schema.String,
  error: RenameError
})

export const Read = Query.make("Read", {
  payload: Schema.String,
  success: Schema.Array(Task.schema),
  error: ReadError,
  dependsOn: []
})

export const definition = ReplicaDefinition.make({
  name: "tasks",
  documents: DocumentSet.make(Task),
  mutations: [Rename],
  projections: [],
  queries: [Read]
})

export const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const readyProjection = "Ready"

export const replica: Replica.Replica["Service"] = {
  create: () => Effect.succeed(documentId),
  get: (document, requestedId) =>
    Document.decode(document, requestedId, { title: "stored" }).pipe(
      Effect.map((value) => ({
        documentId: requestedId,
        value,
        version: 1,
        heads: [],
        tombstone: false,
        projection: readyProjection
      }))
    ),
  inspectConflicts: (document, requestedId) =>
    Document.decode(document, requestedId, { title: "stored" }).pipe(
      Effect.map((value) => ({
        snapshot: {
          documentId: requestedId,
          value,
          version: 1,
          heads: [],
          tombstone: false,
          projection: readyProjection
        },
        conflicts: []
      }))
    ),
  resolveConflict: () => Effect.void,
  mutate: (mutation) => Schema.decodeUnknownEffect(mutation.successSchema)("renamed").pipe(Effect.orDie),
  delete: () => Effect.void,
  query: (query, ...payload) =>
    Schema.decodeUnknownEffect(query.successSchema)([{ title: String(payload[0]) }]).pipe(Effect.orDie),
  lookupMutation: (mutation, commandId) =>
    Schema.decodeUnknownEffect(mutation.successSchema)("renamed").pipe(
      Effect.orDie,
      Effect.map((value) => CommandOutcome.durablyCommitted(commandId, value))
    ),
  lookupCreate: (_document, commandId) => Effect.succeed(CommandOutcome.durablyCommitted(commandId, documentId)),
  lookupDelete: (_document, commandId) => Effect.succeed(CommandOutcome.durablyCommitted(commandId, undefined)),
  lookupConflictResolution: (_document, { commandId }) =>
    Effect.succeed(CommandOutcome.durablyCommitted(commandId, undefined)),
  lookupCommandDelivery: (commandId) => Effect.succeed(CommandDelivery.UnknownCommand.make({ commandId })),
  commandDeliveryChanges: (commandId) => Stream.make(CommandDelivery.UnknownCommand.make({ commandId })),
  flush: Effect.void,
  status: Stream.make({ _tag: "Ready", pendingCommands: 0 } satisfies ReplicaStatus.Ready),
  exportBackup: () => Stream.make(Uint8Array.of(1, 2, 3)),
  restoreBackup: () => Effect.void,
  installBackupDocument: () => Effect.void,
  exportDocument: (document, _documentId) =>
    Schema.encodeUnknownEffect(document.schema)({ title: "stored" }).pipe(
      Effect.orDie,
      Effect.map((value) => ({
        documentName: document.name,
        schemaVersion: document.version,
        value
      }))
    ),
  importDocument: () => Effect.succeed(documentId)
}

/**
 * Never completes. A status stream that ends looks exactly like one that is still open and has
 * nothing new to say, so an Atom observing it would park on the last value forever.
 */
export const peerConnectionStatus: PeerConnectionStatus.PeerConnectionStatus["Service"] = {
  status: () => Stream.make(PeerConnectionStatus.disconnected).pipe(Stream.concat(Stream.never))
}

/** No relay in these fixtures' topology, so `NotConfigured` rather than a `Disconnected` that would imply one. */
export const relayConnectionStatus: RelayConnectionStatus.RelayConnectionStatus["Service"] = {
  status: Stream.make(RelayConnectionStatus.notConfigured).pipe(Stream.concat(Stream.never))
}

export const DeliveryPublisher = Layer.succeed(
  CommandDeliveryPublisher.CommandDeliveryPublisher,
  CommandDeliveryPublisher.CommandDeliveryPublisher.of({
    publishPending: Effect.succeed(0),
    refresh: Effect.void,
    subscribe: Effect.succeed({
      sequence: 0,
      refreshEpoch: 0,
      events: Stream.never
    }),
    // The owner's delivery handler reads the replica directly, so reaching the publisher here
    // would mean the wiring changed underneath these tests.
    changes: () => Stream.die("unexpected command delivery subscription")
  })
)

export const peerRelayRuntimeService = PeerRelayClientRuntime.PeerRelayClientRuntime.of({
  admit: () => Effect.die("unexpected relay outbox admission"),
  dueForEndpoint: () => Effect.die("unexpected relay outbox replay"),
  maximumPendingHorizon: () => Effect.die("unexpected relay outbox horizon lookup"),
  markCustody: () => Effect.die("unexpected relay custody update"),
  validateReplicaIncarnation: () => Effect.die("unexpected replica incarnation validation"),
  validateConnectionConfiguration: () => Effect.die("unexpected connection configuration validation"),
  signalReceiptPrune: Effect.die("unexpected relay receipt prune"),
  health: Effect.void,
  awaitFatal: Effect.never,
  register: () => Effect.die("unexpected peer session registration"),
  send: () => Effect.void,
  transients: Stream.empty
})

export const PeerRelayRuntime = Layer.succeed(
  PeerRelayClientRuntime.PeerRelayClientRuntime,
  peerRelayRuntimeService
)

export const transientClient = {
  transient: () => Effect.void,
  transients: Stream.never
}
