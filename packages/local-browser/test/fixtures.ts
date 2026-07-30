import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Query from "@lucas-barake/effect-local/Query"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
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

export const replica: Replica.Replica["Service"] = {
  create: () => Effect.succeed(documentId),
  get: (_document, requestedId) =>
    Effect.succeed({
      documentId: requestedId,
      value: { title: "stored" },
      version: 1,
      heads: [],
      tombstone: false,
      projection: "Ready"
    }) as never,
  mutate: () => Effect.succeed("renamed") as never,
  delete: () => Effect.void,
  query: (_query, ...payload) => Effect.succeed([{ title: String(payload[0]) }]) as never,
  lookupMutation: (_mutation, commandId) =>
    Effect.succeed(CommandOutcome.durablyCommitted(commandId, "renamed")) as never,
  lookupCreate: (_document, commandId) => Effect.succeed(CommandOutcome.durablyCommitted(commandId, documentId)),
  lookupDelete: (_document, commandId) => Effect.succeed(CommandOutcome.durablyCommitted(commandId, undefined)),
  flush: Effect.void,
  status: Stream.make({ _tag: "Ready" as const, pendingCommands: 0 }),
  exportBackup: () => Stream.make(Uint8Array.of(1, 2, 3)),
  restoreBackup: () => Effect.void,
  exportDocument: (document, _documentId) =>
    Effect.succeed({
      documentName: document.name,
      schemaVersion: document.version,
      value: { title: "stored" }
    }) as never,
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
