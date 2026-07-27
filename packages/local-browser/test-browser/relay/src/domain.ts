import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

/**
 * The smallest domain that can show a change crossing the relay.
 *
 * `labels` is a list rather than another scalar because a list merges additively: one device
 * holding the other's label is evidence the change arrived, not evidence of who won a
 * last-writer-wins coin toss.
 */
export const TaskDocument = Document.make("RelayTask", {
  schema: Schema.Struct({
    title: Schema.String,
    labels: Schema.Array(Schema.String)
  }),
  version: 1
})

export const AddLabel = Mutation.make("RelayTask.AddLabel", {
  document: TaskDocument,
  payload: Schema.String
})

export const definition = ReplicaDefinition.make({
  name: "relay-browser-fixture",
  documents: DocumentSet.make(TaskDocument),
  mutations: [AddLabel],
  projections: [],
  queries: []
})

export const Handlers = AddLabel.toLayer(({ draft, payload }) => {
  draft.labels.push(payload)
  return undefined
})

export const DomainLive = Layer.mergeAll(Handlers)

/** Generous, because the fixture is about reachability rather than about pressure. */
export const limits: ReplicaLimits.Values = {
  maxBackupBytes: 16 * 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 10_000,
  maxJsonDepth: 64,
  maxSyncMessageBytes: 1024 * 1024,
  maxPeerSendMillis: 30_000,
  maxSyncChangesPerMessage: 1_000,
  maxSyncDependencyEdgesPerMessage: 10_000,
  maxSyncOperationsPerMessage: 100_000,
  maxPendingBytesPerDocument: 16 * 1024 * 1024,
  maxPendingBytesPerPeer: 32 * 1024 * 1024,
  maxPendingBytesPerReplica: 64 * 1024 * 1024,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 10_000,
  maxPendingChangesPerPeer: 20_000,
  maxPendingChangesPerReplica: 50_000,
  maxPendingDependencyEdgesPerDocument: 100_000,
  maxPendingDependencyEdgesPerPeer: 200_000,
  maxPendingDependencyEdgesPerReplica: 500_000,
  maxSessions: 32,
  maxStreamsPerSession: 32,
  maxInFlightPerSession: 128,
  maxQueuedRpc: 1_024,
  maxQueuedPermits: 1_024,
  maxActiveRestores: 1_024,
  maxRestoresPerSession: 128,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
}
