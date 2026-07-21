import { Schema } from "effect"
import { ClusterSchema, Entity } from "effect/unstable/cluster"
import { Rpc, RpcGroup } from "effect/unstable/rpc"
import { Workflow } from "effect/unstable/workflow"

export const CommandResult = Schema.Struct({
  commandId: Schema.String,
  documentId: Schema.String,
  revision: Schema.Number,
  value: Schema.String
})

export const CommandSnapshot = Schema.Struct({
  commandId: Schema.String,
  eventCount: Schema.Number,
  latestValue: Schema.String,
  processedCount: Schema.Number,
  replyCount: Schema.Number,
  storedReplyPayload: Schema.String
})

export const RollbackSnapshot = Schema.Struct({
  commandId: Schema.String,
  eventCount: Schema.Number,
  messageCount: Schema.Number,
  processedCount: Schema.Number,
  replyCount: Schema.Number,
  successfulReplyCount: Schema.Number,
  triggerCount: Schema.Number
})

export const Pulse = Schema.Struct({
  emittedAt: Schema.Number,
  index: Schema.Number
})

export const DatabaseWork = Schema.Struct({
  finishedAt: Schema.Number,
  startedAt: Schema.Number,
  total: Schema.Number
})

export const WorkflowSnapshot = Schema.Struct({
  beginCount: Schema.Number,
  completeCount: Schema.Number,
  executionId: Schema.String,
  status: Schema.String
})

export const RecoveryWorkflow = Workflow.make("Stage0RecoveryWorkflow", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})

export const Commit = Rpc.make("Commit", {
  success: CommandResult,
  payload: {
    commandId: Schema.String,
    documentId: Schema.String,
    value: Schema.String
  },
  primaryKey: ({ commandId }) => commandId
}).pipe(
  (rpc) => rpc.annotate(ClusterSchema.Persisted, true),
  (rpc) => rpc.annotate(ClusterSchema.WithTransaction, true),
  (rpc) => rpc.annotate(ClusterSchema.Uninterruptible, "client")
)

export const Rollback = Rpc.make("Rollback", {
  success: CommandResult,
  payload: {
    commandId: Schema.String,
    documentId: Schema.String,
    value: Schema.String
  },
  primaryKey: ({ commandId }) => commandId
}).pipe(
  (rpc) => rpc.annotate(ClusterSchema.Persisted, true),
  (rpc) => rpc.annotate(ClusterSchema.WithTransaction, true),
  (rpc) => rpc.annotate(ClusterSchema.Uninterruptible, "client")
)

export const DocumentEntity = Entity.make("Stage0Document", [Commit, Rollback])

export const PageApi = RpcGroup.make(
  Rpc.make("CommitDocument", {
    success: CommandResult,
    payload: {
      commandId: Schema.String,
      documentId: Schema.String,
      value: Schema.String
    }
  }),
  Rpc.make("InspectCommand", {
    success: CommandSnapshot,
    payload: { commandId: Schema.String }
  }),
  Rpc.make("RollbackDocument", {
    success: CommandResult,
    payload: {
      commandId: Schema.String,
      documentId: Schema.String,
      value: Schema.String
    }
  }),
  Rpc.make("InspectRollback", {
    success: RollbackSnapshot,
    payload: {
      commandId: Schema.String,
      documentId: Schema.String
    }
  }),
  Rpc.make("CleanupRollback", {
    payload: {
      commandId: Schema.String,
      documentId: Schema.String
    }
  }),
  Rpc.make("StressDatabase", {
    success: DatabaseWork,
    payload: { iterations: Schema.Number }
  }),
  Rpc.make("StartWorkflow", {
    success: Schema.String,
    payload: { id: Schema.String }
  }),
  Rpc.make("InspectWorkflow", {
    success: WorkflowSnapshot,
    payload: {
      executionId: Schema.String,
      id: Schema.String
    }
  }),
  Rpc.make("Heartbeat", {
    success: Pulse,
    stream: true,
    payload: {
      count: Schema.Number,
      intervalMs: Schema.Number
    }
  })
)
