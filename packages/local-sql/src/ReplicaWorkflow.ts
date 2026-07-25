import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Compaction from "./Compaction.js"
import * as ReplicaGate from "./ReplicaGate.js"

export const OperationId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/OperationId")
)
export type OperationId = typeof OperationId.Type

export const CompactReplica = Workflow.make("EffectLocal/CompactReplica", {
  payload: {
    replicaIncarnation: Identity.ReplicaIncarnation,
    operationId: OperationId
  },
  success: Schema.Void,
  error: ReplicaError.ReplicaError,
  idempotencyKey: (payload) => `${payload.replicaIncarnation}:${payload.operationId}`
})

export interface Execution {
  readonly executionId: string
  readonly operationId: OperationId
  readonly replicaIncarnation: Identity.ReplicaIncarnation
}

export class CompactionWorkflow extends Context.Service<CompactionWorkflow, {
  readonly execute: (operationId: OperationId) => Effect.Effect<Execution, ReplicaError.ReplicaError>
  readonly poll: (
    execution: Execution
  ) => Effect.Effect<Option.Option<Workflow.Result<void, ReplicaError.ReplicaError>>, ReplicaError.ReplicaError>
  readonly interrupt: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly resume: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaWorkflow/CompactionWorkflow") {}

const validateIncarnation = (
  expected: Identity.ReplicaIncarnation,
  permit: ReplicaGate.Permit
) =>
  permit.incarnation === expected ? Effect.void : Effect.fail(
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({
        expected: `replica incarnation ${expected}`,
        observed: `replica incarnation ${permit.incarnation}`
      })
    })
  )

const withActivityPermit = <A,>(
  gate: ReplicaGate.ReplicaGate["Service"],
  incarnation: Identity.ReplicaIncarnation,
  effect: Effect.Effect<A, ReplicaError.ReplicaError>
) =>
  Effect.gen(function*() {
    const permit = yield* gate.shared
    yield* validateIncarnation(incarnation, permit)
    yield* gate.validate(permit)
    const value = yield* effect
    yield* gate.validate(permit)
    return value
  }).pipe(Effect.scoped)

const DocumentReference = Schema.Struct({
  documentId: Identity.DocumentId,
  documentType: Schema.String
})

/**
 * Total number of publish attempts made for one document before its checkpoint publication is
 * reported as superseded. The checkpoint install is an optimistic compare and set against the
 * global commit sequence, so a concurrent commit anywhere in the replica makes it miss. Retrying
 * re-prepares from fresh durable state, which is safe because a lost install is a committed no-op.
 * Expressed as a total so the retry count and the reported `attempts` cannot disagree.
 */
const compactionPublishAttempts = 9

export const layerRegistration = (
  definition: ReplicaDefinition.Any
): Layer.Layer<
  never,
  never,
  Compaction.Compaction | ReplicaGate.ReplicaGate | SqlClient.SqlClient | WorkflowEngine.WorkflowEngine
> =>
  CompactReplica.toLayer(Effect.fn(function*(payload) {
    const compaction = yield* Compaction.Compaction
    const gate = yield* ReplicaGate.ReplicaGate
    const sql = yield* SqlClient.SqlClient
    const listDocuments = SqlSchema.findAll({
      Request: Schema.Void,
      Result: DocumentReference,
      execute: () =>
        sql`SELECT document_id AS documentId, document_type AS documentType
        FROM effect_local_documents ORDER BY document_id`
    })
    // Runs before the document work, not after it. Receipt reclamation is replica scoped and
    // independent of any document, so it must not sit behind a loop whose failures abort the
    // handler: only `CheckpointSuperseded` is caught below, so one corrupt checkpoint or one
    // unrecognised document type would otherwise starve it on every run, on exactly the replica
    // whose backups are already oversized. Running first also keeps it from replacing the superseded
    // report at the end of the handler.
    yield* Activity.make({
      name: "PruneCommandReceipts",
      error: ReplicaError.ReplicaError,
      execute: withActivityPermit(
        gate,
        payload.replicaIncarnation,
        Effect.gen(function*() {
          const prunedReceipts = yield* compaction.pruneCommandReceipts
          if (prunedReceipts > 0) {
            yield* Effect.logDebug("Reclaimed command receipts from superseded incarnations").pipe(
              Effect.annotateLogs({ prunedReceipts, replicaIncarnation: payload.replicaIncarnation })
            )
          }
        })
      )
    })
    const documents = yield* Activity.make({
      name: "ListDocuments",
      success: Schema.Array(DocumentReference),
      error: ReplicaError.ReplicaError,
      execute: withActivityPermit(
        gate,
        payload.replicaIncarnation,
        listDocuments(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
      )
    })
    const superseded: Array<Identity.DocumentId> = []
    for (const reference of documents) {
      const document = DocumentSet.get(definition.documents, reference.documentType)
      if (document === undefined) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "a document type in the replica definition",
            observed: reference.documentType
          })
        })
      }
      yield* Activity.make({
        name: `CompactDocument:${reference.documentId}`,
        error: ReplicaError.ReplicaError,
        execute: withActivityPermit(
          gate,
          payload.replicaIncarnation,
          Effect.gen(function*() {
            const compacted = yield* compaction.compact(document, reference.documentId)
            if (!compacted.published) {
              yield* Effect.logDebug("Checkpoint publication superseded by a concurrent commit").pipe(
                Effect.annotateLogs(reference)
              )
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointSuperseded({
                  documentIds: [reference.documentId],
                  attempts: compactionPublishAttempts
                })
              })
            }
            yield* compaction.prune(reference.documentId)
          })
        ).pipe(
          Effect.retry({
            times: compactionPublishAttempts - 1,
            while: (error) => error.reason._tag === "CheckpointSuperseded"
          })
        )
      }).pipe(
        Effect.catchReason("ReplicaError", "CheckpointSuperseded", () =>
          Effect.gen(function*() {
            yield* Effect.logWarning("Checkpoint publication superseded after every attempt").pipe(
              Effect.annotateLogs(reference)
            )
            superseded.push(reference.documentId)
          }))
      )
    }
    if (superseded.length > 0) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.CheckpointSuperseded({
          documentIds: superseded,
          attempts: compactionPublishAttempts
        })
      })
    }
  }))

export const layerRuntime: Layer.Layer<
  CompactionWorkflow,
  never,
  ReplicaGate.ReplicaGate | WorkflowEngine.WorkflowEngine
> = Layer.effect(
  CompactionWorkflow,
  Effect.gen(function*() {
    const gate = yield* ReplicaGate.ReplicaGate
    const engine = yield* WorkflowEngine.WorkflowEngine
    const validateExecution = (execution: Execution) =>
      Effect.gen(function*() {
        const executionId = yield* CompactReplica.executionId({
          operationId: execution.operationId,
          replicaIncarnation: execution.replicaIncarnation
        })
        if (executionId !== execution.executionId) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: executionId,
              observed: execution.executionId
            })
          })
        }
      })
    return CompactionWorkflow.of({
      execute: (operationId) =>
        Effect.gen(function*() {
          const permit = yield* gate.admit
          yield* gate.validate(permit)
          const executionId = yield* CompactReplica.execute({
            operationId,
            replicaIncarnation: permit.incarnation
          }, { discard: true }).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
          return { executionId, operationId, replicaIncarnation: permit.incarnation }
        }).pipe(Effect.scoped),
      poll: (execution) =>
        Effect.gen(function*() {
          yield* validateExecution(execution)
          const permit = yield* gate.admit
          yield* validateIncarnation(execution.replicaIncarnation, permit)
          yield* gate.validate(permit)
          const result = yield* CompactReplica.poll(execution.executionId).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
          return result
        }).pipe(Effect.scoped),
      interrupt: (execution) =>
        Effect.gen(function*() {
          const permit = yield* gate.admit
          yield* validateExecution(execution)
          yield* validateIncarnation(execution.replicaIncarnation, permit)
          yield* gate.validate(permit)
          yield* CompactReplica.interrupt(execution.executionId).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
        }).pipe(Effect.scoped),
      resume: (execution) =>
        Effect.gen(function*() {
          const permit = yield* gate.admit
          yield* validateExecution(execution)
          yield* validateIncarnation(execution.replicaIncarnation, permit)
          yield* gate.validate(permit)
          yield* CompactReplica.resume(execution.executionId).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
        }).pipe(Effect.scoped)
    })
  })
)
