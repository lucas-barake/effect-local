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
import * as PeerSync from "./PeerSync.js"
import * as ReplicaGate from "./ReplicaGate.js"

/**
 * Re-exported from `Compaction`, which owns the definition because the history rewrite keys its
 * durable marker on this id. The brand identifier is unchanged, so this stays the same type it
 * always was for every consumer.
 */
export const OperationId = Compaction.OperationId
export type OperationId = Compaction.OperationId

export const CompactReplica = Workflow.make("EffectLocal/CompactReplica", {
  payload: {
    replicaIncarnation: Identity.ReplicaIncarnation,
    operationId: OperationId
  },
  success: Schema.Void,
  error: ReplicaError.ReplicaError,
  idempotencyKey: (payload) => `${payload.replicaIncarnation}:${payload.operationId}`
})

/**
 * Operator driven history rewrite for a single document.
 *
 * `Compaction.rewriteHistory` is destructive: it discards every prior change and checkpoint, so
 * running it twice for what an operator meant as one request costs a second lineage and a second
 * round of peer invalidation. The idempotency key is the first half of what prevents that.
 * `Workflow.execute` hashes `` `${_tag}-${idempotencyKey(payload)}` `` into the execution id, so a
 * repeated request with the same operation id resolves to the same execution and returns the
 * recorded result instead of re-entering the handler at all.
 *
 * That only dedupes REQUESTS. The second half lives in `rewriteHistory` itself, which records the
 * lineage it minted against `(replica_incarnation, operationId)` inside the rewrite's own
 * transaction: a crash between that commit and the journaling of this activity's result re-runs the
 * ACTIVITY within the same execution, and the marker is what makes the re-run return the first
 * lineage instead of minting a second one. Because that marker is keyed by operation and not by
 * document, an operation id is bound to the first document it rewrites and reusing it for another
 * document is rejected; the document stays in this key so the workflow's own dedupe still collapses
 * retries per document.
 *
 * Deliberately not a `Replica` member and not an entity message: a rewrite must be reachable only
 * from an operator, never from the page or from a peer.
 */
export const RewriteDocumentHistory = Workflow.make("EffectLocal/RewriteDocumentHistory", {
  payload: {
    replicaIncarnation: Identity.ReplicaIncarnation,
    documentId: Identity.DocumentId,
    operationId: OperationId
  },
  success: Identity.DocumentLineage,
  error: ReplicaError.ReplicaError,
  idempotencyKey: (payload) => `${payload.replicaIncarnation}:${payload.documentId}:${payload.operationId}`
})

export interface Execution {
  readonly executionId: string
  readonly operationId: OperationId
  readonly replicaIncarnation: Identity.ReplicaIncarnation
}

/**
 * Handle for a workflow whose idempotency key is scoped to one document as well as one incarnation
 * and one operation.
 */
export interface DocumentExecution extends Execution {
  readonly documentId: Identity.DocumentId
}

export class CompactionWorkflow extends Context.Service<CompactionWorkflow, {
  readonly execute: (operationId: OperationId) => Effect.Effect<Execution, ReplicaError.ReplicaError>
  readonly poll: (
    execution: Execution
  ) => Effect.Effect<Option.Option<Workflow.Result<void, ReplicaError.ReplicaError>>, ReplicaError.ReplicaError>
  readonly interrupt: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly resume: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaWorkflow/CompactionWorkflow") {}

/**
 * Operator surface for {@link RewriteDocumentHistory}.
 *
 * The mirror of `CompactionWorkflow`, with the document the rewrite targets added to both the
 * request and the handle. `execute` reports the handle rather than the lineage: the rewrite is
 * durable and outlives this caller, so the lineage is read back through `poll`.
 */
export class HistoryRewriteWorkflow extends Context.Service<HistoryRewriteWorkflow, {
  readonly execute: (
    documentId: Identity.DocumentId,
    operationId: OperationId
  ) => Effect.Effect<DocumentExecution, ReplicaError.ReplicaError>
  readonly poll: (
    execution: DocumentExecution
  ) => Effect.Effect<
    Option.Option<Workflow.Result<Identity.DocumentLineage, ReplicaError.ReplicaError>>,
    ReplicaError.ReplicaError
  >
  readonly interrupt: (execution: DocumentExecution) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly resume: (execution: DocumentExecution) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaWorkflow/HistoryRewriteWorkflow") {}

const validateIncarnation = (
  expected: Identity.ReplicaIncarnation,
  permit: ReplicaGate.Permit
) =>
  (() => {
    if (permit.incarnation === expected) return (Effect.void)
    return (Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: `replica incarnation ${expected}`,
          observed: `replica incarnation ${permit.incarnation}`
        })
      })
    ))
  })()

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
            return undefined
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
    return undefined
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
        return undefined
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

export const layerHistoryRewriteRegistration = (
  definition: ReplicaDefinition.Any
): Layer.Layer<
  never,
  never,
  | Compaction.Compaction
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | SqlClient.SqlClient
  | WorkflowEngine.WorkflowEngine
> =>
  RewriteDocumentHistory.toLayer(Effect.fn(function*(payload) {
    const compaction = yield* Compaction.Compaction
    const peerSync = yield* PeerSync.PeerSync
    const gate = yield* ReplicaGate.ReplicaGate
    const sql = yield* SqlClient.SqlClient
    // The document type is read from storage rather than carried on the payload. It is what selects
    // the `Document` the rewrite decodes and re-encodes with, so taking it from the caller would let
    // a wrong type reach `recover` and re-root a document under a definition that is not its own.
    const findDocument = SqlSchema.findOne({
      Request: Identity.DocumentId,
      Result: DocumentReference,
      execute: (documentId) =>
        sql`SELECT document_id AS documentId, document_type AS documentType
        FROM effect_local_documents WHERE document_id = ${documentId}`
    })
    // One activity, not two. The lookup is cheap and its result is only ever used by the rewrite, so
    // journaling it separately would add a durable record that can go stale against the document row
    // the rewrite actually swaps.
    return yield* Activity.make({
      name: "RewriteDocumentHistory",
      success: Identity.DocumentLineage,
      error: ReplicaError.ReplicaError,
      execute: withActivityPermit(
        gate,
        payload.replicaIncarnation,
        Effect.gen(function*() {
          const reference = yield* findDocument(payload.documentId).pipe(
            Effect.catchTags({
              NoSuchElementError: () =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.DocumentNotFound({ documentId: payload.documentId })
                  })
                ),
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({ cause })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({ cause })
                  })
                )
            })
          )
          const document = DocumentSet.get(definition.documents, reference.documentType)
          if (document === undefined) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "a document type in the replica definition",
                observed: reference.documentType
              })
            })
          }
          // `rewriteHistory` acquires no gate lock of its own and documents that the caller owns one.
          // `withActivityPermit` is that owner, so the permit is held for the whole rewrite without
          // the operator ever handling one. It is a shared permit, never a claim: claiming would bump
          // the replica incarnation and writer generation and invalidate every live workflow handle
          // and peer session for what is one document's maintenance.
          //
          // The rewrite must take the same per document lock as peer generation and receive before
          // it can commit. Acquiring that lock only after the commit lets an already admitted peer
          // operation load the new root under the old lineage and persist discarded history back
          // into it. `withDocumentInvalidation` also clears every session's old sync state before
          // releasing the lock, with an uninterruptible handoff after the interruptible rewrite.
          return yield* peerSync.withDocumentInvalidation(
            payload.documentId,
            compaction.rewriteHistory(document, payload.documentId, payload.operationId)
          )
        })
      )
    })
  }))

export const layerHistoryRewriteRuntime: Layer.Layer<
  HistoryRewriteWorkflow,
  never,
  ReplicaGate.ReplicaGate | WorkflowEngine.WorkflowEngine
> = Layer.effect(
  HistoryRewriteWorkflow,
  Effect.gen(function*() {
    const gate = yield* ReplicaGate.ReplicaGate
    const engine = yield* WorkflowEngine.WorkflowEngine
    const validateExecution = (execution: DocumentExecution) =>
      Effect.gen(function*() {
        const executionId = yield* RewriteDocumentHistory.executionId({
          documentId: execution.documentId,
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
        return undefined
      })
    return HistoryRewriteWorkflow.of({
      execute: (documentId, operationId) =>
        Effect.gen(function*() {
          const permit = yield* gate.admit
          yield* gate.validate(permit)
          const executionId = yield* RewriteDocumentHistory.execute({
            documentId,
            operationId,
            replicaIncarnation: permit.incarnation
          }, { discard: true }).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
          return { documentId, executionId, operationId, replicaIncarnation: permit.incarnation }
        }).pipe(Effect.scoped),
      poll: (execution) =>
        Effect.gen(function*() {
          yield* validateExecution(execution)
          const permit = yield* gate.admit
          yield* validateIncarnation(execution.replicaIncarnation, permit)
          yield* gate.validate(permit)
          const result = yield* RewriteDocumentHistory.poll(execution.executionId).pipe(
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
          yield* RewriteDocumentHistory.interrupt(execution.executionId).pipe(
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
          yield* RewriteDocumentHistory.resume(execution.executionId).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
          )
          yield* gate.validate(permit)
        }).pipe(Effect.scoped)
    })
  })
)
