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

const operation = {
  replicaIncarnation: Identity.ReplicaIncarnation,
  operationId: OperationId
}

export const CompactReplica = Workflow.make("EffectLocal/CompactReplica", {
  payload: operation,
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
                    cause: new ReplicaError.SqlCause({
                      message: String(cause),
                      code: null
                    })
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause: new ReplicaError.SchemaCause({
                      message: String(cause),
                      path: []
                    })
                  })
                })
              )
          })
        )
      )
    })
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
          compaction.compact(document, reference.documentId).pipe(
            Effect.andThen(compaction.prune(reference.documentId)),
            Effect.asVoid
          )
        )
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
          const permit = yield* gate.shared
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
          const permit = yield* gate.shared
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
          const permit = yield* gate.shared
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
          const permit = yield* gate.shared
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
