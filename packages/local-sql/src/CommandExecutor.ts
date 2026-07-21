import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Predicate from "effect/Predicate"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as ReplicaGate from "./ReplicaGate.js"

interface ReceiptRow {
  readonly commit_sequence: number
  readonly document_id: string
  readonly heads: string
  readonly request_hash: string
  readonly result: Uint8Array
}

class DomainRejected {
  readonly _tag = "DomainRejected"
  readonly error: unknown
  constructor(error: unknown) {
    this.error = error
  }
}

const encodeResult = (schema: Document.WireSchema, value: unknown) =>
  Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(value).pipe(
    Effect.map((encoded) => new TextEncoder().encode(encoded)),
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "StorageCorrupt",
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
    )
  )

const decodeResult = (schema: Document.WireSchema, bytes: Uint8Array) =>
  Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.toCodecJson(schema))
  )(new TextDecoder().decode(bytes)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: { _tag: "StorageCorrupt", cause: { _tag: "SchemaCause", message: String(cause), path: [] } }
      })
    )
  )

const withDocuments = <A, E, R,>(
  use: (track: <D extends InternalAutomerge.AnyDocument,>(document: D) => D) => Effect.Effect<A, E, R>
) =>
  Effect.suspend(() => {
    const documents = new Set<InternalAutomerge.AnyDocument>()
    const track = <D extends InternalAutomerge.AnyDocument,>(document: D): D => {
      documents.add(document)
      return document
    }
    return use(track).pipe(
      Effect.ensuring(Effect.sync(() => {
        for (const document of documents) InternalAutomerge.free(document)
      }))
    )
  })

export const createRequestHash = <D extends Document.Any,>(options: {
  readonly incarnation: Identity.ReplicaIncarnation
  readonly commandId: Identity.CommandId
  readonly document: D
  readonly documentId: Identity.DocumentId
  readonly encoded: D["schema"]["Encoded"]
}) =>
  Canonical.digest({
    incarnation: options.incarnation,
    commandId: options.commandId,
    document: options.document.name,
    documentId: options.documentId,
    operation: "create",
    value: options.encoded,
    version: options.document.version
  })

export const mutationRequestHash = <M extends Mutation.Any,>(options: {
  readonly incarnation: Identity.ReplicaIncarnation
  readonly commandId: Identity.CommandId
  readonly documentId: Identity.DocumentId
  readonly mutation: M
  readonly payload: M["payload"]["Encoded"]
}) =>
  Canonical.digest({
    incarnation: options.incarnation,
    commandId: options.commandId,
    document: options.mutation.document.name,
    documentId: options.documentId,
    mutation: options.mutation.name,
    operation: "mutation",
    payload: options.payload,
    version: options.mutation.version
  })

export const deleteRequestHash = (options: {
  readonly incarnation: Identity.ReplicaIncarnation
  readonly commandId: Identity.CommandId
  readonly document: Document.Any
  readonly documentId: Identity.DocumentId
}) =>
  Canonical.digest({
    incarnation: options.incarnation,
    commandId: options.commandId,
    document: options.document.name,
    documentId: options.documentId,
    operation: "delete",
    version: options.document.version
  })

export class CommandExecutor extends Context.Service<CommandExecutor, {
  readonly create: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
      readonly permit: ReplicaGate.Permit
      readonly requestHash: string
      readonly value: D["schema"]["Type"]
    }
  ) => Effect.Effect<CommandOutcome.CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
  readonly mutate: <M extends Mutation.Any,>(
    mutation: M,
    options: {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
      readonly payload: M["payload"]["Type"]
      readonly permit: ReplicaGate.Permit
      readonly requestHash: string
    }
  ) => Effect.Effect<
    CommandOutcome.CommandOutcome<M["success"]["Type"], M["error"]["Type"]>,
    ReplicaError.ReplicaError
  >
  readonly delete: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
      readonly permit: ReplicaGate.Permit
      readonly requestHash: string
    }
  ) => Effect.Effect<CommandOutcome.CommandOutcome<void>, ReplicaError.ReplicaError>
  readonly lookupCreate: (
    commandId: Identity.CommandId,
    permit: ReplicaGate.Permit
  ) => Effect.Effect<CommandOutcome.CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
  readonly lookupMutation: <M extends Mutation.Any,>(
    mutation: M,
    commandId: Identity.CommandId,
    permit: ReplicaGate.Permit
  ) => Effect.Effect<
    CommandOutcome.CommandOutcome<M["success"]["Type"], M["error"]["Type"]>,
    ReplicaError.ReplicaError
  >
  readonly lookupDelete: (
    commandId: Identity.CommandId,
    permit: ReplicaGate.Permit
  ) => Effect.Effect<CommandOutcome.CommandOutcome<void>, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/CommandExecutor") {}

export type MutationHandlers<D extends ReplicaDefinition.Any,> = D["mutations"][number] extends infer M
  ? M extends Mutation.Mutation<infer Name, infer Doc, infer Payload, infer Success, infer Error>
    ? Mutation.HandlerService<Name, Doc, Payload, Success, Error>
  : never
  : never

export const layer = <D extends ReplicaDefinition.Any,>(definition: D): Layer.Layer<
  CommandExecutor,
  never,
  | DocumentStore.DocumentStore
  | ProjectionStore.ProjectionStore
  | ReplicaGate.ReplicaGate
  | SqlClient.SqlClient
  | MutationHandlers<D>
> =>
  Layer.effect(
    CommandExecutor,
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const projections = yield* ProjectionStore.ProjectionStore
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const handlerContext = yield* Effect.context<MutationHandlers<D>>()
      const handlers = new Map<string, Mutation.Handler<any, any, any, any>>()
      for (const mutation of definition.mutations) {
        handlers.set(mutation.name, Context.get(handlerContext, mutation.handler))
      }

      const lookup = (commandId: Identity.CommandId, permit: ReplicaGate.Permit) =>
        sql<ReceiptRow>`SELECT request_hash, result, document_id, heads, commit_sequence
        FROM effect_local_command_receipts
        WHERE replica_incarnation = ${permit.incarnation} AND command_id = ${commandId}`.pipe(
          Effect.map((rows) => rows[0]),
          Effect.mapError((cause) =>
            new ReplicaError.ReplicaError({
              reason: {
                _tag: "StorageUnavailable",
                cause: { _tag: "SqlCause", message: String(cause), code: null }
              }
            })
          )
        )

      const persistReceipt = (options: {
        readonly commandId: Identity.CommandId
        readonly commitSequence: Identity.CommitSequence
        readonly documentId: Identity.DocumentId
        readonly heads: ReadonlyArray<string>
        readonly mutationName: string
        readonly permit: ReplicaGate.Permit
        readonly requestHash: string
        readonly result: Uint8Array
      }) =>
        sql`INSERT INTO effect_local_command_receipts (
        replica_incarnation, command_id, request_hash, mutation_name, result,
        document_id, heads, commit_sequence
      ) VALUES (
        ${options.permit.incarnation}, ${options.commandId}, ${options.requestHash}, ${options.mutationName},
        ${options.result}, ${options.documentId}, ${JSON.stringify(options.heads)}, ${options.commitSequence}
      )`.pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            ),
            Effect.asVoid
          )

      const decodeReceipt = <A extends Document.WireSchema, E extends Document.WireSchema,>(
        success: A,
        error: E,
        receipt: ReceiptRow
      ) =>
        decodeResult(CommandOutcome.schema(success, error), receipt.result) as Effect.Effect<
          CommandOutcome.CommandOutcome<A["Type"], E["Type"]>,
          ReplicaError.ReplicaError
        >

      return CommandExecutor.of({
        create: (document, options) =>
          sql.withTransaction(withDocuments((track) =>
            Effect.gen(function*() {
              yield* gate.validate(options.permit)
              const encoded = yield* Document.encode(document, options.documentId, options.value)
              const expectedHash = yield* createRequestHash({
                incarnation: options.permit.incarnation,
                commandId: options.commandId,
                document,
                documentId: options.documentId,
                encoded
              })
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (existing !== undefined) {
                if (existing.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                  })
                }
                return yield* decodeReceipt(Identity.DocumentId, Schema.Never, existing)
              }
              const stored = yield* store.create(document, options.documentId, options.value).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              yield* projections.replaceDocument(document, stored.snapshot, stored.commitSequence)
              const outcome = CommandOutcome.durablyCommitted(options.commandId, options.documentId)
              const result = yield* encodeResult(CommandOutcome.schema(Identity.DocumentId, Schema.Never), outcome)
              yield* persistReceipt({
                commandId: options.commandId,
                commitSequence: stored.commitSequence,
                documentId: options.documentId,
                heads: stored.materializedHeads,
                mutationName: "$create",
                permit: options.permit,
                requestHash: expectedHash,
                result
              })
              return outcome
            })
          )).pipe(
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageUnavailable",
                    cause: { _tag: "SqlCause", message: String(cause), code: null }
                  }
                })
              ))
          ),
        mutate: (mutation, options) =>
          sql.withTransaction(withDocuments((track) =>
            Effect.gen(function*() {
              yield* gate.validate(options.permit)
              const payload = yield* Schema.encodeEffect(mutation.payload)(options.payload).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "DocumentDecodeError",
                      documentId: options.documentId,
                      cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                    }
                  })
                )
              )
              const expectedHash = yield* mutationRequestHash({
                incarnation: options.permit.incarnation,
                commandId: options.commandId,
                documentId: options.documentId,
                mutation,
                payload
              })
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (existing !== undefined) {
                if (existing.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                  })
                }
                return yield* decodeReceipt(mutation.success, mutation.error, existing)
              }
              const durable = yield* store.load(mutation.document, options.documentId).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              const handler = handlers.get(mutation.name)
              if (handler === undefined) {
                return yield* Effect.die(new Error(`Missing mutation handler: ${mutation.name}`))
              }
              let success: unknown
              const staged = yield* store.stage(durable, (draft) => {
                const result = handler({ draft, payload: options.payload, current: durable.snapshot.value })
                if (mutation.error === Schema.Never) {
                  success = result
                } else if (Result.isFailure(result)) {
                  throw new DomainRejected(result.failure)
                } else {
                  success = result.success
                }
              }).pipe(Effect.catch((cause) =>
                Predicate.isTagged(cause, "DomainRejected") && Predicate.hasProperty(cause, "error")
                  ? Effect.succeed(cause)
                  : Effect.die(cause)
              ))
              if (Predicate.isTagged(staged, "DomainRejected") && Predicate.hasProperty(staged, "error")) {
                const outcome = CommandOutcome.rejected(options.commandId, staged.error)
                const result = yield* encodeResult(
                  CommandOutcome.schema(mutation.success, mutation.error) as unknown as Document.WireSchema,
                  outcome
                )
                yield* persistReceipt({
                  commandId: options.commandId,
                  commitSequence: durable.commitSequence,
                  documentId: options.documentId,
                  heads: durable.materializedHeads,
                  mutationName: mutation.name,
                  permit: options.permit,
                  requestHash: expectedHash,
                  result
                })
                return outcome
              }
              track(staged)
              const persisted = yield* store.persist(mutation.document, options.documentId, durable, staged).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              yield* projections.replaceDocument(mutation.document, persisted.snapshot, persisted.commitSequence)
              const outcome = CommandOutcome.durablyCommitted(options.commandId, success)
              const result = yield* encodeResult(
                CommandOutcome.schema(mutation.success, mutation.error) as unknown as Document.WireSchema,
                outcome
              )
              yield* persistReceipt({
                commandId: options.commandId,
                commitSequence: persisted.commitSequence,
                documentId: options.documentId,
                heads: persisted.materializedHeads,
                mutationName: mutation.name,
                permit: options.permit,
                requestHash: expectedHash,
                result
              })
              return outcome
            })
          )).pipe(
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageUnavailable",
                    cause: { _tag: "SqlCause", message: String(cause), code: null }
                  }
                })
              ))
          ),
        delete: (document, options) =>
          sql.withTransaction(withDocuments((track) =>
            Effect.gen(function*() {
              yield* gate.validate(options.permit)
              const expectedHash = yield* deleteRequestHash({
                incarnation: options.permit.incarnation,
                commandId: options.commandId,
                document,
                documentId: options.documentId
              })
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (existing !== undefined) {
                if (existing.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: { _tag: "CommandIdConflict", commandId: options.commandId }
                  })
                }
                return yield* decodeReceipt(Schema.Void, Schema.Never, existing)
              }
              const durable = yield* store.load(document, options.documentId).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              const staged = track(yield* store.tombstone(durable))
              const persisted = yield* store.persist(document, options.documentId, durable, staged).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              yield* projections.replaceDocument(document, persisted.snapshot, persisted.commitSequence)
              const outcome = CommandOutcome.durablyCommitted(options.commandId, undefined)
              const result = yield* encodeResult(CommandOutcome.schema(Schema.Void, Schema.Never), outcome)
              yield* persistReceipt({
                commandId: options.commandId,
                commitSequence: persisted.commitSequence,
                documentId: options.documentId,
                heads: persisted.materializedHeads,
                mutationName: "$delete",
                permit: options.permit,
                requestHash: expectedHash,
                result
              })
              return outcome
            })
          )).pipe(
            Effect.catch((cause) =>
              Schema.is(ReplicaError.ReplicaError)(cause)
                ? Effect.fail(cause)
                : Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageUnavailable",
                      cause: { _tag: "SqlCause", message: String(cause), code: null }
                    }
                  })
                )
            )
          ),
        lookupCreate: (commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            receipt === undefined
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : decodeReceipt(Identity.DocumentId, Schema.Never, receipt)
          )),
        lookupMutation: (mutation, commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            receipt === undefined
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : decodeReceipt(mutation.success, mutation.error, receipt)
          )),
        lookupDelete: (commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            receipt === undefined
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : decodeReceipt(Schema.Void, Schema.Never, receipt)
          ))
      })
    })
  )
