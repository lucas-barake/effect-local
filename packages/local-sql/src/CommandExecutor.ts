import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as ReplicaGate from "./ReplicaGate.js"

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))

const ReceiptRow = Schema.Struct({
  request_hash: Schema.String,
  mutation_name: Schema.String,
  result: Schema.Uint8Array
})

type ReceiptRow = typeof ReceiptRow.Type

const encodeResult = <S extends Document.WireSchema,>(schema: S, value: S["Type"]) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(value).pipe(
    Effect.map((encoded) => new TextEncoder().encode(encoded)),
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageCorrupt({
          cause
        })
      })
    )
  )

const decodeResult = <S extends Document.WireSchema,>(schema: S, bytes: Uint8Array) =>
  Schema.decodeEffect(
    Schema.fromJsonString(Schema.toCodecJson(schema))
  )(new TextDecoder().decode(bytes)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageCorrupt({
          cause
        })
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
  readonly payload: M["payloadSchema"]["Encoded"]
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
      readonly payload: M["payloadSchema"]["Type"]
      readonly permit: ReplicaGate.Permit
      readonly requestHash: string
    }
  ) => Effect.Effect<
    CommandOutcome.CommandOutcome<M["successSchema"]["Type"], M["errorSchema"]["Type"]>,
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
    CommandOutcome.CommandOutcome<M["successSchema"]["Type"], M["errorSchema"]["Type"]>,
    ReplicaError.ReplicaError
  >
  readonly lookupDelete: (
    commandId: Identity.CommandId,
    permit: ReplicaGate.Permit
  ) => Effect.Effect<CommandOutcome.CommandOutcome<void>, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/CommandExecutor") {}

export type MutationHandlers<D extends ReplicaDefinition.Any,> = Context.Service.Identifier<
  D["mutations"][number]["handler"]
>

export const layer = <D extends ReplicaDefinition.Any,>(definition: D): Layer.Layer<
  CommandExecutor,
  never,
  | DocumentStore.DocumentStore
  | Crypto.Crypto
  | ProjectionStore.ProjectionStore
  | ReplicaGate.ReplicaGate
  | SqlClient.SqlClient
  | MutationHandlers<D>
> =>
  Layer.effect(
    CommandExecutor,
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const crypto = yield* Crypto.Crypto
      const projections = yield* ProjectionStore.ProjectionStore
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const handlerContext = yield* Effect.context<MutationHandlers<D>>()
      const handlers = new Map<string, Mutation.Handler<any, any, any, any>>()
      for (const mutation of definition.mutations) {
        handlers.set(mutation.name, Context.get(handlerContext, mutation.handler))
      }

      const findReceipt = SqlSchema.findOneOption({
        Request: Schema.Struct({
          commandId: Identity.CommandId,
          incarnation: Identity.ReplicaIncarnation
        }),
        Result: ReceiptRow,
        execute: ({ commandId, incarnation }) =>
          sql`SELECT request_hash, mutation_name, result FROM effect_local_command_receipts
            WHERE replica_incarnation = ${incarnation} AND command_id = ${commandId}`
      })
      const lookup = (commandId: Identity.CommandId, permit: ReplicaGate.Permit) =>
        findReceipt({ commandId, incarnation: permit.incarnation }).pipe(
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
        ${options.result}, ${options.documentId}, ${Schema.encodeSync(Heads)(options.heads)}, ${options.commitSequence}
      )`.pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause
                })
              })
            ),
            Effect.asVoid
          )

      const decodeReceipt = <A extends Document.WireSchema, E extends Document.WireSchema,>(
        success: A,
        error: E,
        receipt: ReceiptRow
      ) => decodeResult(CommandOutcome.schema(success, error), receipt.result)

      const operationLabel = (mutationName: string) =>
        mutationName === "$create" ? "create" : mutationName === "$delete" ? "delete" : mutationName

      const requireOperation = (
        commandId: Identity.CommandId,
        receipt: ReceiptRow,
        expected: string
      ) =>
        receipt.mutation_name === expected
          ? Effect.void
          : Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ReceiptOperationMismatch({
                commandId,
                expected: operationLabel(expected),
                observed: operationLabel(receipt.mutation_name)
              })
            })
          )

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
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (Option.isSome(existing)) {
                if (existing.value.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                  })
                }
                return yield* decodeReceipt(Identity.DocumentId, Schema.Never, existing.value)
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
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ))
          ),
        mutate: (mutation, options) =>
          sql.withTransaction(withDocuments((track) =>
            Effect.gen(function*() {
              yield* gate.validate(options.permit)
              const payload = yield* Schema.encodeEffect(mutation.payloadSchema)(options.payload).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.DocumentDecodeError({
                      documentId: options.documentId,
                      cause
                    })
                  })
                )
              )
              const expectedHash = yield* mutationRequestHash({
                incarnation: options.permit.incarnation,
                commandId: options.commandId,
                documentId: options.documentId,
                mutation,
                payload
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (Option.isSome(existing)) {
                if (existing.value.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                  })
                }
                return yield* decodeReceipt(mutation.successSchema, mutation.errorSchema, existing.value)
              }
              const durable = yield* store.load(mutation.document, options.documentId).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              const handler = handlers.get(mutation.name)
              if (handler === undefined) {
                return yield* Effect.die(new Error(`Missing mutation handler: ${mutation.name}`))
              }
              let handlerResult!: Result.Result<
                (typeof mutation)["successSchema"]["Type"],
                (typeof mutation)["errorSchema"]["Type"]
              >
              const staged = track(
                yield* store.stage(durable, (draft) => {
                  const result = handler({ draft, payload: options.payload, current: durable.snapshot.value })
                  handlerResult = SchemaAST.isNever(mutation.errorSchema.ast)
                    ? Result.succeed(result)
                    : result
                })
              )
              if (Result.isFailure(handlerResult)) {
                const outcome = CommandOutcome.rejected(options.commandId, handlerResult.failure)
                const result = yield* encodeResult(
                  CommandOutcome.schema(mutation.successSchema, mutation.errorSchema),
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
              const persisted = yield* store.persist(mutation.document, options.documentId, durable, staged).pipe(
                Effect.map((stored) => ({ ...stored, automerge: track(stored.automerge) }))
              )
              yield* projections.replaceDocument(mutation.document, persisted.snapshot, persisted.commitSequence)
              const outcome = CommandOutcome.durablyCommitted(options.commandId, handlerResult.success)
              const result = yield* encodeResult(
                CommandOutcome.schema(mutation.successSchema, mutation.errorSchema),
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
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
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
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              if (expectedHash !== options.requestHash) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                })
              }
              const existing = yield* lookup(options.commandId, options.permit)
              if (Option.isSome(existing)) {
                if (existing.value.request_hash !== expectedHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CommandIdConflict({ commandId: options.commandId })
                  })
                }
                return yield* decodeReceipt(Schema.Void, Schema.Never, existing.value)
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
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ))
          ),
        lookupCreate: (commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            Option.isNone(receipt)
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : requireOperation(commandId, receipt.value, "$create").pipe(
                Effect.andThen(decodeReceipt(Identity.DocumentId, Schema.Never, receipt.value))
              )
          )),
        lookupMutation: (mutation, commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            Option.isNone(receipt)
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : requireOperation(commandId, receipt.value, mutation.name).pipe(
                Effect.andThen(decodeReceipt(mutation.successSchema, mutation.errorSchema, receipt.value))
              )
          )),
        lookupDelete: (commandId, permit) =>
          lookup(commandId, permit).pipe(Effect.flatMap((receipt) =>
            Option.isNone(receipt)
              ? Effect.succeed(CommandOutcome.unknown(commandId))
              : requireOperation(commandId, receipt.value, "$delete").pipe(
                Effect.andThen(decodeReceipt(Schema.Void, Schema.Never, receipt.value))
              )
          ))
      })
    })
  )
