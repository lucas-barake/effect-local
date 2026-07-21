import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as BackupStore from "./BackupStore.js"
import * as CommandExecutor from "./CommandExecutor.js"
import * as CommitPublisher from "./CommitPublisher.js"
import * as DocumentEntity from "./DocumentEntity.js"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as ReplicaGate from "./ReplicaGate.js"

const encode = <S extends Document.WireSchema,>(schema: S, value: S["Type"]) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(value).pipe(
    Effect.map((encoded) => new TextEncoder().encode(encoded)),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: {
            _tag: "StorageCorrupt",
            cause: { _tag: "SchemaCause", message: String(cause), path: [] }
          }
        })
      ))
  )

const decode = <S extends Document.WireSchema,>(schema: S, bytes: Uint8Array) =>
  Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.toCodecJson(schema))
  )(new TextDecoder().decode(bytes)).pipe(
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: {
            _tag: "StorageCorrupt",
            cause: { _tag: "SchemaCause", message: String(cause), path: [] }
          }
        })
      ))
  )

export const layer = (definition: ReplicaDefinition.Any): Layer.Layer<
  Replica.Replica,
  never,
  | BackupStore.BackupStore
  | CommitPublisher.CommitPublisher
  | CommandExecutor.CommandExecutor
  | DocumentStore.DocumentStore
  | QueryExecutor.QueryExecutor
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Sharding.Sharding
> =>
  Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const commands = yield* CommandExecutor.CommandExecutor
      const publisher = yield* CommitPublisher.CommitPublisher
      const documents = yield* DocumentStore.DocumentStore
      const entity = yield* DocumentEntity.DocumentEntity.client
      const queries = yield* QueryExecutor.QueryExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const limits = yield* ReplicaLimits.ReplicaLimits
      const commandLocks = yield* RcMap.make({
        capacity: limits.maxQueuedRpc,
        lookup: () => Semaphore.make(1)
      })

      const withPermit = <A, E, R,>(f: (permit: ReplicaGate.Permit) => Effect.Effect<A, E, R>) =>
        Effect.scoped(Effect.flatMap(gate.shared, f))

      const withCommandPermit = <A, E, R,>(
        commandId: Identity.CommandId,
        f: (permit: ReplicaGate.Permit) => Effect.Effect<A, E, R>
      ) =>
        withPermit((permit) =>
          Effect.scoped(
            RcMap.get(commandLocks, `${permit.incarnation}:${commandId}`).pipe(
              Effect.mapError(() =>
                new ReplicaError.ReplicaError({
                  reason: { _tag: "QuotaExceeded", resource: "in-flight commands", limit: limits.maxQueuedRpc }
                })
              ),
              Effect.flatMap((lock) => lock.withPermit(f(permit)))
            )
          )
        )

      const service: Replica.Service = {
        create: (document, options) =>
          withCommandPermit(options.commandId, (permit) =>
            Effect.gen(function*() {
              const documentId = Identity.documentIdFromCommandId(options.commandId)
              const encoded = yield* Document.encode(document, documentId, options.value)
              const requestHash = yield* CommandExecutor.createRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                document,
                documentId,
                encoded
              })
              const result = yield* entity(documentId).Create({
                replicaIncarnation: permit.incarnation,
                writerGeneration: permit.writerGeneration,
                commandId: options.commandId,
                documentType: document.name,
                requestHash,
                payload: yield* encode(document.schema, options.value)
              }).pipe(
                Effect.catchTags({
                  MailboxFull: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  AlreadyProcessingMessage: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  PersistenceError: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    )
                })
              )
              yield* publisher.publishPending
              return yield* decode(CommandOutcome.schema(Identity.DocumentId, Schema.Never), result)
            })),
        get: (document, documentId) =>
          withPermit(() =>
            Effect.acquireUseRelease(
              documents.load(document, documentId),
              (stored) => Effect.succeed(stored.snapshot),
              (stored) => Effect.sync(() => InternalAutomerge.free(stored.automerge))
            )
          ),
        mutate: <M extends Mutation.Any,>(mutation: M, options: {
          readonly commandId: Identity.CommandId
          readonly documentId: Identity.DocumentId
          readonly payload?: M["payload"]["Type"]
        }) =>
          withCommandPermit(options.commandId, (permit) =>
            Effect.gen(function*() {
              const payload = options.payload as M["payload"]["Type"]
              const encoded = yield* Schema.encodeEffect(mutation.payload)(payload).pipe(
                Effect.catchTag("SchemaError", (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "DocumentDecodeError",
                        documentId: options.documentId,
                        cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                      }
                    })
                  ))
              )
              const requestHash = yield* CommandExecutor.mutationRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                documentId: options.documentId,
                mutation,
                payload: encoded
              })
              const result = yield* entity(options.documentId).Mutate({
                replicaIncarnation: permit.incarnation,
                writerGeneration: permit.writerGeneration,
                commandId: options.commandId,
                documentType: mutation.document.name,
                mutationTag: mutation.name,
                requestHash,
                payload: yield* encode(mutation.payload, payload)
              }).pipe(
                Effect.catchTags({
                  MailboxFull: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  AlreadyProcessingMessage: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  PersistenceError: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    )
                })
              )
              yield* publisher.publishPending
              return yield* decode(CommandOutcome.schema(mutation.success, mutation.error), result) as Effect.Effect<
                CommandOutcome.CommandOutcome<M["success"]["Type"], M["error"]["Type"]>,
                ReplicaError.ReplicaError
              >
            })),
        delete: (document, options) =>
          withCommandPermit(options.commandId, (permit) =>
            Effect.gen(function*() {
              const requestHash = yield* CommandExecutor.deleteRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                document,
                documentId: options.documentId
              })
              const result = yield* entity(options.documentId).Delete({
                replicaIncarnation: permit.incarnation,
                writerGeneration: permit.writerGeneration,
                commandId: options.commandId,
                documentType: document.name,
                requestHash
              }).pipe(
                Effect.catchTags({
                  MailboxFull: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  AlreadyProcessingMessage: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    ),
                  PersistenceError: (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: String(cause) }
                        }
                      })
                    )
                })
              )
              yield* publisher.publishPending
              return yield* decode(CommandOutcome.schema(Schema.Void, Schema.Never), result)
            })),
        query: <Q extends Query.Any,>(
          query: Q,
          ...payload: [Q["payload"]["Type"]] extends [void] ? readonly [] : readonly [payload: Q["payload"]["Type"]]
        ) => withPermit(() => queries.execute(query, payload[0] as Q["payload"]["Type"])),
        lookupMutation: (mutation, commandId) =>
          withPermit((permit) => commands.lookupMutation(mutation, commandId, permit)),
        lookupCreate: (_document, commandId) => withPermit((permit) => commands.lookupCreate(commandId, permit)),
        lookupDelete: (_document, commandId) => withPermit((permit) => commands.lookupDelete(commandId, permit)),
        flush: withPermit(() => publisher.publishPending).pipe(Effect.asVoid),
        status: Stream.succeed({ _tag: "Ready", pendingCommands: 0 }),
        exportBackup: backups.export,
        restoreBackup: (options) =>
          backups.restore(options).pipe(
            Effect.andThen(publisher.invalidate(ReplicaDefinition.invalidationKeys(definition)))
          ),
        exportDocument: (document, documentId) =>
          withPermit(() =>
            Effect.acquireUseRelease(
              documents.load(document, documentId),
              (stored) =>
                Effect.succeed({
                  documentName: document.name,
                  schemaVersion: document.version,
                  value: stored.encoded
                }),
              (stored) => Effect.sync(() => InternalAutomerge.free(stored.automerge))
            )
          ),
        importDocument: (document, options) =>
          Effect.gen(function*() {
            if (options.value.documentName !== document.name || options.value.schemaVersion !== document.version) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "BackupInvalid",
                  cause: { _tag: "SchemaCause", message: "Portable document definition mismatch", path: [] }
                }
              })
            }
            const documentId = Identity.documentIdFromCommandId(options.commandId)
            const value = yield* Document.decode(document, documentId, options.value.value)
            return yield* service.create(document, { commandId: options.commandId, value })
          })
      }
      return service
    })
  )
