import type * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { RpcClient } from "effect/unstable/rpc"
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as Wire from "./internal/wire.js"
import * as ReplicaRpc from "./ReplicaRpc.js"

export interface Service extends Replica.Service {
  readonly ownerEpoch: string
  readonly invalidations: Stream.Stream<ReplicaRpc.Invalidation, ReplicaError.ReplicaError>
}

export class ReplicaClient extends Context.Service<ReplicaClient, Service>()(
  "@lucas-barake/effect-local-browser/ReplicaClient"
) {}

const isTransient = (error: ReplicaError.ReplicaError) => error.reason._tag === "StorageUnavailable"

const recoverCommand = <A,>(
  commandId: Identity.CommandId,
  dispatch: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>,
  lookup: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>
): Effect.Effect<A | CommandOutcome.OutcomeUnknown, ReplicaError.ReplicaError> =>
  dispatch.pipe(
    Effect.catchTag("RpcClientError", () =>
      lookup.pipe(
        Effect.catchTag("RpcClientError", () => Effect.succeed(CommandOutcome.unknown(commandId)))
      ))
  )

export const fromRpcClient = (
  definition: ReplicaDefinition.Any,
  rpc: RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>
): Effect.Effect<Service, ReplicaError.ReplicaError, Scope.Scope> =>
  Effect.gen(function*() {
    const sessionId = Identity.makeSessionId()
    const lease = yield* Effect.acquireRelease(
      rpc.OpenSession({ sessionId, definitionHash: definition.hash }).pipe(
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
            })
          ))
      ),
      () => Effect.ignore(rpc.CloseSession({ sessionId }))
    )
    if (lease.protocolVersion !== ReplicaRpc.protocolVersion || lease.definitionHash !== definition.hash) {
      return yield* new ReplicaError.ReplicaError({
        reason: {
          _tag: "ProtocolMismatch",
          expected: `${ReplicaRpc.protocolVersion}:${definition.hash}`,
          observed: `${lease.protocolVersion}:${lease.definitionHash}`
        }
      })
    }
    const retrySchedule = Schedule.exponential(250).pipe(Schedule.upTo({ times: 3 }))
    const sessionFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    yield* Effect.sleep(lease.leaseMillis / 2).pipe(
      Effect.andThen(
        rpc.RenewSession({ sessionId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            )),
          Effect.retry({ times: 3, schedule: retrySchedule, while: isTransient })
        )
      ),
      Effect.forever,
      Effect.tapError((error) => Deferred.fail(sessionFailure, error)),
      Effect.tapCause(Effect.logError),
      Effect.ignore,
      Effect.forkScoped
    )
    const allInvalidationKeys = ReplicaDefinition.invalidationKeys(definition)
    const fullRefresh = {
      _tag: "FullRefreshRequired" as const,
      ownerEpoch: lease.ownerEpoch,
      keys: allInvalidationKeys
    }
    const invalidationMessages = (attempt: number): Stream.Stream<
      ReplicaRpc.InvalidationMessage,
      ReplicaError.ReplicaError
    > =>
      rpc.Invalidations({ sessionId, ownerEpoch: lease.ownerEpoch }).pipe(
        Stream.catchTag("RpcClientError", (error) =>
          Stream.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
            })
          )),
        Stream.catch((error) =>
          isTransient(error) && attempt < 3
            ? Stream.unwrap(
              Effect.sleep(250 * 2 ** attempt).pipe(
                Effect.map(() => invalidationMessages(attempt + 1))
              )
            )
            : Stream.fail(error)
        )
      )
    const invalidations = invalidationMessages(0).pipe(
      Stream.mapAccum(
        (): {
          readonly watermark: Identity.CommitSequence | undefined
          readonly refreshGeneration: number | undefined
        } => ({ watermark: undefined, refreshGeneration: undefined }),
        (state, event): readonly [
          {
            readonly watermark: Identity.CommitSequence | undefined
            readonly refreshGeneration: number | undefined
          },
          ReadonlyArray<ReplicaRpc.Invalidation>
        ] => {
          if (event.ownerEpoch !== lease.ownerEpoch) return [state, []]
          if (event._tag === "InvalidationsReady") {
            const refresh = state.watermark === undefined
              ? event.refreshGeneration > 0
              : event.watermark !== state.watermark || event.refreshGeneration !== state.refreshGeneration
            return [
              { watermark: event.watermark, refreshGeneration: event.refreshGeneration },
              refresh ? [fullRefresh] : []
            ]
          }
          if (event._tag === "FullRefreshRequired") return [state, [event]]
          if (state.watermark === undefined) return [state, [fullRefresh]]
          if (event.sequence <= state.watermark) return [state, []]
          if (event.sequence === state.watermark + 1) {
            return [{ ...state, watermark: event.sequence }, [event]]
          }
          return [{ ...state, watermark: event.sequence }, [fullRefresh]]
        }
      ),
      Stream.interruptWhen(Deferred.await(sessionFailure)),
      Stream.catch((error) => Stream.make(fullRefresh).pipe(Stream.concat(Stream.fail(error))))
    )
    return {
      ownerEpoch: lease.ownerEpoch,
      invalidations,
      create: (document, options) =>
        Wire.encode(document.schema, options.value).pipe(
          Effect.flatMap((value) =>
            recoverCommand(
              options.commandId,
              rpc.Create({ sessionId, document: document.name, commandId: options.commandId, value }),
              rpc.LookupCreate({ sessionId, document: document.name, commandId: options.commandId })
            )
          )
        ) as never,
      get: (document, documentId) =>
        rpc.Get({ sessionId, document: document.name, documentId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            )),
          Effect.flatMap((snapshot) =>
            Wire.decode(document.schema, snapshot.value).pipe(
              Effect.map((value) => ({ ...snapshot, value }))
            )
          )
        ) as never,
      mutate: (mutation, options) =>
        Wire.encode(mutation.payload, "payload" in options ? options.payload : undefined).pipe(
          Effect.flatMap((payload) =>
            recoverCommand(
              options.commandId,
              rpc.Mutate({
                sessionId,
                mutation: mutation.name,
                commandId: options.commandId,
                documentId: options.documentId,
                payload
              }),
              rpc.LookupMutation({
                sessionId,
                mutation: mutation.name,
                commandId: options.commandId
              })
            )
          ),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.success, mutation.error, outcome))
        ) as never,
      delete: (document, options) =>
        recoverCommand(
          options.commandId,
          rpc.Delete({ sessionId, document: document.name, ...options }),
          rpc.LookupDelete({ sessionId, document: document.name, commandId: options.commandId })
        ).pipe(
          Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
        ) as never,
      query: (query, ...payload) =>
        Wire.encode(query.payload, payload[0]).pipe(
          Effect.flatMap((encoded) => rpc.Query({ sessionId, query: query.name, payload: encoded })),
          Effect.catchTags({
            RpcClientError: (error) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
                })
              ),
            ReplicaQueryError: (error) => Wire.decode(query.error, error.error).pipe(Effect.flatMap(Effect.fail))
          }),
          Effect.flatMap((encoded) => Wire.decode(query.success, encoded))
        ) as never,
      lookupMutation: (mutation, commandId) =>
        rpc.LookupMutation({ sessionId, mutation: mutation.name, commandId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            )),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.success, mutation.error, outcome))
        ) as never,
      lookupCreate: (document, commandId) =>
        rpc.LookupCreate({ sessionId, document: document.name, commandId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            ))
        ) as never,
      lookupDelete: (document, commandId) =>
        rpc.LookupDelete({ sessionId, document: document.name, commandId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            )),
          Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
        ) as never,
      flush: rpc.Flush({ sessionId }).pipe(
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
            })
          ))
      ),
      status: rpc.Status({ sessionId }).pipe(
        Stream.catchTag("RpcClientError", (error) =>
          Stream.fail(
            new ReplicaError.ReplicaError({
              reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
            })
          ))
      ),
      exportBackup: ({ maxBytes }) =>
        rpc.ExportBackup({ sessionId, maxBytes }).pipe(
          Stream.catchTag("RpcClientError", (error) =>
            Stream.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            ))
        ),
      restoreBackup: <R,>(options: Backup.RestoreOptions<R>) =>
        Stream.runFoldEffect(
          options.source,
          () => ({ bytes: 0, chunks: [] as Array<Uint8Array<ArrayBuffer>> }),
          (accumulator, chunk) => {
            const bytes = accumulator.bytes + chunk.byteLength
            if (bytes > options.maxBytes) {
              return Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: { _tag: "BackupTooLarge", limit: options.maxBytes, observed: bytes }
                })
              )
            }
            accumulator.chunks.push(new Uint8Array(chunk))
            accumulator.bytes = bytes
            return Effect.succeed(accumulator)
          }
        ).pipe(
          Effect.flatMap(({ chunks }) =>
            rpc.RestoreBackup({
              sessionId,
              chunks,
              mode: options.mode,
              maxBytes: options.maxBytes,
              expectedDefinitionHash: options.expectedDefinitionHash
            })
          ),
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            ))
        ),
      exportDocument: (document, documentId) =>
        rpc.ExportDocument({ sessionId, document: document.name, documentId }).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
              })
            )),
          Effect.flatMap((exported) =>
            Wire.decode(Schema.toEncoded(document.schema), exported.value).pipe(
              Effect.map((value) => ({ ...exported, value }))
            )
          )
        ) as never,
      importDocument: (document, options) =>
        Wire.encode(Schema.toEncoded(document.schema), options.value.value).pipe(
          Effect.flatMap((value) =>
            rpc.ImportDocument({
              sessionId,
              document: document.name,
              commandId: options.commandId,
              value: { ...options.value, value }
            }).pipe(
              Effect.catchTag("RpcClientError", (error) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: { _tag: "StorageUnavailable", cause: { _tag: "RpcCause", message: error.message } }
                  })
                ))
            )
          )
        ) as never
    }
  })

export const layer = (definition: ReplicaDefinition.Any) =>
  Layer.effect(
    ReplicaClient,
    RpcClient.make(ReplicaRpc.group).pipe(Effect.flatMap((rpc) => fromRpcClient(definition, rpc)))
  )
