import type * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { RpcClient } from "effect/unstable/rpc"
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as Wire from "./internal/wire.js"
import * as ReplicaRpc from "./ReplicaRpc.js"

export class ReplicaClient extends Context.Service<
  ReplicaClient,
  Replica.Replica["Service"] & {
    readonly ownerEpoch: string
    readonly invalidations: Stream.Stream<ReplicaRpc.Invalidation, ReplicaError.ReplicaError>
  }
>()(
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
): Effect.Effect<ReplicaClient["Service"], ReplicaError.ReplicaError, Scope.Scope | Crypto.Crypto> =>
  Effect.gen(function*() {
    const crypto = yield* Crypto.Crypto
    const makeSessionId = Identity.makeSessionId.pipe(
      Effect.mapError((cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageUnavailable({
            cause
          })
        })
      ),
      Effect.provideService(Crypto.Crypto, crypto)
    )
    const openSession = Effect.fnUntraced(function*(sessionId: Identity.SessionId) {
      const lease = yield* rpc.OpenSession({
        sessionId,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      }).pipe(
        Effect.tapError(() => Effect.ignore(rpc.CloseSession({ sessionId }))),
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          )),
        Effect.onInterrupt(() => Effect.ignore(rpc.CloseSession({ sessionId })))
      )
      if (lease.protocolVersion !== ReplicaRpc.protocolVersion || lease.definitionHash !== definition.hash) {
        yield* Effect.ignore(rpc.CloseSession({ sessionId }))
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: `${ReplicaRpc.protocolVersion}:${definition.hash}`,
            observed: `${lease.protocolVersion}:${lease.definitionHash}`
          })
        })
      }
      return { sessionId, lease }
    })
    const newSession = makeSessionId.pipe(Effect.flatMap(openSession))
    const sessions = yield* Effect.acquireRelease(
      newSession.pipe(Effect.flatMap(SubscriptionRef.make)),
      (sessions) =>
        SubscriptionRef.get(sessions).pipe(
          Effect.flatMap((session) => Effect.ignore(rpc.CloseSession({ sessionId: session.sessionId })))
        )
    )
    const reopen = (stale: Effect.Success<ReturnType<typeof openSession>>) =>
      Effect.uninterruptibleMask((restore) =>
        SubscriptionRef.modifySomeEffect(sessions, (current) =>
          current.sessionId !== stale.sessionId
            ? Effect.succeed(
              [
                { session: current, stale: Option.none<typeof current>() },
                Option.none<typeof current>()
              ] as const
            )
            : restore(
              makeSessionId.pipe(
                Effect.flatMap((sessionId) =>
                  openSession(sessionId).pipe(
                    Effect.retry({ schedule: Schedule.spaced("1 second"), while: isTransient })
                  )
                )
              )
            ).pipe(
              Effect.map((next) => [{ session: next, stale: Option.some(current) }, Option.some(next)] as const)
            )).pipe(
            Effect.tap(({ stale }) =>
              Option.match(stale, {
                onNone: () => Effect.void,
                onSome: (stale) => Effect.ignore(rpc.CloseSession({ sessionId: stale.sessionId }))
              })
            ),
            Effect.map(({ session }) => session)
          )
      )
    const withSession = <A, E, R,>(
      use: (
        session: Effect.Success<ReturnType<typeof openSession>>
      ) => Effect.Effect<A, E | ReplicaError.ReplicaError, R>
    ) =>
      SubscriptionRef.get(sessions).pipe(
        Effect.flatMap((session) =>
          use(session).pipe(
            Effect.catchTag("ReplicaError", (error) =>
              Schema.is(ReplicaError.ReplicaError)(error) && error.reason._tag === "ProtocolMismatch"
                ? reopen(session).pipe(Effect.flatMap(use))
                : Effect.fail(error))
          )
        )
      )
    const withSessionStream = <A, E, R,>(
      use: (
        session: Effect.Success<ReturnType<typeof openSession>>
      ) => Stream.Stream<A, E | ReplicaError.ReplicaError, R>
    ) =>
      Stream.unwrap(Effect.gen(function*() {
        const session = yield* SubscriptionRef.get(sessions)
        const emitted = yield* Ref.make(false)
        return use(session).pipe(
          Stream.tap(() => Ref.set(emitted, true)),
          Stream.catchTag("ReplicaError", (error) =>
            Schema.is(ReplicaError.ReplicaError)(error) && error.reason._tag === "ProtocolMismatch"
              ? Stream.unwrap(Effect.gen(function*() {
                const next = yield* reopen(session)
                return (yield* Ref.get(emitted)) ? Stream.fail(error) : use(next)
              }))
              : Stream.fail(error))
        )
      }))
    const retrySchedule = Schedule.spaced("1 second")
    const sessionFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    yield* Effect.gen(function*() {
      const current = yield* SubscriptionRef.get(sessions)
      yield* Effect.sleep(current.lease.leaseMillis / 2)
      const session = yield* SubscriptionRef.get(sessions)
      const renewed = yield* rpc.RenewSession({ sessionId: session.sessionId }).pipe(
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          )),
        Effect.retry({ schedule: retrySchedule, while: isTransient }),
        Effect.catchReason("ReplicaError", "ProtocolMismatch", () => reopen(session).pipe(Effect.as(undefined)))
      )
      if (renewed !== undefined && renewed.leaseMillis !== session.lease.leaseMillis) {
        yield* SubscriptionRef.updateSome(
          sessions,
          (current) =>
            current.sessionId === session.sessionId
              ? Option.some({ ...session, lease: { ...session.lease, ...renewed } })
              : Option.none()
        )
      }
    }).pipe(
      Effect.forever,
      Effect.tapError((error) => Deferred.fail(sessionFailure, error)),
      Effect.tapCause(Effect.logError),
      Effect.ignore,
      Effect.forkScoped
    )
    const allInvalidationKeys = ReplicaDefinition.invalidationKeys(definition)
    const fullRefresh = (ownerEpoch: string): ReplicaRpc.Invalidation => ({
      _tag: "FullRefreshRequired" as const,
      ownerEpoch,
      keys: [...allInvalidationKeys]
    })
    const invalidationMessages: Stream.Stream<
      ReplicaRpc.InvalidationMessage,
      ReplicaError.ReplicaError
    > = Stream.unwrap(
      SubscriptionRef.get(sessions).pipe(
        Effect.map((session) =>
          rpc.Invalidations({ sessionId: session.sessionId, ownerEpoch: session.lease.ownerEpoch }).pipe(
            Stream.catchTag("RpcClientError", (error) =>
              Stream.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              )),
            Stream.filter((event) => event.ownerEpoch === session.lease.ownerEpoch),
            Stream.retry(
              Schedule.exponential(250).pipe(
                Schedule.upTo({ times: 3 }),
                Schedule.setInputType<ReplicaError.ReplicaError>(),
                Schedule.while(({ input }) => input.reason._tag === "StorageUnavailable")
              )
            ),
            Stream.catchReason(
              "ReplicaError",
              "ProtocolMismatch",
              (_, error) => Stream.unwrap(reopen(session).pipe(Effect.as(Stream.fail(error))))
            )
          )
        )
      )
    ).pipe(
      Stream.retry(
        Schedule.forever.pipe(
          Schedule.setInputType<ReplicaError.ReplicaError>(),
          Schedule.while(({ input }) => input.reason._tag === "ProtocolMismatch")
        )
      )
    )
    const initialSession = yield* SubscriptionRef.get(sessions)
    const invalidations = invalidationMessages.pipe(
      Stream.mapAccum(
        (): {
          readonly ownerEpoch: string
          readonly watermark: Identity.CommitSequence | undefined
          readonly refreshGeneration: number | undefined
        } => ({
          ownerEpoch: initialSession.lease.ownerEpoch,
          watermark: undefined,
          refreshGeneration: undefined
        }),
        (state, event): readonly [
          {
            readonly ownerEpoch: string
            readonly watermark: Identity.CommitSequence | undefined
            readonly refreshGeneration: number | undefined
          },
          ReadonlyArray<ReplicaRpc.Invalidation>
        ] => {
          if (event.ownerEpoch !== state.ownerEpoch) {
            if (event._tag === "InvalidationsReady") {
              return [
                {
                  ownerEpoch: event.ownerEpoch,
                  watermark: event.watermark,
                  refreshGeneration: event.refreshGeneration
                },
                [fullRefresh(event.ownerEpoch)]
              ]
            }
            if (event._tag === "FullRefreshRequired") {
              return [{ ownerEpoch: event.ownerEpoch, watermark: undefined, refreshGeneration: undefined }, [event]]
            }
            return [
              { ownerEpoch: event.ownerEpoch, watermark: event.sequence, refreshGeneration: undefined },
              [fullRefresh(event.ownerEpoch)]
            ]
          }
          if (event._tag === "InvalidationsReady") {
            const refresh = state.watermark === undefined
              ? event.refreshGeneration > 0
              : event.watermark !== state.watermark || event.refreshGeneration !== state.refreshGeneration
            return [
              { ...state, watermark: event.watermark, refreshGeneration: event.refreshGeneration },
              refresh ? [fullRefresh(event.ownerEpoch)] : []
            ]
          }
          if (event._tag === "FullRefreshRequired") return [state, [event]]
          if (state.watermark === undefined) return [state, [fullRefresh(event.ownerEpoch)]]
          if (event.sequence <= state.watermark) return [state, []]
          if (event.sequence === state.watermark + 1) {
            return [{ ...state, watermark: event.sequence }, [event]]
          }
          return [{ ...state, watermark: event.sequence }, [fullRefresh(event.ownerEpoch)]]
        }
      ),
      Stream.interruptWhen(Deferred.await(sessionFailure)),
      Stream.catch((error) =>
        Stream.unwrap(
          SubscriptionRef.get(sessions).pipe(
            Effect.map((session) =>
              Stream.make(fullRefresh(session.lease.ownerEpoch)).pipe(Stream.concat(Stream.fail(error)))
            )
          )
        )
      )
    )
    return {
      get ownerEpoch() {
        return SubscriptionRef.getUnsafe(sessions).lease.ownerEpoch
      },
      invalidations,
      create: (document, options) =>
        Wire.encode(document.schema, options.value).pipe(
          Effect.flatMap((value) =>
            withSession((session) =>
              recoverCommand(
                options.commandId,
                rpc.Create({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: options.commandId,
                  value
                }),
                rpc.LookupCreate({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: options.commandId
                })
              )
            )
          )
        ) as never,
      get: (document, documentId) =>
        withSession((session) => rpc.Get({ sessionId: session.sessionId, document: document.name, documentId })).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
              })
            )),
          Effect.flatMap((snapshot) =>
            Wire.decode(document.schema, snapshot.value).pipe(
              Effect.map((value) => ({ ...snapshot, value }))
            )
          )
        ) as never,
      mutate: (mutation, options) =>
        Wire.encode(mutation.payloadSchema, "payload" in options ? options.payload : undefined).pipe(
          Effect.flatMap((payload) =>
            withSession((session) =>
              recoverCommand(
                options.commandId,
                rpc.Mutate({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: options.commandId,
                  documentId: options.documentId,
                  payload
                }),
                rpc.LookupMutation({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: options.commandId
                })
              )
            )
          ),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.successSchema, mutation.errorSchema, outcome))
        ) as never,
      delete: (document, options) =>
        withSession((session) =>
          recoverCommand(
            options.commandId,
            rpc.Delete({ sessionId: session.sessionId, document: document.name, ...options }),
            rpc.LookupDelete({ sessionId: session.sessionId, document: document.name, commandId: options.commandId })
          )
        ).pipe(
          Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
        ) as never,
      query: (query, ...payload) =>
        Wire.encode(query.payloadSchema, payload[0]).pipe(
          Effect.flatMap((encoded) =>
            withSession((session) => rpc.Query({ sessionId: session.sessionId, query: query.name, payload: encoded }))
          ),
          Effect.catchTags({
            RpcClientError: (error) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              ),
            ReplicaQueryError: (error) => Wire.decode(query.errorSchema, error.error).pipe(Effect.flatMap(Effect.fail))
          }),
          Effect.flatMap((encoded) => Wire.decode(query.successSchema, encoded))
        ) as never,
      lookupMutation: (mutation, commandId) =>
        withSession((session) =>
          rpc.LookupMutation({ sessionId: session.sessionId, mutation: mutation.name, commandId })
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
              })
            )),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.successSchema, mutation.errorSchema, outcome))
        ) as never,
      lookupCreate: (document, commandId) =>
        withSession((session) => rpc.LookupCreate({ sessionId: session.sessionId, document: document.name, commandId }))
          .pipe(
            Effect.catchTag("RpcClientError", (error) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              ))
          ) as never,
      lookupDelete: (document, commandId) =>
        withSession((session) => rpc.LookupDelete({ sessionId: session.sessionId, document: document.name, commandId }))
          .pipe(
            Effect.catchTag("RpcClientError", (error) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              )),
            Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
          ) as never,
      flush: withSession((session) => rpc.Flush({ sessionId: session.sessionId })).pipe(
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          ))
      ),
      status: withSessionStream((session) => rpc.Status({ sessionId: session.sessionId })).pipe(
        Stream.catchTag("RpcClientError", (error) =>
          Stream.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          ))
      ),
      exportBackup: ({ maxBytes }) =>
        withSessionStream((session) => rpc.ExportBackup({ sessionId: session.sessionId, maxBytes })).pipe(
          Stream.catchTag("RpcClientError", (error) =>
            Stream.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
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
                  reason: new ReplicaError.BackupTooLarge({
                    limit: options.maxBytes,
                    observed: bytes
                  })
                })
              )
            }
            accumulator.chunks.push(new Uint8Array(chunk))
            accumulator.bytes = bytes
            return Effect.succeed(accumulator)
          }
        ).pipe(
          Effect.flatMap(({ chunks }) =>
            withSession((session) =>
              rpc.RestoreBackup({
                sessionId: session.sessionId,
                chunks,
                mode: options.mode,
                maxBytes: options.maxBytes,
                expectedDefinitionHash: options.expectedDefinitionHash
              })
            )
          ),
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
              })
            ))
        ),
      exportDocument: (document, documentId) =>
        withSession((session) =>
          rpc.ExportDocument({ sessionId: session.sessionId, document: document.name, documentId })
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
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
            withSession((session) =>
              rpc.ImportDocument({
                sessionId: session.sessionId,
                document: document.name,
                commandId: options.commandId,
                value: { ...options.value, value }
              })
            ).pipe(
              Effect.catchTag("RpcClientError", (error) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause: error
                    })
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
