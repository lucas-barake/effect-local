import * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { RpcClient } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
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

const isTransientRpcClientError = (error: RpcClientError.RpcClientError) => {
  switch (error.reason._tag) {
    case "WorkerSpawnError":
    case "WorkerSendError":
    case "WorkerReceiveError":
    case "WorkerUnknownError":
    case "SocketReadError":
    case "SocketWriteError":
    case "SocketOpenError":
    case "SocketCloseError":
      return true
    case "HttpError":
      return error.reason.kind === "TransportError"
    case "RpcClientDefect":
      return false
  }
}

const isTransientStatus = (error: ReplicaError.ReplicaError) => {
  if (error.reason._tag === "QuotaExceeded") return error.reason.resource === "queued RPCs"
  if (error.reason._tag !== "StorageUnavailable") return false
  return !Schema.is(RpcClientError.RpcClientError)(error.reason.cause) ||
    isTransientRpcClientError(error.reason.cause)
}

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
    type Session = Effect.Success<ReturnType<typeof openSession>>
    type PendingStreamMismatch = {
      readonly stale: Session
      readonly error: ReplicaError.ReplicaError
    }
    const reopenAttempt = (
      stale: Session,
      replacement: Effect.Effect<Session, ReplicaError.ReplicaError>
    ) =>
      sessions.semaphore.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            const current = sessions.value
            if (current.sessionId !== stale.sessionId) return Effect.succeed(current)
            return restore(replacement).pipe(
              Effect.tap((next) =>
                Effect.sync(() => {
                  sessions.value = next
                  PubSub.publishUnsafe(sessions.pubsub, next)
                })
              ),
              Effect.tap(() => Effect.ignore(rpc.CloseSession({ sessionId: current.sessionId })))
            )
          })
        )
      )
    const replacementCandidate = yield* Ref.make<
      Option.Option<{
        readonly staleSessionId: Identity.SessionId
        readonly candidateSessionId: Identity.SessionId
      }>
    >(Option.none())
    const replace = (stale: Session, retryTransient: boolean) =>
      reopenAttempt(
        stale,
        Ref.get(replacementCandidate).pipe(
          Effect.flatMap((candidate) =>
            Option.isSome(candidate) && candidate.value.staleSessionId === stale.sessionId
              ? Effect.succeed(candidate.value.candidateSessionId)
              : makeSessionId.pipe(
                Effect.tap((candidateSessionId) =>
                  Ref.set(
                    replacementCandidate,
                    Option.some({
                      staleSessionId: stale.sessionId,
                      candidateSessionId
                    })
                  )
                )
              )
          ),
          Effect.flatMap((sessionId) => {
            const attempt = openSession(sessionId)
            return retryTransient
              ? attempt.pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), while: isTransient }))
              : attempt
          })
        )
      ).pipe(
        Effect.tap(() =>
          Ref.update(
            replacementCandidate,
            (candidate) =>
              Option.isSome(candidate) && candidate.value.staleSessionId === stale.sessionId
                ? Option.none()
                : candidate
          )
        )
      )
    const reopen = (stale: Session) => replace(stale, true)
    const reopenStatus = (stale: Session) => replace(stale, false)
    const withSession = <A, E, R,>(
      use: (
        session: Session
      ) => Effect.Effect<A, E | ReplicaError.ReplicaError, R>,
      options?: { readonly replayAfterReopen?: boolean }
    ) =>
      SubscriptionRef.get(sessions).pipe(
        Effect.flatMap((session) =>
          use(session).pipe(
            Effect.catchTag("ReplicaError", (error) =>
              Schema.is(ReplicaError.ReplicaError)(error) && error.reason._tag === "ProtocolMismatch"
                ? options?.replayAfterReopen === false
                  ? reopen(session).pipe(Effect.andThen(Effect.fail(error)))
                  : reopen(session).pipe(Effect.flatMap(use))
                : Effect.fail(error))
          )
        )
      )
    const withSessionStream = <A, E, R,>(
      use: (
        session: Session
      ) => Stream.Stream<A, E | ReplicaError.ReplicaError, R>,
      replace: (stale: Session) => Effect.Effect<Session, ReplicaError.ReplicaError> = reopen,
      emittedRef?: Ref.Ref<boolean>,
      pendingMismatchRef?: Ref.Ref<Option.Option<PendingStreamMismatch>>
    ) =>
      Stream.unwrap(Effect.gen(function*() {
        const session = yield* SubscriptionRef.get(sessions)
        const emitted = emittedRef === undefined ? yield* Ref.make(false) : emittedRef
        if (pendingMismatchRef !== undefined) {
          const pending = yield* Ref.get(pendingMismatchRef)
          if (Option.isSome(pending)) {
            return Stream.unwrap(
              replace(pending.value.stale).pipe(
                Effect.andThen(Ref.set(pendingMismatchRef, Option.none())),
                Effect.as(Stream.fail(pending.value.error))
              )
            )
          }
        }
        const tracked = (session: Session) => use(session).pipe(Stream.tap(() => Ref.set(emitted, true)))
        return tracked(session).pipe(
          Stream.catchTag("ReplicaError", (error) =>
            Schema.is(ReplicaError.ReplicaError)(error) && error.reason._tag === "ProtocolMismatch"
              ? Stream.unwrap(Effect.gen(function*() {
                const hasEmitted = yield* Ref.get(emitted)
                if (hasEmitted && pendingMismatchRef !== undefined) {
                  yield* Ref.set(pendingMismatchRef, Option.some({ stale: session, error }))
                }
                const next = yield* replace(session)
                if (!hasEmitted) {
                  return tracked(next)
                }
                if (pendingMismatchRef !== undefined) {
                  yield* Ref.set(pendingMismatchRef, Option.none())
                }
                return Stream.fail(error)
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
      _tag: "FullRefreshRequired",
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
        (state, event) => {
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
              ? event.watermark > 0 || event.refreshGeneration > 0
              : event.watermark !== state.watermark || event.refreshGeneration !== state.refreshGeneration
            return [
              { ...state, watermark: event.watermark, refreshGeneration: event.refreshGeneration },
              refresh ? [fullRefresh(event.ownerEpoch)] : []
            ]
          }
          if (event._tag === "FullRefreshRequired") {
            return [{ ...state, watermark: undefined, refreshGeneration: undefined }, [event]]
          }
          if (state.watermark === undefined) {
            return [{ ...state, watermark: event.sequence }, [fullRefresh(event.ownerEpoch)]]
          }
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
        ),
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
        ),
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
        ),
      delete: (document, options) =>
        withSession((session) =>
          recoverCommand(
            options.commandId,
            rpc.Delete({ sessionId: session.sessionId, document: document.name, ...options }),
            rpc.LookupDelete({ sessionId: session.sessionId, document: document.name, commandId: options.commandId })
          )
        ).pipe(
          Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
        ),
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
        ),
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
        ),
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
          ),
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
          ),
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
      status: Stream.unwrap(
        Effect.gen(function*() {
          const emitted = yield* Ref.make(false)
          const pendingMismatch = yield* Ref.make<Option.Option<PendingStreamMismatch>>(Option.none())
          return withSessionStream(
            (session) => rpc.Status({ sessionId: session.sessionId }),
            reopenStatus,
            emitted,
            pendingMismatch
          ).pipe(
            Stream.catchTag("RpcClientError", (error) =>
              Stream.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              )),
            Stream.catchIf(isTransientStatus, (error) => {
              const degraded: ReplicaStatus.ReplicaStatus = { _tag: "Degraded", reason: error.reason._tag }
              return Stream.make(degraded).pipe(Stream.concat(Stream.fail(error)))
            }),
            Stream.retry(
              Schedule.spaced("1 second").pipe(
                Schedule.setInputType<ReplicaError.ReplicaError>(),
                Schedule.while(({ input }) => isTransientStatus(input))
              )
            )
          )
        })
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
        Backup.validateMaxBytes(options.maxBytes).pipe(
          Effect.flatMap((maxBytes) =>
            Stream.runFoldEffect(
              options.source,
              () => ({ bytes: 0, chunks: [] as Array<Uint8Array<ArrayBuffer>> }),
              (accumulator, chunk) => {
                const bytes = accumulator.bytes + chunk.byteLength
                if (bytes > maxBytes) {
                  return Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupTooLarge({
                        limit: maxBytes,
                        observed: bytes
                      })
                    })
                  )
                }
                accumulator.chunks.push(new Uint8Array(chunk))
                accumulator.bytes = bytes
                return Effect.succeed(accumulator)
              }
            )
          ),
          Effect.flatMap(({ chunks }) =>
            withSession(
              (session) =>
                rpc.RestoreBackup({
                  sessionId: session.sessionId,
                  chunks,
                  mode: options.mode,
                  maxBytes: options.maxBytes,
                  expectedDefinitionHash: options.expectedDefinitionHash,
                  installationId: options.installationId
                }),
              { replayAfterReopen: false }
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
        ),
      importDocument: (document, options) =>
        Wire.encode(Schema.toEncoded(document.schema), options.value.value).pipe(
          Effect.flatMap((value) =>
            withSession(
              (session) =>
                rpc.ImportDocument({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: options.commandId,
                  value: { ...options.value, value }
                }),
              { replayAfterReopen: false }
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
        )
    }
  })

export const layer = (definition: ReplicaDefinition.Any) =>
  Layer.effect(
    ReplicaClient,
    RpcClient.make(ReplicaRpc.group).pipe(Effect.flatMap((rpc) => fromRpcClient(definition, rpc)))
  )
