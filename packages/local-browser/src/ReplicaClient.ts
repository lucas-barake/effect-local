import * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { RpcClient } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as RestoreProtocol from "./internal/restoreProtocol.js"
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

export interface TimeoutOptions {
  readonly sessionTimeout?: Duration.Input | undefined
  readonly operationTimeout?: Duration.Input | undefined
}

export const defaultSessionTimeout: Duration.Duration = Duration.seconds(10)
export const defaultOperationTimeout: Duration.Duration = Duration.seconds(30)

class RestoreBackupError extends Schema.TaggedErrorClass<RestoreBackupError>(
  "@lucas-barake/effect-local-browser/ReplicaClient/RestoreBackupError"
)("RestoreBackupError", {
  error: ReplicaError.ReplicaError
}) {}

const protocolFailure = (observed: string) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({
      expected: "restore protocol version 4",
      observed
    })
  })

const storageFailure = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({
      cause
    })
  })

const isPositiveSafeInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0

const isNativeErrorNamed = (value: unknown, name: string): boolean => {
  if (typeof value !== "object" || value === null) return false
  try {
    return Object.prototype.toString.call(value) === "[object DOMException]" &&
      Reflect.get(value, "name") === name
  } catch {
    return false
  }
}

const isDataCloneError = (value: unknown): boolean => isNativeErrorNamed(value, "DataCloneError")

const ownDataProperties = (
  value: object,
  maxProperties: number
): ReadonlyMap<string, unknown> | undefined => {
  const properties = new Map<string, unknown>()
  try {
    for (const key in value) {
      if (!Object.prototype.hasOwnProperty.call(value, key)) continue
      if (properties.size >= maxProperties) return undefined
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor === undefined || !("value" in descriptor)) return undefined
      properties.set(key, descriptor.value)
    }
    return properties
  } catch {
    return undefined
  }
}

const preflightOwnerFrame = (value: unknown, maxErrorBytes: number): boolean => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false
  const properties = ownDataProperties(value, 4)
  if (properties === undefined) return false
  const tag = properties.get("_tag")
  const nonce = properties.get("nonce")
  const sequence = properties.get("sequence")
  if (
    typeof tag !== "string" ||
    tag.length > 32 ||
    typeof nonce !== "string" ||
    nonce.length > 64 ||
    !Number.isSafeInteger(sequence) ||
    (sequence as number) < 0
  ) return false
  const expectedFields = tag === "TerminalSessionFailure" || tag === "TerminalRestoreFailure"
    ? ["_tag", "nonce", "sequence", "error"]
    : tag === "TerminalDefect"
    ? ["_tag", "nonce", "sequence", "defect"]
    : tag === "Pull" || tag === "TerminalSuccess" || tag === "Released"
    ? ["_tag", "nonce", "sequence"]
    : undefined
  if (
    expectedFields === undefined ||
    properties.size !== expectedFields.length ||
    expectedFields.some((field) => !properties.has(field))
  ) return false
  const error = properties.get("error")
  const defect = properties.get("defect")
  return (error === undefined || RestoreProtocol.preflight(error, maxErrorBytes)) &&
    (defect === undefined || RestoreProtocol.preflight(defect, maxErrorBytes))
}

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

export const fromRpcClient = (
  definition: ReplicaDefinition.Any,
  rpc: RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>,
  options?: TimeoutOptions
): Effect.Effect<ReplicaClient["Service"], ReplicaError.ReplicaError, Scope.Scope | Crypto.Crypto> =>
  Effect.gen(function*() {
    const sessionTimeout = Duration.fromInputUnsafe(options?.sessionTimeout ?? defaultSessionTimeout)
    const operationTimeout = Duration.fromInputUnsafe(options?.operationTimeout ?? defaultOperationTimeout)
    const timeoutMillis = (duration: Duration.Duration) => {
      const millis = Duration.toMillis(duration)
      if (millis <= 0 || Number.isNaN(millis)) return 0
      if (millis >= Number.MAX_SAFE_INTEGER) return Number.MAX_SAFE_INTEGER
      return Math.trunc(millis)
    }
    const boundBy =
      (operation: string, duration: Duration.Duration) =>
      <A, E, R,>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | ReplicaError.ReplicaError, R> =>
        Effect.timeoutOrElse(effect, {
          duration,
          orElse: () =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.OperationTimeout({
                  operation,
                  timeoutMillis: timeoutMillis(duration)
                })
              })
            )
        })
    const boundSession = (operation: string) => boundBy(operation, sessionTimeout)
    const boundOperation = (operation: string) => boundBy(operation, operationTimeout)
    const recoverCommand = <A,>(
      commandId: Identity.CommandId,
      dispatchOperation: string,
      dispatch: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>,
      lookupOperation: string,
      lookup: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>
    ): Effect.Effect<A | CommandOutcome.OutcomeUnknown, ReplicaError.ReplicaError> => {
      const unknown = () => Effect.succeed(CommandOutcome.unknown(commandId))
      const lookupOrUnknown = lookup.pipe(
        boundOperation(lookupOperation),
        Effect.catchTags({
          RpcClientError: unknown,
          ReplicaError: (error) => error.reason._tag === "OperationTimeout" ? unknown() : Effect.fail(error)
        })
      )
      return dispatch.pipe(
        boundOperation(dispatchOperation),
        Effect.catchTags({
          RpcClientError: () => lookupOrUnknown,
          ReplicaError: (error) => error.reason._tag === "OperationTimeout" ? lookupOrUnknown : Effect.fail(error)
        })
      )
    }
    const closeSession = (sessionId: Identity.SessionId) =>
      rpc.CloseSession({ sessionId }).pipe(boundSession("CloseSession"))
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
        boundSession("OpenSession"),
        Effect.tapError(() => Effect.ignore(closeSession(sessionId))),
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          )),
        Effect.onInterrupt(() => Effect.ignore(closeSession(sessionId)))
      )
      if (lease.protocolVersion !== ReplicaRpc.protocolVersion) {
        yield* Effect.ignore(closeSession(sessionId))
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: `protocol version ${ReplicaRpc.protocolVersion}`,
            observed: `protocol version ${lease.protocolVersion}`
          })
        })
      }
      if (
        !isPositiveSafeInteger(lease.maxChunkBytes) ||
        !isPositiveSafeInteger(lease.maxRestoreCoalesceMillis) ||
        !isPositiveSafeInteger(lease.maxRestoreErrorBytes) ||
        lease.maxRestoreErrorBytes < ReplicaLimits.minimumRestoreErrorBytes
      ) {
        yield* Effect.ignore(closeSession(sessionId))
        return yield* protocolFailure("invalid advertised restore limits")
      }
      if (lease.definitionHash !== definition.hash) {
        yield* Effect.ignore(closeSession(sessionId))
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: `${ReplicaRpc.protocolVersion}:${definition.hash}`,
            observed: `${lease.protocolVersion}:${lease.definitionHash}`
          })
        })
      }
      return {
        sessionId,
        lease: {
          ...lease,
          maxChunkBytes: lease.maxChunkBytes,
          maxRestoreCoalesceMillis: lease.maxRestoreCoalesceMillis,
          maxRestoreErrorBytes: lease.maxRestoreErrorBytes
        }
      }
    })
    const newSession = makeSessionId.pipe(Effect.flatMap(openSession))
    const sessions = yield* Effect.acquireRelease(
      newSession.pipe(Effect.flatMap(SubscriptionRef.make)),
      (sessions) =>
        SubscriptionRef.get(sessions).pipe(
          Effect.flatMap((session) => Effect.ignore(closeSession(session.sessionId)))
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
              Effect.tap(() => Effect.ignore(closeSession(current.sessionId)))
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
      operation: string,
      use: (
        session: Session
      ) => Effect.Effect<A, E | ReplicaError.ReplicaError, R>,
      options?: {
        readonly boundOperation?: boolean
        readonly replayAfterReopen?: boolean
      }
    ) => {
      const bounded = (session: Session) =>
        options?.boundOperation === false ? use(session) : use(session).pipe(boundOperation(operation))
      return SubscriptionRef.get(sessions).pipe(
        Effect.flatMap((session) =>
          bounded(session).pipe(
            Effect.catchTag("ReplicaError", (error) =>
              Schema.is(ReplicaError.ReplicaError)(error) && error.reason._tag === "ProtocolMismatch"
                ? options?.replayAfterReopen === false
                  ? reopen(session).pipe(Effect.andThen(Effect.fail(error)))
                  : reopen(session).pipe(Effect.flatMap(bounded))
                : Effect.fail(error))
          )
        )
      )
    }
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
        boundSession("RenewSession"),
        Effect.catchTag("RpcClientError", (error) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: error
              })
            })
          )),
        Effect.retry({
          schedule: retrySchedule,
          while: (error) => isTransient(error) || error.reason._tag === "OperationTimeout"
        }),
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
    const serveRestoreSource = <R,>(
      source: Stream.Stream<Uint8Array, ReplicaError.ReplicaError, R>,
      port: MessagePort,
      nonce: RestoreProtocol.RestoreNonce,
      maxBytes: number,
      maxChunkBytes: number,
      maxCoalesceMillis: number,
      maxErrorBytes: number,
      closePort: () => void,
      acceptTerminal: (exit: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>) => void
    ): Effect.Effect<void, ReplicaError.ReplicaError | RestoreBackupError, R> =>
      Effect.acquireUseRelease(
        Scope.make("sequential"),
        (restoreScope) =>
          Effect.gen(function*() {
            const credits = yield* Queue.bounded<RestoreProtocol.Pull>(1)
            const terminal = yield* Deferred.make<
              | RestoreProtocol.TerminalSuccess
              | RestoreProtocol.TerminalSessionFailure
              | RestoreProtocol.TerminalRestoreFailure
              | RestoreProtocol.TerminalDefect
            >()
            const released = yield* Deferred.make<void>()
            const localFailure = yield* Deferred.make<never, RestoreBackupError>()
            let logicalOpen = true
            let terminalAccepted = false
            let terminalAckPosted = false
            let expectedOwnerSequence = 1
            let pullOutstanding = false
            let sourceComplete = false
            let sourceWorker: Fiber.Fiber<void, ReplicaError.ReplicaError> | undefined

            const removeListeners = () => {
              port.removeEventListener("message", onMessage)
              port.removeEventListener("messageerror", onMessageError)
              port.removeEventListener("close", onClose)
            }
            const stopIngress = () => {
              if (!logicalOpen) return
              logicalOpen = false
              removeListeners()
              closePort()
            }
            const failClosed = (error: ReplicaError.ReplicaError) => {
              stopIngress()
              if (terminalAccepted) {
                Deferred.doneUnsafe(released, Effect.void)
              } else {
                Deferred.doneUnsafe(
                  localFailure,
                  Effect.fail(new RestoreBackupError({ error }))
                )
              }
              sourceWorker?.interruptUnsafe()
            }
            const decodeOwnerFrame = (
              value: unknown
            ): Exit.Exit<RestoreProtocol.OwnerToPageFrame, unknown> => {
              if (!preflightOwnerFrame(value, maxErrorBytes)) {
                return Exit.fail(protocolFailure("invalid restore owner frame"))
              }
              return Schema.decodeUnknownExit(RestoreProtocol.OwnerToPageFrame)(value)
            }
            const postFrame = <A, I,>(
              schema: Schema.Codec<A, I, never>,
              frame: A,
              transfer?: ReadonlyArray<Transferable>
            ): Effect.Effect<void, ReplicaError.ReplicaError> =>
              Effect.suspend(() => {
                let encoded: I
                try {
                  encoded = Schema.encodeSync(schema)(frame)
                } catch (cause) {
                  return Effect.die(cause)
                }
                return Effect.try({
                  try: () => port.postMessage(encoded, transfer === undefined ? [] : [...transfer]),
                  catch: storageFailure
                })
              })
            const postReleasedAck = (frame: RestoreProtocol.Released) => {
              try {
                const encoded = Schema.encodeSync(RestoreProtocol.ReleasedAck)({
                  _tag: "ReleasedAck",
                  nonce,
                  sequence: frame.sequence
                })
                port.postMessage(encoded)
              } catch {
                // The terminal result is already authoritative.
              }
            }
            const acceptOwnerFrame = (frame: RestoreProtocol.OwnerToPageFrame) => {
              if (frame.nonce !== nonce || frame.sequence !== expectedOwnerSequence) {
                failClosed(protocolFailure("invalid restore owner sequence"))
                return
              }
              if (frame._tag === "Pull") {
                if (terminalAccepted || sourceComplete || pullOutstanding || !Queue.offerUnsafe(credits, frame)) {
                  failClosed(protocolFailure("unsolicited restore pull"))
                  return
                }
                pullOutstanding = true
                expectedOwnerSequence += 1
                return
              }
              if (
                frame._tag === "TerminalSuccess" ||
                frame._tag === "TerminalSessionFailure" ||
                frame._tag === "TerminalRestoreFailure" ||
                frame._tag === "TerminalDefect"
              ) {
                if (terminalAccepted) {
                  failClosed(protocolFailure("duplicate restore terminal"))
                  return
                }
                terminalAccepted = true
                expectedOwnerSequence += 1
                sourceWorker?.interruptUnsafe()
                Deferred.doneUnsafe(terminal, Effect.succeed(frame))
                return
              }
              if (frame._tag === "Released") {
                if (!terminalAccepted || !terminalAckPosted) {
                  failClosed(protocolFailure("unexpected restore release"))
                  return
                }
                expectedOwnerSequence += 1
                postReleasedAck(frame)
                Deferred.doneUnsafe(released, Effect.void)
              }
            }
            function onMessage(event: MessageEvent<unknown>) {
              if (!logicalOpen) return
              let decoded: Exit.Exit<RestoreProtocol.OwnerToPageFrame, unknown>
              try {
                decoded = decodeOwnerFrame(event.data)
              } catch {
                failClosed(protocolFailure("invalid restore owner frame"))
                return
              }
              if (Exit.isFailure(decoded)) {
                failClosed(protocolFailure("invalid restore owner frame"))
                return
              }
              acceptOwnerFrame(decoded.value)
            }
            function onMessageError() {
              failClosed(storageFailure(new Error("restore channel message decoding failed")))
            }
            function onClose() {
              failClosed(storageFailure(new Error("restore channel peer closed")))
            }

            yield* Scope.addFinalizer(
              restoreScope,
              Effect.uninterruptible(
                Effect.gen(function*() {
                  stopIngress()
                  yield* Queue.shutdown(credits)
                  if (sourceWorker !== undefined) yield* Fiber.interrupt(sourceWorker)
                })
              )
            )
            port.addEventListener("message", onMessage)
            port.addEventListener("messageerror", onMessageError)
            port.addEventListener("close", onClose)
            port.start()

            sourceWorker = yield* Effect.gen(function*() {
              const pull = yield* Stream.toPull(source)
              const staging = new Uint8Array(Math.min(maxChunkBytes, maxBytes))
              type SourceResult =
                | { readonly _tag: "Chunk"; readonly chunk: Uint8Array }
                | { readonly _tag: "End" }
                | { readonly _tag: "Failure"; readonly error: ReplicaError.ReplicaError }
              const classifySourceCause = (
                cause: Cause.Cause<ReplicaError.ReplicaError | Cause.Done<void>>
              ): SourceResult | undefined => {
                if (cause.reasons.length !== 1) return undefined
                const reason = cause.reasons[0]
                if (reason === undefined || !Cause.isFailReason(reason)) return undefined
                if (Cause.isDone(reason.error)) return { _tag: "End" }
                if (Schema.is(ReplicaError.ReplicaError)(reason.error)) {
                  return { _tag: "Failure", error: reason.error }
                }
                return undefined
              }
              let pendingPull:
                | Fiber.Fiber<
                  ReadonlyArray<Uint8Array>,
                  ReplicaError.ReplicaError | Cause.Done<void>
                >
                | undefined
              let pendingChunks: ReadonlyArray<Uint8Array> | undefined
              let pendingChunkIndex = 0
              let currentChunk: Uint8Array | undefined
              let currentOffset = 0
              let cumulativeBytes = 0
              let pendingCompletion:
                | { readonly _tag: "End" }
                | { readonly _tag: "Failure"; readonly error: ReplicaError.ReplicaError }
                | undefined

              const startPull = Effect.gen(function*() {
                if (pendingPull !== undefined) return pendingPull
                const next = yield* pull.pipe(Effect.forkChild({ startImmediately: true }))
                pendingPull = next
                yield* Fiber.await(next).pipe(
                  Effect.flatMap((exit) => {
                    if (Exit.isSuccess(exit) || terminalAccepted) return Effect.void
                    if (classifySourceCause(exit.cause) !== undefined) return Effect.void
                    return Effect.sync(() => {
                      stopIngress()
                      Deferred.doneUnsafe(localFailure, Effect.failCause(exit.cause as Cause.Cause<never>))
                      sourceWorker?.interruptUnsafe()
                    })
                  }),
                  Effect.forkChild
                )
                return next
              })
              const takeSource: Effect.Effect<SourceResult> = Effect.gen(function*() {
                if (pendingChunks !== undefined) {
                  const chunk = pendingChunks[pendingChunkIndex++]
                  if (pendingChunkIndex === pendingChunks.length) {
                    pendingChunks = undefined
                    pendingChunkIndex = 0
                  }
                  if (chunk === undefined) {
                    return yield* Effect.die(new Error("restore source emitted an empty batch"))
                  }
                  return { _tag: "Chunk", chunk }
                }
                const next = yield* startPull
                const exit = yield* Fiber.await(next)
                pendingPull = undefined
                if (Exit.isSuccess(exit)) {
                  const chunk = exit.value[0]
                  if (chunk === undefined) return yield* Effect.die(new Error("restore source emitted an empty batch"))
                  if (exit.value.length > 1) {
                    pendingChunks = exit.value
                    pendingChunkIndex = 1
                  }
                  return { _tag: "Chunk", chunk }
                }
                const classified = classifySourceCause(exit.cause)
                if (classified !== undefined) return classified
                return yield* Effect.failCause(exit.cause as Cause.Cause<never>)
              })
              const postResponse = <A, I,>(
                schema: Schema.Codec<A, I, never>,
                frame: A,
                transfer?: ReadonlyArray<Transferable>
              ) =>
                terminalAccepted
                  ? Effect.succeed(false)
                  : postFrame(schema, frame, transfer).pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        pullOutstanding = false
                      })
                    ),
                    Effect.as(true)
                  )

              while (true) {
                const credit = yield* Queue.take(credits)
                const startedAt = yield* Clock.currentTimeMillis
                let staged = 0
                if (pendingCompletion !== undefined) {
                  sourceComplete = true
                  if (pendingCompletion._tag === "End") {
                    yield* postResponse(RestoreProtocol.End, {
                      _tag: "End",
                      nonce,
                      sequence: credit.sequence
                    })
                  } else {
                    yield* postResponse(RestoreProtocol.SourceFailure, {
                      _tag: "SourceFailure",
                      nonce,
                      sequence: credit.sequence,
                      error: RestoreProtocol.encodeReplicaError(pendingCompletion.error, maxErrorBytes)
                    })
                  }
                  return
                }
                while (staged < staging.byteLength) {
                  if (currentChunk !== undefined) {
                    const available = currentChunk.byteLength - currentOffset
                    const copied = Math.min(available, staging.byteLength - staged)
                    staging.set(currentChunk.subarray(currentOffset, currentOffset + copied), staged)
                    staged += copied
                    currentOffset += copied
                    if (currentOffset === currentChunk.byteLength) {
                      currentChunk = undefined
                      currentOffset = 0
                    }
                    if (staged === staging.byteLength) break
                  }

                  if (pendingChunks === undefined) {
                    const nextFiber = yield* startPull
                    if (staged > 0) {
                      const now = yield* Clock.currentTimeMillis
                      const remaining = startedAt + maxCoalesceMillis - now
                      if (remaining <= 0) break
                      const next = yield* Effect.raceFirst(
                        Fiber.await(nextFiber).pipe(
                          Effect.map((exit) => ({ _tag: "Source" as const, exit }))
                        ),
                        Effect.sleep(remaining).pipe(
                          Effect.as({ _tag: "Coalesce" as const })
                        )
                      )
                      if (next._tag === "Coalesce") break
                    }
                  }

                  const result = yield* takeSource
                  if (result._tag === "Chunk") {
                    if (result.chunk.byteLength === 0) continue
                    const observed = cumulativeBytes + result.chunk.byteLength
                    if (observed > maxBytes) {
                      pendingCompletion = {
                        _tag: "Failure",
                        error: new ReplicaError.ReplicaError({
                          reason: new ReplicaError.BackupTooLarge({
                            limit: maxBytes,
                            observed
                          })
                        })
                      }
                      break
                    }
                    cumulativeBytes = observed
                    currentChunk = result.chunk
                    currentOffset = 0
                    continue
                  }
                  pendingCompletion = result
                  break
                }

                if (staged > 0) {
                  const bytes = staging.slice(0, staged)
                  const posted = yield* postResponse(RestoreProtocol.Chunk, {
                    _tag: "Chunk",
                    nonce,
                    sequence: credit.sequence,
                    bytes
                  }, [bytes.buffer])
                  if (!posted) return
                  continue
                }
                sourceComplete = true
                if (pendingCompletion?._tag === "Failure") {
                  yield* postResponse(RestoreProtocol.SourceFailure, {
                    _tag: "SourceFailure",
                    nonce,
                    sequence: credit.sequence,
                    error: RestoreProtocol.encodeReplicaError(pendingCompletion.error, maxErrorBytes)
                  })
                } else {
                  yield* postResponse(RestoreProtocol.End, {
                    _tag: "End",
                    nonce,
                    sequence: credit.sequence
                  })
                }
                return
              }
            }).pipe(
              Effect.provideService(Scope.Scope, restoreScope),
              Effect.forkChild({ startImmediately: true })
            )
            yield* Fiber.await(sourceWorker).pipe(
              Effect.flatMap((exit) => {
                if (Exit.isSuccess(exit) || terminalAccepted) return Effect.void
                return Effect.sync(() => {
                  stopIngress()
                  const cause = Cause.map(
                    exit.cause,
                    (error) => new RestoreBackupError({ error })
                  )
                  Deferred.doneUnsafe(localFailure, Effect.failCause(cause))
                })
              }),
              Effect.forkChild
            )

            yield* postFrame(RestoreProtocol.Start, {
              _tag: "Start",
              nonce,
              sequence: 0
            })
            const frame = yield* Effect.raceFirst(
              Deferred.await(terminal),
              Deferred.await(localFailure)
            )
            let authoritative: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
            if (frame._tag === "TerminalSuccess") {
              authoritative = Exit.void
            } else if (frame._tag === "TerminalDefect") {
              authoritative = Exit.failCause(Cause.die(RestoreProtocol.decodeDefect(frame.defect)))
            } else {
              const decoded = yield* Effect.exit(RestoreProtocol.decodeReplicaError(frame.error))
              if (frame._tag === "TerminalSessionFailure") {
                authoritative = decoded
              } else {
                authoritative = Exit.isFailure(decoded)
                  ? Exit.failCause(
                    Cause.map(decoded.cause, (error) => new RestoreBackupError({ error }))
                  )
                  : Exit.die(new Error("restore error decoder unexpectedly succeeded"))
              }
            }
            acceptTerminal(authoritative)
            terminalAckPosted = yield* postFrame(RestoreProtocol.TerminalAck, {
              _tag: "TerminalAck",
              nonce,
              sequence: frame.sequence
            }).pipe(
              Effect.as(true),
              Effect.catchCause((cause) =>
                Cause.hasInterrupts(cause)
                  ? Effect.failCause(cause)
                  : Effect.succeed(false)
              )
            )
            if (terminalAckPosted) yield* Deferred.await(released)
            return yield* authoritative
          }).pipe(Effect.provideService(Scope.Scope, restoreScope)),
        (restoreScope, exit) => Scope.close(restoreScope, exit)
      )

    return {
      get ownerEpoch() {
        return SubscriptionRef.getUnsafe(sessions).lease.ownerEpoch
      },
      invalidations,
      create: (document, options) =>
        Wire.encode(document.schema, options.value).pipe(
          Effect.flatMap((value) =>
            withSession("Create", (session) =>
              recoverCommand(
                options.commandId,
                "Create",
                rpc.Create({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: options.commandId,
                  value
                }),
                "LookupCreate",
                rpc.LookupCreate({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: options.commandId
                })
              ), { boundOperation: false })
          )
        ),
      get: (document, documentId) =>
        withSession("Get", (session) => rpc.Get({ sessionId: session.sessionId, document: document.name, documentId }))
          .pipe(
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
            withSession("Mutate", (session) =>
              recoverCommand(
                options.commandId,
                "Mutate",
                rpc.Mutate({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: options.commandId,
                  documentId: options.documentId,
                  payload
                }),
                "LookupMutation",
                rpc.LookupMutation({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: options.commandId
                })
              ), { boundOperation: false })
          ),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.successSchema, mutation.errorSchema, outcome))
        ),
      delete: (document, options) =>
        withSession("Delete", (session) =>
          recoverCommand(
            options.commandId,
            "Delete",
            rpc.Delete({ sessionId: session.sessionId, document: document.name, ...options }),
            "LookupDelete",
            rpc.LookupDelete({ sessionId: session.sessionId, document: document.name, commandId: options.commandId })
          ), { boundOperation: false }).pipe(
            Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome))
          ),
      query: (query, ...payload) =>
        Wire.encode(query.payloadSchema, payload[0]).pipe(
          Effect.flatMap((encoded) =>
            withSession("Query", (session) =>
              rpc.Query({ sessionId: session.sessionId, query: query.name, payload: encoded }))
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
            ReplicaQueryError: (error) =>
              Wire.decode(query.errorSchema, error.error).pipe(Effect.flatMap(Effect.fail))
          }),
          Effect.flatMap((encoded) => Wire.decode(query.successSchema, encoded))
        ),
      lookupMutation: (mutation, commandId) =>
        withSession(
          "LookupMutation",
          (session) => rpc.LookupMutation({ sessionId: session.sessionId, mutation: mutation.name, commandId })
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
        withSession(
          "LookupCreate",
          (session) => rpc.LookupCreate({ sessionId: session.sessionId, document: document.name, commandId })
        )
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
        withSession(
          "LookupDelete",
          (session) => rpc.LookupDelete({ sessionId: session.sessionId, document: document.name, commandId })
        )
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
      flush: withSession("Flush", (session) => rpc.Flush({ sessionId: session.sessionId })).pipe(
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
        Effect.gen(function*() {
          const startedAt = yield* Clock.currentTimeMillis
          const deadline = Math.min(
            Number.MAX_SAFE_INTEGER,
            startedAt + timeoutMillis(operationTimeout)
          )
          const maxBytes = yield* Backup.validateMaxBytes(options.maxBytes)
          let acceptedTerminal:
            | Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
            | undefined
          const operation = withSession(
            "RestoreBackup",
            (session) =>
              rpc.BeginRestoreBackupV4({
                sessionId: session.sessionId,
                mode: options.mode,
                maxBytes,
                expectedDefinitionHash: options.expectedDefinitionHash,
                installationId: options.installationId
              }).pipe(
                Effect.catchCause((cause) => {
                  if (
                    cause.reasons.length === 1 &&
                    Cause.isDieReason(cause.reasons[0]) &&
                    isDataCloneError(cause.reasons[0].defect)
                  ) {
                    return Effect.fail(
                      storageFailure(cause.reasons[0].defect)
                    )
                  }
                  return Effect.failCause(cause)
                }),
                Effect.catchTag("RpcClientError", (error) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause: error
                      })
                    })
                  )),
                Effect.flatMap(({ nonce, port }) =>
                  Effect.acquireUseRelease(
                    Effect.sync(() => {
                      const nativeClose = port.close.bind(port)
                      let open = true
                      return {
                        nonce,
                        port,
                        close: () => {
                          if (!open) return
                          open = false
                          nativeClose()
                        }
                      }
                    }),
                    ({ close, nonce, port }) =>
                      serveRestoreSource(
                        options.source,
                        port,
                        nonce,
                        maxBytes,
                        session.lease.maxChunkBytes,
                        session.lease.maxRestoreCoalesceMillis,
                        session.lease.maxRestoreErrorBytes,
                        close,
                        (exit) => {
                          acceptedTerminal = exit
                        }
                      ),
                    ({ close }) => Effect.sync(close)
                  )
                ),
                Effect.withSpan("ReplicaClient.restoreBackup", {
                  attributes: {
                    "restore.mode": options.mode,
                    "restore.max_bytes": maxBytes,
                    "restore.max_chunk_bytes": session.lease.maxChunkBytes,
                    "restore.max_coalesce_millis": session.lease.maxRestoreCoalesceMillis,
                    "restore.max_error_bytes": session.lease.maxRestoreErrorBytes
                  }
                })
              ),
            {
              boundOperation: false,
              replayAfterReopen: false
            }
          )
          const now = yield* Clock.currentTimeMillis
          return yield* Effect.timeoutOrElse(operation, {
            duration: Math.max(0, deadline - now),
            orElse: () =>
              acceptedTerminal ??
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.OperationTimeout({
                      operation: "RestoreBackup",
                      timeoutMillis: timeoutMillis(operationTimeout)
                    })
                  })
                )
          })
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(
                cause,
                (error) => error._tag === "RestoreBackupError" ? error.error : error
              )
            )
          )
        ),
      exportDocument: (document, documentId) =>
        withSession(
          "ExportDocument",
          (session) => rpc.ExportDocument({ sessionId: session.sessionId, document: document.name, documentId })
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
              "ImportDocument",
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

export const layer = (definition: ReplicaDefinition.Any, options?: TimeoutOptions) =>
  Layer.effect(
    ReplicaClient,
    RpcClient.make(ReplicaRpc.group).pipe(Effect.flatMap((rpc) => fromRpcClient(definition, rpc, options)))
  )
