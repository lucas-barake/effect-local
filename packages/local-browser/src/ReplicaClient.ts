import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Backup from "@lucas-barake/effect-local/Backup"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Transient from "@lucas-barake/effect-local/Transient"
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
import * as Result from "effect/Result"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
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
    readonly peerConnectionStatus: PeerConnectionStatus.PeerConnectionStatus["Service"]
    readonly relayConnectionStatus: RelayConnectionStatus.RelayConnectionStatus["Service"]
    readonly transient: (
      peerId: Identity.PeerId,
      documentId: Identity.DocumentId,
      payload: Uint8Array
    ) => Effect.Effect<void, ReplicaError.ReplicaError>
    readonly transients: Stream.Stream<ReplicaRpc.TransientMessage, ReplicaError.ReplicaError>
    readonly invalidations: Stream.Stream<ReplicaRpc.Invalidation, ReplicaError.ReplicaError>
  }
>()(
  "@lucas-barake/effect-local-browser/ReplicaClient"
) {}

export interface Options {
  readonly sessionTimeout?: Duration.Input | undefined
  readonly operationTimeout?: Duration.Input | undefined
  /**
   * Closes the current owner session when the page fires `pagehide`, so a tab that navigates away
   * or closes releases its session immediately instead of holding it until the owner side lease
   * expires. Defaults to `true`; the lease remains the backstop for tabs that die without a
   * `pagehide` event.
   */
  readonly closeSessionOnPageHide?: boolean | undefined
}

export const defaultSessionTimeout: Duration.Duration = Duration.seconds(10)
export const defaultOperationTimeout: Duration.Duration = Duration.seconds(30)

const runSyncSafe = <A,>(thunk: () => A, fallback: A): A => {
  const exit = Effect.runSyncExit(Effect.try({ try: thunk, catch: () => fallback }))
  if (Exit.isSuccess(exit)) return exit.value
  return fallback
}

class RestoreBackupError extends Schema.TaggedErrorClass<RestoreBackupError>(
  "@lucas-barake/effect-local-browser/ReplicaClient/RestoreBackupError"
)("RestoreBackupError", {
  error: ReplicaError.ReplicaError
}) {}

class RestoreChannelError extends Schema.TaggedErrorClass<RestoreChannelError>(
  "@lucas-barake/effect-local-browser/ReplicaClient/RestoreChannelError"
)("RestoreChannelError", {
  message: Schema.String
}) {}

const isAuthoritativeRestoreResult = (
  exit: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
): boolean =>
  Exit.isSuccess(exit) ||
  exit.cause.reasons.every((reason) =>
    !Cause.isFailReason(reason) ||
    reason.error._tag !== "RestoreBackupError" ||
    reason.error.error.reason._tag !== "StorageUnavailable" ||
    !Schema.is(RpcClientError.RpcClientError)(reason.error.error.reason.cause)
  )

const acceptedProtocolMismatch = (
  exit: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
): ReplicaError.ReplicaError | undefined => {
  if (Exit.isSuccess(exit) || exit.cause.reasons.length !== 1) return undefined
  const reason = exit.cause.reasons[0]
  if (reason === undefined || !Cause.isFailReason(reason)) return undefined
  if (reason.error._tag === "RestoreBackupError") return undefined
  if (
    reason.error.reason._tag === "ProtocolMismatch" &&
    reason.error.reason.expected === "active session"
  ) return reason.error
  return undefined
}

const protocolFailure = (observed: string) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({
      expected: "the browser restore protocol",
      observed
    })
  })

const isActiveSessionMismatch = (error: ReplicaError.ReplicaError): boolean =>
  error.reason._tag === "ProtocolMismatch" && error.reason.expected === "active session"

const isPositiveSafeInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0

const isValidConflictLimits = (
  value: ReplicaRpc.ConflictLimits | undefined
): value is ReplicaRpc.ConflictLimits =>
  value !== undefined &&
  isPositiveSafeInteger(value.maxConflictDepth) &&
  value.maxConflictDepth <= ReplicaLimits.maxConflictDepthHardLimit &&
  isPositiveSafeInteger(value.maxConflictNodes) &&
  value.maxConflictNodes <= ReplicaLimits.maxConflictNodesHardLimit &&
  isPositiveSafeInteger(value.maxConflictAlternatives) &&
  value.maxConflictAlternatives <= ReplicaLimits.maxConflictAlternativesHardLimit &&
  isPositiveSafeInteger(value.maxConflictPathSegments) &&
  value.maxConflictPathSegments <= ReplicaLimits.maxConflictPathSegmentsHardLimit &&
  isPositiveSafeInteger(value.maxConflictValueBytes) &&
  value.maxConflictValueBytes <= ReplicaLimits.maxConflictValueBytesHardLimit

const isNativeErrorNamed = (value: unknown, name: string): boolean => {
  if (typeof value !== "object" || value === null) return false
  return runSyncSafe(
    () =>
      Object.prototype.toString.call(value) === "[object DOMException]" &&
      Reflect.get(value, "name") === name,
    false
  )
}

const isDataCloneError = (value: unknown): boolean => isNativeErrorNamed(value, "DataCloneError")

const ownDataProperties = (
  value: object,
  maxProperties: number
): ReadonlyMap<string, unknown> | undefined => {
  return runSyncSafe(() => {
    const properties = new Map<string, unknown>()
    for (const key in value) {
      if (!Object.prototype.hasOwnProperty.call(value, key)) continue
      if (properties.size >= maxProperties) return undefined
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor === undefined || !("value" in descriptor)) return undefined
      properties.set(key, descriptor.value)
    }
    return properties
  }, undefined)
}

const preflightOwnerFrame = (value: unknown): boolean => {
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
    typeof sequence !== "number" ||
    !Number.isSafeInteger(sequence) ||
    sequence < 0
  ) return false
  const expectedFields = RestoreProtocol.ownerToPageFrameFields[tag]
  if (
    expectedFields === undefined ||
    properties.size !== expectedFields.size ||
    Array.from(expectedFields).some((field) => !properties.has(field))
  ) return false
  return true
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
    default:
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
  options?: Options
): Effect.Effect<ReplicaClient["Service"], ReplicaError.ReplicaError, Scope.Scope | Crypto.Crypto> =>
  Effect.gen(function*() {
    const sessionTimeout = Duration.fromInputUnsafe(options?.sessionTimeout ?? defaultSessionTimeout)
    const operationTimeout = Duration.fromInputUnsafe(options?.operationTimeout ?? defaultOperationTimeout)
    const reportedTimeoutMillis = (duration: Duration.Duration) => {
      const millis = Duration.toMillis(duration)
      if (millis <= 0 || Number.isNaN(millis)) return 0
      if (millis >= Number.MAX_SAFE_INTEGER) return Number.MAX_SAFE_INTEGER
      return Math.trunc(millis)
    }
    const sessionReportedTimeoutMillis = reportedTimeoutMillis(sessionTimeout)
    const operationReportedTimeoutMillis = reportedTimeoutMillis(operationTimeout)
    const boundBy =
      (operation: string, duration: Duration.Duration, reportedMillis: number) =>
      <A, E, R,>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | ReplicaError.ReplicaError, R> =>
        Effect.acquireUseRelease(
          Effect.forkChild(Effect.interruptible(effect), { startImmediately: true }),
          (fiber) =>
            Fiber.join(fiber).pipe(
              Effect.timeoutOption(duration),
              Effect.flatMap((result) => {
                if (Option.isSome(result)) return Effect.succeed(result.value)
                return Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.OperationTimeout({
                      operation,
                      timeoutMillis: reportedMillis
                    })
                  })
                )
              })
            ),
          Fiber.interrupt
        )
    const boundSession = (operation: string) => boundBy(operation, sessionTimeout, sessionReportedTimeoutMillis)
    const boundOperation = (operation: string) => boundBy(operation, operationTimeout, operationReportedTimeoutMillis)
    /**
     * Dispatches a command and, if the answer is lost in transit, asks the owner what happened to
     * the id rather than reissuing it.
     *
     * Ambiguity leaves as a failure rather than an `OutcomeUnknown` value, and it carries the error
     * that produced it: whether the dispatch timed out or the owner answered but held no receipt
     * changes what an operator should do, and flattening both to a bare tag loses that.
     */
    const recoverCommand = <A extends CommandOutcome.CommandOutcome<unknown, unknown>,>(
      commandId: Identity.CommandId,
      dispatchOperation: string,
      dispatch: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>,
      lookupOperation: string,
      lookup: Effect.Effect<A, ReplicaError.ReplicaError | RpcClientError.RpcClientError>
    ): Effect.Effect<A, ReplicaError.ReplicaError> => {
      const ambiguous = (cause: unknown) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CommandOutcomeUnknown({ commandId, cause })
          })
        )
      return Effect.uninterruptibleMask((restore) => {
        // `cause` is the dispatch failure that sent us here, not the lookup's own. A lookup that
        // simply finds no receipt is the same ambiguity, and the useful half of the story is why the
        // first answer went missing.
        const preserveCause = (
          cause: Cause.Cause<ReplicaError.ReplicaError | RpcClientError.RpcClientError>
        ): Effect.Effect<never, ReplicaError.ReplicaError> =>
          Function.prototype.call.bind(Effect.failCause)(undefined, cause)
        const lookupOrFail = (cause: unknown): Effect.Effect<A, ReplicaError.ReplicaError> =>
          restore(lookup.pipe(boundOperation(lookupOperation))).pipe(
            Effect.catchCause((lookupCause) => {
              if (Cause.hasInterrupts(lookupCause) || Cause.hasDies(lookupCause)) {
                return preserveCause(lookupCause)
              }
              if (lookupCause.reasons.length !== 1) {
                return preserveCause(lookupCause)
              }
              const failure = Cause.findFail(lookupCause)
              if (Result.isFailure(failure)) return preserveCause(lookupCause)
              const reason = failure.success
              if (Schema.is(RpcClientError.RpcClientError)(reason.error)) return ambiguous(cause)
              if (ReplicaError.isReplicaError(reason.error)) {
                if (reason.error.reason._tag === "OperationTimeout") return ambiguous(cause)
                return Effect.fail(reason.error)
              }
              return preserveCause(lookupCause)
            }),
            Effect.flatMap((outcome) => {
              if (outcome._tag === "OutcomeUnknown") return ambiguous(cause)
              return Effect.succeed(outcome)
            })
          )
        return restore(dispatch.pipe(boundOperation(dispatchOperation))).pipe(
          Effect.catchCause((dispatchCause) => {
            if (Cause.hasInterrupts(dispatchCause) || Cause.hasDies(dispatchCause)) {
              return preserveCause(dispatchCause)
            }
            if (dispatchCause.reasons.length !== 1) {
              return preserveCause(dispatchCause)
            }
            const failure = Cause.findFail(dispatchCause)
            if (Result.isFailure(failure)) return preserveCause(dispatchCause)
            const reason = failure.success
            if (Schema.is(RpcClientError.RpcClientError)(reason.error)) {
              return lookupOrFail(reason.error)
            }
            if (ReplicaError.isReplicaError(reason.error)) {
              if (reason.error.reason._tag === "OperationTimeout") return lookupOrFail(reason.error)
              return Effect.fail(reason.error)
            }
            return preserveCause(dispatchCause)
          }),
          // A postcommit publication failure can make the owner report ambiguity even though the
          // durable receipt is already visible. Look it up before reporting the outcome as unknown.
          Effect.flatMap((outcome) => {
            if (outcome._tag === "OutcomeUnknown") return lookupOrFail(undefined)
            return Effect.succeed(outcome)
          })
        )
      })
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
        !isValidConflictLimits(lease.conflictLimits) ||
        !isPositiveSafeInteger(lease.maxChunkBytes) ||
        !isPositiveSafeInteger(lease.maxRestoreCoalesceMillis) ||
        !isPositiveSafeInteger(lease.maxRestoreErrorBytes) ||
        lease.maxRestoreErrorBytes < ReplicaLimits.minimumRestoreErrorBytes
      ) {
        yield* Effect.ignore(closeSession(sessionId))
        return yield* protocolFailure("invalid advertised replica limits")
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
          maxRestoreErrorBytes: lease.maxRestoreErrorBytes,
          conflictLimits: lease.conflictLimits
        }
      }
    })
    const newSession = makeSessionId.pipe(Effect.flatMap(openSession))
    const sessions = yield* Effect.acquireRelease(
      newSession.pipe(Effect.flatMap(SubscriptionRef.make)),
      (sessionRef) =>
        SubscriptionRef.get(sessionRef).pipe(
          Effect.flatMap((session) => Effect.ignore(closeSession(session.sessionId)))
        )
    )
    // Hoisted out of the branch below so long lived streams can observe it even when this client
    // never installs the listener: a stream that outlives the page must still stop.
    const pageHidden = yield* Deferred.make<void>()
    if (options?.closeSessionOnPageHide !== false && typeof globalThis.addEventListener === "function") {
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const onPageHide = () => {
            Deferred.doneUnsafe(pageHidden, Exit.void)
          }
          globalThis.addEventListener("pagehide", onPageHide)
          return onPageHide
        }),
        (onPageHide) => Effect.sync(() => globalThis.removeEventListener("pagehide", onPageHide))
      )
      // The pagehide event only releases the Deferred. The close itself runs as an ordinary
      // scoped fiber of this client, so the RPC goes through the same runtime and interruption
      // rules as every other session operation instead of a detached run.
      yield* Deferred.await(pageHidden).pipe(
        Effect.andThen(
          Effect.flatMap(SubscriptionRef.get(sessions), (session) => Effect.ignore(closeSession(session.sessionId)))
        ),
        Effect.forkScoped
      )
    }
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
            if (Deferred.isDoneUnsafe(pageHidden)) return restore(Effect.interrupt)
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
          Effect.flatMap((candidate) => {
            if (Option.isSome(candidate) && candidate.value.staleSessionId === stale.sessionId) {
              return Effect.succeed(candidate.value.candidateSessionId)
            }
            return makeSessionId.pipe(
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
          }),
          Effect.flatMap((sessionId) => {
            const attempt = openSession(sessionId)
            if (retryTransient) {
              return attempt.pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), while: isTransient }))
            }
            return attempt
          })
        )
      ).pipe(
        Effect.tap(() =>
          Ref.update(
            replacementCandidate,
            (candidate) => {
              if (Option.isSome(candidate) && candidate.value.staleSessionId === stale.sessionId) {
                return Option.none()
              }
              return candidate
            }
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
      sessionOptions?: {
        readonly boundOperation?: boolean
        readonly replayAfterReopen?: boolean
        readonly replaceSession?: (
          stale: Session
        ) => Effect.Effect<Session, ReplicaError.ReplicaError>
      }
    ) => {
      const bounded = (session: Session) => {
        if (sessionOptions?.boundOperation === false) return use(session)
        return use(session).pipe(boundOperation(operation))
      }
      return SubscriptionRef.get(sessions).pipe(
        Effect.flatMap((session) =>
          bounded(session).pipe(
            Effect.catchCause((cause) => {
              let reason: Cause.Cause<unknown>["reasons"][number] | undefined
              if (cause.reasons.length === 1) reason = cause.reasons[0]
              if (
                reason === undefined ||
                !Cause.isFailReason(reason) ||
                !Schema.is(ReplicaError.ReplicaError)(reason.error) ||
                !isActiveSessionMismatch(reason.error)
              ) {
                return Effect.failCause(cause)
              }
              const replaceSession = sessionOptions?.replaceSession ?? reopen
              if (sessionOptions?.replayAfterReopen === false) {
                return replaceSession(session).pipe(Effect.andThen(Effect.fail(reason.error)))
              }
              return replaceSession(session).pipe(Effect.flatMap(bounded))
            })
          )
        )
      )
    }
    const withSessionStream = <A, E, R,>(
      use: (
        session: Session
      ) => Stream.Stream<A, E | ReplicaError.ReplicaError, R>,
      replaceSession: (stale: Session) => Effect.Effect<Session, ReplicaError.ReplicaError> = reopen,
      emittedRef?: Ref.Ref<boolean>,
      pendingMismatchRef?: Ref.Ref<Option.Option<PendingStreamMismatch>>
    ) =>
      Stream.unwrap(Effect.gen(function*() {
        const currentSession = yield* SubscriptionRef.get(sessions)
        let emitted: Ref.Ref<boolean>
        if (emittedRef === undefined) emitted = yield* Ref.make(false)
        else emitted = emittedRef
        if (pendingMismatchRef !== undefined) {
          const pending = yield* Ref.get(pendingMismatchRef)
          if (Option.isSome(pending)) {
            return Stream.unwrap(
              replaceSession(pending.value.stale).pipe(
                Effect.andThen(Ref.set(pendingMismatchRef, Option.none())),
                Effect.as(Stream.fail(pending.value.error))
              )
            )
          }
        }
        const tracked = (trackedSession: Session) => use(trackedSession).pipe(Stream.tap(() => Ref.set(emitted, true)))
        return tracked(currentSession).pipe(
          Stream.catchTag("ReplicaError", (error) => {
            if (Schema.is(ReplicaError.ReplicaError)(error) && isActiveSessionMismatch(error)) {
              return Stream.unwrap(Effect.gen(function*() {
                const hasEmitted = yield* Ref.get(emitted)
                if (hasEmitted && pendingMismatchRef !== undefined) {
                  yield* Ref.set(pendingMismatchRef, Option.some({ stale: currentSession, error }))
                }
                const next = yield* replaceSession(currentSession)
                if (!hasEmitted) {
                  return tracked(next)
                }
                if (pendingMismatchRef !== undefined) {
                  yield* Ref.set(pendingMismatchRef, Option.none())
                }
                return Stream.fail(error)
              }))
            }
            return Stream.fail(error)
          })
        )
      })).pipe(Stream.interruptWhen(Deferred.await(pageHidden)))
    const retrySchedule = Schedule.spaced("1 second")
    const sessionFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    yield* Effect.raceFirst(
      Effect.gen(function*() {
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
          Effect.catchReason(
            "ReplicaError",
            "ProtocolMismatch",
            (reason, error) => {
              if (reason.expected === "active session") return reopen(session).pipe(Effect.as(undefined))
              return Effect.fail(error)
            }
          )
        )
        if (renewed !== undefined && renewed.leaseMillis !== session.lease.leaseMillis) {
          yield* SubscriptionRef.updateSome(
            sessions,
            (latestSession) => {
              if (latestSession.sessionId === session.sessionId) {
                return Option.some({ ...session, lease: { ...session.lease, ...renewed } })
              }
              return Option.none()
            }
          )
        }
      }).pipe(Effect.forever),
      Deferred.await(pageHidden)
    ).pipe(
      Effect.tapError((error) => Deferred.fail(sessionFailure, error)),
      Effect.tapCause(Effect.logError),
      Effect.ignore,
      Effect.forkScoped
    )
    const allInvalidationKeys = [
      ...ReplicaDefinition.invalidationKeys(definition),
      ReplicaRpc.commandDeliveryInvalidationKey
    ]
    const fullRefresh = (ownerEpoch: string): ReplicaRpc.Invalidation => ({
      _tag: "FullRefreshRequired",
      ownerEpoch,
      keys: [...allInvalidationKeys]
    })
    const invalidationsFor = (
      session: Session
    ): Stream.Stream<ReplicaRpc.InvalidationMessage, ReplicaError.ReplicaError> =>
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
          (reason, error) => {
            if (isActiveSessionMismatch(error) || reason.observed === session.lease.ownerEpoch) {
              return Stream.unwrap(reopen(session).pipe(Effect.map(invalidationsFor)))
            }
            return Stream.fail(error)
          }
        )
      )
    const advanceWatermark = (
      observed: number | undefined,
      sequence: number
    ): { readonly next: number; readonly emit: "skip" | "event" | "refresh" } => {
      if (observed === undefined || sequence > observed + 1) {
        return { next: sequence, emit: "refresh" }
      }
      if (sequence === observed + 1) return { next: sequence, emit: "event" }
      return { next: observed, emit: "skip" }
    }
    const invalidationMessages: Stream.Stream<
      ReplicaRpc.InvalidationMessage,
      ReplicaError.ReplicaError
    > = Stream.unwrap(
      SubscriptionRef.get(sessions).pipe(
        Effect.map(invalidationsFor)
      )
    ).pipe(
      Stream.retry(
        Schedule.forever.pipe(
          Schedule.setInputType<ReplicaError.ReplicaError>(),
          Schedule.while(({ input }) => isActiveSessionMismatch(input))
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
          readonly deliveryWatermark: number | undefined
          readonly deliveryRefreshEpoch: number | undefined
        } => ({
          ownerEpoch: initialSession.lease.ownerEpoch,
          watermark: undefined,
          refreshGeneration: undefined,
          deliveryWatermark: undefined,
          deliveryRefreshEpoch: undefined
        }),
        (state, event) => {
          if (event.ownerEpoch !== state.ownerEpoch) {
            if (event._tag === "InvalidationsReady") {
              return [
                {
                  ownerEpoch: event.ownerEpoch,
                  watermark: event.watermark,
                  refreshGeneration: event.refreshGeneration,
                  deliveryWatermark: event.deliveryWatermark,
                  deliveryRefreshEpoch: event.deliveryRefreshEpoch
                },
                [fullRefresh(event.ownerEpoch)]
              ]
            }
            return [
              {
                ownerEpoch: event.ownerEpoch,
                watermark: undefined,
                refreshGeneration: undefined,
                deliveryWatermark: undefined,
                deliveryRefreshEpoch: undefined
              },
              [fullRefresh(event.ownerEpoch)]
            ]
          }
          if (event._tag === "InvalidationsReady") {
            let commitRefresh = event.watermark > 0 || event.refreshGeneration > 0
            if (state.watermark !== undefined) {
              commitRefresh = event.watermark !== state.watermark || event.refreshGeneration !== state.refreshGeneration
            }
            let deliveryRefresh = event.deliveryWatermark > 0 || event.deliveryRefreshEpoch > 0
            if (state.deliveryWatermark !== undefined) {
              deliveryRefresh = event.deliveryWatermark !== state.deliveryWatermark ||
                event.deliveryRefreshEpoch !== state.deliveryRefreshEpoch
            }
            const refreshKeys: Array<ReplicaRpc.Invalidation> = []
            if (commitRefresh || deliveryRefresh) refreshKeys.push(fullRefresh(event.ownerEpoch))
            return [
              {
                ...state,
                watermark: event.watermark,
                refreshGeneration: event.refreshGeneration,
                deliveryWatermark: event.deliveryWatermark,
                deliveryRefreshEpoch: event.deliveryRefreshEpoch
              },
              refreshKeys
            ]
          }
          if (event._tag === "FullRefreshRequired") {
            return [{ ...state, watermark: undefined, refreshGeneration: undefined }, [event]]
          }
          if (event._tag === "DeliveryFullRefreshRequired") {
            return [{
              ...state,
              deliveryWatermark: undefined,
              deliveryRefreshEpoch: undefined
            }, [event]]
          }
          if (event._tag === "Invalidation") {
            const advanced = advanceWatermark(state.watermark, event.sequence)
            let emitted: ReadonlyArray<ReplicaRpc.Invalidation>
            if (advanced.emit === "skip") emitted = []
            else if (advanced.emit === "event") emitted = [event]
            else emitted = [fullRefresh(event.ownerEpoch)]
            return [
              { ...state, watermark: Identity.CommitSequence.make(advanced.next) },
              emitted
            ]
          }
          const advanced = advanceWatermark(state.deliveryWatermark, event.sequence)
          let emitted: ReadonlyArray<ReplicaRpc.Invalidation>
          if (advanced.emit === "skip") emitted = []
          else if (advanced.emit === "event") emitted = [event]
          else emitted = [fullRefresh(event.ownerEpoch)]
          return [{ ...state, deliveryWatermark: advanced.next }, emitted]
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
      awaitResult: Effect.Effect<void, ReplicaError.ReplicaError | RestoreBackupError>,
      acceptResult: (
        exit: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
      ) => Effect.Effect<void, ReplicaError.ReplicaError>
    ): Effect.Effect<void, ReplicaError.ReplicaError | RestoreBackupError, R> =>
      Effect.scoped(
        Effect.gen(function*() {
          const credits = yield* Queue.bounded<RestoreProtocol.Pull>(1)
          const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
          const released = yield* Deferred.make<void>()
          const acceptedResultDisconnect = yield* Deferred.make<void>()
          const localFailure = yield* Deferred.make<never, RestoreBackupError>()
          let logicalOpen = true
          let terminalAccepted = false
          let resultAccepted = false
          let terminalAckPosted = false
          let expectedOwnerSequence = 1
          let pullOutstanding = false
          let sourceComplete = false
          let sourceWorker: Fiber.Fiber<void, ReplicaError.ReplicaError | Cause.Done> | undefined

          const removeListeners = () => {
            runSyncSafe(() => port.removeEventListener("message", onMessage), undefined)
            runSyncSafe(() => port.removeEventListener("messageerror", onMessageError), undefined)
            runSyncSafe(() => port.removeEventListener("close", onClose), undefined)
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
            } else if (resultAccepted) {
              Deferred.doneUnsafe(acceptedResultDisconnect, Effect.void)
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
            if (!preflightOwnerFrame(value)) {
              return Exit.fail(protocolFailure("invalid restore owner frame"))
            }
            return Schema.decodeUnknownExit(
              RestoreProtocol.OwnerToPageFrame,
              { onExcessProperty: "error" }
            )(value)
          }
          const postFrame = <A, I,>(
            schema: Schema.Codec<A, I>,
            frame: A,
            transfer?: ReadonlyArray<Transferable>
          ): Effect.Effect<void, ReplicaError.ReplicaError> =>
            Schema.encodeUnknownEffect(schema)(frame).pipe(
              Effect.flatMap((encoded) => {
                const transfers: Array<Transferable> = []
                if (transfer !== undefined) transfers.push(...transfer)
                return Effect.try({
                  try: () => port.postMessage(encoded, transfers),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({ cause })
                    })
                })
              }),
              Effect.mapError((cause) => {
                if (ReplicaError.isReplicaError(cause)) return cause
                return new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({ cause })
                })
              })
            )
          const postReleasedAck = (frame: RestoreProtocol.Released) => {
            const encoded = Schema.encodeUnknownExit(RestoreProtocol.ReleasedAck)({
              _tag: "ReleasedAck",
              nonce,
              sequence: frame.sequence
            })
            if (Exit.isFailure(encoded)) return
            Effect.runSyncExit(Effect.try({
              try: () => port.postMessage(encoded.value),
              catch: () => undefined
            }))
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
            if (frame._tag === "TerminalReady") {
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
            const decoded = decodeOwnerFrame(event.data)
            if (Exit.isFailure(decoded)) {
              failClosed(protocolFailure("invalid restore owner frame"))
              return
            }
            acceptOwnerFrame(decoded.value)
          }
          function onMessageError() {
            failClosed(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: new RestoreChannelError({ message: "restore channel message decoding failed" })
                })
              })
            )
          }
          function onClose() {
            failClosed(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: new RestoreChannelError({ message: "restore channel peer closed" })
                })
              })
            )
          }

          const cleanup = Effect.uninterruptible(
            Effect.gen(function*() {
              stopIngress()
              yield* Queue.shutdown(credits)
              if (sourceWorker !== undefined) yield* Fiber.interrupt(sourceWorker)
            })
          )
          return yield* Effect.gen(function*() {
            port.addEventListener("message", onMessage)
            port.addEventListener("messageerror", onMessageError)
            port.addEventListener("close", onClose)
            port.start()

            sourceWorker = yield* Effect.gen(function*() {
              const pull = yield* Stream.toPull(source)
              const staging = new Uint8Array(
                Math.min(RestoreProtocol.maxPageStagingBytes, maxChunkBytes, maxBytes)
              )
              type SourceResult =
                | { readonly _tag: "Chunk"; readonly chunk: Uint8Array }
                | { readonly _tag: "End" }
                | { readonly _tag: "Failure"; readonly error: ReplicaError.ReplicaError }
              const classifySourceCause = (
                cause: Cause.Cause<ReplicaError.ReplicaError | Cause.Done>
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
                  ReplicaError.ReplicaError | Cause.Done
                >
                | undefined
              let pendingPullObserved = false
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
                const next = yield* pull.pipe(Effect.forkChild)
                pendingPull = next
                pendingPullObserved = false
                return next
              })
              const observePendingPull = () => {
                if (pendingPull === undefined || pendingPullObserved) return
                pendingPullObserved = true
                pendingPull.addObserver((exit) => {
                  if (Exit.isSuccess(exit) || terminalAccepted) return
                  if (resultAccepted) {
                    stopIngress()
                    Deferred.doneUnsafe(acceptedResultDisconnect, Effect.void)
                    sourceWorker?.interruptUnsafe()
                    return
                  }
                  if (classifySourceCause(exit.cause) !== undefined) return
                  stopIngress()
                  Deferred.doneUnsafe(
                    localFailure,
                    Effect.failCause(
                      Cause.map(exit.cause, (error) => {
                        if (Schema.is(ReplicaError.ReplicaError)(error)) {
                          return new RestoreBackupError({ error })
                        }
                        return new RestoreBackupError({
                          error: new ReplicaError.ReplicaError({
                            reason: new ReplicaError.StorageUnavailable({ cause: error })
                          })
                        })
                      })
                    )
                  )
                  sourceWorker?.interruptUnsafe()
                })
              }
              const takeSource: Effect.Effect<SourceResult, ReplicaError.ReplicaError | Cause.Done> = Effect.gen(
                function*() {
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
                  pendingPullObserved = false
                  if (Exit.isSuccess(exit)) {
                    const chunk = exit.value[0]
                    if (chunk === undefined) {
                      return yield* Effect.die(new Error("restore source emitted an empty batch"))
                    }
                    if (exit.value.length > 1) {
                      pendingChunks = exit.value
                      pendingChunkIndex = 1
                    }
                    return { _tag: "Chunk", chunk }
                  }
                  const classified = classifySourceCause(exit.cause)
                  if (classified !== undefined) return classified
                  return yield* Effect.failCause(exit.cause)
                }
              )
              const postResponse = <A, I,>(
                schema: Schema.Codec<A, I>,
                frame: A,
                transfer?: ReadonlyArray<Transferable>
              ) => {
                if (terminalAccepted) return Effect.succeed(false)
                return postFrame(schema, frame, transfer).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      pullOutstanding = false
                    })
                  ),
                  Effect.as(true)
                )
              }

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
                          Effect.map((
                            exit
                          ) => ({ _tag: "Source", exit } satisfies {
                            readonly _tag: "Source"
                            readonly exit: typeof exit
                          }))
                        ),
                        Effect.sleep(remaining).pipe(
                          Effect.as({ _tag: "Coalesce" } satisfies { readonly _tag: "Coalesce" })
                        )
                      )
                      if (next._tag === "Coalesce") {
                        observePendingPull()
                        break
                      }
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
              Effect.forkChild({ startImmediately: true })
            )
            yield* Fiber.await(sourceWorker).pipe(
              Effect.flatMap((exit) => {
                if (Exit.isSuccess(exit) || terminalAccepted) return Effect.void
                if (resultAccepted) {
                  return Effect.sync(() => {
                    stopIngress()
                    Deferred.doneUnsafe(acceptedResultDisconnect, Effect.void)
                  })
                }
                return Effect.sync(() => {
                  stopIngress()
                  const cause = Cause.map(exit.cause, (error) => {
                    if (Schema.is(ReplicaError.ReplicaError)(error)) {
                      return new RestoreBackupError({ error })
                    }
                    return new RestoreBackupError({
                      error: new ReplicaError.ReplicaError({
                        reason: new ReplicaError.StorageUnavailable({ cause: error })
                      })
                    })
                  })
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
            const awaitTerminal = Effect.raceFirst(
              Deferred.await(terminal),
              Deferred.await(localFailure)
            )
            const first = yield* Effect.raceFirst(
              awaitTerminal.pipe(
                Effect.map((
                  terminalFrame
                ) => ({ _tag: "Terminal", frame: terminalFrame } satisfies {
                  readonly _tag: "Terminal"
                  readonly frame: typeof terminalFrame
                }))
              ),
              Effect.exit(awaitResult).pipe(
                Effect.filterOrElse(
                  isAuthoritativeRestoreResult,
                  () => Effect.never
                ),
                Effect.map((
                  exit
                ) => ({ _tag: "Result", exit } satisfies { readonly _tag: "Result"; readonly exit: typeof exit }))
              )
            )
            let frame: RestoreProtocol.TerminalReady
            let authoritative: Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
            if (first._tag === "Result") {
              resultAccepted = true
              yield* acceptResult(first.exit)
              const next = yield* Effect.raceFirst(
                awaitTerminal.pipe(
                  Effect.map((
                    terminalFrame
                  ) => ({ _tag: "Terminal", frame: terminalFrame } satisfies {
                    readonly _tag: "Terminal"
                    readonly frame: typeof terminalFrame
                  }))
                ),
                Deferred.await(acceptedResultDisconnect).pipe(
                  Effect.as({ _tag: "Disconnected" } satisfies { readonly _tag: "Disconnected" })
                )
              )
              if (next._tag === "Disconnected") return yield* first.exit
              frame = next.frame
              authoritative = first.exit
            } else {
              frame = first.frame
              authoritative = yield* Effect.exit(awaitResult)
              resultAccepted = true
              yield* acceptResult(authoritative)
            }
            terminalAckPosted = yield* postFrame(RestoreProtocol.TerminalAck, {
              _tag: "TerminalAck",
              nonce,
              sequence: frame.sequence
            }).pipe(
              Effect.as(true),
              Effect.catch(() => Effect.succeed(false))
            )
            if (terminalAckPosted) yield* Deferred.await(released)
            return yield* authoritative
          }).pipe(Effect.ensuring(cleanup))
        })
      )

    type ArchiveTransferOptions<R,> =
      | Backup.RestoreOptions<R>
      | (
        Backup.InstallDocumentOptions<R> & {
          readonly mode: "document"
          readonly document: Document.Any
        }
      )
    type ClientService = Omit<ReplicaClient["Service"], "restoreBackup"> & {
      readonly restoreBackup: <R,>(
        options: ArchiveTransferOptions<R>
      ) => Effect.Effect<void, ReplicaError.ReplicaError, R>
    }
    const client: ClientService = {
      get ownerEpoch() {
        return SubscriptionRef.getUnsafe(sessions).lease.ownerEpoch
      },
      peerConnectionStatus: PeerConnectionStatus.PeerConnectionStatus.of({
        status: (peerId) =>
          withSessionStream((session) =>
            rpc.PeerConnectionStatus({
              sessionId: session.sessionId,
              peerId
            })
          ).pipe(
            Stream.catchTag("RpcClientError", (error) =>
              Stream.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: error
                  })
                })
              )),
            // Same recovery as `relayConnectionStatus` below: an owner engine reset fails every
            // rpc port under the tab, and without the retry the per-peer indicator is stranded on
            // the dead subscription while the replacement owner is already serving.
            Stream.retry(
              Schedule.spaced("1 second").pipe(
                Schedule.setInputType<ReplicaError.ReplicaError>(),
                Schedule.while(({ input }) => isTransientStatus(input))
              )
            ),
            Stream.interruptWhen(Deferred.await(pageHidden))
          )
      }),
      relayConnectionStatus: RelayConnectionStatus.RelayConnectionStatus.of({
        // Both refs, as `status` does and unlike `peerConnectionStatus`. This stream is long lived
        // and emits before a lease can lapse, which is the case the pending-mismatch ref exists for:
        // without it a post-emission mismatch fails the stream immediately instead of being carried
        // to the resubscribe that `ReplicaAtom` is waiting to perform.
        status: Stream.unwrap(
          Effect.gen(function*() {
            const emitted = yield* Ref.make(false)
            const pendingMismatch = yield* Ref.make<Option.Option<PendingStreamMismatch>>(Option.none())
            return withSessionStream(
              (session) => rpc.RelayConnectionStatus({ sessionId: session.sessionId }),
              reopen,
              emitted,
              pendingMismatch
            ).pipe(
              Stream.catchTag("RpcClientError", (error) =>
                Stream.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({ cause: error })
                  })
                )),
              // An owner engine reset fails every rpc port under the tab, which surfaces here as a
              // transient transport error. Without this the stream is terminal and the page's relay
              // indicator is stranded on whatever the dead engine last said, even though the
              // replacement owner is already serving.
              Stream.retry(
                Schedule.spaced("1 second").pipe(
                  Schedule.setInputType<ReplicaError.ReplicaError>(),
                  Schedule.while(({ input }) => isTransientStatus(input))
                )
              ),
              // The retry above would otherwise reopen a session the page has already closed.
              Stream.interruptWhen(Deferred.await(pageHidden))
            )
          })
        )
      }),
      transient: (peerId, documentId, payload) =>
        withSession(
          "Transient",
          (session) => rpc.Transient({ sessionId: session.sessionId, peerId, documentId, payload }),
          { replayAfterReopen: false }
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause: error })
              })
            ))
        ),
      transients: withSessionStream(
        (session) => rpc.Transients({ sessionId: session.sessionId })
      ).pipe(
        Stream.catchTag("RpcClientError", (error) =>
          Stream.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause: error })
            })
          )),
        Stream.retry(
          Schedule.spaced("1 second").pipe(
            Schedule.setInputType<ReplicaError.ReplicaError>(),
            Schedule.while(({ input }) => isTransient(input) || isActiveSessionMismatch(input))
          )
        ),
        Stream.interruptWhen(Deferred.await(pageHidden))
      ),
      invalidations,
      create: (document, createOptions) =>
        Wire.encode(document.schema, createOptions.value).pipe(
          Effect.flatMap((value) =>
            withSession("Create", (session) =>
              recoverCommand(
                createOptions.commandId,
                "Create",
                rpc.Create({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: createOptions.commandId,
                  value
                }),
                "LookupCreate",
                rpc.LookupCreate({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: createOptions.commandId
                })
              ), { boundOperation: false }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
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
      inspectConflicts: (document, documentId) =>
        withSession(
          "InspectConflicts",
          (session) =>
            rpc.InspectConflicts({
              sessionId: session.sessionId,
              document: document.name,
              documentId
            }).pipe(
              Effect.flatMap((encoded) =>
                Wire.decodeConflict(
                  Conflict.inspection(Schema.Json),
                  encoded,
                  session.lease.conflictLimits,
                  Conflict.preflightInspection
                )
              ),
              Effect.flatMap((inspection) =>
                Wire.decode(document.schema, inspection.snapshot.value).pipe(
                  Effect.map((value) => ({
                    ...inspection,
                    snapshot: { ...inspection.snapshot, value }
                  }))
                )
              )
            )
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
              })
            )),
          Effect.withSpan("ReplicaClient.inspectConflicts", {
            attributes: { "document.type": document.name }
          })
        ),
      resolveConflict: (document, conflictOptions) =>
        withSession(
          "ResolveConflict",
          (session) => {
            const outcomeSchema = CommandOutcome.schema(Schema.Void, Conflict.ResolutionError)
            return Wire.encodeConflict(
              Conflict.Resolution,
              conflictOptions.resolution,
              session.lease.conflictLimits,
              Conflict.preflightResolution
            ).pipe(
              Effect.flatMap((resolution) =>
                recoverCommand(
                  conflictOptions.commandId,
                  "ResolveConflict",
                  rpc.ResolveConflict({
                    sessionId: session.sessionId,
                    document: document.name,
                    commandId: conflictOptions.commandId,
                    documentId: conflictOptions.documentId,
                    resolution
                  }).pipe(
                    Effect.flatMap((encoded) =>
                      Wire.decodeConflict(outcomeSchema, encoded, session.lease.conflictLimits)
                    )
                  ),
                  "LookupConflictResolution",
                  rpc.LookupConflictResolution({
                    sessionId: session.sessionId,
                    document: document.name,
                    commandId: conflictOptions.commandId,
                    documentId: conflictOptions.documentId,
                    resolution
                  }).pipe(
                    Effect.flatMap((encoded) =>
                      Wire.decodeConflict(outcomeSchema, encoded, session.lease.conflictLimits)
                    )
                  )
                )
              ),
              Effect.flatMap(CommandOutcome.committedOrFail)
            )
          },
          { boundOperation: false }
        ).pipe(
          Effect.withSpan("ReplicaClient.resolveConflict", {
            attributes: {
              "document.type": document.name,
              "conflict.choice": conflictOptions.resolution.choice._tag,
              "conflict.path_depth": conflictOptions.resolution.path.parents.length + 1
            }
          })
        ),
      mutate: (mutation, mutationOptions) => {
        let payload: unknown
        if ("payload" in mutationOptions) payload = mutationOptions.payload
        else payload = undefined
        return Wire.encode(mutation.payloadSchema, payload).pipe(
          Effect.flatMap((encodedPayload) =>
            withSession("Mutate", (session) =>
              recoverCommand(
                mutationOptions.commandId,
                "Mutate",
                rpc.Mutate({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: mutationOptions.commandId,
                  documentId: mutationOptions.documentId,
                  payload: encodedPayload
                }),
                "LookupMutation",
                rpc.LookupMutation({
                  sessionId: session.sessionId,
                  mutation: mutation.name,
                  commandId: mutationOptions.commandId
                })
              ), { boundOperation: false })
          ),
          Effect.flatMap((outcome) => Wire.decodeOutcome(mutation.successSchema, mutation.errorSchema, outcome)),
          Effect.flatMap(CommandOutcome.committedOrFail)
        )
      },
      delete: (document, deleteOptions) =>
        withSession("Delete", (session) =>
          recoverCommand(
            deleteOptions.commandId,
            "Delete",
            rpc.Delete({ sessionId: session.sessionId, document: document.name, ...deleteOptions }),
            "LookupDelete",
            rpc.LookupDelete({
              sessionId: session.sessionId,
              document: document.name,
              commandId: deleteOptions.commandId
            })
          ), { boundOperation: false }).pipe(
            Effect.flatMap((outcome) => Wire.decodeOutcome(Schema.Void, Schema.Never, outcome)),
            Effect.flatMap(CommandOutcome.committedOrFail)
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
      lookupConflictResolution: (document, conflictOptions) =>
        withSession(
          "LookupConflictResolution",
          (session) => {
            const outcomeSchema = CommandOutcome.schema(Schema.Void, Conflict.ResolutionError)
            return Wire.encodeConflict(
              Conflict.Resolution,
              conflictOptions.resolution,
              session.lease.conflictLimits,
              Conflict.preflightResolution
            ).pipe(
              Effect.flatMap((resolution) =>
                rpc.LookupConflictResolution({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: conflictOptions.commandId,
                  documentId: conflictOptions.documentId,
                  resolution
                })
              ),
              Effect.flatMap((encoded) => Wire.decodeConflict(outcomeSchema, encoded, session.lease.conflictLimits))
            )
          }
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: error
                })
              })
            )),
          Effect.withSpan("ReplicaClient.lookupConflictResolution", {
            attributes: {
              "document.type": document.name,
              "conflict.choice": conflictOptions.resolution.choice._tag,
              "conflict.path_depth": conflictOptions.resolution.path.parents.length + 1
            }
          })
        ),
      lookupCommandDelivery: (commandId) =>
        withSession(
          "LookupCommandDelivery",
          (session) => rpc.LookupCommandDelivery({ sessionId: session.sessionId, commandId })
        ).pipe(
          Effect.catchTag("RpcClientError", (error) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause: error })
              })
            ))
        ),
      commandDeliveryChanges: (commandId) =>
        withSessionStream(
          (session) => rpc.CommandDeliveryChanges({ sessionId: session.sessionId, commandId })
        ).pipe(
          Stream.catchTag("RpcClientError", (error) =>
            Stream.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause: error })
              })
            )),
          Stream.retry(
            Schedule.spaced("1 second").pipe(
              Schedule.setInputType<ReplicaError.ReplicaError>(),
              Schedule.while(({ input }) => isTransient(input) || isActiveSessionMismatch(input))
            )
          ),
          Stream.changes,
          // The retry above would otherwise reopen a session the page has already closed.
          Stream.interruptWhen(Deferred.await(pageHidden))
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
      restoreBackup: <RestoreRequirements,>(restoreOptions: ArchiveTransferOptions<RestoreRequirements>) =>
        Effect.gen(function*() {
          const startedAt = yield* Clock.currentTimeMillis
          const deadline = Math.min(
            Number.MAX_SAFE_INTEGER,
            startedAt + operationReportedTimeoutMillis
          )
          const maxBytes = yield* Backup.validateMaxBytes(restoreOptions.maxBytes)
          let acceptedTerminal:
            | Exit.Exit<void, ReplicaError.ReplicaError | RestoreBackupError>
            | undefined
          let acceptedReplacement:
            | Fiber.Fiber<Session, ReplicaError.ReplicaError>
            | undefined
          const timeoutError = () =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.OperationTimeout({
                operation: "RestoreBackup",
                timeoutMillis: operationReportedTimeoutMillis
              })
            })
          const withinDeadline = <DeadlineError, DeadlineRequirements,>(
            operation: Effect.Effect<void, DeadlineError, DeadlineRequirements>
          ): Effect.Effect<
            void,
            DeadlineError | ReplicaError.ReplicaError | RestoreBackupError,
            DeadlineRequirements
          > =>
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) =>
                Effect.timeoutOrElse(operation, {
                  duration: Math.max(0, deadline - now),
                  orElse: () =>
                    acceptedTerminal ??
                      Effect.fail(timeoutError())
                })
              )
            )
          const reopenWithinDeadline = (session: Session) =>
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) => {
                const remaining = Math.max(0, deadline - now)
                const replacement = acceptedReplacement
                if (replacement !== undefined) {
                  const completed = replacement.pollUnsafe()
                  if (completed !== undefined) {
                    if (
                      Exit.isFailure(completed) &&
                      completed.cause.reasons.every(Cause.isInterruptReason)
                    ) {
                      return Effect.fail(timeoutError())
                    }
                    return completed
                  }
                  if (remaining === 0) return Effect.fail(timeoutError())
                  return Effect.timeoutOrElse(Fiber.join(replacement), {
                    duration: remaining,
                    orElse: () => Effect.fail(timeoutError())
                  })
                }
                if (remaining === 0) return Effect.fail(timeoutError())
                return Effect.timeoutOrElse(reopen(session), {
                  duration: remaining,
                  orElse: () => Effect.fail(timeoutError())
                })
              })
            )
          const operation = withSession(
            "RestoreBackup",
            (session) =>
              (() => {
                if (restoreOptions.mode === "document") {
                  return rpc.BeginRestoreBackup({
                    sessionId: session.sessionId,
                    mode: restoreOptions.mode,
                    document: restoreOptions.document.name,
                    documentId: restoreOptions.documentId,
                    maxBytes,
                    expectedDefinitionHash: restoreOptions.expectedDefinitionHash,
                    installationId: restoreOptions.installationId
                  })
                }
                return rpc.BeginRestoreBackup({
                  sessionId: session.sessionId,
                  mode: restoreOptions.mode,
                  maxBytes,
                  expectedDefinitionHash: restoreOptions.expectedDefinitionHash,
                  installationId: restoreOptions.installationId
                })
              })().pipe(
                Effect.catchCause((cause) => {
                  if (
                    cause.reasons.length === 1 &&
                    Cause.isDieReason(cause.reasons[0]) &&
                    isDataCloneError(cause.reasons[0].defect)
                  ) {
                    return Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.StorageUnavailable({ cause: cause.reasons[0].defect })
                      })
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
                Effect.flatMap(({ nonce: rpcNonce, port: rpcPort }) =>
                  Effect.acquireUseRelease(
                    Effect.sync(() => {
                      const nativeClose = rpcPort.close.bind(rpcPort)
                      let open = true
                      return {
                        nonce: rpcNonce,
                        port: rpcPort,
                        close: () => {
                          if (!open) return
                          open = false
                          runSyncSafe(nativeClose, undefined)
                        }
                      }
                    }),
                    ({ close, nonce: finishNonce, port: finishPort }) =>
                      Effect.gen(function*() {
                        const result = rpc.FinishRestoreBackup({
                          sessionId: session.sessionId,
                          nonce: finishNonce
                        }).pipe(
                          Effect.catchCause((cause) =>
                            Effect.failCause(
                              Cause.map(cause, (error) => {
                                if (error._tag === "RestoreResultSessionFailure") {
                                  return RestoreProtocol.replicaErrorFromWire(error.error)
                                }
                                if (error._tag === "RestoreResultRestoreFailure") {
                                  return new RestoreBackupError({
                                    error: RestoreProtocol.replicaErrorFromWire(error.error)
                                  })
                                }
                                return new RestoreBackupError({
                                  error: new ReplicaError.ReplicaError({
                                    reason: new ReplicaError.StorageUnavailable({ cause: error })
                                  })
                                })
                              })
                            )
                          )
                        )
                        const resultFiber = yield* result.pipe(
                          Effect.forkChild({ startImmediately: true })
                        )
                        yield* serveRestoreSource(
                          restoreOptions.source,
                          finishPort,
                          finishNonce,
                          maxBytes,
                          session.lease.maxChunkBytes,
                          session.lease.maxRestoreCoalesceMillis,
                          session.lease.maxRestoreErrorBytes,
                          close,
                          Fiber.join(resultFiber),
                          (exit) =>
                            Effect.gen(function*() {
                              acceptedTerminal = exit
                              if (
                                acceptedReplacement === undefined &&
                                acceptedProtocolMismatch(exit) !== undefined
                              ) {
                                acceptedReplacement = yield* reopen(session).pipe(
                                  Effect.forkChild
                                )
                              }
                            })
                        )
                      }),
                    ({ close }) => Effect.sync(close)
                  )
                ),
                Effect.withSpan("ReplicaClient.restoreBackup", {
                  attributes: {
                    "restore.mode": restoreOptions.mode,
                    "restore.max_bytes": maxBytes,
                    "restore.max_chunk_bytes": session.lease.maxChunkBytes,
                    "restore.max_coalesce_millis": session.lease.maxRestoreCoalesceMillis,
                    "restore.max_error_bytes": session.lease.maxRestoreErrorBytes
                  }
                }),
                withinDeadline
              ),
            {
              boundOperation: false,
              replayAfterReopen: false,
              replaceSession: reopenWithinDeadline
            }
          )
          return yield* operation
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(
                cause,
                (error) => {
                  if (error._tag === "RestoreBackupError") return error.error
                  return error
                }
              )
            )
          )
        ),
      installBackupDocument: (document, installOptions) =>
        client.restoreBackup({
          ...installOptions,
          mode: "document",
          document
        }),
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
      importDocument: (document, importOptions) =>
        Wire.encode(Schema.toEncoded(document.schema), importOptions.value.value).pipe(
          Effect.flatMap((value) =>
            withSession(
              "ImportDocument",
              (session) =>
                rpc.ImportDocument({
                  sessionId: session.sessionId,
                  document: document.name,
                  commandId: importOptions.commandId,
                  value: { ...importOptions.value, value }
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
                )),
              Effect.flatMap(CommandOutcome.committedOrFail)
            )
          )
        )
    }
    return client
  })

export const layer = (definition: ReplicaDefinition.Any, options?: Options) =>
  Layer.effectContext(
    RpcClient.make(ReplicaRpc.group).pipe(
      Effect.flatMap((rpc) => fromRpcClient(definition, rpc, options)),
      Effect.map((client) =>
        Context.make(ReplicaClient, client).pipe(
          Context.add(Transient.Transport, {
            send: client.transient,
            messages: client.transients
          })
        )
      )
    )
  )
