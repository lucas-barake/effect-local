import type * as Backup from "@lucas-barake/effect-local/Backup"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SessionManager from "../SessionManager.js"
import * as RestoreProtocol from "./restoreProtocol.js"

export interface BeginOptions extends Omit<Backup.RestoreOptions<never>, "source"> {
  readonly sessionId: Identity.SessionId
  readonly clientId: number
}

export interface BeginResult {
  readonly nonce: RestoreProtocol.RestoreNonce
  readonly port: MessagePort
}

export interface FinishOptions {
  readonly sessionId: Identity.SessionId
  readonly clientId: number
  readonly nonce: RestoreProtocol.RestoreNonce
}

export class RestoreTransport extends Context.Service<RestoreTransport, {
  readonly begin: (options: BeginOptions) => Effect.Effect<BeginResult, ReplicaError.ReplicaError>
  readonly finish: (
    options: FinishOptions
  ) => Effect.Effect<Deferred.Deferred<void, RestoreProtocol.RestoreResultFailure>>
  readonly activeControllerCount: Effect.Effect<number>
}>()("@lucas-barake/effect-local-browser/RestoreTransport") {}

class SessionFailure extends Schema.TaggedErrorClass<SessionFailure>(
  "@lucas-barake/effect-local-browser/RestoreTransport/SessionFailure"
)("SessionFailure", {
  error: ReplicaError.ReplicaError
}) {}

class RestoreFailure extends Schema.TaggedErrorClass<RestoreFailure>(
  "@lucas-barake/effect-local-browser/RestoreTransport/RestoreFailure"
)("RestoreFailure", {
  error: ReplicaError.ReplicaError
}) {}

type WorkFailure = SessionFailure | RestoreFailure

type AwaitingFrame = {
  readonly _tag: "Start"
  readonly sequence: 0
  readonly deferred: Deferred.Deferred<RestoreProtocol.Start, ReplicaError.ReplicaError>
} | {
  readonly _tag: "Source"
  readonly sequence: number
  readonly deferred: Deferred.Deferred<
    RestoreProtocol.Chunk | RestoreProtocol.End | RestoreProtocol.SourceFailure,
    ReplicaError.ReplicaError
  >
} | {
  readonly _tag: "TerminalAck"
  readonly sequence: number
  readonly deferred: Deferred.Deferred<RestoreProtocol.TerminalAck, ReplicaError.ReplicaError>
} | {
  readonly _tag: "ReleasedAck"
  readonly sequence: number
  readonly deferred: Deferred.Deferred<RestoreProtocol.ReleasedAck, ReplicaError.ReplicaError>
}

type ControllerPhase = "SettingUp" | "Running" | "Finishing" | "ShutdownRequested" | "Closed"

interface Controller {
  readonly nonce: RestoreProtocol.RestoreNonce
  readonly options: BeginOptions
  readonly lease: SessionManager.RestoreLease
  readonly operationScope: Scope.Closeable
  readonly setupReady: Deferred.Deferred<void>
  readonly shutdown: Deferred.Deferred<ReplicaError.ReplicaError>
  readonly cleanupComplete: Deferred.Deferred<void>
  readonly start: Deferred.Deferred<RestoreProtocol.Start, ReplicaError.ReplicaError>
  readonly resultClaimed: Deferred.Deferred<void>
  result: Deferred.Deferred<void, RestoreProtocol.RestoreResultFailure> | undefined
  shutdownReason: ReplicaError.ReplicaError | undefined
  phase: ControllerPhase
  port: MessagePort | undefined
  peerPort: MessagePort | undefined
  awaiting: AwaitingFrame | undefined
  lastSequence: number
  cumulativeBytes: number
  listenersInstalled: boolean
  ignoredOutstandingSequence: number | undefined
  messageListener: ((event: MessageEvent<unknown>) => void) | undefined
  messageErrorListener: ((event: MessageEvent<unknown>) => void) | undefined
  closeListener: (() => void) | undefined
}

const protocolFailure = () =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({
      expected: "valid restore transport frame",
      observed: "invalid restore transport frame"
    })
  })

const unavailable = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({ cause })
  })

const timeout = (operation: string, timeoutMillis: number) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.OperationTimeout({ operation, timeoutMillis })
  })

const ownExactProperties = (
  input: object,
  fields: ReadonlySet<string>
): ReadonlyMap<string, unknown> | undefined => {
  const properties = new Map<string, unknown>()
  for (const key in input) {
    if (!Object.prototype.hasOwnProperty.call(input, key)) continue
    if (properties.size >= fields.size || !fields.has(key)) return undefined
    const descriptor = Object.getOwnPropertyDescriptor(input, key)
    if (descriptor === undefined || !("value" in descriptor)) return undefined
    properties.set(key, descriptor.value)
  }
  return properties.size === fields.size ? properties : undefined
}

const preflightFrame = (
  input: unknown,
  maxErrorBytes: number,
  maxChunkBytes: number
): boolean => {
  try {
    if (typeof input !== "object" || input === null || Array.isArray(input)) return false
    const tagDescriptor = Object.getOwnPropertyDescriptor(input, "_tag")
    if (tagDescriptor === undefined || !("value" in tagDescriptor)) return false
    const tag = tagDescriptor.value
    if (typeof tag !== "string") return false
    const allowedFields = RestoreProtocol.pageToOwnerFrameFields[tag]
    if (allowedFields === undefined) return false
    const properties = ownExactProperties(input, allowedFields)
    if (properties === undefined) return false
    if (tag === "Chunk") {
      const bytes = properties.get("bytes")
      if (
        !(bytes instanceof Uint8Array) ||
        bytes.byteLength === 0 ||
        bytes.byteLength > maxChunkBytes ||
        !(bytes.buffer instanceof ArrayBuffer) ||
        bytes.byteOffset !== 0 ||
        bytes.buffer.byteLength !== bytes.byteLength
      ) {
        return false
      }
    }
    if (tag === "SourceFailure") {
      return RestoreProtocol.preflightReplicaError(properties.get("error"), maxErrorBytes)
    }
    return true
  } catch {
    return false
  }
}

const encodeFrame = <A, I, R,>(
  schema: Schema.Codec<A, I, R>,
  frame: A
): Effect.Effect<I, ReplicaError.ReplicaError, R> =>
  Schema.encodeUnknownEffect(schema)(frame).pipe(
    Effect.mapError(() => protocolFailure())
  )

const decodePageFrame = (
  value: unknown,
  maxStringBytes: number,
  maxChunkBytes: number
): Exit.Exit<RestoreProtocol.PageToOwnerFrame, unknown> => {
  if (!preflightFrame(value, maxStringBytes, maxChunkBytes)) return Exit.fail(protocolFailure())
  return Schema.decodeUnknownExit(
    RestoreProtocol.PageToOwnerFrame,
    { onExcessProperty: "error" }
  )(value)
}

const post = <A, I, R,>(
  controller: Controller,
  schema: Schema.Codec<A, I, R>,
  frame: A,
  transfer?: ReadonlyArray<Transferable>
): Effect.Effect<void, ReplicaError.ReplicaError, R> =>
  encodeFrame(schema, frame).pipe(
    Effect.flatMap((encoded) =>
      Effect.try({
        try: () => {
          if (controller.port === undefined) throw new Error("restore transport endpoint is unavailable")
          controller.port.postMessage(encoded, transfer === undefined ? [] : [...transfer])
        },
        catch: unavailable
      })
    )
  )

const boundWait = <A, E, R,>(
  effect: Effect.Effect<A, E, R>,
  operation: string,
  timeoutMillis: number
): Effect.Effect<A, E | ReplicaError.ReplicaError, R> =>
  Effect.timeoutOrElse(effect, {
    duration: timeoutMillis,
    orElse: () => Effect.fail(timeout(operation, timeoutMillis))
  })

const makeLayer = Layer.effect(
  RestoreTransport,
  Effect.acquireRelease(
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      const sessions = yield* SessionManager.SessionManager
      const replica = yield* Replica.Replica
      const supervisorScope = yield* Scope.make("parallel")
      const finishingCapacity = sessions.effectiveRestoreCapacity
      const finishing = yield* Semaphore.make(finishingCapacity)
      const startingZero = yield* Latch.make(true)

      let closed = false
      let starting = 0
      const controllers = new Map<RestoreProtocol.RestoreNonce, Controller>()

      const enterStarting = Effect.sync(() => {
        if (closed) return false
        if (starting === 0) startingZero.closeUnsafe()
        starting += 1
        return true
      }).pipe(
        Effect.filterOrFail(
          (entered) => entered,
          () => unavailable(new Error("restore transport is closed"))
        )
      )

      const leaveStarting = (owned: { value: boolean }) =>
        Effect.sync(() => {
          if (!owned.value) return
          owned.value = false
          starting -= 1
          if (starting === 0) startingZero.openUnsafe()
        })

      const stopIngress = (controller: Controller) => {
        const port = controller.port
        if (port === undefined) return
        controller.port = undefined
        if (controller.listenersInstalled) {
          if (controller.messageListener !== undefined) {
            port.removeEventListener("message", controller.messageListener)
          }
          if (controller.messageErrorListener !== undefined) {
            port.removeEventListener("messageerror", controller.messageErrorListener)
          }
          if (controller.closeListener !== undefined) {
            port.removeEventListener("close", controller.closeListener)
          }
          controller.listenersInstalled = false
        }
        port.close()
      }

      const failAwaiting = (awaiting: AwaitingFrame, error: ReplicaError.ReplicaError) => {
        switch (awaiting._tag) {
          case "Start":
            Deferred.doneUnsafe(awaiting.deferred, Effect.fail(error))
            break
          case "Source":
            Deferred.doneUnsafe(awaiting.deferred, Effect.fail(error))
            break
          case "TerminalAck":
            Deferred.doneUnsafe(awaiting.deferred, Effect.fail(error))
            break
          case "ReleasedAck":
            Deferred.doneUnsafe(awaiting.deferred, Effect.fail(error))
        }
      }

      const beginShutdown = (controller: Controller, error: ReplicaError.ReplicaError) => {
        if (controller.phase === "Closed" || controller.phase === "Finishing") return
        controller.phase = "ShutdownRequested"
        controller.shutdownReason = error
        stopIngress(controller)
        Deferred.doneUnsafe(controller.shutdown, Effect.succeed(error))
        if (controller.awaiting !== undefined) {
          failAwaiting(controller.awaiting, error)
        }
      }

      const invalidFrame = (controller: Controller) => {
        const error = protocolFailure()
        if (controller.phase === "Finishing") {
          stopIngress(controller)
          if (controller.awaiting !== undefined) failAwaiting(controller.awaiting, error)
          return
        }
        beginShutdown(controller, error)
      }

      const disconnect = (controller: Controller, error: ReplicaError.ReplicaError) => {
        if (controller.phase === "Finishing") {
          controller.shutdownReason = error
          stopIngress(controller)
          if (controller.awaiting !== undefined) failAwaiting(controller.awaiting, error)
          return
        }
        beginShutdown(controller, error)
      }

      const acceptFrame = (controller: Controller, frame: RestoreProtocol.PageToOwnerFrame) => {
        if (frame.nonce !== controller.nonce) {
          invalidFrame(controller)
          return
        }
        const awaiting = controller.awaiting
        if (
          controller.phase === "Finishing" &&
          controller.ignoredOutstandingSequence === frame.sequence &&
          (frame._tag === "Chunk" || frame._tag === "End" || frame._tag === "SourceFailure")
        ) {
          controller.ignoredOutstandingSequence = undefined
          return
        }
        if (awaiting === undefined || awaiting.sequence !== frame.sequence) {
          invalidFrame(controller)
          return
        }
        if (awaiting._tag === "Start" && frame._tag === "Start") {
          controller.awaiting = undefined
          controller.lastSequence = frame.sequence
          if (!Deferred.doneUnsafe(awaiting.deferred, Effect.succeed(frame))) invalidFrame(controller)
          return
        }
        if (
          awaiting._tag === "Source" &&
          (frame._tag === "Chunk" || frame._tag === "End" || frame._tag === "SourceFailure")
        ) {
          if (frame._tag === "Chunk") {
            const bytes = frame.bytes
            if (
              bytes.byteLength === 0 ||
              bytes.byteLength > sessions.maxChunkBytes ||
              !(bytes.buffer instanceof ArrayBuffer) ||
              bytes.byteOffset !== 0 ||
              bytes.buffer.byteLength !== bytes.byteLength ||
              controller.cumulativeBytes + bytes.byteLength > controller.options.maxBytes
            ) {
              invalidFrame(controller)
              return
            }
            controller.cumulativeBytes += bytes.byteLength
          }
          controller.awaiting = undefined
          controller.lastSequence = frame.sequence
          if (!Deferred.doneUnsafe(awaiting.deferred, Effect.succeed(frame))) invalidFrame(controller)
          return
        }
        if (awaiting._tag === "TerminalAck" && frame._tag === "TerminalAck") {
          controller.awaiting = undefined
          if (!Deferred.doneUnsafe(awaiting.deferred, Effect.succeed(frame))) invalidFrame(controller)
          return
        }
        if (awaiting._tag === "ReleasedAck" && frame._tag === "ReleasedAck") {
          controller.awaiting = undefined
          if (!Deferred.doneUnsafe(awaiting.deferred, Effect.succeed(frame))) invalidFrame(controller)
          return
        }
        invalidFrame(controller)
      }

      const installIngress = (controller: Controller, port: MessagePort) => {
        const onMessage = (event: MessageEvent<unknown>) => {
          let decoded: Exit.Exit<RestoreProtocol.PageToOwnerFrame, unknown>
          try {
            decoded = decodePageFrame(
              event.data,
              sessions.maxRestoreErrorBytes,
              sessions.maxChunkBytes
            )
          } catch {
            invalidFrame(controller)
            return
          }
          if (Exit.isFailure(decoded)) {
            invalidFrame(controller)
            return
          }
          acceptFrame(controller, decoded.value)
        }
        const onMessageError = () =>
          disconnect(
            controller,
            unavailable(new Error("restore transport message decoding failed"))
          )
        const onClose = () =>
          disconnect(
            controller,
            unavailable(new Error("restore transport peer closed"))
          )
        controller.messageListener = onMessage
        controller.messageErrorListener = onMessageError
        controller.closeListener = onClose
        port.addEventListener("message", onMessage)
        port.addEventListener("messageerror", onMessageError)
        port.addEventListener("close", onClose)
        controller.listenersInstalled = true
        port.start()
      }

      const awaitStart = (controller: Controller) =>
        boundWait(
          Deferred.await(controller.start),
          "RestoreBackupStart",
          sessions.maxRestorePullMillis
        ).pipe(
          Effect.tapError((error) => Effect.sync(() => beginShutdown(controller, error)))
        )

      const awaitResultClaim = (controller: Controller) =>
        boundWait(
          Deferred.await(controller.resultClaimed),
          "RestoreBackupResultClaim",
          sessions.maxRestorePullMillis
        ).pipe(
          Effect.tapError((error) => Effect.sync(() => beginShutdown(controller, error)))
        )

      const source = (controller: Controller) =>
        Stream.unfold(1, (sequence) =>
          Effect.gen(function*() {
            const deferred = yield* Deferred.make<
              RestoreProtocol.Chunk | RestoreProtocol.End | RestoreProtocol.SourceFailure,
              ReplicaError.ReplicaError
            >()
            controller.awaiting = { _tag: "Source", sequence, deferred }
            controller.ignoredOutstandingSequence = sequence
            controller.lastSequence = sequence
            yield* post(controller, RestoreProtocol.Pull, {
              _tag: "Pull",
              nonce: controller.nonce,
              sequence
            }).pipe(
              Effect.tapError((error) => Effect.sync(() => beginShutdown(controller, error)))
            )
            const frame = yield* boundWait(
              Deferred.await(deferred),
              "RestoreBackupPull",
              sessions.maxRestorePullMillis
            ).pipe(
              Effect.tapError((error) => Effect.sync(() => beginShutdown(controller, error)))
            )
            controller.ignoredOutstandingSequence = undefined
            if (frame._tag === "End") return undefined
            if (frame._tag === "SourceFailure") {
              return yield* Effect.fail(RestoreProtocol.replicaErrorFromWire(frame.error))
            }
            return [frame.bytes, sequence + 1] as const
          }))

      const work = (controller: Controller): Effect.Effect<void, WorkFailure> =>
        Effect.gen(function*() {
          const restore = Effect.gen(function*() {
            yield* awaitStart(controller)
            yield* awaitResultClaim(controller)
            controller.phase = "Running"
            return yield* replica.restoreBackup({
              source: source(controller),
              mode: controller.options.mode,
              maxBytes: controller.options.maxBytes,
              expectedDefinitionHash: controller.options.expectedDefinitionHash,
              installationId: controller.options.installationId
            })
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.failCause(Cause.map(
                cause,
                (error): WorkFailure => new RestoreFailure({ error })
              ))
            )
          )
          const expired = Deferred.await(controller.lease.expired).pipe(
            Effect.catchCause((cause) =>
              Effect.failCause(Cause.map(
                cause,
                (error): WorkFailure => new SessionFailure({ error })
              ))
            )
          )
          return yield* Effect.raceFirst(restore, expired)
        })

      const sendTerminalAndAwaitAck = (
        controller: Controller
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          controller.phase = "Finishing"
          const sequence = controller.lastSequence + 1
          const ack = yield* Deferred.make<RestoreProtocol.TerminalAck, ReplicaError.ReplicaError>()
          controller.awaiting = { _tag: "TerminalAck", sequence, deferred: ack }
          yield* post(controller, RestoreProtocol.TerminalReady, {
            _tag: "TerminalReady",
            nonce: controller.nonce,
            sequence
          })
          yield* Deferred.await(ack)
          controller.lastSequence = sequence
        })

      const sendReleasedAndAwaitAck = (
        controller: Controller
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          const sequence = controller.lastSequence + 1
          yield* controller.lease.release
          const releasedAck = yield* Deferred.make<RestoreProtocol.ReleasedAck, ReplicaError.ReplicaError>()
          controller.awaiting = { _tag: "ReleasedAck", sequence, deferred: releasedAck }
          yield* post(controller, RestoreProtocol.Released, {
            _tag: "Released",
            nonce: controller.nonce,
            sequence
          })
          yield* boundWait(
            Deferred.await(releasedAck),
            "RestoreBackupReleasedAck",
            sessions.maxRestorePullMillis
          )
        })

      const cleanup = (controller: Controller, exit: Exit.Exit<unknown, unknown>) =>
        Effect.uninterruptible(
          Effect.gen(function*() {
            controller.phase = "Closed"
            controller.awaiting = undefined
            stopIngress(controller)
            controller.peerPort = undefined
            const result = controller.result
            if (result !== undefined && !(yield* Deferred.isDone(result))) {
              const error = controller.shutdownReason ??
                unavailable(new Error("restore operation ended before producing a result"))
              yield* Deferred.fail(
                result,
                new RestoreProtocol.RestoreResultRestoreFailure({
                  error: RestoreProtocol.encodeReplicaError(
                    error,
                    sessions.maxRestoreErrorBytes
                  )
                })
              )
            }
            controller.result = undefined
            yield* Scope.close(controller.operationScope, exit)
            if (controllers.get(controller.nonce) === controller) {
              controllers.delete(controller.nonce)
            }
            yield* Deferred.succeed(controller.cleanupComplete, undefined)
          })
        )

      const supervise = (controller: Controller) =>
        Effect.gen(function*() {
          const setup = yield* Effect.raceFirst(
            Deferred.await(controller.setupReady).pipe(Effect.as("Ready" as const)),
            Deferred.await(controller.shutdown).pipe(Effect.as("Shutdown" as const))
          )
          if (setup === "Shutdown") return
          const worker = yield* work(controller).pipe(
            Effect.forkIn(controller.operationScope, { startImmediately: true })
          )
          const outcome = yield* Effect.raceAllFirst([
            Fiber.await(worker).pipe(Effect.map((exit) => ({ _tag: "Worker" as const, exit }))),
            Deferred.await(controller.shutdown).pipe(
              Effect.map(() => ({ _tag: "Shutdown" as const }))
            ),
            Effect.sleep(sessions.maxRestoreMillis).pipe(
              Effect.tap(() =>
                Effect.sync(() =>
                  beginShutdown(
                    controller,
                    timeout("RestoreBackup", sessions.maxRestoreMillis)
                  )
                )
              ),
              Effect.as({ _tag: "Deadline" as const })
            )
          ])
          if (outcome._tag !== "Worker") {
            yield* Fiber.interrupt(worker)
            return
          }
          if (controller.phase === "ShutdownRequested") return
          const result = controller.result
          if (result === undefined) return
          const resultExit = Exit.isSuccess(outcome.exit)
            ? Exit.void
            : Exit.failCause(
              Cause.map(outcome.exit.cause, (failure): RestoreProtocol.RestoreResultFailure =>
                failure._tag === "SessionFailure"
                  ? new RestoreProtocol.RestoreResultSessionFailure({
                    error: RestoreProtocol.encodeReplicaError(
                      failure.error,
                      sessions.maxRestoreErrorBytes
                    )
                  })
                  : new RestoreProtocol.RestoreResultRestoreFailure({
                    error: RestoreProtocol.encodeReplicaError(
                      failure.error,
                      sessions.maxRestoreErrorBytes
                    )
                  }))
            )
          yield* Deferred.done(result, resultExit)
          controller.phase = "Finishing"
          controller.ignoredOutstandingSequence = controller.awaiting?._tag === "Source"
            ? controller.awaiting.sequence
            : undefined
          const acquireFinishing = Effect.acquireRelease(
            finishing.take(1),
            () =>
              finishing.release(1),
            { interruptible: true }
          ).pipe(
            Effect.provideService(Scope.Scope, controller.operationScope),
            Effect.asVoid
          )
          yield* boundWait(
            acquireFinishing,
            "RestoreBackupFinishingCapacity",
            sessions.maxRestorePullMillis
          )
          yield* boundWait(
            sendTerminalAndAwaitAck(controller),
            "RestoreBackupTerminalAck",
            sessions.maxRestorePullMillis
          )
          yield* sendReleasedAndAwaitAck(controller)
        }).pipe(
          Effect.onExit((exit) =>
            cleanup(controller, exit)
          )
        )

      const begin: RestoreTransport["Service"]["begin"] = (options) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            const startingOwned = { value: true }
            let lease: SessionManager.RestoreLease | undefined
            let operationScope: Scope.Closeable | undefined
            let controller: Controller | undefined
            let supervisorLive = false
            yield* enterStarting
            const attempt = Effect.gen(function*() {
              lease = yield* sessions.acquireRestore(options.sessionId, options.clientId)
              const uuid = yield* crypto.randomUUIDv4.pipe(
                Effect.mapError((cause) => unavailable(cause))
              )
              const nonce = RestoreProtocol.RestoreNonce.make(`rst_${uuid}`)
              operationScope = yield* Scope.make("sequential")
              yield* Scope.addFinalizer(operationScope, lease.release)
              const start = yield* Deferred.make<
                RestoreProtocol.Start,
                ReplicaError.ReplicaError
              >()
              controller = {
                nonce,
                options,
                lease,
                operationScope,
                setupReady: yield* Deferred.make<void>(),
                shutdown: yield* Deferred.make<ReplicaError.ReplicaError>(),
                cleanupComplete: yield* Deferred.make<void>(),
                start,
                resultClaimed: yield* Deferred.make<void>(),
                result: yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>(),
                shutdownReason: undefined,
                phase: "SettingUp",
                port: undefined,
                peerPort: undefined,
                awaiting: { _tag: "Start", sequence: 0, deferred: start },
                lastSequence: 0,
                cumulativeBytes: 0,
                listenersInstalled: false,
                ignoredOutstandingSequence: undefined,
                messageListener: undefined,
                messageErrorListener: undefined,
                closeListener: undefined
              }
              yield* supervise(controller).pipe(
                Effect.forkIn(supervisorScope, { startImmediately: true })
              )
              supervisorLive = true
              const accepted = yield* Effect.sync(() => {
                if (closed || controllers.has(controller!.nonce)) return false
                controllers.set(controller!.nonce, controller!)
                return true
              })
              if (!accepted) {
                beginShutdown(controller, unavailable(new Error("restore transport is closed")))
                yield* Deferred.await(controller.cleanupComplete)
                return yield* unavailable(new Error("restore transport is closed"))
              }
              const channel = yield* Effect.sync(() => new MessageChannel())
              if (
                closed ||
                controller.phase !== "SettingUp" ||
                controllers.get(controller.nonce) !== controller
              ) {
                channel.port1.close()
                channel.port2.close()
                beginShutdown(controller, unavailable(new Error("restore transport is closed")))
                yield* Deferred.await(controller.cleanupComplete)
                return yield* unavailable(new Error("restore transport is closed"))
              }
              controller.port = channel.port1
              controller.peerPort = channel.port2
              installIngress(controller, channel.port1)
              yield* Deferred.succeed(controller.setupReady, undefined)
              yield* leaveStarting(startingOwned)
              return yield* restore(
                Effect.succeed({ nonce, port: channel.port2 }).pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => beginShutdown(controller!, unavailable(new Error("restore begin interrupted"))))
                      .pipe(Effect.andThen(Deferred.await(controller!.cleanupComplete)))
                  )
                )
              )
            })
            return yield* attempt.pipe(
              Effect.onExit((exit) =>
                Effect.gen(function*() {
                  if (Exit.isSuccess(exit)) return
                  if (controller !== undefined && supervisorLive) {
                    const peerPort = controller.peerPort
                    controller.peerPort = undefined
                    if (peerPort !== undefined) peerPort.close()
                    beginShutdown(controller, unavailable(Cause.squash(exit.cause)))
                    yield* Deferred.await(controller.cleanupComplete)
                  } else if (operationScope !== undefined) {
                    yield* Scope.close(operationScope, exit)
                  } else if (lease !== undefined) {
                    yield* lease.release
                  }
                  yield* leaveStarting(startingOwned)
                })
              )
            )
          })
        )

      const finish: RestoreTransport["Service"]["finish"] = (options) =>
        Effect.gen(function*() {
          const claimed = yield* Effect.sync(() => {
            const controller = controllers.get(options.nonce)
            if (
              controller === undefined ||
              controller.options.sessionId !== options.sessionId ||
              controller.options.clientId !== options.clientId ||
              controller.phase === "Closed" ||
              controller.result === undefined
            ) {
              return undefined
            }
            const result = controller.result
            if (!Deferred.doneUnsafe(controller.resultClaimed, Effect.void)) return undefined
            return { controller, result }
          })
          if (claimed === undefined) {
            const result = yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>()
            const validSession = yield* sessions.contains(options.sessionId)
            const maxErrorBytes = validSession
              ? sessions.maxRestoreErrorBytes
              : ReplicaLimits.minimumRestoreErrorBytes
            yield* Deferred.fail(
              result,
              new RestoreProtocol.RestoreResultSessionFailure({
                error: RestoreProtocol.encodeReplicaError(
                  protocolFailure(),
                  maxErrorBytes
                )
              })
            )
            return result
          }
          const rpcResult = yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>()
          yield* Deferred.completeWith(
            rpcResult,
            Deferred.await(claimed.result).pipe(
              Effect.exit,
              Effect.onInterrupt(() =>
                Effect.sync(() =>
                  beginShutdown(
                    claimed.controller,
                    unavailable(new Error("restore result consumer interrupted"))
                  )
                )
              ),
              Effect.flatMap((exit) => exit)
            )
          )
          return rpcResult
        })

      const shutdown = Effect.uninterruptible(
        Effect.gen(function*() {
          const snapshot = yield* Effect.sync(() => {
            closed = true
            const snapshot = Array.from(controllers.values())
            for (const controller of snapshot) {
              beginShutdown(controller, unavailable(new Error("restore transport is shutting down")))
            }
            return snapshot
          })
          yield* startingZero.await
          yield* Effect.forEach(
            snapshot,
            (controller) => Deferred.await(controller.cleanupComplete),
            { concurrency: snapshot.length === 0 ? 1 : snapshot.length, discard: true }
          )
          yield* Scope.close(supervisorScope, Exit.void)
        })
      )

      return {
        service: {
          begin,
          finish,
          activeControllerCount: Effect.sync(() => controllers.size)
        } satisfies RestoreTransport["Service"],
        shutdown
      }
    }),
    ({ shutdown }) => shutdown
  ).pipe(Effect.map(({ service }) => service))
)

export const layer = makeLayer

export const freshLayer = Layer.fresh(layer)
