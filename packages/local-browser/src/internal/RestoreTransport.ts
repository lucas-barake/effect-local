import type * as Backup from "@lucas-barake/effect-local/Backup"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
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

export class RestoreTransport extends Context.Service<RestoreTransport, {
  readonly begin: (options: BeginOptions) => Effect.Effect<BeginResult, ReplicaError.ReplicaError>
  readonly activeControllerCount: Effect.Effect<number>
}>()("@lucas-barake/effect-local-browser/RestoreTransport") {}

type WorkFailure =
  | { readonly _tag: "SessionFailure"; readonly error: ReplicaError.ReplicaError }
  | { readonly _tag: "RestoreFailure"; readonly error: ReplicaError.ReplicaError }

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
  readonly id: symbol
  readonly nonce: RestoreProtocol.RestoreNonce
  readonly options: BeginOptions
  readonly lease: SessionManager.RestoreLease
  readonly operationScope: Scope.Closeable
  readonly setupReady: Deferred.Deferred<void>
  readonly supervisorStarted: Deferred.Deferred<void>
  readonly shutdown: Deferred.Deferred<ReplicaError.ReplicaError>
  readonly cleanupComplete: Deferred.Deferred<void>
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

const failSync = <A,>(deferred: Deferred.Deferred<A, ReplicaError.ReplicaError>, error: ReplicaError.ReplicaError) =>
  Effect.runSync(Deferred.fail(deferred, error))

const succeedSync = <A, E,>(deferred: Deferred.Deferred<A, E>, value: A) =>
  Effect.runSync(Deferred.succeed(deferred, value))

const frameFields: Readonly<Record<string, ReadonlySet<string>>> = {
  Start: new Set(["_tag", "nonce", "sequence"]),
  Chunk: new Set(["_tag", "nonce", "sequence", "bytes"]),
  End: new Set(["_tag", "nonce", "sequence"]),
  SourceFailure: new Set(["_tag", "nonce", "sequence", "error"]),
  TerminalAck: new Set(["_tag", "nonce", "sequence"]),
  ReleasedAck: new Set(["_tag", "nonce", "sequence"])
}

const preflightFrame = (
  input: unknown,
  maxErrorBytes: number,
  maxChunkBytes: number
): boolean => {
  try {
    if (typeof input !== "object" || input === null || Array.isArray(input)) return false
    const tag = Reflect.get(input, "_tag")
    if (typeof tag !== "string") return false
    const allowedFields = frameFields[tag]
    if (allowedFields === undefined) return false
    const keys = Reflect.ownKeys(input)
    if (
      keys.length !== allowedFields.size ||
      keys.some((key) => typeof key !== "string" || !allowedFields.has(key))
    ) {
      return false
    }
    if (tag === "Chunk") {
      const bytes = Reflect.get(input, "bytes")
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
      return RestoreProtocol.preflight(Reflect.get(input, "error"), maxErrorBytes)
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
      const initialStartingZero = yield* Deferred.make<void>()
      yield* Deferred.succeed(initialStartingZero, undefined)

      let closed = false
      let starting = 0
      let startingZero = initialStartingZero
      const controllers = new Map<symbol, Controller>()

      const enterStarting = Effect.gen(function*() {
        const nextZero = yield* Deferred.make<void>()
        return yield* Effect.sync(() => {
          if (closed) return false
          if (starting === 0) startingZero = nextZero
          starting += 1
          return true
        })
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
          if (starting === 0) succeedSync(startingZero, undefined)
        })

      const stopIngress = (controller: Controller) => {
        const port = controller.port
        if (port === undefined) return
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
            failSync(awaiting.deferred, error)
            break
          case "Source":
            failSync(awaiting.deferred, error)
            break
          case "TerminalAck":
            failSync(awaiting.deferred, error)
            break
          case "ReleasedAck":
            failSync(awaiting.deferred, error)
        }
      }

      const beginShutdown = (controller: Controller, error: ReplicaError.ReplicaError) => {
        if (controller.phase === "Closed" || controller.phase === "Finishing") return
        controller.phase = "ShutdownRequested"
        stopIngress(controller)
        succeedSync(controller.shutdown, error)
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
          if (!succeedSync(awaiting.deferred, frame)) invalidFrame(controller)
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
          if (!succeedSync(awaiting.deferred, frame)) invalidFrame(controller)
          return
        }
        if (awaiting._tag === "TerminalAck" && frame._tag === "TerminalAck") {
          controller.awaiting = undefined
          if (!succeedSync(awaiting.deferred, frame)) invalidFrame(controller)
          return
        }
        if (awaiting._tag === "ReleasedAck" && frame._tag === "ReleasedAck") {
          controller.awaiting = undefined
          if (!succeedSync(awaiting.deferred, frame)) invalidFrame(controller)
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
          beginShutdown(
            controller,
            unavailable(new Error("restore transport message decoding failed"))
          )
        const onClose = () =>
          beginShutdown(
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
        Effect.gen(function*() {
          const deferred = yield* Deferred.make<RestoreProtocol.Start, ReplicaError.ReplicaError>()
          controller.awaiting = { _tag: "Start", sequence: 0, deferred }
          return yield* boundWait(
            Deferred.await(deferred),
            "RestoreBackupStart",
            sessions.maxRestorePullMillis
          ).pipe(
            Effect.tapError((error) => Effect.sync(() => beginShutdown(controller, error)))
          )
        })

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
              return yield* RestoreProtocol.decodeReplicaError(frame.error)
            }
            return [frame.bytes, sequence + 1] as const
          }))

      const work = (controller: Controller): Effect.Effect<void, WorkFailure> =>
        Effect.gen(function*() {
          const restore = Effect.gen(function*() {
            yield* awaitStart(controller)
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
                (error): WorkFailure => ({ _tag: "RestoreFailure", error })
              ))
            )
          )
          const expired = Deferred.await(controller.lease.expired).pipe(
            Effect.catchCause((cause) =>
              Effect.failCause(Cause.map(
                cause,
                (error): WorkFailure => ({ _tag: "SessionFailure", error })
              ))
            )
          )
          return yield* Effect.raceFirst(restore, expired)
        })

      const sendTerminalAndAwaitAck = (
        controller: Controller,
        exit: Exit.Exit<void, WorkFailure>
      ): Effect.Effect<void, ReplicaError.ReplicaError | WorkFailure> =>
        Effect.gen(function*() {
          controller.phase = "Finishing"
          if (Exit.isFailure(exit)) {
            const reasons = exit.cause.reasons
            if (reasons.length !== 1 || Cause.isInterruptReason(reasons[0]!)) {
              return yield* Effect.failCause(exit.cause)
            }
          }
          const sequence = controller.lastSequence + 1
          const ack = yield* Deferred.make<RestoreProtocol.TerminalAck, ReplicaError.ReplicaError>()
          controller.awaiting = { _tag: "TerminalAck", sequence, deferred: ack }
          if (Exit.isSuccess(exit)) {
            yield* post(controller, RestoreProtocol.TerminalSuccess, {
              _tag: "TerminalSuccess",
              nonce: controller.nonce,
              sequence
            })
          } else {
            const reason = exit.cause.reasons[0]!
            if (Cause.isDieReason(reason)) {
              yield* post(controller, RestoreProtocol.TerminalDefect, {
                _tag: "TerminalDefect",
                nonce: controller.nonce,
                sequence,
                defect: RestoreProtocol.encodeDefect(reason.defect, sessions.maxRestoreErrorBytes)
              })
            } else if (Cause.isFailReason(reason) && reason.error._tag === "SessionFailure") {
              yield* post(controller, RestoreProtocol.TerminalSessionFailure, {
                _tag: "TerminalSessionFailure",
                nonce: controller.nonce,
                sequence,
                error: RestoreProtocol.encodeReplicaError(reason.error.error, sessions.maxRestoreErrorBytes)
              })
            } else if (Cause.isFailReason(reason) && reason.error._tag === "RestoreFailure") {
              yield* post(controller, RestoreProtocol.TerminalRestoreFailure, {
                _tag: "TerminalRestoreFailure",
                nonce: controller.nonce,
                sequence,
                error: RestoreProtocol.encodeReplicaError(reason.error.error, sessions.maxRestoreErrorBytes)
              })
            } else {
              return yield* Effect.die(new Error("restore work failed without a Cause reason"))
            }
          }
          yield* Deferred.await(ack)
        })

      const sendReleasedAndAwaitAck = (
        controller: Controller
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          const sequence = controller.lastSequence + 2
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
            yield* Scope.close(controller.operationScope, exit)
            controllers.delete(controller.id)
            yield* Deferred.succeed(controller.cleanupComplete, undefined)
          })
        )

      const supervise = (controller: Controller) =>
        Effect.gen(function*() {
          yield* Deferred.succeed(controller.supervisorStarted, undefined)
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
          controller.phase = "Finishing"
          controller.ignoredOutstandingSequence = controller.awaiting?._tag === "Source"
            ? controller.awaiting.sequence
            : undefined
          const acquireFinishing = Effect.acquireRelease(
            finishing.take(1),
            () => finishing.release(1),
            { interruptible: true }
          ).pipe(
            Effect.provideService(Scope.Scope, controller.operationScope),
            Effect.asVoid
          )
          yield* boundWait(
            acquireFinishing.pipe(
              Effect.andThen(sendTerminalAndAwaitAck(controller, outcome.exit))
            ),
            "RestoreBackupTerminalAck",
            sessions.maxRestorePullMillis
          )
          yield* sendReleasedAndAwaitAck(controller)
        }).pipe(
          Effect.onExit((exit) => cleanup(controller, exit))
        )

      const begin: RestoreTransport["Service"]["begin"] = (options) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            const startingOwned = { value: true }
            let lease: SessionManager.RestoreLease | undefined
            let operationScope: Scope.Closeable | undefined
            let controller: Controller | undefined
            let supervisorLive = false
            let published = false
            yield* enterStarting
            const attempt = Effect.gen(function*() {
              lease = yield* sessions.acquireRestore(options.sessionId, options.clientId)
              const uuid = yield* crypto.randomUUIDv4.pipe(
                Effect.mapError((cause) => unavailable(cause))
              )
              const nonce = RestoreProtocol.RestoreNonce.make(`rst_${uuid}`)
              operationScope = yield* Scope.make("sequential")
              yield* Scope.addFinalizer(operationScope, lease.release)
              controller = {
                id: Symbol(),
                nonce,
                options,
                lease,
                operationScope,
                setupReady: yield* Deferred.make<void>(),
                supervisorStarted: yield* Deferred.make<void>(),
                shutdown: yield* Deferred.make<ReplicaError.ReplicaError>(),
                cleanupComplete: yield* Deferred.make<void>(),
                phase: "SettingUp",
                port: undefined,
                peerPort: undefined,
                awaiting: undefined,
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
              yield* Deferred.await(controller.supervisorStarted)
              const accepted = yield* Effect.sync(() => {
                if (closed) return false
                controllers.set(controller!.id, controller!)
                published = true
                return true
              })
              if (!accepted) {
                beginShutdown(controller, unavailable(new Error("restore transport is closed")))
                yield* Deferred.await(controller.cleanupComplete)
                return yield* unavailable(new Error("restore transport is closed"))
              }
              yield* leaveStarting(startingOwned)
              const channel = yield* Effect.sync(() => new MessageChannel())
              controller.port = channel.port1
              controller.peerPort = channel.port2
              installIngress(controller, channel.port1)
              yield* Deferred.succeed(controller.setupReady, undefined)
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
                    beginShutdown(controller, unavailable(Cause.squash(exit.cause)))
                    yield* Deferred.await(controller.cleanupComplete)
                  } else if (operationScope !== undefined) {
                    yield* Scope.close(operationScope, exit)
                  } else if (lease !== undefined) {
                    yield* lease.release
                  }
                  if (!published) yield* leaveStarting(startingOwned)
                })
              )
            )
          })
        )

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
          yield* Deferred.await(startingZero)
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
