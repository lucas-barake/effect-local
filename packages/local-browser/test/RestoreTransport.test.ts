import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
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
import * as Layer from "effect/Layer"
import * as PlatformError from "effect/PlatformError"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as RestoreProtocol from "../src/internal/restoreProtocol.js"
import * as RestoreTransport from "../src/internal/RestoreTransport.js"
import * as SessionManager from "../src/SessionManager.js"
import { replica as fixtureReplica } from "./fixtures.js"

const limits = {
  maxBackupBytes: 1024,
  maxChunkBytes: 8,
  maxArchiveRecords: 100,
  maxJsonDepth: 16,
  maxConflictDepth: 16,
  maxConflictNodes: 10_000,
  maxConflictAlternatives: 1_000,
  maxConflictPathSegments: 16,
  maxConflictValueBytes: 1024 * 1024,
  maxConflictSourceChanges: 10_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
  maxSyncMessageBytes: 1024,
  maxPeerSendMillis: 1_000,
  maxSyncChangesPerMessage: 10,
  maxSyncDependencyEdgesPerMessage: 20,
  maxSyncOperationsPerMessage: 100,
  maxPendingBytesPerDocument: 1024,
  maxPendingBytesPerPeer: 2048,
  maxPendingBytesPerReplica: 4096,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 10,
  maxPendingChangesPerPeer: 20,
  maxPendingChangesPerReplica: 40,
  maxPendingDependencyEdgesPerDocument: 100,
  maxPendingDependencyEdgesPerPeer: 200,
  maxPendingDependencyEdgesPerReplica: 400,
  maxSessions: 2,
  maxStreamsPerSession: 1,
  maxInFlightPerSession: 1,
  maxQueuedRpc: 2,
  maxQueuedPermits: 2,
  maxActiveRestores: 2,
  maxRestoresPerSession: 1,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
} satisfies ReplicaLimits.Values

const installationId = Identity.BackupInstallationId.make("bak_ea5a1250-2d04-4c92-987a-e62d411a0b4e")

const installMessageChannel = (constructor: unknown) => {
  Object.defineProperty(globalThis, "MessageChannel", {
    configurable: true,
    value: constructor,
    writable: true
  })
}

const complete = <A, E,>(deferred: Deferred.Deferred<A, E>, value: A): void => {
  Deferred.doneUnsafe(deferred, Effect.succeed(value))
}

const waitForControllerCount = (
  transport: RestoreTransport.RestoreTransport["Service"],
  expected: number
): Effect.Effect<void> =>
  Effect.suspend(() =>
    transport.activeControllerCount.pipe(
      Effect.flatMap((count) => {
        if (count === expected) return Effect.void
        return Effect.yieldNow.pipe(Effect.andThen(waitForControllerCount(transport, expected)))
      })
    )
  )

const makeLayer = (
  replica: Replica.Replica["Service"],
  configuredLimits: ReplicaLimits.Values = limits,
  cryptoLayer: Layer.Layer<Crypto.Crypto> = NodeCrypto.layer
) => {
  const sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(configuredLimits)))
  const dependencies = Layer.mergeAll(
    cryptoLayer,
    sessions,
    Layer.succeed(Replica.Replica, replica)
  )
  return Layer.merge(
    dependencies,
    RestoreTransport.freshLayer.pipe(Layer.provide(dependencies))
  )
}

const beginUnclaimed = Effect.gen(function*() {
  const sessions = yield* SessionManager.SessionManager
  const transport = yield* RestoreTransport.RestoreTransport
  const sessionId = yield* Identity.makeSessionId
  yield* sessions.open(sessionId, 1)
  const started = yield* transport.begin({
    sessionId,
    clientId: 1,
    mode: "replace",
    maxBytes: limits.maxBackupBytes,
    expectedDefinitionHash: "test-definition",
    installationId
  })
  return { sessionId, sessions, started, transport }
})

const begin = Effect.gen(function*() {
  const begun = yield* beginUnclaimed
  const { sessionId, started, transport } = begun
  const result = yield* transport.finish({
    sessionId,
    clientId: 1,
    nonce: started.nonce
  })
  return { ...begun, result }
})

const restoreResultError = (
  exit: Exit.Exit<void, RestoreProtocol.RestoreResultFailure>
): RestoreProtocol.RestoreWireError => {
  if (!Exit.isFailure(exit)) {
    return Effect.runSync(Effect.die("expected restore result failure"))
  }
  const reason = exit.cause.reasons[0]
  if (reason === undefined || !Cause.isFailReason(reason)) {
    return Effect.runSync(Effect.die("expected one typed restore result failure"))
  }
  assert.strictEqual(reason.error._tag, "RestoreResultRestoreFailure")
  return reason.error.error
}

const assertRestoreOperationTimeout = (
  exit: Exit.Exit<void, RestoreProtocol.RestoreResultFailure>,
  operation: string,
  timeoutMillis: number
) => {
  const error = restoreResultError(exit)
  assert.strictEqual(error._tag, "OperationTimeout")
  if (error._tag === "OperationTimeout") {
    assert.strictEqual(error.operation, operation)
    assert.strictEqual(error.timeoutMillis, timeoutMillis)
  }
}

it.effect("does not export the internal RestoreTransport package subpath", () =>
  Effect.gen(function*() {
    const error = yield* Effect.flip(Effect.try({
      try: () => import.meta.resolve("@lucas-barake/effect-local-browser/RestoreTransport"),
      catch: (cause) => cause
    }))
    assert.instanceOf(error, Error)
    let code: unknown
    if (error instanceof Error && "code" in error) code = error.code
    assert.strictEqual(
      code,
      "ERR_PACKAGE_PATH_NOT_EXPORTED"
    )
  }))

it.effect("holds Start until Finish claims the result and does not close an incomplete claimed result", () =>
  Effect.scoped(Effect.gen(function*() {
    const applied = yield* Deferred.make<void>()
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const released = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Stream.runDrain(options.source).pipe(
          Effect.tap(() => Deferred.succeed(applied, undefined))
        )
    }
    return yield* Effect.gen(function*() {
      const { sessionId, started, transport } = yield* beginUnclaimed
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "Pull") {
          started.port.postMessage(
            {
              _tag: "End",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.End
          )
        } else if (event.data._tag === "TerminalReady") {
          complete(terminal, event.data)
        } else if (event.data._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
          complete(released, undefined)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Effect.yieldNow
      assert.isFalse(yield* Deferred.isDone(applied))

      const result = yield* transport.finish({
        sessionId,
        clientId: 1,
        nonce: started.nonce
      })
      yield* Deferred.await(applied)
      const terminalFrame = yield* Deferred.await(terminal)
      yield* Deferred.await(result)
      assert.strictEqual(yield* transport.activeControllerCount, 1)
      started.port.postMessage(
        {
          _tag: "TerminalAck",
          nonce: started.nonce,
          sequence: terminalFrame.sequence
        } satisfies RestoreProtocol.TerminalAck
      )
      yield* Deferred.await(released)
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("accepts Finish before Start and rejects duplicate, wrong nonce, and wrong session claims", () =>
  Effect.scoped(Effect.gen(function*() {
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.never
    }
    return yield* Effect.gen(function*() {
      const { sessionId, started, transport } = yield* beginUnclaimed
      yield* transport.finish({
        sessionId,
        clientId: 1,
        nonce: started.nonce
      })

      const duplicate = yield* transport.finish({
        sessionId,
        clientId: 1,
        nonce: started.nonce
      })
      const wrongNonce = yield* transport.finish({
        sessionId,
        clientId: 1,
        nonce: RestoreProtocol.RestoreNonce.make(
          "rst_00000000-0000-4000-8000-000000000001"
        )
      })
      const wrongSession = yield* transport.finish({
        sessionId: Identity.SessionId.make(
          "ses_00000000-0000-4000-8000-000000000001"
        ),
        clientId: 1,
        nonce: started.nonce
      })
      for (const rejected of [duplicate, wrongNonce, wrongSession]) {
        const exit = yield* Deferred.await(rejected).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) {
          const reason = exit.cause.reasons[0]
          assert.isTrue(reason !== undefined && Cause.isFailReason(reason))
          if (reason !== undefined && Cause.isFailReason(reason)) {
            assert.strictEqual(reason.error._tag, "RestoreResultSessionFailure")
            assert.strictEqual(reason.error.error._tag, "ProtocolMismatch")
          }
        }
      }
      started.port.close()
      yield* waitForControllerCount(transport, 0)
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("uses the owner restore deadline as the backstop for an abandoned claimed result", () =>
  Effect.scoped(Effect.gen(function*() {
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.never
    }
    return yield* Effect.gen(function*() {
      const { started, transport } = yield* begin
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Effect.yieldNow
      assert.strictEqual(yield* transport.activeControllerCount, 1)
      yield* TestClock.adjust(limits.maxRestoreMillis + 1)
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("streams one exact backing buffer per Pull and completes the release handshake", () =>
  Effect.scoped(Effect.gen(function*() {
    const restored = yield* Deferred.make<ReadonlyArray<Uint8Array>>()
    const released = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        options.source.pipe(
          Stream.runCollect,
          Effect.tap((chunks) => Deferred.succeed(restored, chunks))
        )
    }
    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* begin
      const sent = Uint8Array.of(1, 2, 3, 4)
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        const frame = event.data
        if (frame._tag === "Pull" && frame.sequence === 1) {
          started.port.postMessage(
            {
              _tag: "Chunk",
              nonce: started.nonce,
              sequence: frame.sequence,
              bytes: sent
            } satisfies RestoreProtocol.Chunk,
            [sent.buffer]
          )
        } else if (frame._tag === "Pull") {
          started.port.postMessage(
            {
              _tag: "End",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.End
          )
        } else if (frame._tag === "TerminalReady") {
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (frame._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
          complete(released, undefined)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const chunks = yield* Deferred.await(restored)
      yield* Deferred.await(released)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(chunks.length, 1)
      assert.deepStrictEqual(Array.from(chunks[0]), [1, 2, 3, 4])
      assert.strictEqual(chunks[0].byteOffset, 0)
      assert.strictEqual(chunks[0].buffer.byteLength, chunks[0].byteLength)
      assert.strictEqual(sent.buffer.byteLength, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("accepts an encoded SourceFailure at the minimum nested error budget", () =>
  Effect.scoped(Effect.gen(function*() {
    const minimumLimits = {
      ...limits,
      maxRestoreErrorBytes: ReplicaLimits.minimumRestoreErrorBytes
    } satisfies ReplicaLimits.Values
    const sourceError = new ReplicaError.ReplicaError({
      reason: new ReplicaError.UnsupportedDocumentVersion({
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
        observedVersion: 2,
        supportedVersion: 1
      })
    })
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) => Stream.runDrain(options.source)
    }
    return yield* Effect.gen(function*() {
      const { result, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        const frame = event.data
        if (frame._tag === "Pull") {
          started.port.postMessage(
            {
              _tag: "SourceFailure",
              nonce: started.nonce,
              sequence: frame.sequence,
              error: RestoreProtocol.encodeReplicaError(
                sourceError,
                ReplicaLimits.minimumRestoreErrorBytes
              )
            } satisfies RestoreProtocol.SourceFailure
          )
        } else if (frame._tag === "TerminalReady") {
          complete(terminal, frame)
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (frame._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      yield* Deferred.await(terminal)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.strictEqual(exit.cause.reasons.length, 1)
        const reason = exit.cause.reasons[0]
        assert.isTrue(reason !== undefined && Cause.isFailReason(reason))
        if (reason !== undefined && Cause.isFailReason(reason)) {
          assert.strictEqual(reason.error._tag, "RestoreResultRestoreFailure")
          assert.strictEqual(reason.error.error._tag, "UnsupportedDocumentVersion")
        }
      }
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica, minimumLimits)))
  })))

it.effect("reports session expiry before Start as an authoritative session failure", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.die("restore must not start before Start")
    }
    return yield* Effect.gen(function*() {
      const { result, sessionId, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        const frame = event.data
        if (frame._tag === "TerminalReady") {
          complete(terminal, frame)
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (frame._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      started.port.start()

      yield* Effect.yieldNow
      yield* sessions.close(sessionId, 1)
      yield* Effect.yieldNow
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* Effect.yieldNow
      assert.isTrue(yield* Deferred.isDone(terminal))
      const frame = yield* Deferred.await(terminal)
      assert.strictEqual(frame._tag, "TerminalReady")
      assert.strictEqual(frame.sequence, 1)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const reason = exit.cause.reasons[0]
        assert.isTrue(reason !== undefined && Cause.isFailReason(reason))
        if (reason !== undefined && Cause.isFailReason(reason)) {
          assert.strictEqual(reason.error._tag, "RestoreResultSessionFailure")
        }
      }
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("reserves an outstanding Pull sequence before posting the authoritative terminal", () =>
  Effect.scoped(Effect.gen(function*() {
    const pullSeen = yield* Deferred.make<RestoreProtocol.Pull>()
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const released = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Effect.gen(function*() {
          yield* options.source.pipe(
            Stream.runHead,
            Effect.forkChild({ startImmediately: true })
          )
          yield* Deferred.await(pullSeen)
        })
    }
    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        const frame = event.data
        if (frame._tag === "Pull") {
          complete(pullSeen, frame)
        } else if (frame._tag === "TerminalReady") {
          complete(terminal, frame)
          started.port.postMessage(
            {
              _tag: "End",
              nonce: started.nonce,
              sequence: RestoreProtocol.RestoreSequence.make(1)
            } satisfies RestoreProtocol.End
          )
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (frame._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: frame.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
          complete(released, undefined)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      assert.strictEqual((yield* Deferred.await(pullSeen)).sequence, 1)
      assert.strictEqual((yield* Deferred.await(terminal)).sequence, 2)
      yield* Deferred.await(released)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("does not collapse a composite typed failure and defect Cause into one terminal", () =>
  Effect.scoped(Effect.gen(function*() {
    const typed = new ReplicaError.ReplicaError({
      reason: new ReplicaError.RestoreFailed({ cause: Error("typed failure") })
    })
    const workStarted = yield* Deferred.make<void>()
    const terminal = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Deferred.succeed(workStarted, undefined).pipe(
          Effect.andThen(
            Effect.failCause(Cause.combine(Cause.fail(typed), Cause.die(Error("winning defect"))))
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { result, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(terminal, undefined)
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      yield* Deferred.await(workStarted)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map((reason) => reason._tag), ["Fail", "Die"])
      }
      yield* Deferred.await(terminal)
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("does not collapse two typed failures in a composite restore Cause", () =>
  Effect.scoped(Effect.gen(function*() {
    const first = new ReplicaError.ReplicaError({
      reason: new ReplicaError.RestoreFailed({ cause: Error("first failure") })
    })
    const second = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({ cause: Error("second failure") })
    })
    const workStarted = yield* Deferred.make<void>()
    const terminal = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Deferred.succeed(workStarted, undefined).pipe(
          Effect.andThen(
            Effect.failCause(Cause.combine(Cause.fail(first), Cause.fail(second)))
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { result, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(terminal, undefined)
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      yield* Deferred.await(workStarted)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map((reason) => reason._tag), ["Fail", "Fail"])
      }
      yield* Deferred.await(terminal)
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("preserves interruption over a typed failure in a composite restore Cause", () =>
  Effect.scoped(Effect.gen(function*() {
    const typed = new ReplicaError.ReplicaError({
      reason: new ReplicaError.RestoreFailed({ cause: Error("typed failure") })
    })
    const workStarted = yield* Deferred.make<void>()
    const terminal = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Deferred.succeed(workStarted, undefined).pipe(
          Effect.andThen(
            Effect.failCause(Cause.combine(
              Cause.fail(typed),
              Cause.interrupt(1)
            ))
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { result, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(terminal, undefined)
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          started.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      yield* Deferred.await(workStarted)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map((reason) => reason._tag), ["Fail", "Interrupt"])
      }
      yield* Deferred.await(terminal)
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("fails closed on an unsolicited payload before Start without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    let applications = 0
    const closed = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* begin
      started.port.addEventListener("close", () => {
        complete(closed, undefined)
      })
      started.port.start()
      const bytes = Uint8Array.of(1)
      started.port.postMessage(
        {
          _tag: "Chunk",
          nonce: started.nonce,
          sequence: RestoreProtocol.RestoreSequence.make(0),
          bytes
        } satisfies RestoreProtocol.Chunk,
        [bytes.buffer]
      )

      yield* Deferred.await(closed)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("rejects a Start with the wrong page nonce without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: RestoreProtocol.RestoreNonce.make(
            "rst_00000000-0000-4000-8000-000000000001"
          ),
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("rejects a stale Start during Pull without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    const pull = yield* Deferred.make<RestoreProtocol.Pull>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Stream.runDrain(options.source).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              applications += 1
            })
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener(
        "message",
        (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
          if (event.data._tag === "Pull") complete(pull, event.data)
        }
      )
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(pull)
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("rejects a stale Chunk during a later Pull without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    const secondPull = yield* Deferred.make<RestoreProtocol.Pull>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Stream.runDrain(options.source).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              applications += 1
            })
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener(
        "message",
        (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
          if (event.data._tag !== "Pull") return
          if (event.data.sequence === 1) {
            const bytes = Uint8Array.of(1)
            started.port.postMessage(
              {
                _tag: "Chunk",
                nonce: started.nonce,
                sequence: event.data.sequence,
                bytes
              } satisfies RestoreProtocol.Chunk,
              [bytes.buffer]
            )
          } else {
            complete(secondPull, event.data)
          }
        }
      )
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(secondPull)
      const staleBytes = Uint8Array.of(2)
      started.port.postMessage(
        {
          _tag: "Chunk",
          nonce: started.nonce,
          sequence: RestoreProtocol.RestoreSequence.make(1),
          bytes: staleBytes
        } satisfies RestoreProtocol.Chunk,
        [staleBytes.buffer]
      )

      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("rejects a small view over a larger transferred backing buffer", () =>
  Effect.scoped(Effect.gen(function*() {
    let applications = 0
    const closed = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        options.source.pipe(
          Stream.runDrain,
          Effect.tap(() =>
            Effect.sync(() => {
              applications += 1
            })
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag !== "Pull") return
        const backing = new ArrayBuffer(4)
        const view = new Uint8Array(backing, 1, 1)
        view[0] = 42
        started.port.postMessage(
          {
            _tag: "Chunk",
            nonce: started.nonce,
            sequence: event.data.sequence,
            bytes: view
          } satisfies RestoreProtocol.Chunk,
          [backing]
        )
      })
      started.port.addEventListener("close", () => {
        complete(closed, undefined)
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      yield* Deferred.await(closed)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("rejects excess transferred binary fields before applying a valid Chunk", () =>
  Effect.scoped(Effect.gen(function*() {
    const outcome = yield* Deferred.make<"Close" | "Terminal">()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        options.source.pipe(
          Stream.runHead,
          Effect.tap(() =>
            Effect.sync(() => {
              applications += 1
            })
          ),
          Effect.asVoid
        )
    }
    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "Pull") {
          const bytes = Uint8Array.of(1)
          const excess = new ArrayBuffer(1024)
          started.port.postMessage({
            _tag: "Chunk",
            nonce: started.nonce,
            sequence: event.data.sequence,
            bytes,
            excess
          }, [bytes.buffer, excess])
        } else if (event.data._tag === "TerminalReady") {
          complete(outcome, "Terminal")
        }
      })
      started.port.addEventListener("close", () => {
        complete(outcome, "Close")
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      assert.strictEqual(yield* Deferred.await(outcome), "Close")
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("times out Start without applying restore and releases controller admission", () =>
  Effect.scoped(Effect.gen(function*() {
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin

      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assertRestoreOperationTimeout(
        exit,
        "RestoreBackupStart",
        limits.maxRestorePullMillis
      )
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("times out an unclaimed result after Start without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    const startObserved = yield* Deferred.make<void>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    const NativeMessageChannel = globalThis.MessageChannel
    class StartObservedMessageChannel {
      readonly port1: MessagePort
      readonly port2: MessagePort
      constructor() {
        const native = new NativeMessageChannel()
        this.port1 = native.port1
        this.port2 = native.port2
        this.port1.addEventListener("message", (event: MessageEvent<unknown>) => {
          if (
            typeof event.data === "object" &&
            event.data !== null &&
            Reflect.get(event.data, "_tag") === "Start"
          ) {
            complete(startObserved, undefined)
          }
        })
      }
    }
    installMessageChannel(StartObservedMessageChannel)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        globalThis.MessageChannel = NativeMessageChannel
      })
    )

    return yield* Effect.gen(function*() {
      const { sessions, started, transport } = yield* beginUnclaimed
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(startObserved)

      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("times out Pull and finalizes the active restore source", () =>
  Effect.scoped(Effect.gen(function*() {
    const pull = yield* Deferred.make<RestoreProtocol.Pull>()
    const finalized = yield* Deferred.make<void>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Effect.sync(() => {
          applications += 1
        }).pipe(
          Effect.andThen(Stream.runDrain(options.source)),
          Effect.ensuring(Deferred.succeed(finalized, undefined))
        )
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "Pull") {
          complete(pull, event.data)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(pull)

      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      assertRestoreOperationTimeout(
        exit,
        "RestoreBackupPull",
        limits.maxRestorePullMillis
      )
      yield* Deferred.await(finalized)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 1)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("times out TerminalAck after publishing the authoritative result", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(terminal, event.data)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(terminal)
      yield* Deferred.await(result)

      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 1)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("times out ReleasedAck after releasing restore admission", () =>
  Effect.scoped(Effect.gen(function*() {
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          started.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: started.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          complete(released, event.data)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(released)
      yield* Deferred.await(result)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)

      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 1)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("cleans up a messageerror before Start without applying restore", () =>
  Effect.scoped(Effect.gen(function*() {
    let ownerPort: MessagePort | undefined
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    const NativeMessageChannel = globalThis.MessageChannel
    class ExposedOwnerMessageChannel {
      readonly port1: MessagePort
      readonly port2: MessagePort
      constructor() {
        const native = new NativeMessageChannel()
        this.port1 = native.port1
        this.port2 = native.port2
        ownerPort = this.port1
      }
    }
    installMessageChannel(ExposedOwnerMessageChannel)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        globalThis.MessageChannel = NativeMessageChannel
      })
    )

    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      if (ownerPort === undefined) yield* Effect.die("owner endpoint was not created")
      ownerPort.dispatchEvent(new MessageEvent("messageerror"))

      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      const error = restoreResultError(exit)
      assert.strictEqual(error._tag, "StorageUnavailable")
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("cleans up an active Pull messageerror and finalizes the restore source", () =>
  Effect.scoped(Effect.gen(function*() {
    const pull = yield* Deferred.make<RestoreProtocol.Pull>()
    const finalized = yield* Deferred.make<void>()
    let ownerPort: MessagePort | undefined
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        Effect.sync(() => {
          applications += 1
        }).pipe(
          Effect.andThen(Stream.runDrain(options.source)),
          Effect.ensuring(Deferred.succeed(finalized, undefined))
        )
    }
    const NativeMessageChannel = globalThis.MessageChannel
    class ExposedOwnerMessageChannel {
      readonly port1: MessagePort
      readonly port2: MessagePort
      constructor() {
        const native = new NativeMessageChannel()
        this.port1 = native.port1
        this.port2 = native.port2
        ownerPort = this.port1
      }
    }
    installMessageChannel(ExposedOwnerMessageChannel)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        globalThis.MessageChannel = NativeMessageChannel
      })
    )

    return yield* Effect.gen(function*() {
      const { result, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "Pull") {
          complete(pull, event.data)
        }
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(pull)
      if (ownerPort === undefined) yield* Effect.die("owner endpoint was not created")
      ownerPort.dispatchEvent(new MessageEvent("messageerror"))

      const exit = yield* Deferred.await(result).pipe(Effect.exit)
      const error = restoreResultError(exit)
      assert.strictEqual(error._tag, "StorageUnavailable")
      yield* Deferred.await(finalized)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 1)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

const disconnects = ["close", "messageerror"] satisfies ReadonlyArray<"close" | "messageerror">
for (const disconnect of disconnects) {
  it.effect(`cleans up an owner ${disconnect} while awaiting ResultClaim`, () =>
    Effect.scoped(Effect.gen(function*() {
      const startObserved = yield* Deferred.make<void>()
      let ownerPort: MessagePort | undefined
      let applications = 0
      const replica: Replica.Replica["Service"] = {
        ...fixtureReplica,
        restoreBackup: () =>
          Effect.sync(() => {
            applications += 1
          })
      }
      const NativeMessageChannel = globalThis.MessageChannel
      class ExposedOwnerMessageChannel {
        readonly port1: MessagePort
        readonly port2: MessagePort
        constructor() {
          const native = new NativeMessageChannel()
          this.port1 = native.port1
          this.port2 = native.port2
          ownerPort = this.port1
          this.port1.addEventListener("message", (event: MessageEvent<unknown>) => {
            if (
              typeof event.data === "object" &&
              event.data !== null &&
              Reflect.get(event.data, "_tag") === "Start"
            ) {
              complete(startObserved, undefined)
            }
          })
        }
      }
      installMessageChannel(ExposedOwnerMessageChannel)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.MessageChannel = NativeMessageChannel
        })
      )

      return yield* Effect.gen(function*() {
        const { sessions, started, transport } = yield* beginUnclaimed
        started.port.start()
        started.port.postMessage(
          {
            _tag: "Start",
            nonce: started.nonce,
            sequence: 0
          } satisfies RestoreProtocol.Start
        )
        yield* Deferred.await(startObserved)
        yield* Effect.yieldNow
        if (ownerPort === undefined) yield* Effect.die("owner endpoint was not created")
        let event: Event
        if (disconnect === "close") event = new Event("close")
        else event = new MessageEvent("messageerror")
        ownerPort.dispatchEvent(event)

        yield* waitForControllerCount(transport, 0)
        assert.strictEqual(applications, 0)
        assert.strictEqual(yield* sessions.activeRestoreCount, 0)
        started.port.close()
      }).pipe(Effect.provide(makeLayer(replica)))
    })))

  it.effect(`preserves the authoritative result after an owner ${disconnect} during TerminalAck`, () =>
    Effect.scoped(Effect.gen(function*() {
      const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
      let ownerPort: MessagePort | undefined
      let applications = 0
      const replica: Replica.Replica["Service"] = {
        ...fixtureReplica,
        restoreBackup: () =>
          Effect.sync(() => {
            applications += 1
          })
      }
      const NativeMessageChannel = globalThis.MessageChannel
      class ExposedOwnerMessageChannel {
        readonly port1: MessagePort
        readonly port2: MessagePort
        constructor() {
          const native = new NativeMessageChannel()
          this.port1 = native.port1
          this.port2 = native.port2
          ownerPort = this.port1
        }
      }
      installMessageChannel(ExposedOwnerMessageChannel)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.MessageChannel = NativeMessageChannel
        })
      )

      return yield* Effect.gen(function*() {
        const { result, sessions, started, transport } = yield* begin
        started.port.addEventListener(
          "message",
          (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
            if (event.data._tag === "TerminalReady") complete(terminal, event.data)
          }
        )
        started.port.start()
        started.port.postMessage(
          {
            _tag: "Start",
            nonce: started.nonce,
            sequence: 0
          } satisfies RestoreProtocol.Start
        )

        yield* Deferred.await(terminal)
        const resultExit = yield* Deferred.await(result).pipe(Effect.exit)
        assert.isTrue(Exit.isSuccess(resultExit))
        if (ownerPort === undefined) yield* Effect.die("owner endpoint was not created")
        let event: Event
        if (disconnect === "close") event = new Event("close")
        else event = new MessageEvent("messageerror")
        ownerPort.dispatchEvent(event)

        yield* waitForControllerCount(transport, 0)
        assert.strictEqual(applications, 1)
        assert.strictEqual(yield* sessions.activeRestoreCount, 0)
        started.port.close()
      }).pipe(Effect.provide(makeLayer(replica)))
    })))

  it.effect(`preserves cleanup after an owner ${disconnect} during ReleasedAck`, () =>
    Effect.scoped(Effect.gen(function*() {
      const released = yield* Deferred.make<RestoreProtocol.Released>()
      let ownerPort: MessagePort | undefined
      let applications = 0
      const replica: Replica.Replica["Service"] = {
        ...fixtureReplica,
        restoreBackup: () =>
          Effect.sync(() => {
            applications += 1
          })
      }
      const NativeMessageChannel = globalThis.MessageChannel
      class ExposedOwnerMessageChannel {
        readonly port1: MessagePort
        readonly port2: MessagePort
        constructor() {
          const native = new NativeMessageChannel()
          this.port1 = native.port1
          this.port2 = native.port2
          ownerPort = this.port1
        }
      }
      installMessageChannel(ExposedOwnerMessageChannel)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.MessageChannel = NativeMessageChannel
        })
      )

      return yield* Effect.gen(function*() {
        const { result, sessions, started, transport } = yield* begin
        started.port.addEventListener(
          "message",
          (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
            if (event.data._tag === "TerminalReady") {
              started.port.postMessage(
                {
                  _tag: "TerminalAck",
                  nonce: started.nonce,
                  sequence: event.data.sequence
                } satisfies RestoreProtocol.TerminalAck
              )
            } else if (event.data._tag === "Released") {
              complete(released, event.data)
            }
          }
        )
        started.port.start()
        started.port.postMessage(
          {
            _tag: "Start",
            nonce: started.nonce,
            sequence: 0
          } satisfies RestoreProtocol.Start
        )

        yield* Deferred.await(released)
        const resultExit = yield* Deferred.await(result).pipe(Effect.exit)
        assert.isTrue(Exit.isSuccess(resultExit))
        assert.strictEqual(yield* sessions.activeRestoreCount, 0)
        if (ownerPort === undefined) yield* Effect.die("owner endpoint was not created")
        let event: Event
        if (disconnect === "close") event = new Event("close")
        else event = new MessageEvent("messageerror")
        ownerPort.dispatchEvent(event)

        yield* waitForControllerCount(transport, 0)
        assert.strictEqual(applications, 1)
        assert.strictEqual(yield* sessions.activeRestoreCount, 0)
        started.port.close()
      }).pipe(Effect.provide(makeLayer(replica)))
    })))
}

it.effect("gives ReleasedAck its own wait deadline after a near deadline TerminalAck", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    const closed = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.void
    }
    return yield* Effect.gen(function*() {
      const { started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(terminal, event.data)
        } else if (event.data._tag === "Released") {
          complete(released, event.data)
        }
      })
      started.port.addEventListener("close", () => {
        complete(closed, undefined)
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const terminalFrame = yield* Deferred.await(terminal)
      yield* TestClock.adjust(limits.maxRestorePullMillis - 1)
      started.port.postMessage(
        {
          _tag: "TerminalAck",
          nonce: started.nonce,
          sequence: terminalFrame.sequence
        } satisfies RestoreProtocol.TerminalAck
      )
      const releasedFrame = yield* Deferred.await(released)
      yield* TestClock.adjust(limits.maxRestorePullMillis - 1)
      assert.isFalse(yield* Deferred.isDone(closed))
      assert.strictEqual(yield* transport.activeControllerCount, 1)
      started.port.postMessage(
        {
          _tag: "ReleasedAck",
          nonce: started.nonce,
          sequence: releasedFrame.sequence
        } satisfies RestoreProtocol.ReleasedAck
      )
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("bounds finishing admission by the effective restore capacity", () =>
  Effect.scoped(Effect.gen(function*() {
    const constrainedLimits = {
      ...limits,
      maxSessions: 1,
      maxActiveRestores: 3,
      maxRestoresPerSession: 1
    } satisfies ReplicaLimits.Values
    const firstReleased = yield* Deferred.make<RestoreProtocol.Released>()
    const secondCompleted = yield* Deferred.make<void>()
    const secondTerminal = yield* Deferred.make<void>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.gen(function*() {
          applications += 1
          if (applications === 2) yield* Deferred.succeed(secondCompleted, undefined)
        })
    }
    return yield* Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const transport = yield* RestoreTransport.RestoreTransport
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, 1)
      const first = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: constrainedLimits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      })
      yield* transport.finish({ sessionId, clientId: 1, nonce: first.nonce })
      first.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          first.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: first.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          complete(firstReleased, event.data)
        }
      })
      first.port.start()
      first.port.postMessage(
        {
          _tag: "Start",
          nonce: first.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      const firstReleasedFrame = yield* Deferred.await(firstReleased)

      const second = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: constrainedLimits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      })
      yield* transport.finish({ sessionId, clientId: 1, nonce: second.nonce })
      second.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(secondTerminal, undefined)
          second.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: second.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          second.port.postMessage(
            {
              _tag: "ReleasedAck",
              nonce: second.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.ReleasedAck
          )
        }
      })
      second.port.start()
      second.port.postMessage(
        {
          _tag: "Start",
          nonce: second.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(secondCompleted)
      yield* Effect.yieldNow

      assert.isFalse(yield* Deferred.isDone(secondTerminal))
      assert.strictEqual(yield* transport.activeControllerCount, 2)

      first.port.postMessage(
        {
          _tag: "ReleasedAck",
          nonce: first.nonce,
          sequence: firstReleasedFrame.sequence
        } satisfies RestoreProtocol.ReleasedAck
      )
      yield* Deferred.await(secondTerminal)
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      first.port.close()
      second.port.close()
    }).pipe(Effect.provide(makeLayer(replica, constrainedLimits)))
  })))

it.effect("gives TerminalAck a full protocol wait after finishing admission", () =>
  Effect.scoped(Effect.gen(function*() {
    const constrainedLimits = {
      ...limits,
      maxSessions: 1,
      maxActiveRestores: 3,
      maxRestoresPerSession: 1,
      maxRestorePullMillis: 1_000
    } satisfies ReplicaLimits.Values
    const firstReleased = yield* Deferred.make<RestoreProtocol.Released>()
    const secondCompleted = yield* Deferred.make<void>()
    const secondTerminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const secondReleased = yield* Deferred.make<RestoreProtocol.Released>()
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.gen(function*() {
          applications += 1
          if (applications === 2) yield* Deferred.succeed(secondCompleted, undefined)
        })
    }
    return yield* Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const transport = yield* RestoreTransport.RestoreTransport
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, 1)
      const first = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: constrainedLimits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      })
      yield* transport.finish({ sessionId, clientId: 1, nonce: first.nonce })
      first.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          first.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: first.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          complete(firstReleased, event.data)
        }
      })
      first.port.start()
      first.port.postMessage(
        {
          _tag: "Start",
          nonce: first.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      const firstReleasedFrame = yield* Deferred.await(firstReleased)

      const second = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: constrainedLimits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      })
      yield* transport.finish({ sessionId, clientId: 1, nonce: second.nonce })
      second.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalReady") {
          complete(secondTerminal, event.data)
        } else if (event.data._tag === "Released") {
          complete(secondReleased, event.data)
        }
      })
      second.port.start()
      second.port.postMessage(
        {
          _tag: "Start",
          nonce: second.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )
      yield* Deferred.await(secondCompleted)

      yield* TestClock.adjust(constrainedLimits.maxRestorePullMillis - 1)
      first.port.postMessage(
        {
          _tag: "ReleasedAck",
          nonce: first.nonce,
          sequence: firstReleasedFrame.sequence
        } satisfies RestoreProtocol.ReleasedAck
      )
      const secondTerminalFrame = yield* Deferred.await(secondTerminal)

      yield* TestClock.adjust(2)
      assert.strictEqual(yield* transport.activeControllerCount, 1)
      second.port.postMessage(
        {
          _tag: "TerminalAck",
          nonce: second.nonce,
          sequence: secondTerminalFrame.sequence
        } satisfies RestoreProtocol.TerminalAck
      )
      const secondReleasedFrame = yield* Deferred.await(secondReleased)
      second.port.postMessage(
        {
          _tag: "ReleasedAck",
          nonce: second.nonce,
          sequence: secondReleasedFrame.sequence
        } satisfies RestoreProtocol.ReleasedAck
      )
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(applications, 2)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      first.port.close()
      second.port.close()
    }).pipe(Effect.provide(makeLayer(replica, constrainedLimits)))
  })))

it.effect("releases admission when nonce generation fails and accepts a later Begin", () =>
  Effect.scoped(Effect.gen(function*() {
    const nativeCrypto = yield* Crypto.Crypto.pipe(Effect.provide(NodeCrypto.layer))
    let nonceAttempts = 0
    const flakyCrypto: Crypto.Crypto = {
      ...nativeCrypto,
      randomUUIDv4: Effect.suspend(() => {
        nonceAttempts += 1
        if (nonceAttempts === 1) {
          return Effect.fail(PlatformError.systemError({
            _tag: "Unknown",
            module: "RestoreTransportTest",
            method: "randomUUIDv4",
            description: "injected nonce failure"
          }))
        }
        return Effect.succeed("00000000-0000-4000-8000-000000000002")
      })
    }
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.void
    }

    return yield* Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const transport = yield* RestoreTransport.RestoreTransport
      const sessionId = Identity.SessionId.make("ses_51f42f25-7399-4c19-b1da-8ef927ac8640")
      yield* sessions.open(sessionId, 1)
      const failed = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      }).pipe(Effect.exit)

      assert.isTrue(Exit.isFailure(failed))
      assert.strictEqual(yield* transport.activeControllerCount, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)

      const started = yield* transport.begin({
        sessionId,
        clientId: 1,
        mode: "replace",
        maxBytes: limits.maxBackupBytes,
        expectedDefinitionHash: "test-definition",
        installationId
      })
      const result = yield* transport.finish({
        sessionId,
        clientId: 1,
        nonce: started.nonce
      })
      started.port.addEventListener(
        "message",
        (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
          if (event.data._tag === "TerminalReady") {
            started.port.postMessage(
              {
                _tag: "TerminalAck",
                nonce: started.nonce,
                sequence: event.data.sequence
              } satisfies RestoreProtocol.TerminalAck
            )
          } else if (event.data._tag === "Released") {
            started.port.postMessage(
              {
                _tag: "ReleasedAck",
                nonce: started.nonce,
                sequence: event.data.sequence
              } satisfies RestoreProtocol.ReleasedAck
            )
          }
        }
      )
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const resultExit = yield* Deferred.await(result).pipe(Effect.exit)
      assert.isTrue(Exit.isSuccess(resultExit))
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(nonceAttempts, 2)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(
      Effect.provide(
        makeLayer(
          replica,
          limits,
          Layer.succeed(Crypto.Crypto, flakyCrypto)
        )
      )
    )
  })))

it.effect("rejects Begin and closes both endpoints when shutdown wins during channel creation", () =>
  Effect.scoped(Effect.gen(function*() {
    let applications = 0
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Effect.sync(() => {
          applications += 1
        })
    }
    const sessionScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
    const sessionContext = yield* Layer.buildWithScope(
      SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits))),
      sessionScope
    )
    const sessions = Context.get(sessionContext, SessionManager.SessionManager)
    const transportScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(transportScope, Exit.void))
    const transportContext = yield* Layer.buildWithScope(
      RestoreTransport.freshLayer.pipe(
        Layer.provide(Layer.mergeAll(
          NodeCrypto.layer,
          Layer.succeed(SessionManager.SessionManager, sessions),
          Layer.succeed(Replica.Replica, replica)
        ))
      ),
      transportScope
    )
    const transport = Context.get(transportContext, RestoreTransport.RestoreTransport)
    const sessionId = Identity.SessionId.make("ses_5b7ffcc0-5c4e-471a-a0bc-7d41896452dd")
    yield* sessions.open(sessionId, 1)

    const NativeMessageChannel = globalThis.MessageChannel
    let ownerCloseCount = 0
    let peerCloseCount = 0
    let shutdownFiber: Fiber.Fiber<void> | undefined
    class ShutdownMessageChannel {
      readonly port1: MessagePort
      readonly port2: MessagePort
      constructor() {
        const native = new NativeMessageChannel()
        this.port1 = native.port1
        this.port2 = native.port2
        const ownerClose = this.port1.close.bind(this.port1)
        this.port1.close = () => {
          ownerCloseCount += 1
          ownerClose()
        }
        const peerClose = this.port2.close.bind(this.port2)
        this.port2.close = () => {
          peerCloseCount += 1
          peerClose()
        }
        shutdownFiber = Effect.runFork(Scope.close(transportScope, Exit.void))
      }
    }
    installMessageChannel(ShutdownMessageChannel)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        globalThis.MessageChannel = NativeMessageChannel
      })
    )

    const exit = yield* transport.begin({
      sessionId,
      clientId: 1,
      mode: "replace",
      maxBytes: limits.maxBackupBytes,
      expectedDefinitionHash: "test-definition",
      installationId
    }).pipe(Effect.exit)
    if (shutdownFiber !== undefined) yield* Fiber.await(shutdownFiber)
    if (Exit.isSuccess(exit)) exit.value.port.close()

    assert.isTrue(Exit.isFailure(exit))
    assert.strictEqual(ownerCloseCount, 1)
    assert.strictEqual(peerCloseCount, 1)
    assert.strictEqual(yield* transport.activeControllerCount, 0)
    assert.strictEqual(applications, 0)
    assert.strictEqual(yield* sessions.activeRestoreCount, 0)
  })))

it.effect("closes the owner endpoint exactly once when a malformed frame shuts down the controller", () =>
  Effect.scoped(
    Effect.gen(function*() {
      const NativeMessageChannel = globalThis.MessageChannel
      let ownerCloseCount = 0
      class CountingMessageChannel {
        readonly port1: MessagePort
        readonly port2: MessagePort
        constructor() {
          const native = new NativeMessageChannel()
          this.port1 = native.port1
          const close = this.port1.close.bind(this.port1)
          this.port1.close = () => {
            ownerCloseCount += 1
            close()
          }
          this.port2 = native.port2
        }
      }
      installMessageChannel(CountingMessageChannel)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.MessageChannel = NativeMessageChannel
        })
      )

      const { sessions, started, transport } = yield* beginUnclaimed
      started.port.postMessage({ _tag: "Malformed" })
      yield* waitForControllerCount(transport, 0)

      assert.strictEqual(ownerCloseCount, 1)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(fixtureReplica)))
  ))

it.effect("stops ingress before joining restore work during Layer shutdown", () =>
  Effect.scoped(Effect.gen(function*() {
    const consumed = yield* Deferred.make<void>()
    const pullSeen = yield* Deferred.make<void>()
    const cleaned = yield* Deferred.make<void>()
    const block = yield* Deferred.make<void>()
    const closed = yield* Deferred.make<void>()
    const closingStarted = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) =>
        options.source.pipe(
          Stream.runHead,
          Effect.andThen(
            Deferred.succeed(consumed, undefined).pipe(
              Effect.andThen(Deferred.await(block)),
              Effect.uninterruptible
            )
          ),
          Effect.ensuring(Deferred.succeed(cleaned, undefined))
        )
    }
    const sessionScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
    const sessionContext = yield* Layer.buildWithScope(
      SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits))),
      sessionScope
    )
    const sessions = Context.get(sessionContext, SessionManager.SessionManager)
    const transportScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(transportScope, Exit.void))
    const transportContext = yield* Layer.buildWithScope(
      RestoreTransport.freshLayer.pipe(
        Layer.provide(Layer.mergeAll(
          NodeCrypto.layer,
          Layer.succeed(SessionManager.SessionManager, sessions),
          Layer.succeed(Replica.Replica, replica)
        ))
      ),
      transportScope
    )
    const transport = Context.get(transportContext, RestoreTransport.RestoreTransport)
    const sessionId = Identity.SessionId.make("ses_4ad94798-713c-47c9-8150-0ee6014f73a9")
    yield* sessions.open(sessionId, 1)
    const started = yield* transport.begin({
      sessionId,
      clientId: 1,
      mode: "replace",
      maxBytes: limits.maxBackupBytes,
      expectedDefinitionHash: "test-definition",
      installationId
    })
    yield* transport.finish({ sessionId, clientId: 1, nonce: started.nonce })
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag !== "Pull") return
      complete(pullSeen, undefined)
      const bytes = Uint8Array.of(1)
      started.port.postMessage(
        {
          _tag: "Chunk",
          nonce: started.nonce,
          sequence: event.data.sequence,
          bytes
        } satisfies RestoreProtocol.Chunk,
        [bytes.buffer]
      )
    })
    started.port.addEventListener("close", () => {
      complete(closed, undefined)
    })
    started.port.start()
    started.port.postMessage(
      {
        _tag: "Start",
        nonce: started.nonce,
        sequence: 0
      } satisfies RestoreProtocol.Start
    )
    yield* Deferred.await(pullSeen)
    yield* Deferred.await(consumed)

    const closing = yield* Deferred.succeed(closingStarted, undefined).pipe(
      Effect.andThen(Scope.close(transportScope, Exit.void)),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(closingStarted)
    assert.strictEqual(yield* transport.activeControllerCount, 1)
    yield* Deferred.await(closed)
    assert.isFalse(yield* Deferred.isDone(cleaned))
    yield* Deferred.succeed(block, undefined)
    yield* Deferred.await(cleaned)
    yield* Fiber.join(closing)
    assert.strictEqual(yield* transport.activeControllerCount, 0)
    assert.strictEqual(yield* sessions.activeRestoreCount, 0)
    started.port.close()
  })))

it.effect("lets the winning TerminalAck handshake finish during Layer shutdown", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalReady>()
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    const closed = yield* Deferred.make<void>()
    const shutdownEntered = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.void
    }
    const sessionScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
    const sessionContext = yield* Layer.buildWithScope(
      SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits))),
      sessionScope
    )
    const sessions = Context.get(sessionContext, SessionManager.SessionManager)
    const transportScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(transportScope, Exit.void))
    const transportContext = yield* Layer.buildWithScope(
      RestoreTransport.freshLayer.pipe(
        Layer.provide(Layer.mergeAll(
          NodeCrypto.layer,
          Layer.succeed(SessionManager.SessionManager, sessions),
          Layer.succeed(Replica.Replica, replica)
        ))
      ),
      transportScope
    )
    yield* Scope.addFinalizer(transportScope, Deferred.succeed(shutdownEntered, undefined))
    const transport = Context.get(transportContext, RestoreTransport.RestoreTransport)
    const sessionId = Identity.SessionId.make("ses_370eb93d-027f-4a7a-9885-966dbc08fce7")
    yield* sessions.open(sessionId, 1)
    const started = yield* transport.begin({
      sessionId,
      clientId: 1,
      mode: "replace",
      maxBytes: limits.maxBackupBytes,
      expectedDefinitionHash: "test-definition",
      installationId
    })
    yield* transport.finish({ sessionId, clientId: 1, nonce: started.nonce })
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag === "TerminalReady") {
        complete(terminal, event.data)
      } else if (event.data._tag === "Released") {
        complete(released, event.data)
      }
    })
    started.port.addEventListener("close", () => {
      complete(closed, undefined)
    })
    started.port.start()
    started.port.postMessage(
      {
        _tag: "Start",
        nonce: started.nonce,
        sequence: 0
      } satisfies RestoreProtocol.Start
    )
    const terminalFrame = yield* Deferred.await(terminal)

    const closing = yield* Scope.close(transportScope, Exit.void).pipe(
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(shutdownEntered)
    assert.isFalse(yield* Deferred.isDone(closed))
    assert.strictEqual(yield* transport.activeControllerCount, 1)
    started.port.postMessage(
      {
        _tag: "TerminalAck",
        nonce: started.nonce,
        sequence: terminalFrame.sequence
      } satisfies RestoreProtocol.TerminalAck
    )
    const releasedFrame = yield* Deferred.await(released)
    started.port.postMessage(
      {
        _tag: "ReleasedAck",
        nonce: started.nonce,
        sequence: releasedFrame.sequence
      } satisfies RestoreProtocol.ReleasedAck
    )
    yield* Fiber.join(closing)
    assert.strictEqual(yield* transport.activeControllerCount, 0)
    assert.strictEqual(yield* sessions.activeRestoreCount, 0)
    started.port.close()
  })))

it.effect("lets the winning ReleasedAck handshake finish during Layer shutdown", () =>
  Effect.scoped(Effect.gen(function*() {
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    const closed = yield* Deferred.make<void>()
    const shutdownEntered = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.void
    }
    const sessionScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
    const sessionContext = yield* Layer.buildWithScope(
      SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits))),
      sessionScope
    )
    const sessions = Context.get(sessionContext, SessionManager.SessionManager)
    const transportScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(transportScope, Exit.void))
    const transportContext = yield* Layer.buildWithScope(
      RestoreTransport.freshLayer.pipe(
        Layer.provide(Layer.mergeAll(
          NodeCrypto.layer,
          Layer.succeed(SessionManager.SessionManager, sessions),
          Layer.succeed(Replica.Replica, replica)
        ))
      ),
      transportScope
    )
    yield* Scope.addFinalizer(transportScope, Deferred.succeed(shutdownEntered, undefined))
    const transport = Context.get(transportContext, RestoreTransport.RestoreTransport)
    const sessionId = Identity.SessionId.make("ses_f3f159dc-8098-4c83-9616-a0715a5229f3")
    yield* sessions.open(sessionId, 1)
    const started = yield* transport.begin({
      sessionId,
      clientId: 1,
      mode: "replace",
      maxBytes: limits.maxBackupBytes,
      expectedDefinitionHash: "test-definition",
      installationId
    })
    yield* transport.finish({ sessionId, clientId: 1, nonce: started.nonce })
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag === "TerminalReady") {
        started.port.postMessage(
          {
            _tag: "TerminalAck",
            nonce: started.nonce,
            sequence: event.data.sequence
          } satisfies RestoreProtocol.TerminalAck
        )
      } else if (event.data._tag === "Released") {
        complete(released, event.data)
      }
    })
    started.port.addEventListener("close", () => {
      complete(closed, undefined)
    })
    started.port.start()
    started.port.postMessage(
      {
        _tag: "Start",
        nonce: started.nonce,
        sequence: 0
      } satisfies RestoreProtocol.Start
    )
    const releasedFrame = yield* Deferred.await(released)

    const closing = yield* Scope.close(transportScope, Exit.void).pipe(
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(shutdownEntered)
    assert.isFalse(yield* Deferred.isDone(closed))
    assert.strictEqual(yield* transport.activeControllerCount, 1)
    started.port.postMessage(
      {
        _tag: "ReleasedAck",
        nonce: started.nonce,
        sequence: releasedFrame.sequence
      } satisfies RestoreProtocol.ReleasedAck
    )
    yield* Fiber.join(closing)
    assert.strictEqual(yield* transport.activeControllerCount, 0)
    assert.strictEqual(yield* sessions.activeRestoreCount, 0)
    started.port.close()
  })))
