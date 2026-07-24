import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
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
  maxActiveRestores: 2,
  maxRestoresPerSession: 1,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
} satisfies ReplicaLimits.Values

const installationId = Identity.BackupInstallationId.make("bak_ea5a1250-2d04-4c92-987a-e62d411a0b4e")

const waitForControllerCount = (
  transport: RestoreTransport.RestoreTransport["Service"],
  expected: number
): Effect.Effect<void> =>
  Effect.suspend(() =>
    transport.activeControllerCount.pipe(
      Effect.flatMap((count) =>
        count === expected
          ? Effect.void
          : Effect.yieldNow.pipe(Effect.andThen(waitForControllerCount(transport, expected)))
      )
    )
  )

const makeLayer = (
  replica: Replica.Replica["Service"],
  configuredLimits: ReplicaLimits.Values = limits
) => {
  const sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(configuredLimits)))
  const dependencies = Layer.mergeAll(
    NodeCrypto.layer,
    sessions,
    Layer.succeed(Replica.Replica, replica)
  )
  return Layer.merge(
    dependencies,
    RestoreTransport.freshLayer.pipe(Layer.provide(dependencies))
  )
}

const begin = Effect.gen(function*() {
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

it.effect("does not export the internal RestoreTransport package subpath", () =>
  Effect.sync(() => {
    let error: unknown
    try {
      import.meta.resolve("@lucas-barake/effect-local-browser/RestoreTransport")
    } catch (cause) {
      error = cause
    }
    assert.instanceOf(error, Error)
    assert.strictEqual(
      error instanceof Error && "code" in error ? error.code : undefined,
      "ERR_PACKAGE_PATH_NOT_EXPORTED"
    )
  }))

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
        } else if (
          frame._tag === "TerminalSuccess" ||
          frame._tag === "TerminalSessionFailure" ||
          frame._tag === "TerminalRestoreFailure" ||
          frame._tag === "TerminalDefect"
        ) {
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
          Effect.runSync(Deferred.succeed(released, undefined))
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
      assert.deepStrictEqual(Array.from(chunks[0]!), [1, 2, 3, 4])
      assert.strictEqual(chunks[0]!.byteOffset, 0)
      assert.strictEqual(chunks[0]!.buffer.byteLength, chunks[0]!.byteLength)
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
    const outcome = yield* Deferred.make<
      | { readonly _tag: "Closed" }
      | { readonly _tag: "Terminal"; readonly frame: RestoreProtocol.TerminalRestoreFailure }
    >()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: (options) => Stream.runDrain(options.source)
    }
    return yield* Effect.gen(function*() {
      const { started, transport } = yield* begin
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
        } else if (frame._tag === "TerminalRestoreFailure") {
          Effect.runSync(Deferred.succeed(outcome, { _tag: "Terminal", frame }))
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
      started.port.addEventListener("close", () => {
        Effect.runSync(Deferred.succeed(outcome, { _tag: "Closed" }))
      })
      started.port.start()
      started.port.postMessage(
        {
          _tag: "Start",
          nonce: started.nonce,
          sequence: 0
        } satisfies RestoreProtocol.Start
      )

      const result = yield* Deferred.await(outcome)
      assert.strictEqual(result._tag, "Terminal")
      if (result._tag === "Terminal") {
        assert.strictEqual(result.frame.error._tag, "UnsupportedDocumentVersion")
      }
      yield* waitForControllerCount(transport, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica, minimumLimits)))
  })))

it.effect("reports session expiry before Start as an authoritative session failure", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<
      RestoreProtocol.TerminalSessionFailure | RestoreProtocol.TerminalRestoreFailure
    >()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.die(new Error("restore must not start before Start"))
    }
    return yield* Effect.gen(function*() {
      const { sessionId, sessions, started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        const frame = event.data
        if (frame._tag === "TerminalSessionFailure" || frame._tag === "TerminalRestoreFailure") {
          Effect.runSync(Deferred.succeed(terminal, frame))
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
      assert.strictEqual(frame._tag, "TerminalSessionFailure")
      assert.strictEqual(frame.sequence, 1)
      assert.strictEqual(frame.error._tag, "ProtocolMismatch")
      yield* waitForControllerCount(transport, 0)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("reserves an outstanding Pull sequence before posting the authoritative terminal", () =>
  Effect.scoped(Effect.gen(function*() {
    const pullSeen = yield* Deferred.make<RestoreProtocol.Pull>()
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalSuccess>()
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
          Effect.runSync(Deferred.succeed(pullSeen, frame))
        } else if (frame._tag === "TerminalSuccess") {
          Effect.runSync(Deferred.succeed(terminal, frame))
          started.port.postMessage(
            {
              _tag: "End",
              nonce: started.nonce,
              sequence: 1
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
          Effect.runSync(Deferred.succeed(released, undefined))
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
      reason: new ReplicaError.RestoreFailed({ cause: new Error("typed failure") })
    })
    const workStarted = yield* Deferred.make<void>()
    const terminal = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () =>
        Deferred.succeed(workStarted, undefined).pipe(
          Effect.andThen(
            Effect.failCause(Cause.combine(Cause.fail(typed), Cause.die(new Error("winning defect"))))
          )
        )
    }
    return yield* Effect.gen(function*() {
      const { started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (
          event.data._tag === "TerminalSuccess" ||
          event.data._tag === "TerminalSessionFailure" ||
          event.data._tag === "TerminalRestoreFailure" ||
          event.data._tag === "TerminalDefect"
        ) {
          Effect.runSync(Deferred.succeed(terminal, undefined))
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
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.isFalse(yield* Deferred.isDone(terminal))
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("does not collapse two typed failures in a composite restore Cause", () =>
  Effect.scoped(Effect.gen(function*() {
    const first = new ReplicaError.ReplicaError({
      reason: new ReplicaError.RestoreFailed({ cause: new Error("first failure") })
    })
    const second = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({ cause: new Error("second failure") })
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
      const { started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (
          event.data._tag === "TerminalSuccess" ||
          event.data._tag === "TerminalSessionFailure" ||
          event.data._tag === "TerminalRestoreFailure" ||
          event.data._tag === "TerminalDefect"
        ) {
          Effect.runSync(Deferred.succeed(terminal, undefined))
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
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.isFalse(yield* Deferred.isDone(terminal))
      started.port.close()
    }).pipe(Effect.provide(makeLayer(replica)))
  })))

it.effect("preserves interruption over a typed failure in a composite restore Cause", () =>
  Effect.scoped(Effect.gen(function*() {
    const typed = new ReplicaError.ReplicaError({
      reason: new ReplicaError.RestoreFailed({ cause: new Error("typed failure") })
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
      const { started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (
          event.data._tag === "TerminalSuccess" ||
          event.data._tag === "TerminalSessionFailure" ||
          event.data._tag === "TerminalRestoreFailure" ||
          event.data._tag === "TerminalDefect"
        ) {
          Effect.runSync(Deferred.succeed(terminal, undefined))
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
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      yield* waitForControllerCount(transport, 0)
      assert.isFalse(yield* Deferred.isDone(terminal))
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
        Effect.runSync(Deferred.succeed(closed, undefined))
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
        Effect.runSync(Deferred.succeed(closed, undefined))
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
        } else if (event.data._tag === "TerminalSuccess") {
          Effect.runSync(Deferred.succeed(outcome, "Terminal"))
        }
      })
      started.port.addEventListener("close", () => {
        Effect.runSync(Deferred.succeed(outcome, "Close"))
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

it.effect("gives ReleasedAck its own wait deadline after a near deadline TerminalAck", () =>
  Effect.scoped(Effect.gen(function*() {
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalSuccess>()
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    const closed = yield* Deferred.make<void>()
    const replica: Replica.Replica["Service"] = {
      ...fixtureReplica,
      restoreBackup: () => Effect.void
    }
    return yield* Effect.gen(function*() {
      const { started, transport } = yield* begin
      started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalSuccess") {
          Effect.runSync(Deferred.succeed(terminal, event.data))
        } else if (event.data._tag === "Released") {
          Effect.runSync(Deferred.succeed(released, event.data))
        }
      })
      started.port.addEventListener("close", () => {
        Effect.runSync(Deferred.succeed(closed, undefined))
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
      first.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalSuccess") {
          first.port.postMessage(
            {
              _tag: "TerminalAck",
              nonce: first.nonce,
              sequence: event.data.sequence
            } satisfies RestoreProtocol.TerminalAck
          )
        } else if (event.data._tag === "Released") {
          Effect.runSync(Deferred.succeed(firstReleased, event.data))
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
      second.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
        if (event.data._tag === "TerminalSuccess") {
          Effect.runSync(Deferred.succeed(secondTerminal, undefined))
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
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag !== "Pull") return
      Effect.runSync(Deferred.succeed(pullSeen, undefined))
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
      Effect.runSync(Deferred.succeed(closed, undefined))
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
    const terminal = yield* Deferred.make<RestoreProtocol.TerminalSuccess>()
    const released = yield* Deferred.make<RestoreProtocol.Released>()
    const closed = yield* Deferred.make<void>()
    const closingStarted = yield* Deferred.make<void>()
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
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag === "TerminalSuccess") {
        Effect.runSync(Deferred.succeed(terminal, event.data))
      } else if (event.data._tag === "Released") {
        Effect.runSync(Deferred.succeed(released, event.data))
      }
    })
    started.port.addEventListener("close", () => {
      Effect.runSync(Deferred.succeed(closed, undefined))
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

    const closing = yield* Deferred.succeed(closingStarted, undefined).pipe(
      Effect.andThen(Scope.close(transportScope, Exit.void)),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(closingStarted)
    yield* Effect.yieldNow
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
    const closingStarted = yield* Deferred.make<void>()
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
    started.port.addEventListener("message", (event: MessageEvent<RestoreProtocol.OwnerToPageFrame>) => {
      if (event.data._tag === "TerminalSuccess") {
        started.port.postMessage(
          {
            _tag: "TerminalAck",
            nonce: started.nonce,
            sequence: event.data.sequence
          } satisfies RestoreProtocol.TerminalAck
        )
      } else if (event.data._tag === "Released") {
        Effect.runSync(Deferred.succeed(released, event.data))
      }
    })
    started.port.addEventListener("close", () => {
      Effect.runSync(Deferred.succeed(closed, undefined))
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

    const closing = yield* Deferred.succeed(closingStarted, undefined).pipe(
      Effect.andThen(Scope.close(transportScope, Exit.void)),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(closingStarted)
    yield* Effect.yieldNow
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
