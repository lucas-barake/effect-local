import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as RestoreProtocol from "../src/internal/restoreProtocol.js"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import { definition } from "./fixtures.js"

const installationId = Identity.BackupInstallationId.make("bak_43c8d2f4-58ce-4c9a-9155-9d21019f5e9d")
const nonce = RestoreProtocol.RestoreNonce.make("rst_43c8d2f4-58ce-4c9a-9155-9d21019f5e9d")
const sequence = RestoreProtocol.RestoreSequence.make

const rpcClient = (
  begin: () => Effect.Effect<{
    readonly nonce: RestoreProtocol.RestoreNonce
    readonly port: MessagePort
  }>,
  maxRestoreErrorBytes = 4_096,
  maxChunkBytes = 4,
  finish: () => Effect.Effect<void, RestoreProtocol.RestoreResultFailure> = () => Effect.void
) =>
  ({
    OpenSession: () =>
      Effect.succeed({
        leaseMillis: 10_000,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash,
        ownerEpoch: "owner",
        maxChunkBytes,
        maxRestoreCoalesceMillis: 25,
        maxRestoreErrorBytes
      }),
    RenewSession: () => Effect.succeed({ leaseMillis: 10_000 }),
    CloseSession: () => Effect.void,
    BeginRestoreBackup: begin,
    FinishRestoreBackup: finish
  }) as unknown as RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>

it.layer(NodeCrypto.layer)("RestoreClientProtocol", (it) => {
  it.effect("coalesces within the advertised bound without transferring caller storage", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const completed = yield* Deferred.make<void>()
      const received: Array<Uint8Array> = []
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        try {
          const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
          switch (frame._tag) {
            case "Start":
              channel.port2.postMessage(
                Schema.encodeSync(RestoreProtocol.Pull)({
                  _tag: "Pull",
                  nonce,
                  sequence: sequence(1)
                })
              )
              return
            case "Chunk":
              assert.strictEqual(frame.bytes.byteLength, frame.bytes.buffer.byteLength)
              assert.strictEqual(frame.bytes.byteOffset, 0)
              assert.isAbove(frame.bytes.byteLength, 0)
              assert.isAtMost(frame.bytes.byteLength, 4)
              received.push(new Uint8Array(frame.bytes))
              channel.port2.postMessage(
                Schema.encodeSync(RestoreProtocol.Pull)({
                  _tag: "Pull",
                  nonce,
                  sequence: sequence(frame.sequence + 1)
                })
              )
              return
            case "End":
              channel.port2.postMessage(
                Schema.encodeSync(RestoreProtocol.TerminalReady)({
                  _tag: "TerminalReady",
                  nonce,
                  sequence: sequence(frame.sequence + 1)
                })
              )
              return
            case "TerminalAck":
              channel.port2.postMessage(
                Schema.encodeSync(RestoreProtocol.Released)({
                  _tag: "Released",
                  nonce,
                  sequence: sequence(frame.sequence + 1)
                })
              )
              return
            case "ReleasedAck":
              Deferred.doneUnsafe(completed, Effect.void)
              return
            case "SourceFailure":
              Deferred.doneUnsafe(
                completed,
                Effect.die(new Error(`unexpected source failure: ${frame.error._tag}`))
              )
          }
        } catch (cause) {
          Deferred.doneUnsafe(completed, Effect.die(cause))
        }
      })
      channel.port2.start()

      const backing = Uint8Array.of(99, 1, 2, 3, 88)
      const view = backing.subarray(1, 4)
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
      )
      yield* client.restoreBackup({
        source: Stream.fromIterable([
          view,
          new Uint8Array(0),
          Uint8Array.of(4),
          Uint8Array.of(5, 6, 7)
        ]),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      })
      yield* Deferred.await(completed)

      assert.deepStrictEqual(received.map((chunk) => Array.from(chunk)), [
        [1, 2, 3, 4],
        [5, 6, 7]
      ])
      assert.strictEqual(backing.buffer.byteLength, 5)
      assert.deepStrictEqual(Array.from(backing), [99, 1, 2, 3, 88])
    })))

  it.effect("does not allocate the advertised chunk limit for an empty source", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "End") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(2)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(3)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          Number.MAX_SAFE_INTEGER
        )
      )

      yield* client.restoreBackup({
        source: Stream.empty,
        mode: "replace",
        maxBytes: Number.MAX_SAFE_INTEGER,
        expectedDefinitionHash: definition.hash,
        installationId
      })
    })))

  it.effect("does not acknowledge TerminalReady until the Finish result is accepted", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const finishStarted = yield* Deferred.make<void>()
      const completeFinish = yield* Deferred.make<void>()
      const terminalSent = yield* Deferred.make<void>()
      const terminalAck = yield* Deferred.make<void>()
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(1)
            })
          )
          Deferred.doneUnsafe(terminalSent, Effect.void)
        } else if (frame._tag === "TerminalAck") {
          Deferred.doneUnsafe(terminalAck, Effect.void)
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(2)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () =>
            Deferred.succeed(finishStarted, undefined).pipe(
              Effect.andThen(Deferred.await(completeFinish))
            )
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(finishStarted)
      yield* Deferred.await(terminalSent)
      yield* Effect.yieldNow
      assert.isFalse(yield* Deferred.isDone(terminalAck))
      yield* Deferred.succeed(completeFinish, undefined)
      yield* Deferred.await(terminalAck)
      yield* Fiber.join(restore)
    })))

  it.effect("fails closed on an out of order owner frame", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag !== "Start") return
        channel.port2.postMessage(
          Schema.encodeSync(RestoreProtocol.Pull)({
            _tag: "Pull",
            nonce,
            sequence: sequence(2)
          })
        )
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const error = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.observed, "invalid restore owner sequence")
      }
    })))

  it.effect("rejects excess binary fields before pulling the source", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const sourcePulled = yield* Deferred.make<void>()
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag !== "Start") return
        channel.port2.postMessage({
          _tag: "Pull",
          nonce,
          sequence: sequence(1),
          excess: new ArrayBuffer(1)
        })
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Deferred.succeed(sourcePulled, undefined).pipe(Effect.as(Uint8Array.of(1)))
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild)

      const outcome = yield* Effect.raceFirst(
        Fiber.await(restore).pipe(Effect.map((exit) => ({ _tag: "Closed" as const, exit }))),
        Deferred.await(sourcePulled).pipe(Effect.as({ _tag: "Pulled" as const }))
      )
      assert.strictEqual(outcome._tag, "Closed")
      if (outcome._tag === "Closed") {
        assert.isTrue(Exit.isFailure(outcome.exit))
        if (Exit.isFailure(outcome.exit)) {
          const error = Cause.findErrorOption(outcome.exit.cause)
          assert.isTrue(error._tag === "Some")
          if (error._tag === "Some") assert.strictEqual(error.value.reason._tag, "ProtocolMismatch")
        }
      }
    })))

  it.effect("surfaces a retained source pull defect without another credit", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const pendingStarted = yield* Deferred.make<void>()
      const releaseDefect = yield* Deferred.make<void>()
      const firstChunk = yield* Deferred.make<void>()
      const sentinel = new Error("retained source pull defect")
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "Chunk") {
          assert.deepStrictEqual(Array.from(frame.bytes), [1])
          Deferred.doneUnsafe(firstChunk, Effect.void)
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const source = Stream.make(Uint8Array.of(1)).pipe(
        Stream.concat(Stream.fromEffect(
          Deferred.succeed(pendingStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseDefect)),
            Effect.andThen(Effect.die(sentinel))
          )
        ))
      )
      const restore = yield* client.restoreBackup({
        source,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild)

      yield* Deferred.await(pendingStarted)
      yield* TestClock.adjust(25)
      yield* Deferred.await(firstChunk)
      yield* Deferred.succeed(releaseDefect, undefined)
      const exit = yield* Fiber.await(restore)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), sentinel)
    })))

  it.effect("preserves a retained source pull typed failure combined with a defect", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const pendingStarted = yield* Deferred.make<void>()
      const releaseFailure = yield* Deferred.make<void>()
      const firstChunk = yield* Deferred.make<void>()
      const sentinel = new Error("retained composite source defect")
      const sourceFailure = new ReplicaError.ReplicaError({
        reason: new ReplicaError.RestoreBusy({ replica: "retained" })
      })
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "Chunk") {
          assert.deepStrictEqual(Array.from(frame.bytes), [1])
          Deferred.doneUnsafe(firstChunk, Effect.void)
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const composite = Cause.fromReasons([
        Cause.makeFailReason(sourceFailure),
        Cause.makeDieReason(sentinel)
      ])
      const source = Stream.make(Uint8Array.of(1)).pipe(
        Stream.concat(Stream.fromEffect(
          Deferred.succeed(pendingStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFailure)),
            Effect.andThen(Effect.failCause(composite))
          )
        ))
      )
      const restore = yield* client.restoreBackup({
        source,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild)

      yield* Deferred.await(pendingStarted)
      yield* TestClock.adjust(25)
      yield* Deferred.await(firstChunk)
      yield* Deferred.succeed(releaseFailure, undefined)
      const exit = yield* Fiber.await(restore)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map((reason) => reason._tag), ["Fail", "Die"])
        assert.strictEqual(Cause.findErrorOption(exit.cause)._tag, "Some")
        const defect = exit.cause.reasons.find(Cause.isDieReason)
        assert.strictEqual(defect?.defect, sentinel)
      }
    })))

  it.effect("finalizes the transferred port, source pull, and Finish work when interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      const peerClosed = yield* Deferred.make<void>()
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") Deferred.doneUnsafe(startReceived, Effect.void)
      })
      channel.port2.addEventListener("close", () => {
        Deferred.doneUnsafe(peerClosed, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () =>
            Deferred.succeed(finishStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const source = Stream.fromEffect(
        Deferred.succeed(sourceStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
        )
      )
      const restore = yield* client.restoreBackup({
        source,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      yield* Fiber.interrupt(restore)

      yield* Deferred.await(peerClosed)
      yield* Effect.yieldNow
      assert.isTrue(yield* Deferred.isDone(finishFinalized))
      assert.isTrue(yield* Deferred.isDone(sourceFinalized))
    })))

  it.effect("times out pending Finish work after TerminalReady and releases the source and port", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      const peerClosed = yield* Deferred.make<void>()
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") Deferred.doneUnsafe(startReceived, Effect.void)
      })
      channel.port2.addEventListener("close", () => {
        Deferred.doneUnsafe(peerClosed, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () =>
            Deferred.succeed(finishStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        ),
        { operationTimeout: "1 second" }
      )
      const source = Stream.fromEffect(
        Deferred.succeed(sourceStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
        )
      )
      const restore = yield* client.restoreBackup({
        source,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.TerminalReady)({
          _tag: "TerminalReady",
          nonce,
          sequence: sequence(2)
        })
      )
      yield* Effect.yieldNow
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "RestoreBackup")
        assert.strictEqual(error.reason.timeoutMillis, 1_000)
      }
      yield* Deferred.await(peerClosed)
      yield* Deferred.await(sourceFinalized)
      yield* Deferred.await(finishFinalized)
    })))

  it.effect("fails without replay and finalizes pending work on messageerror before TerminalReady", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const releaseSource = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const releaseFinish = yield* Deferred.make<void>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      let beginCalls = 0
      let finishCalls = 0
      let sourcePulls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") Deferred.doneUnsafe(startReceived, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () =>
            Effect.sync(() => {
              finishCalls++
            }).pipe(
              Effect.andThen(Deferred.succeed(finishStarted, undefined)),
              Effect.andThen(Deferred.await(releaseFinish)),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
          }).pipe(
            Effect.andThen(Deferred.succeed(sourceStarted, undefined)),
            Effect.andThen(Deferred.await(releaseSource)),
            Effect.as(Uint8Array.of(1)),
            Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
          )
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      channel.port1.dispatchEvent(new MessageEvent("messageerror"))
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      if (error.reason._tag === "StorageUnavailable") {
        assert.instanceOf(error.reason.cause, Error)
        if (error.reason.cause instanceof Error) {
          assert.strictEqual(error.reason.cause.message, "restore channel message decoding failed")
        }
      }
      yield* Deferred.await(sourceFinalized)
      yield* Deferred.await(finishFinalized)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(2)
        })
      )
      yield* Effect.yieldNow
      assert.strictEqual(sourcePulls, 1)
      assert.strictEqual(finishCalls, 1)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("preserves an authoritative Finish failure when the peer closes before TerminalReady", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const releaseSource = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishResult = yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      let beginCalls = 0
      let finishCalls = 0
      let sourcePulls = 0
      let terminalAckCalls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          Deferred.doneUnsafe(startReceived, Effect.void)
        } else if (frame._tag === "TerminalAck") {
          terminalAckCalls++
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () =>
            Effect.sync(() => {
              finishCalls++
            }).pipe(
              Effect.andThen(Deferred.succeed(finishStarted, undefined)),
              Effect.andThen(Deferred.await(finishResult)),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
          }).pipe(
            Effect.andThen(Deferred.succeed(sourceStarted, undefined)),
            Effect.andThen(Deferred.await(releaseSource)),
            Effect.as(Uint8Array.of(1)),
            Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
          )
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      yield* Deferred.fail(
        finishResult,
        new RestoreProtocol.RestoreResultRestoreFailure({
          error: {
            _tag: "BackupTooLarge",
            limit: 64,
            observed: 65
          }
        })
      )
      yield* Deferred.await(finishFinalized)
      yield* Effect.yieldNow
      channel.port2.close()
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, 64)
        assert.strictEqual(error.reason.observed, 65)
      }
      yield* Deferred.await(sourceFinalized)
      assert.strictEqual(terminalAckCalls, 0)
      assert.strictEqual(sourcePulls, 1)
      assert.strictEqual(finishCalls, 1)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("preserves an authoritative Finish failure when an outstanding source pull defects", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const peerClosed = yield* Deferred.make<void>()
      const releaseSource = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishResult = yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      const sourceDefect = new Error("source failed after authoritative Finish")
      let beginCalls = 0
      let finishCalls = 0
      let sourcePulls = 0
      let terminalAckCalls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          Deferred.doneUnsafe(startReceived, Effect.void)
        } else if (frame._tag === "TerminalAck") {
          terminalAckCalls++
        }
      })
      channel.port2.addEventListener("close", () => {
        Deferred.doneUnsafe(peerClosed, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () =>
            Effect.sync(() => {
              finishCalls++
            }).pipe(
              Effect.andThen(Deferred.succeed(finishStarted, undefined)),
              Effect.andThen(Deferred.await(finishResult)),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
          }).pipe(
            Effect.andThen(Deferred.succeed(sourceStarted, undefined)),
            Effect.andThen(Deferred.await(releaseSource)),
            Effect.andThen(Effect.die(sourceDefect)),
            Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
          )
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      yield* Deferred.fail(
        finishResult,
        new RestoreProtocol.RestoreResultRestoreFailure({
          error: {
            _tag: "BackupTooLarge",
            limit: 64,
            observed: 65
          }
        })
      )
      yield* Deferred.await(finishFinalized)
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseSource, undefined)
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, 64)
        assert.strictEqual(error.reason.observed, 65)
      }
      yield* Deferred.await(sourceFinalized)
      yield* Deferred.await(peerClosed)
      assert.strictEqual(terminalAckCalls, 0)
      assert.strictEqual(sourcePulls, 1)
      assert.strictEqual(finishCalls, 1)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("preserves an accepted Finish failure when the peer closes before Released", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const terminalAckReceived = yield* Deferred.make<void>()
      const releaseSource = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishResult = yield* Deferred.make<void, RestoreProtocol.RestoreResultFailure>()
      const finishStarted = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      let beginCalls = 0
      let finishCalls = 0
      let sourcePulls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          Deferred.doneUnsafe(startReceived, Effect.void)
        } else if (frame._tag === "TerminalAck") {
          Deferred.doneUnsafe(terminalAckReceived, Effect.void)
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () =>
            Effect.sync(() => {
              finishCalls++
            }).pipe(
              Effect.andThen(Deferred.succeed(finishStarted, undefined)),
              Effect.andThen(Deferred.await(finishResult)),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
          }).pipe(
            Effect.andThen(Deferred.succeed(sourceStarted, undefined)),
            Effect.andThen(Deferred.await(releaseSource)),
            Effect.as(Uint8Array.of(1)),
            Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
          )
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      yield* Deferred.await(finishStarted)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.TerminalReady)({
          _tag: "TerminalReady",
          nonce,
          sequence: sequence(2)
        })
      )
      yield* Deferred.await(sourceFinalized)
      yield* Deferred.fail(
        finishResult,
        new RestoreProtocol.RestoreResultRestoreFailure({
          error: {
            _tag: "BackupTooLarge",
            limit: 64,
            observed: 65
          }
        })
      )
      yield* Deferred.await(terminalAckReceived)
      channel.port2.close()
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, 64)
        assert.strictEqual(error.reason.observed, 65)
      }
      yield* Deferred.await(finishFinalized)
      assert.strictEqual(sourcePulls, 1)
      assert.strictEqual(finishCalls, 1)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("fails closed on wrong nonce and wrong phase frames without pulling or replaying", () =>
    Effect.gen(function*() {
      const wrongNonce = RestoreProtocol.RestoreNonce.make(
        "rst_43c8d2f4-58ce-4c9a-9155-9d21019f5e9e"
      )
      const hostileFrames: ReadonlyArray<{
        readonly label: string
        readonly observed: string
        readonly encode: () => unknown
      }> = [
        {
          label: "wrong nonce",
          observed: "invalid restore owner sequence",
          encode: () =>
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce: wrongNonce,
              sequence: sequence(1)
            })
        },
        {
          label: "Released before TerminalReady",
          observed: "unexpected restore release",
          encode: () =>
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(1)
            })
        }
      ]

      for (const hostile of hostileFrames) {
        yield* Effect.scoped(Effect.gen(function*() {
          const channel = new MessageChannel()
          yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
          const peerClosed = yield* Deferred.make<void>()
          let beginCalls = 0
          let sourcePulls = 0
          channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
            const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
            if (frame._tag === "Start") channel.port2.postMessage(hostile.encode())
          })
          channel.port2.addEventListener("close", () => {
            Deferred.doneUnsafe(peerClosed, Effect.void)
          })
          channel.port2.start()
          const client = yield* ReplicaClient.fromRpcClient(
            definition,
            rpcClient(
              () =>
                Effect.sync(() => {
                  beginCalls++
                  return { nonce, port: channel.port1 }
                }),
              4_096,
              4,
              () => Effect.never
            )
          )
          const exit = yield* client.restoreBackup({
            source: Stream.fromEffect(
              Effect.sync(() => {
                sourcePulls++
                return Uint8Array.of(1)
              })
            ),
            mode: "replace",
            maxBytes: 64,
            expectedDefinitionHash: definition.hash,
            installationId
          }).pipe(Effect.exit)

          assert.isTrue(Exit.isFailure(exit), hostile.label)
          if (Exit.isFailure(exit)) {
            const error = Cause.findErrorOption(exit.cause)
            assert.strictEqual(error._tag, "Some", hostile.label)
            if (error._tag === "Some") {
              assert.strictEqual(error.value.reason._tag, "ProtocolMismatch", hostile.label)
              if (error.value.reason._tag === "ProtocolMismatch") {
                assert.strictEqual(error.value.reason.observed, hostile.observed, hostile.label)
              }
            }
          }
          yield* Deferred.await(peerClosed)
          assert.strictEqual(sourcePulls, 0, hostile.label)
          assert.strictEqual(beginCalls, 1, hostile.label)
        }))
      }
    }))

  it.effect("fails closed on a duplicate Pull before a second source pull or replay", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const startReceived = yield* Deferred.make<void>()
      const sourceStarted = yield* Deferred.make<void>()
      const sourceFinalized = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      const peerClosed = yield* Deferred.make<void>()
      let beginCalls = 0
      let sourcePulls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") Deferred.doneUnsafe(startReceived, Effect.void)
      })
      channel.port2.addEventListener("close", () => {
        Deferred.doneUnsafe(peerClosed, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () => Effect.never.pipe(Effect.ensuring(Deferred.succeed(finishFinalized, undefined)))
        )
      )
      const restore = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
          }).pipe(
            Effect.andThen(Deferred.succeed(sourceStarted, undefined)),
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(sourceFinalized, undefined))
          )
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(startReceived)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(1)
        })
      )
      yield* Deferred.await(sourceStarted)
      channel.port2.postMessage(
        Schema.encodeSync(RestoreProtocol.Pull)({
          _tag: "Pull",
          nonce,
          sequence: sequence(2)
        })
      )
      const error = yield* Fiber.join(restore).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.observed, "unsolicited restore pull")
      }
      yield* Deferred.await(peerClosed)
      yield* Deferred.await(sourceFinalized)
      yield* Deferred.await(finishFinalized)
      assert.strictEqual(sourcePulls, 1)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("fails closed on duplicate TerminalReady without another source pull or replay", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const peerClosed = yield* Deferred.make<void>()
      const finishFinalized = yield* Deferred.make<void>()
      let beginCalls = 0
      let sourcePulls = 0
      let terminalAckCalls = 0
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(1)
            })
          )
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(2)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          terminalAckCalls++
        }
      })
      channel.port2.addEventListener("close", () => {
        Deferred.doneUnsafe(peerClosed, Effect.void)
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: channel.port1 }
            }),
          4_096,
          4,
          () =>
            Deferred.await(peerClosed).pipe(
              Effect.andThen(
                Effect.fail(
                  new RestoreProtocol.RestoreResultRestoreFailure({
                    error: {
                      _tag: "ProtocolMismatch",
                      expected: "single TerminalReady",
                      observed: "duplicate TerminalReady"
                    }
                  })
                )
              ),
              Effect.ensuring(Deferred.succeed(finishFinalized, undefined))
            )
        )
      )
      const error = yield* client.restoreBackup({
        source: Stream.fromEffect(
          Effect.sync(() => {
            sourcePulls++
            return Uint8Array.of(1)
          })
        ),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.observed, "duplicate TerminalReady")
      }
      assert.isTrue(yield* Deferred.isDone(peerClosed))
      assert.isTrue(yield* Deferred.isDone(finishFinalized))
      assert.strictEqual(sourcePulls, 0)
      assert.strictEqual(terminalAckCalls, 0)
      assert.strictEqual(beginCalls, 1)
    })))

  it.effect("preserves a source failure combined with a defect", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const sentinel = new Error("composite source defect")
      const sourceFailure = new ReplicaError.ReplicaError({
        reason: new ReplicaError.RestoreBusy({ replica: "test" })
      })
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "SourceFailure") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(2)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(3)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const composite = Cause.fromReasons([
        Cause.makeFailReason(sourceFailure),
        Cause.makeDieReason(sentinel)
      ])
      const exit = yield* client.restoreBackup({
        source: Stream.fromPull(
          Effect.succeed(Effect.failCause(composite))
        ) as Stream.Stream<Uint8Array, ReplicaError.ReplicaError>,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.exit)

      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map((reason) => reason._tag), ["Fail", "Die"])
        assert.isTrue(Cause.hasDies(exit.cause))
        assert.strictEqual(Cause.findErrorOption(exit.cause)._tag, "Some")
        const die = exit.cause.reasons.find(Cause.isDieReason)
        assert.strictEqual(die?.defect, sentinel)
      }
    })))

  it.effect("preserves a source failure combined with interruption", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      const sourceFailure = new ReplicaError.ReplicaError({
        reason: new ReplicaError.RestoreBusy({ replica: "test" })
      })
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Pull)({
              _tag: "Pull",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "SourceFailure") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(2)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(3)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          4_096,
          4,
          () => Effect.never
        )
      )
      const composite = Cause.fromReasons([
        Cause.makeFailReason(sourceFailure),
        Cause.makeInterruptReason(987)
      ])
      const exit = yield* client.restoreBackup({
        source: Stream.fromEffect(Effect.failCause(composite)),
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.exit)

      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.strictEqual(exit.cause.reasons.length, 2)
        assert.isTrue(Cause.hasInterrupts(exit.cause))
        assert.strictEqual(Cause.findErrorOption(exit.cause)._tag, "Some")
      }
    })))

  it.effect("accepts an owner error at the exact ASCII byte budget", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(2)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          ReplicaLimits.minimumRestoreErrorBytes,
          4,
          () =>
            Effect.fail(
              new RestoreProtocol.RestoreResultSessionFailure({
                error: { _tag: "RestoreBusy", replica: "a".repeat(89) }
              })
            )
        )
      )
      const error = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "RestoreBusy")
    })))

  it.effect("counts multibyte owner error text by exact UTF-8 bytes", () =>
    Effect.scoped(Effect.gen(function*() {
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
      channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        const frame = Schema.decodeUnknownSync(RestoreProtocol.PageToOwnerFrame)(event.data)
        if (frame._tag === "Start") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.TerminalReady)({
              _tag: "TerminalReady",
              nonce,
              sequence: sequence(1)
            })
          )
        } else if (frame._tag === "TerminalAck") {
          channel.port2.postMessage(
            Schema.encodeSync(RestoreProtocol.Released)({
              _tag: "Released",
              nonce,
              sequence: sequence(2)
            })
          )
        }
      })
      channel.port2.start()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(
          () => Effect.succeed({ nonce, port: channel.port1 }),
          ReplicaLimits.minimumRestoreErrorBytes,
          4,
          () =>
            Effect.fail(
              new RestoreProtocol.RestoreResultSessionFailure({
                error: {
                  _tag: "ProtocolMismatch",
                  expected: `${"é".repeat(37)}a`,
                  observed: ""
                }
              })
            )
        )
      )
      const error = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.expected, `${"é".repeat(37)}a`)
        assert.strictEqual(error.reason.observed, "")
      }
    })))

  it.effect("preserves the native DataCloneError from the begin boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const sentinel = new DOMException("structured clone failed", "DataCloneError")
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(() => Effect.die(sentinel))
      )
      const error = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      if (error.reason._tag === "StorageUnavailable") {
        assert.strictEqual(error.reason.cause, sentinel)
      }
    })))

  it.effect("preserves the native postMessage failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const sentinel = new DOMException("message port is closed", "InvalidStateError")
      const port = {
        addEventListener() {},
        removeEventListener() {},
        start() {},
        close() {},
        postMessage() {
          throw sentinel
        }
      } as unknown as MessagePort
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(() => Effect.succeed({ nonce, port }))
      )
      const error = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.flip)

      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      if (error.reason._tag === "StorageUnavailable") {
        assert.strictEqual(error.reason.cause, sentinel)
      }
    })))

  it.effect("rejects every invalid advertised chunk and coalesce limit before opening the transport", () =>
    Effect.gen(function*() {
      const invalidLimits: ReadonlyArray<{
        readonly field: "maxChunkBytes" | "maxRestoreCoalesceMillis"
        readonly label: string
        readonly value?: number
      }> = [
        { field: "maxChunkBytes", label: "missing" },
        { field: "maxChunkBytes", label: "zero", value: 0 },
        { field: "maxChunkBytes", label: "fractional", value: 1.5 },
        { field: "maxRestoreCoalesceMillis", label: "missing" },
        { field: "maxRestoreCoalesceMillis", label: "zero", value: 0 },
        {
          field: "maxRestoreCoalesceMillis",
          label: "unsafe",
          value: Number.MAX_SAFE_INTEGER + 1
        }
      ]

      for (const invalid of invalidLimits) {
        let beginCalls = 0
        let closeCalls = 0
        const advertised = {
          maxChunkBytes: 4,
          maxRestoreCoalesceMillis: 25
        } as {
          maxChunkBytes?: number
          maxRestoreCoalesceMillis?: number
        }
        if (invalid.value === undefined) {
          delete advertised[invalid.field]
        } else {
          advertised[invalid.field] = invalid.value
        }
        const invalidRpc = {
          OpenSession: () =>
            Effect.succeed({
              leaseMillis: 10_000,
              protocolVersion: ReplicaRpc.protocolVersion,
              definitionHash: definition.hash,
              ownerEpoch: "owner",
              ...advertised,
              maxRestoreErrorBytes: 4_096
            }),
          CloseSession: () => Effect.sync(() => closeCalls++),
          BeginRestoreBackup: () =>
            Effect.sync(() => {
              beginCalls++
              return { nonce, port: new MessageChannel().port1 }
            })
        } as unknown as RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>

        const exit = yield* Effect.scoped(
          ReplicaClient.fromRpcClient(definition, invalidRpc)
        ).pipe(Effect.exit)

        assert.isTrue(Exit.isFailure(exit), `${invalid.field} ${invalid.label}`)
        if (Exit.isFailure(exit)) {
          const error = Cause.findErrorOption(exit.cause)
          assert.strictEqual(error._tag, "Some", `${invalid.field} ${invalid.label}`)
          if (error._tag === "Some") {
            assert.strictEqual(
              error.value.reason._tag,
              "ProtocolMismatch",
              `${invalid.field} ${invalid.label}`
            )
          }
        }
        assert.strictEqual(closeCalls, 1, `${invalid.field} ${invalid.label}`)
        assert.strictEqual(beginCalls, 0, `${invalid.field} ${invalid.label}`)
      }
    }))

  it.effect("rejects missing and undersized advertised restore error limits", () =>
    Effect.gen(function*() {
      for (
        const maxRestoreErrorBytes of [
          undefined,
          ReplicaLimits.minimumRestoreErrorBytes - 1
        ]
      ) {
        let closeCalls = 0
        const invalidRpc = {
          OpenSession: () =>
            Effect.succeed({
              leaseMillis: 10_000,
              protocolVersion: ReplicaRpc.protocolVersion,
              definitionHash: definition.hash,
              ownerEpoch: "owner",
              maxChunkBytes: 4,
              maxRestoreCoalesceMillis: 25,
              ...(maxRestoreErrorBytes === undefined ? {} : { maxRestoreErrorBytes })
            }),
          CloseSession: () => Effect.sync(() => closeCalls++)
        } as unknown as RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>
        const exit = yield* Effect.scoped(
          ReplicaClient.fromRpcClient(definition, invalidRpc)
        ).pipe(Effect.exit)

        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) {
          const error = Cause.findErrorOption(exit.cause)
          assert.strictEqual(error._tag, "Some")
          if (error._tag === "Some") {
            assert.strictEqual(error.value.reason._tag, "ProtocolMismatch")
          }
        }
        assert.strictEqual(closeCalls, 1)
      }
    }))

  // Expressed as "not ours" rather than as a specific older number. There is only one protocol
  // version, so naming a predecessor would invent a history that cannot exist; what this pins is
  // that an owner from a different build is refused before any transport is opened.
  it.effect("rejects a handshake from an owner speaking another protocol version", () =>
    Effect.scoped(Effect.gen(function*() {
      const otherVersion = ReplicaRpc.protocolVersion + 1
      const otherRpc = {
        OpenSession: () =>
          Effect.succeed({
            leaseMillis: 10_000,
            protocolVersion: otherVersion,
            definitionHash: definition.hash,
            ownerEpoch: "owner"
          }),
        CloseSession: () => Effect.void
      } as unknown as RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>
      const error = yield* ReplicaClient.fromRpcClient(definition, otherRpc).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.expected, `protocol version ${ReplicaRpc.protocolVersion}`)
        assert.strictEqual(error.reason.observed, `protocol version ${otherVersion}`)
      }
    })))
})
