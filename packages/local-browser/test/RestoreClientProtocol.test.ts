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
  maxRestoreErrorBytes = 4_096
) =>
  ({
    OpenSession: () =>
      Effect.succeed({
        leaseMillis: 10_000,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash,
        ownerEpoch: "owner",
        maxChunkBytes: 4,
        maxRestoreCoalesceMillis: 25,
        maxRestoreErrorBytes
      }),
    RenewSession: () => Effect.succeed({ leaseMillis: 10_000 }),
    CloseSession: () => Effect.void,
    BeginRestoreBackupV4: begin
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
                Schema.encodeSync(RestoreProtocol.TerminalSuccess)({
                  _tag: "TerminalSuccess",
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
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
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
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
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
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
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

  it.effect("closes the transferred port once when interrupted at its acquisition boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const beginReady = yield* Deferred.make<void>()
      const releaseBegin = yield* Deferred.make<void>()
      const acquired = yield* Deferred.make<void>()
      let peerCloseCount = 0
      let restore: Fiber.Fiber<void, ReplicaError.ReplicaError> | undefined
      const port = {
        addEventListener() {},
        removeEventListener() {},
        start() {},
        postMessage() {},
        get close() {
          queueMicrotask(() => Deferred.doneUnsafe(acquired, Effect.void))
          return () => {
            peerCloseCount++
          }
        }
      } as unknown as MessagePort
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        rpcClient(() =>
          Deferred.succeed(beginReady, undefined).pipe(
            Effect.andThen(Deferred.await(releaseBegin)),
            Effect.as({ nonce, port }),
            Effect.uninterruptible
          )
        )
      )
      restore = yield* client.restoreBackup({
        source: Stream.never,
        mode: "replace",
        maxBytes: 64,
        expectedDefinitionHash: definition.hash,
        installationId
      }).pipe(Effect.forkChild)

      yield* Deferred.await(beginReady)
      yield* Deferred.succeed(releaseBegin, undefined)
      yield* Deferred.await(acquired)
      yield* Fiber.interrupt(restore)

      assert.strictEqual(peerCloseCount, 1)
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
            Schema.encodeSync(RestoreProtocol.TerminalSuccess)({
              _tag: "TerminalSuccess",
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
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
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
            Schema.encodeSync(RestoreProtocol.TerminalSuccess)({
              _tag: "TerminalSuccess",
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
        rpcClient(() => Effect.succeed({ nonce, port: channel.port1 }))
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
            Schema.encodeSync(RestoreProtocol.TerminalSessionFailure)({
              _tag: "TerminalSessionFailure",
              nonce,
              sequence: sequence(1),
              error: { _tag: "RestoreBusy", replica: "a".repeat(89) }
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
          ReplicaLimits.minimumRestoreErrorBytes
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
            Schema.encodeSync(RestoreProtocol.TerminalSessionFailure)({
              _tag: "TerminalSessionFailure",
              nonce,
              sequence: sequence(1),
              error: {
                _tag: "ProtocolMismatch",
                expected: `${"é".repeat(37)}a`,
                observed: ""
              }
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
          ReplicaLimits.minimumRestoreErrorBytes
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

  it.effect("rejects a version 3 handshake before opening the source transport", () =>
    Effect.scoped(Effect.gen(function*() {
      const oldRpc = {
        OpenSession: () =>
          Effect.succeed({
            leaseMillis: 10_000,
            protocolVersion: 3,
            definitionHash: definition.hash,
            ownerEpoch: "owner"
          }),
        CloseSession: () => Effect.void
      } as unknown as RpcClient.FromGroup<typeof ReplicaRpc.group, RpcClientError.RpcClientError>
      const error = yield* ReplicaClient.fromRpcClient(definition, oldRpc).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.expected, "protocol version 4")
        assert.strictEqual(error.reason.observed, "protocol version 3")
      }
    })))
})
