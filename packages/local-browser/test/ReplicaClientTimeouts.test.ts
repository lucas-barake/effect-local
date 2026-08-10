import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { RpcTest } from "effect/unstable/rpc"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, DeliveryPublisher, documentId, PeerRelayRuntime, replica, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaClient timeouts", (it) => {
  const limits = {
    maxBackupBytes: 1024,
    maxChunkBytes: 128,
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
    maxStreamsPerSession: 2,
    maxInFlightPerSession: 2,
    maxQueuedRpc: 4,
    maxQueuedPermits: 4,
    maxActiveRestores: 4,
    maxRestoresPerSession: 2,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  } satisfies ReplicaLimits.Values
  const Sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)))
  const Publisher = Layer.merge(
    Layer.succeed(
      CommitPublisher.CommitPublisher,
      CommitPublisher.CommitPublisher.of({
        publishPending: Effect.succeed(0),
        drainPending: Effect.succeed(0),
        invalidate: () => Effect.void,
        subscribe: Effect.succeed({
          watermark: Identity.CommitSequence.make(0),
          refreshGeneration: 0,
          events: Stream.never
        })
      })
    ),
    DeliveryPublisher
  )
  const Owner = ReplicaOwner.layerHandlers(definition).pipe(
    Layer.provide(PeerRelayRuntime),
    Layer.provide(PeerConnectionStatus.layer),
    Layer.provide(RelayConnectionStatus.layerNotConfigured),
    Layer.provideMerge(Sessions),
    Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
  )

  const timeouts = { sessionTimeout: 1_000, operationTimeout: 2_000 }
  const resolution = Conflict.Resolution.make({
    heads: [],
    path: { parents: [], target: { _tag: "Key", key: "title" } },
    choice: { _tag: "DeleteValue" }
  })

  it.effect("fails a never-responding session acquire with a typed OperationTimeout", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const openInterrupted = yield* Deferred.make<void>()
      let closeCalls = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) =>
              value(payload).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(openInterrupted, undefined))
              )
          }
          if (property === "CloseSession") {
            return (payload: never) => {
              closeCalls++
              return value(payload)
            }
          }
          return value
        }
      })
      const fiber = yield* Effect.scoped(ReplicaClient.fromRpcClient(definition, wedged, timeouts))
        .pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.sessionTimeout - 1)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "OpenSession")
        assert.strictEqual(error.reason.timeoutMillis, 1_000)
      }
      assert.isTrue(yield* Deferred.isDone(openInterrupted))
      assert.strictEqual(closeCalls, 1)
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(Owner)))

  it.effect("fails a never-responding per-operation RPC with a typed OperationTimeout", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const getInterrupted = yield* Deferred.make<void>()
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Get") {
            return () => Effect.never.pipe(Effect.ensuring(Deferred.succeed(getInterrupted, undefined)))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, {
        sessionTimeout: "1 second",
        operationTimeout: "2 seconds"
      })
      const fiber = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout - 1)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "Get")
        assert.strictEqual(error.reason.timeoutMillis, 2_000)
      }
      assert.isTrue(yield* Deferred.isDone(getInterrupted))
    })).pipe(Effect.provide(Owner)))

  it.effect("uses the default session timeout", () =>
    Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "OpenSession") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const fiber = yield* Effect.scoped(ReplicaClient.fromRpcClient(definition, wedged)).pipe(Effect.forkChild)
      yield* TestClock.adjust(9_999)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "OpenSession")
        assert.strictEqual(error.reason.timeoutMillis, 10_000)
      }
    }).pipe(Effect.provide(Owner)))

  it.effect("uses the default operation timeout", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Get") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged)
      const fiber = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* TestClock.adjust(29_999)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "Get")
        assert.strictEqual(error.reason.timeoutMillis, 30_000)
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("returns an operation result completed before its deadline", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "Get") {
            return (payload: never) => Effect.sleep(1_999).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, delayed, timeouts)
      const fiber = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* TestClock.adjust(1_999)
      assert.strictEqual((yield* Fiber.join(fiber)).documentId, documentId)
    })).pipe(Effect.provide(Owner)))

  it.effect("returns schema-valid metadata for a negative infinite timeout", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Get") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, {
        sessionTimeout: 1_000,
        operationTimeout: "-Infinity"
      })
      const error = yield* client.get(Task, documentId).pipe(Effect.flip)
      assert.isTrue(Schema.is(ReplicaError.ReplicaError)(error))
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.timeoutMillis, 0)
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("times out a replayed operation after ProtocolMismatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replayStarted = yield* Deferred.make<void>()
      let opens = 0
      let gets = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => Effect.sync(() => ++opens).pipe(Effect.andThen(value(payload)))
          }
          if (property === "Get") {
            return () => {
              gets++
              return gets === 1
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed: "stale" })
                  })
                )
                : Deferred.succeed(replayStarted, undefined).pipe(Effect.andThen(Effect.never))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, {
        sessionTimeout: 1_000,
        operationTimeout: 1_000
      })
      const fiber = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* Deferred.await(replayStarted)
      yield* TestClock.adjust(999)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") assert.strictEqual(error.reason.operation, "Get")
      assert.strictEqual(opens, 2)
      assert.strictEqual(gets, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("resolves a committed command when its dispatch response times out", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let lookupExecutions = 0
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "Create") {
            return (payload: never) => value(payload).pipe(Effect.andThen(Effect.never))
          }
          if (property === "LookupCreate") {
            return (payload: never) =>
              Effect.sync(() => {
                lookupExecutions++
              }).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, delayed, timeouts)
      const commandId = yield* Identity.makeCommandId
      const fiber = yield* client.create(Task, {
        commandId,
        value: { title: "new" }
      }).pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout)
      assert.strictEqual(yield* Fiber.join(fiber), documentId)
      assert.strictEqual(lookupExecutions, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("reports ambiguity when a timed out command receipt lookup never responds", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const lookupStarted = yield* Deferred.make<void>()
      const lookupInterrupted = yield* Deferred.make<void>()
      let createCalls = 0
      let lookupCalls = 0
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "Create") {
            return (payload: never) => {
              createCalls++
              return value(payload).pipe(Effect.andThen(Effect.never))
            }
          }
          if (property === "LookupCreate") {
            return () => {
              lookupCalls++
              return Deferred.succeed(lookupStarted, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(lookupInterrupted, undefined))
              )
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, delayed, timeouts)
      const commandId = yield* Identity.makeCommandId
      const fiber = yield* client.create(Task, {
        commandId,
        value: { title: "new" }
      }).pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout)
      yield* Deferred.await(lookupStarted)
      yield* TestClock.adjust(timeouts.operationTimeout - 1)
      assert.isUndefined(fiber.pollUnsafe())
      yield* TestClock.adjust(1)
      const error = yield* Effect.flip(Fiber.join(fiber))
      assert.strictEqual(error.reason._tag, "CommandOutcomeUnknown")
      if (error.reason._tag === "CommandOutcomeUnknown") assert.strictEqual(error.reason.commandId, commandId)
      assert.strictEqual(createCalls, 1)
      assert.strictEqual(lookupCalls, 1)
      assert.isTrue(yield* Deferred.isDone(lookupInterrupted))
    })).pipe(Effect.provide(Owner)))

  it.effect("interrupts an automatic lookup when the caller is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const lookupStarted = yield* Deferred.make<void>()
      const lookupInterrupted = yield* Deferred.make<void>()
      const unknown = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Create") {
            return (payload: { readonly commandId: Identity.CommandId }) =>
              Effect.succeed(CommandOutcome.unknown(payload.commandId))
          }
          if (property === "LookupCreate") {
            return () =>
              Deferred.succeed(lookupStarted, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(lookupInterrupted, undefined))
              )
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, unknown, timeouts)
      const fiber = yield* client.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "new" }
      }).pipe(Effect.forkChild)

      yield* Deferred.await(lookupStarted)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      assert(Exit.isFailure(exit))
      assert.isTrue(exit.cause.reasons.some((reason) => reason._tag === "Interrupt"))
      yield* Deferred.await(lookupInterrupted)
    })).pipe(Effect.provide(Owner)))

  it.effect("recovers a timed out conflict resolution through lookup", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let dispatches = 0
      let lookups = 0
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "ResolveConflict") {
            return (payload: never) => {
              dispatches++
              return value(payload).pipe(Effect.andThen(Effect.never))
            }
          }
          if (property === "LookupConflictResolution") {
            return (payload: never) =>
              Effect.sync(() => {
                lookups++
              }).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, delayed, timeouts)
      const commandId = yield* Identity.makeCommandId
      const fiber = yield* client.resolveConflict(Task, {
        commandId,
        documentId,
        resolution
      }).pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout)
      yield* Fiber.join(fiber)
      assert.strictEqual(dispatches, 1)
      assert.strictEqual(lookups, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("supports manual conflict lookup after caller cancellation", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const committed = yield* Deferred.make<void>()
      let lookups = 0
      const stalled = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "ResolveConflict") {
            return (payload: never) =>
              value(payload).pipe(
                Effect.tap(() => Deferred.succeed(committed, undefined)),
                Effect.andThen(Effect.never)
              )
          }
          if (property === "LookupConflictResolution") {
            return (payload: never) =>
              Effect.sync(() => {
                lookups++
              }).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, stalled, {
        ...timeouts,
        operationTimeout: 1_000_000_000
      })
      const commandId = yield* Identity.makeCommandId
      const fiber = yield* client.resolveConflict(Task, {
        commandId,
        documentId,
        resolution
      }).pipe(Effect.forkChild)
      yield* Deferred.await(committed)
      assert.strictEqual(lookups, 0)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)
      assert.isTrue(Exit.isFailure(exit))
      assert.strictEqual(lookups, 0)
      assert.deepStrictEqual(
        yield* client.lookupConflictResolution(Task, {
          commandId,
          documentId,
          resolution
        }),
        CommandOutcome.durablyCommitted(commandId, undefined)
      )
      assert.strictEqual(lookups, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("keeps invalidations live after one lease renewal timeout", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let renewals = 0
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "RenewSession") {
            return (payload: never) =>
              Effect.sync(() => ++renewals).pipe(
                Effect.flatMap((attempt) => attempt === 1 ? Effect.never : value(payload))
              )
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, delayed, timeouts)
      const invalidations = yield* client.invalidations.pipe(Stream.runDrain, Effect.forkChild)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2)
      yield* TestClock.adjust(timeouts.sessionTimeout)
      assert.isUndefined(invalidations.pollUnsafe())
      yield* TestClock.adjust("1 second")
      assert.strictEqual(renewals, 2)
      assert.isUndefined(invalidations.pollUnsafe())
      yield* Fiber.interrupt(invalidations)
    })).pipe(Effect.provide(Owner)))

  it.effect("times out destructive operations without reopening or replaying them", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let opens = 0
      let restores = 0
      let imports = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => Effect.sync(() => ++opens).pipe(Effect.andThen(value(payload)))
          }
          if (property === "BeginRestoreBackup") {
            return () => {
              restores++
              return Effect.never
            }
          }
          if (property === "ImportDocument") {
            return () => {
              imports++
              return Effect.never
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, {
        sessionTimeout: 1_000,
        operationTimeout: 1_000
      })
      const restore = yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_5f0a6f45-9be2-4c7a-8f45-1d2f3a4b5c6d")
      }).pipe(Effect.flip, Effect.forkChild)
      yield* TestClock.adjust(1_000)
      const restoreError = yield* Fiber.join(restore)
      const imported = yield* client.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: {
          documentName: Task.name,
          schemaVersion: Task.version,
          value: { title: "stored" }
        }
      }).pipe(Effect.flip, Effect.forkChild)
      yield* TestClock.adjust(1_000)
      const importError = yield* Fiber.join(imported)
      assert.strictEqual(restoreError.reason._tag, "OperationTimeout")
      if (restoreError.reason._tag === "OperationTimeout") {
        assert.strictEqual(restoreError.reason.operation, "RestoreBackup")
      }
      assert.strictEqual(importError.reason._tag, "OperationTimeout")
      if (importError.reason._tag === "OperationTimeout") {
        assert.strictEqual(importError.reason.operation, "ImportDocument")
      }
      assert.strictEqual(opens, 1)
      assert.strictEqual(restores, 1)
      assert.strictEqual(imports, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("does not apply unary operation timeouts to status or backup streams", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const releaseStatus = yield* Deferred.make<void>()
      const releaseBackup = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Status") {
            return () => Stream.fromEffect(Deferred.await(releaseStatus)).pipe(Stream.map(() => ready))
          }
          if (property === "ExportBackup") {
            return () => Stream.fromEffect(Deferred.await(releaseBackup)).pipe(Stream.map(() => Uint8Array.of(1, 2, 3)))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
      const status = yield* client.status.pipe(Stream.runCollect, Effect.forkChild)
      const backup = yield* client.exportBackup({ maxBytes: 1024 }).pipe(Stream.runCollect, Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout + 1)
      assert.isUndefined(status.pollUnsafe())
      assert.isUndefined(backup.pollUnsafe())
      yield* Deferred.succeed(releaseStatus, undefined)
      yield* Deferred.succeed(releaseBackup, undefined)
      assert.deepStrictEqual(Array.from(yield* Fiber.join(status)), [ready])
      assert.deepStrictEqual(Array.from(yield* Fiber.join(backup)), [Uint8Array.of(1, 2, 3)])
    })).pipe(Effect.provide(Owner)))

  it.effect("bounds a reopen whose fresh session acquire never responds", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let opens = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "OpenSession") {
            return (payload: never) => {
              opens += 1
              return opens === 1 ? Reflect.get(target, property, receiver)(payload) : Effect.never
            }
          }
          if (property === "Flush") {
            return () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed: "fenced" })
                })
              )
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
      const fiber = yield* client.flush.pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.sessionTimeout)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "OpenSession")
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("bounds teardown when CloseSession never responds", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const closeInterrupted = yield* Deferred.make<void>()
      let closeCalls = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "CloseSession") {
            return (payload: never) => {
              closeCalls++
              return value(payload).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(closeInterrupted, undefined))
              )
            }
          }
          return value
        }
      })
      const ready = yield* Deferred.make<void>()
      const fiber = yield* Effect.scoped(Effect.gen(function*() {
        yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
        yield* Deferred.succeed(ready, undefined)
        yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(ready)
      const interrupting = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.strictEqual(closeCalls, 1)
      assert.strictEqual(yield* sessions.activeCount, 0)
      yield* TestClock.adjust(timeouts.sessionTimeout - 1)
      assert.isUndefined(interrupting.pollUnsafe())
      yield* TestClock.adjust(1)
      yield* Fiber.join(interrupting)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.hasInterrupts(exit!))
      assert.isTrue(yield* Deferred.isDone(closeInterrupted))
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(Owner)))
})
