import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { Headers } from "effect/unstable/http"
import { Rpc, RpcTest } from "effect/unstable/rpc"
import { RequestId } from "effect/unstable/rpc/RpcMessage"
import type * as RestoreProtocol from "../src/internal/restoreProtocol.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, replica } from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaOwner restore", (it) => {
  const limits = {
    maxBackupBytes: 1024,
    maxChunkBytes: 128,
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
    maxSessions: 4,
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
  const Publisher = Layer.succeed(
    CommitPublisher.CommitPublisher,
    CommitPublisher.CommitPublisher.of({
      publishPending: Effect.succeed(0),
      invalidate: () => Effect.void,
      subscribe: Effect.succeed({
        watermark: Identity.CommitSequence.make(0),
        refreshGeneration: 0,
        events: Stream.never
      })
    })
  )
  const ownerLayer = (replicaService: Replica.Replica["Service"] = replica) =>
    ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, replicaService)
      ))
    )
  const restoreOptions = {
    mode: "replace" as const,
    maxBytes: limits.maxBackupBytes,
    expectedDefinitionHash: definition.hash,
    installationId: Identity.BackupInstallationId.make("bak_43c8d2f4-58ce-4c9a-9155-9d21019f5e9d")
  }
  const options = (client: Rpc.ServerClient, requestId: string) => ({
    client,
    requestId: RequestId(requestId),
    headers: Headers.empty
  })
  const unary = <A, E, R,>(effect: Effect.Effect<A | Deferred.Deferred<A, E>, E, R>) =>
    Effect.flatMap(effect, (value) => Deferred.isDeferred<A, E>(value) ? Deferred.await(value) : Effect.succeed(value))

  it.effect("advertises the configured restore transport limits", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const sessionId = yield* Identity.makeSessionId
      const handshake = yield* rpc.OpenSession({
        sessionId,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      })
      assert.strictEqual(handshake.protocolVersion, ReplicaRpc.protocolVersion)
      assert.strictEqual(handshake.maxChunkBytes, limits.maxChunkBytes)
      assert.strictEqual(handshake.maxRestoreCoalesceMillis, limits.maxRestoreCoalesceMillis)
      assert.strictEqual(handshake.maxRestoreErrorBytes, limits.maxRestoreErrorBytes)
    })).pipe(Effect.provide(ownerLayer())))

  it.effect("validates legacy ownership before rejecting without reading chunks", () =>
    Effect.scoped(Effect.gen(function*() {
      const open = yield* ReplicaRpc.group.accessHandler("OpenSession")
      const restore = yield* ReplicaRpc.group.accessHandler("RestoreBackup")
      const sessionId = yield* Identity.makeSessionId
      const owner = new Rpc.ServerClient(1)
      const other = new Rpc.ServerClient(2)
      yield* unary(open({
        sessionId,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      }, options(owner, "open")))

      let chunksRead = false
      const payload: { readonly sessionId: Identity.SessionId } = { sessionId }
      Object.defineProperty(payload, "chunks", {
        enumerable: true,
        get() {
          chunksRead = true
          throw new Error("legacy chunks were read")
        }
      })

      const restoreRpc = ReplicaRpc.group.requests.get("RestoreBackup")
      if (restoreRpc?._tag !== "RestoreBackup") {
        return yield* Effect.die(new Error("RestoreBackup RPC not found"))
      }
      assert.deepStrictEqual(
        yield* Schema.decodeUnknownEffect(restoreRpc.payloadSchema)(payload),
        { sessionId }
      )
      const legacyError = yield* Effect.flip(unary(restore(payload, options(owner, "restore"))))
      assert.isFalse(chunksRead)
      assert.strictEqual(legacyError.reason._tag, "ProtocolMismatch")
      if (legacyError.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(legacyError.reason.expected, "BeginRestoreBackup")
        assert.strictEqual(legacyError.reason.observed, "RestoreBackup")
      }

      const ownershipError = yield* Effect.flip(unary(restore(payload, options(other, "restore-other"))))
      assert.isFalse(chunksRead)
      assert.strictEqual(ownershipError.reason._tag, "ProtocolMismatch")
      if (ownershipError.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(ownershipError.reason.expected, "active session")
        assert.strictEqual(ownershipError.reason.observed, sessionId)
      }
    })).pipe(Effect.provide(ownerLayer())))

  it.effect("validates maxBytes before restore admission and delegates valid requests", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      yield* rpc.OpenSession({
        sessionId,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      })

      const invalid = yield* Effect.flip(rpc.BeginRestoreBackup({
        sessionId,
        ...restoreOptions,
        maxBytes: 0
      }))
      assert.strictEqual(invalid.reason._tag, "BackupInvalid")
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)

      const started = yield* rpc.BeginRestoreBackup({ sessionId, ...restoreOptions })
      assert.match(started.nonce, /^rst_/u)
      assert.strictEqual(typeof started.port.postMessage, "function")
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)
      started.port.close()
    })).pipe(Effect.provide(ownerLayer())))

  it.effect("releases restore admission when the Finish RPC is interrupted", () =>
    Effect.gen(function*() {
      const restoreStarted = yield* Deferred.make<void>()
      const stalledReplica: Replica.Replica["Service"] = {
        ...replica,
        restoreBackup: () =>
          Deferred.succeed(restoreStarted, undefined).pipe(
            Effect.andThen(Effect.never)
          )
      }
      return yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const sessions = yield* SessionManager.SessionManager
        const sessionId = yield* Identity.makeSessionId
        yield* rpc.OpenSession({
          sessionId,
          protocolVersion: ReplicaRpc.protocolVersion,
          definitionHash: definition.hash
        })
        const started = yield* rpc.BeginRestoreBackup({ sessionId, ...restoreOptions })
        yield* Effect.addFinalizer(() => Effect.sync(() => started.port.close()))
        started.port.start()

        const finish = yield* rpc.FinishRestoreBackup({
          sessionId,
          nonce: started.nonce
        }).pipe(Effect.forkChild({ startImmediately: true }))
        started.port.postMessage(
          {
            _tag: "Start",
            nonce: started.nonce,
            sequence: 0
          } satisfies RestoreProtocol.Start
        )
        yield* Deferred.await(restoreStarted)
        assert.strictEqual(yield* sessions.activeRestoreCount, 1)

        yield* Fiber.interrupt(finish)
        yield* Effect.yieldNow
        const activeAfterInterrupt = yield* sessions.activeRestoreCount
        started.port.close()
        yield* Effect.yieldNow
        assert.strictEqual(activeAfterInterrupt, 0)
      })).pipe(Effect.provide(ownerLayer(stalledReplica)))
    }))

  it.effect("builds an independent restore transport for each owner layer", () =>
    Effect.scoped(Effect.gen(function*() {
      const scopeA = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void))
      const scopeB = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void))
      const memoMap = yield* Layer.makeMemoMap
      const contextA = yield* Layer.buildWithMemoMap(ownerLayer(), memoMap, scopeA)
      const contextB = yield* Layer.buildWithMemoMap(ownerLayer(), memoMap, scopeB)
      const sessions = Context.get(contextA, SessionManager.SessionManager)
      const openA = yield* ReplicaRpc.group.accessHandler("OpenSession").pipe(Effect.provide(contextA))
      const openB = yield* ReplicaRpc.group.accessHandler("OpenSession").pipe(Effect.provide(contextB))
      const beginA = yield* ReplicaRpc.group.accessHandler("BeginRestoreBackup").pipe(Effect.provide(contextA))
      const beginB = yield* ReplicaRpc.group.accessHandler("BeginRestoreBackup").pipe(Effect.provide(contextB))
      const clientA = new Rpc.ServerClient(1)
      const clientB = new Rpc.ServerClient(2)
      const sessionA = yield* Identity.makeSessionId
      const sessionB = yield* Identity.makeSessionId
      yield* unary(openA({
        sessionId: sessionA,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      }, options(clientA, "open-a")))
      yield* unary(openB({
        sessionId: sessionB,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      }, options(clientB, "open-b")))

      const startedA = yield* unary(beginA(
        { sessionId: sessionA, ...restoreOptions },
        options(clientA, "begin-a")
      ))
      const startedB = yield* unary(beginB(
        { sessionId: sessionB, ...restoreOptions },
        options(clientB, "begin-b")
      ))
      assert.strictEqual(yield* sessions.activeRestoreCount, 2)

      yield* Scope.close(scopeA, Exit.void)
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)

      const secondB = yield* unary(beginB(
        {
          sessionId: sessionB,
          ...restoreOptions,
          installationId: Identity.BackupInstallationId.make("bak_1510f96c-773b-449e-a034-9131b17c7935")
        },
        options(clientB, "begin-b-2")
      ))
      assert.strictEqual(yield* sessions.activeRestoreCount, 2)
      startedA.port.close()
      startedB.port.close()
      secondB.port.close()
      yield* Scope.close(scopeB, Exit.void)
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
    })))
})
