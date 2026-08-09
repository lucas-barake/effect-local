import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { Headers } from "effect/unstable/http"
import { Rpc, type RpcClient as EffectRpcClient, RpcTest } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import { RequestId } from "effect/unstable/rpc/RpcMessage"
import * as WorkerError from "effect/unstable/workers/WorkerError"
import * as RestoreProtocol from "../src/internal/restoreProtocol.js"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import {
  definition,
  DeliveryPublisher,
  documentId,
  Read,
  ReadError,
  Rename,
  RenameError,
  replica,
  Task
} from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaClient", (it) => {
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
  const allInvalidationKeys = [
    ...ReplicaDefinition.invalidationKeys(definition),
    ReplicaRpc.commandDeliveryInvalidationKey
  ]
  const Owner = ReplicaOwner.layerHandlers(definition).pipe(
    Layer.provide(PeerConnectionStatus.layer),
    Layer.provide(RelayConnectionStatus.layerNotConfigured),
    Layer.provideMerge(Sessions),
    Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
  )

  const disconnected = () =>
    new RpcClientError.RpcClientError({
      reason: new RpcClientError.RpcClientDefect({ message: "disconnected", cause: "disconnected" })
    })
  const transientDisconnected = () =>
    new RpcClientError.RpcClientError({
      reason: new WorkerError.WorkerReceiveError({ message: "disconnected", cause: "disconnected" })
    })
  const protocolMismatch = (observed: string) =>
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed })
    })
  type TestReplicaRpcClient = EffectRpcClient.FromGroup<
    typeof ReplicaRpc.group,
    RpcClientError.RpcClientError
  >
  type FinishRestore = TestReplicaRpcClient["FinishRestoreBackup"]
  const dropTerminalReady = (
    rpc: TestReplicaRpcClient,
    terminalDropped: Deferred.Deferred<void>,
    finish: FinishRestore = rpc.FinishRestoreBackup
  ): TestReplicaRpcClient =>
    new Proxy(rpc, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver)
        if (property === "FinishRestoreBackup") return finish
        if (property !== "BeginRestoreBackup") return value
        return (payload: Parameters<typeof target.BeginRestoreBackup>[0]) =>
          target.BeginRestoreBackup(payload).pipe(
            Effect.flatMap(({ nonce, port: ownerPort }) =>
              Effect.gen(function*() {
                const channel = new MessageChannel()
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    ownerPort.close()
                    channel.port2.close()
                  })
                )
                ownerPort.addEventListener("message", (event: MessageEvent<unknown>) => {
                  const frame = Schema.decodeUnknownSync(RestoreProtocol.OwnerToPageFrame)(event.data)
                  if (frame._tag === "TerminalReady") {
                    Deferred.doneUnsafe(terminalDropped, Effect.void)
                    return
                  }
                  channel.port2.postMessage(event.data)
                })
                channel.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
                  ownerPort.postMessage(event.data)
                })
                ownerPort.start()
                channel.port2.start()
                return { nonce, port: channel.port1 }
              })
            )
          )
      }
    })

  it.effect("round trips typed replica operations and releases its session", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      assert.strictEqual(yield* sessions.activeCount, 0)
      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
        assert.strictEqual(yield* sessions.activeCount, 1)
        yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2)
        yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2 + 1)
        assert.strictEqual(yield* sessions.activeCount, 1)

        const snapshot = yield* client.get(Task, documentId)
        assert.strictEqual(snapshot.value.title, "stored")
        const mutation = yield* client.mutate(Rename, {
          commandId: (yield* Identity.makeCommandId),
          documentId,
          payload: { title: "next" }
        })
        assert.strictEqual(mutation, "renamed")
        assert.deepStrictEqual(yield* client.query(Read, "filter"), [{ title: "filter" }])
        const exported = yield* client.exportDocument(Task, documentId)
        assert.deepStrictEqual(exported, {
          documentName: Task.name,
          schemaVersion: Task.version,
          value: { title: "stored" }
        })
        const importCommandId = yield* Identity.makeCommandId
        assert.deepStrictEqual(
          yield* client.importDocument(Task, { commandId: importCommandId, value: exported }),
          documentId
        )
        assert.deepStrictEqual(Array.from(yield* client.status.pipe(Stream.take(1), Stream.runCollect)), [{
          _tag: "Ready",
          pendingCommands: 0
        }])
      }))
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(
      Effect.provide(Owner)
    ))

  it.effect("round trips transformed conflict inspection, resolution, and lookup", () => {
    const TransformedTask = Document.make("TransformedTask", {
      schema: Schema.Struct({ title: Schema.NumberFromString }),
      version: 1
    })
    const transformedDefinition = ReplicaDefinition.make({
      name: "transformed-tasks",
      documents: DocumentSet.make(TransformedTask),
      mutations: [],
      projections: [],
      queries: []
    })
    const visible = Conflict.AlternativeId.make("2@actor")
    const other = Conflict.AlternativeId.make("1@actor")
    const path = { parents: [], target: { _tag: "Key" as const, key: "title" } }
    const resolution = Conflict.Resolution.make({
      heads: ["head"],
      path,
      choice: { _tag: "SelectAlternative", alternativeId: other }
    })
    let resolved: Conflict.Resolution | undefined
    const transformedReplica: Replica.Replica["Service"] = {
      ...replica,
      inspectConflicts: (_document, requestedId) =>
        Effect.succeed({
          snapshot: {
            documentId: requestedId,
            value: { title: 42 },
            version: 1,
            heads: ["head"],
            tombstone: false,
            projection: "Ready"
          },
          conflicts: [{
            path,
            visible,
            alternatives: [
              { id: other, value: "41" },
              { id: visible, value: "42" }
            ]
          }]
        }) as never,
      resolveConflict: (_document, options) =>
        Effect.sync(() => {
          resolved = options.resolution
        }) as never,
      lookupConflictResolution: (_document, options) =>
        Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined))
    }
    const TransformedOwner = ReplicaOwner.layerHandlers(transformedDefinition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, transformedReplica)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(transformedDefinition, rpc)
      const inspection = yield* client.inspectConflicts(TransformedTask, documentId)
      assert.strictEqual(inspection.snapshot.value.title, 42)
      assert.deepStrictEqual(
        inspection.conflicts[0]?.alternatives.map(({ value }) => value),
        ["41", "42"]
      )

      const commandId = yield* Identity.makeCommandId
      yield* client.resolveConflict(TransformedTask, { commandId, documentId, resolution })
      assert.deepStrictEqual(resolved, resolution)
      assert.deepStrictEqual(
        yield* client.lookupConflictResolution(TransformedTask, {
          commandId,
          documentId,
          resolution
        }),
        CommandOutcome.durablyCommitted(commandId, undefined)
      )
    })).pipe(Effect.provide(TransformedOwner))
  })

  it.effect("round trips tagged query errors through the wire", () => {
    const rejected: Replica.Replica["Service"] = {
      ...replica,
      query: (_query, ...payload) => Effect.fail(new ReadError({ filter: String(payload[0]) })) as never
    }
    const RejectedOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, rejected)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const error = yield* client.query(Read, "blocked").pipe(Effect.flip)
      assert.deepStrictEqual(error, new ReadError({ filter: "blocked" }))
    })).pipe(Effect.provide(RejectedOwner))
  })

  it.effect("closes an opened session when acquisition is interrupted", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const opened = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const delayed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property !== "OpenSession") return value
          return (payload: never) =>
            value(payload).pipe(
              Effect.tap(() => Deferred.succeed(opened, undefined)),
              Effect.tap(() => Deferred.await(release))
            )
        }
      })
      const fiber = yield* Effect.scoped(ReplicaClient.fromRpcClient(definition, delayed)).pipe(Effect.forkChild)
      yield* Deferred.await(opened)
      const interrupted = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(interrupted)
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(Owner)))

  it.effect("rejects clients built for a different definition", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const error = yield* Effect.flip(ReplicaClient.fromRpcClient({ ...definition, hash: "different" }, rpc))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    })).pipe(Effect.provide(Owner)))

  // Drives the real client, because the guard under test is the client's. The version deliberately
  // survives decoding, so nothing but this check stands between a stale owner and the replica.
  it.effect("decodes and rejects owners using an older protocol", () =>
    Effect.scoped(Effect.gen(function*() {
      const open = ReplicaRpc.group.requests.get("OpenSession")
      if (open?._tag !== "OpenSession") return yield* Effect.die(new Error("OpenSession RPC not found"))
      yield* Schema.decodeUnknownEffect(open.successSchema)({
        leaseMillis: 1_000,
        protocolVersion: ReplicaRpc.protocolVersion - 1,
        definitionHash: definition.hash,
        ownerEpoch: "owner"
      })
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const older = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property !== "OpenSession") return value
          return (payload: never) =>
            value(payload).pipe(Effect.map((lease) => ({
              ...(lease as {
                readonly leaseMillis: number
                readonly protocolVersion: number
                readonly definitionHash: string
                readonly ownerEpoch: string
              }),
              protocolVersion: ReplicaRpc.protocolVersion - 1
            })))
        }
      })
      const error = yield* Effect.flip(ReplicaClient.fromRpcClient(definition, older))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    })).pipe(Effect.provide(Owner)))

  it.effect("rejects malformed owner advertised conflict limits", () =>
    Effect.gen(function*() {
      const candidates: ReadonlyArray<ReplicaRpc.ConflictLimits | undefined> = [
        undefined,
        {
          maxConflictDepth: 0,
          maxConflictNodes: 1,
          maxConflictAlternatives: 1,
          maxConflictPathSegments: 1,
          maxConflictValueBytes: 1
        },
        {
          maxConflictDepth: ReplicaLimits.maxConflictDepthHardLimit + 1,
          maxConflictNodes: 1,
          maxConflictAlternatives: 1,
          maxConflictPathSegments: 1,
          maxConflictValueBytes: 1
        }
      ]

      for (const conflictLimits of candidates) {
        yield* Effect.scoped(Effect.gen(function*() {
          const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
          const malformed = new Proxy(rpc, {
            get(target, property, receiver) {
              const value = Reflect.get(target, property, receiver)
              if (property !== "OpenSession") return value
              const openSession = target.OpenSession
              return (payload: Parameters<typeof openSession>[0]) =>
                openSession(payload).pipe(
                  Effect.map((lease) => ({ ...lease, conflictLimits }))
                )
            }
          })
          const error = yield* Effect.flip(ReplicaClient.fromRpcClient(definition, malformed))
          assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        }))
      }
    }).pipe(Effect.provide(Owner)))

  it.effect("recovers ambiguous commands through typed receipt lookup", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ambiguous = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property !== "Create" && property !== "Mutate" && property !== "Delete") return value
          return (payload: never) => value(payload).pipe(Effect.andThen(Effect.fail(disconnected())))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, ambiguous)
      const createId = yield* Identity.makeCommandId
      const mutateId = yield* Identity.makeCommandId
      const deleteId = yield* Identity.makeCommandId

      assert.deepStrictEqual(
        yield* client.create(Task, { commandId: createId, value: { title: "new" } }),
        documentId
      )
      assert.deepStrictEqual(
        yield* client.mutate(Rename, { commandId: mutateId, documentId, payload: { title: "next" } }),
        "renamed"
      )
      assert.deepStrictEqual(
        yield* client.delete(Task, { commandId: deleteId, documentId }),
        undefined
      )
    })).pipe(Effect.provide(Owner)))

  it.effect("preserves interruption combined with an RPC command dispatch failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const interrupted = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Create") {
            return () =>
              Effect.failCause(Cause.combine(
                Cause.fail(disconnected()),
                Cause.interrupt(1)
              ))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, interrupted)
      const commandId = yield* Identity.makeCommandId
      const exit = yield* Effect.exit(
        client.create(Task, { commandId, value: { title: "new" } })
      )

      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isSuccess(exit)) return
      assert.isTrue(Cause.hasInterrupts(exit.cause))
      assert.isTrue(
        exit.cause.reasons.some((reason) =>
          Cause.isFailReason(reason) &&
          Schema.is(RpcClientError.RpcClientError)(reason.error)
        )
      )
      assert.isFalse(
        exit.cause.reasons.some((reason) =>
          Cause.isFailReason(reason) &&
          ReplicaError.isReplicaError(reason.error) &&
          reason.error.reason._tag === "CommandOutcomeUnknown"
        )
      )
    })).pipe(Effect.provide(Owner)))

  it.effect("recovers a direct unknown command outcome through receipt lookup", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let dispatches = 0
      let lookups = 0
      const unknown = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "Mutate") {
            return (payload: { readonly commandId: Identity.CommandId }) =>
              Effect.sync(() => {
                dispatches++
                return CommandOutcome.unknown(payload.commandId)
              })
          }
          if (property === "LookupMutation") {
            return (payload: never) =>
              Effect.sync(() => {
                lookups++
              }).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, unknown)
      const commandId = yield* Identity.makeCommandId

      assert.strictEqual(
        yield* client.mutate(Rename, { commandId, documentId, payload: { title: "next" } }),
        "renamed"
      )
      assert.strictEqual(dispatches, 1)
      assert.strictEqual(lookups, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("reports ambiguity when dispatch and lookup return direct unknown outcomes", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const unknown = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Mutate" || property === "LookupMutation") {
            return (payload: { readonly commandId: Identity.CommandId }) =>
              Effect.succeed(CommandOutcome.unknown(payload.commandId))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, unknown)
      const commandId = yield* Identity.makeCommandId
      const error = yield* Effect.flip(
        client.mutate(Rename, { commandId, documentId, payload: { title: "next" } })
      )

      assert.isTrue(ReplicaError.isReplicaError(error))
      if (!ReplicaError.isReplicaError(error)) return
      assert.strictEqual(error.reason._tag, "CommandOutcomeUnknown")
      if (error.reason._tag === "CommandOutcomeUnknown") {
        assert.strictEqual(error.reason.commandId, commandId)
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("recovers a lost conflict resolution response through complete lookup", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let dispatches = 0
      let lookups = 0
      const ambiguous = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "ResolveConflict") {
            return (payload: never) => {
              dispatches++
              return value(payload).pipe(Effect.andThen(Effect.fail(disconnected())))
            }
          }
          if (property === "LookupConflictResolution") {
            return (payload: never) => {
              lookups++
              return value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, ambiguous)
      const commandId = yield* Identity.makeCommandId
      const resolution = Conflict.Resolution.make({
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "DeleteValue" }
      })

      yield* client.resolveConflict(Task, { commandId, documentId, resolution })
      assert.strictEqual(dispatches, 1)
      assert.strictEqual(lookups, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("reports ambiguity, with its cause, when a command lookup also loses transport", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const unavailable = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Mutate" || property === "LookupMutation") {
            return () => Effect.fail(disconnected())
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, unavailable)
      const commandId = yield* Identity.makeCommandId
      const error = yield* Effect.flip(
        client.mutate(Rename, { commandId, documentId, payload: { title: "next" } })
      )
      // The channel now carries the mutation's own error too, so the replica failure is picked out
      // by the schema rather than by probing for a `_tag` `RenameError` could also have had.
      assert.isTrue(ReplicaError.isReplicaError(error))
      if (!ReplicaError.isReplicaError(error)) return
      assert.strictEqual(error.reason._tag, "CommandOutcomeUnknown")
      if (error.reason._tag !== "CommandOutcomeUnknown") return
      // The id is the handle `lookupMutation` needs, and the cause says why it is needed at all.
      assert.strictEqual(error.reason.commandId, commandId)
      assert.isTrue(Schema.is(RpcClientError.RpcClientError)(error.reason.cause))
    })).pipe(Effect.provide(Owner)))

  it.effect("streams commit invalidations with handshake coverage", () => {
    const Events = Layer.succeed(
      CommitPublisher.CommitPublisher,
      CommitPublisher.CommitPublisher.of({
        publishPending: Effect.succeed(0),
        invalidate: () => Effect.void,
        subscribe: Effect.succeed({
          watermark: Identity.CommitSequence.make(0),
          refreshGeneration: 0,
          events: Stream.make({
            _tag: "Commit" as const,
            commitSequence: Identity.CommitSequence.make(1),
            documentId,
            keys: [Task.name],
            refreshGeneration: 0
          })
        })
      })
    )
    const EventOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.mergeAll(Events, DeliveryPublisher, Layer.succeed(Replica.Replica, replica)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      assert.deepStrictEqual(Array.from(yield* client.invalidations.pipe(Stream.take(1), Stream.runCollect)), [{
        _tag: "Invalidation",
        ownerEpoch: client.ownerEpoch,
        sequence: Identity.CommitSequence.make(1),
        keys: [Task.name]
      }])
    })).pipe(Effect.provide(EventOwner))
  })

  it.effect("acquires a fresh commit subscription for every invalidation stream", () => {
    let subscriptions = 0
    const Events = Layer.succeed(
      CommitPublisher.CommitPublisher,
      CommitPublisher.CommitPublisher.of({
        publishPending: Effect.succeed(0),
        invalidate: () => Effect.void,
        subscribe: Effect.sync(() => {
          subscriptions++
          return {
            watermark: Identity.CommitSequence.make(subscriptions - 1),
            refreshGeneration: 0,
            events: Stream.make({
              _tag: "Commit" as const,
              commitSequence: Identity.CommitSequence.make(subscriptions),
              documentId,
              keys: [`subscription-${subscriptions}`],
              refreshGeneration: 0
            })
          }
        })
      })
    )
    const EventOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.mergeAll(Events, DeliveryPublisher, Layer.succeed(Replica.Replica, replica)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const first = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const second = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const events = yield* Effect.all([
        first.invalidations.pipe(Stream.take(1), Stream.runCollect),
        second.invalidations.pipe(Stream.take(1), Stream.runCollect)
      ], { concurrency: "unbounded" })
      assert.strictEqual(subscriptions, 2)
      assert.notDeepEqual(Array.from(events[0]), Array.from(events[1]))
    })).pipe(Effect.provide(EventOwner))
  })

  it.effect("retries transient invalidation failures and refreshes across a new baseline", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let subscriptions = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.unwrap(Effect.sync(() => {
              subscriptions++
              return subscriptions === 1
                ? Stream.make({
                  _tag: "InvalidationsReady" as const,
                  ownerEpoch,
                  watermark: Identity.CommitSequence.make(0),
                  refreshGeneration: 0,
                  deliveryWatermark: 0,
                  deliveryRefreshEpoch: 0
                }).pipe(Stream.concat(Stream.fail(disconnected())))
                : Stream.make({
                  _tag: "InvalidationsReady" as const,
                  ownerEpoch,
                  watermark: Identity.CommitSequence.make(2),
                  refreshGeneration: 0,
                  deliveryWatermark: 0,
                  deliveryRefreshEpoch: 0
                })
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const fiber = yield* client.invalidations.pipe(Stream.take(1), Stream.runCollect, Effect.forkChild)
      yield* TestClock.adjust(1_000)
      assert.deepStrictEqual(Array.from(yield* Fiber.join(fiber)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: allInvalidationKeys
      }])
      assert.strictEqual(subscriptions, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("requires a full refresh from an initial sticky refresh generation", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const sticky = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.make({
              _tag: "InvalidationsReady" as const,
              ownerEpoch,
              watermark: Identity.CommitSequence.make(0),
              refreshGeneration: 1,
              deliveryWatermark: 0,
              deliveryRefreshEpoch: 0
            })
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, sticky)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: allInvalidationKeys
      }])
    })).pipe(Effect.provide(Owner)))

  it.effect("requires a full refresh when the initial invalidation baseline is already ahead", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ahead = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.make({
              _tag: "InvalidationsReady" as const,
              ownerEpoch,
              watermark: Identity.CommitSequence.make(1),
              refreshGeneration: 0,
              deliveryWatermark: 0,
              deliveryRefreshEpoch: 0
            })
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, ahead)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: allInvalidationKeys
      }])
    })).pipe(Effect.provide(Owner)))

  it.effect("resets invalidation reconnect attempts after each ready message", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let subscriptions = 0
      const disconnectedAfterReady = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.unwrap(Effect.sync(() => {
              subscriptions++
              const ready = Stream.make({
                _tag: "InvalidationsReady" as const,
                ownerEpoch,
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0,
                deliveryWatermark: 0,
                deliveryRefreshEpoch: 0
              })
              return subscriptions < 5
                ? ready.pipe(Stream.concat(Stream.fail(disconnected())))
                : ready.pipe(Stream.concat(Stream.make({
                  _tag: "Invalidation" as const,
                  ownerEpoch,
                  sequence: Identity.CommitSequence.make(1),
                  keys: [Task.name]
                })))
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, disconnectedAfterReady)
      const fiber = yield* client.invalidations.pipe(Stream.take(1), Stream.runCollect, Effect.forkChild)
      yield* TestClock.adjust(10_000)
      assert.deepStrictEqual(Array.from(yield* Fiber.join(fiber)), [{
        _tag: "Invalidation",
        ownerEpoch: client.ownerEpoch,
        sequence: Identity.CommitSequence.make(1),
        keys: [Task.name]
      }])
      assert.strictEqual(subscriptions, 5)
    })).pipe(Effect.provide(Owner)))

  it.effect("continues renewing after a transient failure burst", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let renewals = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property !== "RenewSession") return value
          return (payload: never) =>
            Effect.sync(() => ++renewals).pipe(
              Effect.flatMap((attempt) => attempt < 5 ? Effect.fail(disconnected()) : value(payload))
            )
        }
      })
      yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis + 1)
      assert.strictEqual(renewals, 5)
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("reopens after an owner restart and refreshes invalidations", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const invalidationsStarted = yield* Deferred.make<void>()
      const ownerRestarted = yield* Deferred.make<void>()
      let openSessions = 0
      let restarted = false
      const restarting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) =>
              Effect.sync(() => ++openSessions).pipe(
                Effect.flatMap((generation) =>
                  value(payload).pipe(Effect.map((lease: object) => ({ ...lease, ownerEpoch: `owner-${generation}` })))
                )
              )
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              restarted && openSessions === 1
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "active session",
                      observed: payload.sessionId
                    })
                  })
                )
                : value(payload)
          }
          if (property === "Invalidations") {
            return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
              Stream.make({
                _tag: "InvalidationsReady" as const,
                ownerEpoch,
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0,
                deliveryWatermark: 0,
                deliveryRefreshEpoch: 0
              }).pipe(
                Stream.tap(() => Deferred.succeed(invalidationsStarted, undefined)),
                Stream.concat(
                  ownerEpoch === "owner-1"
                    ? Stream.fromEffect(Deferred.await(ownerRestarted)).pipe(
                      Stream.flatMap(() =>
                        Stream.fail(
                          new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "active session",
                              observed: "owner restarted"
                            })
                          })
                        )
                      )
                    )
                    : Stream.never
                )
              )
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, restarting)
      const initialOwnerEpoch = client.ownerEpoch
      const invalidation = yield* client.invalidations.pipe(Stream.take(1), Stream.runCollect, Effect.forkChild)
      yield* Deferred.await(invalidationsStarted)
      restarted = true
      assert.deepStrictEqual(yield* client.get(Task, documentId), yield* replica.get(Task, documentId))
      yield* Deferred.succeed(ownerRestarted, undefined)
      assert.strictEqual(openSessions, 2)
      assert.notStrictEqual(client.ownerEpoch, initialOwnerEpoch)
      assert.deepStrictEqual(Array.from(yield* Fiber.join(invalidation)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: allInvalidationKeys
      }])
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("preserves a healthy session after a local protocol mismatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      let gets = 0
      const locallyRejected = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) =>
              Effect.sync(() => {
                openSessions++
              }).pipe(Effect.andThen(value(payload)))
          }
          if (property === "Get") {
            return (payload: never) =>
              Effect.sync(() => ++gets).pipe(
                Effect.flatMap((attempt) =>
                  attempt === 1
                    ? Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: "bounded conflict response",
                          observed: "invalid local value"
                        })
                      })
                    )
                    : value(payload)
                )
              )
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, locallyRejected)

      const error = yield* Effect.flip(client.get(Task, documentId))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.strictEqual(openSessions, 1)
      assert.strictEqual(gets, 1)

      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
      assert.strictEqual(openSessions, 1)
      assert.strictEqual(gets, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("rejects an invalid replacement without dispatching or replacing the session", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      let resolutions = 0
      const observed = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) =>
              Effect.sync(() => {
                openSessions++
              }).pipe(Effect.andThen(value(payload)))
          }
          if (property === "ResolveConflict") {
            return (payload: never) =>
              Effect.sync(() => {
                resolutions++
              }).pipe(Effect.andThen(value(payload)))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, observed)
      const invalid = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "ReplaceValue", value: Number.NaN }
      } as never

      const error = yield* Effect.flip(
        client.resolveConflict(Task, {
          commandId: (yield* Identity.makeCommandId),
          documentId,
          resolution: invalid
        })
      )
      if (!Schema.is(ReplicaError.ReplicaError)(error)) {
        assert.fail(`Expected ReplicaError, got ${error._tag}`)
      }
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.strictEqual(openSessions, 1)
      assert.strictEqual(resolutions, 0)
      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
      assert.strictEqual(openSessions, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("serializes concurrent session reopen attempts", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const staleRequests = yield* Deferred.make<void>()
      let initialSessionId: Identity.SessionId | undefined
      let openSessions = 0
      let staleGets = 0
      let restarted = false
      const restarting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              openSessions++
              if (initialSessionId === undefined) initialSessionId = payload.sessionId
              return value(payload)
            }
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              if (!restarted || payload.sessionId !== initialSessionId) return value(payload)
              staleGets++
              return (staleGets === 2 ? Deferred.succeed(staleRequests, undefined) : Effect.void).pipe(
                Effect.andThen(Deferred.await(staleRequests)),
                Effect.andThen(Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "active session",
                      observed: payload.sessionId
                    })
                  })
                ))
              )
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, restarting)
      restarted = true
      const snapshots = yield* Effect.all([
        client.get(Task, documentId),
        client.get(Task, documentId)
      ], { concurrency: "unbounded" })
      assert.strictEqual(snapshots[0].documentId, documentId)
      assert.strictEqual(snapshots[1].documentId, documentId)
      assert.strictEqual(openSessions, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("reuses a session id across ambiguous reopen attempts", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let initialSessionId: Identity.SessionId | undefined
      let openSessions = 0
      const openedSessionIds: Array<Identity.SessionId> = []
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              openSessions++
              openedSessionIds.push(payload.sessionId)
              if (initialSessionId === undefined) initialSessionId = payload.sessionId
              const opened = value(payload)
              return openSessions === 2 || openSessions === 3
                ? opened.pipe(Effect.andThen(Effect.fail(disconnected())))
                : opened
            }
          }
          if (property === "CloseSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId !== initialSessionId && openSessions <= 3
                ? Effect.fail(disconnected())
                : value(payload)
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId === initialSessionId
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "active session",
                      observed: payload.sessionId
                    })
                  })
                )
                : value(payload)
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const snapshot = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* TestClock.adjust("2 seconds")
      assert.strictEqual((yield* Fiber.join(snapshot)).documentId, documentId)
      assert.strictEqual(openSessions, 4)
      assert.notStrictEqual(openedSessionIds[1], openedSessionIds[0])
      assert.strictEqual(openedSessionIds[2], openedSessionIds[1])
      assert.strictEqual(openedSessionIds[3], openedSessionIds[1])
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("requires a full refresh for sequence gaps and discards stale owner events", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const withGap = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.make(
              {
                _tag: "Invalidation" as const,
                ownerEpoch: "stale-owner",
                sequence: Identity.CommitSequence.make(1),
                keys: [Task.name]
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(2),
                keys: [Task.name]
              }
            )
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, withGap)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: allInvalidationKeys
      }])
    })).pipe(Effect.provide(Owner)))

  it.effect("accepts new invalidations after a full refresh resets the commit sequence", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const reset = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.make(
              {
                _tag: "InvalidationsReady" as const,
                ownerEpoch,
                watermark: Identity.CommitSequence.make(5),
                refreshGeneration: 0,
                deliveryWatermark: 0,
                deliveryRefreshEpoch: 0
              },
              {
                _tag: "FullRefreshRequired" as const,
                ownerEpoch,
                keys: [Task.name]
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(1),
                keys: [Task.name]
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(2),
                keys: [Task.name]
              }
            )
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reset)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: allInvalidationKeys
        },
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: [Task.name]
        },
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: allInvalidationKeys
        },
        {
          _tag: "Invalidation",
          ownerEpoch: client.ownerEpoch,
          sequence: Identity.CommitSequence.make(2),
          keys: [Task.name]
        }
      ])
    })).pipe(Effect.provide(Owner)))

  it.effect("preserves a definite domain rejection", () => {
    let lookups = 0
    return Effect.gen(function*() {
      const unknownCommandId = yield* Identity.makeCommandId
      const rejected: Replica.Replica["Service"] = {
        ...replica,
        // A declared rejection is now an ordinary typed failure from the replica itself.
        mutate: () => Effect.fail(new RenameError()) as never,
        lookupMutation: () =>
          Effect.sync(() => {
            lookups++
            return CommandOutcome.unknown(unknownCommandId)
          }) as never
      }
      const RejectedOwner = ReplicaOwner.layerHandlers(definition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(Sessions),
        Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, rejected)))
      )
      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
        const commandId = yield* Identity.makeCommandId
        // The declared rejection arrives unwrapped, so `catchTag` on its own tag is enough.
        assert.deepStrictEqual(
          yield* Effect.flip(client.mutate(Rename, { commandId, documentId, payload: { title: "next" } })),
          new RenameError()
        )
        assert.strictEqual(lookups, 0)
      })).pipe(Effect.provide(RejectedOwner))
    })

    it.effect("preserves and manually looks up a durable stale conflict rejection", () => {
      let lookups = 0
      const resolution = Conflict.Resolution.make({
        heads: ["expected"],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "DeleteValue" }
      })
      const rejection = new Conflict.StaleConflictResolution({
        expectedHeads: ["expected"],
        observedHeads: ["observed"]
      })
      const rejected: Replica.Replica["Service"] = {
        ...replica,
        resolveConflict: () => Effect.fail(rejection),
        lookupConflictResolution: (_document, options) =>
          Effect.sync(() => {
            lookups++
            return CommandOutcome.rejected(options.commandId, rejection)
          })
      }
      const RejectedOwner = ReplicaOwner.layerHandlers(definition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(Sessions),
        Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, rejected)))
      )
      return Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
        const commandId = yield* Identity.makeCommandId
        const error = yield* Effect.flip(
          client.resolveConflict(Task, { commandId, documentId, resolution })
        )
        assert.deepStrictEqual(error, rejection)
        assert.strictEqual(lookups, 0)
        assert.deepStrictEqual(
          yield* client.lookupConflictResolution(Task, { commandId, documentId, resolution }),
          CommandOutcome.rejected(commandId, rejection)
        )
        assert.strictEqual(lookups, 1)
      })).pipe(Effect.provide(RejectedOwner))
    })
  })

  it.effect("rejects operations without an active session", () =>
    Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const error = yield* Effect.flip(rpc.Get({
        sessionId: (yield* Identity.makeSessionId),
        document: Task.name,
        documentId
      }))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    }).pipe(Effect.provide(Owner)))

  it.effect("binds sessions to the transport client", () =>
    Effect.scoped(Effect.gen(function*() {
      const open = yield* ReplicaRpc.group.accessHandler("OpenSession")
      const renew = yield* ReplicaRpc.group.accessHandler("RenewSession")
      const close = yield* ReplicaRpc.group.accessHandler("CloseSession")
      const get = yield* ReplicaRpc.group.accessHandler("Get")
      const status = yield* ReplicaRpc.group.accessHandler("Status")
      const sessionId = yield* Identity.makeSessionId
      const owner = new Rpc.ServerClient(1)
      const other = new Rpc.ServerClient(2)
      const options = (client: Rpc.ServerClient, requestId: string) => ({
        client,
        requestId: RequestId(requestId),
        headers: Headers.empty
      })
      const unary = <A, E, R,>(effect: Effect.Effect<A | Deferred.Deferred<A, E>, E, R>) =>
        Effect.flatMap(effect, (value) =>
          Deferred.isDeferred<A, E>(value) ? Deferred.await(value) : Effect.succeed(value))

      // A tab old enough to omit the field entirely is still refused with a reason it can read,
      // which is only reachable because the payload field stays optional rather than a literal.
      const legacySessionId = yield* Identity.makeSessionId
      assert.strictEqual(
        (yield* Effect.flip(unary(open({
          sessionId: legacySessionId,
          definitionHash: definition.hash
        }, options(owner, "open-legacy"))))).reason._tag,
        "ProtocolMismatch"
      )

      yield* unary(open({
        sessionId,
        protocolVersion: ReplicaRpc.protocolVersion,
        definitionHash: definition.hash
      }, options(owner, "open")))

      assert.strictEqual(
        (yield* Effect.flip(unary(open({
          sessionId,
          protocolVersion: ReplicaRpc.protocolVersion,
          definitionHash: definition.hash
        }, options(other, "open-other")))))
          .reason._tag,
        "ProtocolMismatch"
      )
      assert.strictEqual(
        (yield* Effect.flip(unary(renew({ sessionId }, options(other, "renew-other"))))).reason._tag,
        "ProtocolMismatch"
      )
      assert.strictEqual(
        (yield* Effect.flip(unary(close({ sessionId }, options(other, "close-other"))))).reason._tag,
        "ProtocolMismatch"
      )
      assert.strictEqual(
        (yield* Effect.flip(unary(get(
          { sessionId, document: Task.name, documentId },
          options(other, "get-other")
        )))).reason._tag,
        "ProtocolMismatch"
      )

      const otherStatus = status({ sessionId }, options(other, "status-other"))
      assert.isTrue(Stream.isStream(otherStatus))
      const streamError = yield* (otherStatus as Stream.Stream<unknown, ReplicaError.ReplicaError>).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(streamError.reason._tag, "ProtocolMismatch")

      const snapshot = yield* unary(get(
        { sessionId, document: Task.name, documentId },
        options(owner, "get-owner")
      ))
      assert.strictEqual(snapshot.documentId, documentId)
      const ownerStatus = status({ sessionId }, options(owner, "status-owner"))
      assert.isTrue(Stream.isStream(ownerStatus))
      assert.lengthOf(
        Array.from(yield* Stream.runCollect(ownerStatus as Stream.Stream<unknown, ReplicaError.ReplicaError>)),
        1
      )
      yield* unary(close({ sessionId }, options(owner, "close-owner")))
    })).pipe(Effect.provide(Owner)))

  it.effect("transfers backup bytes through the owner", () => {
    let restored: ReadonlyArray<Uint8Array> = []
    const BackupOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runCollect(source).pipe(
              Effect.map((chunks) => {
                restored = Array.from(chunks)
              })
            )
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const exported = Array.from(yield* Stream.runCollect(client.exportBackup({ maxBytes: 1024 })))
      assert.deepStrictEqual(exported, [Uint8Array.of(1, 2, 3)])
      yield* client.restoreBackup({
        source: Stream.fromIterable(exported),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_5f0a6f45-9be2-4c7a-8f45-1d2f3a4b5c6d")
      })
      assert.deepStrictEqual(restored, exported)
      restored = []
      const oversized = yield* Effect.flip(client.restoreBackup({
        source: Stream.fromIterable([new Uint8Array(700), new Uint8Array(700)]),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_7c1b2d38-6e4f-4b9a-9c2d-3e4f5a6b7c8d")
      }))
      assert.strictEqual(oversized.reason._tag, "BackupTooLarge")
      assert.deepStrictEqual(restored, [])

      const invalid = yield* Effect.flip(client.restoreBackup({
        source: Stream.make(Uint8Array.of(1)).pipe(
          Stream.concat(Stream.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause: new Error("source tail was pulled") })
            })
          ))
        ),
        mode: "replace",
        maxBytes: Number.NaN,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_2a9c4e17-8d3b-4f6a-b5e8-9c0d1e2f3a4b")
      }))
      assert.strictEqual(invalid.reason._tag, "BackupInvalid")
      assert.deepStrictEqual(restored, [])
    })).pipe(Effect.provide(BackupOwner))
  })

  it.effect("transfers a selective backup installation through the owner", () => {
    let installed:
      | {
        readonly document: Document.Any
        readonly documentId: Identity.DocumentId
        readonly chunks: ReadonlyArray<Uint8Array>
        readonly expectedDefinitionHash: string
        readonly installationId: Identity.BackupInstallationId
        readonly maxBytes: number
      }
      | undefined
    const BackupOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          installBackupDocument: (document, { documentId, expectedDefinitionHash, installationId, maxBytes, source }) =>
            Stream.runCollect(source).pipe(
              Effect.map((chunks) => {
                installed = {
                  document,
                  documentId,
                  chunks: Array.from(chunks),
                  expectedDefinitionHash,
                  installationId,
                  maxBytes
                }
              })
            )
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const chunks = [Uint8Array.of(1, 2), Uint8Array.of(3)]
      yield* client.installBackupDocument(Task, {
        source: Stream.fromIterable(chunks),
        documentId,
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_85f269a1-b6f1-48f9-b658-96d75be94c71")
      })

      assert.strictEqual(installed?.document, Task)
      assert.strictEqual(installed?.documentId, documentId)
      assert.strictEqual(installed?.expectedDefinitionHash, definition.hash)
      assert.strictEqual(installed?.installationId, "bak_85f269a1-b6f1-48f9-b658-96d75be94c71")
      assert.strictEqual(installed?.maxBytes, 1024)
      assert.deepStrictEqual(
        installed?.chunks.flatMap((chunk) => Array.from(chunk)),
        chunks.flatMap((chunk) => Array.from(chunk))
      )
    })).pipe(Effect.provide(BackupOwner))
  })

  it.effect("applies maxBytes exactly once and rejects maxBytes plus one without applying", () => {
    const maxBytes = 1_024
    const applications: Array<ReadonlyArray<Uint8Array>> = []
    const BoundaryOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runCollect(source).pipe(
              Effect.tap((chunks) =>
                Effect.sync(() => {
                  applications.push(Array.from(chunks))
                })
              ),
              Effect.asVoid
            )
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      yield* client.restoreBackup({
        source: Stream.make(new Uint8Array(maxBytes)),
        mode: "replace",
        maxBytes,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_36b854b7-100d-4dcc-bcae-05fb2a829121")
      })
      assert.strictEqual(applications.length, 1)
      assert.strictEqual(
        applications[0]?.reduce((total, chunk) => total + chunk.byteLength, 0),
        maxBytes
      )

      const error = yield* client.restoreBackup({
        source: Stream.make(new Uint8Array(maxBytes + 1)),
        mode: "replace",
        maxBytes,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_b3293647-175a-415c-890e-3cd8ac7e86e8")
      }).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "BackupTooLarge")
      if (error.reason._tag === "BackupTooLarge") {
        assert.strictEqual(error.reason.limit, maxBytes)
        assert.strictEqual(error.reason.observed, maxBytes + 1)
      }
      assert.strictEqual(applications.length, 1)
    })).pipe(Effect.provide(BoundaryOwner))
  })

  it.effect("returns the encoded Finish success when TerminalReady is lost", () => {
    const ResultOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) => Stream.runDrain(source)
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const finishDelivered = yield* Deferred.make<void>()
      const terminalDropped = yield* Deferred.make<void>()
      const bridged = dropTerminalReady(
        rpc,
        terminalDropped,
        ((payload) =>
          rpc.FinishRestoreBackup(payload).pipe(
            Effect.tap(() => Deferred.succeed(finishDelivered, undefined))
          )) as FinishRestore
      )
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        bridged,
        { operationTimeout: "1 second" }
      )
      const restore = yield* client.restoreBackup({
        source: Stream.empty,
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_114c1fb0-4d69-4ed8-858c-1e844c673456")
      }).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(finishDelivered)
      yield* Deferred.await(terminalDropped)
      yield* TestClock.adjust("1 second")
      const exit = yield* Fiber.await(restore)
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      assert.strictEqual(yield* (yield* SessionManager.SessionManager).activeRestoreCount, 0)
      return yield* exit
    })).pipe(Effect.provide(ResultOwner))
  })

  interface TerminalCauseScenario {
    readonly name: string
    readonly cause: Cause.Cause<ReplicaError.ReplicaError>
    readonly expectedTags: ReadonlyArray<Cause.Reason<ReplicaError.ReplicaError>["_tag"]>
    readonly expectedFailureTags: ReadonlyArray<ReplicaError.ReplicaError["reason"]["_tag"]>
  }

  const lostTerminalCauseScenarios: ReadonlyArray<TerminalCauseScenario> = [
    {
      name: "preserves a typed Finish failure when TerminalReady is lost",
      cause: Cause.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.RestoreBusy({ replica: "typed-lost-terminal" })
        })
      ),
      expectedTags: ["Fail"],
      expectedFailureTags: ["RestoreBusy"]
    },
    {
      name: "preserves a composite Finish failure when TerminalReady is lost",
      cause: Cause.fromReasons<ReplicaError.ReplicaError>([
        Cause.makeFailReason(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.RestoreBusy({ replica: "composite-lost-terminal" })
          })
        ),
        Cause.makeDieReason(new Error("composite restore defect"))
      ]),
      expectedTags: ["Fail", "Die"],
      expectedFailureTags: ["RestoreBusy"]
    }
  ]

  for (const [index, scenario] of lostTerminalCauseScenarios.entries()) {
    it.effect(scenario.name, () => {
      const ResultOwner = ReplicaOwner.layerHandlers(definition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(Sessions),
        Layer.provide(Layer.merge(
          Publisher,
          Layer.succeed(Replica.Replica, {
            ...replica,
            restoreBackup: ({ source }) =>
              Stream.runDrain(source).pipe(
                Effect.andThen(Effect.failCause(scenario.cause))
              )
          })
        ))
      )
      return Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const terminalDropped = yield* Deferred.make<void>()
        const client = yield* ReplicaClient.fromRpcClient(
          definition,
          dropTerminalReady(rpc, terminalDropped),
          { operationTimeout: "1 second" }
        )
        const restore = yield* client.restoreBackup({
          source: Stream.empty,
          mode: "replace",
          maxBytes: 1024,
          expectedDefinitionHash: definition.hash,
          installationId: Identity.BackupInstallationId.make(
            `bak_821390d7-e9af-4edb-b26a-10000000000${index + 1}`
          )
        }).pipe(Effect.forkChild({ startImmediately: true }))

        yield* Deferred.await(terminalDropped)
        yield* TestClock.adjust("1 second")
        const exit = yield* Fiber.await(restore)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isSuccess(exit)) return
        assert.deepStrictEqual(
          exit.cause.reasons.map((reason) => reason._tag),
          scenario.expectedTags
        )
        assert.deepStrictEqual(
          exit.cause.reasons
            .filter(Cause.isFailReason)
            .map((reason) => reason.error.reason._tag),
          scenario.expectedFailureTags
        )
        yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
      })).pipe(Effect.provide(ResultOwner))
    })
  }

  it.effect("does not accept a Finish RpcClientError when TerminalReady is lost", () => {
    const ResultOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) => Stream.runDrain(source)
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const finishScope = yield* Scope.Scope
      const terminalDropped = yield* Deferred.make<void>()
      const completed = yield* Deferred.make<
        Exit.Exit<void, ReplicaError.ReplicaError>
      >()
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        dropTerminalReady(
          rpc,
          terminalDropped,
          (payload) =>
            rpc.FinishRestoreBackup(payload).pipe(
              Effect.forkIn(finishScope, { startImmediately: true }),
              Effect.andThen(Effect.fail(disconnected()))
            )
        ),
        { operationTimeout: "1 second" }
      )
      yield* client.restoreBackup({
        source: Stream.empty,
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_6a7d87ae-b296-4d68-94c9-28e7fa3e402d")
      }).pipe(
        Effect.onExit((exit) => Deferred.succeed(completed, exit)),
        Effect.forkChild({ startImmediately: true })
      )

      yield* Deferred.await(terminalDropped)
      yield* TestClock.adjust("999 millis")
      assert.isFalse(yield* Deferred.isDone(completed))
      yield* TestClock.adjust("1 millis")
      const exit = yield* Deferred.await(completed)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isSuccess(exit)) return
      const failures = exit.cause.reasons.filter(Cause.isFailReason)
      assert.strictEqual(failures.length, 1)
      const error = failures[0]?.error
      assert.strictEqual(error?._tag, "ReplicaError")
      if (error?._tag === "ReplicaError") {
        assert.strictEqual(error.reason._tag, "OperationTimeout")
      }
      yield* TestClock.adjust(limits.maxRestorePullMillis + 1)
    })).pipe(Effect.provide(ResultOwner))
  })

  const terminalCauseScenarios: ReadonlyArray<TerminalCauseScenario> = [
    {
      name: "preserves two typed restore failures through the public client",
      cause: Cause.fromReasons([
        Cause.makeFailReason(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.RestoreBusy({ replica: "first" })
          })
        ),
        Cause.makeFailReason(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({ resource: "restores", limit: 2 })
          })
        )
      ]),
      expectedTags: ["Fail", "Fail"],
      expectedFailureTags: ["RestoreBusy", "QuotaExceeded"]
    },
    {
      name: "preserves a typed restore failure and defect through the public client",
      cause: Cause.fromReasons([
        Cause.makeFailReason(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.RestoreBusy({ replica: "typed" })
          })
        ),
        Cause.makeDieReason(new Error("secret sql SELECT credential=/private/archive"))
      ]),
      expectedTags: ["Fail", "Die"],
      expectedFailureTags: ["RestoreBusy"]
    },
    {
      name: "preserves a typed restore failure and interrupt through the public client",
      cause: Cause.fromReasons([
        Cause.makeFailReason(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.RestoreBusy({ replica: "typed" })
          })
        ),
        Cause.makeInterruptReason(987)
      ]),
      expectedTags: ["Fail", "Interrupt"],
      expectedFailureTags: ["RestoreBusy"]
    },
    {
      name: "preserves a pure defect through the public client",
      cause: Cause.fromReasons([
        Cause.makeDieReason(new Error("secret password=/private/archive"))
      ]),
      expectedTags: ["Die"],
      expectedFailureTags: []
    },
    {
      name: "preserves more than eight distinct reasons through the public client",
      cause: Cause.fromReasons(
        Array.from({ length: 9 }, (_, reasonIndex) =>
          Cause.makeFailReason(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.RestoreBusy({ replica: `replica-${reasonIndex}` })
            })
          ))
      ),
      expectedTags: Array.from({ length: 9 }, () => "Fail" as const),
      expectedFailureTags: Array.from({ length: 9 }, () => "RestoreBusy" as const)
    }
  ]

  for (const [index, scenario] of terminalCauseScenarios.entries()) {
    it.effect(scenario.name, () => {
      const failureLimits = {
        ...limits,
        maxBackupBytes: 64 * 1024
      } satisfies ReplicaLimits.Values
      const FailureSessions = SessionManager.layer.pipe(
        Layer.provide(ReplicaLimits.layer(failureLimits))
      )
      const failureDefinition = ReplicaDefinition.make({
        name: `terminal-cause-${index}`,
        documents: DocumentSet.make(Task),
        mutations: [],
        projections: [],
        queries: []
      })
      const Sql = SqlReplica.layerWithBindings(failureDefinition, { projections: [] }).pipe(
        Layer.provide(Layer.merge(
          SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
          ReplicaLimits.layer(failureLimits)
        ))
      )
      return Effect.scoped(Effect.gen(function*() {
        const sql = yield* Layer.build(Sql)
        const productionReplica = Context.get(sql, Replica.Replica)
        const publisher = Context.get(sql, CommitPublisher.CommitPublisher)
        const archive = Array.from(
          yield* Stream.runCollect(
            productionReplica.exportBackup({ maxBytes: failureLimits.maxBackupBytes })
          )
        )
        const decoratedReplica = Replica.Replica.of({
          ...productionReplica,
          restoreBackup: (options) =>
            productionReplica.restoreBackup(options).pipe(
              Effect.andThen(Effect.failCause(scenario.cause))
            )
        })
        const FailingOwner = ReplicaOwner.layerHandlers(failureDefinition).pipe(
          Layer.provide(PeerConnectionStatus.layer),
          Layer.provide(RelayConnectionStatus.layerNotConfigured),
          Layer.provideMerge(FailureSessions),
          Layer.provide(Layer.mergeAll(
            Layer.succeed(CommitPublisher.CommitPublisher, publisher),
            DeliveryPublisher,
            Layer.succeed(Replica.Replica, decoratedReplica)
          ))
        )
        yield* Effect.scoped(Effect.gen(function*() {
          const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
          const client = yield* ReplicaClient.fromRpcClient(failureDefinition, rpc)
          const exit = yield* client.restoreBackup({
            source: Stream.fromIterable(archive),
            mode: "replace",
            maxBytes: failureLimits.maxBackupBytes,
            expectedDefinitionHash: failureDefinition.hash,
            installationId: Identity.BackupInstallationId.make(
              `bak_00000000-0000-4000-8000-00000000000${index + 1}`
            )
          }).pipe(Effect.exit)

          assert.isTrue(Exit.isFailure(exit))
          if (Exit.isSuccess(exit)) return
          assert.deepStrictEqual(
            exit.cause.reasons.map((reason) => reason._tag),
            scenario.expectedTags
          )
          assert.deepStrictEqual(
            exit.cause.reasons
              .filter(Cause.isFailReason)
              .map((reason) => reason.error.reason._tag),
            scenario.expectedFailureTags
          )
          const interrupt = exit.cause.reasons.find(Cause.isInterruptReason)
          if (interrupt !== undefined) assert.strictEqual(interrupt.fiberId, 987)
        })).pipe(Effect.provide(FailingOwner))
      }))
    })
  }

  it.effect("dispatches restore before the source completes", () => {
    const backupLimits = {
      ...limits,
      maxBackupBytes: 64 * 1024
    } satisfies ReplicaLimits.Values
    const BackupSessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(backupLimits)))
    const backupDefinition = ReplicaDefinition.make({
      name: "streaming-restore",
      documents: DocumentSet.make(Task),
      mutations: [],
      projections: [],
      queries: []
    })
    const Sql = SqlReplica.layerWithBindings(backupDefinition, { projections: [] }).pipe(
      Layer.provide(Layer.merge(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        ReplicaLimits.layer(backupLimits)
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const sql = yield* Layer.build(Sql)
      const productionReplica = Context.get(sql, Replica.Replica)
      const publisher = Context.get(sql, CommitPublisher.CommitPublisher)
      const archiveChunks = Array.from(
        yield* Stream.runCollect(productionReplica.exportBackup({ maxBytes: backupLimits.maxBackupBytes }))
      )
      const archiveLength = archiveChunks.reduce((length, chunk) => length + chunk.byteLength, 0)
      const archive = new Uint8Array(archiveLength)
      let offset = 0
      for (const chunk of archiveChunks) {
        archive.set(chunk, offset)
        offset += chunk.byteLength
      }
      assert.isAbove(archive.byteLength, 1)

      const outerDispatched = yield* Deferred.make<void>()
      const tailRequested = yield* Deferred.make<void>()
      const ownerConsumedFirst = yield* Deferred.make<void>()
      const observedReplica = Replica.Replica.of({
        ...productionReplica,
        restoreBackup: (options) =>
          productionReplica.restoreBackup({
            ...options,
            source: options.source.pipe(
              Stream.tap(() => Deferred.succeed(ownerConsumedFirst, undefined))
            )
          })
      })
      const ProductionOwner = ReplicaOwner.layerHandlers(backupDefinition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(BackupSessions),
        Layer.provide(Layer.mergeAll(
          DeliveryPublisher,
          Layer.succeed(CommitPublisher.CommitPublisher, publisher),
          Layer.succeed(Replica.Replica, observedReplica)
        ))
      )

      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const observedRpc = new Proxy(rpc, {
          get(target, property, receiver) {
            const value = Reflect.get(target, property, receiver)
            if (property !== "RestoreBackup" && property !== "BeginRestoreBackup") return value
            return (payload: never) =>
              Deferred.succeed(outerDispatched, undefined).pipe(
                Effect.andThen(value(payload))
              )
          }
        })
        const client = yield* ReplicaClient.fromRpcClient(backupDefinition, observedRpc)
        const source = Stream.make(archive.slice(0, 1)).pipe(
          Stream.concat(Stream.fromEffect(
            Deferred.succeed(tailRequested, undefined).pipe(
              Effect.andThen(Deferred.await(ownerConsumedFirst)),
              Effect.as(archive.slice(1))
            )
          ))
        )
        const restore = yield* client.restoreBackup({
          source,
          mode: "replace",
          maxBytes: backupLimits.maxBackupBytes,
          expectedDefinitionHash: backupDefinition.hash,
          installationId: Identity.BackupInstallationId.make("bak_43c8d2f4-58ce-4c9a-9155-9d21019f5e9d")
        }).pipe(Effect.forkChild)

        yield* Deferred.await(tailRequested)
        assert.isTrue(yield* Deferred.isDone(outerDispatched))
        yield* TestClock.adjust(25)
        yield* Fiber.join(restore)
      })).pipe(Effect.provide(ProductionOwner))
    }))
  })

  it.effect("accepts session expiry after a Pull is already outstanding", () => {
    const backupLimits = {
      ...limits,
      maxBackupBytes: 64 * 1024
    } satisfies ReplicaLimits.Values
    const BackupSessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(backupLimits)))
    const backupDefinition = ReplicaDefinition.make({
      name: "restore-expiry",
      documents: DocumentSet.make(Task),
      mutations: [],
      projections: [],
      queries: []
    })
    const Sql = SqlReplica.layerWithBindings(backupDefinition, { projections: [] }).pipe(
      Layer.provide(Layer.merge(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        ReplicaLimits.layer(backupLimits)
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const sql = yield* Layer.build(Sql)
      const productionReplica = Context.get(sql, Replica.Replica)
      const publisher = Context.get(sql, CommitPublisher.CommitPublisher)
      const archiveChunks = Array.from(
        yield* Stream.runCollect(productionReplica.exportBackup({ maxBytes: backupLimits.maxBackupBytes }))
      )
      const archiveLength = archiveChunks.reduce((length, chunk) => length + chunk.byteLength, 0)
      const archive = new Uint8Array(archiveLength)
      let offset = 0
      for (const chunk of archiveChunks) {
        archive.set(chunk, offset)
        offset += chunk.byteLength
      }
      assert.isAbove(archive.byteLength, 1)

      const ownerConsumedFirst = yield* Deferred.make<void>()
      const observedReplica = Replica.Replica.of({
        ...productionReplica,
        restoreBackup: (options) =>
          productionReplica.restoreBackup({
            ...options,
            source: options.source.pipe(
              Stream.tap(() => Deferred.succeed(ownerConsumedFirst, undefined))
            )
          })
      })
      const ProductionOwner = ReplicaOwner.layerHandlers(backupDefinition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(BackupSessions),
        Layer.provide(Layer.mergeAll(
          DeliveryPublisher,
          Layer.succeed(CommitPublisher.CommitPublisher, publisher),
          Layer.succeed(Replica.Replica, observedReplica)
        ))
      )

      yield* Effect.scoped(Effect.gen(function*() {
        const sessions = yield* SessionManager.SessionManager
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const opened = yield* Deferred.make<Identity.SessionId>()
        const firstPull = yield* Deferred.make<void>()
        const secondPull = yield* Deferred.make<void>()
        const observedRpc = new Proxy(rpc, {
          get(target, property, receiver) {
            const value = Reflect.get(target, property, receiver)
            if (property === "OpenSession") {
              return (payload: { readonly sessionId: Identity.SessionId }) =>
                Deferred.succeed(opened, payload.sessionId).pipe(Effect.andThen(value(payload)))
            }
            if (property === "BeginRestoreBackup") {
              return (payload: Parameters<typeof target.BeginRestoreBackup>[0]) =>
                target.BeginRestoreBackup(payload).pipe(
                  Effect.tap(({ port }) =>
                    Effect.sync(() => {
                      port.addEventListener("message", (event: MessageEvent<unknown>) => {
                        if (
                          typeof event.data !== "object" ||
                          event.data === null ||
                          Reflect.get(event.data, "_tag") !== "Pull"
                        ) {
                          return
                        }
                        const sequence = Reflect.get(event.data, "sequence")
                        if (sequence === 1) Deferred.doneUnsafe(firstPull, Effect.void)
                        if (sequence === 2) Deferred.doneUnsafe(secondPull, Effect.void)
                      })
                    })
                  )
                )
            }
            return value
          }
        })
        const client = yield* ReplicaClient.fromRpcClient(backupDefinition, observedRpc)
        const restore = yield* client.restoreBackup({
          source: Stream.make(archive.slice(0, 1)).pipe(
            Stream.concat(Stream.fromEffect(Effect.never))
          ),
          mode: "replace",
          maxBytes: backupLimits.maxBackupBytes,
          expectedDefinitionHash: backupDefinition.hash,
          installationId: Identity.BackupInstallationId.make("bak_830da692-45fb-4e48-bbbb-97d356c08eb3")
        }).pipe(Effect.flip, Effect.forkChild)

        const sessionId = yield* Deferred.await(opened)
        yield* Deferred.await(firstPull)
        yield* TestClock.adjust(backupLimits.maxRestoreCoalesceMillis)
        yield* Deferred.await(ownerConsumedFirst)
        yield* Deferred.await(secondPull)
        yield* sessions.close(sessionId, 0)
        const error = yield* Fiber.join(restore)
        yield* TestClock.adjust(backupLimits.maxRestorePullMillis + 1)
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        if (error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(error.reason.expected, "active session")
          assert.strictEqual(error.reason.observed, sessionId)
        }
      })).pipe(Effect.provide(ProductionOwner))
    }))
  })

  it.effect("surfaces ProtocolMismatch without replaying an in-flight restore", () => {
    let applications = 0
    let restoreCalls = 0
    let sourceSubscriptions = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runDrain(source).pipe(
              Effect.tap(() => Effect.sync(() => ++applications)),
              Effect.tap(() => Effect.sync(() => ++restoreCalls)),
              Effect.andThen(Effect.fail(protocolMismatch("restore")))
            )
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const openedSessionIds: Array<Identity.SessionId> = []
      const getSessionIds: Array<Identity.SessionId> = []
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              Effect.sync(() => openedSessionIds.push(payload.sessionId)).pipe(Effect.andThen(value(payload)))
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              getSessionIds.push(payload.sessionId)
              return value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const error = yield* client.restoreBackup({
        source: Stream.unwrap(Effect.sync(() => {
          sourceSubscriptions++
          return Stream.make(Uint8Array.of(1, 2, 3))
        })),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_5f0a6f45-9be2-4c7a-8f45-1d2f3a4b5c6d")
      }).pipe(Effect.flip)
      const snapshot = yield* client.get(Task, documentId)
      assert.strictEqual(snapshot.value.title, "stored")
      assert.strictEqual(restoreCalls, 1)
      assert.strictEqual(sourceSubscriptions, 1)
      assert.strictEqual(applications, 1)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "restore")
      assert.strictEqual(openedSessionIds.length, 1)
      assert.deepStrictEqual(getSessionIds, [openedSessionIds[0]])
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("surfaces ProtocolMismatch without replaying an in-flight document import", () => {
    let imports = 0
    let importCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          importDocument: () => Effect.sync(() => ++imports).pipe(Effect.as(documentId))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => Effect.sync(() => ++openSessions).pipe(Effect.andThen(value(payload)))
          }
          if (property === "ImportDocument") {
            return (payload: never) => {
              importCalls++
              return importCalls === 1
                ? value(payload).pipe(Effect.andThen(Effect.fail(
                  protocolMismatch("import")
                )))
                : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const commandId = yield* Identity.makeCommandId
      const error = yield* client.importDocument(Task, {
        commandId,
        value: { documentName: Task.name, schemaVersion: Task.version, value: { title: "stored" } }
      }).pipe(Effect.flip)
      assert.strictEqual(importCalls, 1)
      assert.strictEqual(imports, 1)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "import")
      assert.strictEqual(openSessions, 2)
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("does not replay import when ProtocolMismatch arrives before application", () => {
    let imports = 0
    let importCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          importDocument: () => Effect.sync(() => ++imports).pipe(Effect.as(documentId))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => Effect.sync(() => ++openSessions).pipe(Effect.andThen(value(payload)))
          }
          if (property === "ImportDocument") {
            return (payload: never) => {
              importCalls++
              return importCalls === 1 ? Effect.fail(protocolMismatch("before import")) : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const error = yield* client.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { documentName: Task.name, schemaVersion: Task.version, value: { title: "stored" } }
      }).pipe(Effect.flip)
      assert.strictEqual(importCalls, 1)
      assert.strictEqual(imports, 0)
      assert.strictEqual(openSessions, 2)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "before import")
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("waits for a transient reopen without replaying restore", () => {
    let applications = 0
    let restoreCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runDrain(source).pipe(Effect.tap(() => Effect.sync(() => ++applications)))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replacementFailed = yield* Deferred.make<void>()
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Deferred.succeed(replacementFailed, undefined).pipe(
                  Effect.andThen(Effect.fail(transientDisconnected()))
                )
                : value(payload)
            }
          }
          if (property === "BeginRestoreBackup") {
            return (payload: never) => {
              restoreCalls++
              return restoreCalls === 1
                ? Effect.fail(protocolMismatch("restore"))
                : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const fiber = yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_7c1b2d38-6e4f-4b9a-9c2d-3e4f5a6b7c8d")
      }).pipe(Effect.flip, Effect.forkChild)
      yield* Deferred.await(replacementFailed)
      assert.strictEqual(openSessions, 2)
      yield* TestClock.adjust("999 millis")
      assert.strictEqual(openSessions, 2)
      yield* TestClock.adjust("1 millis")
      const error = yield* Fiber.join(fiber)
      assert.strictEqual(openSessions, 3)
      assert.strictEqual(restoreCalls, 1)
      assert.strictEqual(applications, 0)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "restore")
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("bounds transient restore session replacement by the restore deadline", () => {
    let beginCalls = 0
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replacementFailed = yield* Deferred.make<void>()
      const completed = yield* Deferred.make<ReplicaError.ReplicaError>()
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 1
                ? value(payload)
                : Effect.fail(transientDisconnected()).pipe(
                  Effect.tapError(() => Deferred.succeed(replacementFailed, undefined))
                )
            }
          }
          if (property === "BeginRestoreBackup") {
            return () => {
              beginCalls++
              return Effect.fail(protocolMismatch("restore"))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(
        definition,
        faulted,
        { operationTimeout: "1 second" }
      )
      yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_9e214b56-413c-4c63-84a8-3f0105987ea1")
      }).pipe(
        Effect.flip,
        Effect.tap((error) => Deferred.succeed(completed, error)),
        Effect.forkChild
      )

      yield* Deferred.await(replacementFailed)
      yield* TestClock.adjust("999 millis")
      assert.isFalse(yield* Deferred.isDone(completed))
      yield* TestClock.adjust("1 millis")
      assert.isTrue(yield* Deferred.isDone(completed))
      const error = yield* Deferred.await(completed)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "RestoreBackup")
        assert.strictEqual(error.reason.timeoutMillis, 1_000)
      }
      assert.strictEqual(beginCalls, 1)
      assert.isAtLeast(openSessions, 2)
    })).pipe(Effect.provide(Owner))
  })

  it.effect("surfaces a fatal reopen error without replaying restore", () => {
    let applications = 0
    let restoreCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runDrain(source).pipe(Effect.tap(() => Effect.sync(() => ++applications)))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.QuotaExceeded({ resource: "sessions", limit: 2 })
                  })
                )
                : value(payload)
            }
          }
          if (property === "BeginRestoreBackup") {
            return (payload: never) => {
              restoreCalls++
              return Effect.fail(protocolMismatch("restore"))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const error = yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_2a9c4e17-8d3b-4f6a-b5e8-9c0d1e2f3a4b")
      }).pipe(Effect.flip)
      assert.strictEqual(openSessions, 2)
      assert.strictEqual(restoreCalls, 1)
      assert.strictEqual(applications, 0)
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      if (error.reason._tag === "QuotaExceeded") assert.strictEqual(error.reason.resource, "sessions")
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("interrupts restore while its replacement session is opening", () => {
    let applications = 0
    let restoreCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runDrain(source).pipe(Effect.tap(() => Effect.sync(() => ++applications)))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replacementStarted = yield* Deferred.make<void>()
      let openSessions = 0
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Deferred.succeed(replacementStarted, undefined).pipe(Effect.andThen(Effect.never))
                : value(payload)
            }
          }
          if (property === "BeginRestoreBackup") {
            return (payload: never) => {
              restoreCalls++
              return Effect.fail(protocolMismatch("restore"))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const fiber = yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_3b0d5f28-9e4a-4c6b-a7f8-0d1e2f3a4b5c")
      }).pipe(Effect.forkChild)
      yield* Deferred.await(replacementStarted)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(openSessions, 2)
      assert.strictEqual(restoreCalls, 1)
      assert.strictEqual(applications, 0)
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("shares a concurrent replacement without replaying the stale restore", () => {
    let applications = 0
    let restoreCalls = 0
    const CountingOwner = ReplicaOwner.layerHandlers(definition).pipe(
      Layer.provide(PeerConnectionStatus.layer),
      Layer.provide(RelayConnectionStatus.layerNotConfigured),
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(
        Publisher,
        Layer.succeed(Replica.Replica, {
          ...replica,
          restoreBackup: ({ source }) =>
            Stream.runDrain(source).pipe(Effect.tap(() => Effect.sync(() => ++applications)))
        })
      ))
    )
    return Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const restoreApplied = yield* Deferred.make<void>()
      const releaseRestoreMismatch = yield* Deferred.make<void>()
      const openedSessionIds: Array<Identity.SessionId> = []
      const getSessionIds: Array<Identity.SessionId> = []
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              openedSessionIds.push(payload.sessionId)
              return value(payload)
            }
          }
          if (property === "BeginRestoreBackup") {
            return (payload: never) => {
              restoreCalls++
              return Deferred.succeed(restoreApplied, undefined).pipe(
                Effect.andThen(Deferred.await(releaseRestoreMismatch)),
                Effect.andThen(Effect.fail(protocolMismatch("restore")))
              )
            }
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              getSessionIds.push(payload.sessionId)
              return payload.sessionId === openedSessionIds[0]
                ? Effect.fail(protocolMismatch("get"))
                : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const restore = yield* client.restoreBackup({
        source: Stream.make(Uint8Array.of(1, 2, 3)),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash,
        installationId: Identity.BackupInstallationId.make("bak_4c1e6a39-0f5b-4d7c-b8a9-1e2f3a4b5c6d")
      }).pipe(Effect.flip, Effect.forkChild)
      yield* Deferred.await(restoreApplied)
      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
      yield* Deferred.succeed(releaseRestoreMismatch, undefined)
      const restoreError = yield* Fiber.join(restore)
      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
      assert.strictEqual(restoreCalls, 1)
      assert.strictEqual(applications, 0)
      assert.strictEqual(restoreError.reason._tag, "ProtocolMismatch")
      if (restoreError.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(restoreError.reason.observed, "restore")
      }
      assert.strictEqual(openedSessionIds.length, 2)
      assert.deepStrictEqual(getSessionIds, [
        openedSessionIds[0],
        openedSessionIds[1],
        openedSessionIds[1]
      ])
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(CountingOwner))
  })

  it.effect("still transparently replays an idempotent operation after ProtocolMismatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const openedSessionIds: Array<Identity.SessionId> = []
      const getSessionIds: Array<Identity.SessionId> = []
      const faulted = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              Effect.sync(() => openedSessionIds.push(payload.sessionId)).pipe(Effect.andThen(value(payload)))
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              getSessionIds.push(payload.sessionId)
              return getSessionIds.length === 1
                ? Effect.fail(
                  protocolMismatch("get")
                )
                : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, faulted)
      const snapshot = yield* client.get(Task, documentId)
      assert.strictEqual(snapshot.value.title, "stored")
      assert.strictEqual(openedSessionIds.length, 2)
      assert.notStrictEqual(openedSessionIds[1], openedSessionIds[0])
      assert.deepStrictEqual(getSessionIds, [openedSessionIds[0], openedSessionIds[1]])
    })).pipe(Effect.provide(Owner)))

  const lostReleasedScenarios = [
    {
      name: "replaces a stale session when an accepted restore result outlives Released",
      replacementPending: false,
      expected: "ProtocolMismatch"
    },
    {
      name: "returns a typed timeout when an accepted replacement is interrupted at the deadline",
      replacementPending: true,
      expected: "OperationTimeout"
    }
  ] as const

  for (const scenario of lostReleasedScenarios) {
    it.effect(scenario.name, () => {
      let beginCalls = 0
      const ExpiringOwner = ReplicaOwner.layerHandlers(definition).pipe(
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(Sessions),
        Layer.provide(Layer.merge(
          Publisher,
          Layer.succeed(Replica.Replica, {
            ...replica,
            restoreBackup: () => Effect.never
          })
        ))
      )
      return Effect.gen(function*() {
        yield* TestClock.setTime(0)
        const sessions = yield* SessionManager.SessionManager
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const start = yield* Deferred.make<Identity.SessionId>()
        const terminalAck = yield* Deferred.make<void>()
        const replacementStarted = yield* Deferred.make<void>()
        const openedSessionIds: Array<Identity.SessionId> = []
        const observed = new Proxy(rpc, {
          get(target, property, receiver) {
            const value = Reflect.get(target, property, receiver)
            if (property === "OpenSession") {
              return (payload: { readonly sessionId: Identity.SessionId }) =>
                Effect.sync(() => {
                  openedSessionIds.push(payload.sessionId)
                  return openedSessionIds.length
                }).pipe(
                  Effect.flatMap((opened) =>
                    opened === 1
                      ? value(payload)
                      : Deferred.succeed(replacementStarted, undefined).pipe(
                        Effect.andThen(scenario.replacementPending ? Effect.never : value(payload))
                      )
                  )
                )
            }
            if (property === "RenewSession") return () => Effect.never
            if (property === "BeginRestoreBackup") {
              return (payload: Parameters<typeof target.BeginRestoreBackup>[0]) =>
                Effect.sync(() => {
                  beginCalls += 1
                }).pipe(
                  Effect.andThen(target.BeginRestoreBackup(payload)),
                  Effect.map(({ nonce, port }) => {
                    const add = port.addEventListener.bind(port)
                    const remove = port.removeEventListener.bind(port)
                    const post = port.postMessage.bind(port)
                    const listeners = new Map<EventListenerOrEventListenerObject, EventListener>()
                    Object.defineProperties(port, {
                      addEventListener: {
                        configurable: true,
                        value: (
                          type: string,
                          listener: EventListenerOrEventListenerObject,
                          options?: boolean | AddEventListenerOptions
                        ) => {
                          if (type !== "message") {
                            add(type, listener, options)
                            return
                          }
                          const wrapped: EventListener = (event) => {
                            const tag = typeof (event as MessageEvent<unknown>).data === "object" &&
                                (event as MessageEvent<unknown>).data !== null
                              ? Reflect.get((event as MessageEvent<unknown>).data as object, "_tag")
                              : undefined
                            if (tag === "Released") return
                            if (typeof listener === "function") listener.call(port, event)
                            else listener.handleEvent(event)
                          }
                          listeners.set(listener, wrapped)
                          add(type, wrapped, options)
                        }
                      },
                      removeEventListener: {
                        configurable: true,
                        value: (
                          type: string,
                          listener: EventListenerOrEventListenerObject,
                          options?: boolean | EventListenerOptions
                        ) => remove(type, listeners.get(listener) ?? listener, options)
                      },
                      postMessage: {
                        configurable: true,
                        value: (message: unknown, transfer?: ReadonlyArray<Transferable>) => {
                          if (
                            typeof message === "object" &&
                            message !== null &&
                            Reflect.get(message, "_tag") === "Start"
                          ) {
                            Deferred.doneUnsafe(start, Effect.succeed(payload.sessionId))
                          }
                          if (
                            typeof message === "object" &&
                            message !== null &&
                            Reflect.get(message, "_tag") === "TerminalAck"
                          ) {
                            Deferred.doneUnsafe(terminalAck, Effect.void)
                          }
                          post(message, transfer === undefined ? [] : [...transfer])
                        }
                      }
                    })
                    return { nonce, port }
                  })
                )
            }
            return value
          }
        })
        const client = yield* ReplicaClient.fromRpcClient(definition, observed, {
          operationTimeout: 1_000
        })
        const restored = yield* client.restoreBackup({
          source: Stream.never,
          mode: "replace",
          maxBytes: 1_024,
          expectedDefinitionHash: definition.hash,
          installationId: Identity.BackupInstallationId.make(
            scenario.replacementPending
              ? "bak_e8fe506a-2f58-463f-8ef4-a52f363b6314"
              : "bak_3eb7f6c1-53fb-48a7-b292-a1faeb46d507"
          )
        }).pipe(Effect.forkChild)

        const pulled = yield* Effect.raceFirst(
          Deferred.await(start).pipe(Effect.map((sessionId) => ({ _tag: "Start" as const, sessionId }))),
          Fiber.await(restored).pipe(Effect.map((exit) => ({ _tag: "Exit" as const, exit })))
        )
        assert.strictEqual(pulled._tag, "Start")
        if (pulled._tag !== "Start") return
        const staleSessionId = pulled.sessionId
        assert.deepStrictEqual(openedSessionIds, [staleSessionId])
        yield* sessions.close(staleSessionId, 0)
        const ready = yield* Effect.raceFirst(
          Deferred.await(terminalAck).pipe(Effect.as("Ack" as const)),
          Fiber.await(restored).pipe(Effect.map((exit) => ({ _tag: "Exit" as const, exit })))
        )
        assert.strictEqual(ready, "Ack")
        assert.deepStrictEqual(openedSessionIds, [staleSessionId])
        yield* Deferred.await(replacementStarted)
        yield* TestClock.adjust(1_000)
        const exit = yield* Fiber.await(restored)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isSuccess(exit)) return
        assert.strictEqual(exit.cause.reasons.length, 1)
        const reason = exit.cause.reasons[0]
        assert.isTrue(Cause.isFailReason(reason))
        if (!Cause.isFailReason(reason)) return
        assert.strictEqual(reason.error.reason._tag, scenario.expected)
        if (reason.error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(reason.error.reason.expected, "active session")
          assert.strictEqual(reason.error.reason.observed, staleSessionId)
        }
        if (reason.error.reason._tag === "OperationTimeout") {
          assert.strictEqual(reason.error.reason.operation, "RestoreBackup")
          assert.strictEqual(reason.error.reason.timeoutMillis, 1_000)
        }
        assert.strictEqual(beginCalls, 1)
        assert.strictEqual(openedSessionIds.length, 2)
        assert.notStrictEqual(openedSessionIds[1], openedSessionIds[0])
      }).pipe(Effect.scoped, Effect.provide(ExpiringOwner))
    })
  }

  it.effect("keeps the peer connection status stream open across a full round trip", () =>
    Effect.scoped(Effect.gen(function*() {
      const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000c1")
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const seen = yield* Queue.unbounded<PeerConnectionStatus.Status>()
      const fiber = yield* Stream.runForEach(
        client.peerConnectionStatus.status(peerId),
        (status) => Queue.offer(seen, status)
      ).pipe(Effect.forkChild)

      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.disconnected)
      // A full client to owner and back cycle, so the poll below is ordered by an observable event
      // rather than by whichever fiber the queue handoff happened to schedule first.
      yield* client.get(Task, documentId)

      assert.isUndefined(fiber.pollUnsafe())
    })).pipe(Effect.provide(Owner)))

  it.effect("round trips command delivery lookup and changes", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const commandId = yield* Identity.makeCommandId
      assert.strictEqual(
        (yield* client.lookupCommandDelivery(commandId))._tag,
        "UnknownCommand"
      )
      const observed = Array.from(
        yield* client.commandDeliveryChanges(commandId).pipe(
          Stream.take(1),
          Stream.runCollect
        )
      )
      assert.strictEqual(observed.length, 1)
      assert.strictEqual(observed[0]?._tag, "UnknownCommand")
    })).pipe(Effect.provide(Owner)))

  it.effect("rejects negative delivery cursors at the RPC boundary", () =>
    Effect.gen(function*() {
      const delivery = yield* Effect.exit(
        Schema.decodeUnknownEffect(ReplicaRpc.InvalidationMessage)({
          _tag: "DeliveryInvalidation",
          ownerEpoch: "owner",
          sequence: -1,
          keys: []
        })
      )
      const ready = yield* Effect.exit(
        Schema.decodeUnknownEffect(ReplicaRpc.InvalidationMessage)({
          _tag: "InvalidationsReady",
          ownerEpoch: "owner",
          watermark: 0,
          refreshGeneration: 0,
          deliveryWatermark: -1,
          deliveryRefreshEpoch: -1
        })
      )
      assert.strictEqual(delivery._tag, "Failure")
      assert.strictEqual(ready._tag, "Failure")
    }))

  it.effect("retries a command delivery stream after transient ownership transport loss", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const transportFailed = yield* Deferred.make<void>()
      let subscriptions = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "CommandDeliveryChanges") return Reflect.get(target, property, receiver)
          return (payload: never) =>
            Stream.unwrap(Effect.sync(() => {
              subscriptions++
              return subscriptions === 1
                ? Stream.fromEffect(
                  Deferred.succeed(transportFailed, undefined).pipe(
                    Effect.andThen(Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.StorageUnavailable({
                          cause: transientDisconnected()
                        })
                      })
                    ))
                  )
                )
                : target.CommandDeliveryChanges(payload).pipe(Stream.take(1))
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const commandId = yield* Identity.makeCommandId
      const completed = yield* client.commandDeliveryChanges(commandId).pipe(
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(transportFailed)
      yield* TestClock.adjust("1 second")
      yield* Fiber.join(completed)
      assert.strictEqual(subscriptions, 2)
    })).pipe(Effect.provide(Owner)))
})
