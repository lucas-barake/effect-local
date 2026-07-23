import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { Headers } from "effect/unstable/http"
import { Rpc, RpcTest } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import { RequestId } from "effect/unstable/rpc/RpcMessage"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, documentId, Read, ReadError, Rename, RenameError, replica, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaClient", (it) => {
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
    maxSessions: 2,
    maxStreamsPerSession: 2,
    maxInFlightPerSession: 2,
    maxQueuedRpc: 4
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
  const Owner = ReplicaOwner.layerHandlers(definition).pipe(
    Layer.provideMerge(Sessions),
    Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
  )

  const disconnected = () =>
    new RpcClientError.RpcClientError({
      reason: new RpcClientError.RpcClientDefect({ message: "disconnected", cause: "disconnected" })
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
        assert.strictEqual(mutation._tag, "DurablyCommittedLocal")
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
          CommandOutcome.durablyCommitted(importCommandId, documentId)
        )
        assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.status)), [{
          _tag: "Ready",
          pendingCommands: 0
        }])
      }))
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(
      Effect.provide(Owner)
    ))

  it.effect("round trips tagged query errors through the wire", () => {
    const rejected: Replica.Replica["Service"] = {
      ...replica,
      query: (_query, ...payload) => Effect.fail(new ReadError({ filter: String(payload[0]) })) as never
    }
    const RejectedOwner = ReplicaOwner.layerHandlers(definition).pipe(
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
        CommandOutcome.durablyCommitted(createId, documentId)
      )
      assert.deepStrictEqual(
        yield* client.mutate(Rename, { commandId: mutateId, documentId, payload: { title: "next" } }),
        CommandOutcome.durablyCommitted(mutateId, "renamed")
      )
      assert.deepStrictEqual(
        yield* client.delete(Task, { commandId: deleteId, documentId }),
        CommandOutcome.durablyCommitted(deleteId, undefined)
      )
    })).pipe(Effect.provide(Owner)))

  it.effect("returns unknown when ambiguous command lookup also loses transport", () =>
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
      assert.deepStrictEqual(
        yield* client.mutate(Rename, { commandId, documentId, payload: { title: "next" } }),
        CommandOutcome.unknown(commandId)
      )
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
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(Events, Layer.succeed(Replica.Replica, replica)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
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
      Layer.provideMerge(Sessions),
      Layer.provide(Layer.merge(Events, Layer.succeed(Replica.Replica, replica)))
    )
    return Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const first = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const second = yield* ReplicaClient.fromRpcClient(definition, rpc)
      const events = yield* Effect.all([
        Stream.runCollect(first.invalidations),
        Stream.runCollect(second.invalidations)
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
                  refreshGeneration: 0
                }).pipe(Stream.concat(Stream.fail(disconnected())))
                : Stream.make({
                  _tag: "InvalidationsReady" as const,
                  ownerEpoch,
                  watermark: Identity.CommitSequence.make(2),
                  refreshGeneration: 0
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
        keys: [Task.name]
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
              refreshGeneration: 1
            })
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, sticky)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: [Task.name]
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
              refreshGeneration: 0
            })
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, ahead)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [{
        _tag: "FullRefreshRequired",
        ownerEpoch: client.ownerEpoch,
        keys: [Task.name]
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
                refreshGeneration: 0
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
                refreshGeneration: 0
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
        keys: [Task.name]
      }])
      assert.strictEqual(yield* sessions.activeCount, 1)
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
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              openSessions++
              if (initialSessionId === undefined) initialSessionId = payload.sessionId
              const opened = value(payload)
              return openSessions === 2 || openSessions === 3
                ? opened.pipe(Effect.andThen(Effect.fail(disconnected())))
                : opened
            }
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
        keys: [Task.name]
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
                refreshGeneration: 0
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
          keys: [Task.name]
        },
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: [Task.name]
        },
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: [Task.name]
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
        mutate: (_mutation, options) =>
          Effect.succeed(CommandOutcome.rejected(options.commandId, new RenameError())) as never,
        lookupMutation: () =>
          Effect.sync(() => {
            lookups++
            return CommandOutcome.unknown(unknownCommandId)
          }) as never
      }
      const RejectedOwner = ReplicaOwner.layerHandlers(definition).pipe(
        Layer.provideMerge(Sessions),
        Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, rejected)))
      )
      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        const client = yield* ReplicaClient.fromRpcClient(definition, rpc)
        const commandId = yield* Identity.makeCommandId
        assert.deepStrictEqual(
          yield* client.mutate(Rename, { commandId, documentId, payload: { title: "next" } }),
          CommandOutcome.rejected(commandId, new RenameError())
        )
        assert.strictEqual(lookups, 0)
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
        expectedDefinitionHash: definition.hash
      })
      assert.deepStrictEqual(restored, exported)
      restored = []
      const oversized = yield* Effect.flip(client.restoreBackup({
        source: Stream.fromIterable([new Uint8Array(700), new Uint8Array(700)]),
        mode: "replace",
        maxBytes: 1024,
        expectedDefinitionHash: definition.hash
      }))
      assert.strictEqual(oversized.reason._tag, "BackupTooLarge")
      assert.deepStrictEqual(restored, [])
    })).pipe(Effect.provide(BackupOwner))
  })
})
