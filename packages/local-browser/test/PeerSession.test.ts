import { assert, describe, it } from "@effect/vitest"
import * as CommandExecutor from "@lucas-barake/effect-local-sql/CommandExecutor"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as DocumentEntity from "@lucas-barake/effect-local-sql/DocumentEntity"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as PeerSession from "../src/PeerSession.js"

describe("PeerSession", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 32
  } satisfies ReplicaLimits.Values
  const permit = {
    replicaId: Identity.makeReplicaId(),
    incarnation: Identity.ReplicaIncarnation.make(1),
    writerGeneration: Identity.WriterGeneration.make(2),
    definitionHash: definition.hash
  }
  const TestShardingConfig = ShardingConfig.layer({
    shardsPerGroup: 16,
    entityMailboxCapacity: limits.maxQueuedRpc,
    entityTerminationTimeout: 0,
    entityMessagePollInterval: 5_000,
    sendRetryInterval: 100
  })
  const executor = CommandExecutor.CommandExecutor.of({
    create: (_document, options) =>
      Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.documentId)),
    mutate: (_mutation, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.payload)),
    delete: (_document, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
    lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
    lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
    lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
  })
  const result = {
    reply: null,
    heads: [],
    acceptedHeads: [],
    commitSequence: Identity.CommitSequence.make(1),
    observedByPeer: true,
    durableConfirmation: false as const,
    duplicate: false
  }
  const gate = ReplicaGate.ReplicaGate.of({
    current: Effect.succeed(permit),
    shared: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
    exclusive: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
    refresh: Effect.succeed(permit),
    validate: () => Effect.void
  })

  const encode = (envelope: typeof PeerSession.SyncEnvelope.Type) =>
    Schema.encodeUnknownEffect(Schema.toCodecJson(PeerSession.SyncEnvelope))(envelope).pipe(
      Effect.map((value) => new TextEncoder().encode(JSON.stringify(value)))
    )

  it.effect("routes selected inbound documents through the entity and separates observation from durability", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const received = yield* Deferred.make<void>()
        const sent = yield* Deferred.make<Uint8Array>()
        const documentId = Identity.makeDocumentId()
        const peerId = Identity.makePeerId()
        const message = new Uint8Array([1, 2, 3])
        const published = yield* Ref.make(0)
        const gateReleased = yield* Ref.make(true)
        const scopedGate = ReplicaGate.ReplicaGate.of({
          ...gate,
          shared: Effect.acquireRelease(
            Ref.set(gateReleased, false).pipe(Effect.as(permit)),
            () => Ref.set(gateReleased, true)
          )
        })
        const reply = {
          documentId,
          message: new Uint8Array([4, 5, 6]),
          messageHash: "reply-hash",
          heads: []
        }
        const sync = PeerSync.PeerSync.of({
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () => Effect.succeed({ outbound: null, observedByPeer: true, dirty: false }),
          receive: () => Deferred.succeed(received, undefined).pipe(Effect.as({ ...result, reply })),
          enqueue: (_session, value) => Effect.succeed({ ...value, sendSequence: 7 }),
          pending: () => Effect.succeed([]),
          markSent: () => Effect.succeed(true)
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.fromQueue(inbound),
              send: (bytes) =>
                Ref.get(gateReleased).pipe(
                  Effect.tap((released) => Effect.sync(() => assert.isFalse(released))),
                  Effect.andThen(Deferred.succeed(sent, bytes)),
                  Effect.asVoid
                ),
              close: Effect.void
            })
        })
        const entity = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, scopedGate)),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        const session = yield* PeerSession.makeWithClient(
          { peerId, documents: [{ document: Task, documentId }] },
          entity
        ).pipe(
          Effect.provideService(PeerTransport.PeerTransport, transport),
          Effect.provideService(PeerSync.PeerSync, sync),
          Effect.provideService(ReplicaGate.ReplicaGate, scopedGate),
          Effect.provideService(
            CommitPublisher.CommitPublisher,
            CommitPublisher.CommitPublisher.of({
              publishPending: Ref.update(published, (count) => count + 1).pipe(Effect.as(1)),
              invalidate: () => Effect.void,
              subscribe: Effect.succeed({
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0,
                events: Stream.never
              })
            })
          ),
          Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
        )
        yield* Queue.offer(
          inbound,
          yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash: yield* Canonical.digest(message),
            message
          })
        )
        yield* Deferred.await(received)
        const replyEnvelope = yield* Deferred.await(sent).pipe(
          Effect.flatMap((bytes) =>
            Effect.try(() => JSON.parse(new TextDecoder().decode(bytes))).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(Schema.toCodecJson(PeerSession.SyncEnvelope)))
            )
          )
        )
        assert.strictEqual(replyEnvelope.sequence, 7)
        assert.strictEqual(replyEnvelope.connectionEpoch, "local-epoch")
        assert.strictEqual(yield* Ref.get(published), 1)
        assert.isTrue(yield* session.observedByPeer(documentId))
        assert.isFalse(yield* session.durableConfirmation(documentId))
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("keeps the transport connection scope open for the peer session lifetime", () =>
    Effect.gen(function*() {
      const released = yield* Ref.make(false)
      const closed = yield* Ref.make(false)
      const peerId = Identity.makePeerId()
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Effect.void,
        generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.acquireRelease(
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.never,
              send: () => Effect.void,
              close: Ref.set(closed, true)
            }),
            () => Ref.set(released, true)
          )
      })
      yield* Effect.scoped(
        PeerSession.makeWithClient({ peerId, documents: [] }, () => Effect.die("unexpected entity request")).pipe(
          Effect.tap(() => Ref.get(released).pipe(Effect.map((value) => assert.isFalse(value)))),
          Effect.provideService(PeerTransport.PeerTransport, transport),
          Effect.provideService(PeerSync.PeerSync, sync),
          Effect.provideService(ReplicaGate.ReplicaGate, gate),
          Effect.provideService(
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
        )
      )
      assert.isTrue(yield* Ref.get(released))
      assert.isTrue(yield* Ref.get(closed))
    }))

  it.effect("bounds network sends while retaining the restore fence", () =>
    Effect.gen(function*() {
      const documentId = Identity.makeDocumentId()
      const peerId = Identity.makePeerId()
      const sendStarted = yield* Deferred.make<void>()
      const gateReleased = yield* Ref.make(true)
      const scopedGate = ReplicaGate.ReplicaGate.of({
        ...gate,
        shared: Effect.acquireRelease(
          Ref.set(gateReleased, false).pipe(Effect.as(permit)),
          () => Ref.set(gateReleased, true)
        )
      })
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Effect.void,
        generate: () =>
          Effect.succeed({
            outbound: {
              sendSequence: 0,
              documentId,
              message: Uint8Array.of(1),
              messageHash: "message-hash",
              heads: []
            },
            observedByPeer: false,
            dirty: false
          }),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.never,
            send: () => Deferred.succeed(sendStarted, undefined).pipe(Effect.andThen(Effect.never)),
            close: Effect.void
          })
      })
      const fiber = yield* Effect.scoped(PeerSession.makeWithClient(
        { peerId, documents: [{ document: Task, documentId }] },
        () => Effect.die("entity should not be called")
      )).pipe(
        Effect.provideService(PeerTransport.PeerTransport, transport),
        Effect.provideService(PeerSync.PeerSync, sync),
        Effect.provideService(ReplicaGate.ReplicaGate, scopedGate),
        Effect.provideService(
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
        Effect.provideService(ReplicaLimits.ReplicaLimits, { ...limits, maxPeerSendMillis: 100 }),
        Effect.forkChild
      )
      yield* Deferred.await(sendStarted)
      assert.isFalse(yield* Ref.get(gateReleased))
      yield* TestClock.adjust(100)
      assert.strictEqual((yield* Fiber.await(fiber))._tag, "Failure")
      assert.isTrue(yield* Ref.get(gateReleased))
    }))

  it.effect("keeps one send in flight and ignores its completion after scope reset", () =>
    Effect.gen(function*() {
      const firstStarted = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const sends = yield* Ref.make(0)
      const activeSends = yield* Ref.make(0)
      const maximum = yield* Ref.make(0)
      const marked = yield* Ref.make(0)
      const resets = yield* Ref.make(0)
      const documentId = Identity.makeDocumentId()
      const peerId = Identity.makePeerId()
      const generateCalls = yield* Ref.make(0)
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Ref.update(resets, (count) => count + 1),
        generate: (_document, id) =>
          Ref.updateAndGet(generateCalls, (count) => count + 1).pipe(
            Effect.map((call) =>
              call === 1
                ? { outbound: null, observedByPeer: false, dirty: false }
                : {
                  outbound: {
                    sendSequence: call,
                    documentId: id,
                    message: new Uint8Array([call]),
                    messageHash: `hash-${call}`,
                    heads: []
                  },
                  observedByPeer: false,
                  dirty: false
                }
            )
          ),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Ref.update(marked, (count) => count + 1).pipe(Effect.as(true))
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.never,
            send: () =>
              Effect.gen(function*() {
                const count = yield* Ref.updateAndGet(sends, (current) => current + 1)
                const active = yield* Ref.updateAndGet(activeSends, (current) => current + 1)
                yield* Ref.update(maximum, (current) => Math.max(current, active))
                if (count === 1) {
                  yield* Deferred.succeed(firstStarted, undefined)
                  yield* Deferred.await(releaseFirst)
                }
                yield* Ref.update(activeSends, (current) => current - 1)
              }),
            close: Effect.void
          })
      })
      yield* Effect.scoped(
        Effect.gen(function*() {
          const entity = yield* Entity.makeTestClient(
            DocumentEntity.DocumentEntity,
            DocumentEntity.layer(definition).pipe(
              Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
              Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
              Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, gate)),
              Layer.provide(ReplicaLimits.layer(limits))
            )
          )
          const session = yield* PeerSession.makeWithClient(
            { peerId, documents: [{ document: Task, documentId }] },
            entity
          ).pipe(
            Effect.provideService(PeerTransport.PeerTransport, transport),
            Effect.provideService(PeerSync.PeerSync, sync),
            Effect.provideService(ReplicaGate.ReplicaGate, gate),
            Effect.provideService(
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
            Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
          )
          yield* session.markDirty(documentId)
          yield* Effect.forkChild(session.flush)
          yield* Deferred.await(firstStarted)
          yield* session.markDirty(documentId)
          const second = yield* Effect.forkChild(session.flush)
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(sends), 1)
          assert.strictEqual(yield* Ref.get(maximum), 1)
          yield* Fiber.interrupt(second)
        }).pipe(Effect.provide(TestShardingConfig))
      )
      yield* Deferred.succeed(releaseFirst, undefined)
      assert.strictEqual(yield* Ref.get(marked), 0)
      assert.strictEqual(yield* Ref.get(resets), 1)
      assert.strictEqual(yield* Ref.get(maximum), 1)
    }))

  it.effect("binds one remote epoch and resets both session directions", () =>
    Effect.gen(function*() {
      const inbound = yield* Queue.unbounded<Uint8Array>()
      const firstReceived = yield* Deferred.make<void>()
      const receiveEnded = yield* Deferred.make<void>()
      const receives = yield* Ref.make(0)
      const resets = yield* Ref.make<ReadonlyArray<PeerSync.Session>>([])
      const documentId = Identity.makeDocumentId()
      const peerId = Identity.makePeerId()
      const message = new Uint8Array([1])
      const messageHash = yield* Canonical.digest(message)
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: (session) => Ref.update(resets, (current) => [...current, session]),
        generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
        receive: () =>
          Ref.updateAndGet(receives, (count) => count + 1).pipe(
            Effect.tap(() => Deferred.succeed(firstReceived, undefined)),
            Effect.as(result)
          ),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.fromQueue(inbound).pipe(
              Stream.ensuring(Deferred.succeed(receiveEnded, undefined))
            ),
            send: () => Effect.void,
            close: Effect.void
          })
      })
      yield* Effect.scoped(
        Effect.gen(function*() {
          yield* PeerSession.makeWithClient(
            { peerId, documents: [{ document: Task, documentId }] },
            () =>
              Effect.succeed({
                ApplySync: () =>
                  sync.receive(
                    Task,
                    documentId,
                    {
                      peerId,
                      connectionEpoch: "local-epoch",
                      replicaIncarnation: permit.incarnation
                    },
                    {
                      remoteConnectionEpoch: "remote-epoch",
                      receiveSequence: 0,
                      message
                    }
                  )
              } as never)
          )
          const envelope = (connectionEpoch: string) =>
            encode({
              connectionEpoch,
              sequence: 0,
              documentId,
              documentType: Task.name,
              messageHash,
              message
            })
          yield* Queue.offer(inbound, yield* envelope("remote-epoch"))
          yield* Deferred.await(firstReceived)
          yield* Queue.offer(inbound, yield* envelope("changed-epoch"))
          yield* Deferred.await(receiveEnded)
          assert.strictEqual(yield* Ref.get(receives), 1)
        }).pipe(
          Effect.provideService(PeerTransport.PeerTransport, transport),
          Effect.provideService(PeerSync.PeerSync, sync),
          Effect.provideService(ReplicaGate.ReplicaGate, gate),
          Effect.provideService(
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
        )
      )
      assert.deepStrictEqual(
        (yield* Ref.get(resets)).map((session) => session.connectionEpoch).toSorted(),
        ["local-epoch", "remote-epoch"]
      )
    }))

  it.effect("preserves the current and unprocessed dirty documents when sending fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const firstDocumentId = Identity.makeDocumentId()
      const secondDocumentId = Identity.makeDocumentId()
      const peerId = Identity.makePeerId()
      const generated = yield* Ref.make<ReadonlyArray<Identity.DocumentId>>([])
      const sends = yield* Ref.make(0)
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Effect.void,
        generate: (_document, documentId) =>
          Ref.updateAndGet(generated, (current) => [...current, documentId]).pipe(
            Effect.map((current) =>
              current.length <= 2
                ? { outbound: null, observedByPeer: false, dirty: false }
                : {
                  outbound: {
                    sendSequence: current.length,
                    documentId,
                    message: new Uint8Array([current.length]),
                    messageHash: `hash-${current.length}`,
                    heads: []
                  },
                  observedByPeer: false,
                  dirty: false
                }
            )
          ),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.never,
            send: () =>
              Ref.updateAndGet(sends, (count) => count + 1).pipe(
                Effect.flatMap((attempt) =>
                  attempt === 1
                    ? Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: {
                          _tag: "StorageUnavailable",
                          cause: { _tag: "RpcCause", message: "offline" }
                        }
                      })
                    )
                    : Effect.void
                )
              ),
            close: Effect.void
          })
      })
      const session = yield* PeerSession.makeWithClient(
        {
          peerId,
          documents: [
            { document: Task, documentId: firstDocumentId },
            { document: Task, documentId: secondDocumentId }
          ]
        },
        () => Effect.die("unexpected inbound entity request")
      ).pipe(
        Effect.provideService(PeerTransport.PeerTransport, transport),
        Effect.provideService(PeerSync.PeerSync, sync),
        Effect.provideService(ReplicaGate.ReplicaGate, gate),
        Effect.provideService(
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
        Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
      )
      yield* session.markDirty(firstDocumentId)
      yield* session.markDirty(secondDocumentId)
      yield* Effect.flip(session.flush)
      yield* session.flush
      assert.deepStrictEqual(
        (yield* Ref.get(generated)).slice(2),
        [firstDocumentId, firstDocumentId, secondDocumentId]
      )
    })))
})
