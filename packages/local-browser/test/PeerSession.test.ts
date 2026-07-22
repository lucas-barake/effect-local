import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
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
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as PeerSession from "../src/PeerSession.js"

it.layer(NodeCrypto.layer)("PeerSession", (it) => {
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
    replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001"),
    incarnation: Identity.ReplicaIncarnation.make(1),
    writerGeneration: Identity.WriterGeneration.make(2),
    definitionHash: definition.hash
  }
  const TestShardingConfig = Layer.merge(
    ShardingConfig.layer({
      shardsPerGroup: 16,
      entityMailboxCapacity: limits.maxQueuedRpc,
      entityTerminationTimeout: 0,
      entityMessagePollInterval: 5_000,
      sendRetryInterval: 100
    }),
    NodeCrypto.layer
  )
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
    claim: (use) => use(permit),
    refresh: Effect.succeed(permit),
    validate: () => Effect.void
  })

  const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(PeerSession.SyncEnvelope))
  const encode = (envelope: typeof PeerSession.SyncEnvelope.Type) =>
    Schema.encodeEffect(SyncEnvelopeJson)(envelope).pipe(
      Effect.map((value) => new TextEncoder().encode(value))
    )

  it.effect("routes selected inbound documents through the entity and separates observation from durability", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const received = yield* Deferred.make<void>()
        const sent = yield* Deferred.make<Uint8Array>()
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
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
        const session = yield* PeerSession.makeTestClient(
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
          Effect.flatMap((bytes) => Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)))
        )
        assert.strictEqual(replyEnvelope.sequence, 7)
        assert.strictEqual(replyEnvelope.connectionEpoch, "local-epoch")
        assert.strictEqual(yield* Ref.get(published), 1)
        assert.isTrue(yield* session.observedByPeer(documentId))
        assert.isFalse(yield* session.durableConfirmation(documentId))
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("rejects invalid inbound envelopes before dispatch or publication and closes the connection", () =>
    Effect.gen(function*() {
      const documentId = yield* Identity.makeDocumentId
      const otherDocumentId = yield* Identity.makeDocumentId
      const message = Uint8Array.of(1, 2, 3)
      const messageHash = yield* Canonical.digest(message)
      const cases = [
        { name: "malformed JSON", bytes: new TextEncoder().encode("{") },
        {
          name: "oversized envelope",
          bytes: new Uint8Array(limits.maxSyncMessageBytes * 2 + 4_097)
        },
        {
          name: "incorrect hash",
          bytes: yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash: "incorrect-hash",
            message
          })
        },
        {
          name: "unselected document",
          bytes: yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId: otherDocumentId,
            documentType: Task.name,
            messageHash,
            message
          })
        }
      ]

      for (const testCase of cases) {
        yield* Effect.scoped(Effect.gen(function*() {
          const inbound = yield* Queue.unbounded<Uint8Array>()
          const closed = yield* Deferred.make<void>()
          const entityCalls = yield* Ref.make(0)
          const publications = yield* Ref.make(0)
          const peerId = yield* Identity.makePeerId
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
              Effect.succeed({
                peerId,
                capabilities: { storeAndForward: false },
                receive: Stream.fromQueue(inbound),
                send: () => Effect.die(`unexpected send for ${testCase.name}`),
                close: Deferred.succeed(closed, undefined).pipe(Effect.asVoid)
              })
          })
          yield* PeerSession.makeTestClient(
            { peerId, documents: [{ document: Task, documentId }] },
            () =>
              Ref.update(entityCalls, (count) => count + 1).pipe(
                Effect.andThen(Effect.die(`unexpected entity dispatch for ${testCase.name}`))
              )
          ).pipe(
            Effect.provideService(PeerTransport.PeerTransport, transport),
            Effect.provideService(PeerSync.PeerSync, sync),
            Effect.provideService(ReplicaGate.ReplicaGate, gate),
            Effect.provideService(
              CommitPublisher.CommitPublisher,
              CommitPublisher.CommitPublisher.of({
                publishPending: Ref.update(publications, (count) => count + 1).pipe(Effect.as(1)),
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
          yield* Queue.offer(inbound, testCase.bytes)
          yield* Deferred.await(closed)
          assert.strictEqual(yield* Ref.get(entityCalls), 0)
          assert.strictEqual(yield* Ref.get(publications), 0)
        }))
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("keeps the transport connection scope open for the peer session lifetime", () =>
    Effect.gen(function*() {
      const released = yield* Ref.make(false)
      const closed = yield* Ref.make(false)
      const gateReleased = yield* Ref.make(true)
      const peerId = yield* Identity.makePeerId
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
        PeerSession.makeTestClient({ peerId, documents: [] }, () => Effect.die("unexpected entity request")).pipe(
          Effect.tap(() =>
            Effect.all({ gateReleased: Ref.get(gateReleased), transportReleased: Ref.get(released) }).pipe(
              Effect.tap(({ gateReleased, transportReleased }) =>
                Effect.sync(() => {
                  assert.isTrue(gateReleased)
                  assert.isFalse(transportReleased)
                })
              )
            )
          ),
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
        )
      )
      assert.isTrue(yield* Ref.get(released))
      assert.isTrue(yield* Ref.get(closed))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("binds the transport identity and sync incarnation under one gate permit", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const nextPermit = {
        replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000002"),
        incarnation: Identity.ReplicaIncarnation.make(2),
        writerGeneration: Identity.WriterGeneration.make(3)
      }
      const current = yield* Ref.make<ReplicaGate.Permit>(permit)
      const sharedHeld = yield* Ref.make(false)
      const connectedReplica = yield* Ref.make<Identity.ReplicaId | null>(null)
      const scopedGate = ReplicaGate.ReplicaGate.of({
        current: Ref.get(current),
        shared: Effect.acquireRelease(
          Ref.set(sharedHeld, true).pipe(Effect.andThen(Ref.get(current))),
          () => Ref.set(sharedHeld, false)
        ),
        claim: (use) => Ref.get(current).pipe(Effect.flatMap(use)),
        refresh: Ref.get(current),
        validate: () => Effect.void
      })
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Ref.get(current).pipe(
            Effect.map((current) => ({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: current.incarnation
            }))
          ),
        reset: () => Effect.void,
        generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Effect.succeed([]),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: ({ replicaId }) =>
          Ref.get(sharedHeld).pipe(
            Effect.flatMap((held) => held ? Effect.void : Ref.set(current, nextPermit)),
            Effect.andThen(Ref.set(connectedReplica, replicaId)),
            Effect.as({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.never,
              send: () => Effect.void,
              close: Effect.void
            })
          )
      })

      yield* Effect.scoped(
        PeerSession.makeTestClient({ peerId, documents: [] }, () => Effect.die("unexpected entity request")).pipe(
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
        )
      )
      assert.strictEqual(yield* Ref.get(connectedReplica), permit.replicaId)
      assert.isFalse(yield* Ref.get(sharedHeld))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("fails when the peer receive stream ends", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const closed = yield* Ref.make(false)
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
        pending: () => Effect.never,
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.empty,
            send: () => Effect.void,
            close: Ref.set(closed, true)
          })
      })
      const failure = yield* Effect.scoped(
        PeerSession.makeTestClient(
          { peerId, documents: [] },
          () => Effect.die("unexpected entity request")
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
      ).pipe(
        Effect.timeout("1 second"),
        Effect.flip,
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(failure)
      assert.strictEqual(error._tag, "ReplicaError")
      if (error._tag === "ReplicaError") assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.isTrue(yield* Ref.get(closed))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("joins inbound work before resetting a closing session", () =>
    Effect.gen(function*() {
      const inbound = yield* Queue.unbounded<Uint8Array>()
      const receiveStarted = yield* Deferred.make<void>()
      const releaseReceive = yield* Deferred.make<void>()
      const closeStarted = yield* Deferred.make<void>()
      const resets = yield* Ref.make(0)
      const enqueues = yield* Ref.make(0)
      const sends = yield* Ref.make(0)
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const message = Uint8Array.of(1)
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Ref.update(resets, (count) => count + 1),
        generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) =>
          Ref.update(enqueues, (count) => count + 1).pipe(
            Effect.as({ ...reply, sendSequence: 0 })
          ),
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
            send: () => Ref.update(sends, (count) => count + 1),
            close: Effect.void
          })
      })
      yield* Effect.acquireUseRelease(
        Scope.make(),
        (scope) =>
          Effect.gen(function*() {
            yield* PeerSession.makeTestClient(
              { peerId, documents: [{ document: Task, documentId }] },
              () =>
                Effect.succeed({
                  ApplySync: () =>
                    Deferred.succeed(receiveStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseReceive)),
                      Effect.uninterruptible,
                      Effect.as({
                        ...result,
                        reply: {
                          documentId,
                          message,
                          messageHash: "reply-hash",
                          heads: []
                        }
                      })
                    )
                } as never)
            ).pipe(
              Effect.provideService(Scope.Scope, scope),
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
            yield* Scope.addFinalizer(scope, Deferred.succeed(closeStarted, undefined))
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
            yield* Deferred.await(receiveStarted)
            const closing = yield* Scope.close(scope, Exit.succeed(undefined)).pipe(Effect.forkChild)
            yield* Deferred.await(closeStarted)
            yield* Effect.yieldNow
            assert.strictEqual(yield* Ref.get(resets), 0)
            yield* Deferred.succeed(releaseReceive, undefined)
            yield* Fiber.join(closing)
            assert.strictEqual(yield* Ref.get(resets), 2)
            assert.strictEqual(yield* Ref.get(enqueues), 0)
            assert.strictEqual(yield* Ref.get(sends), 0)
          }),
        (scope) =>
          Deferred.succeed(releaseReceive, undefined).pipe(
            Effect.andThen(Scope.close(scope, Exit.succeed(undefined))),
            Effect.ignore
          )
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("bounds network sends while retaining the restore fence", () =>
    Effect.gen(function*() {
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
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
      const fiber = yield* Effect.scoped(PeerSession.makeTestClient(
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
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("interrupts an admitted send before teardown reset", () =>
    Effect.gen(function*() {
      const firstStarted = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const sends = yield* Ref.make(0)
      const activeSends = yield* Ref.make(0)
      const maximum = yield* Ref.make(0)
      const marked = yield* Ref.make(0)
      const resets = yield* Ref.make(0)
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
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
              }).pipe(Effect.ensuring(Ref.update(activeSends, (current) => current - 1))),
            close: Effect.void
          })
      })
      yield* Effect.acquireUseRelease(
        Scope.make(),
        (scope) =>
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
            const session = yield* PeerSession.makeTestClient(
              { peerId, documents: [{ document: Task, documentId }] },
              entity
            ).pipe(
              Effect.provideService(Scope.Scope, scope),
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
            yield* Scope.close(scope, Exit.succeed(undefined))
            assert.strictEqual(yield* Ref.get(activeSends), 0)
            assert.strictEqual(yield* Ref.get(marked), 0)
            assert.strictEqual(yield* Ref.get(resets), 1)
            assert.strictEqual(yield* Ref.get(maximum), 1)
          }),
        (scope) =>
          Deferred.succeed(releaseFirst, undefined).pipe(
            Effect.andThen(Scope.close(scope, Exit.succeed(undefined))),
            Effect.ignore
          )
      ).pipe(Effect.provide(TestShardingConfig))
    }))

  it.effect("does not send pending output after session teardown", () =>
    Effect.gen(function*() {
      const gateRequested = yield* Deferred.make<void>()
      const releaseGate = yield* Deferred.make<void>()
      const sharedCalls = yield* Ref.make(0)
      const pendingCalls = yield* Ref.make(0)
      const generateCalls = yield* Ref.make(0)
      const sends = yield* Ref.make(0)
      const resets = yield* Ref.make(0)
      const closed = yield* Ref.make(false)
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const scopedGate = ReplicaGate.ReplicaGate.of({
        ...gate,
        shared: Effect.acquireRelease(
          Ref.updateAndGet(sharedCalls, (count) => count + 1).pipe(
            Effect.flatMap((call) =>
              call === 1
                ? Effect.succeed(permit)
                : Deferred.succeed(gateRequested, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseGate)),
                  Effect.as(permit)
                )
            )
          ),
          () => Effect.void,
          { interruptible: true }
        )
      })
      const outbound = {
        sendSequence: 1,
        documentId,
        message: Uint8Array.of(1),
        messageHash: "message-hash",
        heads: []
      }
      const sync = PeerSync.PeerSync.of({
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Ref.update(resets, (count) => count + 1),
        generate: () =>
          Ref.update(generateCalls, (count) => count + 1).pipe(
            Effect.as({ outbound: null, observedByPeer: false, dirty: false })
          ),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () =>
          Ref.updateAndGet(pendingCalls, (count) => count + 1).pipe(
            Effect.map((call) => call === 1 ? [] : [outbound])
          ),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.never,
            send: () => Ref.update(sends, (count) => count + 1),
            close: Ref.set(closed, true)
          })
      })

      yield* Effect.acquireUseRelease(
        Scope.make(),
        (scope) =>
          Effect.gen(function*() {
            const session = yield* PeerSession.makeTestClient(
              { peerId, documents: [{ document: Task, documentId }] },
              () => Effect.die("unexpected entity request")
            ).pipe(
              Effect.provideService(Scope.Scope, scope),
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
              Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
            )
            yield* session.markDirty(documentId)
            const flushing = yield* session.flush.pipe(Effect.forkChild)
            yield* Deferred.await(gateRequested)
            yield* Scope.close(scope, Exit.succeed(undefined))
            assert.strictEqual(yield* Ref.get(resets), 1)
            assert.isTrue(yield* Ref.get(closed))
            yield* session.flush
            yield* Deferred.succeed(releaseGate, undefined)
            yield* Fiber.join(flushing)
            assert.strictEqual(yield* Ref.get(sends), 0)
            assert.strictEqual(yield* Ref.get(pendingCalls), 2)
            assert.strictEqual(yield* Ref.get(generateCalls), 1)
          }),
        (scope) =>
          Deferred.succeed(releaseGate, undefined).pipe(
            Effect.andThen(Scope.close(scope, Exit.succeed(undefined))),
            Effect.ignore
          )
      )
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("interrupts blocked pending and generated output before teardown reset", () =>
    Effect.gen(function*() {
      for (const blocked of ["pending", "generate"] as const) {
        const blockedStarted = yield* Deferred.make<void>()
        const releaseBlocked = yield* Deferred.make<void>()
        const pendingCalls = yield* Ref.make(0)
        const generateCalls = yield* Ref.make(0)
        const resets = yield* Ref.make(0)
        const closed = yield* Ref.make(false)
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const generated = { outbound: null, observedByPeer: false, dirty: false } as const
        const sync = PeerSync.PeerSync.of({
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Ref.update(resets, (count) => count + 1),
          generate: () =>
            Ref.updateAndGet(generateCalls, (count) => count + 1).pipe(
              Effect.flatMap((call) =>
                blocked === "generate" && call === 2
                  ? Deferred.succeed(blockedStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseBlocked)),
                    Effect.as(generated)
                  )
                  : Effect.succeed(generated)
              )
            ),
          receive: () => Effect.succeed(result),
          enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
          pending: () =>
            Ref.updateAndGet(pendingCalls, (count) => count + 1).pipe(
              Effect.flatMap((call) =>
                blocked === "pending" && call === 2
                  ? Deferred.succeed(blockedStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseBlocked)),
                    Effect.as([])
                  )
                  : Effect.succeed([])
              )
            ),
          markSent: () => Effect.succeed(true)
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.never,
              send: () => Effect.void,
              close: Ref.set(closed, true)
            })
        })

        yield* Effect.acquireUseRelease(
          Scope.make(),
          (scope) =>
            Effect.gen(function*() {
              const session = yield* PeerSession.makeTestClient(
                { peerId, documents: [{ document: Task, documentId }] },
                () => Effect.die("unexpected entity request")
              ).pipe(
                Effect.provideService(Scope.Scope, scope),
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
              const flushing = yield* session.flush.pipe(Effect.forkChild)
              yield* Deferred.await(blockedStarted)
              yield* Scope.close(scope, Exit.succeed(undefined))
              yield* Fiber.join(flushing)
              yield* session.flush
              assert.strictEqual(yield* Ref.get(resets), 1)
              assert.isTrue(yield* Ref.get(closed))
              assert.strictEqual(yield* Ref.get(pendingCalls), 2)
              assert.strictEqual(yield* Ref.get(generateCalls), blocked === "pending" ? 1 : 2)
            }),
          (scope) =>
            Deferred.succeed(releaseBlocked, undefined).pipe(
              Effect.andThen(Scope.close(scope, Exit.succeed(undefined))),
              Effect.ignore
            )
        )
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("binds one remote epoch and resets both session directions", () =>
    Effect.gen(function*() {
      const inbound = yield* Queue.unbounded<Uint8Array>()
      const firstReceived = yield* Deferred.make<void>()
      const receiveEnded = yield* Deferred.make<void>()
      const receives = yield* Ref.make(0)
      const resets = yield* Ref.make<ReadonlyArray<PeerSync.Session>>([])
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
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
          yield* PeerSession.makeTestClient(
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
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("preserves the current and unprocessed dirty documents when sending fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const firstDocumentId = yield* Identity.makeDocumentId
      const secondDocumentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
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
                        reason: new ReplicaError.StorageUnavailable({
                          cause: new Error("offline")
                        })
                      })
                    )
                    : Effect.void
                )
              ),
            close: Effect.void
          })
      })
      const session = yield* PeerSession.makeTestClient(
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
    })).pipe(Effect.provide(NodeCrypto.layer)))
})
