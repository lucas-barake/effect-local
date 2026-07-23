import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Entity from "effect/unstable/cluster/Entity"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as DocumentEntity from "../src/DocumentEntity.js"
import * as PeerSession from "../src/PeerSession.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

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

  const makeLiveFixture = (documents: ReadonlyArray<PeerSession.SelectedDocument>) =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const events = yield* Queue.unbounded<CommitPublisher.CommitEvent>()
      const generateStarted = yield* Queue.unbounded<Identity.DocumentId>()
      const generateReleases = yield* Queue.unbounded<void>()
      const generated = yield* Queue.unbounded<Identity.DocumentId>()
      const failGenerate = yield* Ref.make(false)
      const pendingCalls = yield* Ref.make(0)
      const receiveFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
      const subscribed = yield* Deferred.make<void>()
      const subscriberEnded = yield* Deferred.make<void>()
      const closed = yield* Ref.make(0)
      const generateError = new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageUnavailable({ cause: new Error("generate failed") })
      })
      const sync = PeerSync.PeerSync.of({
        definitionHash: permit.definitionHash,
        open: (id) =>
          Effect.succeed({
            peerId: id,
            connectionEpoch: "local-epoch",
            replicaIncarnation: permit.incarnation
          }),
        reset: () => Effect.void,
        generate: (_document, documentId) =>
          Queue.offer(generateStarted, documentId).pipe(
            Effect.andThen(Queue.take(generateReleases)),
            Effect.andThen(Ref.get(failGenerate)),
            Effect.flatMap((shouldFail) =>
              shouldFail ? Effect.fail(generateError) : Queue.offer(generated, documentId)
            ),
            Effect.as({ outbound: null, observedByPeer: false, dirty: false })
          ),
        receive: () => Effect.succeed(result),
        enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
        pending: () => Ref.updateAndGet(pendingCalls, (count) => count + 1).pipe(Effect.as([])),
        markSent: () => Effect.succeed(true)
      })
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.fromEffect(Deferred.await(receiveFailure)),
            send: () => Effect.void,
            close: Ref.update(closed, (count) => count + 1)
          })
      })
      const publisher = CommitPublisher.CommitPublisher.of({
        publishPending: Effect.succeed(0),
        invalidate: () =>
          Queue.offer(events, { _tag: "FullRefreshRequired", refreshGeneration: 1 }).pipe(Effect.asVoid),
        subscribe: Deferred.succeed(subscribed, undefined).pipe(
          Effect.as({
            watermark: Identity.CommitSequence.make(0),
            refreshGeneration: 0,
            events: Stream.fromQueue(events).pipe(Stream.ensuring(Deferred.succeed(subscriberEnded, undefined)))
          })
        )
      })
      const sharding = Sharding.Sharding.of({
        ...({} as Sharding.Sharding["Service"]),
        makeClient: () => Effect.succeed(() => Effect.die("unexpected entity request") as never)
      })
      return {
        peerId,
        documents,
        publisher,
        events,
        generateStarted,
        generateReleases,
        generated,
        failGenerate,
        pendingCalls,
        receiveFailure,
        subscribed,
        subscriberEnded,
        closed,
        generateError,
        layer: Layer.mergeAll(
          Layer.succeed(CommitPublisher.CommitPublisher, publisher),
          Layer.succeed(PeerTransport.PeerTransport, transport),
          Layer.succeed(PeerSync.PeerSync, sync),
          Layer.succeed(ReplicaGate.ReplicaGate, gate),
          Layer.succeed(ReplicaLimits.ReplicaLimits, limits),
          Layer.succeed(Sharding.Sharding, sharding)
        )
      }
    })

  const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(PeerSession.SyncEnvelope))
  const encode = (envelope: typeof PeerSession.SyncEnvelope.Type) =>
    Schema.encodeEffect(SyncEnvelopeJson)(envelope).pipe(
      Effect.map((value) => new TextEncoder().encode(value))
    )

  it.effect("rejects one document id selected with conflicting document types before opening transport", () =>
    Effect.gen(function*() {
      const Note = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const connectCalls = yield* Ref.make(0)
      const sync = PeerSync.PeerSync.of({
        definitionHash: permit.definitionHash,
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
          Ref.updateAndGet(connectCalls, (count) => count + 1).pipe(
            Effect.as({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.never,
              send: () => Effect.void,
              close: Effect.void
            })
          )
      })
      const exit = yield* Effect.exit(Effect.scoped(
        PeerSession.makeTestClient(
          {
            peerId,
            documents: [
              { document: Task, documentId },
              { document: Note, documentId }
            ]
          },
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
      ))
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const failure = Cause.findErrorOption(exit.cause)
        assert.strictEqual(failure._tag, "Some")
        if (failure._tag === "Some") {
          assert.strictEqual(failure.value.reason._tag, "ProtocolMismatch")
        }
      }
      assert.strictEqual(yield* Ref.get(connectCalls), 0)
    }))

  it.effect("does not let a completed inbound apply overwrite newer observed state", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const applyCompleted = yield* Deferred.make<void>()
        const releaseApply = yield* Deferred.make<void>()
        const firstInboundCompleted = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseApply, undefined).pipe(Effect.asVoid))
        const generateCalls = yield* Ref.make(0)
        const applyCalls = yield* Ref.make(0)
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const message = Uint8Array.of(1)
        const messageHash = yield* Canonical.digest(message)
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () =>
            Ref.updateAndGet(generateCalls, (count) => count + 1).pipe(
              Effect.map((call) => ({
                outbound: null,
                observedByPeer: call === 1,
                dirty: false
              }))
            ),
          receive: () => Effect.die("unexpected direct receive"),
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
              send: () => Effect.void,
              close: Effect.void
            })
        })
        const session = yield* PeerSession.makeTestClient(
          { peerId, documents: [{ document: Task, documentId }] },
          () =>
            Effect.succeed({
              ApplySync: () =>
                Ref.updateAndGet(applyCalls, (count) => count + 1).pipe(
                  Effect.flatMap((call) =>
                    call === 1
                      ? Deferred.succeed(applyCompleted, undefined).pipe(
                        Effect.andThen(Deferred.await(releaseApply)),
                        Effect.as({ ...result, observedByPeer: true })
                      )
                      : Deferred.succeed(firstInboundCompleted, undefined).pipe(
                        Effect.andThen(Effect.never)
                      )
                  )
                )
            } as never)
        ).pipe(
          Effect.provideService(PeerTransport.PeerTransport, transport),
          Effect.provideService(PeerSync.PeerSync, sync),
          Effect.provideService(ReplicaGate.ReplicaGate, gate),
          Effect.provideService(
            CommitPublisher.CommitPublisher,
            CommitPublisher.CommitPublisher.of({
              publishPending: Effect.succeed(1),
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
        const envelope = (sequence: number) =>
          encode({
            connectionEpoch: "remote-epoch",
            sequence,
            documentId,
            documentType: Task.name,
            messageHash,
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        yield* Queue.offer(inbound, yield* envelope(0))
        yield* Deferred.await(applyCompleted)
        yield* session.markDirty(documentId)
        const flushing = yield* session.flush.pipe(Effect.forkChild)
        assert.isFalse(yield* session.observedByPeer(documentId))
        yield* Deferred.succeed(releaseApply, undefined)
        yield* Fiber.join(flushing)
        yield* Queue.offer(inbound, yield* envelope(1))
        yield* Deferred.await(firstInboundCompleted)
        assert.isFalse(yield* session.observedByPeer(documentId))
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("does not deadlock a gate claim between inbound and outbound document synchronization", () => {
    const Database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
    const Dependencies = Layer.merge(Database, Bootstrap)
    const Gate = Layer.merge(Dependencies, ReplicaGate.layer.pipe(Layer.provide(Dependencies)))

    return Effect.scoped(
      Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const crypto = yield* Crypto.Crypto
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const digestArmed = yield* Ref.make(false)
        const inboundDigested = yield* Deferred.make<void>()
        const generateLocked = yield* Deferred.make<void>()
        const releaseGenerate = yield* Deferred.make<void>()
        const claimAcquired = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseGenerate, undefined).pipe(Effect.asVoid))
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const message = Uint8Array.of(1)
        const messageHash = yield* Canonical.digest(message)
        const permit = yield* gate.current
        let generateCalls = 0
        const sync = PeerSync.PeerSync.of({
          definitionHash: definition.hash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () => {
            generateCalls++
            return generateCalls === 1
              ? Effect.succeed({ outbound: null, observedByPeer: false, dirty: false })
              : Deferred.succeed(generateLocked, undefined).pipe(
                Effect.andThen(Deferred.await(releaseGenerate)),
                Effect.andThen(Effect.scoped(gate.shared)),
                Effect.as({ outbound: null, observedByPeer: false, dirty: false })
              )
          },
          receive: () => Effect.die("unexpected direct receive"),
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
              send: () => Effect.void,
              close: Effect.void
            })
        })
        const instrumentedCrypto: Crypto.Crypto = {
          ...crypto,
          digest: (algorithm, data) =>
            crypto.digest(algorithm, data).pipe(
              Effect.tap(() =>
                Ref.get(digestArmed).pipe(
                  Effect.flatMap((armed) =>
                    armed ? Deferred.succeed(inboundDigested, undefined).pipe(Effect.asVoid) : Effect.void
                  )
                )
              )
            )
        }
        const session = yield* PeerSession.makeTestClient(
          { peerId, documents: [{ document: Task, documentId }] },
          () =>
            Effect.succeed({
              ApplySync: () => Effect.succeed(result)
            } as never)
        ).pipe(
          Effect.provideService(Crypto.Crypto, instrumentedCrypto),
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
        const flushing = yield* session.flush.pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(generateLocked)
        yield* Ref.set(digestArmed, true)
        yield* Queue.offer(
          inbound,
          yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash,
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        )
        yield* Deferred.await(inboundDigested)
        const claiming = yield* gate.claim(() => Deferred.succeed(claimAcquired, undefined)).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        const acquired = yield* Deferred.await(claimAcquired).pipe(
          Effect.timeout("1 second"),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Effect.yieldNow
        yield* TestClock.adjust("1 second")
        yield* Fiber.join(acquired)
        yield* Deferred.succeed(releaseGenerate, undefined)
        yield* Fiber.join(claiming)
        yield* Fiber.interrupt(flushing)
      }).pipe(
        Effect.provide(Gate),
        Effect.provide(TestShardingConfig)
      )
    )
  })

  it.effect("keeps draining inbound documents and retransmits replies while a send is blocked", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const firstReceived = yield* Deferred.make<void>()
        const secondReceived = yield* Deferred.make<void>()
        const sendStarted = yield* Deferred.make<void>()
        const secondSent = yield* Deferred.make<void>()
        const releaseSend = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseSend, undefined).pipe(Effect.asVoid))
        const sent = yield* Deferred.make<Uint8Array>()
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const message = new Uint8Array([1, 2, 3])
        const published = yield* Ref.make(0)
        const receiveCalls = yield* Ref.make(0)
        const enqueueCalls = yield* Ref.make(0)
        const sendCalls = yield* Ref.make(0)
        const pending = yield* Ref.make<ReadonlyArray<PeerSync.Outbound>>([])
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
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () => Effect.succeed({ outbound: null, observedByPeer: true, dirty: false }),
          receive: () =>
            Effect.gen(function*() {
              const call = yield* Ref.updateAndGet(receiveCalls, (count) => count + 1)
              if (call === 1) {
                yield* Deferred.succeed(firstReceived, undefined)
                return { ...result, reply }
              }
              yield* Deferred.succeed(secondReceived, undefined)
              return { ...result, reply }
            }),
          enqueue: (_session, value) => {
            const outbound = { ...value, sendSequence: 7 }
            return Ref.updateAndGet(enqueueCalls, (count) => count + 1).pipe(
              Effect.flatMap((call) => call === 1 ? Ref.set(pending, [outbound]) : Effect.void),
              Effect.as(outbound)
            )
          },
          pending: () => Ref.get(pending),
          markSent: () => Ref.set(pending, []).pipe(Effect.as(true))
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.fromQueue(inbound),
              send: (bytes) =>
                Effect.gen(function*() {
                  assert.isFalse(yield* Ref.get(gateReleased))
                  const call = yield* Ref.updateAndGet(sendCalls, (count) => count + 1)
                  if (call === 1) {
                    yield* Deferred.succeed(sendStarted, undefined)
                    yield* Deferred.await(releaseSend)
                    yield* Deferred.succeed(sent, bytes)
                  } else {
                    yield* Deferred.succeed(secondSent, undefined)
                  }
                }),
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
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        )
        yield* Deferred.await(firstReceived)
        yield* Deferred.await(sendStarted)
        yield* Queue.offer(
          inbound,
          yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 1,
            documentId,
            documentType: Task.name,
            messageHash: yield* Canonical.digest(message),
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        )
        yield* Deferred.await(secondReceived)
        yield* Deferred.succeed(releaseSend, undefined)
        yield* Deferred.await(secondSent)
        const replyEnvelope = yield* Deferred.await(sent).pipe(
          Effect.flatMap((bytes) => Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)))
        )
        assert.strictEqual(replyEnvelope.sequence, 7)
        assert.strictEqual(replyEnvelope.connectionEpoch, "local-epoch")
        assert.strictEqual(yield* Ref.get(published), 2)
        assert.isTrue(yield* session.observedByPeer(documentId))
        assert.isFalse(yield* session.durableConfirmation(documentId))
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("sends a concurrently enqueued reply before a later generated message", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const sent = yield* Queue.unbounded<number>()
        const firstSendStarted = yield* Deferred.make<void>()
        const releaseFirstSend = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseFirstSend, undefined).pipe(Effect.asVoid))
        const enqueued = yield* Deferred.make<void>()
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const inboundMessage = Uint8Array.of(1)
        const pendingCalls = yield* Ref.make(0)
        const pending = yield* Ref.make<ReadonlyArray<PeerSync.Outbound>>([])
        const initial = {
          sendSequence: 0,
          documentId,
          message: Uint8Array.of(2),
          messageHash: "initial",
          heads: []
        }
        const reply = {
          documentId,
          message: Uint8Array.of(3),
          messageHash: "reply",
          heads: []
        }
        const generated = {
          sendSequence: 2,
          documentId,
          message: Uint8Array.of(4),
          messageHash: "generated",
          heads: []
        }
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () =>
            Ref.get(pending).pipe(
              Effect.flatMap((current) =>
                current.length === 0
                  ? Effect.succeed({ outbound: generated, observedByPeer: false, dirty: false })
                  : Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.QuotaExceeded({
                        resource: "peer sync outbox messages",
                        limit: 1
                      })
                    })
                  )
              )
            ),
          receive: () => Effect.succeed({ ...result, reply }),
          enqueue: (_session, value) => {
            const outbound = { ...value, sendSequence: 1 }
            return Ref.set(pending, [outbound]).pipe(
              Effect.andThen(Deferred.succeed(enqueued, undefined)),
              Effect.as(outbound)
            )
          },
          pending: () =>
            Ref.updateAndGet(pendingCalls, (count) => count + 1).pipe(
              Effect.flatMap((call) => call === 1 ? Effect.succeed([initial]) : Ref.get(pending))
            ),
          markSent: (_session, sendSequence) =>
            Ref.update(pending, (current) => current.filter((outbound) => outbound.sendSequence !== sendSequence)).pipe(
              Effect.as(true)
            )
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.fromQueue(inbound),
              send: (bytes) =>
                Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
                  Effect.tap((envelope) => Queue.offer(sent, envelope.sequence)),
                  Effect.tap((envelope) =>
                    envelope.sequence === 0
                      ? Deferred.succeed(firstSendStarted, undefined).pipe(
                        Effect.andThen(Deferred.await(releaseFirstSend))
                      )
                      : Effect.void
                  ),
                  Effect.asVoid,
                  Effect.orDie
                ),
              close: Effect.void
            })
        })
        const entity = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, gate)),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        const opening = yield* PeerSession.makeTestClient(
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, { ...limits, maxPendingChangesPerPeer: 1 }),
          Effect.forkChild
        )
        yield* Deferred.await(firstSendStarted)
        yield* Queue.offer(
          inbound,
          yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash: yield* Canonical.digest(inboundMessage),
            message: inboundMessage,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        )
        yield* Deferred.await(enqueued)
        yield* Deferred.succeed(releaseFirstSend, undefined)
        yield* Fiber.join(opening)
        assert.deepStrictEqual(
          [yield* Queue.take(sent), yield* Queue.take(sent), yield* Queue.take(sent)],
          [0, 1, 2]
        )
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("drains each generated message before consuming the next pending quota slot", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const firstDocumentId = yield* Identity.makeDocumentId
        const secondDocumentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const pendingCount = yield* Ref.make(0)
        const nextSequence = yield* Ref.make(0)
        const sentDocuments = yield* Ref.make<ReadonlyArray<Identity.DocumentId>>([])
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: (_document, documentId) =>
            Ref.get(pendingCount).pipe(
              Effect.flatMap((count) =>
                count >= 1
                  ? Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.QuotaExceeded({
                        resource: "peer sync outbox messages",
                        limit: 1
                      })
                    })
                  )
                  : Ref.set(pendingCount, 1)
              ),
              Effect.andThen(Ref.getAndUpdate(nextSequence, (sequence) => sequence + 1)),
              Effect.map((sendSequence) => ({
                outbound: {
                  sendSequence,
                  documentId,
                  message: Uint8Array.of(sendSequence),
                  messageHash: `hash-${sendSequence}`,
                  heads: []
                },
                observedByPeer: false,
                dirty: false
              }))
            ),
          receive: () => Effect.succeed(result),
          enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0 }),
          pending: () => Effect.succeed([]),
          markSent: () => Ref.set(pendingCount, 0).pipe(Effect.as(true))
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.never,
              send: (bytes) =>
                Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
                  Effect.flatMap((envelope) =>
                    Ref.update(sentDocuments, (current) => [...current, envelope.documentId])
                  ),
                  Effect.orDie
                ),
              close: Effect.void
            })
        })
        yield* PeerSession.makeTestClient(
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
          Effect.provideService(ReplicaLimits.ReplicaLimits, { ...limits, maxPendingChangesPerPeer: 1 })
        )
        assert.deepStrictEqual(yield* Ref.get(sentDocuments), [firstDocumentId, secondDocumentId])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("retries scheduled output after a manual flush defects", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const generateCalls = yield* Ref.make(0)
        const sendCalls = yield* Ref.make(0)
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () =>
            Ref.updateAndGet(generateCalls, (count) => count + 1).pipe(
              Effect.map((call) => ({
                outbound: call === 2
                  ? {
                    sendSequence: 9,
                    documentId,
                    message: Uint8Array.of(9),
                    messageHash: "hash-9",
                    heads: []
                  }
                  : null,
                observedByPeer: false,
                dirty: false
              }))
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
                Ref.updateAndGet(sendCalls, (count) => count + 1).pipe(
                  Effect.flatMap((call) =>
                    call === 1
                      ? Effect.die(new Error("send defect"))
                      : Effect.void
                  ),
                  Effect.asVoid
                ),
              close: Effect.void
            })
        })
        const session = yield* PeerSession.makeTestClient(
          { peerId, documents: [{ document: Task, documentId }] },
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
        yield* session.markDirty(documentId)
        assert.strictEqual((yield* Effect.exit(session.flush))._tag, "Failure")
        yield* session.flush
        assert.strictEqual(yield* Ref.get(sendCalls), 2)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("fails the session when the automatic reply flush defects", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<Uint8Array>()
        const closed = yield* Deferred.make<void>()
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const message = Uint8Array.of(1)
        const pending = yield* Ref.make<ReadonlyArray<PeerSync.Outbound>>([])
        const reply = {
          documentId,
          message: Uint8Array.of(2),
          messageHash: "reply",
          heads: []
        }
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
          open: (id) =>
            Effect.succeed({
              peerId: id,
              connectionEpoch: "local-epoch",
              replicaIncarnation: permit.incarnation
            }),
          reset: () => Effect.void,
          generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
          receive: () => Effect.succeed({ ...result, reply }),
          enqueue: (_session, value) => {
            const outbound = { ...value, sendSequence: 0 }
            return Ref.set(pending, [outbound]).pipe(Effect.as(outbound))
          },
          pending: () => Ref.get(pending),
          markSent: () => Effect.succeed(true)
        })
        const transport = PeerTransport.PeerTransport.of({
          capabilities: { storeAndForward: false },
          connect: () =>
            Effect.succeed({
              peerId,
              capabilities: { storeAndForward: false },
              receive: Stream.fromQueue(inbound),
              send: () => Effect.die(new Error("automatic send defect")),
              close: Deferred.succeed(closed, undefined).pipe(Effect.asVoid)
            })
        })
        const entity = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, gate)),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        yield* PeerSession.makeTestClient(
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
        yield* Queue.offer(
          inbound,
          yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash: yield* Canonical.digest(message),
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        )
        yield* Deferred.await(closed)
        assert.strictEqual((yield* Ref.get(pending)).length, 1)
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
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
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
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          })
        },
        {
          name: "non-positive writer schema version",
          bytes: yield* encode({
            connectionEpoch: "remote-epoch",
            sequence: 0,
            documentId,
            documentType: Task.name,
            messageHash,
            message,
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          }).pipe(
            Effect.map((valid) => {
              const tampered = JSON.parse(new TextDecoder().decode(valid))
              tampered.writerSchemaVersion = 0
              return new TextEncoder().encode(JSON.stringify(tampered))
            })
          )
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
            definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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

  it.effect("reports an ended receive stream before blocked connection cleanup completes", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const peerId = yield* Identity.makePeerId
        const endReceive = yield* Deferred.make<void>()
        const closeStarted = yield* Deferred.make<void>()
        const releaseClose = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() => Deferred.succeed(releaseClose, undefined).pipe(Effect.asVoid))
        const sync = PeerSync.PeerSync.of({
          definitionHash: permit.definitionHash,
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
              receive: Stream.fromEffect(Deferred.await(endReceive)).pipe(Stream.drain),
              send: () => Effect.void,
              close: Deferred.succeed(closeStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseClose))
              )
            })
        })
        const session = yield* PeerSession.makeTestClient(
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
        yield* Deferred.succeed(endReceive, undefined)
        yield* Deferred.await(closeStarted)
        const error = yield* session.flush.pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "StorageUnavailable")
        yield* Deferred.succeed(releaseClose, undefined)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

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
        definitionHash: permit.definitionHash,
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
                message,
                writerSchemaVersion: Task.version,
                writerDefinitionHash: definition.hash
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
        definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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
          definitionHash: permit.definitionHash,
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
        definitionHash: permit.definitionHash,
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
                      message,
                      writerSchemaVersion: Task.version,
                      writerDefinitionHash: definition.hash
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
              message,
              writerSchemaVersion: Task.version,
              writerDefinitionHash: definition.hash
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

  it.effect("retries every unsent dirty output after a send fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const firstDocumentId = yield* Identity.makeDocumentId
      const secondDocumentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const generated = yield* Ref.make<ReadonlyArray<Identity.DocumentId>>([])
      const sends = yield* Ref.make(0)
      const sentSequences = yield* Ref.make<ReadonlyArray<number>>([])
      const sync = PeerSync.PeerSync.of({
        definitionHash: permit.definitionHash,
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
            send: (bytes) =>
              Schema.decodeUnknownEffect(SyncEnvelopeJson)(new TextDecoder().decode(bytes)).pipe(
                Effect.orDie,
                Effect.tap((envelope) => Ref.update(sentSequences, (current) => [...current, envelope.sequence])),
                Effect.andThen(Ref.updateAndGet(sends, (count) => count + 1)),
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
      assert.deepStrictEqual(yield* Ref.get(sentSequences), [3, 3, 4, 5])
    })).pipe(Effect.provide(NodeCrypto.layer)))

  it("keeps existing structural PeerSession implementations source compatible", () => {
    const session: PeerSession.PeerSession = {
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001"),
      connectionEpoch: "structural",
      markDirty: () => Effect.void,
      flush: Effect.void,
      observedByPeer: () => Effect.succeed(false),
      durableConfirmation: () => Effect.succeed(false as const)
    }
    assert.strictEqual(session.connectionEpoch, "structural")
  })

  it.effect("publishes a selected commit made after the live subscription is acquired", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const documentId = yield* Identity.makeDocumentId
        const fixture = yield* makeLiveFixture([{ document: Task, documentId }])
        const sessionFiber = yield* PeerSession.makeLive({
          peerId: fixture.peerId,
          documents: fixture.documents
        }).pipe(Effect.provide(fixture.layer), Effect.forkChild)
        yield* Deferred.await(fixture.subscribed)
        yield* Queue.offer(fixture.events, {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(1),
          documentId,
          keys: [],
          refreshGeneration: 0
        })
        assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
        yield* Queue.offer(fixture.generateReleases, undefined)
        const session = yield* Fiber.join(sessionFiber)
        const base: PeerSession.PeerSession = session
        assert.strictEqual(base.connectionEpoch, "local-epoch")
        assert.strictEqual(yield* Queue.take(fixture.generated), documentId)
        assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
        yield* Queue.offer(fixture.generateReleases, undefined)
        assert.strictEqual(yield* Queue.take(fixture.generated), documentId)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("ignores commits for documents outside the selected set", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const documentId = yield* Identity.makeDocumentId
        const otherDocumentId = yield* Identity.makeDocumentId
        const fixture = yield* makeLiveFixture([{ document: Task, documentId }])
        const sessionFiber = yield* PeerSession.makeLive({
          peerId: fixture.peerId,
          documents: fixture.documents
        }).pipe(Effect.provide(fixture.layer), Effect.forkChild)
        assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
        yield* Queue.offer(fixture.generateReleases, undefined)
        yield* Fiber.join(sessionFiber)
        yield* Queue.take(fixture.generated)
        yield* Queue.offerAll(fixture.events, [
          {
            _tag: "Commit",
            commitSequence: Identity.CommitSequence.make(1),
            documentId: otherDocumentId,
            keys: [],
            refreshGeneration: 0
          },
          {
            _tag: "Commit",
            commitSequence: Identity.CommitSequence.make(2),
            documentId,
            keys: [],
            refreshGeneration: 0
          }
        ])
        assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
        yield* Queue.offer(fixture.generateReleases, undefined)
        yield* Queue.take(fixture.generated)
        assert.strictEqual(yield* Queue.size(fixture.generateStarted), 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("marks every selected document after full refresh is required", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const firstDocumentId = yield* Identity.makeDocumentId
        const secondDocumentId = yield* Identity.makeDocumentId
        const fixture = yield* makeLiveFixture([
          { document: Task, documentId: firstDocumentId },
          { document: Task, documentId: secondDocumentId }
        ])
        const sessionFiber = yield* PeerSession.makeLive({
          peerId: fixture.peerId,
          documents: fixture.documents
        }).pipe(Effect.provide(fixture.layer), Effect.forkChild)
        for (const documentId of [firstDocumentId, secondDocumentId]) {
          assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
          yield* Queue.offer(fixture.generateReleases, undefined)
          yield* Queue.take(fixture.generated)
        }
        yield* Fiber.join(sessionFiber)
        yield* fixture.publisher.invalidate([])
        for (const documentId of [firstDocumentId, secondDocumentId]) {
          assert.strictEqual(yield* Queue.take(fixture.generateStarted), documentId)
          yield* Queue.offer(fixture.generateReleases, undefined)
          yield* Queue.take(fixture.generated)
        }
        assert.strictEqual(yield* Ref.get(fixture.pendingCalls), 2)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("stops the live commit subscriber when the session scope closes", () =>
    Effect.gen(function*() {
      const documentId = yield* Identity.makeDocumentId
      const fixture = yield* makeLiveFixture([{ document: Task, documentId }])
      yield* Effect.scoped(Effect.gen(function*() {
        const sessionFiber = yield* PeerSession.makeLive({
          peerId: fixture.peerId,
          documents: fixture.documents
        }).pipe(Effect.provide(fixture.layer), Effect.forkChild)
        yield* Queue.take(fixture.generateStarted)
        yield* Queue.offer(fixture.generateReleases, undefined)
        yield* Fiber.join(sessionFiber)
        yield* Queue.take(fixture.generated)
      }))
      yield* Deferred.await(fixture.subscriberEnded)
      yield* Queue.offer(fixture.events, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId,
        keys: [],
        refreshGeneration: 0
      })
      assert.strictEqual(yield* Queue.size(fixture.generateStarted), 0)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("fails awaitDisconnect with the receive failure", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fixture = yield* makeLiveFixture([])
        const session = yield* PeerSession.makeLive({ peerId: fixture.peerId, documents: [] }).pipe(
          Effect.provide(fixture.layer)
        )
        const receiveError = new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageUnavailable({ cause: new Error("receive failed") })
        })
        yield* Deferred.fail(fixture.receiveFailure, receiveError)
        assert.strictEqual(yield* Effect.flip(session.awaitDisconnect), receiveError)
        yield* Deferred.await(fixture.subscriberEnded)
        assert.isAbove(yield* Ref.get(fixture.closed), 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("exposes supervised disconnect failure without a commit subscription", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fixture = yield* makeLiveFixture([])
        const session = yield* PeerSession.makeSupervised({ peerId: fixture.peerId, documents: [] }).pipe(
          Effect.provide(fixture.layer)
        )
        assert.strictEqual((yield* Deferred.poll(fixture.subscribed))._tag, "None")
        const receiveError = new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageUnavailable({ cause: new Error("supervised receive failed") })
        })
        yield* Deferred.fail(fixture.receiveFailure, receiveError)
        assert.strictEqual(yield* Effect.flip(session.awaitDisconnect), receiveError)
        assert.isAbove(yield* Ref.get(fixture.closed), 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("interrupts awaitDisconnect when the session scope closes normally", () =>
    Effect.gen(function*() {
      const fixture = yield* makeLiveFixture([])
      const disconnectFiber = yield* Effect.scoped(Effect.gen(function*() {
        const session = yield* PeerSession.makeLive({ peerId: fixture.peerId, documents: [] }).pipe(
          Effect.provide(fixture.layer)
        )
        return yield* session.awaitDisconnect.pipe(Effect.forkScoped)
      }))
      const exit = yield* Fiber.await(disconnectFiber)
      assert.isTrue(exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("fails the supervised session when commit driven flush fails", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const documentId = yield* Identity.makeDocumentId
        const fixture = yield* makeLiveFixture([{ document: Task, documentId }])
        const sessionFiber = yield* PeerSession.makeLive({
          peerId: fixture.peerId,
          documents: fixture.documents
        }).pipe(Effect.provide(fixture.layer), Effect.forkChild)
        yield* Queue.take(fixture.generateStarted)
        yield* Queue.offer(fixture.generateReleases, undefined)
        const session = yield* Fiber.join(sessionFiber)
        yield* Queue.take(fixture.generated)
        yield* Ref.set(fixture.failGenerate, true)
        yield* Queue.offer(fixture.events, {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(1),
          documentId,
          keys: [],
          refreshGeneration: 0
        })
        yield* Queue.take(fixture.generateStarted)
        yield* Queue.offer(fixture.generateReleases, undefined)
        assert.strictEqual(yield* Effect.flip(session.awaitDisconnect), fixture.generateError)
        yield* Deferred.await(fixture.subscriberEnded)
        assert.isAbove(yield* Ref.get(fixture.closed), 0)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("does not hold the shared gate permit across an inbound apply dispatch", () => {
    const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
    const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
    const Dependencies = Layer.merge(Database, Bootstrap)
    const TestGate = Layer.merge(Dependencies, ReplicaGate.layer.pipe(Layer.provide(Dependencies)))
    return Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const crypto = yield* Crypto.Crypto
      const initial = yield* gate.current
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      const message = Uint8Array.of(1)
      const messageHash = yield* Canonical.digest(message).pipe(Effect.provideService(Crypto.Crypto, crypto))
      const applyReady = yield* Deferred.make<void>()
      const applyProceed = yield* Deferred.make<void>()
      const claimRan = yield* Deferred.make<void>()
      const applyResult = {
        reply: null,
        heads: [],
        acceptedHeads: [],
        commitSequence: Identity.CommitSequence.make(1),
        observedByPeer: false,
        durableConfirmation: false as const,
        duplicate: false
      }
      const entity = (): Effect.Effect<ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>> =>
        Effect.succeed(
          {
            ApplySync: () =>
              Deferred.succeed(applyReady, undefined).pipe(
                Effect.andThen(Deferred.await(applyProceed)),
                Effect.andThen(Effect.scoped(gate.shared)),
                Effect.as(applyResult),
                Effect.forkChild,
                Effect.flatMap(Fiber.join)
              )
          } as unknown as ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>
        )
      const inbound = yield* Queue.unbounded<Uint8Array>()
      const sync = PeerSync.PeerSync.of({
        definitionHash: permit.definitionHash,
        open: (id) =>
          Effect.succeed({ peerId: id, connectionEpoch: "local-epoch", replicaIncarnation: initial.incarnation }),
        reset: () => Effect.void,
        generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
        receive: () => Effect.succeed(applyResult),
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
            send: () => Effect.void,
            close: Effect.void
          })
      })
      yield* PeerSession.makeTestClient({ peerId, documents: [{ document: Task, documentId }] }, entity).pipe(
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
      yield* Queue.offer(
        inbound,
        yield* encode({
          connectionEpoch: "remote-epoch",
          sequence: 0,
          documentId,
          documentType: Task.name,
          messageHash,
          message,
          writerSchemaVersion: Task.version,
          writerDefinitionHash: definition.hash
        })
      )
      yield* Deferred.await(applyReady)
      yield* Effect.forkChild(gate.claim(() => Deferred.succeed(claimRan, undefined)))
      for (let index = 0; index < 20; index++) yield* Effect.yieldNow
      yield* Deferred.succeed(applyProceed, undefined)
      for (let index = 0; index < 200; index++) yield* Effect.yieldNow
      assert.isTrue(Option.isSome(yield* Deferred.poll(claimRan)))
    }).pipe(Effect.provide(TestGate))
  }, 20_000)
})
