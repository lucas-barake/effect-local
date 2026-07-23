import { assert, describe, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import type * as DocumentEntity from "@lucas-barake/effect-local-sql/DocumentEntity"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Tracer from "effect/Tracer"
import * as Sharding from "effect/unstable/cluster/Sharding"
import type * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import { createHash, randomBytes } from "node:crypto"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerAuthorization from "../src/PeerAuthorization.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const Note = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
const taskId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const noteId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerA = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const remotePeerB = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const missingSessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const permit = {
  replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001"),
  incarnation: Identity.ReplicaIncarnation.make(1),
  writerGeneration: Identity.WriterGeneration.make(1)
}
const replicaLimits = ReplicaLimits.Values.make({
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
})
const rpcLimits = PeerRpcLimits.Values.make({
  ...PeerRpcLimits.defaults,
  openRatePerSecond: 1_000,
  openBurst: 1_000,
  pushRatePerSecond: 1_000,
  pushBurst: 1_000,
  authenticationRatePerSecond: 1_000,
  authenticationBurst: 1_000,
  maximumReauthorizationInterval: 60_000
})
const crypto = Crypto.make({
  randomBytes: (size) => randomBytes(size),
  digest: (algorithm, bytes) =>
    Effect.sync(() => new Uint8Array(createHash(algorithm.replace("-", "").toLowerCase()).update(bytes).digest()))
})
const SyncEnvelopeJson = Schema.fromJsonString(Schema.toCodecJson(PeerSession.SyncEnvelope))
const baseOptions = {
  rpcLimits: {},
  replicaLimits: {},
  initialOutbound: null,
  authorization: undefined,
  authenticationValidUntil: Number.MAX_SAFE_INTEGER,
  authorizationValidUntil: Number.MAX_SAFE_INTEGER,
  blockInbound: false,
  blockAuthorization: false,
  failSessionOpen: false,
  manualClock: false
}

const makeFixture = (options: {
  readonly rpcLimits: Partial<PeerRpcLimits.Values>
  readonly replicaLimits: Partial<ReplicaLimits.Values>
  readonly initialOutbound: PeerSync.Outbound | null
  readonly authorization:
    | ((request: {
      readonly principal: PeerAuthentication.PeerPrincipal
      readonly documents: ReadonlyArray<PeerRpc.RequestedDocument>
    }) => Effect.Effect<{
      readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
      readonly validUntil: number
      readonly invalidated: Effect.Effect<void>
    }, PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable>)
    | undefined
  readonly authenticationValidUntil: number
  readonly authorizationValidUntil: number
  readonly blockInbound: boolean
  readonly blockAuthorization: boolean
  readonly failSessionOpen: boolean
  readonly manualClock: boolean
}) =>
  Effect.gen(function*() {
    const configuredReplicaLimits = ReplicaLimits.Values.make({ ...replicaLimits, ...options.replicaLimits })
    const configuredRpcLimits = PeerRpcLimits.Values.make({ ...rpcLimits, ...options.rpcLimits })
    const commits = yield* Queue.unbounded<CommitPublisher.CommitEvent>()
    const generated = yield* Queue.unbounded<Identity.DocumentId>()
    const received = yield* Queue.unbounded<number>()
    const sent = yield* Queue.unbounded<number>()
    const enqueued = yield* Queue.unbounded<Identity.DocumentId>()
    const pendingStarted = yield* Queue.unbounded<Identity.PeerId>()
    const inboundRelease = yield* Deferred.make<void>()
    const inboundBlocked = yield* Deferred.make<void>()
    const authenticationInvalidated = yield* Deferred.make<void>()
    const commitProcessed = yield* Queue.unbounded<void>()
    const commitFlushStarted = yield* Deferred.make<void>()
    const commitFlushRelease = yield* Deferred.make<void>()
    const authorizationInvalidated = yield* Deferred.make<void>()
    const authorizationRelease = yield* Deferred.make<void>()
    const authorizationStarted = yield* Queue.unbounded<string>()
    const authorizationCalls = yield* Ref.make(0)
    const subscriptions = yield* Ref.make(0)
    const publications = yield* Ref.make(0)
    let activeFibers = 0
    let credential = "owner"
    let initialOutbound = options.initialOutbound
    let failSessionOpen = options.failSessionOpen
    let reply: PeerSync.Reply | null = null
    let blockCommitFlush = false
    let currentTime = 0
    let currentTimeOnNextRandomBytes: number | undefined
    const clock = {
      currentTimeMillisUnsafe: () => currentTime,
      currentTimeMillis: Effect.sync(() => currentTime),
      currentTimeNanosUnsafe: () => BigInt(currentTime) * 1_000_000n,
      currentTimeNanos: Effect.sync(() => BigInt(currentTime) * 1_000_000n),
      sleep: () => Effect.never
    } satisfies Clock.Clock
    const fixtureCrypto = Crypto.make({
      randomBytes: (size) => {
        if (currentTimeOnNextRandomBytes !== undefined) {
          currentTime = currentTimeOnNextRandomBytes
          currentTimeOnNextRandomBytes = undefined
        }
        return randomBytes(size)
      },
      digest: crypto.digest
    })
    const nextOutbound = yield* Ref.make<PeerSync.Outbound | null>(null)
    const enqueuedOutbounds = yield* Ref.make(new Map<Identity.PeerId, ReadonlyArray<PeerSync.Outbound>>())
    const principals = new Map([
      [
        "owner",
        PeerAuthentication.PeerPrincipal.make({ tenantId: "tenant", subjectId: "subject-a", peerId: remotePeerA })
      ],
      [
        "same-subject",
        PeerAuthentication.PeerPrincipal.make({
          tenantId: "tenant",
          subjectId: "subject-a",
          peerId: remotePeerB
        })
      ],
      [
        "same-peer",
        PeerAuthentication.PeerPrincipal.make({
          tenantId: "tenant",
          subjectId: "subject-b",
          peerId: remotePeerA
        })
      ],
      [
        "foreign",
        PeerAuthentication.PeerPrincipal.make({
          tenantId: "tenant",
          subjectId: "subject-b",
          peerId: remotePeerB
        })
      ],
      [
        "other-tenant",
        PeerAuthentication.PeerPrincipal.make({
          tenantId: "other",
          subjectId: "subject-c",
          peerId: remotePeerB
        })
      ],
      [
        "third",
        PeerAuthentication.PeerPrincipal.make({
          tenantId: "tenant",
          subjectId: "subject-c",
          peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000010")
        })
      ]
    ])
    const gate = ReplicaGate.ReplicaGate.of({
      current: Effect.succeed(permit),
      shared: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
      claim: (use) => use(permit),
      refresh: Effect.succeed(permit),
      validate: () => Effect.void
    })
    const sync = PeerSync.PeerSync.of({
      open: (peerId) =>
        failSessionOpen
          ? Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause: new Error("session startup failed") })
            })
          )
          : Effect.succeed({ peerId, connectionEpoch: "local-epoch", replicaIncarnation: permit.incarnation }),
      reset: () => Effect.void,
      generate: (_document, documentId) =>
        Queue.offer(generated, documentId).pipe(
          Effect.andThen(
            blockCommitFlush
              ? Deferred.succeed(commitFlushStarted, undefined).pipe(
                Effect.andThen(Deferred.await(commitFlushRelease))
              )
              : Effect.void
          ),
          Effect.andThen(Ref.getAndSet(nextOutbound, null)),
          Effect.map((outbound) => ({ outbound, observedByPeer: false, dirty: false }))
        ),
      receive: () => Effect.die("unexpected direct PeerSync receive"),
      enqueue: (session, reply) => {
        const outbound = { ...reply, sendSequence: 0 }
        return Queue.offer(enqueued, reply.documentId).pipe(
          Effect.andThen(Ref.update(enqueuedOutbounds, (pendingByPeer) => {
            const next = new Map(pendingByPeer)
            next.set(session.peerId, [...(next.get(session.peerId) ?? []), outbound])
            return next
          })),
          Effect.as(outbound)
        )
      },
      pending: (session) =>
        Queue.offer(pendingStarted, session.peerId).pipe(
          Effect.andThen(Ref.modify(enqueuedOutbounds, (pendingByPeer) => {
            const next = new Map(pendingByPeer)
            const pending = next.get(session.peerId) ?? []
            next.delete(session.peerId)
            const initial = initialOutbound === null ? [] : [initialOutbound]
            initialOutbound = null
            return [[...initial, ...pending], next]
          }))
        ),
      markSent: (_session, sequence) => Queue.offer(sent, sequence).pipe(Effect.as(true))
    })
    const publisher = CommitPublisher.CommitPublisher.of({
      publishPending: Ref.updateAndGet(publications, (count) => count + 1),
      invalidate: () => Effect.void,
      subscribe: Ref.update(subscriptions, (count) => count + 1).pipe(
        Effect.as({
          watermark: Identity.CommitSequence.make(0),
          refreshGeneration: 0,
          events: Stream.fromQueue(commits).pipe(
            Stream.tap(() => Queue.offer(commitProcessed, undefined))
          )
        })
      )
    })
    const applyResult = {
      reply: null,
      heads: [],
      acceptedHeads: [],
      commitSequence: Identity.CommitSequence.make(1),
      observedByPeer: true,
      durableConfirmation: false as const,
      duplicate: false
    }
    const sharding = Sharding.Sharding.of({
      ...({} as Sharding.Sharding["Service"]),
      makeClient: () =>
        Effect.succeed(() =>
          ({
            ApplySync: (payload: typeof DocumentEntity.ApplySync.payloadSchema.Type) =>
              Queue.offer(received, payload.receiveSequence).pipe(
                Effect.andThen(
                  options.blockInbound
                    ? Deferred.succeed(inboundBlocked, undefined).pipe(Effect.andThen(Deferred.await(inboundRelease)))
                    : Effect.void
                ),
                Effect.as({ ...applyResult, reply })
              )
          }) as never
        )
    })
    const authorization = PeerAuthorization.PeerAuthorization.of({
      authorize: options.authorization ?? ((request) =>
        Ref.update(authorizationCalls, (count) => count + 1).pipe(
          Effect.andThen(Queue.offer(authorizationStarted, request.principal.subjectId)),
          Effect.andThen(options.blockAuthorization ? Deferred.await(authorizationRelease) : Effect.void),
          Effect.as({
            documents: request.documents.map((requested) => ({
              document: requested.documentType === Task.name ? Task : Note,
              documentId: requested.documentId
            })),
            validUntil: options.authorizationValidUntil,
            invalidated: Deferred.await(authorizationInvalidated)
          })
        ))
    })
    const services = Layer.mergeAll(
      Layer.succeed(Crypto.Crypto, fixtureCrypto),
      Layer.succeed(CommitPublisher.CommitPublisher, publisher),
      Layer.succeed(PeerSync.PeerSync, sync),
      Layer.succeed(ReplicaGate.ReplicaGate, gate),
      Layer.succeed(ReplicaLimits.ReplicaLimits, configuredReplicaLimits),
      Layer.succeed(Sharding.Sharding, sharding),
      Layer.succeed(PeerRpcLimits.PeerRpcLimits, configuredRpcLimits),
      Layer.succeed(PeerAuthorization.PeerAuthorization, authorization),
      options.manualClock ? Layer.succeed(Clock.Clock, clock) : Layer.empty,
      Layer.succeed(Metric.FiberRuntimeMetrics, {
        recordFiberStart: () => void (activeFibers += 1),
        recordFiberEnd: () => void (activeFibers -= 1)
      })
    )
    const handlers = PeerRpcServer.layerHandlers({ tenantId: "tenant", peerId: serverPeerId }).pipe(
      Layer.provide(services)
    )
    const handlerScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void))
    const handlerContext = yield* Layer.build(handlers).pipe(Effect.provideService(Scope.Scope, handlerScope))
    const openHandler = handlerContext.mapUnsafe.get(PeerRpc.OpenRpc.key) as Rpc.Handler<"Open">
    const pushHandler = handlerContext.mapUnsafe.get(PeerRpc.PushRpc.key) as Rpc.Handler<"Push">
    const directOpenAs = (
      principal: PeerAuthentication.PeerPrincipal,
      documents: ReadonlyArray<PeerRpc.RequestedDocument>
    ) =>
      (openHandler.handler({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents
      }, {} as never) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>).pipe(
        Stream.provideContext(Context.add(
          openHandler.context,
          PeerAuthentication.AuthenticatedPeer,
          {
            principal,
            validUntil: options.authenticationValidUntil,
            invalidated: Deferred.await(authenticationInvalidated)
          }
        ))
      )
    const directOpen = (documents: ReadonlyArray<PeerRpc.RequestedDocument>) =>
      directOpenAs(principals.get("owner")!, documents)
    const directPushAs = (
      validUntil: number,
      request: typeof PeerRpc.PushRpc.payloadSchema.Type
    ) =>
      (pushHandler.handler(request, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>).pipe(
        Effect.provideContext(Context.add(
          pushHandler.context,
          PeerAuthentication.AuthenticatedPeer,
          {
            principal: principals.get("owner")!,
            validUntil,
            invalidated: Deferred.await(authenticationInvalidated)
          }
        ))
      )
    const directPush = (request: typeof PeerRpc.PushRpc.payloadSchema.Type) =>
      directPushAs(options.authenticationValidUntil, request)
    const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
      Effect.provide(handlerContext),
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: (value) => {
          const credential = Redacted.value(value)
          const bulk = /^bulk-(\d+)$/.exec(credential)
          const subject = /^subject-(\d+)$/.exec(credential)
          const principal = principals.get(credential) ?? (bulk === null && subject === null
            ? undefined
            : PeerAuthentication.PeerPrincipal.make({
              tenantId: "tenant",
              subjectId: subject === null ? "bulk" : `subject-${subject[1]}`,
              peerId: Identity.PeerId.make(
                `peer_00000000-0000-4000-8000-${Number((bulk ?? subject)![1]).toString(16).padStart(12, "0")}`
              )
            }))
          return principal === undefined
            ? Effect.fail(new PeerRpcError.AuthenticationFailure())
            : Effect.succeed({
              principal,
              validUntil: options.authenticationValidUntil,
              invalidated: Deferred.await(authenticationInvalidated)
            })
        }
      }),
      Effect.provide(PeerAuthentication.layerClient),
      Effect.provideService(PeerCredentials.PeerCredentials, {
        get: Effect.sync(() => Redacted.make(credential))
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, configuredRpcLimits)
    )
    const open = (documents: ReadonlyArray<PeerRpc.RequestedDocument>) =>
      Effect.gen(function*() {
        const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
        const fiber = yield* Stream.runForEach(
          client.Open({ protocolVersion: PeerRpc.protocolVersion, expectedPeerId: serverPeerId, documents }),
          (event) => Queue.offer(events, event).pipe(Effect.asVoid)
        ).pipe(Effect.forkChild)
        const opened = yield* Effect.raceFirst(
          Queue.take(events),
          Fiber.join(fiber).pipe(Effect.andThen(Effect.die("Open stream ended before Opened")))
        )
        assert.strictEqual(opened._tag, "Opened")
        return { opened: opened as PeerRpc.Opened, events, fiber }
      })
    const encodeMessage = (
      sequence: number,
      documentId: Identity.DocumentId,
      documentType: string,
      message: Uint8Array
    ) =>
      Effect.gen(function*() {
        const value = yield* Schema.encodeEffect(SyncEnvelopeJson)({
          connectionEpoch: "remote-epoch",
          sequence,
          documentId,
          documentType,
          messageHash: yield* Canonical.digest(message).pipe(Effect.provideService(Crypto.Crypto, crypto)),
          message
        })
        return new TextEncoder().encode(value)
      })
    const encode = (sequence: number, documentId = taskId, documentType = Task.name) =>
      encodeMessage(sequence, documentId, documentType, Uint8Array.of(sequence + 1))
    return {
      client,
      commits,
      generated,
      received,
      sent,
      enqueued,
      pendingStarted,
      inboundRelease,
      inboundBlocked,
      authenticationInvalidated,
      commitProcessed,
      commitFlushStarted,
      commitFlushRelease,
      authorizationInvalidated,
      authorizationRelease,
      authorizationStarted,
      authorizationCalls,
      subscriptions,
      publications,
      setCredential: (value: string) => Effect.sync(() => void (credential = value)),
      allowSessionOpen: Effect.sync(() => void (failSessionOpen = false)),
      setPendingOutbound: (outbound: PeerSync.Outbound) => Effect.sync(() => void (initialOutbound = outbound)),
      setNextOutbound: (outbound: PeerSync.Outbound) => Ref.set(nextOutbound, outbound),
      setReply: (value: PeerSync.Reply) => Effect.sync(() => void (reply = value)),
      setCurrentTime: (value: number) => Effect.sync(() => void (currentTime = value)),
      setCurrentTimeOnNextRandomBytes: (value: number) =>
        Effect.sync(() => void (currentTimeOnNextRandomBytes = value)),
      blockCommitGeneration: Effect.sync(() => void (blockCommitFlush = true)),
      activeFiberCount: Effect.sync(() => activeFibers),
      closeServer: Scope.close(handlerScope, Exit.void),
      open,
      directOpen,
      directPush,
      directPushAs,
      directOpenAs: (credential: "owner" | "foreign" | "third", documents: ReadonlyArray<PeerRpc.RequestedDocument>) =>
        directOpenAs(principals.get(credential)!, documents),
      encode,
      encodeMessage
    }
  })

describe("PeerRpcServer", () => {
  it.effect("emits Opened before any peer Message", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: Uint8Array.of(1, 2, 3),
          messageHash: "hash",
          heads: []
        }
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      assert.strictEqual((yield* Queue.take(session.events))._tag, "Message")
      yield* Fiber.interrupt(session.fiber)
      assert.strictEqual(yield* Ref.get(fixture.subscriptions), 1)
    })))

  it.effect("hosts the existing canonical replica through the real PeerSession", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(0) })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      assert.strictEqual(yield* Ref.get(fixture.publications), 1)
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("serially applies Push messages in accepted order", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(0) })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(1) })
      assert.strictEqual(yield* Queue.take(fixture.received), 1)
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("replaces the prior session without letting stale cleanup remove the replacement", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const first = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      const replacement = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      const oldExit = yield* Fiber.await(first.fiber)
      assert.isTrue(Exit.isFailure(oldExit))
      if (Exit.isFailure(oldExit)) {
        const streamError = Cause.findErrorOption(oldExit.cause)
        assert.strictEqual(streamError._tag, "Some")
        if (streamError._tag === "Some") assert.strictEqual(streamError.value._tag, "SessionUnavailable")
      }
      const oldError = yield* fixture.client.Push({
        sessionId: first.opened.sessionId,
        payload: Uint8Array.of(1)
      }).pipe(Effect.flip)
      assert.instanceOf(oldError, PeerRpcError.SessionUnavailable)
      yield* Fiber.interrupt(first.fiber)
      yield* fixture.client.Push({
        sessionId: replacement.opened.sessionId,
        payload: yield* fixture.encode(0)
      })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      yield* Fiber.interrupt(replacement.fiber)
    })))

  it.effect("enforces the global session limit", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({ ...baseOptions, replicaLimits: { maxSessions: 1 } })
      const first = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.setCredential("foreign")
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionOverloaded)
      yield* Fiber.interrupt(first.fiber)
    })))

  it.effect("enforces the per subject session limit", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({ ...baseOptions, rpcLimits: { maxSessionsPerSubject: 1 } })
      const first = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.setCredential("same-subject")
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionOverloaded)
      yield* Fiber.interrupt(first.fiber)
    })))

  it.effect("retains touched subjects while evicting the oldest inactive subject", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: {
          openRatePerSecond: 1,
          openBurst: 1,
          maxRetainedRateLimitedSubjects: 3
        },
        authorization: () => Effect.fail(new PeerRpcError.AccessDenied())
      })
      const attempt = (subject: number) =>
        fixture.setCredential(`subject-${subject}`).pipe(
          Effect.andThen(
            fixture.client.Open({
              protocolVersion: PeerRpc.protocolVersion,
              expectedPeerId: serverPeerId,
              documents: [{ documentType: Task.name, documentId: taskId }]
            }).pipe(Stream.runDrain, Effect.flip)
          )
        )
      for (const subject of [1, 2, 3]) {
        assert.instanceOf(yield* attempt(subject), PeerRpcError.AccessDenied)
      }
      assert.instanceOf(yield* attempt(2), PeerRpcError.RequestCapacityExceeded)
      assert.instanceOf(yield* attempt(4), PeerRpcError.AccessDenied)
      assert.instanceOf(yield* attempt(1), PeerRpcError.AccessDenied)
      assert.instanceOf(yield* attempt(2), PeerRpcError.RequestCapacityExceeded)
      assert.instanceOf(yield* attempt(3), PeerRpcError.AccessDenied)
    })))

  it.effect("retains an active subject when the subject state bound is full", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { maxRetainedRateLimitedSubjects: 1 }
      })
      const active = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.setCredential("foreign")
      const full = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(full, PeerRpcError.RequestCapacityExceeded)
      yield* Fiber.interrupt(active.fiber)
      const replacement = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* Fiber.interrupt(replacement.fiber)
    })))

  for (
    const [description, elapsed, expected] of [
      ["retains an exhausted inactive subject just before idle expiry", 999, "RequestCapacityExceeded"],
      ["refreshes an exhausted inactive subject exactly at idle expiry", 1_000, "AccessDenied"]
    ] as const
  ) {
    it.effect(description, () =>
      Effect.scoped(Effect.gen(function*() {
        const fixture = yield* makeFixture({
          ...baseOptions,
          rpcLimits: {
            openRatePerSecond: Number.MIN_VALUE,
            openBurst: 1,
            rateLimitIdleRetention: 1_000,
            maxRetainedRateLimitedSubjects: 8
          },
          authorization: () => Effect.fail(new PeerRpcError.AccessDenied())
        })
        const attempt = () =>
          fixture.client.Open({
            protocolVersion: PeerRpc.protocolVersion,
            expectedPeerId: serverPeerId,
            documents: [{ documentType: Task.name, documentId: taskId }]
          }).pipe(Stream.runDrain, Effect.flip)
        assert.instanceOf(yield* attempt(), PeerRpcError.AccessDenied)
        yield* TestClock.adjust(elapsed)
        assert.strictEqual((yield* attempt())._tag, expected)
      })))
  }

  it.effect("serves outbound byte waiters in FIFO order without bypassing a large head", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) },
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: new Uint8Array(48_000),
          messageHash: "fifo-blocker",
          heads: []
        }
      })
      const opened = yield* Queue.unbounded<string>()
      const blockerReady = yield* Deferred.make<void>()
      const blocker = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        assert.strictEqual((yield* pull)[0]._tag, "Opened")
        assert.strictEqual((yield* pull)[0]._tag, "Message")
        yield* Deferred.succeed(blockerReady, undefined)
        return yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(blockerReady)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerA)
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: noteId,
        message: new Uint8Array(replicaLimits.maxSyncMessageBytes),
        messageHash: "fifo-head",
        heads: []
      })
      yield* fixture.setCredential("foreign")
      const head = yield* Stream.runForEach(
        fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Note.name, documentId: noteId }]
        }),
        (event) => event._tag === "Message" ? Queue.offer(opened, "head").pipe(Effect.asVoid) : Effect.void
      ).pipe(
        Effect.forkChild
      )
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerB)
      for (let index = 0; index < 5; index++) yield* Effect.yieldNow
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: new Uint8Array(48_000),
        messageHash: "fifo-tail",
        heads: []
      })
      yield* fixture.setCredential("subject-10")
      const tail = yield* Stream.runForEach(
        fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }),
        (event) => event._tag === "Message" ? Queue.offer(opened, "tail").pipe(Effect.asVoid) : Effect.void
      ).pipe(
        Effect.forkChild
      )
      yield* Queue.take(fixture.pendingStarted)
      for (let index = 0; index < 5; index++) yield* Effect.yieldNow
      assert.strictEqual((yield* Queue.poll(opened))._tag, "None")
      yield* Fiber.interrupt(blocker)
      assert.strictEqual(yield* Queue.take(opened), "head")
      assert.strictEqual(yield* Queue.take(opened), "tail")
      yield* Fiber.interrupt(head)
      yield* Fiber.interrupt(tail)
    })))

  it.effect("advances outbound byte waiters and restores capacity when the head is canceled", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) }
      })
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: new Uint8Array(replicaLimits.maxSyncMessageBytes),
        messageHash: "cancel-blocker",
        heads: []
      })
      const blockerReady = yield* Deferred.make<void>()
      const blocker = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        yield* pull
        yield* pull
        yield* Deferred.succeed(blockerReady, undefined)
        return yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(blockerReady)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerA)
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: noteId,
        message: new Uint8Array(replicaLimits.maxSyncMessageBytes),
        messageHash: "cancel-head",
        heads: []
      })
      const head = yield* fixture.directOpenAs(
        "foreign",
        [{ documentType: Note.name, documentId: noteId }]
      ).pipe(Stream.runDrain, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerB)
      for (let index = 0; index < 5; index++) yield* Effect.yieldNow
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: Uint8Array.of(1),
        messageHash: "cancel-tail",
        heads: []
      })
      const tailMessage = yield* Deferred.make<void>()
      const tail = yield* fixture.directOpenAs(
        "third",
        [{ documentType: Task.name, documentId: taskId }]
      ).pipe(
        Stream.runForEach((event) =>
          event._tag === "Message" ? Deferred.succeed(tailMessage, undefined).pipe(Effect.asVoid) : Effect.void
        ),
        Effect.forkChild
      )
      yield* Queue.take(fixture.pendingStarted)
      for (let index = 0; index < 5; index++) yield* Effect.yieldNow
      yield* Fiber.interrupt(head)
      yield* Deferred.await(tailMessage)
      yield* Fiber.interrupt(tail)
      yield* Fiber.interrupt(blocker)
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: new Uint8Array(replicaLimits.maxSyncMessageBytes),
        messageHash: "cancel-probe",
        heads: []
      })
      const probeMessage = yield* Deferred.make<void>()
      const probe = yield* fixture.directOpenAs(
        "third",
        [{ documentType: Task.name, documentId: taskId }]
      ).pipe(
        Stream.runForEach((event) =>
          event._tag === "Message" ? Deferred.succeed(probeMessage, undefined).pipe(Effect.asVoid) : Effect.void
        ),
        Effect.forkChild
      )
      yield* Deferred.await(probeMessage)
      yield* Fiber.interrupt(probe)
    })))

  it.effect("bounds outbound byte waiters by the active session limit", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        replicaLimits: { maxSessions: 8 },
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) }
      })
      const large = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: large,
        messageHash: "bounded-blocker",
        heads: []
      })
      const blockerReady = yield* Deferred.make<void>()
      const blocker = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        yield* pull
        yield* pull
        yield* Deferred.succeed(blockerReady, undefined)
        return yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(blockerReady)
      yield* Queue.take(fixture.pendingStarted)
      const waiters = []
      for (let index = 0; index < 7; index++) {
        yield* fixture.setPendingOutbound({
          sendSequence: 0,
          documentId: taskId,
          message: large,
          messageHash: `bounded-waiter-${index}`,
          heads: []
        })
        yield* fixture.setCredential(`subject-${index + 10}`)
        waiters.push(
          yield* fixture.client.Open({
            protocolVersion: PeerRpc.protocolVersion,
            expectedPeerId: serverPeerId,
            documents: [{ documentType: Task.name, documentId: taskId }]
          }).pipe(Stream.runDrain, Effect.forkChild)
        )
        yield* Queue.take(fixture.pendingStarted)
        for (let turn = 0; turn < 5; turn++) yield* Effect.yieldNow
      }
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: large,
        messageHash: "bounded-rejected",
        heads: []
      })
      yield* fixture.setCredential("subject-20")
      const rejected = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(rejected, PeerRpcError.SessionOverloaded)
      for (const waiter of waiters) {
        assert.isUndefined(waiter.pollUnsafe())
        yield* Fiber.interrupt(waiter)
      }
      yield* Fiber.interrupt(blocker)
    })))

  it.effect("bounds global Open setup while authorization is blocked", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockAuthorization: true,
        rpcLimits: { maxInFlightOpen: 1, maxInFlightOpenPerSubject: 2 }
      })
      const first = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(fixture.authorizationStarted), "subject-a")
      yield* fixture.setCredential("foreign")
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.RequestCapacityExceeded)
      yield* Deferred.succeed(fixture.authorizationRelease, undefined)
      yield* Fiber.interrupt(first)
    })))

  it.effect("bounds per subject Open setup while authorization is blocked", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockAuthorization: true,
        rpcLimits: { maxInFlightOpen: 2, maxInFlightOpenPerSubject: 1 }
      })
      const first = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(fixture.authorizationStarted), "subject-a")
      yield* fixture.setCredential("same-subject")
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.RequestCapacityExceeded)
      yield* Deferred.succeed(fixture.authorizationRelease, undefined)
      yield* Fiber.interrupt(first)
    })))

  it.effect("enforces the selected document limit before allocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({ ...baseOptions, replicaLimits: { maxStreamsPerSession: 1 } })
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [
          { documentType: Task.name, documentId: taskId },
          { documentType: Note.name, documentId: noteId }
        ]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.RequestLimitExceeded)
      assert.strictEqual(yield* Ref.get(fixture.authorizationCalls), 0)
    })))

  it.effect("validates Open identity protocol and selection before authorization", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const cases = [
        {
          request: {
            protocolVersion: 2,
            expectedPeerId: serverPeerId,
            documents: [{ documentType: Task.name, documentId: taskId }]
          },
          tag: "UnsupportedVersion"
        },
        {
          request: {
            protocolVersion: PeerRpc.protocolVersion,
            expectedPeerId: remotePeerA,
            documents: [{ documentType: Task.name, documentId: taskId }]
          },
          tag: "PeerMismatch"
        },
        {
          request: { protocolVersion: PeerRpc.protocolVersion, expectedPeerId: serverPeerId, documents: [] },
          tag: "InvalidRequest"
        },
        {
          request: {
            protocolVersion: PeerRpc.protocolVersion,
            expectedPeerId: serverPeerId,
            documents: [
              { documentType: Task.name, documentId: taskId },
              { documentType: Task.name, documentId: taskId }
            ]
          },
          tag: "InvalidRequest"
        }
      ]
      for (const testCase of cases) {
        const error = yield* fixture.client.Open(testCase.request).pipe(Stream.runDrain, Effect.flip)
        assert.strictEqual(error._tag, testCase.tag)
      }
      yield* fixture.setCredential("other-tenant")
      const tenantError = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(tenantError, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(fixture.authorizationCalls), 0)
    })))

  it.effect("mediates every direct authorization selection before session allocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const cases = [
        [],
        [{ document: Task, documentId: taskId }, { document: Note, documentId: noteId }],
        [{ document: Task, documentId: taskId }, { document: Task, documentId: taskId }],
        [{ document: Note, documentId: taskId }],
        [{ document: Task, documentId: noteId }]
      ]
      for (const documents of cases) {
        const fixture = yield* makeFixture({
          ...baseOptions,
          authorization: () =>
            Effect.succeed({
              documents,
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.void
            })
        })
        const error = yield* fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }).pipe(Stream.runDrain, Effect.flip)
        assert.instanceOf(error, PeerRpcError.AccessDenied)
        assert.strictEqual((yield* Queue.poll(fixture.pendingStarted))._tag, "None")
      }
    })))

  it.effect("maps direct authorization defects and unexpected failures without exposing their contents", () =>
    Effect.scoped(Effect.gen(function*() {
      const sentinel = "authorization-private-sentinel-836592"
      for (
        const authorization of [
          () => Effect.die(new Error(sentinel)),
          () => Effect.fail(new Error(sentinel) as never)
        ]
      ) {
        const fixture = yield* makeFixture({ ...baseOptions, authorization })
        const error = yield* fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }).pipe(Stream.runDrain, Effect.flip)
        assert.instanceOf(error, PeerRpcError.ServerUnavailable)
        assert.isFalse(JSON.stringify(error).includes(sentinel))
      }
    })))

  it.effect("preserves declared direct authorization failures", () =>
    Effect.scoped(Effect.gen(function*() {
      for (const expected of [new PeerRpcError.AccessDenied(), new PeerRpcError.ServerUnavailable()]) {
        const fixture = yield* makeFixture({
          ...baseOptions,
          authorization: () => Effect.fail(expected)
        })
        const actual = yield* fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }).pipe(Stream.runDrain, Effect.flip)
        assert.strictEqual(actual._tag, expected._tag)
      }
    })))

  it.effect("maps mixed authorization interruption and defects without exposing their contents", () =>
    Effect.scoped(Effect.gen(function*() {
      const sentinel = "mixed-authorization-private-sentinel-361794"
      const fixture = yield* makeFixture({
        ...baseOptions,
        authorization: () =>
          Effect.failCause(Cause.fromReasons([
            Cause.makeInterruptReason(1),
            Cause.makeDieReason(new Error(sentinel))
          ]))
      })
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.ServerUnavailable)
      assert.isFalse(JSON.stringify(error).includes(sentinel))
      assert.strictEqual((yield* Queue.poll(fixture.pendingStarted))._tag, "None")
    })))

  it.effect("preserves direct authorization interruption", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authorization: () => Effect.interrupt
      })
      const exit = yield* fixture.directOpen([{ documentType: Task.name, documentId: taskId }]).pipe(
        Stream.runDrain,
        Effect.exit
      )
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterruptsOnly(exit.cause))
    })))

  it.effect("rejects an expired direct authorization before session allocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authorization: () =>
          Effect.succeed({
            documents: [{ document: Task, documentId: taskId }],
            validUntil: 0,
            invalidated: Effect.void
          })
      })
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.AccessDenied)
      assert.strictEqual((yield* Queue.poll(fixture.pendingStarted))._tag, "None")
    })))

  for (const validUntil of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    it.effect(`rejects nonfinite direct authorization lease ${String(validUntil)} before session allocation`, () =>
      Effect.scoped(Effect.gen(function*() {
        const fixture = yield* makeFixture({
          ...baseOptions,
          authorization: () =>
            Effect.succeed({
              documents: [{ document: Task, documentId: taskId }],
              validUntil,
              invalidated: Effect.void
            })
        })
        const error = yield* fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }).pipe(Stream.runDrain, Effect.flip)
        assert.instanceOf(error, PeerRpcError.AccessDenied)
        assert.strictEqual((yield* Queue.poll(fixture.pendingStarted))._tag, "None")
      })))
  }

  for (const validUntil of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    it.effect(`rejects nonfinite direct authentication lease ${String(validUntil)} before session allocation`, () =>
      Effect.scoped(Effect.gen(function*() {
        const fixture = yield* makeFixture({ ...baseOptions, authenticationValidUntil: validUntil })
        const error = yield* fixture.directOpen([{ documentType: Task.name, documentId: taskId }]).pipe(
          Stream.runDrain,
          Effect.flip
        )
        assert.instanceOf(error, PeerRpcError.AuthenticationFailure)
        assert.strictEqual((yield* Queue.poll(fixture.pendingStarted))._tag, "None")
        assert.strictEqual(yield* Ref.get(fixture.authorizationCalls), 0)
      })))
  }

  it.effect("closes the session instead of dropping an inbound message on overflow", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({ ...baseOptions, blockInbound: true })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(0) })
      yield* Deferred.await(fixture.inboundBlocked)
      const error = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encode(1)
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionOverloaded)
      const streamExit = yield* Fiber.await(session.fiber)
      assert.isTrue(Exit.isFailure(streamExit))
      if (Exit.isFailure(streamExit)) {
        const streamError = Cause.findErrorOption(streamExit.cause)
        assert.strictEqual(streamError._tag, "Some")
        if (streamError._tag === "Some") assert.strictEqual(streamError.value._tag, "SessionOverloaded")
      }
      yield* Deferred.succeed(fixture.inboundRelease, undefined)
      const unavailable = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encode(3)
      }).pipe(Effect.flip)
      assert.instanceOf(unavailable, PeerRpcError.SessionUnavailable)
    })))

  it.effect("does not expose sessions across the ownership matrix", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const missing = yield* fixture.client.Push({ sessionId: missingSessionId, payload: Uint8Array.of(1) }).pipe(
        Effect.flip
      )
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      assert.instanceOf(missing, PeerRpcError.SessionUnavailable)
      for (const credential of ["same-peer", "same-subject", "other-tenant"] as const) {
        yield* fixture.setCredential(credential)
        const foreign = yield* fixture.client.Push({
          sessionId: session.opened.sessionId,
          payload: Uint8Array.of(1)
        }).pipe(Effect.flip)
        assert.instanceOf(foreign, PeerRpcError.SessionUnavailable)
        assert.strictEqual(missing._tag, foreign._tag)
      }
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("rejects Push after the Open stream is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Fiber.interrupt(session.fiber)
      const error = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: Uint8Array.of(1)
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("stops admission before draining sessions on Layer close", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.closeServer
      const streamExit = yield* Fiber.await(session.fiber)
      assert.isTrue(Exit.isFailure(streamExit))
      if (Exit.isFailure(streamExit)) {
        const streamError = Cause.findErrorOption(streamExit.cause)
        assert.strictEqual(streamError._tag, "Some")
        if (streamError._tag === "Some") assert.strictEqual(streamError.value._tag, "ServerUnavailable")
      }
      const error = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: Uint8Array.of(1)
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.ServerUnavailable)
    })))

  it.effect("interrupts a captured Open request while the server scope is closing", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: Uint8Array.of(1),
          messageHash: "held",
          heads: []
        }
      })
      const messageHeld = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const request = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        assert.strictEqual((yield* pull)[0]._tag, "Opened")
        assert.strictEqual((yield* pull)[0]._tag, "Message")
        yield* Deferred.succeed(messageHeld, undefined)
        return yield* Effect.never
      })).pipe(
        Effect.ensuring(Deferred.succeed(interrupted, undefined)),
        Effect.forkChild
      )
      yield* Deferred.await(messageHeld)
      yield* fixture.closeServer
      yield* Deferred.await(interrupted)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(request)))
    })))

  it.effect("preserves overload when detachment follows an outbound take", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockInbound: true,
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: Uint8Array.of(1),
          messageHash: "taken-before-overload",
          heads: []
        }
      })
      const opened = yield* Deferred.make<PeerRpc.Opened>()
      const messageTaken = yield* Deferred.make<void>()
      const request = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        const first = yield* pull
        assert.strictEqual(first[0]._tag, "Opened")
        yield* Deferred.succeed(opened, first[0] as PeerRpc.Opened)
        assert.strictEqual((yield* pull)[0]._tag, "Message")
        yield* Deferred.succeed(messageTaken, undefined)
        yield* pull
      })).pipe(Effect.forkChild)
      const session = yield* Deferred.await(opened)
      yield* Deferred.await(messageTaken)
      yield* fixture.client.Push({
        sessionId: session.sessionId,
        payload: yield* fixture.encode(0)
      })
      yield* Deferred.await(fixture.inboundBlocked)
      const overload = yield* fixture.client.Push({
        sessionId: session.sessionId,
        payload: yield* fixture.encode(1)
      }).pipe(Effect.flip)
      assert.strictEqual(overload._tag, "SessionOverloaded")
      yield* Deferred.succeed(fixture.inboundRelease, undefined)
      const streamExit = yield* Fiber.await(request)
      assert.isTrue(Exit.isFailure(streamExit))
      if (Exit.isFailure(streamExit)) {
        const streamError = Cause.findErrorOption(streamExit.cause)
        assert.strictEqual(streamError._tag, "Some")
        if (streamError._tag === "Some") assert.instanceOf(streamError.value, PeerRpcError.SessionOverloaded)
      }
    })))

  it.effect("revokes the session when an authorization lease is invalidated", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Deferred.succeed(fixture.authorizationInvalidated, undefined)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(session.fiber)))
      const error = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: Uint8Array.of(1)
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("revokes the active session when authentication is invalidated", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Deferred.succeed(fixture.authenticationInvalidated, undefined)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(session.fiber)))
      const error = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: Uint8Array.of(1)
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  for (const lease of ["authentication", "authorization"] as const) {
    it.effect(`revokes the active session when ${lease} expires`, () =>
      Effect.scoped(Effect.gen(function*() {
        const fixture = yield* makeFixture({
          ...baseOptions,
          authenticationValidUntil: lease === "authentication" ? 1_000 : Number.MAX_SAFE_INTEGER,
          authorizationValidUntil: lease === "authorization" ? 1_000 : Number.MAX_SAFE_INTEGER
        })
        const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
        yield* TestClock.adjust(1_000)
        assert.isTrue(Exit.isFailure(yield* Fiber.await(session.fiber)))
      })))
  }

  it.effect("rejects Push at its current authentication deadline", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authenticationValidUntil: 10_000,
        manualClock: true
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.setCurrentTime(1)

      const error = yield* fixture.directPushAs(1, {
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encode(0)
      }).pipe(Effect.flip)

      assert.instanceOf(error, PeerRpcError.AuthenticationFailure)
      assert.strictEqual((yield* Queue.poll(fixture.received))._tag, "None")
    })))

  it.effect("rejects every Push racing the exact earliest lease boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authenticationValidUntil: 2_000,
        authorizationValidUntil: 1_000,
        manualClock: true
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.setCurrentTime(999)
      yield* fixture.directPush({
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encode(0)
      })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      yield* fixture.setCurrentTime(1_000)
      const exits = yield* Effect.forEach(
        Array.from({ length: 16 }, (_, sequence) => sequence + 1),
        (sequence) =>
          Effect.gen(function*() {
            const payload = yield* fixture.encode(sequence)
            return yield* fixture.directPush({
              sessionId: session.opened.sessionId,
              payload
            })
          }).pipe(Effect.exit),
        { concurrency: "unbounded" }
      )
      assert.isTrue(exits.every(Exit.isFailure))
      assert.isTrue(exits.every((exit) => {
        if (Exit.isSuccess(exit)) return false
        const error = Cause.findErrorOption(exit.cause)
        return error._tag === "Some" && error.value instanceof PeerRpcError.SessionUnavailable
      }))
      assert.isTrue(Exit.isFailure(yield* Fiber.await(session.fiber)))
    })))

  it.effect("rejects a queued outbound Message at the exact earliest lease boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authenticationValidUntil: 2_000,
        authorizationValidUntil: 1_000,
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: Uint8Array.of(1, 2, 3),
          messageHash: "lease-boundary",
          heads: []
        },
        manualClock: true
      })
      const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
      assert.strictEqual((yield* pull)[0]._tag, "Opened")
      assert.strictEqual(yield* Queue.take(fixture.sent), 0)
      yield* fixture.setCurrentTime(1_000)
      const error = yield* pull.pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("rejects the first Opened event at the exact earliest lease boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authenticationValidUntil: 2_000,
        authorizationValidUntil: 1_000,
        manualClock: true
      })
      yield* fixture.setCurrentTimeOnNextRandomBytes(1_000)
      const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
      const error = yield* pull.pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("rejects commit flush generation at the exact earliest lease boundary", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        authenticationValidUntil: 2_000,
        authorizationValidUntil: 1_000,
        manualClock: true
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      assert.strictEqual(yield* Queue.take(fixture.generated), taskId)
      yield* fixture.setNextOutbound({
        sendSequence: 0,
        documentId: taskId,
        message: Uint8Array.of(1, 2, 3),
        messageHash: "lease-boundary",
        heads: []
      })
      yield* fixture.setCurrentTime(1_000)
      yield* Queue.offer(fixture.commits, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId: taskId,
        keys: [],
        refreshGeneration: 0
      })
      yield* Queue.take(fixture.commitProcessed)
      const error = yield* Fiber.join(session.fiber).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
      assert.strictEqual((yield* Queue.poll(fixture.generated))._tag, "None")
    })))

  it.effect("revokes the active session at the maximum reauthorization interval", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { maximumReauthorizationInterval: 1_000 }
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* TestClock.adjust(1_000)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(session.fiber)))
    })))

  it.effect("publishes no session when a lease is invalidated during setup", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      yield* Deferred.succeed(fixture.authenticationInvalidated, undefined)
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("releases partial acquisition after session startup failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        failSessionOpen: true,
        replicaLimits: { maxSessions: 1 }
      })
      const error = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)
      assert.instanceOf(error, PeerRpcError.ServerUnavailable)
      yield* fixture.allowSessionOpen
      yield* fixture.setCredential("foreign")
      const session = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("maps initialization send capacity timeout to SessionOverloaded", () =>
    Effect.scoped(Effect.gen(function*() {
      const message = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockInbound: true,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) }
      })
      const blocker = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Queue.take(fixture.generated)
      yield* Queue.take(fixture.pendingStarted)
      yield* fixture.client.Push({
        sessionId: blocker.opened.sessionId,
        payload: yield* fixture.encodeMessage(0, taskId, Task.name, message)
      })
      yield* Deferred.await(fixture.inboundBlocked)
      yield* fixture.setPendingOutbound({
        sendSequence: 0,
        documentId: noteId,
        message,
        messageHash: "initial-capacity",
        heads: []
      })
      yield* fixture.setCredential("foreign")
      const opening = yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerB)
      yield* TestClock.adjust(replicaLimits.maxPeerSendMillis + 1)
      const error = yield* Fiber.join(opening).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SessionOverloaded")
      yield* Deferred.succeed(fixture.inboundRelease, undefined)
      yield* Fiber.interrupt(blocker.fiber)
    })))

  it.effect("maps a post ready Push reply capacity timeout to SessionOverloaded", () =>
    Effect.scoped(Effect.gen(function*() {
      const message = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) },
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message,
          messageHash: "held-for-push-reply",
          heads: []
        }
      })
      const messageHeld = yield* Deferred.make<void>()
      const blocker = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        assert.strictEqual((yield* pull)[0]._tag, "Opened")
        assert.strictEqual((yield* pull)[0]._tag, "Message")
        yield* Deferred.succeed(messageHeld, undefined)
        return yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(messageHeld)
      yield* Queue.take(fixture.generated)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerA)
      yield* fixture.setCredential("foreign")
      const session = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* Queue.take(fixture.generated)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerB)
      yield* fixture.setReply({
        documentId: noteId,
        message,
        messageHash: "push-reply-capacity",
        heads: []
      })
      yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encodeMessage(0, noteId, Note.name, Uint8Array.of(1))
      })
      assert.strictEqual(yield* Queue.take(fixture.enqueued), noteId)
      assert.strictEqual(yield* Queue.take(fixture.pendingStarted), remotePeerB)
      yield* TestClock.adjust(replicaLimits.maxPeerSendMillis + 1)
      const error = yield* Fiber.join(session.fiber).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SessionOverloaded")
      yield* Fiber.interrupt(blocker)
    })))

  it.effect("does not retain disconnected watchers after repeated startup failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({ ...baseOptions, failSessionOpen: true })
      const baseline = yield* fixture.activeFiberCount
      assert.isAbove(baseline, 0)
      for (let index = 0; index < 20; index++) {
        const error = yield* fixture.client.Open({
          protocolVersion: PeerRpc.protocolVersion,
          expectedPeerId: serverPeerId,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }).pipe(Stream.runDrain, Effect.flip)
        assert.instanceOf(error, PeerRpcError.ServerUnavailable)
      }
      for (let index = 0; index < 10; index++) yield* Effect.yieldNow
      assert.strictEqual(yield* fixture.activeFiberCount, baseline)
    })))

  it.effect("interrupts an overloaded Open without draining an unacknowledged message", () =>
    Effect.scoped(Effect.gen(function*() {
      const message = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: {
          maxOutboundBufferedBytesPerSession: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes),
          maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes)
        },
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message,
          messageHash: "first",
          heads: []
        }
      })
      const opened = yield* Deferred.make<PeerRpc.Opened>()
      const messageHeld = yield* Deferred.make<void>()
      const stream = yield* Effect.scoped(Effect.gen(function*() {
        const pull = yield* Stream.toPull(fixture.directOpen([{ documentType: Task.name, documentId: taskId }]))
        const first = yield* pull
        assert.strictEqual(first[0]._tag, "Opened")
        yield* Deferred.succeed(opened, first[0] as PeerRpc.Opened)
        const second = yield* pull
        assert.strictEqual(second[0]._tag, "Message")
        yield* Deferred.succeed(messageHeld, undefined)
        return yield* Effect.never
      })).pipe(Effect.forkChild)
      const session = yield* Deferred.await(opened)
      yield* Deferred.await(messageHeld)
      yield* Queue.take(fixture.generated)
      assert.strictEqual(yield* Queue.take(fixture.sent), 0)
      yield* fixture.setNextOutbound({
        sendSequence: 1,
        documentId: taskId,
        message,
        messageHash: "second",
        heads: []
      })
      yield* Queue.offer(fixture.commits, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId: taskId,
        keys: [],
        refreshGeneration: 0
      })
      yield* Queue.take(fixture.generated)
      for (let index = 0; index < 5; index++) {
        yield* Effect.yieldNow
        yield* TestClock.adjust(replicaLimits.maxPeerSendMillis + 1)
      }
      yield* Effect.yieldNow
      assert.isTrue(Exit.isFailure(yield* Fiber.await(stream)))
      const error = yield* fixture.client.Push({ sessionId: session.sessionId, payload: Uint8Array.of(1) }).pipe(
        Effect.flip
      )
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
    })))

  it.effect("maps commit flush send capacity timeout to SessionOverloaded", () =>
    Effect.scoped(Effect.gen(function*() {
      const message = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockInbound: true,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) }
      })
      const blocker = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Queue.take(fixture.pendingStarted)
      yield* Queue.take(fixture.generated)
      yield* fixture.client.Push({
        sessionId: blocker.opened.sessionId,
        payload: yield* fixture.encodeMessage(0, taskId, Task.name, message)
      })
      yield* Deferred.await(fixture.inboundBlocked)
      yield* fixture.setCredential("foreign")
      const session = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* Queue.take(fixture.pendingStarted)
      yield* Queue.take(fixture.generated)
      yield* fixture.setNextOutbound({
        sendSequence: 0,
        documentId: noteId,
        message,
        messageHash: "flush-capacity",
        heads: []
      })
      yield* Queue.offer(fixture.commits, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId: noteId,
        keys: [],
        refreshGeneration: 0
      })
      assert.strictEqual(yield* Queue.take(fixture.generated), noteId)
      yield* TestClock.adjust(replicaLimits.maxPeerSendMillis + 1)
      const error = yield* Fiber.join(session.fiber).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SessionOverloaded")
      yield* Deferred.succeed(fixture.inboundRelease, undefined)
      yield* Fiber.interrupt(blocker.fiber)
    })))

  it.effect("returns inbound byte reservations after overload cleanup", () =>
    Effect.scoped(Effect.gen(function*() {
      const message = new Uint8Array(replicaLimits.maxSyncMessageBytes)
      const fixture = yield* makeFixture({
        ...baseOptions,
        blockInbound: true,
        rpcLimits: { maxBufferedBytes: PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) }
      })
      const first = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      const payload = yield* fixture.encodeMessage(0, taskId, Task.name, message)
      yield* fixture.client.Push({ sessionId: first.opened.sessionId, payload })
      yield* Deferred.await(fixture.inboundBlocked)
      const overload = yield* fixture.client.Push({
        sessionId: first.opened.sessionId,
        payload: yield* fixture.encode(1)
      }).pipe(Effect.flip)
      assert.strictEqual(overload._tag, "SessionOverloaded")
      yield* Deferred.succeed(fixture.inboundRelease, undefined)
      yield* fixture.setCredential("foreign")
      const second = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* fixture.client.Push({
        sessionId: second.opened.sessionId,
        payload: yield* fixture.encodeMessage(0, noteId, Note.name, message)
      })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      yield* Fiber.interrupt(second.fiber)
    })))

  it.effect("survives repeated replacement followed by server shutdown", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        replicaLimits: { maxSessions: 64 },
        rpcLimits: { maxSessionsPerSubject: 64 }
      })
      let current = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      for (let index = 0; index < 20; index++) {
        const replacement = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
        const exit = yield* Fiber.await(current.fiber)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) {
          const error = Cause.findErrorOption(exit.cause)
          assert.strictEqual(error._tag, "Some")
          if (error._tag === "Some") assert.strictEqual(error.value._tag, "SessionUnavailable")
        }
        current = replacement
      }
      yield* fixture.closeServer
      const exit = yield* Fiber.await(current.fiber)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const error = Cause.findErrorOption(exit.cause)
        assert.strictEqual(error._tag, "Some")
        if (error._tag === "Some") assert.strictEqual(error.value._tag, "ServerUnavailable")
      }
    })))

  it.effect("coalesces 256 commits while 64 sessions are active", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        replicaLimits: { maxSessions: 64 },
        rpcLimits: { maxSessionsPerSubject: 64 }
      })
      const documentIds = Array.from({ length: 64 }, (_, index) =>
        Identity.DocumentId.make(
          `doc_00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`
        ))
      const sessions = yield* Effect.forEach(documentIds, (documentId, index) =>
        fixture.setCredential(`bulk-${index}`).pipe(
          Effect.andThen(fixture.open([{ documentType: Task.name, documentId }])),
          Effect.tap(() =>
            Queue.take(fixture.generated)
          )
        ))
      yield* fixture.blockCommitGeneration
      const commits = Array.from({ length: 256 }, (_, index) => ({
        _tag: "Commit" as const,
        commitSequence: Identity.CommitSequence.make(index + 1),
        documentId: documentIds[0],
        keys: [],
        refreshGeneration: 0
      }))
      yield* Queue.offer(fixture.commits, commits[0])
      yield* Queue.take(fixture.commitProcessed)
      yield* Deferred.await(fixture.commitFlushStarted)
      yield* Queue.offerAll(fixture.commits, commits.slice(1))
      for (let index = 1; index < commits.length; index++) yield* Queue.take(fixture.commitProcessed)
      yield* Deferred.succeed(fixture.commitFlushRelease, undefined)
      assert.strictEqual(yield* Queue.take(fixture.generated), documentIds[0])
      assert.strictEqual(yield* Queue.take(fixture.generated), documentIds[0])
      for (let index = 0; index < 10; index++) yield* Effect.yieldNow
      assert.strictEqual((yield* Queue.poll(fixture.generated))._tag, "None")
      yield* Effect.forEach(sessions, (session) => Fiber.interrupt(session.fiber), {
        concurrency: "unbounded",
        discard: true
      })
    })))

  it.effect("routes disjoint commits only to interested sessions", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const task = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* Queue.take(fixture.generated)
      yield* fixture.setCredential("foreign")
      const note = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      yield* Queue.take(fixture.generated)
      yield* Queue.offer(fixture.commits, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId: taskId,
        keys: [],
        refreshGeneration: 0
      })
      assert.strictEqual(yield* Queue.take(fixture.generated), taskId)
      yield* Fiber.interrupt(task.fiber)
      yield* Fiber.interrupt(note.fiber)
    })))

  it.effect("records fixed cardinality metrics and safe finite spans at live boundaries", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    return Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        initialOutbound: {
          sendSequence: 0,
          documentId: taskId,
          message: Uint8Array.of(17, 18, 19),
          messageHash: "hash",
          heads: []
        },
        authorization: (request) =>
          Effect.logDebug("authorization-log-forbidden-value").pipe(
            Effect.andThen(
              request.documents.some((document) => document.documentType === Note.name)
                ? Effect.fail(new PeerRpcError.AccessDenied())
                : Effect.succeed({
                  documents: request.documents.map((document) => ({ document: Task, documentId: document.documentId })),
                  validUntil: Number.MAX_SAFE_INTEGER,
                  invalidated: Effect.never
                })
            )
          )
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      assert.strictEqual((yield* Queue.take(session.events))._tag, "Message")
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(0) })
      yield* Queue.take(fixture.received)
      yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Note.name, documentId: noteId }]
      }).pipe(Stream.runDrain, Effect.flip)
      yield* fixture.client.Open({
        protocolVersion: PeerRpc.protocolVersion + 1,
        expectedPeerId: serverPeerId,
        documents: [{ documentType: Task.name, documentId: taskId }]
      }).pipe(Stream.runDrain, Effect.flip)

      assert.strictEqual((yield* Metric.value(PeerRpcObservability.boundary("Open", "Attempt"))).count, 3)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.boundary("Open", "Success"))).count, 1)
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Open", "AuthorizationDenied"))).count,
        1
      )
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Open", "ProtocolRejected"))).count,
        1
      )
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.boundary("Push", "Attempt"))).count, 1)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.boundary("Push", "Success"))).count, 1)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.activeSessions())).value, 1)
      assert.deepInclude(yield* Metric.value(PeerRpcObservability.selectedDocuments()), { count: 3, sum: 3 })
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.bytes("Inbound"))).count, 1)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.bytes("Outbound"))).count, 1)

      yield* fixture.closeServer
      yield* Fiber.await(session.fiber)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.activeSessions())).value, 0)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.queueItems("Inbound"))).value, 0)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.queueItems("Outbound"))).value, 0)
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Server", "ShutdownClosed"))).count,
        1
      )

      const safeSpans = spans.filter((span) => span.name.startsWith("effect_local_rpc."))
      assert.deepStrictEqual(
        new Set(safeSpans.map((span) => span.name)),
        new Set([
          "effect_local_rpc.authentication",
          "effect_local_rpc.server.open",
          "effect_local_rpc.server.push"
        ])
      )
      const allowedAttributes = new Set([
        "rpc.operation",
        "rpc.result",
        "rpc.selected_documents",
        "rpc.payload_bytes"
      ])
      for (const span of safeSpans) {
        for (const [key, value] of span.attributes) {
          assert.isTrue(allowedAttributes.has(key))
          if (typeof value === "number") assert.isTrue(Number.isFinite(value))
        }
        assert.strictEqual(span.status._tag, "Ended")
        if (span.status._tag === "Ended") assert.isTrue(Exit.isSuccess(span.status.exit))
        assert.deepStrictEqual(span.events, [])
      }
      const telemetry = JSON.stringify(safeSpans.map((span) => ({
        name: span.name,
        attributes: [...span.attributes],
        status: span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
          ? Cause.pretty(span.status.exit.cause)
          : span.status._tag
      }))) + (yield* Metric.dump)
      for (
        const forbidden of [
          "owner",
          "authorization-log-forbidden-value",
          "tenant",
          "subject-a",
          taskId,
          noteId,
          remotePeerA,
          session.opened.sessionId
        ]
      ) assert.notInclude(telemetry, forbidden)
    })).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })

  it.effect("rejects Push beyond the per subject burst", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture({
        ...baseOptions,
        rpcLimits: { pushBurst: 1, pushRatePerSecond: Number.MIN_VALUE }
      })
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      yield* fixture.client.Push({ sessionId: session.opened.sessionId, payload: yield* fixture.encode(0) })
      assert.strictEqual(yield* Queue.take(fixture.received), 0)
      const rejected = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: yield* fixture.encode(1)
      }).pipe(Effect.flip)
      assert.instanceOf(rejected, PeerRpcError.RequestCapacityExceeded)
      assert.strictEqual((yield* Queue.poll(fixture.received))._tag, "None")
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("rejects a Push payload beyond the sync envelope limit", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const session = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      const oversized = new Uint8Array(
        PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes) + 1
      )
      const rejected = yield* fixture.client.Push({
        sessionId: session.opened.sessionId,
        payload: oversized
      }).pipe(Effect.flip)
      assert.instanceOf(rejected, PeerRpcError.RequestLimitExceeded)
      assert.strictEqual((yield* Queue.poll(fixture.received))._tag, "None")
      yield* Fiber.interrupt(session.fiber)
    })))

  it.effect("marks every session dirty on a full refresh commit", () =>
    Effect.scoped(Effect.gen(function*() {
      const fixture = yield* makeFixture(baseOptions)
      const task = yield* fixture.open([{ documentType: Task.name, documentId: taskId }])
      assert.strictEqual(yield* Queue.take(fixture.generated), taskId)
      yield* fixture.setCredential("foreign")
      const note = yield* fixture.open([{ documentType: Note.name, documentId: noteId }])
      assert.strictEqual(yield* Queue.take(fixture.generated), noteId)
      yield* Queue.offer(fixture.commits, { _tag: "FullRefreshRequired", refreshGeneration: 1 })
      yield* Queue.take(fixture.commitProcessed)
      const refreshed = yield* Effect.all([Queue.take(fixture.generated), Queue.take(fixture.generated)])
      assert.deepStrictEqual(new Set(refreshed), new Set([taskId, noteId]))
      yield* Fiber.interrupt(task.fiber)
      yield* Fiber.interrupt(note.fiber)
    })))
})
