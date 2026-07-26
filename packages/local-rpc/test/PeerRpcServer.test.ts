import { assert, describe, it, vi } from "@effect/vitest"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
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
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import type * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import { createHash, randomBytes } from "node:crypto"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRelayIngress from "../src/PeerRelayIngress.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const Note = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
const taskId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerA = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const remotePeerB = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const crypto = Crypto.make({
  randomBytes: (size) => randomBytes(size),
  digest: (algorithm, bytes) =>
    Effect.sync(() => new Uint8Array(createHash(algorithm.replace("-", "").toLowerCase()).update(bytes).digest()))
})

const relayTransition: PeerRelayStore.TransitionResult = {
  status: "Changed",
  ready: false,
  nextEligibleAt: Option.none(),
  lane: "Retry"
}

const makeRelayStore = (
  overrides: Partial<PeerRelayStore.Service> = {}
): PeerRelayStore.Service => ({
  admit: (input) =>
    Effect.succeed({
      status: "Accepted",
      channel: input.channel,
      ready: false,
      nextEligibleAt: 0,
      lane: "New"
    }),
  claim: () =>
    Effect.succeed({
      message: Option.none(),
      ready: false,
      nextEligibleAt: Option.none(),
      lane: "Retry"
    }),
  loadClaimedPayload: () => Effect.succeed(new Uint8Array()),
  acknowledge: () => Effect.succeed(relayTransition),
  reject: () => Effect.succeed(relayTransition),
  release: () => Effect.succeed(relayTransition),
  recover: ({ cursor }) =>
    Effect.succeed({
      ...(cursor === undefined ? {} : { cursor }),
      processed: 0,
      hasMore: false
    }),
  expire: ({ cursor }) =>
    Effect.succeed({
      ...(cursor === undefined ? {} : { cursor }),
      processed: 0,
      hasMore: false
    }),
  repair: ({ cursor }) =>
    Effect.succeed({
      ...(cursor === undefined ? {} : { cursor }),
      processed: 0,
      hasMore: false
    }),
  reconcile: ({ cursor }) =>
    Effect.succeed({
      ...(cursor === undefined ? {} : { cursor }),
      processed: 0,
      hasMore: false
    }),
  collect: ({ cursor }) =>
    Effect.succeed({
      ...(cursor === undefined ? {} : { cursor }),
      processed: 0,
      hasMore: false
    }),
  usage: () =>
    Effect.succeed({
      activeCount: 0,
      activeBytes: 0,
      retainedCount: 0,
      retainedBytes: 0
    }),
  ...overrides
})

const relayPrincipal: PeerAuthentication.PeerPrincipal = {
  tenantId: "tenant",
  subjectId: "sender",
  peerId: remotePeerA
}

const relayOpenRequest = {
  protocolVersion: PeerRpc.protocolVersion,
  expectedRelayPeerId: serverPeerId,
  expectedLocal: relayPrincipal,
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  remote: {
    subjectId: "recipient",
    peerId: remotePeerB
  },
  documents: [{ documentType: Task.name, documentId: taskId }],
  receiptRetentionMillis: PeerRelayLimits.defaults.maximumReceiptRetentionMillis,
  senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis
} satisfies typeof PeerRpc.OpenRpc.payloadSchema.Type

const relayAuthenticated = {
  principal: relayPrincipal,
  validUntil: Number.MAX_SAFE_INTEGER,
  invalidated: Effect.never
}

const authorizeUnsafeRelayDecode: PeerRelayAuthorization.AuthorizeUnsafeUnboundedAutomerge3Decode = (
  request
) =>
  Effect.succeed({
    _tag: "UnsafeUnboundedAutomerge3DecodeGrant",
    risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
    principal: request.principal,
    remote: {
      tenantId: request.principal.tenantId,
      subjectId: request.remote.subjectId,
      peerId: request.remote.peerId
    },
    direction: request.direction,
    documents: request.documents,
    validUntil: Number.MAX_SAFE_INTEGER,
    invalidated: Effect.never
  })

const authorizeRelay: PeerRelayAuthorization.Authorize = (request) =>
  Effect.succeed({
    remote: {
      tenantId: request.principal.tenantId,
      subjectId: request.remote.subjectId,
      peerId: request.remote.peerId
    },
    documents: request.documents.map((document) => ({
      document: Task,
      documentId: document.documentId
    })),
    validUntil: Number.MAX_SAFE_INTEGER,
    invalidated: Effect.never
  })

const RelaySyncEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

const makeRelayPayload = Effect.gen(function*() {
  const message = Uint8Array.of(1, 2, 3)
  const messageHash = yield* Canonical.digest(message).pipe(
    Effect.provideService(Crypto.Crypto, crypto)
  )
  const payload = new TextEncoder().encode(
    yield* Schema.encodeEffect(RelaySyncEnvelopeJson)({
      connectionEpoch: "epoch",
      sequence: 0,
      documentId: taskId,
      documentType: Task.name,
      messageHash,
      message,
      lineage: Identity.genesisLineage,
      writerProvenance: []
    })
  )
  return { messageHash, payload }
})

const makeRelayClaim = (
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRpc.ClaimToken,
  messageHash: string,
  outerEnvelopeDigest: PeerRpc.RelayDigest,
  payloadBytes: number,
  sessionGeneration: number
) =>
  PeerRelayStore.ClaimedMessage.make({
    rowId: 1,
    channel: {
      tenantId: "tenant",
      senderSubjectId: relayPrincipal.subjectId,
      senderPeerId: relayPrincipal.peerId,
      senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
      recipientSubjectId: relayOpenRequest.remote.subjectId,
      recipientPeerId: relayOpenRequest.remote.peerId
    },
    relayMessageId,
    relayPeerId: serverPeerId,
    senderConnectionEpoch: "epoch",
    senderSequence: 0,
    documentIds: [taskId],
    payloadVersion: 1,
    messageHash,
    outerEnvelopeDigest,
    payloadBytes,
    claimToken,
    claimDeadline: Number.MAX_SAFE_INTEGER,
    sessionGeneration
  })

const makeRelayOuterEnvelopeDigest = (
  relayMessageId: Identity.RelayMessageId,
  messageHash: string,
  payload: Uint8Array
) =>
  PeerSyncEnvelope.digestRelayOuterEnvelope({
    domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
    version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
    expectedLocal: relayPrincipal,
    remote: {
      tenantId: relayPrincipal.tenantId,
      subjectId: relayOpenRequest.remote.subjectId,
      peerId: relayOpenRequest.remote.peerId
    },
    relayPeerId: serverPeerId,
    relayMessageId,
    protocolVersion: PeerRpc.protocolVersion,
    payloadVersion: 1,
    senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
    senderConnectionEpoch: "epoch",
    senderSequence: 0,
    document: {
      documentId: taskId,
      documentType: Task.name
    },
    lineage: Identity.genesisLineage,
    writerProvenance: [],
    messageHash,
    payload
  }).pipe(Effect.provideService(Crypto.Crypto, crypto))

const makeRelayHandlerContext = (
  authorization: Context.Service.Shape<typeof PeerRelayAuthorization.PeerRelayAuthorization>,
  store: PeerRelayStore.Service,
  limits: PeerRelayLimits.Values = PeerRelayLimits.defaults,
  ingress: Context.Service.Shape<typeof PeerRelayIngress.PeerRelayIngress> = {
    address: { _tag: "UnixAddress", path: "relay-test" },
    reserveOutbound: () => Effect.fail(new PeerRpcError.RequestCapacityExceeded()),
    usage: Effect.succeed({
      connections: 0,
      reservedBytes: 0,
      byteReservationWaiters: 0
    }),
    await: Effect.never
  }
) =>
  Layer.build(
    PeerRpcServer.layerHandlers({
      tenantId: "tenant",
      peerId: serverPeerId
    }).pipe(
      Layer.provide(Layer.mergeAll(
        Layer.succeed(Crypto.Crypto, crypto),
        Layer.succeed(PeerRelayLimits.PeerRelayLimits, limits),
        Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization, authorization),
        Layer.succeed(PeerRelayStore.PeerRelayStore, store),
        Layer.succeed(PeerRelayIngress.PeerRelayIngress, ingress)
      ))
    )
  )

const relayHandlerEffect = <A, E, R,>(
  handler: Rpc.Handler<any>,
  effect: Effect.Effect<A, E, R>
) =>
  effect.pipe(
    Effect.provideContext(Context.add(
      handler.context,
      PeerAuthentication.AuthenticatedPeer,
      relayAuthenticated
    ))
  )

const makeTerminalRelayContext = (
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRpc.ClaimToken,
  authorization: Context.Service.Shape<typeof PeerRelayAuthorization.PeerRelayAuthorization>,
  acknowledge: PeerRelayStore.Service["acknowledge"]
) =>
  Effect.gen(function*() {
    const { messageHash, payload } = yield* makeRelayPayload
    const outerEnvelopeDigest = yield* makeRelayOuterEnvelopeDigest(
      relayMessageId,
      messageHash,
      payload
    )
    let claimed = false
    const store = makeRelayStore({
      claim: (request) =>
        Effect.sync(() => {
          if (claimed) {
            return {
              message: Option.none(),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }
          claimed = true
          return {
            message: Option.some(makeRelayClaim(
              relayMessageId,
              claimToken,
              messageHash,
              outerEnvelopeDigest,
              payload.byteLength,
              request.sessionGeneration
            )),
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry" as const
          }
        }),
      loadClaimedPayload: () => Effect.succeed(payload),
      acknowledge
    })
    const ingress = PeerRelayIngress.PeerRelayIngress.of({
      address: { _tag: "UnixAddress", path: "relay-test" },
      reserveOutbound: (bytes) =>
        Effect.succeed({
          bytes,
          release: Effect.void,
          transferToCurrentRequest: Effect.void
        }),
      usage: Effect.succeed({
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      }),
      await: Effect.never
    })
    return yield* makeRelayHandlerContext(
      authorization,
      store,
      PeerRelayLimits.defaults,
      ingress
    )
  })

describe("PeerRpcServer", () => {
  it.effect("denies operation admission after an absolute grant deadline", () =>
    Effect.scoped(Effect.gen(function*() {
      let now = 0
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => now,
        currentTimeMillis: Effect.sync(() => now),
        currentTimeNanosUnsafe: () => BigInt(now) * 1_000_000n,
        currentTimeNanos: Effect.sync(() => BigInt(now) * 1_000_000n),
        sleep: () => Effect.never
      }
      let sendAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Send") sendAuthorizations += 1
              return request.direction === "Send" && sendAuthorizations === 2
                ? {
                  ...result,
                  validUntil: 1_000,
                  invalidated: Effect.sync(() => {
                    now = 1_001
                  }).pipe(Effect.andThen(Effect.never))
                }
                : result
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const admitCalls = yield* Ref.make(0)
      const store = makeRelayStore({
        admit: (input) =>
          Ref.update(admitCalls, (count) => count + 1).pipe(
            Effect.as({
              status: "Accepted",
              channel: input.channel,
              ready: false,
              nextEligibleAt: 0,
              lane: "New"
            })
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(
        PeerRpc.PushRpc.key
      ) as Rpc.Handler<"Push">
      const provide = <A, E, R,>(
        handler: Rpc.Handler<any>,
        effect: Effect.Effect<A, E, R>
      ) =>
        effect.pipe(
          Effect.provideContext(
            Context.add(
              Context.add(
                handler.context,
                PeerAuthentication.AuthenticatedPeer,
                relayAuthenticated
              ),
              Clock.Clock,
              clock
            )
          )
        )
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => provide(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      const { payload } = yield* makeRelayPayload
      const denied = yield* (
        pushHandler.handler({
          sessionId: (opened as PeerRpc.Opened).sessionId,
          relayMessageId: Identity.RelayMessageId.make(
            "rly_00000000-0000-4000-8000-000000000052"
          ),
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => provide(pushHandler, effect),
        Effect.flip
      )
      assert.instanceOf(denied, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(admitCalls), 0)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("rejects Open when authentication expires during relay authorization", () =>
    Effect.scoped(Effect.gen(function*() {
      const authorizationStarted = yield* Deferred.make<void>()
      const authorizationRelease = yield* Deferred.make<void>()
      let authorizationCalls = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          Effect.gen(function*() {
            authorizationCalls++
            if (authorizationCalls === 1) {
              yield* Deferred.succeed(authorizationStarted, undefined)
              yield* Deferred.await(authorizationRelease)
            }
            return yield* authorizeRelay(request)
          }),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const testClock = yield* TestClock.testClockWith(Effect.succeed)
      const context = yield* makeRelayHandlerContext(
        authorization,
        makeRelayStore()
      ).pipe(Effect.provideService(Clock.Clock, testClock))
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const now = yield* Clock.currentTimeMillis
      const fiber = yield* Stream.runHead(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      ).pipe(
        Effect.provideContext(
          Context.add(
            Context.add(
              handler.context,
              PeerAuthentication.AuthenticatedPeer,
              {
                principal: relayPrincipal,
                validUntil: now + 1_000,
                invalidated: Effect.never
              }
            ),
            Clock.Clock,
            testClock
          )
        ),
        Effect.exit,
        Effect.forkScoped
      )
      yield* Deferred.await(authorizationStarted)
      yield* TestClock.adjust(1_001)
      yield* Deferred.succeed(authorizationRelease, undefined)
      const exit = yield* Fiber.join(fiber)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.instanceOf(Cause.squash(exit.cause), PeerRpcError.AuthenticationFailure)
      }
    })))

  it.effect("requires a live unsafe decode grant before relay Push admission", () =>
    Effect.scoped(Effect.gen(function*() {
      let sendMode: "Deny" | "Stale" = "Deny"
      const unsafeRequests = yield* Ref.make<
        Array<PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest>
      >([])
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: authorizeRelay,
        authorizeUnsafeUnboundedAutomerge3Decode: (request) =>
          Ref.update(unsafeRequests, (requests) => [...requests, request]).pipe(
            Effect.andThen(
              request.direction === "Receive"
                ? authorizeUnsafeRelayDecode(request)
                : sendMode === "Deny"
                ? Effect.fail(new PeerRpcError.AccessDenied())
                : authorizeUnsafeRelayDecode(request).pipe(
                  Effect.map((grant) => ({
                    ...grant,
                    invalidated: Effect.void
                  }))
                )
            )
          )
      })
      const admitCalls = yield* Ref.make(0)
      const store = makeRelayStore({
        admit: (input) =>
          Ref.update(admitCalls, (count) => count + 1).pipe(
            Effect.as({
              status: "Accepted" as const,
              channel: input.channel,
              ready: false,
              nextEligibleAt: 0,
              lane: "New" as const
            })
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(
        PeerRpc.PushRpc.key
      ) as Rpc.Handler<"Push">
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      const sessionId = (opened as PeerRpc.Opened).sessionId
      const { payload } = yield* makeRelayPayload
      const push = (relayMessageId: Identity.RelayMessageId) =>
        pushHandler.handler({
          sessionId,
          relayMessageId,
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>

      const denied = yield* push(
        Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000041")
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.flip
      )
      assert.instanceOf(denied, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(admitCalls), 0)

      sendMode = "Stale"
      const stale = yield* push(
        Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000042")
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.flip
      )
      assert.instanceOf(stale, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(admitCalls), 0)

      const sendRequests = (yield* Ref.get(unsafeRequests)).filter(
        (request) => request.direction === "Send"
      )
      assert.deepStrictEqual(sendRequests, [
        {
          risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
          direction: "Send",
          principal: relayPrincipal,
          remote: relayOpenRequest.remote,
          documents: [{ documentType: Task.name, documentId: taskId }]
        },
        {
          risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
          direction: "Send",
          principal: relayPrincipal,
          remote: relayOpenRequest.remote,
          documents: [{ documentType: Task.name, documentId: taskId }]
        }
      ])
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("requires unsafe Receive trust before relay claim or payload read", () =>
    Effect.scoped(Effect.gen(function*() {
      const receiveDenied = yield* Deferred.make<void>()
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: authorizeRelay,
        authorizeUnsafeUnboundedAutomerge3Decode: (request) =>
          request.direction === "Send"
            ? authorizeUnsafeRelayDecode(request)
            : Deferred.succeed(receiveDenied, undefined).pipe(
              Effect.andThen(Effect.fail(new PeerRpcError.AccessDenied()))
            )
      })
      const claimCalls = yield* Ref.make(0)
      const payloadReads = yield* Ref.make(0)
      const store = makeRelayStore({
        claim: () =>
          Ref.update(claimCalls, (count) => count + 1).pipe(
            Effect.as({
              message: Option.none(),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            })
          ),
        loadClaimedPayload: () =>
          Ref.update(payloadReads, (count) => count + 1).pipe(
            Effect.as(new Uint8Array())
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      yield* Deferred.await(receiveDenied)
      yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(claimCalls), 0)
      assert.strictEqual(yield* Ref.get(payloadReads), 0)
      assert.isTrue(Option.isNone(yield* Queue.poll(events)))
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("drains admitted Push while revocation denies later operations", () =>
    Effect.scoped(Effect.gen(function*() {
      const sendInvalidated = yield* Deferred.make<void>()
      let pushing = false
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => ({
              ...result,
              invalidated: pushing && request.direction === "Send"
                ? Deferred.await(sendInvalidated)
                : Effect.never
            }))
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const admitStarted = yield* Deferred.make<void>()
      const allowCommit = yield* Deferred.make<void>()
      const commits = yield* Ref.make(0)
      const store = makeRelayStore({
        admit: (input) =>
          Effect.uninterruptible(
            Deferred.succeed(admitStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowCommit)),
              Effect.andThen(Ref.update(commits, (count) => count + 1)),
              Effect.as({
                status: "Accepted",
                channel: input.channel,
                ready: false,
                nextEligibleAt: 0,
                lane: "New"
              })
            )
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(
        PeerRpc.PushRpc.key
      ) as Rpc.Handler<"Push">
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      pushing = true
      const { payload } = yield* makeRelayPayload
      const pushFiber = yield* (
        pushHandler.handler({
          sessionId: (opened as PeerRpc.Opened).sessionId,
          relayMessageId: Identity.RelayMessageId.make(
            "rly_00000000-0000-4000-8000-000000000043"
          ),
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.exit,
        Effect.forkScoped
      )
      yield* Deferred.await(admitStarted)
      yield* Deferred.succeed(sendInvalidated, undefined)
      yield* Effect.yieldNow
      yield* Deferred.succeed(allowCommit, undefined)
      const pushExit = yield* Fiber.join(pushFiber)
      assert.isTrue(Exit.isSuccess(pushExit))
      assert.strictEqual(yield* Ref.get(commits), 1)
      const later = yield* (
        pushHandler.handler({
          sessionId: (opened as PeerRpc.Opened).sessionId,
          relayMessageId: Identity.RelayMessageId.make(
            "rly_00000000-0000-4000-8000-000000000046"
          ),
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.flip
      )
      assert.instanceOf(later, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(commits), 1)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("denies terminal mutation when fresh Receive revocation wins admission", () =>
    Effect.scoped(Effect.gen(function*() {
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Receive") receiveAuthorizations += 1
              return {
                ...result,
                invalidated: request.direction === "Receive" && receiveAuthorizations === 3
                  ? Effect.void
                  : Effect.never
              }
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const acknowledgeCalls = yield* Ref.make(0)
      const context = yield* makeTerminalRelayContext(
        Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000050"),
        PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000050"),
        authorization,
        () =>
          Ref.update(acknowledgeCalls, (count) => count + 1).pipe(
            Effect.as(relayTransition)
          )
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const acknowledgeHandler = context.mapUnsafe.get(
        PeerRpc.AcknowledgeRpc.key
      ) as Rpc.Handler<"Acknowledge">
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      const stored = yield* Queue.take(events)
      if (opened._tag !== "Opened" || stored._tag !== "StoredMessage") {
        return assert.fail("Expected relay open and stored message events")
      }
      const denied = yield* (
        acknowledgeHandler.handler({
          sessionId: opened.sessionId,
          relayMessageId: stored.relayMessageId,
          claimToken: stored.claimToken,
          messageHash: stored.messageHash
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(acknowledgeHandler, effect),
        Effect.flip
      )
      assert.instanceOf(denied, PeerRpcError.AccessDenied)
      assert.strictEqual(yield* Ref.get(acknowledgeCalls), 0)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("drains an admitted terminal mutation through revocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const terminalInvalidated = yield* Deferred.make<void>()
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Receive") receiveAuthorizations += 1
              return {
                ...result,
                invalidated: request.direction === "Receive" && receiveAuthorizations === 3
                  ? Deferred.await(terminalInvalidated)
                  : Effect.never
              }
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const acknowledgeStarted = yield* Deferred.make<void>()
      const allowAcknowledge = yield* Deferred.make<void>()
      const acknowledgeCalls = yield* Ref.make(0)
      const context = yield* makeTerminalRelayContext(
        Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000051"),
        PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000051"),
        authorization,
        () =>
          Effect.uninterruptible(
            Deferred.succeed(acknowledgeStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowAcknowledge)),
              Effect.andThen(Ref.update(acknowledgeCalls, (count) => count + 1)),
              Effect.as(relayTransition)
            )
          )
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const acknowledgeHandler = context.mapUnsafe.get(
        PeerRpc.AcknowledgeRpc.key
      ) as Rpc.Handler<"Acknowledge">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      const stored = yield* Queue.take(events)
      if (opened._tag !== "Opened" || stored._tag !== "StoredMessage") {
        return assert.fail("Expected relay open and stored message events")
      }
      const terminalFiber = yield* (
        acknowledgeHandler.handler({
          sessionId: opened.sessionId,
          relayMessageId: stored.relayMessageId,
          claimToken: stored.claimToken,
          messageHash: stored.messageHash
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(acknowledgeHandler, effect),
        Effect.exit,
        Effect.forkScoped
      )
      yield* Deferred.await(acknowledgeStarted)
      yield* Deferred.succeed(terminalInvalidated, undefined)
      yield* Effect.yieldNow
      yield* Deferred.succeed(allowAcknowledge, undefined)
      assert.isTrue(Exit.isSuccess(yield* Fiber.join(terminalFiber)))
      assert.strictEqual(yield* Ref.get(acknowledgeCalls), 1)
      assert.strictEqual((yield* runtime.usage).activeClaims, 0)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("drains admitted delivery through outbound offer during revocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000047"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000047"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* makeRelayOuterEnvelopeDigest(
        relayMessageId,
        messageHash,
        payload
      )
      const receiveInvalidated = yield* Deferred.make<void>()
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Receive") receiveAuthorizations += 1
              return {
                ...result,
                invalidated: request.direction === "Receive" && receiveAuthorizations > 1
                  ? Deferred.await(receiveInvalidated)
                  : Effect.never
              }
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const claimStarted = yield* Deferred.make<void>()
      const allowClaimCommit = yield* Deferred.make<void>()
      const payloadReads = yield* Ref.make(0)
      let claimed = false
      const store = makeRelayStore({
        claim: (request) =>
          Effect.uninterruptible(
            Deferred.succeed(claimStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowClaimCommit)),
              Effect.map(() => {
                if (claimed) {
                  return {
                    message: Option.none(),
                    ready: false,
                    nextEligibleAt: Option.none(),
                    lane: "Retry" as const
                  }
                }
                claimed = true
                return {
                  message: Option.some(makeRelayClaim(
                    relayMessageId,
                    claimToken,
                    messageHash,
                    outerEnvelopeDigest,
                    payload.byteLength,
                    request.sessionGeneration
                  )),
                  ready: false,
                  nextEligibleAt: Option.none(),
                  lane: "Retry" as const
                }
              })
            )
          ),
        loadClaimedPayload: () =>
          Ref.update(payloadReads, (count) => count + 1).pipe(
            Effect.as(payload)
          )
      })
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Effect.void,
            transferToCurrentRequest: Effect.void
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      yield* Deferred.await(claimStarted)
      yield* Deferred.succeed(receiveInvalidated, undefined)
      yield* Effect.yieldNow
      yield* Deferred.succeed(allowClaimCommit, undefined)
      assert.strictEqual((yield* Queue.take(events))._tag, "StoredMessage")
      assert.strictEqual(yield* Ref.get(payloadReads), 1)
      assert.strictEqual((yield* runtime.usage).activeClaims, 1)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("does not emit a dequeued delivery after session revocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000048"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000048"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* makeRelayOuterEnvelopeDigest(
        relayMessageId,
        messageHash,
        payload
      )
      const sessionInvalidated = yield* Deferred.make<void>()
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Receive") receiveAuthorizations += 1
              return {
                ...result,
                invalidated: request.direction === "Receive" && receiveAuthorizations === 1
                  ? Deferred.await(sessionInvalidated)
                  : Effect.never
              }
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      let claimed = false
      const store = makeRelayStore({
        claim: (request) =>
          Effect.sync(() => {
            if (claimed) {
              return {
                message: Option.none(),
                ready: false,
                nextEligibleAt: Option.none(),
                lane: "Retry" as const
              }
            }
            claimed = true
            return {
              message: Option.some(makeRelayClaim(
                relayMessageId,
                claimToken,
                messageHash,
                outerEnvelopeDigest,
                payload.byteLength,
                request.sessionGeneration
              )),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }),
        loadClaimedPayload: () => Effect.succeed(payload)
      })
      const transferStarted = yield* Deferred.make<void>()
      const transferGate = yield* Deferred.make<void>()
      const reservationReleases = yield* Ref.make(0)
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Ref.update(reservationReleases, (count) => count + 1),
            transferToCurrentRequest: Deferred.succeed(transferStarted, undefined).pipe(
              Effect.andThen(Deferred.await(transferGate))
            )
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.exit,
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      yield* Deferred.await(transferStarted)
      yield* Deferred.succeed(sessionInvalidated, undefined)
      for (let index = 0; index < 20; index++) yield* Effect.yieldNow
      assert.strictEqual((yield* runtime.usage).sessions, 0)
      yield* Deferred.succeed(transferGate, undefined)
      yield* Fiber.join(openFiber)
      assert.isTrue(Option.isNone(yield* Queue.poll(events)))
      assert.strictEqual(yield* Ref.get(reservationReleases), 1)
    })))

  it.effect("treats a stale claimed payload as a local delivery race", () =>
    Effect.scoped(Effect.gen(function*() {
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000044"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000044"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* makeRelayOuterEnvelopeDigest(
        relayMessageId,
        messageHash,
        payload
      )
      let claimed = false
      const released = yield* Deferred.make<void>()
      const store = makeRelayStore({
        claim: (request) =>
          Effect.sync(() => {
            if (claimed) {
              return {
                message: Option.none(),
                ready: false,
                nextEligibleAt: Option.none(),
                lane: "Retry" as const
              }
            }
            claimed = true
            return {
              message: Option.some(makeRelayClaim(
                relayMessageId,
                claimToken,
                messageHash,
                outerEnvelopeDigest,
                payload.byteLength,
                request.sessionGeneration
              )),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }),
        loadClaimedPayload: () =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active relay claim",
                observed: "stale relay claim"
              })
            })
          ),
        release: () =>
          Deferred.succeed(released, undefined).pipe(
            Effect.as(relayTransition)
          )
      })
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Effect.void,
            transferToCurrentRequest: Effect.void
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        PeerRelayAuthorization.PeerRelayAuthorization.of({
          authorize: authorizeRelay,
          authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
        }),
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      yield* Deferred.await(released)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      yield* runtime.health
      assert.strictEqual((yield* runtime.usage).activeClaims, 0)
      assert.isTrue(Option.isNone(yield* Queue.poll(events)))
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("clears local claim ownership after a stale terminal transition", () =>
    Effect.scoped(Effect.gen(function*() {
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000045"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000045"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* makeRelayOuterEnvelopeDigest(
        relayMessageId,
        messageHash,
        payload
      )
      let claimed = false
      const store = makeRelayStore({
        claim: (request) =>
          Effect.sync(() => {
            if (claimed) {
              return {
                message: Option.none(),
                ready: false,
                nextEligibleAt: Option.none(),
                lane: "Retry" as const
              }
            }
            claimed = true
            return {
              message: Option.some(makeRelayClaim(
                relayMessageId,
                claimToken,
                messageHash,
                outerEnvelopeDigest,
                payload.byteLength,
                request.sessionGeneration
              )),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }),
        loadClaimedPayload: () => Effect.succeed(payload),
        acknowledge: () =>
          Effect.succeed({
            status: "Stale",
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry"
          })
      })
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Effect.void,
            transferToCurrentRequest: Effect.void
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        PeerRelayAuthorization.PeerRelayAuthorization.of({
          authorize: authorizeRelay,
          authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
        }),
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const acknowledgeHandler = context.mapUnsafe.get(
        PeerRpc.AcknowledgeRpc.key
      ) as Rpc.Handler<"Acknowledge">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      const stored = yield* Queue.take(events)
      assert.strictEqual(stored._tag, "StoredMessage")
      if (opened._tag !== "Opened" || stored._tag !== "StoredMessage") {
        return assert.fail("Expected relay open and stored message events")
      }
      const error = yield* (
        acknowledgeHandler.handler({
          sessionId: opened.sessionId,
          relayMessageId: stored.relayMessageId,
          claimToken: stored.claimToken,
          messageHash: stored.messageHash
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(acknowledgeHandler, effect),
        Effect.flip
      )
      assert.instanceOf(error, PeerRpcError.SessionUnavailable)
      yield* runtime.health
      assert.deepStrictEqual(yield* runtime.usage, {
        accepting: true,
        sessions: 1,
        subjects: 1,
        activeClaims: 0,
        queuedChannels: 0
      })
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("drains an admitted empty claim after session revocation", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessionInvalidated = yield* Deferred.make<void>()
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizeRelay(request).pipe(
            Effect.map((result) => {
              if (request.direction === "Receive") receiveAuthorizations += 1
              return {
                ...result,
                invalidated: request.direction === "Receive" && receiveAuthorizations === 1
                  ? Deferred.await(sessionInvalidated)
                  : Effect.never
              }
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const claimStarted = yield* Deferred.make<void>()
      const allowClaim = yield* Deferred.make<void>()
      const claimCompleted = yield* Deferred.make<void>()
      const store = makeRelayStore({
        claim: () =>
          Deferred.succeed(claimStarted, undefined).pipe(
            Effect.andThen(Deferred.await(allowClaim)),
            Effect.andThen(Deferred.succeed(claimCompleted, undefined)),
            Effect.as({
              message: Option.none(),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry"
            })
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      yield* Deferred.await(claimStarted)
      yield* Deferred.succeed(sessionInvalidated, undefined)
      yield* Effect.yieldNow
      assert.strictEqual((yield* runtime.usage).sessions, 0)
      yield* Deferred.succeed(allowClaim, undefined)
      yield* Deferred.await(claimCompleted)
      yield* Effect.yieldNow
      yield* runtime.health
      assert.deepStrictEqual(yield* runtime.usage, {
        accepting: true,
        sessions: 0,
        subjects: 0,
        activeClaims: 0,
        queuedChannels: 0
      })
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("reports the logical remote recipient separately from the relay endpoint", () =>
    Effect.scoped(Effect.gen(function*() {
      const localPrincipal: PeerAuthentication.PeerPrincipal = {
        tenantId: "tenant",
        subjectId: "sender",
        peerId: remotePeerA
      }
      let authorizationDefect: unknown | undefined
      const relayAuthorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          authorizationDefect === undefined
            ? Effect.succeed({
              remote: {
                tenantId: request.principal.tenantId,
                subjectId: request.remote.subjectId,
                peerId: request.remote.peerId
              },
              documents: request.documents.map((document) => ({
                document: Task,
                documentId: document.documentId
              })),
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
            : Effect.die(authorizationDefect),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const transition: PeerRelayStore.TransitionResult = {
        status: "Changed",
        ready: false,
        nextEligibleAt: Option.none(),
        lane: "Retry"
      }
      let maintenanceDefect: unknown | undefined
      const admitted = yield* Deferred.make<PeerRelayStore.Admission>()
      const acknowledgeCalls = yield* Ref.make(0)
      const relayStore = PeerRelayStore.PeerRelayStore.of({
        admit: (input) =>
          Deferred.succeed(admitted, input).pipe(
            Effect.as({
              status: "Accepted",
              channel: input.channel,
              ready: false,
              nextEligibleAt: 0,
              lane: "New"
            })
          ),
        claim: () =>
          Effect.succeed({
            message: Option.none(),
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry"
          }),
        loadClaimedPayload: () => Effect.succeed(new Uint8Array()),
        acknowledge: () => Ref.update(acknowledgeCalls, (count) => count + 1).pipe(Effect.as(transition)),
        reject: () => Effect.succeed(transition),
        release: () => Effect.succeed(transition),
        recover: ({ cursor }) =>
          maintenanceDefect === undefined
            ? Effect.succeed({
              ...(cursor === undefined ? {} : { cursor }),
              processed: 0,
              hasMore: false
            })
            : Effect.die(maintenanceDefect),
        expire: ({ cursor }) =>
          Effect.succeed({ ...(cursor === undefined ? {} : { cursor }), processed: 0, hasMore: false }),
        repair: ({ cursor }) =>
          Effect.succeed({ ...(cursor === undefined ? {} : { cursor }), processed: 0, hasMore: false }),
        reconcile: ({ cursor }) =>
          Effect.succeed({ ...(cursor === undefined ? {} : { cursor }), processed: 0, hasMore: false }),
        collect: ({ cursor }) =>
          Effect.succeed({ ...(cursor === undefined ? {} : { cursor }), processed: 0, hasMore: false }),
        usage: () =>
          Effect.succeed({
            activeCount: 0,
            activeBytes: 0,
            retainedCount: 0,
            retainedBytes: 0
          })
      })
      const relayIngress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: () => Effect.fail(new PeerRpcError.RequestCapacityExceeded()),
        usage: Effect.succeed({ connections: 0, reservedBytes: 0, byteReservationWaiters: 0 }),
        await: Effect.never
      })
      const handlers = PeerRpcServer.layerHandlers({
        tenantId: "tenant",
        peerId: serverPeerId
      }).pipe(
        Layer.provide(Layer.mergeAll(
          Layer.succeed(Crypto.Crypto, crypto),
          Layer.succeed(PeerRelayLimits.PeerRelayLimits, PeerRelayLimits.defaults),
          Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization, relayAuthorization),
          Layer.succeed(PeerRelayStore.PeerRelayStore, relayStore),
          Layer.succeed(PeerRelayIngress.PeerRelayIngress, relayIngress)
        ))
      )
      const context = yield* Layer.build(handlers)
      const openHandler = context.mapUnsafe.get(PeerRpc.OpenRpc.key) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(PeerRpc.PushRpc.key) as Rpc.Handler<"Push">
      const acknowledgeHandler = context.mapUnsafe.get(
        PeerRpc.AcknowledgeRpc.key
      ) as Rpc.Handler<"Acknowledge">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const protocolStopped = yield* Deferred.make<void>()
      const ingressStopped = yield* Deferred.make<void>()
      const baseProtocol = yield* RpcServer.Protocol.make(() =>
        Effect.succeed({
          disconnects: Queue.unbounded<number>().pipe(Effect.runSync),
          send: () => Effect.void,
          end: () => Effect.void,
          clientIds: Effect.succeed(new Set<number>()),
          initialMessage: Effect.succeed(Option.none()),
          supportsAck: true,
          supportsTransferables: false,
          supportsSpanPropagation: true
        })
      )
      const protocol: RpcServer.Protocol["Service"] = {
        ...baseProtocol,
        run: (handler) =>
          baseProtocol.run(handler).pipe(
            Effect.ensuring(Deferred.succeed(protocolStopped, undefined))
          )
      }
      const openRequest = {
        protocolVersion: PeerRpc.protocolVersion,
        expectedRelayPeerId: serverPeerId,
        expectedLocal: localPrincipal,
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        remote: {
          subjectId: "recipient",
          peerId: remotePeerB
        },
        documents: [{ documentType: Task.name, documentId: taskId }],
        receiptRetentionMillis: PeerRelayLimits.defaults.maximumReceiptRetentionMillis,
        senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis
      } satisfies typeof PeerRpc.OpenRpc.payloadSchema.Type
      const authenticated = {
        principal: localPrincipal,
        validUntil: Number.MAX_SAFE_INTEGER,
        invalidated: Effect.never
      }
      const stream = openHandler.handler(
        openRequest,
        {} as never
      ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        stream.pipe(
          Stream.provideContext(Context.add(
            openHandler.context,
            PeerAuthentication.AuthenticatedPeer,
            authenticated
          ))
        ),
        (event) => Queue.offer(events, event)
      ).pipe(Effect.forkScoped)
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      assert.strictEqual(
        (opened as PeerRpc.Opened).remotePeerId,
        remotePeerB
      )
      const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
      const message = Uint8Array.of(1, 2, 3)
      const messageHash = yield* Canonical.digest(message).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      const payload = new TextEncoder().encode(
        yield* Schema.encodeEffect(RelaySyncEnvelopeJson)({
          connectionEpoch: "epoch",
          sequence: 0,
          documentId: taskId,
          documentType: Task.name,
          messageHash,
          message,
          lineage: Identity.genesisLineage,
          writerProvenance: []
        })
      )
      const expectedDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: localPrincipal,
        remote: {
          tenantId: "tenant",
          subjectId: "recipient",
          peerId: remotePeerB
        },
        relayPeerId: serverPeerId,
        relayMessageId,
        protocolVersion: PeerRpc.protocolVersion,
        payloadVersion: 1,
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        senderConnectionEpoch: "epoch",
        senderSequence: 0,
        document: {
          documentId: taskId,
          documentType: Task.name
        },
        lineage: Identity.genesisLineage,
        writerProvenance: [],
        messageHash,
        payload
      }).pipe(Effect.provideService(Crypto.Crypto, crypto))
      yield* (pushHandler.handler({
        sessionId: (opened as PeerRpc.Opened).sessionId,
        relayMessageId,
        payload
      }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>).pipe(
        Effect.provideContext(Context.add(
          pushHandler.context,
          PeerAuthentication.AuthenticatedPeer,
          authenticated
        ))
      )
      assert.strictEqual((yield* Deferred.await(admitted)).outerEnvelopeDigest, expectedDigest)
      const defect = new Error("relay authorization defect")
      authorizationDefect = defect
      const defectExit = yield* Stream.runHead(
        (openHandler.handler(
          openRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>).pipe(
          Stream.provideContext(Context.add(
            openHandler.context,
            PeerAuthentication.AuthenticatedPeer,
            authenticated
          ))
        )
      ).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(defectExit))
      if (Exit.isFailure(defectExit)) {
        assert.strictEqual(Cause.squash(defectExit.cause), defect)
      }
      authorizationDefect = undefined
      const ownerContext = yield* Layer.build(Layer.fresh(handlers))
      const ownerRuntime = Context.get(ownerContext, PeerRpcServer.PeerRpcServerRuntime)
      const serverContext = Context.add(
        Context.add(
          Context.add(
            ownerContext,
            RpcServer.Protocol,
            protocol
          ),
          PeerRelayIngress.PeerRelayIngress,
          {
            ...relayIngress,
            await: Effect.never.pipe(
              Effect.ensuring(Deferred.succeed(ingressStopped, undefined))
            )
          }
        ),
        PeerAuthentication.PeerAuthentication,
        PeerAuthentication.PeerAuthentication.of((effect) =>
          Effect.provideService(effect, PeerAuthentication.AuthenticatedPeer, {
            principal: localPrincipal,
            validUntil: Number.MAX_SAFE_INTEGER,
            invalidated: Effect.never
          })
        )
      )
      yield* Layer.build(
        PeerRpcServer.layerServer.pipe(
          Layer.provide(Layer.succeedContext(serverContext))
        )
      )
      const ownerDefect = new Error("relay maintenance defect")
      maintenanceDefect = ownerDefect
      yield* TestClock.adjust(PeerRelayLimits.defaults.maintenanceIntervalMillis)
      const ownerExit = yield* ownerRuntime.owner.pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(ownerExit))
      if (Exit.isFailure(ownerExit)) {
        assert.strictEqual(Cause.squash(ownerExit.cause), ownerDefect)
      }
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.isTrue(Option.isSome(yield* Deferred.poll(protocolStopped)))
      assert.isTrue(Option.isSome(yield* Deferred.poll(ingressStopped)))
      yield* runtime.shutdown
      yield* runtime.shutdown
      const stale = yield* (acknowledgeHandler.handler({
        sessionId: (opened as PeerRpc.Opened).sessionId,
        relayMessageId,
        claimToken: PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000001"),
        messageHash
      }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>).pipe(
        Effect.provideContext(Context.add(
          acknowledgeHandler.context,
          PeerAuthentication.AuthenticatedPeer,
          authenticated
        )),
        Effect.flip
      )
      assert.strictEqual(stale._tag, "SessionUnavailable")
      assert.strictEqual(yield* Ref.get(acknowledgeCalls), 0)
      assert.deepStrictEqual(yield* runtime.usage, {
        accepting: false,
        sessions: 0,
        subjects: 0,
        activeClaims: 0,
        queuedChannels: 0
      })
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("does not deliver a document type excluded by fresh Receive authorization", () =>
    Effect.scoped(Effect.gen(function*() {
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000021"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000021"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: relayPrincipal,
        remote: {
          tenantId: "tenant",
          subjectId: relayOpenRequest.remote.subjectId,
          peerId: relayOpenRequest.remote.peerId
        },
        relayPeerId: serverPeerId,
        relayMessageId,
        protocolVersion: PeerRpc.protocolVersion,
        payloadVersion: 1,
        senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
        senderConnectionEpoch: "epoch",
        senderSequence: 0,
        document: {
          documentId: taskId,
          documentType: Task.name
        },
        lineage: Identity.genesisLineage,
        writerProvenance: [],
        messageHash,
        payload
      }).pipe(Effect.provideService(Crypto.Crypto, crypto))
      let receiveAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          Effect.sync(() => {
            if (request.direction === "Receive") receiveAuthorizations++
            return {
              remote: {
                tenantId: request.principal.tenantId,
                subjectId: request.remote.subjectId,
                peerId: request.remote.peerId
              },
              documents: request.documents.map((document) => ({
                document: request.direction === "Receive" && receiveAuthorizations > 1
                  ? Note
                  : Task,
                documentId: document.documentId
              })),
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            }
          }),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      let claimed = false
      const storeReleaseCalls = yield* Ref.make(0)
      const reservationReleaseCalls = yield* Ref.make(0)
      const reservationTransferCalls = yield* Ref.make(0)
      const deliverySettled = yield* Deferred.make<"Released" | "Transferred">()
      const store = makeRelayStore({
        claim: (request) =>
          Effect.sync(() => {
            if (claimed) {
              return {
                message: Option.none(),
                ready: false,
                nextEligibleAt: Option.none(),
                lane: "Retry" as const
              }
            }
            claimed = true
            return {
              message: Option.some(PeerRelayStore.ClaimedMessage.make({
                rowId: 1,
                channel: {
                  tenantId: "tenant",
                  senderSubjectId: relayPrincipal.subjectId,
                  senderPeerId: relayPrincipal.peerId,
                  senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
                  recipientSubjectId: relayOpenRequest.remote.subjectId,
                  recipientPeerId: relayOpenRequest.remote.peerId
                },
                relayMessageId,
                relayPeerId: serverPeerId,
                senderConnectionEpoch: "epoch",
                senderSequence: 0,
                documentIds: [taskId],
                payloadVersion: 1,
                messageHash,
                outerEnvelopeDigest,
                payloadBytes: payload.byteLength,
                claimToken,
                claimDeadline: 1,
                sessionGeneration: request.sessionGeneration
              })),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }),
        loadClaimedPayload: () => Effect.succeed(payload),
        release: () =>
          Ref.update(storeReleaseCalls, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(deliverySettled, "Released")),
            Effect.as(relayTransition)
          )
      })
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Ref.update(reservationReleaseCalls, (count) => count + 1),
            transferToCurrentRequest: Ref.update(
              reservationTransferCalls,
              (count) => count + 1
            ).pipe(
              Effect.andThen(Deferred.succeed(deliverySettled, "Transferred")),
              Effect.asVoid
            )
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      assert.strictEqual(yield* Deferred.await(deliverySettled), "Released")
      assert.strictEqual(yield* Ref.get(storeReleaseCalls), 1)
      assert.strictEqual(yield* Ref.get(reservationReleaseCalls), 1)
      assert.strictEqual(yield* Ref.get(reservationTransferCalls), 0)
      assert.isTrue(Option.isNone(yield* Queue.poll(events)))
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("keeps the sole subject Open slot owned after another Open is rejected", () =>
    Effect.scoped(Effect.gen(function*() {
      const authorizationStarted = yield* Deferred.make<void>()
      const releaseAuthorization = yield* Deferred.make<void>()
      const authorizationCalls = yield* Ref.make(0)
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          Ref.updateAndGet(authorizationCalls, (count) => count + 1).pipe(
            Effect.tap((count) =>
              count === 1
                ? Deferred.succeed(authorizationStarted, undefined)
                : Effect.void
            ),
            Effect.andThen(Deferred.await(releaseAuthorization)),
            Effect.as({
              remote: {
                tenantId: request.principal.tenantId,
                subjectId: request.remote.subjectId,
                peerId: request.remote.peerId
              },
              documents: request.documents.map((document) => ({
                document: Task,
                documentId: document.documentId
              })),
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
          ),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        makeRelayStore(),
        PeerRelayLimits.Values.make({
          ...PeerRelayLimits.defaults,
          maxInFlightOpen: 3,
          maxInFlightOpenPerSubject: 1,
          openRatePerSecond: 1_000,
          openBurst: 100
        })
      )
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const invoke = Stream.runHead(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.asVoid
      )
      const first = yield* invoke.pipe(Effect.forkScoped)
      yield* Deferred.await(authorizationStarted)
      const second = yield* invoke.pipe(Effect.flip)
      assert.instanceOf(second, PeerRpcError.RequestCapacityExceeded)
      const third = yield* invoke.pipe(Effect.flip)
      assert.instanceOf(third, PeerRpcError.RequestCapacityExceeded)
      assert.strictEqual(yield* Ref.get(authorizationCalls), 1)
      yield* Deferred.succeed(releaseAuthorization, undefined)
      yield* Fiber.join(first)
    })))

  it.effect("detaches the incumbent when an interrupted replacement acquires the registry lock", () =>
    Effect.scoped(Effect.gen(function*() {
      const replacementAuthorized = yield* Deferred.make<void>()
      let trackReplacement = false
      let replacementAuthorizationCalls = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) => {
          if (trackReplacement) {
            replacementAuthorizationCalls += 1
            if (replacementAuthorizationCalls === 2) {
              Deferred.doneUnsafe(replacementAuthorized, Effect.void)
            }
          }
          return authorizeRelay(request)
        },
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const context = yield* makeRelayHandlerContext(authorization, makeRelayStore())
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const invoke = (request: typeof PeerRpc.OpenRpc.payloadSchema.Type) =>
        Stream.runForEach(
          handler.handler(
            request,
            {} as never
          ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
          () => Effect.void
        ).pipe((effect) => relayHandlerEffect(handler, effect))
      const incumbent = yield* invoke(relayOpenRequest).pipe(Effect.forkScoped)
      const blockerRequest = {
        ...relayOpenRequest,
        remote: {
          subjectId: "registry-blocker",
          peerId: remotePeerB
        }
      }
      const blocker = yield* invoke(blockerRequest).pipe(Effect.forkScoped)
      while ((yield* runtime.usage).sessions !== 2) {
        yield* Effect.yieldNow
      }

      const registryLocked = yield* Deferred.make<void>()
      const releaseRegistry = yield* Deferred.make<void>()
      const setRelayActiveClaims = PeerRpcObservability.setRelayActiveClaims
      const metricSpy = vi.spyOn(PeerRpcObservability, "setRelayActiveClaims")
        .mockImplementation((amount) =>
          Deferred.succeed(registryLocked, undefined).pipe(
            Effect.andThen(Deferred.await(releaseRegistry)),
            Effect.andThen(setRelayActiveClaims(amount))
          )
        )

      yield* Effect.gen(function*() {
        const interruptBlocker = yield* Fiber.interrupt(blocker).pipe(Effect.forkScoped)
        yield* Deferred.await(registryLocked)

        trackReplacement = true
        const replacement = yield* invoke(relayOpenRequest).pipe(Effect.forkScoped)
        yield* Deferred.await(replacementAuthorized)
        yield* Effect.forEach(
          Array.from({ length: 10 }),
          () => Effect.yieldNow,
          { discard: true }
        )
        const interruptReplacement = yield* Fiber.interrupt(replacement).pipe(Effect.forkScoped)
        yield* Effect.forEach(
          Array.from({ length: 10 }),
          () => Effect.yieldNow,
          { discard: true }
        )
        assert.isUndefined(interruptReplacement.pollUnsafe())

        yield* Deferred.succeed(releaseRegistry, undefined)
        yield* Fiber.join(interruptBlocker)
        yield* Fiber.join(interruptReplacement)
        assert.strictEqual((yield* runtime.usage).sessions, 0)
        yield* Fiber.interrupt(incumbent)
      }).pipe(
        Effect.ensuring(
          Deferred.succeed(releaseRegistry, undefined).pipe(
            Effect.andThen(Effect.sync(() => metricSpy.mockRestore()))
          )
        )
      )
    })))

  it.effect("removes a newly registered Open when replacement cleanup is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const lateMonitors = yield* Ref.make(0)
      const lateMonitorSignal = yield* Deferred.make<void>()
      let trackLateMonitors = false
      let trackedAuthorizations = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) => {
          const tracked = trackLateMonitors
          if (tracked) trackedAuthorizations += 1
          return Effect.succeed({
            remote: {
              tenantId: request.principal.tenantId,
              subjectId: request.remote.subjectId,
              peerId: request.remote.peerId
            },
            documents: request.documents.map((document) => ({
              document: Task,
              documentId: document.documentId
            })),
            validUntil: Number.MAX_SAFE_INTEGER,
            invalidated: tracked
              ? Deferred.await(lateMonitorSignal).pipe(
                Effect.tap(() => Ref.update(lateMonitors, (count) => count + 1))
              )
              : Effect.never
          })
        },
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const relayMessageId = Identity.RelayMessageId.make(
        "rly_00000000-0000-4000-8000-000000000031"
      )
      const claimToken = PeerRpc.ClaimToken.make(
        "clm_00000000-0000-4000-8000-000000000031"
      )
      const { messageHash, payload } = yield* makeRelayPayload
      const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: relayPrincipal,
        remote: {
          tenantId: "tenant",
          subjectId: relayOpenRequest.remote.subjectId,
          peerId: relayOpenRequest.remote.peerId
        },
        relayPeerId: serverPeerId,
        relayMessageId,
        protocolVersion: PeerRpc.protocolVersion,
        payloadVersion: 1,
        senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
        senderConnectionEpoch: "epoch",
        senderSequence: 0,
        document: {
          documentId: taskId,
          documentType: Task.name
        },
        lineage: Identity.genesisLineage,
        writerProvenance: [],
        messageHash,
        payload
      }).pipe(Effect.provideService(Crypto.Crypto, crypto))
      let claimed = false
      let releaseStarted = yield* Deferred.make<void>()
      let releaseGate = yield* Deferred.make<void>()
      const store = makeRelayStore({
        claim: (request) =>
          Effect.sync(() => {
            if (claimed) {
              return {
                message: Option.none(),
                ready: false,
                nextEligibleAt: Option.none(),
                lane: "Retry" as const
              }
            }
            claimed = true
            return {
              message: Option.some(PeerRelayStore.ClaimedMessage.make({
                rowId: 1,
                channel: {
                  tenantId: "tenant",
                  senderSubjectId: relayPrincipal.subjectId,
                  senderPeerId: relayPrincipal.peerId,
                  senderReplicaIncarnation: relayOpenRequest.senderReplicaIncarnation,
                  recipientSubjectId: relayOpenRequest.remote.subjectId,
                  recipientPeerId: relayOpenRequest.remote.peerId
                },
                relayMessageId,
                relayPeerId: serverPeerId,
                senderConnectionEpoch: "epoch",
                senderSequence: 0,
                documentIds: [taskId],
                payloadVersion: 1,
                messageHash,
                outerEnvelopeDigest,
                payloadBytes: payload.byteLength,
                claimToken,
                claimDeadline: 1,
                sessionGeneration: request.sessionGeneration
              })),
              ready: false,
              nextEligibleAt: Option.none(),
              lane: "Retry" as const
            }
          }),
        loadClaimedPayload: () => Effect.succeed(payload),
        release: () =>
          Deferred.succeed(releaseStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseGate)),
            Effect.as(relayTransition)
          )
      })
      const ingress = PeerRelayIngress.PeerRelayIngress.of({
        address: { _tag: "UnixAddress", path: "relay-test" },
        reserveOutbound: (bytes) =>
          Effect.succeed({
            bytes,
            release: Effect.void,
            transferToCurrentRequest: Effect.void
          }),
        usage: Effect.succeed({
          connections: 0,
          reservedBytes: 0,
          byteReservationWaiters: 0
        }),
        await: Effect.never
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        store,
        PeerRelayLimits.defaults,
        ingress
      )
      const handler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const first = yield* Stream.runForEach(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(events))._tag, "Opened")
      assert.strictEqual((yield* Queue.take(events))._tag, "StoredMessage")
      assert.strictEqual((yield* runtime.usage).activeClaims, 1)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayActiveClaims())).value, 1)
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.relayWorkers())).value,
        PeerRelayLimits.defaults.relayWorkerConcurrency
      )
      assert.strictEqual(
        (yield* Metric.value(
          PeerRpcObservability.relayOutcomes("RelayClaim", "Receive", "Delivered")
        )).count,
        1
      )
      assert.strictEqual(
        (yield* Metric.value(
          PeerRpcObservability.relayBytes("RelayClaim", "Receive", "Delivered", 1)
        )).count,
        1
      )
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayPendingItems())).value, 0)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayPendingBytes())).value, 0)
      const second = yield* Stream.runHead(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.forkScoped
      )
      yield* Deferred.await(releaseStarted)
      const interrupted = yield* Fiber.interrupt(second).pipe(Effect.forkScoped)
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseGate, undefined)
      yield* Fiber.join(interrupted)
      assert.deepStrictEqual(yield* runtime.usage, {
        accepting: true,
        sessions: 0,
        subjects: 0,
        activeClaims: 0,
        queuedChannels: 0
      })
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayActiveClaims())).value, 0)
      yield* Fiber.interrupt(first)

      claimed = false
      releaseStarted = yield* Deferred.make<void>()
      releaseGate = yield* Deferred.make<void>()
      const nextEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const nextIncumbent = yield* Stream.runForEach(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(nextEvents, event)
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(nextEvents))._tag, "Opened")
      assert.strictEqual((yield* Queue.take(nextEvents))._tag, "StoredMessage")
      trackLateMonitors = true
      const staleOpen = yield* Stream.runHead(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(handler, effect),
        Effect.forkScoped
      )
      yield* Deferred.await(releaseStarted)
      assert.strictEqual(trackedAuthorizations, 2)
      trackLateMonitors = false
      yield* Stream.runHead(
        handler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>
      ).pipe((effect) => relayHandlerEffect(handler, effect))
      yield* Deferred.succeed(releaseGate, undefined)
      yield* Fiber.join(staleOpen)
      yield* Effect.forEach(
        Array.from({ length: 100 }),
        () => Effect.yieldNow,
        { discard: true }
      )
      yield* Deferred.succeed(lateMonitorSignal, undefined)
      yield* Effect.forEach(
        Array.from({ length: 100 }),
        () => Effect.yieldNow,
        { discard: true }
      )
      assert.strictEqual(yield* Ref.get(lateMonitors), 0)
      yield* Fiber.interrupt(nextIncumbent)

      yield* runtime.shutdown
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayWorkers())).value, 0)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayReadyQueueItems("New"))).value, 0)
      assert.strictEqual((yield* Metric.value(PeerRpcObservability.relayReadyQueueItems("Retry"))).value, 0)
    })).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map())
    ))

  it.effect("drains admitted Push after a session authorization monitor defects", () =>
    Effect.scoped(Effect.gen(function*() {
      const authorizationDefect = yield* Deferred.make<void>()
      const blockingFinalizer = yield* Deferred.make<void>()
      let authorizationCalls = 0
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          Effect.sync(() => {
            authorizationCalls++
            return {
              remote: {
                tenantId: request.principal.tenantId,
                subjectId: request.remote.subjectId,
                peerId: request.remote.peerId
              },
              documents: request.documents.map((document) => ({
                document: Task,
                documentId: document.documentId
              })),
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: authorizationCalls === 1
                ? Deferred.await(authorizationDefect)
                : authorizationCalls === 2
                ? Effect.never.pipe(
                  Effect.ensuring(Deferred.await(blockingFinalizer))
                )
                : Effect.never
            }
          }),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const admitStarted = yield* Deferred.make<void>()
      const allowAdmit = yield* Deferred.make<void>()
      const admitCalls = yield* Ref.make(0)
      const store = makeRelayStore({
        admit: (input) =>
          Deferred.succeed(admitStarted, undefined).pipe(
            Effect.andThen(Deferred.await(allowAdmit)),
            Effect.andThen(Ref.update(admitCalls, (count) => count + 1)),
            Effect.as({
              status: "Accepted",
              channel: input.channel,
              ready: false,
              nextEligibleAt: 0,
              lane: "New"
            })
          )
      })
      const context = yield* makeRelayHandlerContext(authorization, store)
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(
        PeerRpc.PushRpc.key
      ) as Rpc.Handler<"Push">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      const { payload } = yield* makeRelayPayload
      const pushFiber = yield* (
        pushHandler.handler({
          sessionId: (opened as PeerRpc.Opened).sessionId,
          relayMessageId: Identity.RelayMessageId.make(
            "rly_00000000-0000-4000-8000-000000000041"
          ),
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.exit,
        Effect.forkScoped
      )
      yield* Deferred.await(admitStarted)
      const defect = new Error("authorization invalidation defect")
      yield* Deferred.die(authorizationDefect, defect)
      yield* Effect.yieldNow
      assert.strictEqual((yield* runtime.usage).sessions, 0)
      yield* Deferred.succeed(allowAdmit, undefined)
      const pushExit = yield* Fiber.join(pushFiber)
      assert.isTrue(Exit.isSuccess(pushExit))
      assert.strictEqual(yield* Ref.get(admitCalls), 1)
      const later = yield* (
        pushHandler.handler({
          sessionId: (opened as PeerRpc.Opened).sessionId,
          relayMessageId: Identity.RelayMessageId.make(
            "rly_00000000-0000-4000-8000-000000000049"
          ),
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      ).pipe(
        (effect) => relayHandlerEffect(pushHandler, effect),
        Effect.flip
      )
      assert.instanceOf(later, PeerRpcError.SessionUnavailable)
      assert.strictEqual(yield* Ref.get(admitCalls), 1)
      yield* Effect.yieldNow
      assert.deepStrictEqual(yield* runtime.usage, {
        accepting: true,
        sessions: 0,
        subjects: 0,
        activeClaims: 0,
        queuedChannels: 0
      })
      yield* Deferred.succeed(blockingFinalizer, undefined)
      yield* Fiber.interrupt(openFiber)
    })))

  it.effect("completes queued and permit-waiting SQL submissions during shutdown", () =>
    Effect.scoped(Effect.gen(function*() {
      const authorization = PeerRelayAuthorization.PeerRelayAuthorization.of({
        authorize: (request) =>
          Effect.succeed({
            remote: {
              tenantId: request.principal.tenantId,
              subjectId: request.remote.subjectId,
              peerId: request.remote.peerId
            },
            documents: request.documents.map((document) => ({
              document: Task,
              documentId: document.documentId
            })),
            validUntil: Number.MAX_SAFE_INTEGER,
            invalidated: Effect.never
          }),
        authorizeUnsafeUnboundedAutomerge3Decode: authorizeUnsafeRelayDecode
      })
      const admitStarted = yield* Deferred.make<void>()
      const admitCalls = yield* Ref.make(0)
      const store = makeRelayStore({
        admit: (input) =>
          Ref.updateAndGet(admitCalls, (count) => count + 1).pipe(
            Effect.tap(() => Deferred.succeed(admitStarted, undefined)),
            Effect.andThen(Effect.never),
            Effect.as({
              status: "Accepted",
              channel: input.channel,
              ready: false,
              nextEligibleAt: 0,
              lane: "New"
            })
          )
      })
      const context = yield* makeRelayHandlerContext(
        authorization,
        store,
        PeerRelayLimits.Values.make({
          ...PeerRelayLimits.defaults,
          maxInFlightPush: 4,
          maxInFlightPushPerSubject: 4,
          admissionRatePerSecond: 1_000,
          admissionBurst: 100,
          maxInFlightSqlTransactions: 2,
          maxInFlightSqlAdmission: 1,
          sqlAdmissionQueueCapacity: 4
        })
      )
      const openHandler = context.mapUnsafe.get(
        PeerRpc.OpenRpc.key
      ) as Rpc.Handler<"Open">
      const pushHandler = context.mapUnsafe.get(
        PeerRpc.PushRpc.key
      ) as Rpc.Handler<"Push">
      const runtime = Context.get(context, PeerRpcServer.PeerRpcServerRuntime)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const openFiber = yield* Stream.runForEach(
        openHandler.handler(
          relayOpenRequest,
          {} as never
        ) as Stream.Stream<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>,
        (event) => Queue.offer(events, event)
      ).pipe(
        (effect) => relayHandlerEffect(openHandler, effect),
        Effect.forkScoped
      )
      const opened = yield* Queue.take(events)
      assert.strictEqual(opened._tag, "Opened")
      const sessionId = (opened as PeerRpc.Opened).sessionId
      const { payload } = yield* makeRelayPayload
      const invoke = (relayMessageId: Identity.RelayMessageId) =>
        pushHandler.handler({
          sessionId,
          relayMessageId,
          payload
        }, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>
      const calls = yield* Effect.forEach(
        [
          "rly_00000000-0000-4000-8000-000000000011",
          "rly_00000000-0000-4000-8000-000000000012",
          "rly_00000000-0000-4000-8000-000000000013"
        ],
        (id) =>
          Effect.gen(function*() {
            const completed = yield* Deferred.make<Exit.Exit<void, PeerRpcError.PeerRpcError>>()
            const fiber = yield* relayHandlerEffect(
              pushHandler,
              invoke(Identity.RelayMessageId.make(id))
            ).pipe(
              Effect.exit,
              Effect.flatMap((exit) => Deferred.succeed(completed, exit)),
              Effect.forkScoped
            )
            return { completed, fiber }
          })
      )
      yield* Effect.yieldNow
      yield* Deferred.await(admitStarted)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(admitCalls), 1)
      yield* runtime.shutdown
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const completed = yield* Effect.forEach(calls, (call) => Deferred.poll(call.completed))
      yield* Effect.forEach(calls, (call) => Fiber.interrupt(call.fiber), {
        discard: true
      })
      assert.isTrue(completed.every(Option.isSome))
      yield* Fiber.interrupt(openFiber)
    })))
})
