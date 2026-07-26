import { assert, describe, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import type * as DocumentEntity from "@lucas-barake/effect-local-sql/DocumentEntity"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
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
import * as Redacted from "effect/Redacted"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Sharding from "effect/unstable/cluster/Sharding"
import type * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import { createHash, randomBytes } from "node:crypto"
import { vi } from "vitest"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerAuthorization from "../src/PeerAuthorization.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"

/**
 * The stand-in for `PeerSession.makeSupervised`, which is the only export this file replaces.
 *
 * It runs in the very context production hands the real one at `PeerRpcServer.ts:978`: the ambient
 * `Scope` is `entry.scope` and the ambient `PeerTransport` is `sessionTransport(entry)`. So the
 * `Connection` it publishes to the test is the production connection built at
 * `PeerRpcServer.ts:695-701`, including the `close` under test at `:700`.
 */
type SessionStandIn = (
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
  }
) => Effect.Effect<
  PeerSession.SupervisedPeerSession,
  ReplicaError.ReplicaError,
  Scope.Scope | PeerTransport.PeerTransport | ReplicaGate.ReplicaGate
>

const sessionMock = vi.hoisted(() => ({
  /**
   * Incremented inside the replacement `makeSupervised`, never in the factory. Only
   * `PeerRpcServer.ts:978` calls `makeSupervised` in this suite, so a non-zero count is evidence
   * that the specifier imported at `PeerRpcServer.ts:2` was the one `vi.mock` intercepted. A
   * factory that merely runs would only prove this test file's own import was rewritten.
   */
  intercepted: 0,
  standIn: undefined as SessionStandIn | undefined
}))

vi.mock("@lucas-barake/effect-local-sql/PeerSession", async (importActual) => {
  const actual = await importActual<typeof PeerSession>()
  const makeSupervised: typeof actual.makeSupervised = (options) => {
    sessionMock.intercepted += 1
    const standIn = sessionMock.standIn
    return standIn === undefined
      ? Effect.die(new Error("PeerSession.makeSupervised stand-in was not installed"))
      : standIn(options)
  }
  // Every other export, notably `maximumSyncEnvelopeBytes`, stays the real one: production reads it
  // at `PeerRpcServer.ts:604` and `:1021`, and `PeerRpcLimits.ts:73` validates against it.
  return { ...actual, makeSupervised }
})

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const taskId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerA = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const definition = ReplicaDefinition.make({
  name: "peer-rpc-server-session-transport-test",
  documents: DocumentSet.make(Task),
  mutations: [],
  projections: [],
  queries: []
})
const definitionHash = definition.hash
const selected = [{ documentType: Task.name, documentId: taskId }]
const permit = {
  replicaId: Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001"),
  incarnation: Identity.ReplicaIncarnation.make(1),
  writerGeneration: Identity.WriterGeneration.make(1)
}
// Production values, built through the real constructors, so the bounds `PeerRpcLimits.ts:80-83`
// validates are the ones the inbound quota accounting depends on.
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
  maxQueuedRpc: 32,
  maxQueuedPermits: 32,
  maxActiveRestores: 32,
  maxRestoresPerSession: 1,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
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
const principal = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "subject-a",
  peerId: remotePeerA
})

const makeHarness = (options?: {
  readonly rpcLimits?: Partial<PeerRpcLimits.Values>
  /**
   * Runs inside the stand-in's element handler, after the payload has been recorded and `received`
   * signalled, so a test can hold the handler open exactly the way
   * `RpcPeerTransport.test.ts:707-711` does.
   */
  readonly onPayload?: (payload: Uint8Array) => Effect.Effect<void>
  /** Drives `close` before the consumer's very first pull. */
  readonly closeBeforeConsumer?: boolean
}) =>
  Effect.gen(function*() {
    const configuredRpcLimits = PeerRpcLimits.Values.make({ ...rpcLimits, ...options?.rpcLimits })
    const connectionReady = yield* Deferred.make<PeerTransport.Connection>()
    const consumerReady = yield* Deferred.make<Fiber.Fiber<void, ReplicaError.ReplicaError>>()
    const delivered = yield* Queue.unbounded<Uint8Array>()
    const received = yield* Deferred.make<void>()
    // Test owned, so a test can fail it on demand. Criterion 11 requires `close` never to be the
    // sole terminator: the real monitor at `PeerRpcServer.ts:984-995` only revokes once this
    // resolves.
    const disconnect = yield* Deferred.make<never, ReplicaError.ReplicaError>()

    const standIn: SessionStandIn = (standInOptions) =>
      Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const claimed = yield* gate.shared
        const transport = yield* PeerTransport.PeerTransport
        const connection = yield* transport.connect({
          replicaId: claimed.replicaId,
          peerId: standInOptions.peerId
        })
        yield* Deferred.succeed(connectionReady, connection)
        if (options?.closeBeforeConsumer === true) yield* connection.close
        // Exactly one consumer of `connection.receive`, for the whole suite. It closes over the
        // transport's mutable `currentInbound` and `started` (`PeerRpcServer.ts:579-580`), so a
        // second consumer would clobber the inbound reservation accounting.
        const consumer = yield* Stream.runForEach(connection.receive, (payload) =>
          Queue.offer(delivered, payload).pipe(
            Effect.andThen(Deferred.succeed(received, undefined)),
            Effect.andThen(options?.onPayload === undefined ? Effect.void : options.onPayload(payload))
          )).pipe(Effect.forkScoped)
        yield* Deferred.succeed(consumerReady, consumer)
        return {
          peerId: connection.peerId,
          connectionEpoch: "stand-in-epoch",
          markDirty: () => Effect.void,
          flush: Effect.void,
          observedByPeer: () => Effect.succeed(false),
          durableConfirmation: () => Effect.succeed(false as const),
          awaitDisconnect: Deferred.await(disconnect)
        }
      })
    yield* Effect.sync(() => void (sessionMock.standIn = standIn))
    yield* Effect.addFinalizer(() => Effect.sync(() => void (sessionMock.standIn = undefined)))

    const gate = ReplicaGate.ReplicaGate.of({
      current: Effect.succeed(permit),
      claiming: Effect.succeed(false),
      shared: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
      admit: Effect.acquireRelease(Effect.succeed(permit), () => Effect.void),
      claim: (use) => use(permit),
      refresh: Effect.succeed(permit),
      validate: () => Effect.void
    })
    const sync = PeerSync.PeerSync.of({
      withDocumentInvalidation: (_documentId, effect) => effect,
      invalidateDocument: () => Effect.void,
      open: (peerId) =>
        Effect.succeed({ peerId, connectionEpoch: "local-epoch", replicaIncarnation: permit.incarnation }),
      reset: () => Effect.void,
      generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
      receive: () => Effect.die("unexpected direct PeerSync receive"),
      enqueue: (_session, reply) =>
        Effect.succeed({
          ...reply,
          sendSequence: 0,
          lineage: Identity.genesisLineage,
          writerProvenance: []
        }),
      pending: () => Effect.succeed([]),
      markSent: () => Effect.succeed(true)
    })
    const publisher = CommitPublisher.CommitPublisher.of({
      publishPending: Effect.succeed(0),
      invalidate: () => Effect.void,
      subscribe: Effect.succeed({
        watermark: Identity.CommitSequence.make(0),
        refreshGeneration: 0,
        events: Stream.never
      })
    })
    const sharding = Sharding.Sharding.of({
      ...({} as Sharding.Sharding["Service"]),
      makeClient: () =>
        Effect.succeed(() =>
          ({
            ApplySync: (_payload: typeof DocumentEntity.ApplySync.payloadSchema.Type) =>
              Effect.die("unexpected DocumentEntity ApplySync")
          }) as never
        )
    })
    const authorization = PeerAuthorization.PeerAuthorization.of({
      authorize: (request) =>
        Effect.succeed({
          documents: request.documents.map((requested) => ({
            document: Task,
            documentId: requested.documentId
          })),
          validUntil: Number.MAX_SAFE_INTEGER,
          // Nothing in this suite invalidates a lease, and the lease watcher
          // (`PeerRpcServer.ts:955-961`) only ever awaits this arm. `Effect.never` states that
          // directly instead of parking on a `Deferred` no test ever completes.
          invalidated: Effect.never
        })
    })
    const services = Layer.mergeAll(
      Layer.succeed(Crypto.Crypto, crypto),
      Layer.succeed(CommitPublisher.CommitPublisher, publisher),
      Layer.succeed(PeerSync.PeerSync, sync),
      Layer.succeed(ReplicaGate.ReplicaGate, gate),
      Layer.succeed(ReplicaLimits.ReplicaLimits, replicaLimits),
      Layer.succeed(Sharding.Sharding, sharding),
      Layer.succeed(PeerRpcLimits.PeerRpcLimits, configuredRpcLimits),
      Layer.succeed(PeerAuthorization.PeerAuthorization, authorization),
      Layer.succeed(Metric.FiberRuntimeMetrics, {
        recordFiberStart: () => {},
        recordFiberEnd: () => {}
      })
    )
    // The subject under test is built through the real Layer, at the real call site, never
    // hand-assembled: `sessionTransport(entry)` is only reachable from `layerHandlers`.
    const handlers = PeerRpcServer.layerHandlers({ tenantId: "tenant", peerId: serverPeerId, definition }).pipe(
      Layer.provide(services)
    )
    const handlerScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void))
    const handlerContext = yield* Layer.build(handlers).pipe(Effect.provideService(Scope.Scope, handlerScope))
    const pushHandler = handlerContext.mapUnsafe.get(PeerRpc.PushRpc.key) as Rpc.Handler<"Push">
    // The direct handler shape, for `Push` only: it is the observed production handler
    // (`PeerRpcServer.ts:1216` -> `:1125`), and it surfaces the typed `PeerRpcError` these tests
    // discriminate on rather than a serialized client failure.
    const directPush = (request: typeof PeerRpc.PushRpc.payloadSchema.Type) =>
      (pushHandler.handler(request, {} as never) as Effect.Effect<void, PeerRpcError.PeerRpcError>).pipe(
        Effect.provideContext(Context.add(
          pushHandler.context,
          PeerAuthentication.AuthenticatedPeer,
          {
            principal,
            validUntil: Number.MAX_SAFE_INTEGER,
            invalidated: Effect.never
          }
        ))
      )
    const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
      Effect.provide(handlerContext),
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: (value) =>
          Redacted.value(value) === "owner"
            ? Effect.succeed({
              principal,
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
            : Effect.fail(new PeerRpcError.AuthenticationFailure())
      }),
      Effect.provide(PeerAuthentication.layerClient),
      Effect.provideService(PeerCredentials.PeerCredentials, {
        get: Effect.sync(() => Redacted.make("owner"))
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, configuredRpcLimits)
    )
    const open = (documents: ReadonlyArray<PeerRpc.RequestedDocument>) =>
      Effect.gen(function*() {
        const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
        const fiber = yield* Stream.runForEach(
          client.Open({
            protocolVersion: PeerRpc.protocolVersion,
            expectedPeerId: serverPeerId,
            definitionHash,
            documents
          }),
          (event) => Queue.offer(events, event).pipe(Effect.asVoid)
        ).pipe(Effect.forkChild)
        const first = yield* Effect.raceFirst(
          Queue.take(events),
          Fiber.join(fiber).pipe(Effect.andThen(Effect.die("Open stream ended before Opened")))
        )
        if (first._tag !== "Opened") return yield* Effect.die("the first Open event was not Opened")
        return { opened: first, events, fiber }
      })
    return {
      connectionReady,
      consumerReady,
      delivered,
      received,
      disconnect,
      open,
      directPush,
      closeServer: Scope.close(handlerScope, Exit.void)
    }
  })

/**
 * Every await in this suite is guarded, so a regression fails with a named message well inside the
 * `it.live` budget instead of hanging until vitest kills the file. `assert.isTrue` does not narrow,
 * so the `Option` narrowing lives here once rather than at every call site.
 */
const within = <A, E, R,>(effect: Effect.Effect<A, E, R>, message: string) =>
  effect.pipe(
    Effect.timeoutOption("4 seconds"),
    Effect.map((result) => {
      assert.isTrue(Option.isSome(result), message)
      return Option.getOrThrow(result)
    })
  )

/** The captured production `Connection`, built at `PeerRpcServer.ts:695-701`. */
const attach = (harness: { readonly connectionReady: Deferred.Deferred<PeerTransport.Connection> }) =>
  within(Deferred.await(harness.connectionReady), "the stand-in never published the production connection")

const consumerOf = (harness: {
  readonly consumerReady: Deferred.Deferred<Fiber.Fiber<void, ReplicaError.ReplicaError>>
}) => within(Deferred.await(harness.consumerReady), "the stand-in never published its receive consumer")

/**
 * Criterion 2: an interrupt-only `Exit`, not a normal end (which `PeerSession.ts:544-553` converts
 * into a retryable `StorageUnavailable`) and not a manufactured failure. The expanded form of this
 * check stays inline in the first test, which is where the distinction is established.
 */
const awaitInterrupted = (fiber: Fiber.Fiber<void, ReplicaError.ReplicaError>, message: string) =>
  within(Fiber.await(fiber), message).pipe(
    Effect.map((exit) =>
      assert.isTrue(
        Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause),
        "the receive consumer's Exit was not interrupt-only"
      )
    )
  )

/**
 * Suspended, not a hoisted constant: a `Metric` instance caches its metadata against the first
 * registry it resolves against (`node_modules/effect/src/Metric.ts:1762-1771`), so a shared
 * instance would keep reading the first test's registry once each test provides a fresh one.
 */
const inboundGauge = Effect.suspend(() => Metric.value(PeerRpcObservability.queueItems("Inbound"))).pipe(
  Effect.map((gauge) => gauge.value)
)

describe("PeerRpcServer sessionTransport", () => {
  it.live(
    "intercepts PeerSession.makeSupervised while every other export stays real",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)

        assert.isTrue(
          sessionMock.intercepted > 0,
          "vi.mock did not intercept @lucas-barake/effect-local-sql/PeerSession as imported by PeerRpcServer.ts:2"
        )

        assert.strictEqual(
          typeof PeerSession.maximumSyncEnvelopeBytes,
          "function",
          "importActual did not survive the partial mock"
        )
        const oversized = new Uint8Array(
          PeerSession.maximumSyncEnvelopeBytes(
            replicaLimits.maxSyncMessageBytes,
            replicaLimits.maxSyncChangesPerMessage
          ) + 1
        )
        const rejected = yield* harness.directPush({
          sessionId: session.opened.sessionId,
          payload: oversized
        }).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(rejected), "an oversized Push was not rejected")
        assert.strictEqual(
          PeerRpcObservability.failure(rejected)?._tag,
          "RequestLimitExceeded",
          "production did not read the real maximumSyncEnvelopeBytes"
        )

        assert.strictEqual(session.opened._tag, "Opened", "the client never observed Opened")
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "terminates a parked receive consumer when the server-hosted connection closes",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1, 2, 3) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        yield* Effect.yieldNow
        assert.isUndefined(receiving.pollUnsafe(), "the receive consumer terminated before close ran")

        const closing = yield* connection.close.pipe(Effect.forkChild)
        yield* within(Fiber.await(closing), "connection.close did not complete")

        // Deliberately expanded rather than folded into `awaitInterrupted`: this is the criterion-2
        // detector, and the two halves it separates - "did not end normally" and "was not a
        // manufactured failure" - are the whole reason `close` interrupts the race arm instead of
        // completing it.
        const exit = yield* within(Fiber.await(receiving), "close did not terminate the receive consumer")
        assert.isTrue(Exit.isFailure(exit), "the receive consumer ended normally instead of being interrupted")
        if (Exit.isFailure(exit)) {
          assert.isTrue(Cause.hasInterruptsOnly(exit.cause), "the receive consumer's Exit was not interrupt-only")
        }

        yield* within(harness.closeServer, "the server scope did not close")
        assert.strictEqual(
          yield* inboundGauge,
          0,
          "the delivered payload's inbound reservation was not reclaimed"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "completes every close call without suspending",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        // The one assertion that fails if `close` is ever made to await its own cleanup the way
        // `RpcPeerTransport.ts:124` does. Criterion 3: `close` runs on the receive fiber itself
        // (`PeerSession.ts:557`), so a suspending `close` would deadlock that fiber against itself.
        const closing = yield* connection.close.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        const first = closing.pollUnsafe()
        assert.isDefined(first, "close suspended instead of completing on the calling fiber")
        if (first === undefined) return yield* Effect.die("unreachable")
        assert.isTrue(Exit.isSuccess(first), "the first close call did not succeed")

        const second = yield* connection.close.pipe(Effect.exit)
        const third = yield* connection.close.pipe(Effect.exit)
        assert.isTrue(Exit.isSuccess(second), "the second sequential close call did not succeed")
        assert.isTrue(Exit.isSuccess(third), "the third sequential close call did not succeed")

        const concurrent = yield* Effect.all(
          [connection.close.pipe(Effect.exit), connection.close.pipe(Effect.exit)],
          { concurrency: "unbounded" }
        )
        assert.isTrue(concurrent.every(Exit.isSuccess), "a concurrent close call did not succeed")

        yield* awaitInterrupted(receiving, "close did not terminate the receive consumer")
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "does not interrupt an in-flight element handler, and terminates at the next pull",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const inHandler = yield* Deferred.make<void>()
        const releaseHandler = yield* Deferred.make<void>()
        const handlerInterrupted = yield* Ref.make(false)
        const harness = yield* makeHarness({
          onPayload: () =>
            Deferred.succeed(inHandler, undefined).pipe(
              Effect.andThen(Deferred.await(releaseHandler)),
              Effect.onInterrupt(() => Ref.set(handlerInterrupted, true))
            )
        })
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1, 2, 3) })
        yield* within(Deferred.await(inHandler), "the consumer never entered its element handler")

        // Criterion 6: the reservation for item N is held for the whole of item N's handler. Under
        // the rejected `Stream.interruptWhen` design the producer would already have re-entered the
        // pull and run `releaseInbound` (`PeerRpcServer.ts:585`), so this would read 0. It is also
        // the gate the `:1051` item-capacity check reads, which is why no separate test is needed
        // to prove a concurrent Push is rejected while this reservation is held.
        assert.strictEqual(
          yield* inboundGauge,
          1,
          "the inbound reservation was released while the element handler was still in flight"
        )

        yield* connection.close
        yield* Effect.yieldNow
        assert.isUndefined(receiving.pollUnsafe(), "close terminated the consumer inside its element handler")
        assert.isFalse(yield* Ref.get(handlerInterrupted), "close interrupted the in-flight element handler")

        yield* Deferred.succeed(releaseHandler, undefined)
        yield* awaitInterrupted(receiving, "the consumer did not terminate at its next pull")
        assert.isFalse(yield* Ref.get(handlerInterrupted), "the released element handler was interrupted after all")
        assert.strictEqual(
          yield* inboundGauge,
          0,
          "the next pull did not release the handled item's reservation before teardown"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "does not revoke, detach, or terminate the outbound stream when close runs",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        yield* connection.close

        // Offered at `PeerRpcServer.ts:665` and drained by `responseStream` at `:827`. A `detach`
        // would have failed `entry.terminal` at `:286`, which surfaces at `:825-827`.
        const outbound = Uint8Array.of(9, 8, 7)
        yield* connection.send(outbound)
        const message = yield* within(Queue.take(session.events), "the Open stream stopped delivering after close")
        assert.strictEqual(message._tag, "Message")
        if (message._tag === "Message") {
          assert.deepStrictEqual(message.payload, outbound)
        }

        yield* Effect.yieldNow
        assert.isUndefined(session.fiber.pollUnsafe(), "close terminated the client's Open stream")
        assert.strictEqual(
          (yield* Metric.value(PeerRpcObservability.activeSessions())).value,
          1,
          "close revoked the session"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "accepts the first post-close Push, then overloads on the next",
    () =>
      Effect.scoped(Effect.gen(function*() {
        // The default `inboundItemCapacity` of 1 (`PeerRpcLimits.ts:40`), so the second post-close
        // Push is the overflowing one. It is rejected by the item-capacity gate at
        // `PeerRpcServer.ts:1051-1052`, not by the `!offered` branch at `:1064`: raising only
        // `entry.inbound`'s own capacity (`:927-929`) leaves this test green, while raising only
        // the gate hands the same rejection to `:1064`. Both bounds are `inboundItemCapacity` and
        // every queued item holds a reservation, so no capacity separates them - the gate always
        // short-circuits first.
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        yield* connection.close
        // The consumer's exit is the barrier that its reservation is gone: released either by the
        // next pull (`PeerRpcServer.ts:585`) or by `Stream.ensuring` (`:597-599`).
        yield* within(Fiber.await(receiving), "close did not terminate the receive consumer")

        // The rejected `Queue.shutdown` design made `Queue.offer` return false here
        // (`Queue.ts:632-639`), which fabricates `SessionOverloaded` at `PeerRpcServer.ts:1065`.
        const accepted = yield* harness.directPush({
          sessionId: session.opened.sessionId,
          payload: Uint8Array.of(2)
        }).pipe(Effect.exit)
        assert.isTrue(Exit.isSuccess(accepted), "the first post-close Push was rejected")
        assert.strictEqual(
          (yield* Metric.value(PeerRpcObservability.boundary("Push", "Success"))).count,
          2,
          "the post-close Push was not recorded as a successful Push"
        )
        assert.strictEqual(
          (yield* Metric.value(PeerRpcObservability.boundary("Push", "Overloaded"))).count,
          0,
          "close reclassified a Push as Overloaded"
        )

        // Nothing consumes the accepted item now, so this one finds the only slot reserved and hits
        // the pre-existing gate at `PeerRpcServer.ts:1051`, detaching at `:1056`. Criterion 7
        // accepts this; the assertion pins it so a future change cannot silently widen it.
        const overflow = yield* harness.directPush({
          sessionId: session.opened.sessionId,
          payload: Uint8Array.of(3)
        }).pipe(Effect.exit)
        assert.strictEqual(
          PeerRpcObservability.failure(overflow)?._tag,
          "SessionOverloaded",
          "the overflowing post-close Push was not classified as SessionOverloaded"
        )
        assert.strictEqual(
          (yield* Metric.value(PeerRpcObservability.boundary("Push", "Overloaded"))).count,
          1,
          "the overflowing Push was not recorded as Overloaded"
        )

        yield* within(harness.closeServer, "the server scope did not close")
        assert.strictEqual(yield* inboundGauge, 0, "the post-close Push's reservation was not reclaimed")
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "emits Opened even when close precedes the first pull, and still terminates",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness({ closeBeforeConsumer: true })
        // Criterion 8: `inboundConsumerStarted` (`PeerRpcServer.ts:590`) is completed before the
        // race, so the `Opened` gate at `:808` still resolves when close won the very first pull.
        const session = yield* harness.open(selected)
        assert.strictEqual(session.opened._tag, "Opened", "the client never observed Opened")

        const receiving = yield* consumerOf(harness)
        yield* awaitInterrupted(receiving, "close before the first pull did not terminate the consumer")

        // Criterion 11: close is never the sole terminator. Nothing fails `entry.terminal` on its
        // own, so without this the Open stream would park forever at `PeerRpcServer.ts:825-828`.
        yield* Deferred.fail(
          harness.disconnect,
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause: new Error("peer session lost") })
          })
        )
        const ended = yield* within(Fiber.await(session.fiber), "the monitor never revoked the session")
        assert.strictEqual(
          PeerRpcObservability.failure(ended)?._tag,
          "ServerUnavailable",
          "the Open stream did not terminate with the classified PeerRpcError"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "does not take a queued item once close has been requested",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const inHandler = yield* Deferred.make<void>()
        const releaseHandler = yield* Deferred.make<void>()
        const harness = yield* makeHarness({
          rpcLimits: { inboundItemCapacity: 2 },
          onPayload: (payload) =>
            payload[0] === 1
              ? Deferred.succeed(inHandler, undefined).pipe(Effect.andThen(Deferred.await(releaseHandler)))
              : Effect.void
        })
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(inHandler), "the consumer never entered its element handler")
        // Lands in `entry.inbound` while the consumer is inside the handler for the first item, so
        // the next pull starts with an item already buffered and close already requested.
        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(2) })

        yield* connection.close
        yield* Deferred.succeed(releaseHandler, undefined)

        yield* awaitInterrupted(receiving, "close did not terminate the receive consumer")
        // `Effect.raceFirst` is `raceAllFirst([self, that])` (`internal/effect.ts:1611`), which forks
        // the arms in order and breaks at `internal/effect.ts:1515` as soon as one settles, and both
        // arms settle synchronously here. Signal first means the buffered item is never taken;
        // reversing the arms would drain the queue before observing close.
        assert.deepStrictEqual(
          yield* Queue.takeAll(harness.delivered),
          [Uint8Array.of(1)],
          "the pull took a buffered item after close was requested"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "terminates the consumer after many delivered messages",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const messageCount = 50
        const acknowledged: Array<Deferred.Deferred<void>> = []
        for (let index = 0; index < messageCount; index++) {
          acknowledged.push(yield* Deferred.make<void>())
        }
        // Two slots, so message N+1 can be admitted while the handler for message N still holds its
        // reservation. That is the only outstanding-reservation state this loop ever reaches, so
        // the sequence stays deterministic without a single sleep.
        const harness = yield* makeHarness({
          rpcLimits: { inboundItemCapacity: 2 },
          onPayload: (payload) => Deferred.succeed(acknowledged[payload[0]!]!, undefined).pipe(Effect.asVoid)
        })
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        // Each pull installs a fresh `Deferred.await(closeRequested)` arm in the race at
        // `PeerRpcServer.ts:592`. If a losing arm were left resumable across pulls, `close` would
        // become a no-op once the stale resume had been consumed, which is the original bug
        // reappearing only under load.
        for (let index = 0; index < messageCount; index++) {
          yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(index) })
          yield* within(Deferred.await(acknowledged[index]!), `message ${index} was never delivered`)
        }

        yield* connection.close
        yield* awaitInterrupted(receiving, `close did not terminate the consumer after ${messageCount} messages`)

        yield* within(harness.closeServer, "the server scope did not close")
        assert.strictEqual(
          yield* inboundGauge,
          0,
          "a delivered message's inbound reservation survived teardown"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "leaves the entry active and buffering when close runs with no other terminator",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const connection = yield* attach(harness)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        // `harness.disconnect` is left unresolved, so the monitor at `PeerRpcServer.ts:984-995`
        // never fires and `close` is the only thing that ran. Criterion 11 accepts exactly this
        // hazard, and this test is what pins it: the entry stays registered, the client keeps its
        // Open stream, and inbound work keeps being admitted with nothing left to consume it.
        yield* connection.close
        yield* within(Fiber.await(receiving), "close did not terminate the receive consumer")

        assert.strictEqual(
          (yield* Metric.value(PeerRpcObservability.activeSessions())).value,
          1,
          "close revoked the session even though nothing else terminated it"
        )
        yield* Effect.yieldNow
        assert.isUndefined(session.fiber.pollUnsafe(), "close failed the client's Open stream")

        const accepted = yield* harness.directPush({
          sessionId: session.opened.sessionId,
          payload: Uint8Array.of(2)
        }).pipe(Effect.exit)
        assert.isTrue(Exit.isSuccess(accepted), "the entry stopped admitting Pushes after close")
        assert.strictEqual(
          yield* inboundGauge,
          1,
          "the admitted Push was not buffered against the still-active entry"
        )
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )

  it.live(
    "leaves a classified receive failure intact when close never ran",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeHarness()
        const session = yield* harness.open(selected)
        const receiving = yield* consumerOf(harness)

        yield* harness.directPush({ sessionId: session.opened.sessionId, payload: Uint8Array.of(1) })
        yield* within(Deferred.await(harness.received), "the receive consumer never observed a delivered payload")

        // No `close` anywhere on this path. Real teardown runs `finishCleanup`, whose
        // `Queue.fail(entry.inbound, replicaFailure())` at `PeerRpcServer.ts:358` is what the
        // consumer must observe. D3 only holds because `close` cannot preempt it here.
        yield* within(harness.closeServer, "the server scope did not close")

        const exit = yield* within(Fiber.await(receiving), "teardown did not terminate the receive consumer")
        assert.isTrue(Exit.isFailure(exit), "teardown ended the receive consumer normally")
        if (Exit.isFailure(exit)) {
          assert.isFalse(
            Cause.hasInterruptsOnly(exit.cause),
            "the classified receive failure was replaced by an interrupt"
          )
          const error = Cause.findErrorOption(exit.cause)
          assert.isTrue(Option.isSome(error), "the consumer did not observe a ReplicaError")
          if (Option.isSome(error)) {
            assert.strictEqual(error.value.reason._tag, "StorageUnavailable")
          }
        }
      })).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
    30000
  )
})
