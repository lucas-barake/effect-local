import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import type * as PeerRelayOutbox from "@lucas-barake/effect-local-sql/PeerRelayOutbox"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
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
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Tracer from "effect/Tracer"
import { RpcClientDefect, RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerRelayRpc from "../src/PeerRelayRpc.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const TaskV2 = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 2 })
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const otherPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const otherSessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000002")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000005")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
const senderReplicaIncarnation = Identity.ReplicaIncarnation.make(1)
const claimToken = PeerRelayRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000001")
const relayMessageHash = "1".repeat(64)
const rewrittenLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000001")
const documents = [{ document: Task, documentId }]
const definition = ReplicaDefinition.make({
  name: "rpc-peer-transport-test",
  documents: DocumentSet.make(Task),
  mutations: [],
  projections: [],
  queries: []
})

const opened = (peerId: Identity.PeerId, openSessionId: Identity.SessionId) =>
  PeerRpc.Opened.make({
    _tag: "Opened",
    protocolVersion: PeerRpc.protocolVersion,
    sessionId: openSessionId,
    peerId,
    capabilities: { storeAndForward: false }
  })
const serverOpened = opened(serverPeerId, sessionId)

const makeClient = (
  open: (
    request: typeof PeerRpc.OpenRpc.payloadSchema.Type,
    options: { readonly streamBufferSize?: number | undefined }
  ) => Stream.Stream<PeerRpc.OpenEvent, unknown>,
  push: (request: typeof PeerRpc.PushRpc.payloadSchema.Type) => Effect.Effect<void, unknown>
): PeerRpc.RpcClient => ({ Open: open, Push: push }) as never

const connect = (client: PeerRpc.RpcClient, peerId: Identity.PeerId) =>
  Effect.gen(function*() {
    const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents, definition }))
    return yield* Context.get(context, PeerTransport.PeerTransport).connect({ replicaId, peerId })
  })

const liveOpen = (event: PeerRpc.OpenEvent) => Stream.concat(Stream.make(event), Stream.never)

const openWithMessage = (payload: Uint8Array) =>
  Stream.fromIterable([serverOpened, PeerRpc.Message.make({ _tag: "Message", payload })]).pipe(Stream.rechunk(1))

const relayOptions: RpcPeerTransport.StoreAndForwardOptions = {
  expectedLocal: {
    tenantId: "tenant",
    subjectId: "local-subject",
    peerId: localPeerId
  },
  senderReplicaIncarnation,
  expectedRelayPeerId: relayPeerId,
  remote: {
    subjectId: "remote-subject",
    peerId: remotePeerId
  },
  documents,
  definition,
  receiptRetentionMillis: 8 * 24 * 60 * 60 * 1_000,
  senderRetryHorizonMillis: 7 * 24 * 60 * 60 * 1_000,
  replayBatchSize: 16
}

const relayOpened = PeerRelayRpc.RelayOpened.make({
  _tag: "RelayOpened",
  version: PeerRelayRpc.protocolVersion,
  sessionId,
  remotePeerId,
  authenticatedLocal: relayOptions.expectedLocal,
  capabilities: { storeAndForward: true }
})

const makeRelayClient = (
  open: (
    request: typeof PeerRelayRpc.OpenRelayRpc.payloadSchema.Type,
    options: { readonly streamBufferSize?: number | undefined }
  ) => Stream.Stream<PeerRelayRpc.OpenRelayEvent, unknown>,
  push: (
    request: typeof PeerRelayRpc.PushRelayRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void,
  acknowledge: (
    request: typeof PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void,
  reject: (
    request: typeof PeerRelayRpc.RejectRelayRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void
): PeerRelayRpc.RpcClient =>
  ({
    OpenRelay: open,
    PushRelay: push,
    AcknowledgeRelay: acknowledge,
    RejectRelay: reject
  }) as never

const relayEntry = (payload: Uint8Array): PeerRelayOutbox.Entry => ({
  rowId: 1,
  replicaId,
  replicaIncarnation: senderReplicaIncarnation,
  writerGeneration: Identity.WriterGeneration.make(1),
  expectedLocal: relayOptions.expectedLocal,
  remote: {
    tenantId: relayOptions.expectedLocal.tenantId,
    subjectId: relayOptions.remote.subjectId,
    peerId: relayOptions.remote.peerId
  },
  relayPeerId,
  relayMessageId,
  outerEnvelopeDigest: "2".repeat(64),
  protocolVersion: 3,
  payloadVersion: 1,
  senderConnectionEpoch: "sender-epoch",
  senderSequence: 0,
  document: { documentType: Task.name, documentId },
  writerProvenance: [],
  messageHash: relayMessageHash,
  payload,
  encodedSize: payload.byteLength,
  createdAt: "2026-07-25T00:00:00.000Z",
  retryDeadline: "2026-08-01T00:00:00.000Z",
  nextAttemptAt: "2026-07-25T00:00:00.000Z"
})

const makeStoredMessage = (
  payloadSeed: Uint8Array,
  lineage: Identity.DocumentLineage = Identity.genesisLineage
) =>
  Effect.gen(function*() {
    let source = Automerge.from(
      { value: [...payloadSeed] },
      { actor: "a".repeat(32) }
    )
    const remote = Automerge.init()
    const handshake = Automerge.generateSyncMessage(
      remote,
      Automerge.initSyncState()
    )[1]!
    const received = Automerge.receiveSyncMessage(
      source,
      Automerge.initSyncState(),
      handshake
    )
    source = received[0]
    const message = Automerge.generateSyncMessage(source, received[1])[1]!
    const writerProvenance = Automerge.getAllChanges(source).map((bytes) => {
      const change = Automerge.decodeChange(bytes)
      return {
        changeHash: change.hash,
        writerSchemaVersion: 1,
        writerDefinitionHash: definition.hash
      }
    })
    Automerge.free(source)
    Automerge.free(remote)
    const messageHash = yield* Canonical.digest(message)
    const payload = yield* PeerSyncEnvelope.encodeSyncEnvelope({
      connectionEpoch: "remote-epoch",
      sequence: 3,
      documentId,
      documentType: Task.name,
      messageHash,
      message,
      lineage,
      writerProvenance
    })
    const envelope: PeerSyncEnvelope.RelayOuterEnvelope = {
      domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
      version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
      expectedLocal: {
        tenantId: relayOptions.expectedLocal.tenantId,
        subjectId: relayOptions.remote.subjectId,
        peerId: relayOptions.remote.peerId
      },
      remote: relayOptions.expectedLocal,
      relayPeerId,
      relayMessageId,
      protocolVersion: PeerRelayRpc.protocolVersion,
      payloadVersion: PeerSyncEnvelope.syncEnvelopeVersion,
      senderReplicaIncarnation,
      senderConnectionEpoch: "remote-epoch",
      senderSequence: 3,
      document: { documentType: Task.name, documentId },
      lineage,
      writerProvenance,
      messageHash,
      payload
    }
    const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(envelope)
    return PeerRelayRpc.StoredMessage.make({
      _tag: "StoredMessage",
      relayMessageId,
      claimToken,
      relayPeerId,
      sender: {
        tenantId: envelope.expectedLocal.tenantId,
        subjectId: envelope.expectedLocal.subjectId,
        peerId: envelope.expectedLocal.peerId,
        replicaIncarnation: envelope.senderReplicaIncarnation,
        connectionEpoch: envelope.senderConnectionEpoch,
        sequence: envelope.senderSequence
      },
      recipient: envelope.remote,
      payloadVersion: envelope.payloadVersion,
      document: envelope.document,
      writerProvenance: envelope.writerProvenance,
      messageHash: envelope.messageHash,
      outerEnvelopeDigest,
      payload
    })
  })

interface RuntimeOverrides {
  readonly admit?: (
    input: PeerRelayOutbox.AdmitInput
  ) => Effect.Effect<PeerRelayOutbox.Entry, ReplicaError.ReplicaError>
  readonly dueForEndpoint?: (
    input: PeerRelayOutbox.ReplayInput
  ) => Effect.Effect<ReadonlyArray<PeerRelayOutbox.Entry>, ReplicaError.ReplicaError>
  readonly maximumPendingHorizon?: (
    endpoint: PeerRelayOutbox.Endpoint
  ) => Effect.Effect<number | null, ReplicaError.ReplicaError>
  readonly markCustody?: (
    input: PeerRelayOutbox.CustodyInput
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly validateConnectionConfiguration?: (
    input: {
      readonly replicaIncarnation: Identity.ReplicaIncarnation
      readonly retryHorizonMillis: number
      readonly replayBatchSize: number
    }
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly signalReceiptPrune?: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly health?: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly awaitFatal?: Effect.Effect<never, ReplicaError.ReplicaError>
}

const makeRuntime = (overrides: RuntimeOverrides = {}) =>
  PeerRelayClientRuntime.PeerRelayClientRuntime.of({
    admit: overrides.admit ?? ((input) => Effect.succeed(relayEntry(input.payload))),
    dueForEndpoint: overrides.dueForEndpoint ?? (() => Effect.succeed([])),
    maximumPendingHorizon: overrides.maximumPendingHorizon ?? (() => Effect.succeed(null)),
    markCustody: overrides.markCustody ?? (() => Effect.void),
    validateReplicaIncarnation: () => Effect.void,
    validateConnectionConfiguration: overrides.validateConnectionConfiguration ?? (() => Effect.void),
    signalReceiptPrune: overrides.signalReceiptPrune ?? Effect.void,
    health: overrides.health ?? Effect.void,
    awaitFatal: overrides.awaitFatal ?? Effect.never
  })

const connectRelay = (
  client: PeerRelayRpc.RpcClient,
  runtime: ReturnType<typeof makeRuntime>,
  peerId: Identity.PeerId = remotePeerId
) =>
  Effect.gen(function*() {
    const context = yield* Layer.build(
      RpcPeerTransport.layerStoreAndForward(client, relayOptions).pipe(
        Layer.provide(Layer.merge(
          Layer.succeed(PeerRelayClientRuntime.PeerRelayClientRuntime, runtime),
          Layer.merge(
            NodeCrypto.layer,
            ReplicaLimits.layer(TestReplica.defaultLimits)
          )
        ))
      )
    )
    return yield* Context.get(context, PeerTransport.PeerTransport).connect({
      replicaId,
      peerId
    })
  })

describe("RpcPeerTransport", () => {
  it.effect("validates the first streamed event as the handshake and uses response capacity one", () =>
    Effect.scoped(Effect.gen(function*() {
      const payload = Uint8Array.of(1, 2, 3)
      const client = makeClient(
        (request, options) => {
          assert.strictEqual(options.streamBufferSize, 1)
          assert.strictEqual(request.expectedPeerId, serverPeerId)
          assert.deepStrictEqual(request.documents, [{ documentType: Task.name, documentId }])
          return openWithMessage(payload)
        },
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      assert.strictEqual(connection.peerId, serverPeerId)
      assert.deepStrictEqual(yield* Stream.runCollect(connection.receive), [payload])
      yield* connection.close
    })))

  it.effect("rejects selected documents that do not belong to the supplied definition", () =>
    Effect.scoped(Effect.gen(function*() {
      const opens = yield* Ref.make(0)
      const client = makeClient(
        () => Ref.update(opens, (count) => count + 1).pipe(Effect.as(liveOpen(serverOpened)), Stream.unwrap),
        () => Effect.void
      )
      const context = yield* Layer.build(RpcPeerTransport.layer(client, {
        documents: [{ document: TaskV2, documentId }],
        definition
      }))
      const exit = yield* Context.get(context, PeerTransport.PeerTransport).connect({
        replicaId,
        peerId: serverPeerId
      }).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const error = Cause.findErrorOption(exit.cause)
        assert.isTrue(Option.isSome(error) && error.value.reason._tag === "ProtocolMismatch")
      }
      assert.strictEqual(yield* Ref.get(opens), 0)
    })))

  it.effect("records adapter boundaries without peer session document or payload values", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    return Effect.scoped(Effect.gen(function*() {
      const client = makeClient(
        () => liveOpen(serverOpened),
        () => Effect.logDebug("adapter-log-forbidden-value")
      )
      const connection = yield* connect(client, serverPeerId)
      yield* connection.send(Uint8Array.of(201, 202, 203))
      yield* connection.close
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("AdapterOpen", "Success"))).count,
        1
      )
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("AdapterPush", "Success"))).count,
        1
      )
      const safeSpans = spans.filter((span) => span.name.startsWith("effect_local_rpc.adapter."))
      assert.deepStrictEqual(
        new Set(safeSpans.map((span) => span.name)),
        new Set(["effect_local_rpc.adapter.open", "effect_local_rpc.adapter.push"])
      )
      for (const span of safeSpans) {
        assert.strictEqual(span.status._tag, "Ended")
        if (span.status._tag === "Ended") assert.isTrue(Exit.isSuccess(span.status.exit))
        assert.deepStrictEqual(span.events, [])
      }
      const telemetry = JSON.stringify(safeSpans.map((span) => ({
        name: span.name,
        attributes: [...span.attributes]
      }))) + (yield* Metric.dump)
      for (const forbidden of [serverPeerId, sessionId, documentId, "201,202,203", "adapter-log-forbidden-value"]) {
        assert.notInclude(telemetry, forbidden)
      }
    })).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })

  it.effect("rejects Message before Opened and closes the child scope", () =>
    Effect.scoped(Effect.gen(function*() {
      const finalized = yield* Ref.make(0)
      const client = makeClient(
        () =>
          Stream.make(PeerRpc.Message.make({ _tag: "Message", payload: Uint8Array.of(1) })).pipe(
            Stream.ensuring(Ref.update(finalized, (count) => count + 1))
          ),
        () => Effect.void
      )
      const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.strictEqual(yield* Ref.get(finalized), 1)
    })))

  it.effect("rejects an Open stream that ends before the handshake", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(() => Stream.empty, () => Effect.void)
      const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    })))

  it.effect("rejects a mismatched server peer identity", () =>
    Effect.scoped(Effect.gen(function*() {
      const finalized = yield* Ref.make(0)
      const client = makeClient(
        () =>
          liveOpen(opened(otherPeerId, sessionId)).pipe(
            Stream.ensuring(Ref.update(finalized, (count) => count + 1))
          ),
        () => Effect.void
      )
      const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.expected, serverPeerId)
        assert.strictEqual(error.reason.observed, otherPeerId)
      }
      assert.strictEqual(yield* Ref.get(finalized), 1)
    })))

  it.effect("rejects another Opened event after the handshake", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(
        () => Stream.fromIterable([serverOpened, opened(serverPeerId, otherSessionId)]).pipe(Stream.rechunk(1)),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      const error = yield* Stream.runDrain(connection.receive).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      yield* connection.close
    })))

  it.effect("serializes concurrent send calls", () =>
    Effect.scoped(Effect.gen(function*() {
      const starts = yield* Queue.unbounded<number>()
      const release = yield* Deferred.make<void>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        (request) =>
          Queue.offer(starts, request.payload[0]).pipe(
            Effect.andThen(request.payload[0] === 1 ? Deferred.await(release) : Effect.void)
          )
      )
      const connection = yield* connect(client, serverPeerId)
      const first = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      assert.strictEqual(yield* Queue.take(starts), 1)
      const second = yield* connection.send(Uint8Array.of(2)).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Queue.poll(starts)))
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(first)
      assert.strictEqual(yield* Queue.take(starts), 2)
      yield* Fiber.join(second)
      yield* connection.close
    })))

  it.effect("maps typed RPC failures to stable public ReplicaError values", () =>
    Effect.scoped(Effect.gen(function*() {
      const permanent = [
        new PeerRpcError.AuthenticationFailure(),
        new PeerRpcError.AccessDenied(),
        new PeerRpcError.UnsupportedVersion(),
        new PeerRpcError.PeerMismatch(),
        new PeerRpcError.InvalidRequest(),
        new PeerRpcError.DocumentLineageChanged()
      ]
      for (const rpcError of permanent) {
        const client = makeClient(() => Stream.fail(rpcError), () => Effect.void)
        const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        // The wire tag stays diagnosable without inventing the document identity it withholds.
        if (error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(error.reason.observed, rpcError._tag)
        }
        assert.isFalse(RpcPeerTransport.isRetryable(error))
      }
      const limited = makeClient(() => Stream.fail(new PeerRpcError.RequestLimitExceeded()), () => Effect.void)
      const limitError = yield* connect(limited, serverPeerId).pipe(Effect.flip)
      assert.strictEqual(limitError.reason._tag, "ProtocolMismatch")
      assert.isFalse(RpcPeerTransport.isRetryable(limitError))
    })))

  it.effect("ends receive with a retryable public error when the Open stream fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>()
      yield* Queue.offer(events, serverOpened)
      const client = makeClient(
        () => Stream.fromQueue(events),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      yield* Queue.fail(events, new PeerRpcError.ServerUnavailable())
      const receiveExit = yield* Effect.exit(Stream.runDrain(connection.receive))
      assert.isTrue(Exit.isFailure(receiveExit))
      if (Exit.isFailure(receiveExit)) {
        const error = Cause.findErrorOption(receiveExit.cause)
        assert.isTrue(Option.isSome(error))
        if (Option.isSome(error)) {
          assert.strictEqual(error.value.reason._tag, "StorageUnavailable")
          assert.isTrue(RpcPeerTransport.isRetryable(error.value))
        }
      }
    })))

  it.effect("interrupts the Open stream exactly once when closed", () =>
    Effect.scoped(Effect.gen(function*() {
      const finalized = yield* Ref.make(0)
      const client = makeClient(
        () => liveOpen(serverOpened).pipe(Stream.ensuring(Ref.update(finalized, (count) => count + 1))),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      yield* connection.close
      yield* connection.close
      assert.strictEqual(yield* Ref.get(finalized), 1)
    })))

  it.effect("does not send after close", () =>
    Effect.scoped(Effect.gen(function*() {
      const pushes = yield* Ref.make(0)
      const client = makeClient(
        () => liveOpen(serverOpened),
        () => Ref.update(pushes, (count) => count + 1)
      )
      const connection = yield* connect(client, serverPeerId)
      yield* connection.close
      const error = yield* connection.send(Uint8Array.of(1)).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.strictEqual(yield* Ref.get(pushes), 0)
    })))

  it.effect("does not send after the supplied ambient scope closes", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const pushes = yield* Ref.make(0)
      const client = makeClient(
        () => liveOpen(serverOpened),
        () => Ref.update(pushes, (count) => count + 1)
      )
      const connection = yield* connect(client, serverPeerId).pipe(Effect.provideService(Scope.Scope, ambient))
      yield* Scope.close(ambient, Exit.void)
      const error = yield* connection.send(Uint8Array.of(1)).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.strictEqual(yield* Ref.get(pushes), 0)
    }))

  it.effect("interrupts a pending Open when the supplied ambient scope closes", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const openStarted = yield* Deferred.make<void>()
      const openFinalized = yield* Ref.make(0)
      const client = makeClient(
        () =>
          Stream.fromEffect(
            Deferred.succeed(openStarted, undefined).pipe(
              Effect.andThen(Effect.never)
            )
          ).pipe(
            Stream.ensuring(Ref.update(openFinalized, (count) => count + 1))
          ),
        () => Effect.void
      )
      const connecting = yield* connect(client, serverPeerId).pipe(
        Effect.provideService(Scope.Scope, ambient),
        Effect.forkChild
      )
      yield* Deferred.await(openStarted)
      yield* Scope.close(ambient, Exit.void)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(connecting)))
      assert.strictEqual(yield* Ref.get(openFinalized), 1)
    }))

  it.effect("does not complete Open after the supplied ambient scope closes during request construction", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const pulled = yield* Ref.make(0)
      const client = makeClient(
        () => {
          Effect.runSync(Scope.close(ambient, Exit.void))
          return Stream.fromEffect(
            Ref.updateAndGet(pulled, (count) => count + 1).pipe(
              Effect.as(serverOpened),
              Effect.uninterruptible
            )
          )
        },
        () => Effect.void
      )
      const error = yield* connect(client, serverPeerId).pipe(
        Effect.provideService(Scope.Scope, ambient),
        Effect.flip
      )
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.strictEqual(yield* Ref.get(pulled), 0)
    }))

  it.effect("rejects a send racing supplied ambient scope cleanup", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const pushStarted = yield* Deferred.make<void>()
      const cleanupStarted = yield* Deferred.make<void>()
      const cleanupRelease = yield* Deferred.make<void>()
      const pushes = yield* Ref.make(0)
      const client = makeClient(
        () => liveOpen(serverOpened),
        () =>
          Ref.update(pushes, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(pushStarted, undefined)),
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(cleanupStarted, undefined).pipe(
                Effect.andThen(Deferred.await(cleanupRelease))
              )
            )
          )
      )
      const connection = yield* connect(client, serverPeerId).pipe(Effect.provideService(Scope.Scope, ambient))
      const sending = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      yield* Deferred.await(pushStarted)
      const closing = yield* Scope.close(ambient, Exit.void).pipe(Effect.forkChild)
      yield* Deferred.await(cleanupStarted)
      const error = yield* connection.send(Uint8Array.of(2)).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.strictEqual(yield* Ref.get(pushes), 1)
      yield* Deferred.succeed(cleanupRelease, undefined)
      yield* Fiber.join(closing)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(sending)))
    }))

  it.effect("joins explicit and ambient close races through one child cleanup", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const pushStarted = yield* Deferred.make<void>()
      const pushCleanupStarted = yield* Deferred.make<void>()
      const pushCleanupRelease = yield* Deferred.make<void>()
      const childFinalized = yield* Ref.make(0)
      const openFinalized = yield* Ref.make(0)
      const pushFinalized = yield* Ref.make(0)
      const pushes = yield* Ref.make(0)
      const client = makeClient(
        () =>
          Stream.unwrap(Effect.gen(function*() {
            const scope = yield* Scope.Scope
            yield* Scope.addFinalizer(scope, Ref.update(childFinalized, (count) => count + 1))
            return liveOpen(serverOpened).pipe(
              Stream.ensuring(Ref.update(openFinalized, (count) => count + 1))
            )
          })),
        () =>
          Ref.update(pushes, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(pushStarted, undefined)),
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(pushCleanupStarted, undefined).pipe(
                Effect.andThen(Deferred.await(pushCleanupRelease)),
                Effect.andThen(Ref.update(pushFinalized, (count) => count + 1))
              )
            )
          )
      )
      const connection = yield* connect(client, serverPeerId).pipe(Effect.provideService(Scope.Scope, ambient))
      const sending = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      yield* Deferred.await(pushStarted)
      const explicitClose = yield* connection.close.pipe(Effect.forkChild)
      yield* Deferred.await(pushCleanupStarted)
      const ambientClose = yield* Scope.close(ambient, Exit.void).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.isUndefined(explicitClose.pollUnsafe())
      assert.isUndefined(ambientClose.pollUnsafe())
      yield* Deferred.succeed(pushCleanupRelease, undefined)
      yield* Fiber.join(explicitClose)
      yield* Fiber.join(ambientClose)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(sending)))
      assert.strictEqual(yield* Ref.get(childFinalized), 1)
      assert.strictEqual(yield* Ref.get(openFinalized), 1)
      assert.strictEqual(yield* Ref.get(pushFinalized), 1)
      const error = yield* connection.send(Uint8Array.of(2)).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.strictEqual(yield* Ref.get(pushes), 1)
    }))

  it.effect("does not let a queued Push begin after close", () =>
    Effect.scoped(Effect.gen(function*() {
      const starts = yield* Queue.unbounded<number>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        (request) =>
          Queue.offer(starts, request.payload[0]).pipe(
            Effect.andThen(Effect.never)
          )
      )
      const connection = yield* connect(client, serverPeerId)
      const first = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      assert.strictEqual(yield* Queue.take(starts), 1)
      const second = yield* connection.send(Uint8Array.of(2)).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Queue.poll(starts)))
      yield* connection.close
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)))
      assert.isTrue(Exit.isFailure(yield* Fiber.await(second)))
      assert.isTrue(Option.isNone(yield* Queue.poll(starts)))
    })))

  it.effect("maps Push failures through the same stable public categories", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(
        () => liveOpen(serverOpened),
        () => Effect.fail(new PeerRpcError.SessionOverloaded())
      )
      const connection = yield* connect(client, serverPeerId)
      const error = yield* connection.send(Uint8Array.of(1)).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.isTrue(RpcPeerTransport.isRetryable(error))
      yield* connection.close
    })))

  it.effect("reports storeAndForward false and its own lineage awareness", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(() => liveOpen(serverOpened), () => Effect.void)
      const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents, definition }))
      const transport = Context.get(context, PeerTransport.PeerTransport)
      assert.isFalse(transport.capabilities.storeAndForward)
      assert.isTrue(transport.capabilities.lineageAware)
      const connection = yield* transport.connect({ replicaId, peerId: serverPeerId })
      assert.isFalse(connection.capabilities.storeAndForward)
      // The connection level value is the server's, never the adapter's. This stub `Opened` omits
      // the key the way a peer built before lineage would, and it must stay absent rather than
      // inherit the adapter's own true.
      assert.isUndefined(connection.capabilities.lineageAware)
      yield* connection.close
    })))

  it.effect("advertises its own lineage awareness on the Open it sends", () =>
    Effect.scoped(Effect.gen(function*() {
      const requests = yield* Queue.unbounded<typeof PeerRpc.OpenRpc.payloadSchema.Type>()
      const client = makeClient(
        (request) => Queue.offer(requests, request).pipe(Effect.as(liveOpen(serverOpened)), Stream.unwrap),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      // The server infers nothing from the protocol version, which lineage deliberately did not
      // bump. This claim is the only thing that separates this build from an older one that would
      // union a rewritten document and push the discarded history back.
      assert.deepStrictEqual((yield* Queue.take(requests)).capabilities, { lineageAware: true })
      yield* connection.close
    })))

  it.effect("passes a lineage aware server handshake through to the connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(
        () =>
          liveOpen(PeerRpc.Opened.make({
            _tag: "Opened",
            protocolVersion: PeerRpc.protocolVersion,
            sessionId,
            peerId: serverPeerId,
            capabilities: { storeAndForward: false, lineageAware: true }
          })),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      assert.isTrue(connection.capabilities.lineageAware)
      yield* connection.close
    })))

  it.effect("opens a fresh RPC session after reconnect", () =>
    Effect.scoped(Effect.gen(function*() {
      const opens = yield* Ref.make(0)
      const pushedSessions = yield* Queue.unbounded<Identity.SessionId>()
      const client = makeClient(
        () =>
          Ref.updateAndGet(opens, (count) => count + 1).pipe(
            Effect.map((count) => liveOpen(opened(serverPeerId, count === 1 ? sessionId : otherSessionId))),
            Stream.unwrap
          ),
        (request) => Queue.offer(pushedSessions, request.sessionId).pipe(Effect.asVoid)
      )
      const first = yield* connect(client, serverPeerId)
      yield* first.send(Uint8Array.of(1))
      yield* first.close
      const second = yield* connect(client, serverPeerId)
      yield* second.send(Uint8Array.of(2))
      yield* second.close
      assert.strictEqual(yield* Ref.get(opens), 2)
      assert.deepStrictEqual(yield* Queue.takeAll(pushedSessions), [sessionId, otherSessionId])
    })))

  it.effect("closes only the Open child scope and preserves ambient resources", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const ambientClosed = yield* Ref.make(false)
      yield* Scope.addFinalizer(ambient, Ref.set(ambientClosed, true))
      const client = makeClient(() => liveOpen(serverOpened), () => Effect.void)
      const connection = yield* connect(client, serverPeerId).pipe(Effect.provideService(Scope.Scope, ambient))
      yield* connection.close
      assert.isFalse(yield* Ref.get(ambientClosed))
      yield* Scope.close(ambient, Exit.void)
      assert.isTrue(yield* Ref.get(ambientClosed))
    }))

  it.effect("interrupts and joins a Deferred blocked Push when close runs", () =>
    Effect.scoped(Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const stopped = yield* Deferred.make<void>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(stopped, undefined))
          )
      )
      const connection = yield* connect(client, serverPeerId)
      const sending = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* connection.close
      yield* Deferred.await(stopped)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(sending)))
    })))

  it.effect("makes concurrent close callers await the same Push cleanup", () =>
    Effect.scoped(Effect.gen(function*() {
      const pushStarted = yield* Deferred.make<void>()
      const cleanupStarted = yield* Deferred.make<void>()
      const cleanupRelease = yield* Deferred.make<void>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        () =>
          Deferred.succeed(pushStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(cleanupStarted, undefined).pipe(
                Effect.andThen(Deferred.await(cleanupRelease))
              )
            )
          )
      )
      const connection = yield* connect(client, serverPeerId)
      const sending = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      yield* Deferred.await(pushStarted)
      const firstClose = yield* connection.close.pipe(Effect.forkChild)
      yield* Deferred.await(cleanupStarted)
      const secondClose = yield* connection.close.pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.isUndefined(secondClose.pollUnsafe())
      yield* Deferred.succeed(cleanupRelease, undefined)
      yield* Fiber.join(firstClose)
      yield* Fiber.join(secondClose)
      assert.isTrue(Exit.isFailure(yield* Fiber.await(sending)))
    })))

  it.live("interrupts a parked receive consumer when close runs", () =>
    Effect.scoped(Effect.gen(function*() {
      const payload = Uint8Array.of(1, 2, 3)
      const received = yield* Deferred.make<void>()
      const client = makeClient(() => openWithMessage(payload).pipe(Stream.concat(Stream.never)), () => Effect.void)
      const connection = yield* connect(client, serverPeerId)
      const receiving = yield* Stream.runForEach(
        connection.receive,
        () => Deferred.succeed(received, undefined)
      ).pipe(Effect.forkChild)

      const arrived = yield* Deferred.await(received).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(arrived), "consumer never received a Message")

      const closing = yield* connection.close.pipe(Effect.forkChild)
      const closed = yield* Fiber.await(closing).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(closed), "connection.close did not complete")

      const exit = yield* Fiber.await(receiving).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(exit), "close did not terminate the receive consumer")
      if (Option.isSome(exit)) {
        assert.isTrue(Exit.isFailure(exit.value))
        if (Exit.isFailure(exit.value)) {
          assert.isTrue(Cause.hasInterruptsOnly(exit.value.cause))
        }
      }

      const late = yield* Stream.runDrain(connection.receive).pipe(Effect.forkChild)
      const lateExit = yield* Fiber.await(late).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(lateExit), "receive did not terminate for a consumer started after close")
      if (Option.isSome(lateExit)) {
        assert.isTrue(Exit.isFailure(lateExit.value), "receive ended normally for a consumer started after close")
        if (Exit.isFailure(lateExit.value)) {
          assert.isTrue(Cause.hasInterruptsOnly(lateExit.value.cause))
        }
      }
    })), 30000)

  it.live(
    "finishes an in-flight receive handler and terminates at the next pull when close runs",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const first = Uint8Array.of(1)
        const second = Uint8Array.of(2)
        const delivered = yield* Queue.unbounded<number>()
        const inHandler = yield* Deferred.make<void>()
        const releaseHandler = yield* Deferred.make<void>()
        const accepted = yield* Deferred.make<void>()
        const handlerInterrupted = yield* Ref.make(false)
        const client = makeClient(
          () =>
            Stream.fromIterable([
              serverOpened,
              PeerRpc.Message.make({ _tag: "Message", payload: first }),
              PeerRpc.Message.make({ _tag: "Message", payload: second })
            ]).pipe(
              Stream.rechunk(1),
              Stream.tap((event) =>
                event._tag === "Message" && event.payload === second
                  ? Deferred.succeed(accepted, undefined)
                  : Effect.void
              ),
              Stream.concat(Stream.never)
            ),
          () => Effect.void
        )
        const connection = yield* connect(client, serverPeerId)
        const receiving = yield* Stream.runForEach(
          connection.receive,
          (payload) =>
            Queue.offer(delivered, payload[0]!).pipe(
              Effect.andThen(
                payload[0] === 1
                  ? Deferred.succeed(inHandler, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseHandler)),
                    Effect.onInterrupt(() => Ref.set(handlerInterrupted, true))
                  )
                  : Effect.void
              )
            )
        ).pipe(Effect.forkChild)

        const entered = yield* Deferred.await(inHandler).pipe(Effect.timeoutOption("4 seconds"))
        assert.isTrue(Option.isSome(entered), "consumer never entered its element handler")
        const pulled = yield* Deferred.await(accepted).pipe(Effect.timeoutOption("4 seconds"))
        assert.isTrue(Option.isSome(pulled), "the transport never accepted the second payload")
        yield* Effect.yieldNow

        yield* connection.close
        assert.isUndefined(receiving.pollUnsafe(), "close terminated the consumer inside its element handler")
        assert.isFalse(yield* Ref.get(handlerInterrupted), "close interrupted the in-flight element handler")

        yield* Deferred.succeed(releaseHandler, undefined)
        const exit = yield* Fiber.await(receiving).pipe(Effect.timeoutOption("4 seconds"))
        assert.isTrue(Option.isSome(exit), "consumer did not terminate at its next pull")
        if (Option.isSome(exit)) {
          assert.isTrue(Exit.isFailure(exit.value))
          if (Exit.isFailure(exit.value)) {
            assert.isTrue(Cause.hasInterruptsOnly(exit.value.cause))
          }
        }
        // Channel.merge hands the consumer a rendezvous queue. Whether the payload the transport
        // already pulled is still handed off depends on whether its Queue.offer registered before
        // the close trigger failed that queue, so both [1] and [1, 2] are legal deliveries.
        const payloads = yield* Queue.takeAll(delivered)
        assert.deepStrictEqual(payloads, [1, 2].slice(0, payloads.length))
        assert.strictEqual(payloads[0], 1)
      })),
    30000
  )

  it.live(
    "makes concurrent close callers interrupt one parked receive consumer",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const received = yield* Deferred.make<void>()
        const pushStarted = yield* Deferred.make<void>()
        const cleanupStarted = yield* Deferred.make<void>()
        const cleanupRelease = yield* Deferred.make<void>()
        const finalized = yield* Ref.make(0)
        const client = makeClient(
          () =>
            openWithMessage(Uint8Array.of(1, 2, 3)).pipe(
              Stream.concat(Stream.never),
              Stream.ensuring(Ref.update(finalized, (count) => count + 1))
            ),
          () =>
            Deferred.succeed(pushStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(cleanupStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(cleanupRelease))
                )
              )
            )
        )
        const connection = yield* connect(client, serverPeerId)
        const receiving = yield* Stream.runForEach(
          connection.receive,
          () => Deferred.succeed(received, undefined)
        ).pipe(Effect.forkChild)
        const arrived = yield* Deferred.await(received).pipe(Effect.timeoutOption("4 seconds"))
        assert.isTrue(Option.isSome(arrived), "consumer never received a Message")

        const sending = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
        yield* Deferred.await(pushStarted)
        const firstClose = yield* connection.close.pipe(Effect.forkChild)
        yield* Deferred.await(cleanupStarted)
        const secondClose = yield* connection.close.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        assert.isUndefined(secondClose.pollUnsafe())

        yield* Deferred.succeed(cleanupRelease, undefined)
        yield* Fiber.join(firstClose)
        yield* Fiber.join(secondClose)

        const exit = yield* Fiber.await(receiving).pipe(Effect.timeoutOption("4 seconds"))
        assert.isTrue(Option.isSome(exit), "concurrent close did not terminate the receive consumer")
        if (Option.isSome(exit)) {
          assert.isTrue(Exit.isFailure(exit.value))
          if (Exit.isFailure(exit.value)) {
            assert.isTrue(Cause.hasInterruptsOnly(exit.value.cause))
          }
        }
        assert.isTrue(Exit.isFailure(yield* Fiber.await(sending)))
        assert.strictEqual(yield* Ref.get(finalized), 1)
      })),
    30000
  )

  it.live("interrupts a parked receive consumer when the supplied ambient scope closes", () =>
    Effect.gen(function*() {
      const ambient = yield* Scope.make()
      const received = yield* Deferred.make<void>()
      const client = makeClient(
        () => openWithMessage(Uint8Array.of(1, 2, 3)).pipe(Stream.concat(Stream.never)),
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId).pipe(Effect.provideService(Scope.Scope, ambient))
      const receiving = yield* Stream.runForEach(
        connection.receive,
        () => Deferred.succeed(received, undefined)
      ).pipe(Effect.forkChild)
      const arrived = yield* Deferred.await(received).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(arrived), "consumer never received a Message")

      yield* Scope.close(ambient, Exit.void)

      const exit = yield* Fiber.await(receiving).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(exit), "ambient scope close did not terminate the receive consumer")
      if (Option.isSome(exit)) {
        assert.isTrue(Exit.isFailure(exit.value))
        if (Exit.isFailure(exit.value)) {
          assert.isTrue(Cause.hasInterruptsOnly(exit.value.cause))
        }
      }
    }), 30000)

  it.live("drops payloads the peer emits after close", () =>
    Effect.scoped(Effect.gen(function*() {
      const before = Uint8Array.of(1)
      const after = Uint8Array.of(2)
      const events = yield* Queue.unbounded<PeerRpc.OpenEvent, PeerRpcError.PeerRpcError>()
      yield* Queue.offer(events, serverOpened)
      const delivered = yield* Queue.unbounded<Uint8Array>()
      const received = yield* Deferred.make<void>()
      const client = makeClient(() => Stream.fromQueue(events), () => Effect.void)
      const connection = yield* connect(client, serverPeerId)
      const receiving = yield* Stream.runForEach(
        connection.receive,
        (payload) => Queue.offer(delivered, payload).pipe(Effect.andThen(Deferred.succeed(received, undefined)))
      ).pipe(Effect.forkChild)

      yield* Queue.offer(events, PeerRpc.Message.make({ _tag: "Message", payload: before }))
      const arrived = yield* Deferred.await(received).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(arrived), "consumer never received the pre-close Message")

      yield* connection.close
      yield* Queue.offer(events, PeerRpc.Message.make({ _tag: "Message", payload: after }))

      const exit = yield* Fiber.await(receiving).pipe(Effect.timeoutOption("4 seconds"))
      assert.isTrue(Option.isSome(exit), "close did not terminate the receive consumer")
      if (Option.isSome(exit)) {
        assert.isTrue(Exit.isFailure(exit.value))
        if (Exit.isFailure(exit.value)) {
          assert.isTrue(Cause.hasInterruptsOnly(exit.value.cause))
        }
      }
      assert.deepStrictEqual(yield* Queue.takeAll(delivered), [before])
    })), 30000)

  it.effect("interrupts and joins a canceled send before a later send begins", () =>
    Effect.scoped(Effect.gen(function*() {
      const starts = yield* Queue.unbounded<number>()
      const firstStopped = yield* Deferred.make<void>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        (request) =>
          Queue.offer(starts, request.payload[0]).pipe(
            Effect.andThen(request.payload[0] === 1 ? Effect.never : Effect.void),
            Effect.ensuring(
              request.payload[0] === 1 ? Deferred.succeed(firstStopped, undefined) : Effect.void
            )
          )
      )
      const connection = yield* connect(client, serverPeerId)
      const first = yield* connection.send(Uint8Array.of(1)).pipe(Effect.forkChild)
      assert.strictEqual(yield* Queue.take(starts), 1)
      yield* Fiber.interrupt(first)
      assert.isTrue(yield* Deferred.isDone(firstStopped))
      yield* connection.send(Uint8Array.of(2))
      assert.strictEqual(yield* Queue.take(starts), 2)
      yield* connection.close
    })))

  it.effect("classifies transient capacity and session failures as retryable", () =>
    Effect.scoped(Effect.gen(function*() {
      const transient = [
        new PeerRpcError.RequestCapacityExceeded(),
        new PeerRpcError.SessionUnavailable(),
        new PeerRpcError.SessionOverloaded(),
        new PeerRpcError.ServerUnavailable()
      ]
      for (const rpcError of transient) {
        const client = makeClient(() => Stream.fail(rpcError), () => Effect.void)
        const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "StorageUnavailable")
        assert.isTrue(RpcPeerTransport.isRetryable(error))
      }
      const disconnected = makeClient(
        () =>
          Stream.fail(
            new RpcClientError({
              reason: new RpcClientDefect({ message: "disconnected", cause: new Error("private transport detail") })
            })
          ),
        () => Effect.void
      )
      const disconnectError = yield* connect(disconnected, serverPeerId).pipe(Effect.flip)
      assert.strictEqual(disconnectError.reason._tag, "StorageUnavailable")
      assert.isTrue(RpcPeerTransport.isRetryable(disconnectError))
    })))

  it.effect("builds the transport composition used by makeSession", () =>
    Effect.scoped(Effect.gen(function*() {
      const pushed = yield* Deferred.make<typeof PeerRpc.PushRpc.payloadSchema.Type>()
      const client = makeClient(
        () => liveOpen(serverOpened),
        (request) => Deferred.succeed(pushed, request).pipe(Effect.asVoid)
      )
      const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents, definition }))
      const connection = yield* Context.get(context, PeerTransport.PeerTransport).connect({
        replicaId,
        peerId: serverPeerId
      })
      yield* connection.send(Uint8Array.of(9))
      assert.deepStrictEqual(yield* Deferred.await(pushed), {
        sessionId,
        payload: Uint8Array.of(9)
      })
      yield* connection.close
    })))

  it("keeps nonretryable ReplicaError reasons stable", () => {
    assert.isFalse(RpcPeerTransport.isRetryable(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({ expected: "expected", observed: "observed" })
      })
    ))
    assert.isFalse(RpcPeerTransport.isRetryable(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.QuotaExceeded({ resource: "request", limit: 1 })
      })
    ))
  })
})

describe("RpcPeerTransport store and forward", () => {
  it.effect("opens strict version 3 relay mode with distinct logical and relay identities", () =>
    Effect.scoped(Effect.gen(function*() {
      const configurations = yield* Ref.make(0)
      const client = makeRelayClient((request, options) => {
        assert.strictEqual(options.streamBufferSize, 1)
        assert.strictEqual(request.version, PeerRelayRpc.protocolVersion)
        assert.strictEqual(request.expectedRelayPeerId, relayPeerId)
        assert.deepStrictEqual(request.expectedLocal, relayOptions.expectedLocal)
        assert.strictEqual(request.senderReplicaIncarnation, senderReplicaIncarnation)
        assert.deepStrictEqual(request.remote, relayOptions.remote)
        assert.strictEqual(
          request.senderRetryHorizonMillis,
          relayOptions.senderRetryHorizonMillis
        )
        return Stream.concat(Stream.make(relayOpened), Stream.never)
      })
      const runtime = makeRuntime({
        validateConnectionConfiguration: (input) => {
          assert.deepStrictEqual(input, {
            replicaIncarnation: senderReplicaIncarnation,
            retryHorizonMillis: relayOptions.senderRetryHorizonMillis,
            replayBatchSize: relayOptions.replayBatchSize
          })
          return Ref.update(configurations, (count) => count + 1)
        }
      })
      const connection = yield* connectRelay(client, runtime)
      assert.strictEqual(connection.peerId, remotePeerId)
      assert.strictEqual(connection.relayPeerId, relayPeerId)
      assert.isTrue(connection.capabilities.storeAndForward)
      assert.strictEqual(yield* Ref.get(configurations), 1)
      yield* connection.close
    })))

  it.effect("replays stable outbox rows before new Push and retires only after custody", () =>
    Effect.scoped(Effect.gen(function*() {
      const replayPayload = Uint8Array.of(7)
      const newPayload = Uint8Array.of(8)
      const dueCalls = yield* Ref.make(0)
      const admitted = yield* Ref.make(false)
      const pushed = yield* Queue.unbounded<Uint8Array>()
      const custody = yield* Queue.unbounded<Identity.RelayMessageId>()
      const runtime = makeRuntime({
        maximumPendingHorizon: () => Effect.succeed(relayOptions.senderRetryHorizonMillis),
        dueForEndpoint: () =>
          Ref.updateAndGet(dueCalls, (count) => count + 1).pipe(
            Effect.map((count) => count === 1 ? [relayEntry(replayPayload)] : [])
          ),
        admit: (input) =>
          Ref.set(admitted, true).pipe(
            Effect.as(relayEntry(input.payload))
          ),
        markCustody: (input) => Queue.offer(custody, input.relayMessageId).pipe(Effect.asVoid)
      })
      const client = makeRelayClient(
        () => Stream.concat(Stream.make(relayOpened), Stream.never),
        (request) =>
          Effect.gen(function*() {
            if (request.payload === newPayload) assert.isTrue(yield* Ref.get(admitted))
            yield* Queue.offer(pushed, request.payload)
          })
      )
      const connection = yield* connectRelay(client, runtime)
      assert.deepStrictEqual(yield* Queue.take(pushed), replayPayload)
      assert.strictEqual(yield* Queue.take(custody), relayMessageId)
      yield* connection.send(newPayload)
      assert.deepStrictEqual(yield* Queue.take(pushed), newPayload)
      assert.strictEqual(yield* Queue.take(custody), relayMessageId)
      yield* connection.close
    })))

  it.effect("keeps one stable relay identity across a failed Push and reconnect replay", () =>
    Effect.scoped(Effect.gen(function*() {
      const payload = Uint8Array.of(10)
      const entry = relayEntry(payload)
      const pushAttempts = yield* Ref.make(0)
      const replayed = yield* Ref.make(false)
      const pushedIds = yield* Queue.unbounded<Identity.RelayMessageId>()
      const firstRuntime = makeRuntime({
        admit: () => Effect.succeed(entry),
        dueForEndpoint: () => Effect.succeed([])
      })
      const restartedRuntime = makeRuntime({
        admit: () => Effect.succeed(entry),
        dueForEndpoint: () =>
          Ref.getAndSet(replayed, true).pipe(
            Effect.map((alreadyReplayed) => alreadyReplayed ? [] : [entry])
          )
      })
      const client = makeRelayClient(
        () => Stream.concat(Stream.make(relayOpened), Stream.never),
        (request) =>
          Queue.offer(pushedIds, request.relayMessageId).pipe(
            Effect.andThen(Ref.updateAndGet(pushAttempts, (count) => count + 1)),
            Effect.flatMap((attempt) =>
              attempt === 1
                ? Effect.fail(new PeerRpcError.ServerUnavailable())
                : Effect.void
            )
          )
      )
      const first = yield* connectRelay(client, firstRuntime)
      const firstError = yield* first.send(payload).pipe(Effect.flip)
      assert.strictEqual(firstError.reason._tag, "StorageUnavailable")
      yield* first.close

      const second = yield* connectRelay(client, restartedRuntime)
      yield* second.close
      assert.deepStrictEqual(yield* Queue.takeAll(pushedIds), [
        relayMessageId,
        relayMessageId
      ])
    })))

  it.effect("rejects a retry horizon shrink before opening the relay stream", () =>
    Effect.scoped(Effect.gen(function*() {
      const opens = yield* Ref.make(0)
      const client = makeRelayClient(
        () =>
          Ref.update(opens, (count) => count + 1).pipe(
            Effect.as(Stream.concat(Stream.make(relayOpened), Stream.never)),
            Stream.unwrap
          )
      )
      const error = yield* connectRelay(
        client,
        makeRuntime({
          maximumPendingHorizon: () => Effect.succeed(relayOptions.senderRetryHorizonMillis + 1)
        })
      ).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ReplicaError")
      if (error._tag === "ReplicaError") {
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      }
      assert.strictEqual(yield* Ref.get(opens), 0)
    })))

  it.effect("rejects a mismatched RelayOpened before exposing a connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const finalized = yield* Ref.make(0)
      const mismatched = {
        ...relayOpened,
        authenticatedLocal: {
          ...relayOpened.authenticatedLocal,
          subjectId: "other-subject"
        }
      } as PeerRelayRpc.RelayOpened
      const client = makeRelayClient(
        () =>
          Stream.concat(Stream.make(mismatched), Stream.never).pipe(
            Stream.ensuring(Ref.update(finalized, (count) => count + 1))
          )
      )
      const error = yield* connectRelay(client, makeRuntime()).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ReplicaError")
      if (error._tag === "ReplicaError") {
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      }
      assert.strictEqual(yield* Ref.get(finalized), 1)
    })))

  it.effect("validates the complete outer digest before exposing acknowledged delivery", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const valid = yield* makeStoredMessage(Uint8Array.of(1, 2, 3))
        const invalid = PeerRelayRpc.StoredMessage.make({
          ...valid,
          outerEnvelopeDigest: "0".repeat(64)
        })
        const acknowledgements = yield* Ref.make(0)
        const client = makeRelayClient(
          () => Stream.fromIterable([relayOpened, invalid]).pipe(Stream.rechunk(1)),
          undefined,
          () => Ref.update(acknowledgements, (count) => count + 1)
        )
        const connection = yield* connectRelay(client, makeRuntime())
        const error = yield* Stream.runHead(
          connection.receiveWithAcknowledgement!
        ).pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        assert.strictEqual(yield* Ref.get(acknowledgements), 0)
        yield* connection.close
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("reconstructs a non-genesis lineage when verifying the outer digest", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const stored = yield* makeStoredMessage(
          Uint8Array.of(2, 3, 4),
          rewrittenLineage
        )
        const client = makeRelayClient(
          () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1))
        )
        const connection = yield* connectRelay(client, makeRuntime())
        const delivery = yield* Stream.runHead(
          connection.receiveWithAcknowledgement!
        ).pipe(Effect.map(Option.getOrThrow))
        assert.deepStrictEqual(delivery.message, stored.payload)
        yield* connection.close
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("defers Ack until the delivery consumer commits and closes on Ack failure", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const stored = yield* makeStoredMessage(Uint8Array.of(4, 5, 6))
        const acknowledgements = yield* Ref.make(0)
        const finalized = yield* Ref.make(0)
        const client = makeRelayClient(
          () =>
            Stream.fromIterable([relayOpened, stored]).pipe(
              Stream.rechunk(1),
              Stream.concat(Stream.never),
              Stream.ensuring(Ref.update(finalized, (count) => count + 1))
            ),
          undefined,
          (request) => {
            assert.strictEqual(request.sessionId, sessionId)
            assert.strictEqual(request.relayMessageId, relayMessageId)
            assert.strictEqual(request.claimToken, claimToken)
            assert.strictEqual(request.messageHash, stored.messageHash)
            return Ref.update(acknowledgements, (count) => count + 1).pipe(
              Effect.andThen(Effect.fail(new PeerRpcError.ServerUnavailable()))
            )
          }
        )
        const connection = yield* connectRelay(client, makeRuntime())
        const deliveryOption = yield* Stream.runHead(connection.receiveWithAcknowledgement!)
        assert.isTrue(Option.isSome(deliveryOption))
        if (Option.isNone(deliveryOption)) return
        const delivery = deliveryOption.value
        assert.strictEqual(yield* Ref.get(acknowledgements), 0)
        assert.strictEqual(
          delivery.receiptRetentionMillis,
          relayOptions.receiptRetentionMillis
        )
        assert.strictEqual(delivery.identity.relayPeerId, relayPeerId)
        assert.strictEqual(delivery.identity.senderPeerId, remotePeerId)
        const error = yield* delivery.acknowledge.pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "StorageUnavailable")
        assert.strictEqual(yield* Ref.get(acknowledgements), 1)
        assert.strictEqual(yield* Ref.get(finalized), 1)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("signals receipt maintenance only after Ack succeeds", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const stored = yield* makeStoredMessage(Uint8Array.of(5, 6, 7))
        const acknowledgements = yield* Ref.make(0)
        const pruneSignals = yield* Ref.make(0)
        const client = makeRelayClient(
          () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1)),
          undefined,
          () => Ref.update(acknowledgements, (count) => count + 1)
        )
        const connection = yield* connectRelay(
          client,
          makeRuntime({
            signalReceiptPrune: Ref.update(pruneSignals, (count) => count + 1)
          })
        )
        const delivery = yield* Stream.runHead(
          connection.receiveWithAcknowledgement!
        ).pipe(Effect.map(Option.getOrThrow))
        assert.strictEqual(yield* Ref.get(acknowledgements), 0)
        assert.strictEqual(yield* Ref.get(pruneSignals), 0)
        yield* delivery.acknowledge
        assert.strictEqual(yield* Ref.get(acknowledgements), 1)
        assert.strictEqual(yield* Ref.get(pruneSignals), 1)
        yield* connection.close
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("binds permanent rejection to the current relay session and claim", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const stored = yield* makeStoredMessage(Uint8Array.of(6, 7, 8))
        const rejected = yield* Deferred.make<
          typeof PeerRelayRpc.RejectRelayRpc.payloadSchema.Type
        >()
        const client = makeRelayClient(
          () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1)),
          undefined,
          undefined,
          (request) => Deferred.succeed(rejected, request).pipe(Effect.asVoid)
        )
        const connection = yield* connectRelay(client, makeRuntime())
        const delivery = yield* Stream.runHead(
          connection.receiveWithAcknowledgement!
        ).pipe(Effect.map(Option.getOrThrow))
        yield* delivery.reject("ApplicationRejected")
        assert.deepStrictEqual(yield* Deferred.await(rejected), {
          sessionId,
          relayMessageId,
          claimToken,
          messageHash: stored.messageHash,
          reason: "ApplicationRejected"
        })
        yield* connection.close
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("terminates a dependent relay connection when runtime health fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const fatal = yield* Deferred.make<never, ReplicaError.ReplicaError>()
      const finalized = yield* Deferred.make<void>()
      const pushStarted = yield* Deferred.make<void>()
      const client = makeRelayClient(
        () =>
          Stream.concat(Stream.make(relayOpened), Stream.never).pipe(
            Stream.ensuring(Deferred.succeed(finalized, undefined))
          ),
        () =>
          Deferred.succeed(pushStarted, undefined).pipe(
            Effect.andThen(Effect.never)
          )
      )
      const connection = yield* connectRelay(
        client,
        makeRuntime({
          awaitFatal: Deferred.await(fatal)
        })
      )
      const receiving = yield* Stream.runDrain(
        connection.receiveWithAcknowledgement!
      ).pipe(Effect.forkChild)
      const sending = yield* connection.send(Uint8Array.of(9)).pipe(
        Effect.forkChild
      )
      yield* Deferred.await(pushStarted)
      const fatalError = new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageUnavailable({
          cause: new Error("runtime maintenance failed")
        })
      })
      yield* Deferred.fail(
        fatal,
        fatalError
      )
      const receiveExit = yield* Fiber.await(receiving)
      assert.isTrue(Exit.isFailure(receiveExit))
      if (Exit.isFailure(receiveExit)) {
        const error = Cause.findErrorOption(receiveExit.cause)
        assert.isTrue(Option.isSome(error))
        if (Option.isSome(error)) assert.strictEqual(error.value, fatalError)
      }
      const sendExit = yield* Fiber.await(sending)
      assert.isTrue(Exit.isFailure(sendExit))
      if (Exit.isFailure(sendExit)) {
        const error = Cause.findErrorOption(sendExit.cause)
        assert.isTrue(Option.isSome(error))
        if (Option.isSome(error)) assert.strictEqual(error.value, fatalError)
      }
      yield* Deferred.await(finalized)
      const lateSendExit = yield* connection.send(Uint8Array.of(10)).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(lateSendExit))
      if (Exit.isFailure(lateSendExit)) {
        const error = Cause.findErrorOption(lateSendExit.cause)
        assert.isTrue(Option.isSome(error))
        if (Option.isSome(error)) assert.strictEqual(error.value, fatalError)
      }
    })))

  it.effect("records fixed adapter acknowledgement outcomes and bounded facts", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    return Effect.scoped(Effect.gen(function*() {
      const stored = yield* makeStoredMessage(Uint8Array.of(7, 8, 9))
      const ackClient = makeRelayClient(
        () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1))
      )
      const ackConnection = yield* connectRelay(ackClient, makeRuntime())
      const acknowledged = yield* Stream.runHead(
        ackConnection.receiveWithAcknowledgement!
      ).pipe(Effect.map(Option.getOrThrow))
      yield* acknowledged.acknowledge
      yield* ackConnection.close

      const rejectClient = makeRelayClient(
        () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1))
      )
      const rejectConnection = yield* connectRelay(rejectClient, makeRuntime())
      const rejected = yield* Stream.runHead(
        rejectConnection.receiveWithAcknowledgement!
      ).pipe(Effect.map(Option.getOrThrow))
      yield* rejected.reject("ApplicationRejected")
      yield* rejectConnection.close

      const unavailableClient = makeRelayClient(
        () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1)),
        undefined,
        () => Effect.fail(new PeerRpcError.ServerUnavailable())
      )
      const unavailableConnection = yield* connectRelay(
        unavailableClient,
        makeRuntime()
      )
      const unavailable = yield* Stream.runHead(
        unavailableConnection.receiveWithAcknowledgement!
      ).pipe(Effect.map(Option.getOrThrow))
      assert.isTrue(Exit.isFailure(yield* unavailable.acknowledge.pipe(Effect.exit)))

      const terminalSpans = spans.filter((span) => span.name === "effect_local_rpc.adapter.relay_acknowledge")
      assert.strictEqual(terminalSpans.length, 3)
      assert.deepStrictEqual(
        terminalSpans.map((span) => {
          const attributes = Object.fromEntries(span.attributes)
          return {
            name: span.name,
            operation: attributes["rpc.operation"],
            direction: attributes["rpc.direction"],
            result: attributes["rpc.result"],
            bytes: attributes["rpc.bytes"],
            items: attributes["rpc.items"],
            version: attributes["rpc.version"],
            latencyMillis: attributes["rpc.latency_millis"]
          }
        }),
        [
          {
            name: "effect_local_rpc.adapter.relay_acknowledge",
            operation: "AdapterAcknowledge",
            direction: "Receive",
            result: "Acknowledged",
            bytes: stored.payload.byteLength,
            items: 1,
            version: String(stored.payloadVersion),
            latencyMillis: undefined
          },
          {
            name: "effect_local_rpc.adapter.relay_acknowledge",
            operation: "AdapterAcknowledge",
            direction: "Receive",
            result: "DeadLettered",
            bytes: stored.payload.byteLength,
            items: 1,
            version: String(stored.payloadVersion),
            latencyMillis: undefined
          },
          {
            name: "effect_local_rpc.adapter.relay_acknowledge",
            operation: "AdapterAcknowledge",
            direction: "Receive",
            result: "Unavailable",
            bytes: stored.payload.byteLength,
            items: 1,
            version: String(stored.payloadVersion),
            latencyMillis: undefined
          }
        ]
      )
    })).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.provide(NodeCrypto.layer)
    )
  })

  it.effect("records relay boundaries without endpoint payload or claim values", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    return Effect.scoped(Effect.gen(function*() {
      const client = makeRelayClient(
        () => Stream.concat(Stream.make(relayOpened), Stream.never)
      )
      const connection = yield* connectRelay(client, makeRuntime())
      yield* connection.send(Uint8Array.of(201, 202, 203))
      yield* connection.close
      const telemetry = JSON.stringify(
        spans
          .filter((span) => span.name.startsWith("effect_local_rpc.adapter.relay_"))
          .map((span) => ({
            name: span.name,
            attributes: [...span.attributes]
          }))
      ) + (yield* Metric.dump)
      for (
        const forbidden of [
          relayPeerId,
          localPeerId,
          remotePeerId,
          sessionId,
          relayMessageId,
          claimToken,
          documentId,
          "local-subject",
          "remote-subject",
          "201,202,203"
        ]
      ) {
        assert.notInclude(telemetry, forbidden)
      }
    })).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })
})
