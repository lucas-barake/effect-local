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
import * as Stream from "effect/Stream"
import * as Tracer from "effect/Tracer"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000005")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
const senderReplicaIncarnation = Identity.ReplicaIncarnation.make(1)
const claimToken = PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000001")
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

const relayOptions: RpcPeerTransport.Options = {
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

const relayOpened = PeerRpc.Opened.make({
  _tag: "Opened",
  protocolVersion: PeerRpc.protocolVersion,
  sessionId,
  remotePeerId,
  authenticatedLocal: relayOptions.expectedLocal
})

const makeRelayClient = (
  open: (
    request: typeof PeerRpc.OpenRpc.payloadSchema.Type,
    options: { readonly streamBufferSize?: number | undefined }
  ) => Stream.Stream<PeerRpc.OpenEvent, unknown>,
  push: (
    request: typeof PeerRpc.PushRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void,
  acknowledge: (
    request: typeof PeerRpc.AcknowledgeRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void,
  reject: (
    request: typeof PeerRpc.RejectRpc.payloadSchema.Type
  ) => Effect.Effect<void, unknown> = () => Effect.void
): PeerRpc.RpcClient =>
  ({
    Open: open,
    Push: push,
    Acknowledge: acknowledge,
    Reject: reject
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
  protocolVersion: PeerRpc.protocolVersion,
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

/**
 * `overrides` re-derives the digest from the tampered envelope. A message edited afterwards fails
 * the digest check, so it proves nothing about the endpoint and document checks beside it.
 */
const makeStoredMessage = (
  payloadSeed: Uint8Array,
  lineage: Identity.DocumentLineage = Identity.genesisLineage,
  overrides?: {
    readonly sender?: PeerSyncEnvelope.RelayPeerPrincipal
    readonly recipient?: PeerSyncEnvelope.RelayPeerPrincipal
    readonly document?: { readonly documentType: string; readonly documentId: Identity.DocumentId }
  }
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
      expectedLocal: overrides?.sender ?? {
        tenantId: relayOptions.expectedLocal.tenantId,
        subjectId: relayOptions.remote.subjectId,
        peerId: relayOptions.remote.peerId
      },
      remote: overrides?.recipient ?? relayOptions.expectedLocal,
      relayPeerId,
      relayMessageId,
      protocolVersion: PeerRpc.protocolVersion,
      payloadVersion: PeerSyncEnvelope.syncEnvelopeVersion,
      senderReplicaIncarnation,
      senderConnectionEpoch: "remote-epoch",
      senderSequence: 3,
      document: overrides?.document ?? { documentType: Task.name, documentId },
      lineage,
      writerProvenance,
      messageHash,
      payload
    }
    const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(envelope)
    return PeerRpc.StoredMessage.make({
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
  client: PeerRpc.RpcClient,
  runtime: ReturnType<typeof makeRuntime>,
  peerId: Identity.PeerId = remotePeerId
) =>
  Effect.gen(function*() {
    const context = yield* Layer.build(
      RpcPeerTransport.layer(client, relayOptions).pipe(
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
  it.effect("opens strict version 1 durable mode with distinct logical and relay identities", () =>
    Effect.scoped(Effect.gen(function*() {
      const configurations = yield* Ref.make(0)
      const client = makeRelayClient((request, options) => {
        assert.strictEqual(options.streamBufferSize, 1)
        assert.strictEqual(request.protocolVersion, PeerRpc.protocolVersion)
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

  it.effect("rejects a mismatched Opened before exposing a connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const finalized = yield* Ref.make(0)
      const mismatched = {
        ...relayOpened,
        authenticatedLocal: {
          ...relayOpened.authenticatedLocal,
          subjectId: "other-subject"
        }
      } as PeerRpc.Opened
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
        const invalid = PeerRpc.StoredMessage.make({
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
          connection.receive
        ).pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
        assert.strictEqual(yield* Ref.get(acknowledgements), 0)
        yield* connection.close
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  // The digest is recomputed from the CONFIGURED sender and recipient, so a wrong principal fails it
  // whatever the endpoint checks do. The document is taken from the event itself, so a misdirected
  // one produces a valid digest and only the selected-document check refuses it - and that check was
  // pinned by nothing in the repository.
  for (
    const wrong of [
      {
        name: "a sender that is not the configured remote peer",
        overrides: {
          sender: {
            tenantId: "tenant",
            subjectId: "remote-subject",
            peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000ff")
          }
        }
      },
      {
        name: "a recipient that is not this peer",
        overrides: {
          recipient: {
            tenantId: "tenant",
            subjectId: "local-subject",
            peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000fe")
          }
        }
      },
      {
        name: "a document this session did not select",
        overrides: {
          document: {
            documentType: Task.name,
            documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-0000000000ff")
          }
        }
      }
    ]
  ) {
    it.effect(`refuses a stored message carrying ${wrong.name}`, () =>
      Effect.scoped(
        Effect.gen(function*() {
          const misaddressed = yield* makeStoredMessage(Uint8Array.of(1, 2, 3), undefined, wrong.overrides)
          const acknowledgements = yield* Ref.make(0)
          const client = makeRelayClient(
            () => Stream.fromIterable([relayOpened, misaddressed]).pipe(Stream.rechunk(1)),
            undefined,
            () => Ref.update(acknowledgements, (count) => count + 1)
          )
          const connection = yield* connectRelay(client, makeRuntime())
          const error = yield* Stream.runHead(connection.receive).pipe(Effect.flip)
          assert.strictEqual(error.reason._tag, "ProtocolMismatch")
          assert.strictEqual(yield* Ref.get(acknowledgements), 0)
          yield* connection.close
        }).pipe(Effect.provide(NodeCrypto.layer))
      ))
  }

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
          connection.receive
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
        const deliveryOption = yield* Stream.runHead(connection.receive)
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
          connection.receive
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
          typeof PeerRpc.RejectRpc.payloadSchema.Type
        >()
        const client = makeRelayClient(
          () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1)),
          undefined,
          undefined,
          (request) => Deferred.succeed(rejected, request).pipe(Effect.asVoid)
        )
        const connection = yield* connectRelay(client, makeRuntime())
        const delivery = yield* Stream.runHead(
          connection.receive
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
        connection.receive
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
        ackConnection.receive
      ).pipe(Effect.map(Option.getOrThrow))
      yield* acknowledged.acknowledge
      yield* ackConnection.close

      const rejectClient = makeRelayClient(
        () => Stream.fromIterable([relayOpened, stored]).pipe(Stream.rechunk(1))
      )
      const rejectConnection = yield* connectRelay(rejectClient, makeRuntime())
      const rejected = yield* Stream.runHead(
        rejectConnection.receive
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
        unavailableConnection.receive
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

  // Nothing in this package calls it: reconnect policy is the application's. That is exactly why
  // the classification is pinned here rather than discovered wrong by a consumer.
  describe("isRetryable", () => {
    it("treats unavailability as retryable and every permanent rejection as not", () => {
      assert.isTrue(
        RpcPeerTransport.isRetryable(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause: new Error("relay down") })
          })
        )
      )
      for (
        const reason of [
          new ReplicaError.ProtocolMismatch({ expected: "valid relay handshake", observed: "AccessDenied" }),
          new ReplicaError.QuotaExceeded({ resource: "relay outbox", limit: 0 })
        ]
      ) {
        assert.isFalse(RpcPeerTransport.isRetryable(new ReplicaError.ReplicaError({ reason })))
      }
    })
  })
})
