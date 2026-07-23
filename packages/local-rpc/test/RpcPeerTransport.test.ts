import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
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
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"

const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const otherPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const otherSessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000002")
const documents = [{ document: Task, documentId }]

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
    const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents }))
    return yield* Context.get(context, PeerTransport.PeerTransport).connect({ replicaId, peerId })
  })

const liveOpen = (event: PeerRpc.OpenEvent) => Stream.concat(Stream.make(event), Stream.never)

describe("RpcPeerTransport", () => {
  it.effect("validates the first streamed event as the handshake and uses response capacity one", () =>
    Effect.scoped(Effect.gen(function*() {
      const payload = Uint8Array.of(1, 2, 3)
      const client = makeClient(
        (request, options) => {
          assert.strictEqual(options.streamBufferSize, 1)
          assert.strictEqual(request.expectedPeerId, serverPeerId)
          assert.deepStrictEqual(request.documents, [{ documentType: Task.name, documentId }])
          return Stream.fromIterable([
            serverOpened,
            PeerRpc.Message.make({ _tag: "Message", payload })
          ]).pipe(Stream.rechunk(1))
        },
        () => Effect.void
      )
      const connection = yield* connect(client, serverPeerId)
      assert.strictEqual(connection.peerId, serverPeerId)
      assert.deepStrictEqual(yield* Stream.runCollect(connection.receive), [payload])
      yield* connection.close
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
        new PeerRpcError.InvalidRequest()
      ]
      for (const rpcError of permanent) {
        const client = makeClient(() => Stream.fail(rpcError), () => Effect.void)
        const error = yield* connect(client, serverPeerId).pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, "ProtocolMismatch")
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

  it.effect("reports storeAndForward false", () =>
    Effect.scoped(Effect.gen(function*() {
      const client = makeClient(() => liveOpen(serverOpened), () => Effect.void)
      const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents }))
      const transport = Context.get(context, PeerTransport.PeerTransport)
      assert.isFalse(transport.capabilities.storeAndForward)
      const connection = yield* transport.connect({ replicaId, peerId: serverPeerId })
      assert.isFalse(connection.capabilities.storeAndForward)
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
      const context = yield* Layer.build(RpcPeerTransport.layer(client, { documents }))
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
