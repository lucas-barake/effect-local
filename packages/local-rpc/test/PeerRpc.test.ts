import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"

const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")

describe("PeerRpc", () => {
  it.effect("executes the generated client through handlers and client authentication middleware", () =>
    Effect.scoped(Effect.gen(function*() {
      const openRequest = yield* Deferred.make<typeof PeerRpc.OpenRpc.payloadSchema.Type>()
      const pushRequest = yield* Deferred.make<typeof PeerRpc.PushRpc.payloadSchema.Type>()
      const disconnects = yield* Queue.unbounded<number>()
      const clients = new Set<number>()
      let sendToServer: (clientId: number, request: RpcMessage.FromClientEncoded) => Effect.Effect<void> = () =>
        Effect.void
      let sendToClient: (clientId: number, response: RpcMessage.FromServerEncoded) => Effect.Effect<void> = () =>
        Effect.void
      const serverProtocol = yield* RpcServer.Protocol.make((writeRequest) =>
        Effect.sync(() => {
          sendToServer = writeRequest
          return {
            disconnects,
            send: (clientId, response) => sendToClient(clientId, response),
            end: () => Effect.void,
            clientIds: Effect.succeed(clients),
            initialMessage: Effect.succeed(Option.none()),
            supportsAck: true,
            supportsTransferables: false,
            supportsSpanPropagation: true
          }
        })
      )
      const clientProtocol = yield* RpcClient.Protocol.make((writeResponse, clientIds) =>
        Effect.sync(() => {
          clients.clear()
          for (const clientId of clientIds) clients.add(clientId)
          sendToClient = writeResponse
          return {
            send: (clientId, request) => sendToServer(clientId, request),
            supportsAck: true,
            supportsTransferables: false
          }
        })
      )
      const authenticated = {
        principal: PeerAuthentication.PeerPrincipal.make({
          tenantId: "tenant",
          subjectId: "subject",
          peerId
        }),
        validUntil: Number.MAX_SAFE_INTEGER,
        invalidated: Effect.void
      }
      const handlers = PeerRpc.Rpcs.toLayer(PeerRpc.Rpcs.of({
        Open: (request) =>
          Stream.fromEffect(
            Deferred.succeed(openRequest, request).pipe(
              Effect.as(PeerRpc.Opened.make({
                _tag: "Opened",
                protocolVersion: PeerRpc.protocolVersion,
                sessionId,
                peerId,
                capabilities: { storeAndForward: false }
              }))
            )
          ),
        Push: (request) => Deferred.succeed(pushRequest, request).pipe(Effect.asVoid)
      }))
      yield* RpcServer.make(PeerRpc.Rpcs).pipe(
        Effect.provideService(RpcServer.Protocol, serverProtocol),
        Effect.provide(handlers),
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: (credential) => {
            assert.strictEqual(Redacted.value(credential), "secret")
            return Effect.succeed(authenticated)
          }
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults),
        Effect.forkScoped
      )
      const client = yield* PeerRpc.makeRpcClient.pipe(
        Effect.provideService(RpcClient.Protocol, clientProtocol),
        Effect.provide(PeerAuthentication.layerClient),
        Effect.provideService(PeerCredentials.PeerCredentials, {
          get: Effect.succeed(Redacted.make("secret"))
        })
      )
      const events = yield* client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: peerId,
        documents: [{ documentType: "Task", documentId }]
      }).pipe(Stream.runCollect)
      yield* client.Push({ sessionId, payload: Uint8Array.of(4, 5, 6) })
      assert.deepStrictEqual(events, [PeerRpc.Opened.make({
        _tag: "Opened",
        protocolVersion: PeerRpc.protocolVersion,
        sessionId,
        peerId,
        capabilities: { storeAndForward: false }
      })])
      assert.strictEqual(Redacted.value((yield* Deferred.await(openRequest)).credential!), "secret")
      assert.strictEqual(Redacted.value((yield* Deferred.await(pushRequest)).credential!), "secret")
    })))

  it("roundtrips every version one request and response event", () => {
    const open = PeerRpc.OpenRpc.payloadSchema.make({
      protocolVersion: PeerRpc.protocolVersion,
      expectedPeerId: peerId,
      documents: [{ documentType: "Task", documentId }]
    })
    const opened = PeerRpc.Opened.make({
      _tag: "Opened",
      protocolVersion: PeerRpc.protocolVersion,
      sessionId,
      peerId,
      capabilities: { storeAndForward: false }
    })
    const message = PeerRpc.Message.make({ _tag: "Message", payload: Uint8Array.of(1, 2, 3) })
    const push = PeerRpc.PushRpc.payloadSchema.make({ sessionId, payload: Uint8Array.of(4, 5, 6) })

    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenRpc.payloadSchema)(open), open)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenEvent)(opened), opened)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenEvent)(message), message)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.PushRpc.payloadSchema)(push), push)
    assert.strictEqual(PeerRpc.OpenRpc._tag, "Open")
    assert.strictEqual(PeerRpc.PushRpc._tag, "Push")
  })

  it("roundtrips every tagged wire error with its exact tag", () => {
    const errors = [
      new PeerRpcError.AuthenticationFailure(),
      new PeerRpcError.AccessDenied(),
      new PeerRpcError.UnsupportedVersion(),
      new PeerRpcError.PeerMismatch(),
      new PeerRpcError.InvalidRequest(),
      new PeerRpcError.RequestLimitExceeded(),
      new PeerRpcError.RequestCapacityExceeded(),
      new PeerRpcError.SessionUnavailable(),
      new PeerRpcError.SessionOverloaded(),
      new PeerRpcError.ServerUnavailable()
    ]

    for (const error of errors) {
      assert.deepStrictEqual(Schema.encodeSync(PeerRpcError.PeerRpcError)(error), { _tag: error._tag })
    }
  })

  it("redacts defects to one fixed sentinel", () => {
    assert.deepStrictEqual(Schema.encodeSync(PeerRpcError.Defect)(new Error("secret")), { _tag: "InternalError" })
    assert.isUndefined(Schema.decodeUnknownSync(PeerRpcError.Defect)({ _tag: "InternalError", secret: "ignored" }))
  })

  it("rejects malformed identities and empty document types", () => {
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.OpenRpc.payloadSchema)({
        protocolVersion: 1,
        expectedPeerId: "peer_invalid",
        documents: [{ documentType: "", documentId }]
      })
    )
  })
})
