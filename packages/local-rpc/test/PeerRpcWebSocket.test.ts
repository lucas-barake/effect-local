import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerAuthorization from "../src/PeerAuthorization.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"

const Task = Document.make("WebSocketTask", {
  schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
  version: 1
})

const RenameTask = Mutation.make("WebSocketTask.Rename", {
  document: Task,
  payload: Schema.String
})

const AddLabel = Mutation.make("WebSocketTask.AddLabel", {
  document: Task,
  payload: Schema.String
})

const definition = ReplicaDefinition.make({
  name: "rpc-websocket-test",
  documents: DocumentSet.make(Task),
  mutations: [RenameTask, AddLabel],
  projections: [],
  queries: []
})

const HandlersLive = Layer.mergeAll(
  RenameTask.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  }),
  AddLabel.toLayer(({ draft, payload }) => {
    draft.labels.push(payload)
    return undefined
  })
)

const ReplicaLive = TestReplica.layer(definition, { projections: [] }).pipe(
  Layer.provide(HandlersLive)
)

const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const clientPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const missingSessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")

describe("PeerRpc WebSocket", () => {
  it.live(
    "synchronizes two replicas through the application owned WebSocket server",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const serverEngineContext = yield* Layer.build(ReplicaLive)
        const clientEngineContext = yield* Layer.build(ReplicaLive)
        const serverReplica = Context.get(serverEngineContext, Replica.Replica)
        const clientReplica = Context.get(clientEngineContext, Replica.Replica)
        const serverSharding = Context.get(serverEngineContext, Sharding.Sharding)
        const clientSharding = Context.get(clientEngineContext, Sharding.Sharding)

        const created = yield* serverReplica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before WebSocket sync", labels: [] }
        })
        const documentId = yield* CommandOutcome.committedOrFail(created)
        const archive = yield* serverReplica.exportBackup({
          maxBytes: TestReplica.defaultLimits.maxBackupBytes
        }).pipe(Stream.runCollect)
        yield* clientReplica.restoreBackup({
          source: Stream.fromIterable(archive),
          mode: "clone",
          maxBytes: TestReplica.defaultLimits.maxBackupBytes,
          expectedDefinitionHash: definition.hash
        })

        const credentials: Array<string> = []
        let credential = "client-payload-credential"
        let authorizationCalls = 0
        const ServerDependencies = Layer.mergeAll(
          PeerRpcLimits.layerDefaults,
          Layer.succeed(PeerAuthenticator.PeerAuthenticator)({
            authenticate: (value) => {
              const received = Redacted.value(value)
              credentials.push(received)
              return received === "defect-credential"
                ? Effect.die(new Error("raw-authentication-secret"))
                : Effect.succeed({
                  principal: PeerAuthentication.PeerPrincipal.make({
                    tenantId: "tenant",
                    subjectId: "websocket-client",
                    peerId: clientPeerId
                  }),
                  validUntil: Number.MAX_SAFE_INTEGER,
                  invalidated: Effect.never
                })
            }
          }),
          PeerAuthorization.layer((request) => {
            authorizationCalls++
            assert.strictEqual(request.principal.tenantId, "tenant")
            assert.strictEqual(request.principal.subjectId, "websocket-client")
            assert.strictEqual(request.principal.peerId, clientPeerId)
            assert.deepStrictEqual(request.documents, [{ documentType: Task.name, documentId }])
            return Effect.succeed({
              documents: [{ document: Task, documentId }],
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
          })
        )
        const AuthenticationLive = PeerAuthentication.layerServer.pipe(
          Layer.provide(ServerDependencies)
        )
        const PeerHandlersLive = PeerRpcServer.layerHandlers({ tenantId: "tenant", peerId: serverPeerId }).pipe(
          Layer.provide(ServerDependencies)
        )
        const WsProtocol = RpcServer.layerProtocolWebsocket({ path: "/rpc" }).pipe(
          Layer.provide(HttpRouter.layer)
        )
        const RpcLive = RpcServer.layer(PeerRpc.Rpcs, { disableFatalDefects: true }).pipe(
          Layer.provide([PeerHandlersLive, AuthenticationLive])
        )
        const WebSocketServerLive = RpcLive.pipe(
          Layer.provideMerge(WsProtocol),
          Layer.provide(HttpRouter.serve(WsProtocol, { disableListenLog: true, disableLogger: true }))
        )
        const serverContext = yield* Layer.build(WebSocketServerLive.pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
          Layer.provide(RpcSerialization.layerMsgPack)
        )).pipe(
          Effect.provide(serverEngineContext)
        )
        const address = Context.get(serverContext, HttpServer.HttpServer).address
        if (address._tag !== "TcpAddress") return yield* Effect.die("Expected a TCP test server address")

        let connections = 0
        let disconnections = 0
        yield* Effect.scoped(
          Effect.gen(function*() {
            const CredentialsLive = Layer.succeed(PeerCredentials.PeerCredentials)({
              get: Effect.sync(() => Redacted.make(credential))
            })
            const ClientAuthenticationLive = PeerAuthentication.layerClient.pipe(
              Layer.provide(CredentialsLive)
            )
            const ConnectionHooksLive = Layer.succeed(RpcClient.ConnectionHooks)({
              onConnect: Effect.sync(() => connections++).pipe(Effect.asVoid),
              onDisconnect: Effect.sync(() => disconnections++).pipe(Effect.asVoid)
            })
            const ClientProtocolLive = RpcClient.layerProtocolSocket().pipe(
              Layer.provide([
                NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`),
                RpcSerialization.layerMsgPack,
                ConnectionHooksLive
              ])
            )
            const clientContext = yield* Layer.build(Layer.merge(ClientProtocolLive, ClientAuthenticationLive))
            const client = yield* PeerRpc.makeRpcClient.pipe(Effect.provide(clientContext))
            const session = yield* RpcPeerTransport.makeSession(client, {
              peerId: serverPeerId,
              documents: [{ document: Task, documentId }]
            }).pipe(Effect.provide(clientEngineContext))

            assert.strictEqual(session.peerId, serverPeerId)
            assert.strictEqual(authorizationCalls, 1)

            yield* clientReplica.mutate(RenameTask, {
              commandId: yield* Identity.makeCommandId,
              documentId,
              payload: "renamed by the WebSocket client"
            })

            let serverObservedClientMutation = false
            for (let attempt = 0; attempt < 200; attempt++) {
              yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
              serverObservedClientMutation =
                (yield* serverReplica.get(Task, documentId)).value.title === "renamed by the WebSocket client"
              if (serverObservedClientMutation) break
              yield* Effect.sleep("10 millis")
            }
            assert.isTrue(serverObservedClientMutation)

            yield* serverReplica.mutate(AddLabel, {
              commandId: yield* Identity.makeCommandId,
              documentId,
              payload: "server"
            })

            let replicasConverged = false
            for (let attempt = 0; attempt < 200; attempt++) {
              yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
              const serverSnapshot = yield* serverReplica.get(Task, documentId)
              const clientSnapshot = yield* clientReplica.get(Task, documentId)
              replicasConverged = JSON.stringify(serverSnapshot.value) === JSON.stringify(clientSnapshot.value) &&
                JSON.stringify(serverSnapshot.heads) === JSON.stringify(clientSnapshot.heads)
              if (replicasConverged) break
              yield* Effect.sleep("10 millis")
            }

            yield* session.markDirty(documentId)
            yield* session.flush
            let observedByServer = false
            for (let attempt = 0; attempt < 200; attempt++) {
              yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
              observedByServer = yield* session.observedByPeer(documentId)
              if (observedByServer) break
              yield* Effect.sleep("10 millis")
            }

            const serverSnapshot = yield* serverReplica.get(Task, documentId)
            const clientSnapshot = yield* clientReplica.get(Task, documentId)
            assert.isTrue(replicasConverged)
            assert.isTrue(observedByServer)
            assert.deepStrictEqual(clientSnapshot.value, {
              title: "renamed by the WebSocket client",
              labels: ["server"]
            })
            assert.deepStrictEqual(clientSnapshot.heads, serverSnapshot.heads)
            assert.isFalse(yield* session.durableConfirmation(documentId))
            assert.isAtLeast(credentials.length, 2)
            assert.isTrue(credentials.every((value) => value === "client-payload-credential"))

            credential = "defect-credential"
            const authenticationError = yield* client.Push({
              sessionId: missingSessionId,
              payload: Uint8Array.of(1)
            }).pipe(Effect.flip)
            assert.instanceOf(authenticationError, PeerRpcError.AuthenticationFailure)
            assert.notInclude(String(authenticationError), "raw-authentication-secret")
          }).pipe(Effect.provide(clientEngineContext))
        )

        assert.strictEqual(credentials.at(-1), "defect-credential")
        assert.strictEqual(connections, 1)
        assert.strictEqual(disconnections, 1)
      })).pipe(Effect.provide([
        NodeCrypto.layer,
        ReplicaLimits.layer(TestReplica.defaultLimits)
      ])),
    15_000
  )
})
