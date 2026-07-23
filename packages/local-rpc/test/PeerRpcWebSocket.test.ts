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
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Http from "node:http"
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
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId
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
        const PeerHandlersLive = PeerRpcServer.layerHandlers({
          tenantId: "tenant",
          peerId: serverPeerId,
          definitionHash: definition.hash
        }).pipe(
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
              documents: [{ document: Task, documentId }],
              definitionHash: definition.hash
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

  it.live(
    "redelivers pending client outbound after the server restarts on the same port",
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
          value: { title: "before restart", labels: [] }
        })
        const documentId = yield* CommandOutcome.committedOrFail(created)
        const archive = yield* serverReplica.exportBackup({
          maxBytes: TestReplica.defaultLimits.maxBackupBytes
        }).pipe(Stream.runCollect)
        yield* clientReplica.restoreBackup({
          source: Stream.fromIterable(archive),
          mode: "clone",
          maxBytes: TestReplica.defaultLimits.maxBackupBytes,
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId
        })

        const ServerDependencies = Layer.mergeAll(
          PeerRpcLimits.layerDefaults,
          Layer.succeed(PeerAuthenticator.PeerAuthenticator)({
            authenticate: () =>
              Effect.succeed({
                principal: PeerAuthentication.PeerPrincipal.make({
                  tenantId: "tenant",
                  subjectId: "websocket-client",
                  peerId: clientPeerId
                }),
                validUntil: Number.MAX_SAFE_INTEGER,
                invalidated: Effect.never
              })
          }),
          PeerAuthorization.layer(() =>
            Effect.succeed({
              documents: [{ document: Task, documentId }],
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
          )
        )
        const AuthenticationLive = PeerAuthentication.layerServer.pipe(Layer.provide(ServerDependencies))
        const PeerHandlersLive = PeerRpcServer.layerHandlers({
          tenantId: "tenant",
          peerId: serverPeerId,
          definitionHash: definition.hash
        }).pipe(
          Layer.provide(ServerDependencies)
        )
        const WsProtocol = RpcServer.layerProtocolWebsocket({ path: "/rpc" }).pipe(Layer.provide(HttpRouter.layer))
        const RpcLive = RpcServer.layer(PeerRpc.Rpcs, { disableFatalDefects: true }).pipe(
          Layer.provide([PeerHandlersLive, AuthenticationLive])
        )
        const WebSocketServerLive = RpcLive.pipe(
          Layer.provideMerge(WsProtocol),
          Layer.provide(HttpRouter.serve(WsProtocol, { disableListenLog: true, disableLogger: true }))
        )

        const buildServer = (port: number) =>
          Effect.gen(function*() {
            const scope = yield* Scope.make()
            const context = yield* Layer.build(
              WebSocketServerLive.pipe(
                Layer.provideMerge(NodeHttpServer.layer(() => Http.createServer(), { port })),
                Layer.provide(RpcSerialization.layerMsgPack)
              )
            ).pipe(
              Effect.provide(serverEngineContext),
              Effect.provideService(Scope.Scope, scope)
            )
            const address = Context.get(context, HttpServer.HttpServer).address
            if (address._tag !== "TcpAddress") return yield* Effect.die("Expected a TCP test server address")
            return { scope, port: address.port }
          })

        const connectSession = (port: number) =>
          Effect.gen(function*() {
            const CredentialsLive = Layer.succeed(PeerCredentials.PeerCredentials)({
              get: Effect.sync(() => Redacted.make("client-payload-credential"))
            })
            const ClientAuthenticationLive = PeerAuthentication.layerClient.pipe(Layer.provide(CredentialsLive))
            const ClientProtocolLive = RpcClient.layerProtocolSocket().pipe(
              Layer.provide([
                NodeSocket.layerWebSocket(`ws://127.0.0.1:${port}/rpc`),
                RpcSerialization.layerMsgPack
              ])
            )
            const clientContext = yield* Layer.build(Layer.merge(ClientProtocolLive, ClientAuthenticationLive))
            const client = yield* PeerRpc.makeRpcClient.pipe(Effect.provide(clientContext))
            return yield* RpcPeerTransport.makeSession(client, {
              peerId: serverPeerId,
              documents: [{ document: Task, documentId }],
              definitionHash: definition.hash
            }).pipe(Effect.provide(clientEngineContext))
          })

        const addLabel = (payload: string) =>
          Effect.gen(function*() {
            yield* clientReplica.mutate(AddLabel, {
              commandId: yield* Identity.makeCommandId,
              documentId,
              payload
            })
          })

        const pollUntil = <E,>(predicate: Effect.Effect<boolean, E>) =>
          Effect.gen(function*() {
            for (let attempt = 0; attempt < 500; attempt++) {
              yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
              if (yield* predicate) return true
              yield* Effect.sleep("10 millis")
            }
            return false
          })

        const server1 = yield* buildServer(0)
        const assignedPort = server1.port

        const session1Scope = yield* Scope.make()
        yield* connectSession(assignedPort).pipe(Effect.provideService(Scope.Scope, session1Scope))

        // Converge one mutation so the server durably holds it before the restart.
        yield* addLabel("committed-before-restart")
        const serverHoldsFirst = yield* pollUntil(
          Effect.map(
            serverReplica.get(Task, documentId),
            (snapshot) => snapshot.value.labels.includes("committed-before-restart")
          )
        )
        assert.isTrue(serverHoldsFirst)

        // Leave a second mutation un-acked, then take the server down mid-session.
        yield* addLabel("pending-at-restart")
        // Releasing the client scope severs the upgraded socket so closing the node http
        // server does not block on the still-open connection during graceful shutdown.
        const killing = yield* Scope.close(server1.scope, Exit.void).pipe(Effect.forkChild)
        yield* Scope.close(session1Scope, Exit.void)
        yield* Fiber.join(killing)

        // A mutation applied while disconnected is also pending outbound work.
        yield* addLabel("committed-while-offline")

        // Reclaim the same port the client still targets; a fresh listener can briefly
        // race the previous socket teardown, so bounded retries reclaim it.
        const server2 = yield* Effect.gen(function*() {
          let lastCause: unknown
          for (let attempt = 0; attempt < 50; attempt++) {
            const outcome = yield* buildServer(assignedPort).pipe(Effect.exit)
            if (Exit.isSuccess(outcome)) return outcome.value
            lastCause = outcome.cause
            yield* Effect.sleep("20 millis")
          }
          return yield* Effect.die(`Could not rebind port ${assignedPort}: ${String(lastCause)}`)
        })
        assert.strictEqual(server2.port, assignedPort)

        const session2Scope = yield* Scope.make()
        yield* connectSession(assignedPort).pipe(Effect.provideService(Scope.Scope, session2Scope))

        const converged = yield* pollUntil(Effect.gen(function*() {
          const server = yield* serverReplica.get(Task, documentId)
          const client = yield* clientReplica.get(Task, documentId)
          return server.value.labels.includes("committed-while-offline") &&
            JSON.stringify(server.value) === JSON.stringify(client.value) &&
            JSON.stringify(server.heads) === JSON.stringify(client.heads)
        }))
        assert.isTrue(converged)

        const serverSnapshot = yield* serverReplica.get(Task, documentId)
        const clientSnapshot = yield* clientReplica.get(Task, documentId)
        assert.deepStrictEqual(serverSnapshot.value, clientSnapshot.value)
        assert.deepStrictEqual(serverSnapshot.heads, clientSnapshot.heads)
        // Every mutation is applied exactly once: no work lost across the restart, and the
        // change the server already held before the restart is not applied twice on redelivery.
        assert.deepStrictEqual([...serverSnapshot.value.labels].toSorted(), [
          "committed-before-restart",
          "committed-while-offline",
          "pending-at-restart"
        ])

        yield* Scope.close(session2Scope, Exit.void)
        yield* Scope.close(server2.scope, Exit.void)
      })).pipe(Effect.provide([
        NodeCrypto.layer,
        ReplicaLimits.layer(TestReplica.defaultLimits)
      ])),
    30_000
  )

  it.live(
    "rejects a version-skewed reconnect with a typed error and applies no data",
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
          value: { title: "matched build", labels: [] }
        })
        const documentId = yield* CommandOutcome.committedOrFail(created)
        const archive = yield* serverReplica.exportBackup({
          maxBytes: TestReplica.defaultLimits.maxBackupBytes
        }).pipe(Stream.runCollect)
        yield* clientReplica.restoreBackup({
          source: Stream.fromIterable(archive),
          mode: "clone",
          maxBytes: TestReplica.defaultLimits.maxBackupBytes,
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId
        })

        const ServerDependencies = Layer.mergeAll(
          PeerRpcLimits.layerDefaults,
          Layer.succeed(PeerAuthenticator.PeerAuthenticator)({
            authenticate: () =>
              Effect.succeed({
                principal: PeerAuthentication.PeerPrincipal.make({
                  tenantId: "tenant",
                  subjectId: "websocket-client",
                  peerId: clientPeerId
                }),
                validUntil: Number.MAX_SAFE_INTEGER,
                invalidated: Effect.never
              })
          }),
          PeerAuthorization.layer(() =>
            Effect.succeed({
              documents: [{ document: Task, documentId }],
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
          )
        )
        const AuthenticationLive = PeerAuthentication.layerServer.pipe(Layer.provide(ServerDependencies))
        const PeerHandlersLive = PeerRpcServer.layerHandlers({
          tenantId: "tenant",
          peerId: serverPeerId,
          definitionHash: definition.hash
        }).pipe(
          Layer.provide(ServerDependencies)
        )
        const WsProtocol = RpcServer.layerProtocolWebsocket({ path: "/rpc" }).pipe(Layer.provide(HttpRouter.layer))
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
        )).pipe(Effect.provide(serverEngineContext))
        const address = Context.get(serverContext, HttpServer.HttpServer).address
        if (address._tag !== "TcpAddress") return yield* Effect.die("Expected a TCP test server address")

        const CredentialsLive = Layer.succeed(PeerCredentials.PeerCredentials)({
          get: Effect.sync(() => Redacted.make("client-payload-credential"))
        })
        const ClientAuthenticationLive = PeerAuthentication.layerClient.pipe(Layer.provide(CredentialsLive))
        const buildClient = Effect.gen(function*() {
          const ClientProtocolLive = RpcClient.layerProtocolSocket().pipe(
            Layer.provide([
              NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`),
              RpcSerialization.layerMsgPack
            ])
          )
          const clientContext = yield* Layer.build(Layer.merge(ClientProtocolLive, ClientAuthenticationLive))
          return yield* PeerRpc.makeRpcClient.pipe(Effect.provide(clientContext))
        })

        // Establish a matched-build session and converge a mutation so there is real data to protect.
        yield* Effect.scoped(Effect.gen(function*() {
          const client = yield* buildClient
          yield* RpcPeerTransport.makeSession(client, {
            peerId: serverPeerId,
            documents: [{ document: Task, documentId }],
            definitionHash: definition.hash
          }).pipe(Effect.provide(clientEngineContext))
          yield* clientReplica.mutate(AddLabel, {
            commandId: yield* Identity.makeCommandId,
            documentId,
            payload: "matched"
          })
          let observed = false
          for (let attempt = 0; attempt < 300; attempt++) {
            yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
            observed = (yield* serverReplica.get(Task, documentId)).value.labels.includes("matched")
            if (observed) break
            yield* Effect.sleep("10 millis")
          }
          assert.isTrue(observed)
        }))

        const baseline = yield* serverReplica.get(Task, documentId)

        // A peer built against a newer wire protocol reconnects. The transport pins the
        // current protocol version, so drive Open directly to model the skewed build.
        yield* Effect.scoped(Effect.gen(function*() {
          const client = yield* buildClient
          const error = yield* client.Open({
            protocolVersion: PeerRpc.protocolVersion + 1,
            expectedPeerId: serverPeerId,
            definitionHash: definition.hash,
            documents: [{ documentType: Task.name, documentId }]
          }).pipe(Stream.runDrain, Effect.flip)
          assert.strictEqual(error._tag, "UnsupportedVersion")
        }))

        // The rejected handshake left no session and applied nothing.
        yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
        const afterSkew = yield* serverReplica.get(Task, documentId)
        assert.deepStrictEqual(afterSkew.value, baseline.value)
        assert.deepStrictEqual(afterSkew.heads, baseline.heads)

        // A matched-build client still converges afterwards: the skew was rejected, not corrupting.
        yield* Effect.scoped(Effect.gen(function*() {
          const client = yield* buildClient
          yield* RpcPeerTransport.makeSession(client, {
            peerId: serverPeerId,
            documents: [{ document: Task, documentId }],
            definitionHash: definition.hash
          }).pipe(Effect.provide(clientEngineContext))
          yield* clientReplica.mutate(AddLabel, {
            commandId: yield* Identity.makeCommandId,
            documentId,
            payload: "after-skew"
          })
          let converged = false
          for (let attempt = 0; attempt < 300; attempt++) {
            yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
            const serverSnapshot = yield* serverReplica.get(Task, documentId)
            const clientSnapshot = yield* clientReplica.get(Task, documentId)
            converged = serverSnapshot.value.labels.includes("after-skew") &&
              JSON.stringify(serverSnapshot.value) === JSON.stringify(clientSnapshot.value)
            if (converged) break
            yield* Effect.sleep("10 millis")
          }
          assert.isTrue(converged)
        }))
      })).pipe(Effect.provide([
        NodeCrypto.layer,
        ReplicaLimits.layer(TestReplica.defaultLimits)
      ])),
    20_000
  )

  it.live(
    "rejects a definition-hash-skewed peer at session open and syncs no data",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const SkewTask = Document.make("WebSocketSkewTask", {
          schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
          version: 1
        })
        const SkewAddLabel = Mutation.make("WebSocketSkewTask.AddLabel", { document: SkewTask, payload: Schema.String })
        const serverDefinition = ReplicaDefinition.make({
          name: "rpc-skew-test",
          documents: DocumentSet.make(SkewTask),
          mutations: [SkewAddLabel],
          projections: [],
          queries: []
        })
        const SkewTaskV2 = Document.make("WebSocketSkewTask", {
          schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
          version: 2
        })
        const SkewAddLabelV2 = Mutation.make("WebSocketSkewTask.AddLabel", {
          document: SkewTaskV2,
          payload: Schema.String
        })
        const clientDefinition = ReplicaDefinition.make({
          name: "rpc-skew-test",
          documents: DocumentSet.make(SkewTaskV2),
          mutations: [SkewAddLabelV2],
          projections: [],
          queries: []
        })
        assert.notStrictEqual(serverDefinition.hash, clientDefinition.hash)

        const ServerReplicaLive = TestReplica.layer(serverDefinition, { projections: [] }).pipe(
          Layer.provide(SkewAddLabel.toLayer(({ draft, payload }) => {
            draft.labels.push(payload)
            return undefined
          }))
        )
        const ClientReplicaLive = TestReplica.layer(clientDefinition, { projections: [] }).pipe(
          Layer.provide(SkewAddLabelV2.toLayer(({ draft, payload }) => {
            draft.labels.push(payload)
            return undefined
          }))
        )
        const serverEngineContext = yield* Layer.build(ServerReplicaLive)
        const clientEngineContext = yield* Layer.build(ClientReplicaLive)
        const serverReplica = Context.get(serverEngineContext, Replica.Replica)
        const clientReplica = Context.get(clientEngineContext, Replica.Replica)
        const serverSharding = Context.get(serverEngineContext, Sharding.Sharding)
        const clientSharding = Context.get(clientEngineContext, Sharding.Sharding)

        // Two skewed builds own the same logical document: seed the same deterministic id on both.
        const sharedCommandId = yield* Identity.makeCommandId
        const createdServer = yield* serverReplica.create(SkewTask, {
          commandId: sharedCommandId,
          value: { title: "seed", labels: [] }
        })
        const documentId = yield* CommandOutcome.committedOrFail(createdServer)
        const createdClient = yield* clientReplica.create(SkewTaskV2, {
          commandId: sharedCommandId,
          value: { title: "seed", labels: [] }
        })
        assert.strictEqual(documentId, yield* CommandOutcome.committedOrFail(createdClient))

        const ServerDependencies = Layer.mergeAll(
          PeerRpcLimits.layerDefaults,
          Layer.succeed(PeerAuthenticator.PeerAuthenticator)({
            authenticate: () =>
              Effect.succeed({
                principal: PeerAuthentication.PeerPrincipal.make({
                  tenantId: "tenant",
                  subjectId: "websocket-client",
                  peerId: clientPeerId
                }),
                validUntil: Number.MAX_SAFE_INTEGER,
                invalidated: Effect.never
              })
          }),
          PeerAuthorization.layer(() =>
            Effect.succeed({
              documents: [{ document: SkewTask, documentId }],
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
          )
        )
        const AuthenticationLive = PeerAuthentication.layerServer.pipe(Layer.provide(ServerDependencies))
        const PeerHandlersLive = PeerRpcServer.layerHandlers({
          tenantId: "tenant",
          peerId: serverPeerId,
          definitionHash: serverDefinition.hash
        }).pipe(
          Layer.provide(ServerDependencies)
        )
        const WsProtocol = RpcServer.layerProtocolWebsocket({ path: "/rpc" }).pipe(Layer.provide(HttpRouter.layer))
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
        )).pipe(Effect.provide(serverEngineContext))
        const address = Context.get(serverContext, HttpServer.HttpServer).address
        if (address._tag !== "TcpAddress") return yield* Effect.die("Expected a TCP test server address")

        const CredentialsLive = Layer.succeed(PeerCredentials.PeerCredentials)({
          get: Effect.sync(() => Redacted.make("client-payload-credential"))
        })
        const ClientAuthenticationLive = PeerAuthentication.layerClient.pipe(Layer.provide(CredentialsLive))

        const openExit = yield* Effect.scoped(Effect.gen(function*() {
          const ClientProtocolLive = RpcClient.layerProtocolSocket().pipe(
            Layer.provide([
              NodeSocket.layerWebSocket(`ws://127.0.0.1:${address.port}/rpc`),
              RpcSerialization.layerMsgPack
            ])
          )
          const clientContext = yield* Layer.build(Layer.merge(ClientProtocolLive, ClientAuthenticationLive))
          const client = yield* PeerRpc.makeRpcClient.pipe(Effect.provide(clientContext))
          return yield* RpcPeerTransport.makeSession(client, {
            peerId: serverPeerId,
            documents: [{ document: SkewTaskV2, documentId }],
            definitionHash: clientDefinition.hash
          }).pipe(Effect.provide(clientEngineContext))
        })).pipe(Effect.exit)

        assert.isTrue(Exit.isFailure(openExit), "skewed session must be rejected at open")
        if (Exit.isFailure(openExit)) {
          const error = Cause.findErrorOption(openExit.cause)
          assert.isTrue(Option.isSome(error) && error.value.reason._tag === "ProtocolMismatch")
        }

        // No data crossed the rejected boundary: a client mutation never reaches the server.
        yield* clientReplica.mutate(SkewAddLabelV2, {
          commandId: yield* Identity.makeCommandId,
          documentId,
          payload: "from-skewed-client"
        })
        for (let attempt = 0; attempt < 30; attempt++) {
          yield* Effect.all([serverSharding.pollStorage, clientSharding.pollStorage], { discard: true })
          yield* Effect.sleep("10 millis")
        }
        const serverSnapshot = yield* serverReplica.get(SkewTask, documentId)
        assert.deepStrictEqual(serverSnapshot.value.labels, [])
      })).pipe(Effect.provide([
        NodeCrypto.layer,
        ReplicaLimits.layer(TestReplica.defaultLimits)
      ])),
    30_000
  )
})
