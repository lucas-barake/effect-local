import { NodeCrypto, NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerAuthenticator from "@lucas-barake/effect-local-rpc/PeerAuthenticator"
import * as PeerRelayAuthorization from "@lucas-barake/effect-local-rpc/PeerRelayAuthorization"
import * as PeerRelayLimits from "@lucas-barake/effect-local-rpc/PeerRelayLimits"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as PeerRpcError from "@lucas-barake/effect-local-rpc/PeerRpcError"
import * as RelayServer from "@lucas-barake/effect-local-rpc/RelayServer"
import * as SqlRelayInboxStore from "@lucas-barake/effect-local-rpc/SqlRelayInboxStore"
import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import { createServer } from "node:http"
import { definition } from "./src/domain.ts"
import { devices } from "./src/identities.ts"

/**
 * A relay the browser fixture can actually connect to.
 *
 * Everything the deployment owns is supplied here, because that is the point: the package ships the
 * front door, the entity and the store, and leaves authentication, authorization, the socket and
 * the cluster to whoever runs it. This file is the smallest honest version of that.
 *
 * Single runner on purpose. Multi-runner ownership is covered by `RelayInboxMultiRunner.test.ts`
 * against PostgreSQL; what this fixture has to prove is that a browser client can talk to a relay
 * at all, so it uses in-memory cluster storage and one node.
 */

const port = Number(process.env.EFFECT_LOCAL_RELAY_PORT ?? 4176)

/**
 * Bearer tokens to principals.
 *
 * A real deployment verifies a signed token here. The fixture keeps a fixed table so the browser
 * can present a token that means something without this file growing a token issuer.
 */
const principals = new Map(devices.map((device) => [device.token, device.principal]))

const Authenticator = Layer.succeed(PeerAuthenticator.PeerAuthenticator)({
  authenticate: (secret) => {
    const principal = principals.get(Redacted.value(secret))
    return principal === undefined
      ? Effect.fail(new PeerRpcError.AuthenticationFailure())
      : Effect.succeed({
        principal,
        validUntil: Number.MAX_SAFE_INTEGER,
        invalidated: Effect.never
      })
  }
})

/**
 * Every authenticated device of this tenant may relay to any other, for the documents it asks for.
 *
 * The second callback is the one that matters here. Automerge 3.3.2 has no allocation bounded
 * decode, so relay send admission and recipient delivery both require an explicit
 * `UnsafeUnboundedAutomerge3DecodeGrant` on top of ordinary authorization. Default deny therefore
 * means a deployment cannot relay Automerge at all until it consciously accepts that risk.
 *
 * This fixture grants it because it owns both producers: they are two pages of this same test. A
 * product may only do the same where it has independently established that the producer's bytes are
 * resource trusted, and being authenticated or allowed to edit the document is not that.
 */
const Authorization = PeerRelayAuthorization.layer(
  (request) =>
    Effect.succeed({
      remote: {
        tenantId: request.principal.tenantId,
        subjectId: request.remote.subjectId,
        peerId: request.remote.peerId
      },
      // The relay resolves a requested `documentType` against the deployment's own definition. It
      // is the application that knows what a document type means; the package only routes bytes.
      documents: request.documents.flatMap((requested) => {
        const document = definition.documents.byName.get(requested.documentType)
        return document === undefined ? [] : [{ document, documentId: requested.documentId }]
      }),
      validUntil: Number.MAX_SAFE_INTEGER,
      invalidated: Effect.never
    }),
  (request) =>
    Effect.map(Clock.currentTimeMillis, (now) => ({
      _tag: "UnsafeUnboundedAutomerge3DecodeGrant" as const,
      risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
      principal: request.principal,
      remote: {
        tenantId: request.principal.tenantId,
        subjectId: request.remote.subjectId,
        peerId: request.remote.peerId
      },
      direction: request.direction,
      documents: request.documents,
      validUntil: now + 60_000,
      invalidated: Effect.never
    }))
)

const Cluster = Sharding.layer.pipe(
  Layer.provide(Runners.layerNoop),
  Layer.provideMerge(MessageStorage.layerMemory),
  Layer.provide(RunnerStorage.layerMemory),
  Layer.provide(RunnerHealth.layerNoop),
  Layer.provide(ShardingConfig.layer({ shardsPerGroup: 8, entityTerminationTimeout: 0 }))
)

const Relay = RelayServer.layer({
  tenantId: devices[0]!.principal.tenantId,
  peerId: devices[0]!.relayPeerId,
  heartbeatInterval: Duration.seconds(10),
  entityCallTimeout: Duration.seconds(30),
  inbox: {
    maxDeliveries: 16,
    messageTtl: Duration.minutes(30),
    terminalRetention: Duration.minutes(60),
    sessionDeadline: Duration.seconds(90),
    sessionSweep: Duration.seconds(5),
    maxConcurrentChannels: 8,
    storeRetry: Duration.seconds(1),
    maxPendingMessages: 1_000,
    maxPendingBytes: 32 * 1024 * 1024,
    mailboxCapacity: 64,
    maxIdleTime: Duration.minutes(30)
  },
  maintenance: {
    interval: Duration.minutes(1),
    batchLimit: 500,
    terminalRetention: Duration.minutes(60),
    enabled: true
  }
}).pipe(
  Layer.provide(Layer.orDie(SqlRelayInboxStore.layer)),
  Layer.provide(PeerRelayLimits.layer(PeerRelayLimits.defaults)),
  Layer.provide(Authorization),
  Layer.provide(Cluster),
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer)
)

// The websocket front door. `RpcServer` owns the request lifecycle; the upgrade, the port and the
// process belong to the deployment, which is why they are written out here rather than shipped.
const Rpc = RpcServer.layer(PeerRpc.Rpcs).pipe(
  Layer.provide(Relay),
  Layer.provide(PeerAuthentication.layerServer),
  Layer.provide(Authenticator),
  Layer.provide(PeerRelayLimits.layer(PeerRelayLimits.defaults)),
  Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/relay" })),
  Layer.provide(RpcSerialization.layerJson)
)

// A plain health route so the harness can wait for readiness without opening a websocket.
const Ready = HttpRouter.add("GET", "/ready", HttpServerResponse.text("ok"))

HttpRouter.serve(Layer.mergeAll(Rpc, Ready)).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port, host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
