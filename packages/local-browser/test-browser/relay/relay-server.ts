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
 * Everything the deployment owns, written out: the package ships the front door, the entity and
 * the store, and leaves authentication, authorization, the socket and the cluster to whoever runs
 * it. Single runner on purpose - multi-runner ownership is covered by `RelayInboxMultiRunner`.
 */

const port = Number(process.env.EFFECT_LOCAL_RELAY_PORT ?? 4176)

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
 * Automerge 3.3.2 has no allocation bounded decode, so relaying it at all requires an explicit
 * `UnsafeUnboundedAutomerge3DecodeGrant` on top of ordinary authorization. Granted here because
 * both producers are pages of this same test; a product needs the producer's bytes to be resource
 * trusted, which being authenticated is not.
 */
const Authorization = PeerRelayAuthorization.layer(
  (request) =>
    Effect.succeed({
      remote: {
        tenantId: request.principal.tenantId,
        subjectId: request.remote.subjectId,
        peerId: request.remote.peerId
      },
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

const Rpc = RpcServer.layer(PeerRpc.Rpcs).pipe(
  Layer.provide(Relay),
  Layer.provide(PeerAuthentication.layerServer),
  Layer.provide(Authenticator),
  Layer.provide(PeerRelayLimits.layer(PeerRelayLimits.defaults)),
  Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/relay" })),
  Layer.provide(RpcSerialization.layerJson)
)

// Readiness without opening a websocket.
const Ready = HttpRouter.add("GET", "/ready", HttpServerResponse.text("ok"))

HttpRouter.serve(Layer.mergeAll(Rpc, Ready)).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port, host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
