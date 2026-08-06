import { NodeCrypto, NodeHttpClient, NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerAuthenticator from "@lucas-barake/effect-local-rpc/PeerAuthenticator"
import * as PeerRelayAuthorization from "@lucas-barake/effect-local-rpc/PeerRelayAuthorization"
import * as PeerRelayLimits from "@lucas-barake/effect-local-rpc/PeerRelayLimits"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as PeerRpcError from "@lucas-barake/effect-local-rpc/PeerRpcError"
import * as RelayServer from "@lucas-barake/effect-local-rpc/RelayServer"
import * as SqlRelayInboxStore from "@lucas-barake/effect-local-rpc/SqlRelayInboxStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization"
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { createServer } from "node:http"
import { Conversation, definition, DomainLive, limits, sqlProjections } from "../src/shared/domain.ts"
import {
  channels,
  conversationCommandId,
  conversationDocumentId,
  relayPeerId,
  tenantId,
  users
} from "../src/shared/identities.ts"

const port = Number(process.env.CHAT_RELAY_PORT ?? 8787)
const databaseUrl = process.env.DATABASE_URL ?? "postgres://chat:chat@localhost:5433/chat"
const otlpUrl = process.env.OTLP_URL ?? "http://localhost:4318/v1/traces"

const Sql = Layer.orDie(PgClient.layer({ url: Redacted.make(databaseUrl) }))

const Telemetry = OtlpTracer.layer({
  url: otlpUrl,
  resource: { serviceName: "chat-relay" },
  exportInterval: "1 second"
}).pipe(
  Layer.provide(OtlpSerialization.layerJson),
  Layer.provide(NodeHttpClient.layerUndici)
)

const principals = new Map(
  channels.flatMap((channel) => [
    [channel.left.token, channel.left.principal] as const,
    [channel.right.token, channel.right.principal] as const
  ])
)

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

interface ChannelPolicy {
  readonly documentId: Identity.DocumentId
  readonly remote: { readonly subjectId: string; readonly peerId: Identity.PeerId }
}

/** Endpoint peer id → the one conversation document and the one remote endpoint it may talk to. */
const channelDirectory = Effect.gen(function*() {
  const byPeerId = new Map<string, ChannelPolicy>()
  for (const channel of channels) {
    const documentId = yield* conversationDocumentId(channel.pair[0], channel.pair[1])
    byPeerId.set(channel.left.principal.peerId, {
      documentId,
      remote: { subjectId: channel.right.principal.subjectId, peerId: channel.right.principal.peerId }
    })
    byPeerId.set(channel.right.principal.peerId, {
      documentId,
      remote: { subjectId: channel.left.principal.subjectId, peerId: channel.left.principal.peerId }
    })
  }
  return byPeerId
})

/**
 * An endpoint may talk to exactly its pair counterpart about exactly the pair's conversation
 * document. Automerge 3.3.2 has no allocation bounded decode, so relaying it at all additionally
 * requires the explicit `UnsafeUnboundedAutomerge3DecodeGrant`; acceptable here because every
 * producer is one of the hardcoded demo identities.
 */
const Authorization = Layer.unwrap(
  Effect.map(channelDirectory, (byPeerId) =>
    PeerRelayAuthorization.layer(
      (request) => {
        const policy = byPeerId.get(request.principal.peerId)
        const allowed = policy !== undefined &&
            policy.remote.subjectId === request.remote.subjectId &&
            policy.remote.peerId === request.remote.peerId
          ? policy.documentId
          : undefined
        return Effect.succeed({
          remote: {
            tenantId: request.principal.tenantId,
            subjectId: request.remote.subjectId,
            peerId: request.remote.peerId
          },
          documents: request.documents.flatMap((requested) => {
            if (requested.documentId !== allowed) return []
            const document = definition.documents.byName.get(requested.documentType)
            return document === undefined ? [] : [{ document, documentId: requested.documentId }]
          }),
          validUntil: Number.MAX_SAFE_INTEGER,
          invalidated: Effect.never
        })
      },
      (request) =>
        Effect.map(Clock.currentTimeMillis, (now) =>
          PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeGrant.make({
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
    ))
)

const Cluster = Sharding.layer.pipe(
  Layer.provide(Runners.layerNoop),
  Layer.provideMerge(MessageStorage.layerMemory),
  Layer.provide(RunnerStorage.layerMemory),
  Layer.provide(RunnerHealth.layerNoop),
  Layer.provide(ShardingConfig.layer({ shardsPerGroup: 8, entityTerminationTimeout: 0 }))
)

/**
 * 3 steady-state sessions per subject (one per counterpart) plus reconnect and SharedWorker
 * takeover overlap: the default cap of 4 refuses the overlap, so raise it well clear.
 */
const relayLimits = { ...PeerRelayLimits.defaults, maxSessionsPerSubject: 16 }

const Relay = RelayServer.layer({
  tenantId,
  peerId: relayPeerId,
  heartbeatInterval: Duration.seconds(10),
  entityCallTimeout: Duration.seconds(30),
  inbox: {
    maxDeliveries: 64,
    // Presence heartbeats stream through the same inboxes as messages, and an offline user keeps
    // accumulating them; a chat message must survive a recipient who is gone for hours. The
    // handshake requires receiptRetention (client default 8d) >= max(messageTtl, retryHorizon 7d)
    // + terminalRetention, so the day long TTL leaves 12h of terminal retention inside the window.
    messageTtl: Duration.days(1),
    terminalRetention: Duration.hours(12),
    sessionDeadline: Duration.seconds(90),
    sessionSweep: Duration.seconds(5),
    maxConcurrentChannels: 8,
    storeRetry: Duration.seconds(1),
    maxPendingMessages: 20_000,
    maxPendingBytes: 128 * 1024 * 1024,
    mailboxCapacity: 64,
    maxIdleTime: Duration.minutes(30)
  },
  maintenance: {
    interval: Duration.minutes(1),
    batchLimit: 500,
    terminalRetention: Duration.hours(12),
    enabled: true
  }
}).pipe(
  Layer.provide(Layer.orDie(SqlRelayInboxStore.layer)),
  Layer.provide(PeerRelayLimits.layer(relayLimits)),
  Layer.provide(Authorization),
  Layer.provide(Cluster),
  Layer.provide(Sql),
  Layer.provide(NodeCrypto.layer)
)

const Rpc = RpcServer.layer(PeerRpc.Rpcs).pipe(
  Layer.provide(Relay),
  Layer.provide(PeerAuthentication.layerServer),
  Layer.provide(Authenticator),
  Layer.provide(PeerRelayLimits.layer(relayLimits)),
  Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/relay" })),
  Layer.provide(RpcSerialization.layerJson)
)

/**
 * Conversation provisioning. Peer sync can only converge documents both replicas already hold, and
 * two replicas independently creating the same document id fork the Automerge genesis (one side's
 * writes are shadowed on merge, silently). So exactly one replica ever creates the conversation
 * documents: an ephemeral seeder built here, whose exported backup every browser replica restores
 * at first launch. The archive must be generated once and persisted — an archive regenerated on a
 * later boot would carry a different genesis and fork users bootstrapped before the restart.
 */
const generateSeedArchive = Effect.gen(function*() {
  const SqliteMem = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  const SeederLive = SqlReplica.layerWithBindings(definition, { projections: sqlProjections }).pipe(
    Layer.provideMerge(Layer.mergeAll(
      SqliteMem,
      NodeCrypto.layer,
      DomainLive.pipe(Layer.provide(SqliteMem)),
      ReplicaLimits.layer(limits)
    )),
    Layer.orDie
  )
  const context = yield* Layer.build(SeederLive)
  const replica = Context.get(context, Replica.Replica)
  for (const [index, left] of users.entries()) {
    for (const right of users.slice(index + 1)) {
      const commandId = yield* conversationCommandId(left.id, right.id)
      yield* replica.create(Conversation, { commandId, value: {} })
    }
  }
  const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
  const joined = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.byteLength, 0))
  let offset = 0
  for (const chunk of chunks) {
    joined.set(chunk, offset)
    offset += chunk.byteLength
  }
  return joined
}).pipe(Effect.scoped)

class SeedArchive extends Context.Service<SeedArchive, { readonly bytes: Uint8Array }>()(
  "chat-example/SeedArchive"
) {}

const SeedRow = Schema.Struct({ archive: Schema.Uint8ArrayFromBase64 })

const SeedArchiveLive = Layer.effect(SeedArchive)(
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE IF NOT EXISTS chat_seed (seed_key TEXT PRIMARY KEY, archive TEXT NOT NULL)`
    // Keyed by the definition hash and the roster: the archive must stay byte-stable for a given
    // domain (a regenerated archive would fork geneses across server restarts), but a changed
    // schema or roster must mint a fresh archive or restoreBackup rejects the stale manifest on
    // every client with nothing pointing at the seed. Never delete a seed row while relay inboxes
    // still hold traffic minted under it: replicas provisioned from a regenerated archive would
    // receive old-genesis changes as a shadowing register conflict.
    const seedKey = `${definition.hash}|${users.map((user) => user.id).join(",")}`
    const findSeed = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: SeedRow,
      execute: () => sql`SELECT archive FROM chat_seed WHERE seed_key = ${seedKey}`
    })
    const existing = yield* findSeed(undefined)
    if (Option.isSome(existing)) return { bytes: existing.value.archive }
    const generated = yield* generateSeedArchive
    const encoded = Schema.encodeSync(SeedRow)({ archive: generated }).archive
    // ON CONFLICT DO NOTHING and a reread: if two server processes race the generation, both must
    // serve the row that won, not the archive they generated.
    yield* sql`INSERT INTO chat_seed (seed_key, archive) VALUES (${seedKey}, ${encoded}) ON CONFLICT (seed_key) DO NOTHING`
    const stored = yield* findSeed(undefined)
    return { bytes: Option.isSome(stored) ? stored.value.archive : generated }
  }).pipe(Effect.orDie)
).pipe(Layer.provide(Sql))

// The browser fetches the seed from the Vite origin, so the response needs CORS.
const Seed = HttpRouter.add(
  "GET",
  "/seed",
  Effect.gen(function*() {
    const { bytes } = yield* SeedArchive
    return HttpServerResponse.uint8Array(bytes, {
      contentType: "application/octet-stream",
      headers: { "access-control-allow-origin": "*" }
    })
  })
)

const Ready = HttpRouter.add("GET", "/ready", HttpServerResponse.text("ok"))

/**
 * `NodeHttpServer.layer` starts listening as soon as its layer builds, but the router's handler is
 * attached later, once the backend layers (Postgres pool, inbox migrations, cluster, seed
 * provisioning) have built. Node drops a `request` event that fires with no listener, so a request
 * accepted in that window would never get a response and its connection would hang — readiness
 * probes with no request timeout hang with it. This fallback answers 503 until the real handler is
 * attached; it stays registered afterwards but yields as soon as it sees the second listener.
 */
const createGatedServer = () => {
  const server = createServer()
  server.on("request", (_request, response) => {
    if (server.listenerCount("request") > 1) return
    response.writeHead(503, { "retry-after": "1" }).end()
  })
  return server
}

HttpRouter.serve(Layer.mergeAll(Rpc, Seed, Ready)).pipe(
  Layer.provide(SeedArchiveLive),
  Layer.provide(NodeHttpServer.layer(createGatedServer, { port, host: "0.0.0.0" })),
  Layer.provide(Telemetry),
  Layer.launch,
  NodeRuntime.runMain
)
