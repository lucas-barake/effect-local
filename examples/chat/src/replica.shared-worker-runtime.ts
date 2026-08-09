import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserSocket from "@effect/platform-browser/BrowserSocket"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import type * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as SessionManager from "@lucas-barake/effect-local-browser/SessionManager"
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerCredentials from "@lucas-barake/effect-local-rpc/PeerCredentials"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Redacted from "effect/Redacted"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { Conversation, definition, DomainLive, limits, sqlProjections } from "./shared/domain.ts"
import {
  type ChatUser,
  conversationDocumentId,
  counterpartsOf,
  endpointFor,
  relayPeerId,
  userById
} from "./shared/identities.ts"

declare const self: SharedWorkerGlobalScope

/**
 * The tab constructs this SharedWorker with the name `chat<generation>-<user>`, which is the
 * worker's whole identity: same script, different name, different worker. Each user runs its own
 * engine, database, and relay connections.
 */
const userId = self.name.slice(self.name.indexOf("-") + 1)
const me = userById(userId)

const relayPort = import.meta.env.VITE_RELAY_PORT ?? "8787"
const relayHost = `${self.location.hostname}:${relayPort}`

class RelayClient extends Context.Service<RelayClient, PeerRpc.RpcClient>()(
  "chat-example/RelayClient"
) {}

/**
 * One socket, client, and session per counterpart, because the relay allows one live session per
 * peer endpoint and a session pushes to one remote — so each conversation runs over its own
 * pair-scoped endpoint (see identities.ts).
 */
const linkLayer = (token: string) =>
  Layer.effect(RelayClient)(PeerRpc.makeRpcClient).pipe(
    Layer.provideMerge(RelayConnectionStatus.layerProtocolSocket()),
    Layer.provide(BrowserSocket.layerWebSocket(`ws://${relayHost}/relay`)),
    Layer.provide(RpcSerialization.layerJson),
    Layer.provide(PeerAuthentication.layerClient),
    Layer.provide(
      Layer.succeed(PeerCredentials.PeerCredentials)({
        get: Effect.succeed(Redacted.make(token))
      })
    )
  )

interface RelayLink {
  readonly counterpart: ChatUser
  readonly client: PeerRpc.RpcClient
  readonly status: Stream.Stream<RelayConnectionStatus.Status, unknown>
}

class RelayLinks extends Context.Service<RelayLinks, ReadonlyArray<RelayLink>>()(
  "chat-example/RelayLinks"
) {}

const RelayLinksLive = Layer.effect(RelayLinks)(
  Effect.forEach(
    counterpartsOf(me.id),
    (counterpart) =>
      Layer.build(linkLayer(endpointFor(me.id, counterpart.id).token)).pipe(
        Effect.map((context) => ({
          counterpart,
          client: Context.get(context, RelayClient),
          status: Context.get(context, RelayConnectionStatus.RelayConnectionStatus).status
        }))
      )
  )
)

const statusSeverity = { Connected: 0, NotConfigured: 1, Connecting: 2, Disconnected: 3 } as const

/**
 * The engine contract wants one relay status; with one socket per counterpart the honest answer
 * is the worst of them. All sockets point at the same relay, so they normally agree.
 */
const AggregateStatusLive = Layer.effect(RelayConnectionStatus.RelayConnectionStatus)(
  Effect.gen(function*() {
    const links = yield* RelayLinks
    // Suspended so each subscription owns its own accumulator: the owner opens one status
    // subscription per attached tab, and a shared array would let a slow subscriber overwrite a
    // newer value for everyone else.
    const status = Stream.suspend(() => {
      const latest: Array<RelayConnectionStatus.Status> = links.map(() => RelayConnectionStatus.disconnected)
      return Stream.mergeAll(
        links.map((link, index) =>
          link.status.pipe(
            Stream.orDie,
            Stream.map((value) => ({ index, value }))
          )
        ),
        { concurrency: "unbounded" }
      ).pipe(
        Stream.map(({ index, value }) => {
          latest[index] = value
          return latest.reduce((worst, candidate) =>
            statusSeverity[candidate._tag] > statusSeverity[worst._tag] ? candidate : worst
          )
        }),
        Stream.changes
      )
    })
    return { status }
  })
)

const fetchSeed = Effect.tryPromise(async () => {
  const response = await fetch(`http://${relayHost}/seed`)
  if (!response.ok) throw new Error(`seed request failed with ${response.status}`)
  return new Uint8Array(await response.arrayBuffer())
})

const ensureSeeded = Effect.gen(function*() {
  const replica = yield* Replica.Replica
  const expected = yield* Effect.forEach(
    counterpartsOf(me.id),
    (counterpart) => conversationDocumentId(me.id, counterpart.id)
  )
  const missing = yield* Effect.filter(expected, (documentId) =>
    replica.get(Conversation, documentId).pipe(
      Effect.as(false),
      Effect.catchReason("ReplicaError", "DocumentNotFound", () => Effect.succeed(true))
    ))
  if (missing.length === 0) return
  const bytes = yield* fetchSeed.pipe(
    Effect.retry(Schedule.spaced(Duration.seconds(3)))
  )
  yield* Effect.forEach(missing, (documentId) =>
    Effect.gen(function*() {
      const installationId = yield* Identity.makeBackupInstallationId
      yield* replica.installBackupDocument(Conversation, {
        documentId,
        expectedDefinitionHash: definition.hash,
        installationId,
        maxBytes: limits.maxBackupBytes,
        source: Stream.make(bytes)
      }).pipe(
        Effect.retry({
          while: (error) => error.reason._tag === "StorageUnavailable" || error.reason._tag === "QuotaExceeded",
          schedule: Schedule.spaced(Duration.seconds(3))
        })
      )
    }), { discard: true })
  yield* Effect.log(`chat replica installed ${missing.length} missing seed conversations`)
})

const sessionOptions = (
  counterpart: ChatUser,
  documentId: Identity.DocumentId,
  senderReplicaIncarnation: Identity.ReplicaIncarnation
): RpcPeerTransport.Options => ({
  expectedLocal: endpointFor(me.id, counterpart.id).principal,
  senderReplicaIncarnation,
  expectedRelayPeerId: relayPeerId,
  remote: {
    subjectId: endpointFor(counterpart.id, me.id).principal.subjectId,
    peerId: endpointFor(counterpart.id, me.id).principal.peerId
  },
  documents: [{ document: Conversation, documentId }],
  definition,
  receiptRetentionMillis: PeerRelayReceiptLimits.defaults.receiptRetentionMillis,
  senderRetryHorizonMillis: Duration.toMillis(Duration.days(7)),
  replayBatchSize: 16
})

/**
 * Holds each channel's session open for the engine's lifetime. `makeSession` builds a live
 * session that subscribes to the commit publisher and pushes every local commit on its own; this
 * loop only opens it, primes an initial exchange, and reopens it after any disconnect or failure.
 */
const superviseSession = (link: RelayLink) =>
  Effect.gen(function*() {
    const documentId = yield* conversationDocumentId(me.id, link.counterpart.id)
    const attempt = Effect.scoped(
      Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const incarnation = (yield* gate.current).incarnation
        const session = yield* RpcPeerTransport.makeSession(
          link.client,
          sessionOptions(link.counterpart, documentId, incarnation)
        )
        yield* session.markDirty(documentId)
        yield* session.flush
        yield* session.awaitDisconnect
      })
    )
    yield* attempt.pipe(
      Effect.catchCause((cause) => Effect.logDebug("relay session ended, reopening", cause)),
      Effect.andThen(Effect.sleep(Duration.seconds(2))),
      Effect.forever
    )
  })

const Bootstrap = Layer.effectDiscard(
  Effect.gen(function*() {
    const links = yield* RelayLinks
    const daemon = ensureSeeded.pipe(
      Effect.tapCause((cause) => Effect.logError("chat provisioning failed; the app cannot sync", cause)),
      Effect.andThen(Effect.forEach(links, superviseSession, { concurrency: "unbounded" }))
    )
    yield* Effect.forkScoped(daemon)
  })
)

const MetadataRow = Schema.Struct({
  replica_id: Schema.String,
  writer_generation: Identity.WriterGeneration
})

const OwnerInfo = Schema.Struct({
  replicaId: Schema.String,
  writerGeneration: Identity.WriterGeneration
})

const makeEngine = (databasePort: MessagePort) => {
  const DatabaseLive = BrowserSqlite.layerMessagePort(databasePort)
  const Dependencies = Layer.mergeAll(
    DatabaseLive,
    BrowserCrypto.layer,
    DomainLive.pipe(Layer.provide(DatabaseLive)),
    ReplicaLimits.layer(limits),
    PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults),
    PeerRelayOutboxLimits.layer(PeerRelayOutboxLimits.defaults)
  )
  // `provideMerge` for the status aggregate is load-bearing: `layerRelayWithBindings` deliberately
  // does not provide `RelayConnectionStatus`, and the coordinator's engine contract requires it.
  const EngineLive = Bootstrap.pipe(
    Layer.provideMerge(PeerRelayClientRuntime.layerSql),
    Layer.provideMerge(Layer.merge(
      SqlReplica.layerRelayWithBindings(definition, { projections: sqlProjections }),
      SessionManager.layer
    )),
    Layer.provideMerge(AggregateStatusLive),
    Layer.provideMerge(RelayLinksLive),
    Layer.provideMerge(Dependencies),
    Layer.orDie
  )
  return ManagedRuntime.make(EngineLive)
}

export const options: OwnershipCoordinator.SharedWorkerOptions<unknown, typeof OwnerInfo.Type, unknown> = {
  name: self.name,
  definition,
  engine: makeEngine,
  info: {
    schema: OwnerInfo,
    make: Effect.flatMap(SqlClient.SqlClient, (sql) =>
      SqlSchema.findOne({
        Request: Schema.Void,
        Result: MetadataRow,
        execute: () => sql`SELECT replica_id, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      })(undefined).pipe(
        Effect.map((row) => ({ replicaId: row.replica_id, writerGeneration: row.writer_generation }))
      ))
  }
}
