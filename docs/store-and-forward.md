# Store and forward

## Contract

Store and forward is the RPC synchronization topology for SQL replicas. Protocol version `1` adds durable sender
admission, durable relay custody, reconnect delivery, and durable recipient receipts. There is no separate direct
protocol or topology.

The delivery guarantee is at least once. A successful `Push` means the relay SQL authority committed
the complete envelope before replying. A recipient can therefore see a duplicate after a lost response, expired
claim, interrupted acknowledgement, or reconnect. The recipient SQL path makes that duplicate safe within the
negotiated receipt retention window. Effect Local does not claim exactly once delivery.

Store and forward does not make the relay authoritative for document contents. Each replica remains authoritative for
its local writes. Automerge remains responsible for convergence after valid changes reach a replica.

## Composition

The application supplies relay policy, custody, limits, authentication, SQL, a socket, and a cluster. The server
composes `RelayServer.layer`, which is the front door handlers plus the `RelayInbox` entity behaviour plus the
retention singleton. The relay requires a `Sharding` and never builds one, so the deployment shape — single process
in memory, one runner over SQL, or many sharded runners — stays the application's choice.

On the client, `SqlReplica.layerRelay` (or `layerRelayWithBindings` when there are projections) installs relay receipt
support. `PeerRelayClientRuntime.layerSql` stores stable sender admissions in the SQL outbox and supervises sender
outbox and receipt maintenance. `RpcPeerTransport.makeSession` binds the generated relay client to the ordinary
`PeerSession`.

This example shows the server and client composition. The named application values provide SQL, Crypto,
authentication, socket, cluster, definition, projection, and mutation or query Layers.

```ts
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerRelayAuthorization from "@lucas-barake/effect-local-rpc/PeerRelayAuthorization"
import * as PeerRelayLimits from "@lucas-barake/effect-local-rpc/PeerRelayLimits"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as RelayServer from "@lucas-barake/effect-local-rpc/RelayServer"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import * as SqlRelayInboxStore from "@lucas-barake/effect-local-rpc/SqlRelayInboxStore"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import { Duration, Effect, Layer } from "effect"
import { RpcSerialization, RpcServer } from "effect/unstable/rpc"

const RelayLive = RelayServer.layer({
  tenantId,
  peerId: relayPeerId,
  heartbeatInterval: Duration.seconds(30),
  entityCallTimeout: Duration.seconds(30),
  inbox: {
    maxDeliveries: 16,
    messageTtl: Duration.days(7),
    terminalRetention: Duration.days(8),
    sessionDeadline: Duration.seconds(90),
    sessionSweep: Duration.seconds(5),
    maxConcurrentChannels: 8,
    storeRetry: Duration.seconds(1),
    maxPendingMessages: 10_000,
    maxPendingBytes: 256 * 1_024 * 1_024,
    mailboxCapacity: 64,
    maxIdleTime: Duration.minutes(30)
  },
  // Required, not optional. Without it messageTtl and terminalRetention are inert: nothing expires,
  // nothing is collected, and the retained row count climbs until admission is refused.
  maintenance: {
    interval: Duration.minutes(1),
    batchLimit: 500,
    terminalRetention: Duration.days(8),
    enabled: true
  }
}).pipe(
  Layer.provide(SqlRelayInboxStore.layer),
  Layer.provide(PeerRelayLimits.layer(relayLimits)),
  Layer.provide(PeerRelayAuthorization.layer(
    authorizeRelay,
    PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode
  )),
  // Supplied by the application: SingleRunner for one node, SocketRunner for many.
  Layer.provide(shardingLive)
)

const ServerLive = RpcServer.layer(PeerRpc.Rpcs).pipe(
  Layer.provide(RelayLive),
  Layer.provide(PeerAuthentication.layerServer),
  Layer.provide(RpcServer.layerProtocolSocketServer),
  Layer.provide(RpcSerialization.layerJson),
  Layer.provide(socketServerLive)
)

const ReceiptLimitsLive = PeerRelayReceiptLimits.layer(receiptLimits)
// `layerRelayWithBindings` rather than `layerRelay` whenever there are projections: it provides the
// binding Layers, exactly as `layerWithBindings` does for the direct topology. Reaching for
// `layerWithBindings` here instead is the easy mistake, and it builds a direct PeerSync that
// PeerRelayClientRuntime then refuses.
const RelayReplicaLive = SqlReplica.layerRelayWithBindings(definition, { projections }).pipe(
  Layer.provideMerge(ReceiptLimitsLive)
)
const RelayClientLive = PeerRelayClientRuntime.layerSql.pipe(
  Layer.provideMerge(RelayReplicaLive),
  Layer.provideMerge(PeerRelayOutboxLimits.layer(outboxLimits))
)

const session = RpcPeerTransport.makeSession(relayRpcClient, {
  expectedLocal,
  senderReplicaIncarnation,
  expectedRelayPeerId: relayPeerId,
  remote: { subjectId: remoteSubjectId, peerId: remotePeerId },
  documents: selectedDocuments,
  definition,
  receiptRetentionMillis,
  senderRetryHorizonMillis,
  replayBatchSize
}).pipe(Effect.provide(RelayClientLive))
```

`relayRpcClient` must be a `PeerRpc.RpcClient` created with `PeerRpc.makeRpcClient` over
`RpcClient.layerProtocolSocket()` with a matching `RpcSerialization` — the same codec the server uses. The server
composition still requires the application SQL and Crypto Layers, `PeerAuthentication.layerServer`, the relay
authorization Layer, a `PeerRelayLimits` Layer, and a `Sharding`. The client composition still requires the
application SQL, Crypto, core replica limits, generated client authentication middleware, and a scoped relay socket.

`RelayServer.layer` refuses to build when `heartbeatInterval * 2` is not less than `inbox.sessionDeadline`, or when
`entityCallTimeout` is not positive. Those two values have to agree or every session is reaped on a fixed cycle and
no client can hold a delivery stream, so the mistake is refused where it is written rather than discovered in
production.

Every configured duration takes `Duration.Input`, so `"30 seconds"`, `Duration.seconds(30)` and `30_000` are
equivalent. Values that cross the wire contract, such as `receiptRetentionMillis` above, keep the unit in the name
because `Duration` has no stable serialized encoding.

The example denies the separate unsafe Automerge decode grant. That is the required default. Ordinary authentication
and `authorizeRelay` document authorization do not promote a caller into resource trust.

The `expectedRelayPeerId`, `expectedLocal`, exact remote subject and peer, replica incarnation, selected documents,
receipt retention, and sender retry horizon are part of the version `1` handshake. A mismatch fails the connection.

### In the browser

Nothing above is node specific. No `packages/*/src` module imports `node:` anything, and the client custody tables are
part of the shared migration set the browser already applies, so `PeerRelayOutbox` and `PeerRelayClientRuntime` run in
a browser over `BrowserSqlite` and `BrowserCrypto` exactly as they do on a server.

Two placement rules matter more than the wiring.

The relay session belongs to the **owner**, not to a tab. `BrowserReplica` and `ReplicaOwner` are the two halves of a
tab to SharedWorker RPC, not a network transport; the owner's runtime is the only place that holds `SqlClient`,
`ReplicaGate`, `PeerSync`, `CommitPublisher` and a `Sharding`, which is exactly what `RpcPeerTransport.makeSession`
requires. Opening a session per attached tab would make one device several senders.

The owner's replica has to be built relay flavoured. A browser app almost always has projections, so that means
`SqlReplica.layerRelayWithBindings`. `PeerRelayClientRuntime` fails at construction on a direct `PeerSync` rather than
degrading, so this is caught immediately, but it is caught in a service the consumer did not think it was choosing.

The socket is the platform's, per the responsibility table in the README: this package does not ship a WebSocket
client. `@effect/platform-browser` already provides one.

```ts
import * as BrowserSocket from "@effect/platform-browser/BrowserSocket"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import { RpcClient, RpcSerialization } from "effect/unstable/rpc"

// One socket for the device. The transport is provided around the work that USES the sessions, not
// around `makeRpcClient` alone.
const RelayLink = Layer.effect(RelayClient)(PeerRpc.makeRpcClient).pipe(
  // Not `RpcClient.layerProtocolSocket`: this builds the protocol and the relay status reader
  // together, which is the only composition that cannot be wired up wrong.
  Layer.provideMerge(RelayConnectionStatus.layerProtocolSocket()),
  Layer.provide(BrowserSocket.layerWebSocket(relayUrl)),
  Layer.provide(RpcSerialization.layerJson),
  Layer.provide(PeerAuthentication.layerClient),
  Layer.provide(Layer.succeed(PeerCredentials.PeerCredentials)(credentials))
)

// Sessions are layered over that one client. Opening a session is what registers a peer connection
// attempt, so it stays here rather than moving up with the socket.
const relaySession = (documents: ReadonlyArray<PeerSession.SelectedDocument>) =>
  Effect.gen(function*() {
    const client = yield* RelayClient
    const session = yield* RpcPeerTransport.makeSession(client, relayOptions(documents))
    yield* useTheSession(session)
  })
```

One socket carries as many sessions as you need. The relay multiplexes them: each `Open` gets its own
session id, and the server tracks them independently of which connection they arrived on. A socket
per session pays for a TCP and WebSocket handshake per document set and buys nothing. The limit that
actually applies is `maxSessionsPerSubject` (default 4), which the relay counts per tenant and
subject regardless of how many connections carry them.

The scoping above is load bearing and the mistake it avoids is silent. A `Layer` provided to
`makeRpcClient` alone lives exactly as long as that constructor: the socket and its pump are built,
a client is handed back, and both are released before the client is ever used. What is left is a
client whose every request waits forever on a transport that no longer exists - no socket is opened,
nothing is logged, and nothing fails. Provide the transport around the work, not around the
constructor.

`credentials` is the application's. `PeerAuthentication.layerClient` calls `PeerCredentials.get` once per request, so a
`Ref` or `Deferred` backed implementation that the page refreshes is a valid shape. Supplying it inside a SharedWorker
is the part with no answer in this package: `SharedWorkerGlobalScope` has no `localStorage` and no cookie jar, so the
token has to travel from the page across the existing `MessagePort`, and `ReplicaRpc` has no slot for it today.

Whether the socket itself is up is reported by `RelayConnectionStatus`, built by the same
`layerProtocolSocket` above. It is deliberately separate from `PeerConnectionStatus`: the relay link
is one socket shared by every peer session, so when it drops they all go quiet at once, and per peer
status alone would render that as several peers vanishing rather than as one link failing. The two
`Status` types are branded so neither can be passed where the other belongs.

Reconnect is likewise the application's. `SupervisedPeerSession.awaitDisconnect` reports the disconnect and
`RpcPeerTransport.isRetryable` classifies whether the failure is worth retrying, but the schedule is a deployment
policy, not a library default. One detail is easy to get wrong: `senderReplicaIncarnation` is captured in
`RpcPeerTransport.Options` and validated against the live `ReplicaGate`, and a restore bumps the incarnation. A
supervisor that holds one options value across attempts works until the first restore and then fails every reconnect,
so rebuild the options from `gate.current` on each attempt.

## Protocol and durable identity

`PeerRpc.Rpcs` contains `Open`, `Push`, `Acknowledge`, and `Reject`. Every request uses the
same required authentication middleware. The first `Open` stream value is `Opened`. Later
values are `StoredMessage` deliveries.

Each sender admission has one stable `RelayMessageId`. The sender outbox persists that identity, the exact relay and
recipient endpoint, the sender replica incarnation and connection sequence, document identity, writer provenance,
message hash, payload, and canonical outer envelope digest before the network call. Retrying a pending row sends the
same identity and envelope to the same relay endpoint.

The relay admits an envelope only after Schema decoding, authentication, send authorization, digest verification,
payload validation, document selection validation, and quota reservation. Duplicate admission with the same identity
and digest is safe. Conflicting reuse is rejected.

## Acknowledgement boundary

Each delivery attempt mints an opaque claim token. A recipient acknowledgement can terminalize only the exact
message, under the caller's own inbox key, the live session, that attempt's token, and the matching message hash. A
token from an earlier attempt of the same message cannot settle a later one. There is no lease deadline to expire:
what is in flight lives only in memory, so an abandoned attempt simply leaves the row `Pending`.

`PeerSession` sends `Acknowledge` only after all of these operations succeed:

1. The relay outer envelope and inner sync envelope are validated.
2. The Automerge message is applied through the production `DocumentEntity.ApplySync` path.
3. The sender scoped relay receipt and any resulting canonical state commit in recipient SQLite.
4. Pending commit invalidations are published.
5. Any generated reply is durably enqueued for the local peer session.

This boundary proves durable recipient processing for replay suppression. It does not change
`PeerSession.durableConfirmation()`, which remains `false`. It also does not prove that another peer observed the
change.

A stable protocol violation uses `Reject` with `ProtocolInvalid`. Application code can use
`ApplicationRejected` through the acknowledged delivery contract. A permanent rejection becomes a retained dead
letter row. Infrastructure failure, interruption, timeout, disconnect, and claim expiry release or recover the
message for retry.

## Ordering, retry, expiry, and retention

Ordering is FIFO only within one exact directed channel. The recipient is the inbox itself, so the channel key is
the tenant, sender subject, sender peer, sender replica incarnation, and sender connection epoch. The epoch is part
of the key because `sender_sequence` restarts at zero on every sender reconnect; without it one channel would hold
sequences `{0,1,2,0,1}` and head selection would be arbitrary. Delivery is stop and wait within a channel: one head
is in flight at a time and later rows wait behind it. Different channels progress concurrently, bounded by
`maxConcurrentChannels`.

A message the front door may not hand over — because receive authorization narrowed since it was admitted — is
`Release`d rather than settled. The row stays `Pending` for a later session, and the channel is skipped for the rest
of this session only, so withheld messages cannot occupy every delivery slot and starve channels the recipient is
entitled to. The sender outbox keeps pending admissions until custody succeeds or their configured retry horizon
expires.

`RelayInbox.Options.maxDeliveries` caps recipient delivery. It is required per deployment. An attempt is charged
only once the message has provably reached the transport, so a delivery prepared for a channel and then abandoned
when the recipient disconnected — ordinary behaviour on a flaky connection — costs nothing. A withheld attempt does
spend one, which is what bounds how long an unauthorized message lingers. When the count reaches the cap the relay
moves the message to `DeadLettered` so it stops blocking its channel. Process restart does not reset the count.
Message expiry can terminalize the row before the attempt cap.

The relay message time to live is `RelayInbox.Options.messageTtl`. A terminal row — acknowledged, rejected, dead
lettered, or expired — keeps its stored envelope until `RelayInboxMaintenance` collects it past its deduplication
horizon, at which point the whole row is deleted. The envelope is retained rather than erased at the terminal
transition because re-admitting a dead lettered or expired identity revives that row in place, which is what lets a
sender that still holds custody recover a message the relay gave up on. `PeerRelayLimits.maximumReceiptRetention`
must cover the message time to live, the maximum sender retry horizon, and the minimum terminal retention, and
`RelayServer` refuses a handshake that would breach it.

The recipient keeps sender scoped receipts under its current replica incarnation. Receipt identity includes sender
tenant, sender subject, sender peer, and relay message ID. `PeerRelayReceiptLimits` bounds their count, encoded bytes,
retention, pruning batch, rate, and interval. Replace or clone restore fences old relay state and reconciles receipt
and outbox usage for the installed incarnation.

Relay custody rows and client relay outbox or receipt rows are not part of the canonical backup archive.

## Capacity and overload

Admission is bounded **per inbox** by `RelayInbox.Options.maxPendingMessages` and `maxPendingBytes`, and the cap is
re-checked inside the same transaction that performs the write. Accepted work is not evicted to admit new work.

Cross inbox quotas — per sender peer, per recipient subject, per tenant, per shard — are not enforced. Those counters
span many inboxes, so no entity is their sole writer, and enforcing them would reintroduce the cross process
arbitration this topology exists to remove. A deployment that needs tenant wide caps needs a mechanism with its own
single writer story.

Connection and frame accounting is likewise not enforced here. It bounded the bespoke length prefixed framing that
standard Effect RPC over a socket replaces. A single relayed payload is still bounded, by `PeerRpc`'s schema check
against `maximumRelayPayloadBytes`; concurrent connections and in flight bytes are the deployment's socket server to
bound. `PeerRelayLimits` retains the negotiation windows, the authentication rate limits, and
`maxSessionsPerSubject`.

`PeerRelayOutboxLimits` independently bounds pending sender rows and encoded bytes per remote and per replica. It also
bounds retry horizon, pruning batch, pruning rate, and maintenance interval.

Capacity failures are explicit. They do not imply custody. An application may retry
`RequestCapacityExceeded` only with its own bounded backoff and the same stable sender admission.

## Security

The application remains responsible for credentials, identity issuance, TLS, endpoint routing, and authorization
policy. `PeerAuthenticator` derives a fresh request principal for every RPC operation.
`PeerRelayAuthorization.authorize` receives the direction, authenticated principal, exact remote subject and peer,
and selected whole documents. The server checks send authorization before custody and receive authorization before
disclosure.

### Unsafe Automerge resource trust

[Automerge `3.3.2`](https://github.com/automerge/automerge/releases/tag/js%2Fautomerge-3.3.2) does not expose an
allocation bounded semantic decode API. A small authenticated byte string can therefore cause allocation beyond its
wire size when Automerge parses it. Authentication proves a principal. Ordinary relay authorization grants access to
an endpoint and document set. Neither fact proves that the producer's Automerge bytes are safe to allocate and
decode. This contract is pinned to the installed `3.3.2` release. It makes no claim about a later Automerge release.

`PeerRelayAuthorization` consequently has a second, explicit policy callback:
`authorizeUnsafeUnboundedAutomerge3Decode`. It is default deny when the application supplies
`denyUnsafeUnboundedAutomerge3Decode`. Relay send admission and recipient delivery require a grant from this callback
in addition to ordinary authentication and document authorization.

The unsafe grant is exact. It carries the literal
`risk: "Automerge3.3.2DecodeIsNotAllocationBounded"`, the authenticated local principal, the resolved remote principal
in the same tenant, `Send` or `Receive`, the complete unique document set, a finite future `validUntil`, and an
`invalidated` Effect. The constructor rejects substitutions, missing or extra documents, duplicate document
identities, malformed grants, and expired grants. A grant for one direction, endpoint, principal, or document set
cannot authorize another.

After fresh ordinary and unsafe grants, the bounded server operation and policy expiry or revocation contend on a
local gate. If revocation wins before operation admission, no relay SQL mutation or payload emission occurs. If the
operation wins, it is in flight. It may finish its durable commit or delivery and returns its real result. Revocation
drains that bounded in flight operation and prevents later operations. It does not retroactively cancel the winner.
SQLite and an external authentication or authorization authority do not share an atomic transaction.

Grant this capability only when the application controls the producer and treats its bytes as resource trusted. Do
not grant it merely because the producer is authenticated, is allowed to edit the documents, is another tenant
member, or supplied a valid envelope.

> **Warning:** The following callback accepts the known allocation risk. Use this shape only behind an application
> policy that has independently established producer byte resource trust.

```ts
import * as PeerRelayAuthorization from "@lucas-barake/effect-local-rpc/PeerRelayAuthorization"
import { Clock, Effect } from "effect"

const ResourceTrustedRelayAuthorizationLive = PeerRelayAuthorization.layer(
  authorizeRelay,
  (request) =>
    Effect.gen(function*() {
      yield* assertResourceTrustedProducer(request)
      const now = yield* Clock.currentTimeMillis
      return {
        _tag: "UnsafeUnboundedAutomerge3DecodeGrant",
        risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
        principal: request.principal,
        remote: {
          tenantId: request.principal.tenantId,
          subjectId: request.remote.subjectId,
          peerId: request.remote.peerId
        },
        direction: request.direction,
        documents: request.documents,
        validUntil: now + unsafeGrantLeaseMillis,
        invalidated: unsafeGrantInvalidated
      }
    })
)
```

Relay infrastructure remains opaque to Automerge semantics. Before custody or forwarding it validates bounded frame
and envelope size, Effect Schema shape, endpoint and document routing, message hash, canonical outer digest, and
writer provenance shape. It does not interpret Automerge changes, dependency graphs, or operations. The relay enabled
`PeerSync` receive path performs the one semantic Automerge decode and applies the existing change, dependency, and
operation limits.

The durable protocol always requires the separate unsafe resource trust callback. When Automerge exposes an
allocation bounded decode API, Effect Local should use it and remove this unsafe grant instead of normalizing the
exception as permanent policy.

The relay outer digest binds the complete versioned envelope. It provides integrity and conflict detection. It is not
encryption or a signature. The relay stores and can read the payload supplied by the client. Applications that need
relay blind content must encrypt before Effect Local and must bind their own authenticated encryption context to the
relay identities.

Wire and SQL values are Schema decoded. Payload, credential, claim token, and full principals are not added to relay
span attributes. Public wire errors are fieldless so authorization and storage failures do not disclose message
existence.

## Deployment boundary and limitations

Several relay nodes may run against one logical SQL database as active active. Single owner per recipient is
enforced by cluster sharding rather than by deployment convention: exactly one runner owns a device's entity at a
time, and ownership moves with the shard. `SqlRelayInboxStore.layer` supports SQLite, PostgreSQL, and MySQL.
Replication and failover remain properties of the selected database deployment.

Because the owning entity is the sole writer for its inbox, there is no claim ledger, lease deadline, or session
generation to recover. Nothing in flight is persisted: a lost runner loses only memory, and the next owner finds the
same `Pending` rows.

When the SQL authority is unavailable, new custody stops and delivery pauses. Durable pending rows remain
discoverable after recovery. Retention runs as a cluster singleton so expiry and collection have exactly one owner
deployment wide.

On a rebalance, an entity's termination handshake waits up to the cluster's `entityTerminationTimeout` for its forked
subscription before the session is closed, and rejects `Deliver` and `Settle` for that device during the window. Size
that value accordingly.

Single owner per key across multiple runners is verified rather than assumed. `RelayInboxMultiRunner.test.ts` brings
up two socket runners against one PostgreSQL, and reads ownership off each runner's own shard map: exactly one hosts a
given inbox key, the other reaches it over the wire, and a second session opened through the non-owner replaces the
first runner's session. Its control splits the two runners across databases, where each acquires every shard and hosts
its own instance instead — which is what gives the first result meaning.

That test also pins an operational requirement worth stating plainly: **a multi-node relay needs a real inter-runner
transport, and configuring one without it fails silently rather than loudly.** Every `RelayInbox` rpc is deliberately
volatile, so `Sharding` routes a message for a remote shard through `Runners.send`; under `Runners.layerNoop` that
fails `EntityNotAssignedToRunner`, and the failure is retried forever rather than surfaced. Two nodes wired that way
hang instead of erroring. Use a socket runner, and note that `RunnerHealth.layerNoop` is likewise wrong for more than
one node, because dropping an unhealthy runner from the hash ring is the mechanism that moves ownership.

For contrast, the cluster inside `SqlReplica` is single node by construction and correctly so — a client replica is
one process, and `internal/clusterStorage.ts` pairs `Runners.layerNoop` with `RunnerHealth.layerNoop` deliberately.
The relay's `Sharding` is the application's to supply and is the one that has to be socket backed.

This package supplies a relay building block. It does not claim global availability, managed operations, peer
discovery, end to end encryption, account management, or tenant routing.

## Observability

The relay exposes Effect services and spans. It does not install a metrics exporter.

| Source                      | Exposed values                                                    |
| --------------------------- | ----------------------------------------------------------------- |
| `RelayInboxStore.usage`     | `pendingCount`, `pendingBytes`, `retainedCount` for one inbox     |
| `RelayInboxStore.abandoned` | Dead lettered and expired rows for one inbox, with attempt counts |
| `PeerRelayOutbox.usage`     | Remote and replica `messageCount` and `encodedBytes`              |

Store operations use spans named `SqlRelayInboxStore.admit`, `pendingHeads`, `recordDelivery`, `settle`, `usage`,
`abandoned`, `expire`, and `collect`. Entity spans are `RelayInbox.Deliver`, `Subscribe`, `Settle`, `Release`,
`Heartbeat`, and `EndSession`. Front door spans are `RelayServer.Open`, `RelayServer.Push`, and `RelayServer.Settle`.
Retention sweeps use `RelayInboxMaintenance.sweep`. Client relay spans are
`effect_local_rpc.adapter.relay_open` with `rpc.selected_documents` and
`effect_local_rpc.adapter.relay_push` with `rpc.payload_bytes`.

Attributes are identifiers only. The inbox key is a digest and the message and relay identifiers are opaque; the
payload, the credential, and full principals are never span attributes.
