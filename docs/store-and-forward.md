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

The application supplies relay policy, custody, limits, authentication, SQL, and a socket. The server uses
`PeerRpcServer.layerHandlers` with `PeerRpc.Rpcs`, plus `PeerRpcServer.layerServer` and `PeerRelayIngress` for the
bounded listener.

On the client, `SqlReplica.layerRelay` installs relay receipt support. `PeerRelayOutbox.layerSql` stores stable sender
admissions. `PeerRelayClientRuntime.layer` supervises sender outbox and receipt maintenance.
`RpcPeerTransport.makeSession` binds the generated relay client to the ordinary `PeerSession`.

This example shows the server and client composition. The named application values provide SQL, Crypto,
authentication, socket, definition, projection, and mutation or query Layers.

```ts
import * as PeerRelayAuthorization from "@lucas-barake/effect-local-rpc/PeerRelayAuthorization"
import * as PeerRelayLimits from "@lucas-barake/effect-local-rpc/PeerRelayLimits"
import * as PeerRpcServer from "@lucas-barake/effect-local-rpc/PeerRpcServer"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import * as SqlPeerRelayStore from "@lucas-barake/effect-local-rpc/SqlPeerRelayStore"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutbox from "@lucas-barake/effect-local-sql/PeerRelayOutbox"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import { Effect, Layer } from "effect"

const RelayLimitsLive = PeerRelayLimits.layer(relayLimits)
const RelayStoreLive = SqlPeerRelayStore.layer.pipe(
  Layer.provideMerge(RelayLimitsLive)
)
const RelayAuthorizationLive = PeerRelayAuthorization.layer(
  authorizeRelay,
  PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode
)
const RelayHandlersLive = PeerRpcServer.layerHandlers({
  tenantId,
  peerId: relayPeerId
}).pipe(
  Layer.provideMerge(RelayLimitsLive),
  Layer.provideMerge(RelayStoreLive),
  Layer.provideMerge(RelayAuthorizationLive)
)

const ReceiptLimitsLive = PeerRelayReceiptLimits.layer(receiptLimits)
const RelayReplicaLive = SqlReplica.layerRelay(definition, { projections }).pipe(
  Layer.provideMerge(ReceiptLimitsLive)
)
const RelayOutboxLive = PeerRelayOutbox.layerSql.pipe(
  Layer.provideMerge(RelayReplicaLive),
  Layer.provideMerge(PeerRelayOutboxLimits.layer(outboxLimits))
)
const RelayClientLive = PeerRelayClientRuntime.layer.pipe(
  Layer.provideMerge(RelayOutboxLive)
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
`PeerRelayIngress.layerProtocolSocket`. The server
composition still requires the application SQL and Crypto Layers, `PeerAuthentication.layerServer`, the relay
authorization Layer, and a `PeerRelayLimits` Layer. The client composition still requires the application SQL,
Crypto, core replica limits, generated client authentication middleware, and a scoped relay socket.

The example denies the separate unsafe Automerge decode grant. That is the required default. Ordinary authentication
and `authorizeRelay` document authorization do not promote a caller into resource trust.

The `expectedRelayPeerId`, `expectedLocal`, exact remote subject and peer, replica incarnation, selected documents,
receipt retention, and sender retry horizon are part of the version `1` handshake. A mismatch fails the connection.

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

A relay claim carries an opaque claim token and a finite deadline. A recipient acknowledgement can terminalize only
the exact message, recipient principal, live session generation, token, and message hash.

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

Ordering is FIFO only within one exact directed channel. The channel key is the tenant, sender subject, sender peer,
sender replica incarnation, recipient subject, and recipient peer. The relay permits one claimed head for that
channel. Earlier pending or claimed rows block later rows in the same channel. Different channels can progress
independently.

Claims are finite fences. A stale worker can duplicate delivery after its claim expires, but its old token cannot
acknowledge or reject the new claim. Retry uses bounded exponential delay with jitter. The sender outbox keeps pending
admissions until custody succeeds or their configured retry horizon expires.

`PeerRelayLimits.maximumDeliveryAttempts` caps recipient delivery. The default is `16`. A failed, interrupted,
disconnected, or expired claim durably advances the attempt count. When that count reaches the configured cap, the
relay moves the message to `DeadLettered`, erases its payload, and retains only bounded terminal deduplication
evidence. Process restart does not reset the count. Message expiry can terminalize the row before the attempt cap.

The relay message time to live is configured by `PeerRelayLimits.messageTtlMillis`. Expired active rows erase their
payload and become terminal. Acknowledged, rejected, and expired rows retain only bounded deduplication evidence until
their retention deadline. `PeerRelayLimits.maximumReceiptRetentionMillis` must cover the message time to live, the
maximum sender retry horizon, and minimum terminal retention.

The recipient keeps sender scoped receipts under its current replica incarnation. Receipt identity includes sender
tenant, sender subject, sender peer, and relay message ID. `PeerRelayReceiptLimits` bounds their count, encoded bytes,
retention, pruning batch, rate, and interval. Replace or clone restore fences old relay state and reconciles receipt
and outbox usage for the installed incarnation.

Relay custody rows and client relay outbox or receipt rows are not part of the canonical backup archive.

## Capacity and overload

`PeerRelayLimits` validates active and retained count and byte quotas for sender peers, recipient peers, recipient
subjects, tenants, and the shard. Admission reserves immutable quota entitlements in the same SQLite transaction as
the message. Accepted work is not evicted to admit new work.

The same limits bound relay connections, raw chunks, declared frames, incomplete frames, shared payload bytes, byte
reservation waiters, per subject sessions, Open and Push concurrency and rates, terminal response work, channel
queues, relay workers, maximum delivery attempts, SQL work classes, maintenance batches and rates, and shutdown claim
release.

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

One logical SQL database is the custody authority for one relay shard. The application must route every write and
claim for that shard to it. `SqlPeerRelayStore.layer` supports SQLite, PostgreSQL, and MySQL. Replication and failover
are properties of the selected database deployment. The relay does not add leader election, shard rebalance, cross
region routing, or split brain recovery.

When that SQL authority is unavailable, new custody stops and delivery pauses. Durable pending rows remain
discoverable after recovery. Claims recover after their deadlines. The server also runs bounded
compensation and maintenance so a missed process notification does not permanently strand committed work.

This package supplies a bounded single authority relay building block. It does not claim WhatsApp scale, global
availability, managed operations, peer discovery, end to end encryption, account management, or tenant routing.

## Observability

The relay exposes Effect services and spans. It does not install a metrics exporter.

| Source                       | Exposed values                                                                      |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `PeerRpcServerRuntime.usage` | `accepting`, `sessions`, `subjects`, `activeClaims`, `queuedChannels`               |
| `PeerRelayIngress.usage`     | `connections`, `reservedBytes`, `byteReservationWaiters`                            |
| `PeerRelayStore.usage`       | `activeCount`, `activeBytes`, `retainedCount`, `retainedBytes` for a selected scope |
| `PeerRelayOutbox.usage`      | Remote and replica `messageCount` and `encodedBytes`                                |

Store operations use spans named `PeerRelayStore.admit`, `claim`, `loadClaimedPayload`, `acknowledge`, `reject`,
`release`, `recover`, `expire`, `repair`, `reconcile`, `collect`, and `usage`. Client relay spans are
`effect_local_rpc.adapter.relay_open` with `rpc.selected_documents` and
`effect_local_rpc.adapter.relay_push` with `rpc.payload_bytes`.
