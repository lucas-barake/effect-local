# Synchronization

`SyncEngine` is the transport neutral client contract:

- `submitBatch` sends up to `Protocol.maximumSubmitBatchEntries` stable mutation envelopes of one space in local
  order and returns their durable terminal receipts in the same order. The result may cover only a nonempty prefix of
  the envelopes. The caller resubmits the rest.
- `discard` resolves one quarantined envelope without executing its mutation handler
- `pull` advances one durable client view through bounded `Upsert`, `Delete`, and `Retract` changes, or returns the
  immutable scoped snapshot manifest required for bootstrap
- `bootstrap` reads one client, scope, and principal bound, ordered, count and byte bounded snapshot page
- `watch` streams payload free wake hints for a scoped client view

Every client declares a `ReplicationScope` containing the model names it subscribes to. An empty scope is valid. A
whole definition subscription is explicit. The server normalizes and validates the scope against the request schema
identity before reading entities. Scope changes advance a durable generation. Widening backfills newly selected and
authorized entities through incremental pull. Narrowing emits `Retract` changes so excluded entities are evicted
without a full bootstrap.

The reconciler does not trust notification delivery or ordering. A notification only requests another durable
generation for its space. Notifications that arrive before a turn reads the generations share the request already
written, so a burst of wakes costs one write per turn. Every sync pass reads that space's SQLite cursor, catches up, submits pending mutations in
local order, and catches up again. Pending mutations leave in batches of at most `Protocol.maximumSubmitBatchEntries`
envelopes whose encoded size stays within `Protocol.maximumBatchBytes`, so N pending mutations cost
`ceil(N / maximumSubmitBatchEntries)` round trips when nothing fails. The client marks a batch as submitting in one
SQLite transaction and records its receipts in one transaction with a savepoint per receipt, so a receipt that fails
validation rolls back alone and the receipts before it stay recorded. The next batch is sent only after the previous
batch's receipts are durable. SQLite commits requested generation changes with local mutations and records
completed generations idempotently. The in memory composition has one dispatcher with one keyed watch and one keyed
turn per foreground active space. A joined space that is not foreground active has no watch. Its pending work drains
through short background sync passes. A blocked or retrying turn cannot prevent another key from starting, and all
keys share the same RPC protocol and physical WebSocket.

Each Workflow is finite, coalesces requests made while it ran, and has bounded exponential retry attempts. Its
identity includes the membership incarnation, so leave and rejoin cannot resume stale execution identity. This bounds
one execution's history and repairs a lost wake when the next runner starts. A terminal failure waits for a later local
mutation or server wake to request a new generation. Completed execution retention belongs to the selected Workflow
storage.

## Offline wake delivery

`ServerStore.layer` can accept an `offlineWake` option for push notifications without depending on a provider. An
accepted mutation commits a per-space high water mark in the admission transaction. A scoped dispatcher later asks
the application's `recipients({ spaceId })` callback for the authoritative client membership, then creates one durable
delivery row per client. Mutation admission never waits for membership lookup or push delivery.

The delivery hook must decide current membership and send inside one application-owned serialized operation. It
returns `"Delivered"` after sending or `"NotRecipient"` when membership has ended. `"NotRecipient"` retires the current work,
so removal after expansion also blocks retries without a check-to-send race.

`deliver({ wakeId, spaceId, clientId })` is the provider boundary. It carries routing and idempotency values only. It
does not contain a mutation, entity key, sender, message count, or server sequence. The application resolves the
client's current FCM, APNs, or web push endpoint and uses `wakeId` as the provider idempotency key. Send a content-free
signal that tells the app to sync. Do not copy `wakeId`, `spaceId`, or `clientId` into provider-visible notification
content.

A live Watch or acknowledged Pull can make work obsolete before the hook runs. Once delivery starts, it is at least
once. A callback failure, defect, or timeout retries the same `wakeId` with capped exponential backoff. A database
failure after the provider send can also retry that ID. This prevents lost work but permits duplicate provider calls.

The dispatcher waits `coalescingWindow` before expanding newly idle space work. An unnotified client wake keeps its
`wakeId` while later mutations raise its high water fence. After the current work finishes, any remaining work receives
a new identity and another coalescing delay.

A client is online only while its production Watch has an active SQL presence lease. The Watch does not become ready
while a delivery claim for that client is in flight. Every server runtime sharing the database checks the same leases,
and scope finalizers remove them. Every runtime that accepts Watch streams must configure the same `offlineWake`
adapter so it publishes those leases. Expiration recovers from crashed runtimes. Connected clients keep using the live
payload-free Watch stream and do not enter the push path.

Pull is the durable acknowledgement. When a client presents the cursor for an applied incremental page, the same
transaction advances a separate wake acknowledgement fence and retires delivery rows at or below that fence. The
separate fence matters during bootstrap because installing a snapshot initializes replication state at the server head
before the client has acknowledged a later pull.

## History lifecycle

The server retains a configurable dense accepted suffix and a configurable terminal receipt suffix. Hard caps are
larger than retained targets and apply backpressure before mutation handlers run. A write that takes a space past the
midpoint between its retained target and hard cap starts one background maintenance run for that space.
`ServerStore.layerMaintenance` also sweeps every space from an Effect Cluster singleton, once at start and then every
interval. Deployments that use an external scheduler call the same `maintainAll` operation.

Maintenance snapshots the current authoritative entities at accepted sequence `S` and terminal sequence `T`. The
manifest binds space, definition, snapshot identity, both fences, entity count, content bytes, and a chained SHA 256
digest. The publication transaction compares the observed heads, inserts the immutable snapshot, then publishes
logical floors before deleting any rows. A concurrent admission causes preparation to be discarded and retried later.

The dense per space server sequence remains the mutation order and client mutation basis. Replication progress uses a
separate per client view cursor. The server durably stores the acknowledged materialized view and at most one immutable
outstanding page. Retrying a page from its base cursor returns the same page. Acknowledging its target cursor applies
the page to the server view before the next diff is created. Empty pages can therefore advance the global server
watermark without inventing entity changes.

A fresh, unknown, or schema-invalidated view cursor receives `BootstrapRequired`. The client stages pages durably and
atomically installs the verified scoped snapshot. A newer manifest supersedes an older partial stage. The manifest
binds the client ID, normalized scope digest, scope generation, view cursor, global sequence and terminal fences,
count, bytes, and rolling digest. Snapshot entries contain only current authorized `Upsert` rows. They never contain
mutation payloads, private results, receipt bodies, retractions, or identifiers from a previous principal. Installation
derives absence retractions from the client's prior canonical state.

`Delete` means the authoritative entity no longer exists. `Retract` means it exists but is outside the current scope or
principal visibility. Retractions remove canonical and visible state and persist a generation owned tombstone so
pending optimistic replay cannot restore revoked data. A later authorized `Upsert` clears that tombstone. Whole scope
authorization failure atomically clears replicated canonical and visible state before the failure is surfaced.

Receipt reclamation advances a per client expired local sequence watermark. A retry below that watermark returns
`Expired`, bound to a covering published snapshot, and never reexecutes. The pending client mutation remains visible
until that snapshot installs. If the durable cursor already covers the snapshot sequence, the accepted state is
already canonical and the receipt can settle without replacing state. This preserves at most once execution after the
full private result was reclaimed. An `Expired` receipt depends on the snapshot current when it was issued, so a later retry of the same mutation can
return a different one. The client keeps the receipt it already stored when the new `Expired` receipt has the same
identity and its snapshot covers the stored outcome. Any other difference is a conflicting duplicate.

## WebSocket RPC

`SyncRpc.Rpcs` uses one Effect RPC group for negotiation, batch submit, discard, pull, bootstrap, watch,
ephemeral join, publish, and heartbeat. Effect's RPC
Schema codecs define the external contract. `SyncServer.layer` is the authenticated facade. It routes each operation
through the Effect Cluster entity named by the request's space. The entity validates that embedded space identity
matches its Cluster address, then calls `ServerStore` or `EphemeralHub`. `SyncClient.layer` maps the generated client
to `SyncEngine`, while `EphemeralClient.layer` exposes the joined ephemeral channel.

Authentication is RPC middleware. `CredentialProvider.acquire` is evaluated for every request and returns a redacted
bearer credential with a nonnegative generation. The server authenticates it into a JSON principal for that request.
The authenticated facade issues an opaque principal assertion for the internal Cluster hop. The space entity verifies
that assertion before deriving a principal and calling storage or ephemeral services. Raw caller supplied JSON never
confers authority inside the cluster. `ServerStore.layer` applies access, mutation admission, and read policies. Read
policy receives a tagged scope check before schema or space disclosure, then a separate check for every candidate
entity. Pull and every bootstrap page recheck entity visibility before sending data. Receipt retries recheck current
access before reading durable state. `EphemeralHub.layer` requires a tagged join, publish, or heartbeat policy with the
claimed member and space. The explicit `layerTrusted` constructors are reserved for tests and
already trusted processes.

The generation identifies the credential used for a request. When the server returns `CredentialRejected`, the client
attaches that generation to the typed failure. Reconciliation reports `NeedsAuthentication` and stops both turns and
watches from retrying it. `CredentialProvider.awaitChange(rejectedGeneration)` must complete only after `acquire` can
return a different generation. Completion requests a new reconciliation generation, so synchronization resumes on the
same replica and WebSocket. Rotating a bearer credential does not require rebuilding either Layer.

A reconciliation pass records the generation it starts under. An answer to a pull, a submit or a bootstrap request that
arrives after the generation changed is discarded, and the pass starts again under the new credential. A credential
for a different principal must therefore carry a new generation. The replica cannot observe a change of principal that
keeps the generation, and it would apply that answer. A new generation does not remove what the previous principal
already replicated. On sign out, leave the spaces of the previous principal or build the replica on another database.

`acquire` must return the same generation until the credential is replaced. The generation is a property of the
credential, not of the request, so a provider must not derive a new one each time it is asked. The replica reads the
generation through `acquire` when a pass starts and after every answer, and a provider that changes it on each read
makes every answer look stale. The first change during a pass restarts the pass at once. A second change during that
restart fails the pass with `UnexpectedFailure`, which is retried with the normal backoff, so a generation that never
settles costs two server calls per retry delay and the space reports `Failed` until it does.

The same comparison guards the discard of a quarantined mutation. Its answer is dropped when the generation changed
while the call was in flight, and the call is sent once more. A watch keeps the credential it was opened with, so
`SyncClient` ends the watch stream when `awaitChange` completes for the generation that `acquire` returned when the
watch started. The replica handles that like any watch the server closed: it waits the closed watch delay and opens the
watch again under the new credential.

Authentication and authorization failures remain distinct:

| Failure                    | Meaning                                                         | Reconciliation status and policy                 |
| -------------------------- | --------------------------------------------------------------- | ------------------------------------------------ |
| `CredentialRejected`       | The bearer credential is missing, invalid, revoked, or expired  | `NeedsAuthentication`. Wait for a new generation |
| `AuthenticatorUnavailable` | The identity verifier or one of its dependencies is unavailable | `Offline`. Retry with capped exponential backoff |
| `AuthorizationDenied`      | The authenticated principal may not perform the operation       | `Failed`. Do not retry automatically             |
| `OperationTimeout`         | Session acquisition or an RPC exceeded its configured bound     | `Offline`. Retry with capped exponential backoff |
| `ServerUnavailable`        | The RPC transport or server is unavailable                      | `Offline`. Retry with capped exponential backoff |

Reconciliation classifies every `ReplicaError` once, in `Reconciler.ts`, and both reconcilers and the background
scheduler read that one classification:

| Class            | Failures                                                                                                            | Status                | Policy                           |
| ---------------- | ------------------------------------------------------------------------------------------------------------------- | --------------------- | -------------------------------- |
| Unreachable      | `ServerUnavailable`, `OperationTimeout`, `AuthenticatorUnavailable`                                                 | `Offline`             | Retry with backoff               |
| Retryable        | `StorageUnavailable`, `UnknownCommitOutcome`, `OwnerUnavailable`, `UnexpectedFailure`, retryable `CapacityExceeded` | `Failed`              | Retry with backoff               |
| Needs credential | `CredentialRejected`                                                                                                | `NeedsAuthentication` | Wait for a new generation        |
| Terminal         | Every other tag, including `StorageCorrupt`, `AuthorizationDenied`, and terminal `CapacityExceeded`                 | `Failed`              | Stop until new work is requested |

`CapacityExceeded.resource` is the closed union `ReplicaError.CapacityResource`, and each resource has its own class.
Limits that load can clear retry: `read authorizations`, `sync watchers`, `sync watchers per principal`,
`server receipts`, `server history`, `bootstrap authorizations`, `bootstrap pages`, `pending mutations`, and the
ephemeral join, watcher, space, member, per space byte and key, and event resources. The rest are terminal: client
receipts, bootstrap and snapshot sizes, page, mutation, receipt, and schema evolution row bytes, submission attempts,
schema, scope, projection, and reconciliation generations, the sequence ceilings, and the ephemeral payload, snapshot,
and per member limits. A `Failed` status carries the failure tag as its `message`. A
`CredentialRejected` that carries no credential generation cannot be waited on and stops like a terminal failure.

`UnexpectedFailure` reports a defect inside the replica's own sync machinery, such as a storage client that died. It
carries the defect as `cause`, is logged at error level with its full cause, and retries with the same backoff. A
server response the client could not decode is not one of these. It is `ProtocolInvalid` and terminal. An inactive space keeps it in `space.status` while its retry is pending, so
the failure stays visible until a sync succeeds.

Ephemeral operations expose the same typed failures directly, but ephemera is outside reconciliation and does not
change replica status.

The space entity is the single live owner and relay for a space. Its operations are deliberately volatile. A live wire
wake contains only the space ID. Entity changes wake a connected client only when they are currently in scope and
visible, or could remove something from its acknowledged view. Periodic authorization refresh hints ensure policy-only
revocations are eventually pulled as retractions. Live and offline wakes never carry an entity or global sequence.
Durable mutation custody has two owners at different stages. Before admission, the client keeps the pending envelope in its
SQLite outbox. After admission, `ServerStore` keeps the terminal receipt and accepted event in
`effect_local_server_receipts` and `effect_local_authoritative_log`. If a runner fails before SQL commit, the entity call
fails and the client resubmits. If SQL committed before the reply was lost, exact resubmission returns the stored receipt.

A batch is one entity call under the space's admission permit. The entity admits its envelopes in order and each
admission is its own SQL transaction, exactly as if each envelope had arrived in its own batch. A terminal
rejection is a receipt, so later envelopes in the batch are still admitted. Any other failure stops the batch. When it
happens at the first envelope the call fails with that error. When it happens later the call returns the receipts
admitted so far, and the client's next batch starts at the failed envelope and receives the error there. The receipts
of one response are bounded by `Protocol.maximumBatchBytes`. An admitted envelope whose receipt would exceed that bound
is left out of the response and returned by exact resubmission. The store also stops starting admissions once a batch
has run for its `maximumSubmitBatchDuration`, default 1 second, so a slow database or authorization hook shortens the
response instead of letting one call outlive the client's `rpcTimeout`. An interrupted or lost batch leaves a committed
prefix that exact resubmission returns without executing anything twice.

One SQL transaction per batch was measured and rejected. Admission issues about 25 statements per mutation, so
commits are a small share of its cost. On PostgreSQL a single transaction was 3 to 6 percent slower for 56 mutations
and its savepoints overflow the 64 entry subtransaction cache after 32 mutations. It would also hold the space lock,
and on SQLite the database writer lock, for the whole batch and keep compaction from running between mutations.

This is a store backed actor pattern. Persisting SubmitBatch through Cluster
`MessageStorage` would store every request payload and reply a second time beside the server log, and the application
would have to clear them with `clearReplies` once the SQL receipt exists. Keeping entity calls
volatile avoids that duplicate history while Cluster still supplies unique ownership, cross runner routing, and live
recipient streams.

The application can route a `ServerStore` service by `spaceId`, use one shared database, or colocate a database with a
runner whose placement policy keeps the data reachable. Pull recovers from the authoritative sequence. Wake and
ephemeral streams reconnect to the current space owner.

`SyncRpc.layerJson` is the WebSocket serializer. WebSocket messages are already framed, so it keeps no cumulative
buffer. It rejects an inbound UTF-8 frame before JSON parsing and rejects an outbound frame after encoding when it
exceeds the configurable bound. The default bound is `SyncRpc.maximumFrameBytes`, which is
`Protocol.maximumRpcFrameBytes`: the 4 MiB `Protocol.maximumBatchBytes` plus 64 KiB. It also replaces RPC defects and
infrastructure causes with opaque wire values.

Application ingress remains responsible for TLS, allowed Origins, socket limits, and HTTP server configuration.
`NodeHttpServer` creates its `ws` server with the 100 MiB `ws` default unless told otherwise. Its `websocket` option
forwards to the `ws` `WebSocketServer`, so a production server sets the limit there or in a reverse proxy:

```ts
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as Http from "node:http"

const layerHttpServer = NodeHttpServer.layer(() => Http.createServer(), {
  port: 4100,
  websocket: { maxPayload: SyncRpc.maximumFrameBytes }
})
```

The serializer bound protects parsing and application allocation after the native socket has assembled a message. It
does not replace an ingress payload limit.

## Reconnect and retry

The first operation on a logical client session negotiates the highest shared protocol version. Every later sync and
ephemeral operation must carry that selected version. There is no implicit protocol version for an omitted field. An
operation rejected after reconnect clears the cached selection, negotiates against the new peer, and retries once. No
shared version returns typed `UpgradeRequired`. Reconciliation treats it as terminal. Transport loss and
`ServerUnavailable` remain retryable. A malformed frame remains `ProtocolInvalid`. It is not used as a version signal.
A well formed reply whose success, error, or streamed value does not match the client's schema for that RPC is also
`ProtocolInvalid`. It names the RPC, carries the `SchemaError` as `cause`, and fails only that call. The socket stays
open, and for a stream the client sends the server an interrupt for it.

Clients and servers support protocol version 1 by default. Pending mutations are submitted with `SubmitBatch`, which
carries up to `Protocol.maximumSubmitBatchEntries` envelopes of one space. A remote defect is a server failure. Every
sync and ephemeral operation reports it as typed `ProtocolInvalid` naming the RPC whose handler died, so reconciliation
shows `Failed` instead of losing its worker. A negotiation that dies on the server names `Negotiate` whichever
operation started it. An ephemeral session whose join stream or heartbeat dies on the server fails its `events`,
`state`, and `members` streams with that `ProtocolInvalid` and does not rejoin.

`sessionAcquisitionTimeout` bounds negotiation and renegotiation. `rpcTimeout` bounds every unary sync and ephemeral
RPC plus stream acquisition. Both accept `Duration.Input` and default to 10 seconds. Expiry interrupts the operation
and returns `OperationTimeout` with the operation name and configured `timeoutMillis`. An established watch may remain
idle indefinitely. Socket ping and reconnect behavior detect a dead connection without treating healthy inactivity as
an RPC timeout.

The in memory and Workflow reconcilers start retries of the unreachable and retryable classes at `retryDelay`,
defaulting to 1 second, then double the delay up to `maximumRetryDelay`, defaulting to 1 minute. A successful
reconciliation resets the attempt count. `maximumRetryDelay` must be greater than or equal to `retryDelay`, and an
invalid value fails Layer construction with `InvalidConfiguration`. In the in memory reconciler and the background
scheduler, a wait that follows `ServerUnavailable` or `OperationTimeout` also ends as soon as the socket reconnects.
Workflow also uses `maximumAttempts`, default 8, to bound one durable execution. Its supervisor can start another finite execution after a transient failure.

The background scheduler that drains inactive spaces uses the same classes and the same delays. A terminal failure
stops its retries for that space. The failure is remembered and shown by `space.status` as `Failed`, or as
`NeedsAuthentication` for a rejected credential, instead of `Idle`, and it counts in `Replica.status`. A rejected
credential that carries its generation is retried when the provider publishes a new one. The next successful
reconciliation of the space clears the remembered failure.

`SyncClient.layerProtocolSocket` implements the socket protocol itself and takes `retryPolicy` and
`retryTransientErrors`. The default `retryPolicy` doubles from 250 milliseconds up to 2 seconds between attempts, with
jitter between half and the whole delay. The schedule starts over once a connection has received a frame. With
`retryTransientErrors: true`, a socket open error stays internal while no RPC is in flight. A socket open error with
an RPC in flight, and any other socket error, fails the RPCs of that connection with `ServerUnavailable`, and new RPCs
keep failing with it until the socket reconnects. If a finite `retryPolicy` exhausts, that protocol instance stops opening the socket and every later RPC
fails with `ServerUnavailable`. Restarting socket acquisition requires rebuilding the protocol Layer.

An interrupted socket may fail an active request even when the server committed it. The client retains the pending
mutation and retries exactly. Retained server receipts deduplicate by mutation identity and client sequence. After a
receipt expires, the durable watermark prevents reexecution and directs the client to its covering snapshot. The
accepted log or verified snapshot, not an acknowledgement, changes client canonical state.

When a watch ends, the reconciler reopens it. The server sends one wake when a watch opens, so the reopened watch
requests another reconciliation generation. The delay before reopening starts at `retryDelay` and doubles up to
`maximumRetryDelay` while watches keep closing early. It returns to `retryDelay` once a watch has stayed open longer
than the previous delay. An unreachable or retryable failure reopens the watch after that delay. A terminal failure
stops the watch and is reported in the space status, and a rejected credential waits for a new generation. The
transport may reconnect independently. The durable view cursor, scope generation, global mutation watermark, pending
queue, retractions, and reconciliation generation counters contain everything required to resume.

## Schema deployment

Servers accept their current application schema plus `acceptedSchemaVersions` immediately preceding definitions from
the configured Evolution chain. Inbound old envelopes migrate forward. Receipts, pulls, and snapshots project back to
the caller with explicit downgrade transforms. Responses also identify the server schema. A compatible old replica
keeps syncing and reports `SchemaUpdateAvailable` so the application can prompt for reload. Removing a definition from
the configured horizon is the explicit deprecation action. Requests outside the horizon receive terminal
`StaleSchema`.

The window is an explicit operational choice. Server layer construction fails with `InvalidConfiguration` when the
evolution catalog has steps and `acceptedSchemaVersions` is omitted. Pass `0` to serve only the current schema or `N`
to accept the previous `N` versions. A nonzero window additionally requires a complete downgrade transform for every
accepted version, validated at construction. Without an evolution catalog there is nothing to accept and the window is
implicitly zero.

Deploy a server that accepts both schema and protocol generations before deploying a new client bundle. Keep the
window open through the client rollout, then reduce it only after the operator's deprecation horizon has passed.

Two restrictions survive any window setting. A replication scope that declares windows requires the exact current
schema and rejects historical callers with `ProtocolInvalid`. And a space that is mid promotion returns `StaleSchema`
to every caller until promotion completes, so a `StaleSchema` observed during a deploy does not always mean the
caller's bundle is outside the window.

If a current handler rejects an old pending envelope during local schema promotion, its savepoint rolls back and the
envelope moves to durable quarantine. Promotion still completes and the app starts. `Replica.quarantine` exposes the
original envelope and typed rejection. `discardQuarantined` asks the server to consume its local sequence without
handler execution. `resubmitQuarantined` first proves that nonexecution, then creates a corrected mutation at the tail.
An already accepted or expired server receipt suppresses replacement.

## Ephemera

Ephemera is intentionally outside reconciliation and the durable mutation log. A join returns one ordered stream whose
first message snapshots the complete roster and retained state for that space. Events are live only. Retained state is
last writer wins per member, channel, and key and replays to later joiners. The server enforces member, event, and state
TTLs and emits departure, clear, and removal deltas.

`EphemeralHub` bounds active spaces, joined streams, members, event keys, state keys, retained bytes, and complete
snapshot bytes. Shared sliding fan-out retains only bounded recent deltas. A subscriber that misses a revision rejoins
and replaces roster and retained state from a fresh snapshot without reconnecting healthy subscribers. The join
privately establishes a server capability and accepted lease. Scoped heartbeat maintains that lease, while expiry,
teardown, replacement, or periodic authorization revocation emits `MemberLeft` and closes the joined stream.
Retained state remains until its own TTL after departure. Applications persist read or delivery positions with a
normal mutation when those positions must survive server restart or the configured state TTL.
