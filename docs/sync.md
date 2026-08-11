# Synchronization

`SyncEngine` is the transport neutral client contract:

- `submit` sends one stable mutation envelope and returns its durable terminal receipt
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
generation. Every sync pass reads the SQLite view cursor, catches up, submits pending mutations in local order, and catches
up again. SQLite commits requested generation changes with local mutations and records completed generations
idempotently. Each Workflow is finite, coalesces requests made while it ran, and has bounded exponential retry
attempts. This bounds one execution's history and repairs a lost wake when the next runner starts. A terminal failure
waits for a later local mutation or server wake to request a new generation. Completed execution retention belongs to
the selected Workflow storage. The explicit in memory Layer uses the same sync pass with a sliding queue for
lightweight processes and tests.

## History lifecycle

The server retains a configurable dense accepted suffix and a configurable terminal receipt suffix. Hard caps are
larger than retained targets and apply backpressure before mutation handlers run. `ServerStore.layerMaintenance`
periodically publishes recovery state and reclaims bounded prefixes. Deployments that use an external scheduler call
the same `maintainAll` operation.

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
count, bytes, and rolling digest. Snapshot entries contain only authorized view changes. They never contain mutation
payloads, private results, or receipt bodies.

`Delete` means the authoritative entity no longer exists. `Retract` means it exists but is outside the current scope or
principal visibility. Retractions remove canonical and visible state and persist a generation owned tombstone so
pending optimistic replay cannot restore revoked data. A later authorized `Upsert` clears that tombstone. Whole scope
authorization failure atomically clears replicated canonical and visible state before the failure is surfaced.

Receipt reclamation advances a per client expired local sequence watermark. A retry below that watermark returns
`Expired`, bound to a covering published snapshot, and never reexecutes. The pending client mutation remains visible
until that snapshot installs. If the durable cursor already covers the snapshot sequence, the accepted state is
already canonical and the receipt can settle without replacing state. This preserves at most once execution after the
full private result was reclaimed.

## WebSocket RPC

`SyncRpc.Rpcs` uses one Effect RPC group for submit, pull, bootstrap, watch, presence publish, and presence watch. Effect's RPC
Schema codecs define the external contract. `SyncServer.layer` is the authenticated facade. It routes each operation
through the Effect Cluster entity named by the request's space. The entity validates that embedded space identity
matches its Cluster address, then calls `ServerStore` or `PresenceHub`. `SyncClient.layer` maps the generated client
to `SyncEngine` while preserving typed replica failures.

Authentication is RPC middleware. The client reads a redacted credential and sets an `Authorization: Bearer` header.
The server authenticates it into a JSON principal for each request. The authenticated facade issues an opaque principal
assertion for the internal Cluster hop. The space entity verifies that assertion before deriving a principal and
calling storage or presence services. Raw caller supplied JSON never confers authority inside the cluster.
`ServerStore.layer` requires access, mutation admission, and read policies. Read policy receives a tagged scope check
before schema or space disclosure, then a separate check for every candidate entity. Pull and every bootstrap page
recheck entity visibility before sending data. Receipt retries recheck current access before reading durable state. `PresenceHub.layer`
requires a tagged publish or watch policy that can bind the claimed presence client ID to the principal. The explicit
`layerTrusted` constructors are reserved for tests and already trusted processes.

The space entity is the single live owner and relay for a space. Its operations are deliberately volatile. A wire wake
contains only the space ID. Entity changes wake a client only when they are currently in scope and visible, or could
remove something from its acknowledged view. Periodic authorization refresh hints ensure policy-only revocations are
eventually pulled as retractions. Wakes never carry an entity or the global server sequence. Durable
mutation custody has two owners at different stages. Before admission, the client keeps the pending envelope in its
SQLite outbox. After admission, `ServerStore` keeps the terminal receipt and accepted event in
`effect_local_server_receipts` and `effect_local_authoritative_log`. If a runner fails before SQL commit, the entity call
fails and the client resubmits. If SQL committed before the reply was lost, exact resubmission returns the stored receipt.

This is the same store backed actor pattern as the former recipient relay. Persisting Submit through Effect beta.103
`MessageStorage` would retain every completed request payload and reply with no per-request retention control. The
authoritative mutation would then exist permanently in both Cluster history and the server log. Keeping entity calls
volatile avoids that duplicate history while Cluster still supplies unique ownership, cross runner routing, and live
recipient streams.

The application can route a `ServerStore` service by `spaceId`, use one shared database, or colocate a database with a
runner whose placement policy keeps the data reachable. Pull recovers from the authoritative sequence. Wake and
presence streams reconnect to the current space owner.

`SyncRpc.layerJson` is the WebSocket serializer. WebSocket messages are already framed, so it keeps no cumulative
buffer. It rejects an inbound UTF-8 frame before JSON parsing and rejects an outbound frame after encoding when it
exceeds the configurable bound. It also replaces RPC defects and infrastructure causes with opaque wire values.

Application ingress remains responsible for TLS, allowed Origins, socket limits, and HTTP server configuration. The
installed high level `NodeHttpServer` creates `ws` with its 100 MiB default and does not expose `maxPayload`.
Production servers must enforce a limit no larger than `SyncRpc.maximumFrameBytes` in a reverse proxy or construct a
lower level `WebSocketServer({ noServer: true, maxPayload })` through `NodeHttpServer.makeUpgradeHandler`. The
serializer bound protects parsing and application allocation after the native socket has assembled a message. It does
not replace an ingress payload limit.

## Reconnect and retry

An interrupted socket may fail an active request even when the server committed it. The client retains the pending
mutation and retries exactly. Retained server receipts deduplicate by mutation identity and client sequence. After a
receipt expires, the durable watermark prevents reexecution and directs the client to its covering snapshot. The
accepted log or verified snapshot, not an acknowledgement, changes client canonical state.

When a watch ends, its finalizer requests another reconciliation generation. The transport may reconnect
independently. The durable view cursor, scope generation, global mutation watermark, pending queue, retractions, and
reconciliation generation counters contain everything required to resume.

## Presence

Presence is intentionally outside reconciliation. It is Schema decoded, size bounded, published through scoped
per-space sliding hubs on the Cluster space owner, and expires by TTL. Effect `RcMap` releases a space hub after its
last subscriber closes. A slow subscriber may miss updates. Browser presence assigns an arrival token before decode
so a slow older value cannot overwrite a newer valid value or survive an explicit remove. Invalid values do not
suppress valid in flight values.
