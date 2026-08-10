# Synchronization

`SyncEngine` is the transport neutral client contract:

- `submit` sends one stable mutation envelope and returns its durable terminal receipt
- `pull` reads accepted entries after a cursor with count and byte bounds
- `watch` streams wake notifications for a space

The reconciler does not trust notification delivery or ordering. A notification only schedules another pull. Every
sync attempt reads the durable client cursor, catches up, submits pending mutations in local order, and catches up
again. A single Effect semaphore prevents overlapping reconciliation transactions. A sliding queue coalesces local
commits, reconnects, and remote wakes.

## WebSocket RPC

`SyncRpc.Rpcs` uses one Effect RPC group for submit, pull, watch, presence publish, and presence watch. Effect's RPC
Schema codecs define the external contract. `SyncServer.layer` binds handlers to `ServerStore` and `PresenceHub`.
`SyncClient.layer` maps the generated client to `SyncEngine` while preserving typed replica failures.

Authentication is RPC middleware. The client reads a redacted credential and sets an `Authorization: Bearer` header.
The server authenticates it into a JSON principal for each request. ServerStore admission and read callbacks enforce
tenant policy. PresenceHub has its own authorization callback.

Application ingress remains responsible for TLS, allowed Origins, socket limits, and HTTP server configuration.

## Reconnect and retry

An interrupted socket may fail an active request even when the server committed it. The client retains the pending
mutation and retries exactly. Server receipts deduplicate by mutation identity and client sequence. The accepted log,
not an acknowledgement, changes client canonical state.

When a watch ends, its finalizer schedules another reconciliation attempt. The transport may reconnect independently.
The durable cursor and pending queue contain everything required to resume.

## Presence

Presence is intentionally outside reconciliation. It is Schema decoded, size bounded, published through a sliding
in memory hub, scoped by space, and expires by TTL. A slow subscriber may miss updates. Browser presence assigns an
arrival token before decode so a slow older value cannot overwrite a newer valid value or survive an explicit remove.
Invalid values do not suppress valid in flight values.
