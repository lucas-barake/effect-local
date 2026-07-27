# Sync

## Model

Peer sync exchanges Automerge messages for selected whole documents through the `PeerTransport` service. A relay can
route encrypted or plain application frames, but it is never authoritative for local writes. Offline edits continue
without a connection.

Each connection has its own Automerge `SyncState`. State transitions are serialized. Only one send is in flight per
peer epoch and sequence. Inbound receipts store session neutral reply bytes. `PeerSession` idempotently enqueues those
bytes under the current local epoch, so Cluster replay cannot reuse an expired connection sequence. Closing a session
removes its outbox and starts the next connection with fresh Automerge reconciliation. It recovers document state, not
the identity of the previous in flight transport operation. A bounded transport send retains the shared restore fence
until the network effect completes or reaches `maxPeerSendMillis`, so destructive restore cannot overtake an old
outbound frame.

Convergence means two replicas have received the same valid change set. It does not mean every peer is current at all
times. The engine preserves temporary divergence as normal operation.

## Bounds

Inbound frames are decoded before application. Limits cover bytes, changes, dependency edges, operations, pending
bytes, pending changes, pending dependency edges, and age. Limits exist per document, per peer, and per replica. A
peer cannot force unbounded missing dependency retention.

Unknown but valid canonical changes remain durable even when the installed application schema cannot project them.
The document becomes read only with `ProjectionBlocked` until compatible code arrives.

## Presence

Presence is a separate best effort channel with expiration. Cursor positions, typing state, connectivity, and session
leases do not enter Automerge history. Applications that need durable awareness must define it as normal document
data.

## Transport responsibility

Effect Local does not ship a backend or a server transport. An application supplied `PeerTransport` must define peer
identity, authentication, authorization, encryption, and routing. The test package supplies a bounded duplex
transport with deterministic drop, delay, duplicate, reorder, partition, and reconnect behavior.

Effect RPC uses one durable protocol at version `1`. The sender first writes a stable envelope to its local SQLite
outbox. Relay acceptance means that the configured backend SQL store committed custody.
The relay then delivers the oldest eligible message for one exact directed channel when the recipient reconnects.
The recipient acknowledges only after its production sync workflow and sender scoped receipt commit. Lost
acknowledgements and abandoned delivery attempts can duplicate delivery, so the guarantee is at least once.

Ordering is FIFO within the tenant, sender subject, sender peer, sender replica incarnation, and sender connection
epoch channel; the recipient is the inbox itself. Other channels can progress independently. Message expiry, terminal retention, receipt
retention, sender retry horizon, storage quotas, connection bounds, worker bounds, maximum delivery attempts, and
maintenance rates are explicit configuration. The default maximum is `16` delivery attempts. Reaching it durably
dead letters the message and erases its payload.

Authentication, authorization, TLS, endpoint routing, and optional payload encryption remain application
responsibilities. The relay outer digest provides integrity and conflict detection. It does not encrypt the payload.
Automerge `3.3.2` does not expose allocation bounded semantic decode. Relay infrastructure therefore validates only
the bounded opaque frame, envelope Schema, routing, hashes, outer digest, and provenance shape. Relay enabled
`PeerSync` performs the one semantic decode. Relay admission and delivery also require an explicit
`UnsafeUnboundedAutomerge3DecodeGrant` that exactly binds principal, remote, direction, documents, finite lease, and
revocation. Ordinary authentication and document authorization do not imply this resource trust. Default deny with
`PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode`.

After fresh ordinary and unsafe grants, revocation and the bounded relay operation contend on a local gate. A
revocation admitted first prevents SQL mutation or payload emission. An operation admitted first may finish and
returns its real result. Revocation drains that in flight operation and prevents later work. SQLite and the external
policy authority do not share an atomic transaction.

The durable protocol requires the separate unsafe grant. A future allocation bounded Automerge decode API should
remove that grant.

Relay custody uses the injected `RelayInboxStore`, owned by one cluster entity per recipient device.
`SqlRelayInboxStore.layer` supports SQLite, PostgreSQL, and MySQL through a generic `SqlClient`. Several relay nodes
may serve one database, but the relay does not add cross region routing or peer discovery. See
[Store and forward](store-and-forward.md) for the exact contract and composition.

Automerge provides merge infrastructure, not collaboration UX. The public snapshot exposes the decoded value and head
frontier. History traversal, conflict inspection, review, sharing, and conflict resolution interfaces remain
application responsibilities.
