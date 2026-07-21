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

The current protocol proves direct peer exchange while both peers participate. It does not yet implement asynchronous
store and forward collaboration. That requires relay storage, peer discovery, durable head receipts, retry policy, and
protocol handling for a peer that reconnects long after the sender has gone away. Setting a transport capability flag
does not supply those semantics.

Authentication, end to end encryption, subtree sync, and relay deployment are outside the first release. The current
engine is suitable as a local first persistence and client sync core, not a complete collaboration product by itself.

Automerge provides merge infrastructure, not collaboration UX. The public snapshot exposes the decoded value and head
frontier. History traversal, conflict inspection, review, sharing, and conflict resolution interfaces remain
application responsibilities.
