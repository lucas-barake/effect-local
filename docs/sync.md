# Sync

## Model

Peer sync exchanges Automerge messages for selected whole documents through the `PeerTransport` service. A relay can
route encrypted or plain application frames, but it is never authoritative for local writes. Offline edits continue
without a connection.

Each connection has its own Automerge `SyncState`. State transitions are serialized. Only one send is in flight per
peer epoch and sequence. Inbound receipts store exact ordered reply fragments with session neutral identities and
durable sent progress. `PeerSession` idempotently enqueues only the pending suffix under the current local epoch, so a
duplicate delivery cannot recreate a sent prefix or reuse an expired connection sequence. Closing a session removes
its outbox and starts the next connection with fresh Automerge reconciliation. It recovers document state, not the
identity of the previous in flight transport operation. A bounded transport send retains the shared restore fence
until the network effect completes or reaches `maxPeerSendMillis`, so destructive restore cannot overtake an old
outbound frame.

Convergence means two replicas have received the same valid change set. It does not mean every peer is current at all
times. The engine preserves temporary divergence as normal operation.

## Bounds

Inbound frames are decoded before application. Limits cover bytes, changes, dependency edges, operations, pending
bytes, pending changes, pending dependency edges, and age. Limits exist per document, per peer, and per replica. A
peer cannot force unbounded missing dependency retention.

A logical sync response that exceeds the per message change, writer provenance, or byte limit is split into fragments
in dependency order. Every fragment satisfies `maxSyncChangesPerMessage`, the wire provenance limit, and
`maxSyncMessageBytes`. The complete batch also remains within the peer's pending message and byte quotas. If one
change cannot fit in a valid fragment, the response fails with `QuotaExceeded` instead of emitting an oversized frame.

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

## Conflicts and resolution

Automerge convergence does not choose an application policy for concurrent register assignments.
`Replica.inspectConflicts` returns the decoded document snapshot together with the retained alternatives at its
exact head frontier. Conflict paths are structured key and index segments. Intermediate segments can identify a
specific conflicted composite branch. Alternative identifiers are opaque Automerge operation identifiers and
must not be parsed or synthesized.

The snapshot and alternatives cross different representation boundaries. `inspection.snapshot.value` is
decoded through the registered document schema. Conflict alternatives expose the document schema's encoded
native CRDT value. The `Conflict.Value` codec preserves Text, `ImmutableString`, dates, bytes, counters, maps,
and lists through a tagged portable JSON representation. A replacement value uses that same native encoded
model.

Inspection is bounded by configured traversal limits and verified cumulative source history counters. A legacy
or checkpoint backed import whose complete raw history cannot be measured remains readable, but conflict
inspection and further local or peer changes fail with `ReplicaError` / `QuotaExceeded`. Operator driven history
rewrite makes the source measurable again by creating new lineage. It also permanently discards losing
alternatives, so it is not a transparent way to recover conflict review.

`Replica.resolveConflict` is a durable local command. It requires the exact heads returned by inspection and
commits through the normal document, projection, outbox, and command receipt transaction. A changed frontier
returns the durable `StaleConflictResolution` rejection. The application must inspect again, review the new
state, and issue a new command ID. The frontier guard is conservative, so a concurrent change to an unrelated
field also makes the resolution stale. If the command outcome is ambiguous,
`lookupConflictResolution` requires the original document, command ID, and complete resolution descriptor.
This prevents a lookup from returning a receipt for different intent.

Selecting an alternative is supported for scalar values. Direct selection of a map or list is rejected because
copying a composite gives it new Automerge object identity. That replacement can hide concurrent descendant
edits in another branch. An application can use `ReplaceValue` only after making that loss of identity and
visibility an explicit product decision.

Resolution is not a synchronization barrier or global finality. Two partitioned replicas can resolve the same
conflict concurrently. Their fresh resolution assignments converge to a new conflict when both change sets
arrive. Applications that need a resolved view must inspect again after later synchronization.

`DeleteValue` follows Automerge observed remove semantics. It removes assignments in the resolver's causal
past. A concurrent assignment survives, which gives assignment add wins behavior against that delete.
Concurrent deletes do not add a value alternative.

Ordinary schema strings encode as Automerge Text for character sequence coediting. Concurrent passages can
interleave. Edited messages that must preserve each complete revision for later review should encode their
application `string` as `Automerge.ImmutableString`. See the
[conflict workflow](../README.md#conflict-inspection-and-durable-resolution) and the compile checked
[`edited-messages.ts`](../packages/local-sql/examples/edited-messages.ts).

History traversal, review presentation, sharing, and the final product policy remain application
responsibilities. History rewrite is not conflict resolution. It changes lineage and permanently removes losing
alternatives.
