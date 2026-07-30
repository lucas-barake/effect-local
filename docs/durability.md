# Durability

## Command acknowledgement

A local command is acknowledged only after these records commit:

1. Canonical Automerge changes and heads.
2. Projection rows and projected heads.
3. A receipt keyed by replica incarnation and command ID.
4. The monotonic visible commit sequence.
5. The persisted Cluster request and reply.

`DocumentEntity` marks create, mutate, and delete RPCs as persisted, transactional, and client uninterruptible. Its
primary key includes replica incarnation, command ID, and canonical request hash. A repeated matching request returns
the stored reply. Reusing a command ID for different input reaches receipt validation and fails with
`CommandIdConflict`. Receipts record the operation that produced them, and a lookup under a different operation type
or mutation fails with `ReceiptOperationMismatch`.

The page acquires the shared operation gate before Cluster dispatch. The entity validates its captured writer epoch
inside the transaction without reacquiring the gate. Restore can therefore acquire the exclusive gate without a
single connection deadlock.

## Recovery

Document loads verify checkpoint checksums, Automerge heads, change metadata, and the tombstone marker. Recovery
falls back to an older verified checkpoint and replays accepted changes. Corrupt candidates are quarantined.

Compaction has separate prepare, compare and publish, change prune, and receipt reclamation phases. It retains two checkpoints and prunes only
changes dominated by both retained recovery paths. A crash before publish leaves the old checkpoint authoritative. A
crash after publish leaves enough history to recover.

Compaction also reclaims command receipts on a second, independent axis. Receipts are keyed by replica incarnation
and are only ever read at the current one, so rows below it can never resolve a lookup again. Compaction deletes them
in bounded batches, each batch revalidating the writer permit inside its own transaction. Receipts at the current
incarnation are never touched, so replay suppression and outcome recovery are unchanged. Restore is the only shipped
code path that advances the incarnation, so a replica that has never restored has nothing to reclaim.

Publish installs the checkpoint with an optimistic compare and set against the global commit sequence, so a
concurrent commit anywhere in the replica supersedes a prepared checkpoint. A superseded publish is a committed no
op, which makes re-preparing safe. Change pruning runs only after a checkpoint is published. Receipt reclamation is
independent of any checkpoint and runs once per compaction run.

## Document lineage and history rewrite

Compaction never discards causal history. It bounds SQL change rows while the surviving checkpoint still encodes
every change the document ever had, because `Automerge.save` writes the whole change graph and Automerge `3.3.2`
exposes no way to remove history from a live document. `Compaction.rewriteHistory` is the one operation that does
discard it. It rebuilds the document as a fresh single change document carrying the value the current one
materializes to. It is reached only through `ReplicaWorkflow.HistoryRewriteWorkflow`, never from a page, a peer, or
a document entity.

Every document carries a lineage. Migration 8 adds the column to `effect_local_documents`,
`effect_local_checkpoints`, and `effect_local_peer_outbox` with a default of the empty string. That is the genesis
lineage, so every existing row and every never rewritten document agrees without a handshake. A rewrite mints `lin_`
followed by a UUID and installs it with the swap that installs the rebuilt document.

Expensive work happens outside the transaction, as it does in prepare and prune. Inside one transaction the rewrite
rechecks the durable marker, requires a settled history, compares and swaps the document row against the heads the
rebuild was actually derived from, deletes that document's changes, checkpoints, peer outbox rows, and peer
receipts, installs one verified checkpoint and the root change row, and reads its own work back while the
transaction can still roll back. Command receipts are retained, so command idempotency is unchanged. A failed
compare and swap fails the effect instead of committing a no op, because a commit after those deletes would leave a
document with no recoverable state.

Migration 9 adds `effect_local_history_rewrites`, keyed `(replica_incarnation, operation_id)`. Workflow idempotency
dedupes operator requests, not activity attempts. A crash between the rewrite's SQL commit and the journaling of its
activity reply reruns the activity, and this marker is what makes the rerun return the lineage already minted rather
than a second one. The row is written by the same transaction as the rewrite, so it cannot be observed apart from
it. The incarnation is part of the key for the same reason command receipts carry one: restore raises the
incarnation, so a marker written before a restore cannot short circuit a rewrite after it. An operation identity is
bound to the first document it rewrote, and reusing it against a different document fails with `ProtocolMismatch`
before anything destructive runs.

Lineage is compared, never merged. Every Automerge ingestion path is a union, so applying a message from a
superseded lineage restores the discarded history and then reverts the rewritten value, because register winners are
ordered by Lamport operation counter before actor identity. Both directions therefore refuse. A peer that has not
advertised `lineageAware` is never sent a rewritten document. A peer whose asserted lineage differs from the local
one is refused for that document alone, and the session keeps serving every other selected document. Lineage is an
unauthenticated peer assertion. It is a correctness signal against an honest but stale peer, not a security control.

Canonical archives carry lineage on every document and checkpoint record, and restore preserves it. A refused peer
can therefore be repaired by exporting the authoritative replica after the rewrite and clone restoring that archive
onto the refused replica. Replace restore is appropriate only when the archive source replica has been retired,
because it adopts the source replica identity. Either mode discards any unsynced local change on the superseded
lineage because it has no causal relationship to the new root and cannot be merged safely.

## Workflows

`ClusterWorkflowEngine` uses the same SQL backed single runner composition as document entities. Message and runner
storage use private `effect_local_cluster` and `effect_local_runner` table prefixes. The registered
compaction workflow derives its execution identity from replica incarnation and operation identity. It journals a
command receipt reclamation activity, then a document listing activity, then one compact activity per document.
Reclamation is journaled first so a document that cannot publish does not starve it. `CompactionWorkflow` exposes execute, poll,
and resume while rejecting handles from a prior replica incarnation.

The registered history rewrite workflow derives its execution identity from replica incarnation, document identity,
and operation identity. It journals one activity, which reads the document type from storage rather than from the
payload, performs the rewrite, and drops that document's in memory peer sync state. `HistoryRewriteWorkflow` exposes
the same execute, poll, interrupt, and resume surface and rejects handles from a prior replica incarnation in the
same way. Its handles carry the document identity as well.

A compact activity retries a bounded number of times when its checkpoint publication is superseded. If every attempt
is superseded the document is recorded and the run continues, and the workflow fails once at the end with
`CheckpointSuperseded` listing every superseded document. A failed exit therefore means partial compaction, not
zero. That continuation guarantee covers a superseded publish only. Any other failure stops the run at the document
that raised it. Because the outcome is journaled per execution identity, retrying needs a new operation identity.

Adding a reason to `ReplicaError` is backward compatible for records an older build wrote, but not forward
compatible. A record carrying a reason a build does not know fails to decode and becomes a defect, so a local
replica database must not be opened by a build older than the one that wrote its workflow records. This release adds
`DocumentLineageChanged` and `ReplicaMetadataMissing`, so a journaled workflow record carrying either is undecodable
by every earlier build.

`ReplicaMetadataMissing` reports that the metadata singleton row is gone, so the replica has no identity. It is
replica wide and is deliberately not `StorageCorrupt`, which means one document's stored bytes are unusable and which
consumers act on per document. Every read of that row answers with this one reason, and `operation` names the read
that observed the absence.

Projection rebuild, backup, and restore definitions reserve stable identities but are not registered operations in
the current beta. Backup creation needs an explicit durable destination contract. Restore needs an explicit durable
source contract. Neither is inferred from an in memory browser stream.

Workflow code may replay from its beginning. Named activity replies are durable. An external activity is therefore
at least once unless its own boundary honors the workflow idempotency key. Compensation is terminal cleanup, not a
promise that browser termination will run a finalizer.

Effect beta.99 does not safely retain dynamic transaction annotations during every activity defect replay. Database
activities are independently idempotent instead of relying on an undocumented ambient transaction.

## Backup and restore

Canonical backup is a bounded NDJSON archive with checked record schemas, checksums, and raw binary data encoded as
base64. Replace restore stages and validates the archive before changing the active incarnation. Clone restore creates
a new local identity. Portable document export is flattened schema coded data and creates fresh causal history when
imported.

Restore never treats projection tables as canonical backup content. It clears derived tables and deterministically
rebuilds every registered projection from the installed canonical documents inside the fenced restore transaction.
Projection failure rolls back the replacement.

Browser persistence does not make backup optional. OPFS may be evicted while its origin storage bucket remains best
effort. A complete product must expose export, restore, duplicate, and deletion flows to the person who owns the data.

## Store and forward custody

Store and forward adds three different durable boundaries.

1. Sender admission commits a stable relay message identity and complete outer envelope to the local relay outbox
   before a network attempt.
2. `Push` succeeds only after the backend relay store commits the envelope and immutable quota reservation.
3. `AcknowledgeRelay` is attempted only after the recipient commits the Automerge application result and sender
   scoped replay receipt through the production sync path.

Relay admission and delivery require the ordinary authorization lease and a separate exact unsafe Automerge resource
trust lease. After fresh grants, a bounded server operation and policy expiry or revocation contend on a local gate.
Revocation that wins before admission prevents the SQL mutation or payload emission. An operation that wins is in
flight, may finish its durable commit or delivery, and returns its real result. Revocation drains that operation and
prevents later work. It does not retroactively cancel the winner. SQLite and the external policy authority do not
share an atomic transaction. The extra grant exists because Automerge `3.3.2` does not expose allocation bounded
semantic decode. Authentication and document access alone do not satisfy it.

These boundaries provide at least once transfer. They do not provide exactly once delivery. A response can be lost
after any commit. A delivery attempt can be abandoned while its message is still `Pending`. The durable identities
and per attempt claim tokens make retries and stale acknowledgements safe, while retained recipient receipts make
duplicate application idempotent during the negotiated window.

The sender also retains a command delivery ledger after an outbox payload is removed. Each locally committed command
is linked to its exact Automerge change hashes. Relay messages are linked to the exact hashes they contain. Relay
custody acknowledgement and sender deadline expiry update that ledger and emit durable delivery invalidations.
Applications read it with `Replica.lookupCommandDelivery` or subscribe with `Replica.commandDeliveryChanges`.

An expired sender row records `RelayCustodyUnconfirmedAtDeadline`. It does not record failure because a response may
have been lost after the relay commit. A matching late acknowledgement changes the durable state to
`RelayCustodyAccepted`. Restore clears command delivery evidence with the old replica incarnation and advances a
refresh epoch so active subscribers reload their snapshots.

Relay FIFO ordering is scoped to one exact directed peer channel, which includes the sender's connection epoch. One
in flight head blocks later rows in that channel. Unrelated channels do not share that ordering fence.

Recipient delivery attempts are durable and bounded by `RelayInbox.Options.maxDeliveries`. An attempt is charged only
once the message has reached the transport, so a delivery abandoned by a disconnect costs nothing. Reaching the cap
changes the message to `DeadLettered` so it stops blocking its channel. Restart cannot reset this poison bound.

Undelivered messages expire at `RelayInbox.Options.messageTtl`. A terminal row keeps its stored envelope until
retention collects the whole row past its deduplication horizon, which is what lets a sender that still holds custody
revive a dead lettered or expired identity. Sender outbox rows expire at their configured retry horizon. Recipient receipts expire according to `PeerRelayReceiptLimits`.

Relay custody rows and client relay outbox or receipt rows are not canonical document history and are not included in
canonical backup archives. Restore fences the previous incarnation, removes stale relay client state, and reconciles
usage before installing the new incarnation. See [Store and forward](store-and-forward.md) for the complete contract.
