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

Compaction has separate prepare, compare and publish, and prune phases. It retains two checkpoints and prunes only
changes dominated by both retained recovery paths. A crash before publish leaves the old checkpoint authoritative. A
crash after publish leaves enough history to recover.

## Workflows

`ClusterWorkflowEngine` uses the same SQL backed single runner composition as document entities. Message and runner
storage use private `effect_local_cluster` and `effect_local_runner` table prefixes. The registered
compaction workflow derives its execution identity from replica incarnation and operation identity. It journals a
document listing activity and one compact and prune activity per document. `CompactionWorkflow` exposes execute, poll,
and resume while rejecting handles from a prior replica incarnation.

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
