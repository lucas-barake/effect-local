# Durability

SQLite is the client commit boundary. The server SQL transaction is the shared authority boundary.

## Client transactions

A local mutation transaction contains sequence allocation, handler writes, the pending envelope, its optimistic
result, and the write set. A process crash either preserves all of them or none of them.

A replication page transaction validates its scope generation, exact next view revision, change schemas, encoded byte
count, and digest. It applies all `Upsert`, `Delete`, and `Retract` changes, rebuilds visible state with unresolved
pending mutations, and advances the view cursor atomically. Only a final page advances the separate dense server
watermark used as the next mutation basis. Retractions are durable and suppress matching pending replay. A rejection
transaction records the receipt, removes its pending mutation, and performs the same visible replay without advancing
either cursor. Settled receipts and accepted evidence are deleted only behind explicit local retention targets. A
receipt referenced by pending work is never pruned.

A bootstrap page transaction validates the complete client, scope, schema, and view identity, exact next ordinal,
domain key and value Schemas, entry bytes, duplicate keys, aggregate bytes, continuation, and rolling digest before
extending a durable stage. Canonical state is unchanged while the stage is incomplete. Final installation revalidates the stage,
classifies pending work against the accepted and terminal snapshot fences, replaces canonical state, advances the
cursor, rebuilds optimistic visible state, and clears staging in one transaction. Restart resumes the next ordinal.

On restart, joined spaces, visible entities, and pending envelopes are already durable. One replica restores every
membership. Each membership persists its desired scope, view cursor, global watermark, and retractions, so
reconciliation resumes every stored view independently and resubmits the same pending identities. Leaving closes that
space's runtime and cascades its rows through the membership owner. The singleton client identity and other spaces
remain intact.

## Server transactions

The server creates and locks one space order row with `UPDATE ... RETURNING`. SQLite serializes the connection and
PostgreSQL holds the updated row lock until transaction completion. The lock covers identity rechecks, authorization,
handler execution, authoritative entity writes, sequence allocation, log append, receipt insertion, and client
watermark advancement.

The server stores terminal receipts before replying. A dropped response is therefore an ambiguous acknowledgement,
not an ambiguous mutation. Exact resubmission returns the stored receipt without executing the handler again.

Each scoped client view is also transactional. Acknowledged entities, the view revision, normalized scope, global
watermark, and one immutable outstanding page are durable SQL state. The server does not recompute an exposed page on
retry. It first acknowledges the exact target revision, updates materialized membership, and only then creates the
next page. The materialized view stores only current authorized `Upsert` rows. Acknowledged `Delete` and `Retract`
changes remove membership instead of accumulating tombstones. Per entity authorization and scope are recomputed for
the exact persisted entity value before a page reaches the wire. Principal changes or a now unsafe outstanding page
rotate the view and require a new scoped bootstrap. Replacement snapshots never carry identifiers from the prior
principal. The client derives missing canonical identities locally during atomic replacement.

The space row and a trigger maintained shadow row maintain matching retained history and receipt counts. Admission
cross checks them under the space lock. Entity count and snapshot bytes use the same lock. Hard caps fail before handler execution. State capacity checks run inside the handler
savepoint, so a mutation that cannot be represented by a future bounded snapshot becomes a durable terminal rejection
without changing authoritative entities.

Maintenance reads the materialized entities in one bounded consistent transaction and computes a deterministic digest.
Publication locks the space and compares both accepted and terminal heads. If either moved, the candidate is discarded.
Otherwise snapshot rows, the current snapshot pointer, and logical retained floors commit atomically. Physical prefix
deletion is bounded and occurs only after those writes. Receipt deletion advances each client's expired local sequence
watermark in the same transaction. Repeating maintenance finishes surplus rows left by interruption.

An exact retry whose full receipt remains returns that receipt after current authorization. Once its local sequence is
at or below the durable expired watermark, the server returns an `Expired` receipt bound to the current published
snapshot and never runs the handler. The client retains that pending envelope and its optimistic visibility until it
atomically installs the named or a newer covering snapshot, or until its durable cursor already covers the snapshot
sequence and therefore contains any accepted outcome.

SQL errors after a submit begins are exposed as `UnknownCommitOutcome`. The client keeps the pending envelope and
retries its stable identity. A later exact receipt resolves whether the original transaction committed.

## Validation

Every durable JSON column is parsed and Schema decoded before use. The definition hash prevents a database from being
opened with a different domain. The local database validates its singleton client identity while membership rows
define the joined spaces. Envelopes have a canonical SHA 256 digest. Version 3 binds the membership incarnation into
that digest. Unknown mutation names, malformed payloads, cursor gaps, identity conflicts, and capacity violations are
typed failures.

Client and server storage schemas advance through a package owned ordered migration catalog. Each stored descriptor includes
its id, name, and checksum. Migration acquires a database writer mutex before catalog reads, validates the complete
stored prefix, and retries only SQLite lock timeouts within the configured attempt bound. Bodies and catalog inserts
share one transaction. There is no backward storage migration path before v1.

Domain schema evolution is forward only and crash resumable. It copies log provenance, entities, receipts, pending
mutations, and retraction keys into an inactive generation in bounded batches. The client flips generations only after
all state is ready, clears schema bound view and bootstrap cursors, then cleans the source generation resumably. The
server activates the new entity generation and logically invalidates every scoped view by schema identity in the same
constant work transaction. It then deletes old snapshot entries, snapshots, outstanding pages, view members, and views
child first in bounded resumable phases. A compatible `Evolution` catalog therefore upgrades an existing database. A
changed definition without a matching evolution path is rejected.

Server migration requires a quiesced writer rollout. The migrated tables reject the legacy insert shape explicitly,
so an old process cannot continue writing without terminal sequences and retention metadata.
