# Durability

SQLite is the client commit boundary. The server SQL transaction is the shared authority boundary.

Client secondary indexes are derived durable state. A static migration installs their checksum catalog. Each declared
owner qualified layout creates one shadow table and covering scan index whose normalized `sqlite_schema` statements
must match the catalog checksums. Catalog rows store a backfill generation, durable entity key cursor, visible revision,
and ready generation. A changed visible revision restarts the bounded scan before the layout can become ready. A lost
physical table clears readiness and rebuilds. Superseded objects are removed only after every current declaration is
ready. Startup after schema evolution exposes the replica only after every declared index is ready for the active
generation. Local mutation, receipt replay, accepted entry installation, and bootstrap replacement update or rebuild
index rows inside the same SQLite transaction as visible state.

## Client transactions

A local mutation transaction contains sequence allocation, handler writes, the pending envelope, its optimistic
result, and the write set. A process crash either preserves all of them or none of them.

An accepted batch transaction validates dense sequence continuity, stores each accepted entry, applies its write set
to canonical state, records local receipts, settles matching pending mutations, rebuilds visible state, advances the
cursor, and advances the visible revision. A rejection transaction records the receipt, removes its pending mutation,
and performs the same visible replay without advancing the server cursor. Settled receipts and accepted evidence are
deleted only behind explicit local retention targets. A receipt referenced by pending work is never pruned.

A bootstrap page transaction validates the complete manifest identity, exact next ordinal, domain key and value
Schemas, canonical entity bytes, duplicate keys, aggregate bytes, continuation, and rolling digest before extending a
durable stage. Canonical state is unchanged while the stage is incomplete. Final installation revalidates the stage,
classifies pending work against the accepted and terminal snapshot fences, replaces canonical state, advances the
cursor, rebuilds optimistic visible state, and clears staging in one transaction. Restart resumes the next ordinal.

On restart, joined spaces, visible entities, and pending envelopes are already durable. One replica restores every
membership and reconciliation resumes each stored cursor independently. Leaving closes that space's runtime and
cascades its rows through the membership owner. The singleton client identity and other spaces remain intact.

## Server transactions

The server creates and locks one space order row with `UPDATE ... RETURNING`. SQLite serializes the connection and
PostgreSQL holds the updated row lock until transaction completion. The lock covers identity rechecks, authorization,
handler execution, authoritative entity writes, sequence allocation, log append, receipt insertion, and client
watermark advancement.

The server stores terminal receipts before replying. A dropped response is therefore an ambiguous acknowledgement,
not an ambiguous mutation. Exact resubmission returns the stored receipt without executing the handler again.

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

Client and server schemas advance through a package owned ordered migration catalog. Each stored descriptor includes
its id, name, and checksum. Migration acquires a database writer mutex before catalog reads, validates the complete
stored prefix, and retries only SQLite lock timeouts within the configured attempt bound. Bodies and catalog inserts
share one transaction. There is no backward migration path before v1. Changing the domain definition changes its hash
and requires a fresh database.

Server migration requires a quiesced writer rollout. The migrated tables reject the legacy insert shape explicitly,
so an old process cannot continue writing without terminal sequences and retention metadata.
