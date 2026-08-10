# Durability

SQLite is the client commit boundary. The server SQL transaction is the shared authority boundary.

## Client transactions

A local mutation transaction contains sequence allocation, handler writes, the pending envelope, its optimistic
result, and the write set. A process crash either preserves all of them or none of them.

An accepted batch transaction validates dense sequence continuity, stores each accepted entry, applies its write set
to canonical state, records local receipts, settles matching pending mutations, rebuilds visible state, advances the
cursor, and advances the visible revision. A rejection transaction records the receipt, removes its pending mutation,
and performs the same visible replay without advancing the server cursor.

On restart, visible entities and pending envelopes are already durable. Reconciliation resumes from the stored cursor
and resubmits the same pending identities.

## Server transactions

The server creates and locks one space order row with `UPDATE ... RETURNING`. SQLite serializes the connection and
PostgreSQL holds the updated row lock until transaction completion. The lock covers identity rechecks, authorization,
handler execution, authoritative entity writes, sequence allocation, log append, receipt insertion, and client
watermark advancement.

The server stores terminal receipts before replying. A dropped response is therefore an ambiguous acknowledgement,
not an ambiguous mutation. Exact resubmission returns the stored receipt without executing the handler again.

SQL errors after a submit begins are exposed as `UnknownCommitOutcome`. The client keeps the pending envelope and
retries its stable identity. A later exact receipt resolves whether the original transaction committed.

## Validation

Every durable JSON column is parsed and Schema decoded before use. The definition hash prevents a database from being
opened with a different domain. The local database also validates its space and client identity. Envelopes have a
canonical SHA 256 digest. Unknown mutation names, malformed payloads, cursor gaps, identity conflicts, and capacity
violations are typed failures.

There is no backward migration path before v1. Changing the domain definition changes its hash and requires a fresh
database.
