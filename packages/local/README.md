# @lucas-barake/effect-local

Schema defined domain and protocol primitives for Effect Local.

Define models with `Model.make`, mutations with `Mutation.make`, queries with `Query.make`, and collect them with
`Definition.make`. Declare ephemeral channels with `Ephemeral.make` (an explicit `kind: "event"` or `kind: "state"`
with a typed payload, plus a typed key codec for state) and the roster value schema with `Ephemeral.member`;
`Ephemeral.group` rejects duplicate channel names. Ephemeral definitions drive the typed transport in
`@lucas-barake/effect-local-rpc` and never participate in durable schema identity or the mutation log. Mutation and query handlers use Effect Layers and a constrained transaction capability. The package
also exports stable identities, accepted, rejected, and expired terminal receipts, immutable snapshot and bootstrap
contracts, tagged `ReplicaError` failures, replica status, canonical encoding, and opt in `Field.Semantics`. A public
`Replica.Space` addresses data, pending work, receipts, replication scope, activation, and status for one durable
membership. Root aggregate status is a constant size count summary rather than a list of every space status.

`ReplicaError.ReplicaError` is the union the replica and sync services fail with. Storage failures say what went wrong:
`StorageUnavailable` wraps a SQL or platform error, `StorageCorrupt` reports a row that is undecodable or missing, and
`SpaceUnavailable` reports a space whose membership is gone. `CapacityExceeded.resource` is the closed union
`ReplicaError.CapacityResource`, so a consumer can match on the exact limit. `Canonical.stringify` sorts object keys
and encodes `Map`, `Set`, `HashMap`, and `HashSet` values with their members sorted, and `Chunk` values in order, so
equal values share one encoding and one hash.

Query handlers read through the `Transaction.Query` capability. `query.get(model, key)` reads one entity and re-runs
only when that entity changes. `query.sql(models, statement)` runs one raw SQLite statement over a CTE per declared
model, named after the model, with a `key` column, a `value` column holding the encoded entity JSON, and one column per
top-level field. Bounds, ordering, limits, and keyset pagination are plain SQL in that statement, and the mounted query
re-runs when any entity of a declared model changes.

`Model.make` accepts secondary index declarations with ordered partition and sort components. The server materializes
them to back replication windows, which bound a model to the newest entities per index partition. Index layouts stay
outside schema identity. See the repository guide for the complete declaration and query examples.

This package does not persist or transport data. Use `@lucas-barake/effect-local-sql` for the local and authoritative
logs, `@lucas-barake/effect-local-rpc` for WebSockets and the Effect Atom graph (`ReplicaAtom`),
`@lucas-barake/effect-local-browser` for the multi-tab browser replica, and `@lucas-barake/effect-local-expo` for React
Native.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
