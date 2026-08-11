# @lucas-barake/effect-local

Schema defined domain and protocol primitives for Effect Local.

Define models with `Model.make`, mutations with `Mutation.make`, queries with `Query.make`, and collect them with
`Definition.make`. Mutation and query handlers use Effect Layers and a constrained transaction capability. The package
also exports stable identities, accepted, rejected, and expired terminal receipts, immutable snapshot and bootstrap
contracts, tagged `ReplicaError` failures, replica status, canonical encoding, and opt in `Field.Semantics`.

`Model.make` accepts local secondary index declarations with ordered partition and sort components. The typed
`Transaction.Query.from` builder exposes index checked bounds, ordering, limits, opaque keyset cursors, pages, and
streams. Index layouts stay outside the wire definition and schema identity. See the repository guide for the complete
declaration and query examples.

This package does not persist or transport data. Use `@lucas-barake/effect-local-sql` for the local and authoritative
logs, `@lucas-barake/effect-local-rpc` for WebSockets, and `@lucas-barake/effect-local-browser` for Effect Atom.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
