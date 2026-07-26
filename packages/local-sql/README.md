# @lucas-barake/effect-local-sql

SQLite persistence, Automerge history, Effect Cluster command execution, and Effect Workflow operations for Effect Local.

Durable relay support adds the stable sender outbox, sender scoped recipient receipts, quota usage, maintenance, and
restore fencing used by store and forward. These frontend and replica stores remain SQLite. The backend relay custody
store is supplied separately by `@lucas-barake/effect-local-rpc`.
Relay infrastructure keeps Automerge bytes opaque. Relay enabled `PeerSync` owns the one semantic decode and its
existing change, dependency, and operation limits.
Relay SQLite commits are not atomic with an external policy authority. The RPC server local gate decides whether
revocation prevents admission or drains an already admitted bounded operation.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for durable composition,
projections, recovery, compaction, sync, and API reference. The relay durability boundaries are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
