# @lucas-barake/effect-local-sql

SQLite persistence, Automerge history, Effect Cluster command execution, and Effect Workflow operations for Effect Local.

Optional relay support adds the stable sender outbox, sender scoped recipient receipts, quota usage, maintenance, and
restore fencing used by store and forward. Direct SQL composition remains unchanged.
Relay infrastructure keeps Automerge bytes opaque. Relay enabled `PeerSync` owns the one semantic decode and its
existing change, dependency, and operation limits.
Relay SQLite commits are not atomic with an external policy authority. The RPC server local gate decides whether
revocation prevents admission or drains an already admitted bounded operation.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for durable composition,
projections, recovery, compaction, sync, and API reference. The relay durability boundaries are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
