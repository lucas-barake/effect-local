# @lucas-barake/effect-local-sql

SQLite persistence, Automerge history, Effect Cluster command execution, and Effect Workflow operations for Effect Local.

Durable relay support adds the stable sender outbox, sender scoped recipient receipts, quota usage, maintenance, and
restore fencing used by store and forward. These frontend and replica stores remain SQLite. The backend relay custody
store is supplied separately by `@lucas-barake/effect-local-rpc`.
The command delivery ledger links local command receipts to exact Automerge changes and retained relay message
identity. `Replica.lookupCommandDelivery` and `Replica.commandDeliveryChanges` expose pending custody, accepted
custody, and unconfirmed sender deadline expiry without retaining payload bytes after terminalization.
Relay infrastructure keeps Automerge bytes opaque. Relay enabled `PeerSync` owns the one semantic decode and its
existing change, dependency, and operation limits.
Relay SQLite commits are not atomic with an external policy authority. The RPC server local gate decides whether
revocation prevents admission or drains an already admitted bounded operation.

`PeerSession` uses the same live transport connection for durable sync and document scoped transient values. Its
`transient` operation sends once, and its `transients` stream is bounded, multicast, and no replay.
`PeerRelayClientRuntime` registers live peer and document routes so an owner can send and receive transient values
without putting them in the SQL outbox or receipt stores.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for durable composition,
projections, recovery, compaction, sync, and API reference. The relay durability boundaries are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
