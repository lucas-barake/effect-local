# @lucas-barake/effect-local-rpc

Platform neutral Effect RPC building blocks for authenticated, durable peer synchronization with an Effect Local SQL
replica. Protocol version `1` uses one bounded relay topology with durable custody, reconnect delivery, and fenced
acknowledgements.

Backend custody is injected through `PeerRelayStore`. `SqlPeerRelayStore.layer` follows Effect Workflow's SQL storage
shape and works with a supplied `SqlClient` for SQLite, PostgreSQL, or MySQL. Frontend replica state, sender outbox,
and recipient receipts remain SQLite.
Automerge `3.3.2` has no allocation bounded semantic decode API, so safe relay composition explicitly passes
`denyUnsafeUnboundedAutomerge3Decode`. A separate exact unsafe resource trust grant is required to proceed. Ordinary
authentication and document authorization do not provide that grant.
Delivery attempts are durable and bounded. `RelayInbox.Options.maxDeliveries` is required per deployment, then dead
letters the message and erases its payload.
Policy revocation and each bounded relay operation contend on a local gate. Revocation admitted first prevents SQL
mutation or payload emission. An operation admitted first may finish. Revocation drains it and blocks later work.
External policy and SQLite do not share an atomic transaction.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for synchronization
architecture, deployment boundaries, and API reference. The relay contract, composition, limits, security,
and failure model are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
