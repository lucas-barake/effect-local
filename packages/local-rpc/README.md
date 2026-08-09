# @lucas-barake/effect-local-rpc

Platform neutral Effect RPC building blocks for authenticated peer synchronization with an Effect Local SQL replica.
Protocol version `1` uses one bounded relay topology for both durable custody with reconnect delivery and fenced
acknowledgements, and best effort transient delivery over the same live session.

The relay is a cluster of `RelayInbox` entities, one per recipient device, so more than one relay node can serve one
database. `RelayServer` is the stateless front door; each entity is the sole writer for its device's inbox. Backend
custody is injected through `RelayInboxStore`, and `SqlRelayInboxStore.layer` implements it against a supplied
`SqlClient` for SQLite, PostgreSQL, or MySQL. The relay requires a `Sharding` but never builds one, so the deployment
shape stays the application's choice. Frontend replica state, sender outbox, and recipient receipts remain SQLite.

Automerge `3.3.2` has no allocation bounded semantic decode API, so safe relay composition explicitly passes
`denyUnsafeUnboundedAutomerge3Decode`. A separate exact unsafe resource trust grant is required to proceed. Ordinary
authentication and document authorization do not provide that grant.

Delivery attempts are durable and bounded. `RelayInbox.Options.maxDeliveries` is required per deployment; a message
that exhausts it is dead lettered so it stops blocking its channel. An attempt is charged only once the message has
reached the transport, so a delivery prepared and then abandoned on a flaky connection costs nothing.

Authorization is re-checked on every operation rather than once at the handshake, because a grant can be narrowed or
revoked while a session is live. A session whose credential or grants lapse is ended by the relay, which is the only
party that observes them.

Transient payloads are limited to 4 KiB. The relay derives sender and document routing from the authenticated session,
checks directional authorization, applies a per session token bucket, and offers to a bounded nonblocking recipient
queue. Offline and slow recipients lose values. Transient values never enter relay custody, sender outbox, recipient
receipts, settlement, backup, or Automerge history.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for synchronization
architecture, deployment boundaries, and API reference. The relay contract, composition, limits, security,
and failure model are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
