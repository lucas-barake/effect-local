# @lucas-barake/effect-local-rpc

Platform neutral Effect RPC building blocks for authenticated peer synchronization with an Effect Local SQL replica.
Direct protocol version `2` remains application owned and live only. Optional protocol version `3` adds a separate
bounded listener, authenticated relay authorization, and single authority SQLite store and forward custody.
Automerge `3.3.2` has no allocation bounded semantic decode API, so safe relay composition explicitly passes
`denyUnsafeUnboundedAutomerge3Decode`. A separate exact unsafe resource trust grant is required to proceed. Ordinary
authentication and document authorization do not provide that grant.
Delivery attempts are durable and bounded. `PeerRelayLimits.maximumDeliveryAttempts` defaults to `16`, then dead
letters the message and erases its payload.
Policy revocation and each bounded relay operation contend on a local gate. Revocation admitted first prevents SQL
mutation or payload emission. An operation admitted first may finish. Revocation drains it and blocks later work.
External policy and SQLite do not share an atomic transaction.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for synchronization
architecture, deployment boundaries, and API reference. The optional relay contract, composition, limits, security,
and failure model are documented in
[Store and forward](https://github.com/lucas-barake/effect-local/blob/main/docs/store-and-forward.md).
