# @lucas-barake/effect-local-browser

Worker and RPC composition, OPFS database ports, peer sessions, presence, and Effect Atom integration for Effect Local.

`BrowserReplica` provides `ReplicaClient` through normal Layer composition. The client exposes
`transient(peerId, documentId, payload)` and a reconnecting `transients` stream over the page to owner session.
Transient sends are never replayed after session replacement, and the stream can only observe future values.
`BrowserReplica.layer*` also provides registered typed transient contracts. Applications should use each contract's
document scoped `client`, `publish`, and `messages` API. The raw `ReplicaClient` operations are an advanced transport
escape hatch.

Import transport neutral peer sessions from `@lucas-barake/effect-local-sql/PeerSession`. The existing `@lucas-barake/effect-local-browser/PeerSession` subpath remains as a compatibility reexport.

See the [Effect Local documentation](https://github.com/lucas-barake/effect-local#readme) for browser setup, reactive atoms, sessions, presence, and API reference.
