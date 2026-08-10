# @lucas-barake/effect-local-browser

Browser SQLite, Effect Atom, and best effort presence for Effect Local.

`BrowserSqlite.layerMessagePort` adapts an application owned SQLite WASM worker port. `BrowserReplica.make` creates one
Atom runtime with entity families, query families, concurrent mutation functions, receipt families, and replica
status. It uses Effect `Reactivity`, so committed local writes and installed server entries refresh only matching model
dependencies.

`Presence.make` Schema decodes ephemeral values, expires them by TTL, and prevents slow stale decodes or scope
finalizers from replacing newer client state. Presence never enters SQLite or the mutation log.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
