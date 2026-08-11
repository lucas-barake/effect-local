# @lucas-barake/effect-local-browser

Browser SQLite, Effect Atom, and best effort presence for Effect Local.

`BrowserSqlite.layerMessagePort` adapts an application owned SQLite WASM worker port. `BrowserReplica.make` creates one
Atom runtime with entity families, query families, concurrent mutation functions, receipt families, and replica
status. It uses Effect `Reactivity`, so committed local writes and installed server entries refresh only matching model
dependencies.

Pass either `SqlReplica.layer` or `SqlReplica.layerWorkflow` to the graph. The Workflow composition can run in an
application owned dedicated Worker or SharedWorker with SQL backed `SingleRunner` and `ClusterWorkflowEngine`.
`BrowserReplica` deliberately does not create a Worker or choose its URL, database name, credentials, runner storage,
or lifecycle policy. A SharedWorker must build one replica runtime per database and share it across page ports.
`Atom.runtime` stays on the page side when an application adds an RPC bridge to a worker owned replica.

`Presence.make` Schema decodes ephemeral values, expires them by TTL, and prevents slow stale decodes or scope
finalizers from replacing newer client state. Presence never enters SQLite or the mutation log.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
