# @lucas-barake/effect-local-browser

Browser SQLite, OPFS attachments, Effect Atom, and best effort presence for Effect Local.

`BrowserSqlite.layerMessagePort` adapts an application owned SQLite WASM worker port. `BrowserReplica.make` creates one
Atom runtime with space addressed entity families, query families, concurrent mutation functions, receipts, and
per space status. It exposes `scope`, `setScope`, `activation`, `activate`, and `deactivate` families beside `spaces`,
`join`, `leave`, and the constant size `aggregateStatus`. It uses Effect `Reactivity`, so committed local writes and
installed server entries refresh only matching space and model dependencies. Its `attachment` family exposes lazy
`AsyncResult` placeholder, failure, and byte states through `Space.readAttachment`. Membership, aggregate, scope,
activation, and addressed status have separate keys. Leaving a space invalidates retained atoms for that address.

Pass either `SqlReplica.layer` or `SqlReplica.layerWorkflow` to the graph. The Workflow composition can run in an
application owned dedicated Worker or SharedWorker with SQL backed `SingleRunner` and `ClusterWorkflowEngine`.
`BrowserReplica` deliberately does not create a Worker or choose its URL, database name, credentials, runner storage,
or lifecycle policy. A SharedWorker must build one replica runtime per database and share it across page ports.
`Atom.runtime` stays on the page side when an application adds an RPC bridge to a worker owned replica.

`BrowserAttachmentStorage.layerMessagePort` streams bounded chunks to an application owned worker port.
Run `BrowserAttachmentWorker.serveMessagePort` in that dedicated worker to keep attachment bytes as direct OPFS files.
The application chooses the worker URL, port lifetime, OPFS directory, byte limit, and read chunk size.

`Presence.make` Schema decodes ephemeral values, expires them by TTL, and prevents slow stale decodes or scope
finalizers from replacing newer client state. Presence never enters SQLite or the mutation log.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
