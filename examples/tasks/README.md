# Local Tasks

This example is the complete browser composition for Effect Local.

Install dependencies from the repository root with `pnpm install`. The example requires a Chromium browser with
SharedWorker, OPFS, WebAssembly, and service worker support.

The React page uses Effect Atom query and command atoms. `replica-client.ts` also exports the public document family and
mutation helpers as focused API examples. `BrowserReplica.layer` receives the official Effect browser worker layer. All
tabs attach to one named SharedWorker. The first tab provisions the dedicated OPFS worker and transfers its database
port. Later tabs attach RPC ports to the existing owner without creating another database worker. The SharedWorker
builds one `SqlReplica.layer`, which routes commands through the durable Effect Cluster runner and stores documents,
receipts, projections, queries, and cluster state in SQLite.

Chromium does not expose the `Worker` constructor inside a SharedWorker. The provisioning tab must therefore remain open while other attached tabs use the owner. Reloading the provider starts a fresh owner. Automatic handover to an already attached secondary tab is not implemented yet.

The application shell is installable. Its service worker caches only the known shell, worker, and WebAssembly assets after the first controlled load. Task data remains in OPFS independently of the shell cache.

The first launch waits for service worker control before starting the replica. An installed shell can therefore reload offline, reprovision the dedicated OPFS worker, read existing tasks, and commit new local changes. Expiring provision requests prevent a dead candidate from stalling ownership. The provider control port is also used to reject a stale SharedWorker owner before a new attachment is served.

The UI requests persistent browser storage and reports whether the browser granted it or retained best effort policy. Backup controls export the canonical bounded NDJSON archive and restore it through destructive replace mode after explicit confirmation.

```sh
pnpm --dir examples/tasks dev
pnpm --dir examples/tasks build
pnpm --dir examples/tasks preview
pnpm --dir examples/tasks test:browser
```
