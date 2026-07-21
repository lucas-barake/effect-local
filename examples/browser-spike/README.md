# Browser durability proof

This is an internal integration proof for the Effect primitives under Effect Local. It exercises raw Effect Cluster,
Workflow, RPC, and SQL APIs. It is not a public Effect Local API tutorial. Start with the root quick start, the API
tour, or Local Tasks when learning the library.

This spike runs the page RPC client, a SharedWorker RPC facade, and a dedicated
OPFS SQLite worker. Chromium does not expose the `Worker` constructor inside a
`SharedWorkerGlobalScope`. The provisioning page therefore creates the OPFS
worker and transfers separate database and RPC `MessagePort` values to the
SharedWorker.

The OPFS worker is owned by the provisioning page. Closing that page terminates
the worker after the RPC runtime scope closes. A later page creates a fresh
SharedWorker and OPFS worker, then reopens the same durable database. This spike
does not prove that one SharedWorker can survive the closure of its provisioning
page or share the database worker across tabs.

Run the production build and browser proof from the repository root:

```sh
pnpm --dir examples/browser-spike build
pnpm --dir examples/browser-spike test:browser
```

The browser test proves the transactional entity commit, stored reply
deduplication, same database rollback when reply persistence fails, reload
persistence, Workflow recovery after owner restart, and streaming
responsiveness. The rollback case uses a SQLite trigger scoped to its dedicated
entity RPC. The trigger rejects the final reply insert after the application
write has run. The test then verifies that the application row, processed flag,
and reply all rolled back while the original mailbox message remains pending.
The test removes that probe message and trigger after the assertion.
