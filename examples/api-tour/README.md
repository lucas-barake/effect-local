# Effect Local API tour

These runnable Node examples cover the high level Effect Local API without a browser or application server.

```sh
pnpm --dir examples/api-tour check
pnpm --dir examples/api-tour quickstart
pnpm --dir examples/api-tour backup
pnpm --dir examples/api-tour peer-sync
pnpm --dir examples/api-tour peer-session
pnpm --dir examples/api-tour faults
pnpm --dir examples/api-tour all
```

`quickstart` defines a document, fallible mutations, a projection, a SQL query, and a replica definition. It then exercises create, get, mutate, delete, receipt lookup, status, flush, outcome matching, projection evaluation, and the `TestReplica` convenience layer.

`backup` demonstrates whole replica clone and replace restoration. It also exports and imports a portable document and round trips its encoded value through the document codec.

`peer-sync` creates two independent replicas, makes concurrent offline edits, delivers durable peer sync messages out of order with a duplicate, and verifies convergence.

`peer-session` composes two complete SQL engines with a shared in-memory `TestPeer` transport. It keeps both session scopes active, selects one document, flushes an edit through the Cluster-backed document entity, and verifies convergence and peer observation.

`faults` builds a `TestPeer` network through `TestPeer.transportLayer` and applies a deterministic `FaultInjection` plan. It demonstrates dropped, reordered, duplicated, delayed, and explicitly flushed packets.

The `tasks` example covers browser ownership, SharedWorker transport, and Effect Atom integration. The `advanced-api` example covers Presence contracts. The `peer-session` example uses the public test transport because the repository intentionally does not bundle a real network transport.
