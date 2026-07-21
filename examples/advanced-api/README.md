# Advanced API examples

These focused command line examples cover public APIs that are easier to understand outside the full browser application.
They use Effect 4.0.0 beta.99 and import only public Effect Local package entry points.

Run every example from the repository root:

```sh
pnpm --dir examples/advanced-api examples
```

Type check the package without running it:

```sh
pnpm --dir examples/advanced-api check
```

## Presence and peer sessions

Run the Presence example:

```sh
pnpm --dir examples/advanced-api presence
```

[`src/presence.ts`](src/presence.ts) creates an in memory implementation of the public `PeerSession.Service` contract
and provides it through an Effect Layer. It then creates a schema validated Presence registry and demonstrates:

- scoped publication for the local peer
- decoding valid and invalid transport payloads
- reading active values
- receiving remote peer state
- removing a peer explicitly
- automatic TTL expiration
- session dirty tracking, flushing, and observation

Presence is intentionally ephemeral. Restarting the registry starts with no peer state. Durable document state belongs in
the replica and SQLite layers instead.

## Operational service contracts

Run the operations example:

```sh
pnpm --dir examples/advanced-api operations
```

[`src/operations.ts`](src/operations.ts) demonstrates the public operational service contracts through injected Layers:

- `WorkflowRuntime.execute`, `poll`, and `resume`
- `Compaction.prepare`, `publish`, `compact`, and `prune`
- `Recovery.exportRaw`
- scoped `CommitPublisher.subscribe` streams
- commit delivery and full refresh invalidations

The implementations in this example are deterministic in memory adapters. This is deliberate. A live `WorkflowRuntime`,
`Compaction`, and `Recovery` composition requires the SQLite schema, replica gate, Workflow engine, replica definition, and
Cluster runners. The Tasks application demonstrates that complete browser composition. The browser durability proof covers
restart recovery and durable workflow behavior. This example instead isolates the dependency injection surface consumers use
for tests, alternate hosts, and operational tooling without importing package internals.
