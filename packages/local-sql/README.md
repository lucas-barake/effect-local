# @lucas-barake/effect-local-sql

SQLite persistence and authoritative mutation ordering for Effect Local.

`SqlReplica.layer` provides one public replica for many spaces in one SQLite database. Membership is durable and every
client table is partitioned by `space_id`. Each `Replica.Space` handle owns its operations, replication scope,
activation, and status. Membership is lightweight. Addressed operations activate foreground LRU residents, while
inactive spaces with pending mutations use short lived background runtimes. `maximumActiveSpaces` bounds all per space
runtimes and `foregroundActiveSpaces` reserves capacity for addressed work. `foregroundReconciliationConcurrency`
reserves foreground turns within the total `reconciliationConcurrency`. Active logical watches share the one
`SyncEngine` and RPC WebSocket.

`SqlReplica.layerWorkflow` uses the same store, query executor, and idempotent reconciliation pass with finite Effect
Workflow generations. Local SQLite stores canonical entities, visible entities, pending mutations, bounded terminal
receipts, a bounded accepted suffix, per space cursors, resumable snapshot staging, and requested and completed
reconciliation generations. Optimistic writes, incremental reconciliation, and snapshot installation are
transactional. Workflow storage contains execution control only.

Declared model indexes are materialized as owner qualified SQLite shadow tables with typed component columns and
covering scan indexes. A checksum catalog verifies exact DDL and resumes bounded active generation backfills only
while the visible revision remains stable. Missing tables are rebuilt and obsolete layouts are removed after current
layouts become ready. Query
handlers use SQLite bounds, ordering, limits, and keyset continuation, then decode every selected row through
`SqlSchema` and the model Schema. Portable streams paginate because the pinned Node and worker SQLite drivers do not
provide a schema decoded statement stream. Local writes and sync replay update shadow rows in the same transaction.
Each replica owns its mounted query footprints. Range intersection refreshes only results that can change. Publication
is queued before transaction exit and flushed by the outer Reactivity batch after commit or rollback.

The caller chooses the Workflow engine and runner. A durable single runner composition is:

```ts
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Layer from "effect/Layer"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import { definition, Todo } from "./domain.js"

const layerWorkflowEngine = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(SingleRunner.layer({ runnerStorage: "sql" }))
)

const layerReplica = SqlReplica.layerWorkflow({
  definition,
  clientId,
  defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
  initialSpaces: [spaceId]
}).pipe(
  Layer.provide(layerWorkflowEngine)
)
```

Only `definition` and `clientId` are required. `SqlReplica.defaults` lists the rest: 16 active spaces with 4 reserved
for foreground work, 256 retained receipts under a cap of 10000, 256 retained history entries, bootstrap bounds of
100000 entities, 64 MiB, and 4 MiB pages. `defaultScope` defaults to every model in the definition. A caller-minted
`mutationId` stays idempotent while its receipt is retained, and after that for the next `retainedMutationIds` (100000)
mutations of the space, where reusing it fails with `MutationIdentityConflict` instead of running the handler again.

`initialSpaces` seeds remembered membership without opening every space. Later calls to `Replica.join` persist
membership and restart restores every handle inactive. Data operations and `space.activate` acquire foreground
capacity. `space.deactivate` closes its runtime without deleting data. Pending work still drains in the background.
`Replica.leave` closes any runtime before one cascading delete removes local state. The database keeps the singleton
`clientId`. Rejoining creates a new membership incarnation and local sequence.

`defaultScope` initializes only new membership. Every handle exposes its durable `space.scope` and
`space.setScope`. Changing one space advances its generation, restarts its active watch, and reconciles it as foreground
work. A wider scope backfills through incremental pull. A narrower scope receives `Retract` changes without a new
bootstrap. Scopes support complete models and bounded secondary index windows with per partition overrides.

`retryDelay`, `maximumRetryDelay`, and `maximumAttempts` bound exponential retries within one Workflow execution. A
terminal failed generation stays failed until a later mutation or server wake requests a new generation. Effect
4.0.0-rc.117 does not expose per Workflow completed history retention through `WorkflowEngine`; storage lifecycle remains
an operational responsibility of the selected engine and runner.

Provide separate `SqlClient` connections to the replica and to SQL backed `SingleRunner`. They may use the same file,
but a Workflow runner can retain its transaction while the handler uses the application database. Browser applications can build this graph in a long lived
worker over SQLite WASM. Worker construction, database identity, socket credentials, and shutdown remain application
owned. Disposing a worker runtime allows unfinished executions to recover later. Do not call `Workflow.interrupt` for
ordinary shutdown because it durably cancels the reconciliation.

`ServerStore.layer` requires application supplied access, mutation admission, and read callbacks. It reauthorizes
retries, deduplicates stable mutation identities, stores terminal rejections, assigns the next dense sequence to
accepted mutations, and materializes authoritative state in the same SQL transaction. Its history options set
retained targets, hard admission caps, snapshot capacity, bootstrap page capacity, prune batches, retained snapshots,
migration retry, maintenance concurrency, and the keyset page size used to enumerate spaces. `maximumWatchersPerSpace`,
`readAuthorizationRefreshInterval`, `maximumConcurrentReadAuthorizations`, `maximumPendingReadAuthorizations`, and
`readAuthorizationCacheCapacity` bound live sync streams and their policy work. Every one of them is optional and
`ServerStore.defaults` lists the values used. `ServerStore.layerTrusted` is the explicit allow all composition.
`SyncServer.layer` in `@lucas-barake/effect-local-rpc` builds the store for you.

Sync watch authorization shares successful structural `(spaceId, clientId, normalized scope, principal)` checks.
`maximumConcurrentReadAuthorizations` bounds executing policy calls. `maximumPendingReadAuthorizations` independently
bounds all live authorization callers and distinct owner lookups. It also bounds distinct per-wake visibility work
waiting for execution. `readAuthorizationCacheCapacity` bounds completed successes. Active watchers have their own
limit. Equal checks use one lookup within the caller allowance and denials are not cached. Overflow fails with typed
`CapacityExceeded { resource: "read authorizations", limit }`. Each watcher starts refresh halfway through the configured
interval. If no fresh success is available at the current success expiry, the watcher scope closes and its stream fails
with `AuthorizationDenied`. `readAuthorizationRefreshInterval` is therefore the fail closed worst case revocation bound.
Pull and Bootstrap perform uncached one shot read authorization.

Each accepted admission publishes a shared wake after its transaction commits. Watchers do not perform a SQLite
transaction or space row write per publication. `wakeCapacity` is the optional sliding wake queue depth, while
`maximumWatchersPerSpace` is the separate live watcher allowance. Excess streams fail with typed
`CapacityExceeded { resource: "sync watchers", limit }`.

The optional `offlineWake` configuration adds a provider-neutral durable path for clients without a live Watch.
`recipients({ spaceId })` returns authoritative member client IDs. `deliver({ wakeId, spaceId, clientId })` maps one
client to the application's FCM, APNs, web push, or other endpoint and returns `"Delivered"` or `"NotRecipient"`.
The application must decide current membership and send inside one serialized operation so revocation cannot race the
provider call. `"NotRecipient"` retires the current work. The hook carries routing and idempotency IDs, but no mutation
or entity content. Keep provider-visible notification content free of those IDs and all sync data. A retried hook keeps
the same `wakeId`, so the provider send must be idempotent.

Accepted mutations transactionally advance a per-space high water mark. The scoped dispatcher resolves membership
outside admission, coalesces client work, retries failures with capped exponential backoff, and bounds recipient
resolution and delivery separately. SQL Watch leases suppress the push path across every runtime sharing the database.
Every runtime that shares the database and accepts Watch streams must configure the same `offlineWake` adapter so its
presence is visible to dispatchers. A Pull cursor acknowledgement retires work at or below the acknowledged fence. All
durations, batch sizes, concurrency limits, lease intervals, hook timeout, and recipient capacity are explicit in
`OfflineWake.Options`.

`authorizeRead` receives a tagged union. `_tag: "Scope"` authorizes the client and requested model set before space or
schema disclosure. `_tag: "Entity"` authorizes one Schema encoded entity key and value. Only entities that pass both
scope selection and entity policy can enter pull or bootstrap responses. Policy-only revocations are discovered by
the periodic wake interval and delivered as durable `Retract` changes. A true authoritative deletion remains a
`Delete`.

The returned Effect may require application services. Those requirements are part of the resulting server Layer, so
the idiomatic implementation can be a Context service consumed by the option callback:

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

class ReadPolicy extends Context.Service<ReadPolicy, {
  readonly authorize: (
    input: ServerStore.ReadAuthorizationInput
  ) => Effect.Effect<void, typeof Schema.Json.Type>
}>()("app/ReadPolicy") {}

const layerStore = ServerStore.layer({
  definition,
  authorizeAccess,
  authorizeMutation,
  authorizeRead: (input) => ReadPolicy.use((policy) => policy.authorize(input))
}).pipe(Layer.provide(layerReadPolicy))
```

Maintenance publishes an immutable snapshot and logical floors before bounded physical deletion. A space compacts
itself: a write that takes its history or receipts past the midpoint between the retained target and the hard cap
starts one background compaction of that space, so admission only reaches the cap, where it fails before handler
execution, if compaction cannot keep up. `ServerStore.layerMaintenance` adds a sweep over every space as an Effect
Cluster singleton, so it runs on one runner at a time. It sweeps once when it starts and then every `interval` (one
hour by default), and it requires `Sharding`. Old cursors use the authenticated bootstrap path. Snapshot pages are
identity bound, Schema decoded, byte bounded, ordered, and digest chained. Client staging survives interruption and
installs with one atomic canonical replacement.

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Layer from "effect/Layer"

const layerStore = ServerStore.layer({
  definition,
  authorizeAccess,
  authorizeMutation,
  authorizeRead,
  retainedHistoryEntries: 10_000,
  maximumHistoryEntries: 20_000,
  maintenanceConcurrency: 4
})

const layerServer = ServerStore.layerMaintenance({ interval: "30 minutes" }).pipe(
  Layer.provideMerge(layerStore),
  Layer.provide(layerSharding)
)
```

Exact retries return retained receipts. Once receipt evidence has crossed the published terminal fence, an old exact
retry returns `Expired` and never executes again. The client retains an expired pending mutation until it installs the
covering snapshot, unless its durable cursor already proves that canonical state includes the snapshot sequence.
`SyncEngine` is the transport neutral boundary used by direct tests and the RPC client.

## PostgreSQL server storage

`ServerStore`, `Migrations.server`, and `SchemaEvolution.server` run on SQLite and on PostgreSQL 16 or later with the
same results. The dialect comes from the `SqlClient` in context. Every other dialect fails with `InvalidConfiguration`
before any statement runs. Client storage (`LocalStore`, `SqlReplica`, `QueryExecutor`, and `Migrations.client`) is
SQLite only.

```ts
import { PgClient } from "@effect/sql-pg"
import * as Config from "effect/Config"
import * as Layer from "effect/Layer"

const layerStore = ServerStore.layer(options).pipe(
  Layer.provide(PgClient.layerConfig({ url: Config.Redacted("DATABASE_URL") }))
)
```

PostgreSQL has its own migration catalog. Migration 1, `postgres-baseline`, creates the current server schema
directly. Later server migrations are appended to both catalogs. Sequences, counts, byte sizes, generations, and epoch
milliseconds are `BIGINT`, and 0 or 1 flags are `SMALLINT`, so every integer decodes to the same JavaScript number as
on SQLite. JSON columns stay `TEXT`, compared byte for byte. Every `TEXT` column uses `COLLATE "C"`, so ordering,
cursors, and window membership follow UTF-8 byte order exactly like SQLite `BINARY`, whatever the database locale.
PostgreSQL `TEXT` cannot hold U+0000, so text index components are stored with an order preserving escape (U+0001
becomes U+0001 U+0002 and U+0000 becomes U+0001 U+0001). Queries decode it, so entity values containing control
characters behave as on SQLite.

SQLite serializes every write transaction. On PostgreSQL the same guarantees come from explicit locks, so several
runners can share one database:

- Admission, pull, bootstrap, snapshot publication, and pruning lock the space row. A pull or bootstrap therefore
  reads one server head, and admission waits for it as it would behind SQLite's writer lock.
- Snapshot preparation reads under `REPEATABLE READ`. Admission is not blocked, and a snapshot that went stale while
  it was prepared is discarded when publication compares heads.
- Each schema evolution batch locks the space row before validating its progress. A second runner working on the same
  space fails that batch with `SchemaGenerationConflict` and never applies it twice.
- Offline wake claims use `FOR UPDATE SKIP LOCKED` and repeat their claim conditions, so one wake is claimed by one
  runtime. A transaction scoped advisory lock per space and client orders Watch presence registration against delivery
  claims, so a live Watch and a delivery claim for the same client never both commit.
- Migrations and index table creation take a transaction scoped advisory lock, so concurrent first boots create the
  schema once.
- A transaction that PostgreSQL aborts as a deadlock victim or as a serialization failure is retried as a whole, up
  to 8 attempts, before the error surfaces.

Pass the client without `transformResultNames` or `transformQueryNames`, because rows are decoded by their snake case
column names. The test suite runs every server test on both dialects against a PostgreSQL container started through
testcontainers, so running the tests requires Docker.

## Operational metrics

`ServerStore` records `effect_local_server_admission` by completed attempt outcome and
`effect_local_server_rejection` by stable receipt origin or typed error `_tag`. The
`effect_local_server_history_depth` and `effect_local_server_receipt_depth` gauges report the maximum retained rows in
any one space. Their matching `_limit` gauges report the configured per space caps.

`effect_local_server_sync_watcher_count` is the live sync watcher population.
`effect_local_server_wake_fanout_duration` records publication to one subscriber delivery for accepted wakes.
`effect_local_server_maintenance` records completed and failed maintenance runs, while `effect_local_server_pruned`
counts committed deleted rows with `resource=history|receipt`.

`LocalStore` increments `effect_local_client_bootstrap_install` after a snapshot installation commits and maintains
`effect_local_client_pending_mutation_count` as the pending population across active stores. Metric labels use bounded
outcomes and resource classes. They do not contain space, client, mutation, request, or principal identifiers. The
production watcher benchmark is `../local-rpc/bench/Fanout.bench.ts`. `bench/ReplicaScale.bench.ts` compares retained
heap, child fibers, watches, and startup time for eager and lazy clients from 1 through 1,000 remembered spaces.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme) and
[durability notes](https://github.com/lucas-barake/effect-local/blob/main/docs/durability.md).
