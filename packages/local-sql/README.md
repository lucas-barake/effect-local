# @lucas-barake/effect-local-sql

SQLite persistence and authoritative mutation ordering for Effect Local.

`SqlReplica.layer` provides one public replica for many spaces in one SQLite database. Membership is durable and every
client table is partitioned by `space_id`. Each `Replica.Space` handle owns its operations, replication scope,
activation, and status. Membership is lightweight. Addressed operations activate foreground LRU residents, while
inactive spaces with pending mutations use short lived background runtimes. `maximumActiveSpaces` bounds all per space
runtimes and `foregroundActiveSpaces` reserves capacity for addressed work. `foregroundReconciliationConcurrency`
reserves foreground turns within the total `reconciliationConcurrency`. Active logical watches share the one
`SyncEngine` and RPC WebSocket.

Every local transaction and statement goes through one `ConnectionLane` per database, so the lane decides who uses the
single SQLite connection next. Work runs at the `ConnectionLane.Priority` of the calling fiber, which is `Foreground`
by default: app reads, queries, and commits need no extra setup. Reconciliation, settlement, bootstrap, and background
activation run as `Background`. Waiting foreground work is served before waiting background work, in arrival order
within each priority, and a background waiter joins the foreground order once it has waited `maximumBackgroundWait`
(50 milliseconds by default), so sync cannot starve behind a busy UI. The per space projection gate follows the same
rule, and a background holder of that gate is served as foreground while a foreground commit waits on it. A
background gate holder keeps one lane turn across the transactions of its gated step until other work has waited
`maximumBackgroundWait`, so a sync step does not queue again for each of its transactions. Receipt batches commit every
`receiptPersistBatchSize` receipts (8 by default) while foreground work is waiting. Code that uses `LocalStore.layer`
or `QueryExecutor.layer` directly provides one `ConnectionLane.makeLayer()` per database.

`Replica.Space.status` reports a `SpaceStatus`. A remembered space that is not active is `Idle`, with its pending
count: before its first activation, after deactivation or eviction, and between background syncs. `Idle` says nothing
about the transport. The exception is an inactive space whose last background sync ended in a failure that does not
retry. It reports `Failed`, or `NeedsAuthentication` for a rejected credential, until a reconciliation of that space
succeeds. An activated space is `Connecting` until its first sync attempt resolves. It becomes `Online` once a sync
completes, or `SchemaUpdateAvailable` when the server reports a different schema identity. It becomes `Offline` only after a sync or
its watch failed because the server could not be reached: `ServerUnavailable`, `OperationTimeout`, or
`AuthenticatorUnavailable`. A watch failure of that kind while a sync is still running does not change the status.
`CredentialRejected` reports `NeedsAuthentication` and every other failure reports `Failed` with the failure tag as
its `message`. Later syncs keep the last outcome until they resolve. Every status carries `synced`, which is `true` once the space
has an installed replication view, meaning a bootstrap completed at least once, and `false` before that. It is read
from durable storage, so a synced space stays `synced` after an offline reload. It returns to `false` only when the
view is cleared: after leaving and rejoining the space, after the server revokes read access, or after a schema
migration that requires a fresh bootstrap. A scope change keeps the installed view until the next bootstrap replaces
it. An app can show an empty state when `synced` is `true` and a loading state while it is `false`.

`Replica.status` summarizes every remembered space. `counts` holds one count per category, idle included, and
`totalPending` sums every space. `SchemaUpdateAvailable` counts as online. `state` is computed from the spaces that
are not `Idle`: `Idle` when there is none, `Failed` when any of them is, otherwise `NeedsAuthentication` when any of
them is, `Online` or `Offline` when every one of them is, `Connecting` when any of them is still connecting, and
`Degraded` otherwise.

Reconciliation classifies every `ReplicaError` once, in `Reconciler.ts`. `ServerUnavailable`, `OperationTimeout`, and
`AuthenticatorUnavailable` retry and report `Offline`. `StorageUnavailable`, `UnknownCommitOutcome`,
`OwnerUnavailable`, and the `CapacityExceeded` resources that load can clear retry and report `Failed`.
`CredentialRejected` reports `NeedsAuthentication` and waits for a new credential generation. Everything else stops and
reports `Failed`. Retries start at `retryDelay` (1 second) and double up to `maximumRetryDelay` (1 minute). The
background scheduler that drains inactive spaces follows the same classes: it retries the first two with the same
delays, stops on the rest, and remembers that failure in the space status until a reconciliation succeeds. A watch
that closes is reopened after the same delays, which reset once a watch has stayed open longer than the previous
delay. See [synchronization](https://github.com/lucas-barake/effect-local/blob/main/docs/sync.md#websocket-rpc) for
the per tag and per resource table.

Storage failures are reported by tag. A SQL or platform error is `StorageUnavailable`. A row that cannot be decoded, or
a required row that is missing, is `StorageCorrupt`. A store or query operation that finds no membership row for its
space fails with `SpaceUnavailable`.

`SqlReplica.layerWorkflow` uses the same store, query executor, and idempotent reconciliation pass with finite Effect
Workflow generations. Local SQLite stores canonical entities, visible entities, pending mutations, bounded terminal
receipts, a bounded accepted suffix, per space cursors, resumable snapshot staging, and requested and completed
reconciliation generations. Optimistic writes, incremental reconciliation, and snapshot installation are
transactional. Workflow storage contains execution control only.

Client SQLite stores no secondary index tables. Declared model indexes are materialized only in server storage, where
they back replication windows. Query handlers read the visible entities through `query.get` and `query.sql`, and express
bounds, ordering, limits, and keyset continuation as plain SQL over the generated model CTEs, decoding the raw rows with
`SqlSchema` at the call site. A `query.sql` read is invalidated whenever any entity of a model it declared changes,
whatever the row range, and a `query.get` read is invalidated only when its exact entity changes.

The caller chooses the Workflow engine and runner. A durable single runner composition is:

```ts
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ClusterWorkflowEngine from "effect/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/cluster/SingleRunner"
import * as Layer from "effect/Layer"
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

Only `definition` is required. `SqlReplica.defaults` lists the active space, receipt, history, bootstrap, and migration
defaults: 16 active spaces with 4 reserved for foreground work, 256 retained receipts under a cap of 10000, 256
retained history entries, bootstrap bounds of 100000 entities, 64 MiB, and 4 MiB pages, and migration retries of 8
attempts 100 milliseconds apart. The other options default inline: `maximumPendingMutations` 10000,
`retainedMutationIds` 100000, `pageSize` 256, `reconciliationConcurrency` 8 with `foregroundReconciliationConcurrency`
1, `retryDelay` 1 second, and `maximumRetryDelay` 1 minute. `defaultScope` defaults to every model in the definition. A caller-minted
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
4.0.0 does not expose per Workflow completed history retention through `WorkflowEngine`; storage lifecycle remains
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
migration retry and mode, maintenance concurrency, and the keyset page size used to enumerate spaces. `maximumWatchersPerSpace`,
`maximumWatchersPerPrincipal`, `readAuthorizationRefreshInterval`, `maximumConcurrentReadAuthorizations`, `maximumPendingReadAuthorizations`, and
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

`maximumWatchersPerPrincipal` (default 64) caps the live watchers one authenticated principal holds in one space, so a
single member cannot take every space slot and lock other members out of live sync. The quota is keyed by the canonical
principal, not the client ID, because clients choose their own client IDs and could rotate them to evade a per client
cap. Every device and tab of a user shares the quota, and a principal that every caller shares, such as `null` for
anonymous access, shares one quota across all of them. Admission checks the space and principal allowances in one step
and releases both when the stream ends, fails, or is interrupted. Excess streams fail with typed
`CapacityExceeded { resource: "sync watchers per principal", limit }`. Watches opened through the trusted `watch`
method carry no principal and count only toward the space allowance.

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
import * as Layer from "effect/Layer"

class ReadPolicy extends Context.Service<ReadPolicy, {
  readonly authorize: ServerStore.Options["authorizeRead"]
}>()("app/ReadPolicy") {}

const layerStore = ServerStore.layer({
  definition,
  authorizeAccess,
  authorizeMutation,
  authorizeRead: (input) => ReadPolicy.use((policy) => policy.authorize(input))
}).pipe(Layer.provide(layerReadPolicy))
```

Maintenance publishes an immutable snapshot and logical floors before bounded physical deletion. A space compacts
itself: a write that brings its history or receipts to the midpoint between the retained target and the hard cap
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
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Config from "effect/Config"
import * as Layer from "effect/Layer"

const layerStore = ServerStore.layer(options).pipe(
  Layer.provide(PgClient.layerConfig({ url: Config.Redacted("DATABASE_URL") }))
)
```

PostgreSQL has its own migration catalog. Migration 1, `postgres-baseline`, creates the server schema from the same
table definitions as the SQLite catalog's `server-baseline`. Later server migrations are appended to both catalogs. Sequences, counts, byte sizes, generations, and epoch
milliseconds are `BIGINT`, and 0 or 1 flags are `SMALLINT`, so every integer decodes to the same JavaScript number as
on SQLite. JSON columns stay `TEXT`, compared byte for byte. Every `TEXT` column uses `COLLATE "C"`, so ordering,
cursors, and window membership follow UTF-8 byte order exactly like SQLite `BINARY`, whatever the database locale.
PostgreSQL `TEXT` cannot hold U+0000, so text index components are stored with an order preserving escape (U+0001
becomes U+0001 U+0002 and U+0000 becomes U+0001 U+0001). Queries decode it, so entity values containing control
characters behave as on SQLite. On both dialects an unpaired surrogate, and U+D7FF itself, is stored as U+D7FF
followed by an offset code unit, so text index columns keep such strings losslessly.

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

## Migrations applied by a DBA

By default `ServerStore` migrates its own schema when the layer builds. Where the server's database role may not run
DDL and schema changes go through a DBA, render the pending schema as a script and start the server in verify mode:

```ts
import * as Migrations from "@lucas-barake/effect-local-sql/Migrations"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Option from "effect/Option"

export const writeMigrationScript = Effect.gen(function*() {
  const script = yield* Migrations.renderServer({ definition })
  if (Option.isNone(script)) return
  yield* FileSystem.FileSystem.use((fs) => fs.writeFileString("effect-local-server.sql", script.value))
})

const layerStore = ServerStore.layer({ ...options, migration: { mode: "verify" } })
```

`Migrations.renderServer` reads the database in context and never writes to it or locks it, so it can run against
production with a read only connection. It returns `Option.none()` when the database is already current. Otherwise
the script holds exactly what automatic migration would do, in one transaction:

- On PostgreSQL it takes the same advisory lock automatic migration takes.
- It inserts the migration ledger rows, with the checksums of the catalog, before the statements they record.
- It copies the pending migration statements verbatim.
- When it changes index tables it first inserts the next row of `effect_local_server_index_generations`, numbered
  from the database it was rendered against. Automatic migration inserts a row whenever it changes index tables too.
  A script applied a second time, or after anything else changed the index tables, fails on that key, so a stale
  script cannot undo a later deploy.
- It creates the index tables the definition declares and records them in the index catalog.
- It drops index tables the definition no longer declares, with their build state. Indexes are not part of the schema
  identity, so an index that is removed and later added back must start from an empty table.

Apply it with `psql -v ON_ERROR_STOP=1 -f effect-local-server.sql` on PostgreSQL, or with `sqlite3 -bail` on SQLite,
so the first failing statement stops the script. Render again after every deploy that adds migrations or changes the
definition's indexes.

`migration: { mode: "verify" }` (in `SyncServer.layer` this is `store: { migration: { mode: "verify" } }`) reads the
ledger and the index catalog instead of migrating, without writing or locking. The layer then fails, so the server
does not serve:

- `StorageMigrationPending` with `catalog: "Server"` when migrations are pending, or `catalog: "ServerIndex"` when
  index tables must be created or dropped.
- `StorageMigrationMismatch` when the ledger diverges from the catalog, for example an edited checksum or a migration
  the code does not know.
- `StorageCorrupt` when the index catalog contradicts its descriptors.

Both cover the tables this package owns. Effect Cluster's SQL runner and message storage create their own tables with
Effect's migrator when they start.

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
