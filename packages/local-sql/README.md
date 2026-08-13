# @lucas-barake/effect-local-sql

SQLite persistence and authoritative mutation ordering for Effect Local.

`SqlReplica.layer` provides one public replica for many spaces in one SQLite database. Membership is durable and every
client table is partitioned by `space_id`. Each `Replica.Space` handle owns its operations and status. The root service
joins, leaves, lists, and addresses handles, and reports aggregate status. One dispatcher schedules keyed watches and
turns over the shared `SyncEngine`, so the RPC composition uses one WebSocket while spaces make progress independently.
`reconciliationConcurrency` bounds how many finite turns the lightweight `SqlReplica.layer` dispatcher can run at once.

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

const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })

const layerReplica = SqlReplica.layerWorkflow({
  definition,
  clientId,
  scope,
  initialSpaces: [spaceId],
  retainedReceipts: 256,
  maximumReceipts: 1_024,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 100_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "25 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(layerWorkflowEngine)
)
```

`initialSpaces` seeds first startup. Later calls to `Replica.join` persist membership and restart restores every joined
space automatically. `Replica.leave` closes the space runtime before one cascading delete removes its local state.
The database keeps the singleton `clientId`. Rejoining creates a new membership incarnation and local sequence.

The required `scope` is a model subscription shared by the replica's joined spaces. Changing the configured scope on
the next startup advances a durable generation. A wider scope backfills through incremental pull. A narrower scope
receives `Retract` changes and evicts the excluded canonical and visible entities without a new bootstrap. The current
scope contract does not express key ranges, rolling windows, lazy fetches, or automatic cache eviction.

`retryDelay`, `maximumRetryDelay`, and `maximumAttempts` bound exponential retries within one Workflow execution. A
terminal failed generation stays failed until a later mutation or server wake requests a new generation. Effect
beta.103 does not expose per Workflow completed history retention through `WorkflowEngine`; storage lifecycle remains
an operational responsibility of the selected engine and runner.

Provide separate `SqlClient` connections to the replica and to SQL backed `SingleRunner`. They may use the same file,
but a Workflow runner can retain its transaction while the handler uses the application database. Browser applications can build this graph in a long lived
worker over SQLite WASM. Worker construction, database identity, socket credentials, and shutdown remain application
owned. Disposing a worker runtime allows unfinished executions to recover later. Do not call `Workflow.interrupt` for
ordinary shutdown because it durably cancels the reconciliation.

`ServerStore.layer` requires application supplied access, mutation admission, and read callbacks. It reauthorizes
retries, deduplicates stable mutation identities, stores terminal rejections, assigns the next dense sequence to
accepted mutations, and materializes authoritative state in the same SQL transaction. Its required history options
set retained targets, hard admission caps, snapshot capacity, bootstrap page capacity, prune batches, retained
snapshots, migration retry, maintenance concurrency, and the keyset page size used to enumerate spaces. The required
`maximumWatchersPerSpace`, `readAuthorizationRefreshInterval`, `maximumConcurrentReadAuthorizations`,
`maximumPendingReadAuthorizations`, and `readAuthorizationCacheCapacity` options bound live sync streams and their
policy work. `ServerStore.layerTrusted` is the explicit allow all composition.

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
`recipients({ spaceId })` returns authoritative member client IDs. `isRecipient({ spaceId, clientId })` rechecks
membership immediately before every delivery attempt so a revoked client does not receive a later retry.
`deliver({ wakeId, spaceId, clientId })` maps one client to the application's FCM, APNs, web push, or other endpoint.
It receives no mutation or entity content. Keep the provider payload content-free and use the stable `wakeId` to make
at-least-once calls idempotent.

Accepted mutations transactionally advance a per-space high water mark. The scoped dispatcher resolves membership
outside admission, coalesces client work, retries failures with capped exponential backoff, and bounds recipient
resolution and delivery separately. SQL Watch leases suppress the push path across every runtime sharing the database.
Pull acknowledgement retires work at or below the acknowledged fence. All durations, batch sizes, concurrency limits,
lease intervals, hook timeout, and recipient capacity are explicit in `OfflineWake.Options`.

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
  ...serverHistory,
  definition,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  authorizeAccess,
  authorizeMutation,
  authorizeRead: (input) => ReadPolicy.use((policy) => policy.authorize(input))
}).pipe(Layer.provide(layerReadPolicy))
```

Provide `ServerStore.layerMaintenance({ interval, runOnStart })` beside the store, or schedule `maintainAll` through an
application owned job runner. Maintenance publishes an immutable snapshot and logical floors before bounded physical
deletion. Admission fails before handler execution at a hard cap until maintenance creates capacity. Old cursors use
the authenticated bootstrap path. Snapshot pages are identity bound, Schema decoded, byte bounded, ordered, and digest
chained. Client staging survives interruption and installs with one atomic canonical replacement.

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"

const layerStore = ServerStore.layer({
  definition,
  authorizeAccess,
  authorizeMutation,
  authorizeRead,
  retainedHistoryEntries: 10_000,
  maximumHistoryEntries: 20_000,
  retainedReceipts: 10_000,
  maximumReceipts: 20_000,
  maximumSnapshotEntities: 100_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 4,
  maintenanceSpaceBatchSize: 128,
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration: { retryDelay: "25 millis", maximumAttempts: 8 }
})

const layerServer = ServerStore.layerMaintenance({
  interval: "30 seconds",
  runOnStart: true
}).pipe(Layer.provideMerge(layerStore))
```

Exact retries return retained receipts. Once receipt evidence has crossed the published terminal fence, an old exact
retry returns `Expired` and never executes again. The client retains an expired pending mutation until it installs the
covering snapshot, unless its durable cursor already proves that canonical state includes the snapshot sequence.
`SyncEngine` is the transport neutral boundary used by direct tests and the RPC client.

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
production watcher benchmark is `../local-rpc/bench/Fanout.bench.ts`.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme) and
[durability notes](https://github.com/lucas-barake/effect-local/blob/main/docs/durability.md).
