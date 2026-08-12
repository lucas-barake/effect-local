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

const WorkflowEngineLive = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(SingleRunner.layer({ runnerStorage: "sql" }))
)

const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })

const ReplicaLive = SqlReplica.layerWorkflow({
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
  Layer.provide(WorkflowEngineLive)
)
```

`initialSpaces` seeds first startup. Later calls to `Replica.join` persist membership and restart restores every joined
space automatically. `Replica.leave` closes the space runtime before one cascading delete removes its local state.
The database keeps the singleton `clientId`. Rejoining creates a new membership incarnation and local sequence.

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
snapshots, migration retry, maintenance concurrency, and the keyset page size used to enumerate spaces. `ServerStore.layerTrusted` is the explicit allow all
composition.

Provide `ServerStore.layerMaintenance({ interval, runOnStart })` beside the store, or schedule `maintainAll` through an
application owned job runner. Maintenance publishes an immutable snapshot and logical floors before bounded physical
deletion. Admission fails before handler execution at a hard cap until maintenance creates capacity. Old cursors use
the authenticated bootstrap path. Snapshot pages are identity bound, Schema decoded, byte bounded, ordered, and digest
chained. Client staging survives interruption and installs with one atomic canonical replacement.

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"

const StoreLive = ServerStore.layer({
  definition,
  readAuthorizationRefreshInterval: "30 seconds",
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
  migration: { retryDelay: "25 millis", maximumAttempts: 8 }
})

const ServerLive = ServerStore.layerMaintenance({
  interval: "30 seconds",
  runOnStart: true
}).pipe(Layer.provideMerge(StoreLive))
```

Exact retries return retained receipts. Once receipt evidence has crossed the published terminal fence, an old exact
retry returns `Expired` and never executes again. The client retains an expired pending mutation until it installs the
covering snapshot, unless its durable cursor already proves that canonical state includes the snapshot sequence.
`SyncEngine` is the transport neutral boundary used by direct tests and the RPC client.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme) and
[durability notes](https://github.com/lucas-barake/effect-local/blob/main/docs/durability.md).
