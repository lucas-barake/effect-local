# @lucas-barake/effect-local-sql

SQLite persistence and authoritative mutation ordering for Effect Local.

`SqlReplica.layer` provides the public replica with the lightweight in memory scheduler. `SqlReplica.layerWorkflow`
uses the same store, query executor, and idempotent reconciliation pass with finite Effect Workflow generations.
Local SQLite stores canonical entities, visible entities, pending mutations, terminal receipts, accepted entries, the
server cursor, and requested and completed reconciliation generations. Optimistic writes and reconciliation are
transactional. Workflow storage contains execution control only.

The caller chooses the Workflow engine and runner. A durable single runner composition is:

```ts
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Layer from "effect/Layer"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"

const WorkflowEngineLive = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(SingleRunner.layer({ runnerStorage: "sql" }))
)

const ReplicaLive = SqlReplica.layerWorkflow({ definition, spaceId, clientId }).pipe(
  Layer.provide(WorkflowEngineLive)
)
```

`retryDelay`, `maximumRetryDelay`, and `maximumAttempts` bound exponential retries within one Workflow execution. A
terminal failed generation stays failed until a later mutation or server wake requests a new generation. Effect
beta.101 does not expose per Workflow completed history retention through `WorkflowEngine`; storage lifecycle remains
an operational responsibility of the selected engine and runner.

Provide the same application `SqlClient` to the replica and to `SingleRunner`, or prebind separate clients when the
deployment separates application and Workflow storage. Browser applications can build this graph in a long lived
worker over SQLite WASM. Worker construction, database identity, socket credentials, and shutdown remain application
owned. Disposing a worker runtime allows unfinished executions to recover later. Do not call `Workflow.interrupt` for
ordinary shutdown because it durably cancels the reconciliation.

`ServerStore.layer` requires application supplied access, mutation admission, and read callbacks. It reauthorizes
retries, deduplicates stable mutation identities, stores terminal rejections, assigns the next dense sequence to
accepted mutations, and materializes authoritative state in the same SQL transaction. `ServerStore.layerTrusted` is
the explicit allow all composition. `SyncEngine` is the transport neutral boundary used by direct tests and the RPC
client.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme) and
[durability notes](https://github.com/lucas-barake/effect-local/blob/main/docs/durability.md).
