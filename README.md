# Effect Local

Effect Local is a frontend only local first engine for Effect v4. Application reads and writes complete against a
durable browser replica. Automerge owns causal history and convergence. SQLite WASM in OPFS owns durable bytes and
rebuildable query projections. Effect Cluster serializes document commands and stores their replies. Effect Workflow
resumes long running maintenance. Effect Atom exposes reactive views without becoming another source of truth.

The design follows the ownership, longevity, and offline principles in
[Local first software](https://www.inkandswitch.com/essay/local-first/). This repository contains client code only. It
does not provide a backend, relay, authentication, encryption, or a prescribed server protocol.

> **Beta:** The current release is `0.1.0` and targets Effect `4.0.0-beta.99` and Automerge `3.3.2`. Durable formats,
> worker protocols, and public APIs can still change. Read [Limits and security](#limits-and-security) before adopting
> it for user data.

## Why Effect Local

- The local replica is authoritative for interactive reads and writes.
- Every public boundary uses Effect Schema codecs.
- Mutation handlers, query handlers, SQL projections, workers, transports, and limits are Effect services or Layers.
- Command IDs make retries explicit and distinguish a durable commit from an unknown outcome.
- Canonical Automerge state is separate from disposable SQLite projections and Atom caches.
- Cluster and Workflow provide durable execution without replacing Automerge merge semantics.
- Production shaped in memory layers and deterministic peer faults keep tests fast and reproducible.

## Installation

Install only the packages used by the application surface. All four packages are ESM.

```sh
pnpm add effect@4.0.0-beta.99 @automerge/automerge@3.3.2
pnpm add @lucas-barake/effect-local @lucas-barake/effect-local-sql
pnpm add @lucas-barake/effect-local-browser @effect/platform-browser@4.0.0-beta.99
pnpm add @effect/sql-sqlite-wasm@4.0.0-beta.99 @effect/wa-sqlite@0.2.1
pnpm add @effect/atom-react@4.0.0-beta.99
pnpm add -D @lucas-barake/effect-local-test @effect/sql-sqlite-node@4.0.0-beta.99
```

Package roles:

| Package                              | Purpose                                                                                      |
| ------------------------------------ | -------------------------------------------------------------------------------------------- |
| `@lucas-barake/effect-local`         | Documents, mutations, projections, queries, backups, sync transport, and `Replica`           |
| `@lucas-barake/effect-local-sql`     | SQLite persistence, durable Cluster execution, Workflow, recovery, compaction, and peer sync |
| `@lucas-barake/effect-local-browser` | Effect Worker and RPC composition, OPFS ports, sessions, presence, and Atom builders         |
| `@lucas-barake/effect-local-test`    | In memory production shaped replicas and deterministic bounded peer faults                   |

## Prerequisites and browser support

The durable browser composition requires:

- An ESM build tool that supports module workers and WebAssembly.
- `SharedWorker`, dedicated `Worker`, transferable `MessagePort`, Web Locks, OPFS, and WebAssembly.
- A secure context for production deployment. Localhost remains available for development.
- A Content Security Policy and asset pipeline that allow the selected SQLite WebAssembly files and module workers.
- A page that can provision the dedicated OPFS worker. Chromium does not expose `Worker` inside
  `SharedWorkerGlobalScope`.

The browser suites currently exercise Chromium. Other engines are not claimed as supported until the same OPFS,
worker, locking, reload, and browser test suite passes there. Browser storage starts as best effort. Request
`navigator.storage.persist()`, report the result, and provide backup and restore controls.

## Cookbook

The following sections build one task replica from schema to React. Imports use public subpath exports so every
dependency stays visible.

### 1. Model documents

A document schema must encode to Automerge compatible values. Names and versions are durable protocol identities.

```ts
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Schema from "effect/Schema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

export const Task = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 1
})

export const Documents = DocumentSet.make(Task)
```

The supported encoded shape consists of Automerge scalar values, arrays, and plain records. `Document.encode` and
`Document.decode` expose the checked conversion when an application needs it directly.

### 2. Define mutations and inject handlers

Mutation definitions contain schemas. Implementations are separate Layer services. A mutation handler is synchronous
because it runs inside an Automerge change and a durable SQL command transaction.

```ts
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { Task } from "./domain.js"

export const RenameTask = Mutation.make("RenameTask", {
  document: Task,
  payload: Schema.Struct({ title: Schema.String }),
  error: Schema.Literal("TitleEmpty")
})

export const SetTaskCompleted = Mutation.make("SetTaskCompleted", {
  document: Task,
  payload: Schema.Struct({ completed: Schema.Boolean })
})

export const MutationLive = Layer.mergeAll(
  Mutation.layer(RenameTask, ({ draft, payload }) => {
    const title = payload.title.trim()
    if (title.length === 0) return Result.fail("TitleEmpty" as const)
    draft.title = title
    draft.updatedAt = Date.now()
    return Result.succeed(undefined)
  }),
  Mutation.layer(SetTaskCompleted, ({ draft, payload }) => {
    draft.completed = payload.completed
    draft.updatedAt = Date.now()
    return undefined
  })
)
```

The handler receives the mutable encoded `draft`, the decoded `payload`, and the decoded `current` value. A mutation
with a declared error schema returns `Result`. A mutation whose error is `Schema.Never` returns its success directly.

### 3. Define projections, queries, and the replica

Projections are deterministic functions of a canonical snapshot. Queries declare which projections invalidate their
reactive results.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Schema from "effect/Schema"
import { Documents, RenameTask, SetTaskCompleted, Task } from "./domain.js"

export const TaskRow = Schema.Struct({
  sourceDocumentId: Identity.DocumentId,
  title: Schema.String,
  completed: Schema.Boolean,
  updatedAt: Schema.Number
})

export const TaskList = Projection.make("TaskList", {
  document: Task,
  version: 1,
  Row: TaskRow,
  key: (row) => row.sourceDocumentId,
  project: (snapshot) => [{
    sourceDocumentId: snapshot.documentId,
    title: snapshot.value.title,
    completed: snapshot.value.completed,
    updatedAt: snapshot.value.updatedAt
  }]
})

export const ListTasks = Query.make("ListTasks", {
  payload: Schema.Struct({ search: Schema.String }),
  success: Schema.Array(TaskRow),
  dependsOn: [TaskList]
})

export const definition = ReplicaDefinition.make({
  name: "tasks",
  documents: Documents,
  mutations: [RenameTask, SetTaskCompleted],
  projections: [TaskList],
  queries: [ListTasks]
})
```

`definition.hash` covers names, versions, schemas, and query dependencies. The current beta requires an exact hash
match when opening or restoring a replica.

### 4. Use `Replica` and handle command outcomes

`Replica` is an Effect service. A command is not equivalent to a plain success value.

```ts
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import { RenameTask, Task } from "./domain.js"

const create = Replica.Replica.use((replica) => {
  const now = Date.now()
  return replica.create(Task, {
    commandId: Identity.makeCommandId(),
    value: { title: "Read the paper", completed: false, createdAt: now, updatedAt: now }
  })
}).pipe(Effect.flatMap(CommandOutcome.committedOrFail))

const rename = (documentId: Identity.DocumentId) =>
  Replica.Replica.use((replica) =>
    replica.mutate(RenameTask, {
      commandId: Identity.makeCommandId(),
      documentId,
      payload: { title: "Build the engine" }
    })
  ).pipe(
    Effect.map((outcome) =>
      CommandOutcome.match(outcome, {
        onCommitted: ({ value }) => ({ _tag: "Committed" as const, value }),
        onRejected: ({ error }) => ({ _tag: "Rejected" as const, error }),
        onUnknown: ({ commandId }) => ({ _tag: "RetryLookup" as const, commandId })
      })
    )
  )
```

Outcome meanings:

| Outcome                 | Meaning                                                                    | Application action                                      |
| ----------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------- |
| `DurablyCommittedLocal` | Canonical state, projections, receipt, and Cluster reply committed locally | Use the returned value                                  |
| `Rejected`              | The mutation handler rejected the command with its declared domain error   | Show or handle the domain error                         |
| `OutcomeUnknown`        | The caller cannot prove whether the command committed                      | Keep the command ID and call the matching lookup method |

Create document IDs are derived from command IDs. Repeating an identical command is idempotent. Reusing a command ID
for different input fails with `CommandIdConflict`.

The full service surface is:

| Method                                                 | Result                                                                  |
| ------------------------------------------------------ | ----------------------------------------------------------------------- |
| `create(document, { commandId, value })`               | `CommandOutcome<DocumentId>`                                            |
| `get(document, documentId)`                            | Decoded `Snapshot` with heads, version, tombstone, and projection state |
| `mutate(mutation, { commandId, documentId, payload })` | The mutation's typed `CommandOutcome`                                   |
| `delete(document, { commandId, documentId })`          | `CommandOutcome<void>` and a durable tombstone                          |
| `query(query, payload)`                                | The query's decoded success or declared error                           |
| `lookupCreate(document, commandId)`                    | Durable or unknown create outcome                                       |
| `lookupMutation(mutation, commandId)`                  | Durable, rejected, or unknown mutation outcome                          |
| `lookupDelete(document, commandId)`                    | Durable or unknown delete outcome                                       |
| `flush`                                                | Publishes pending local commit invalidations                            |
| `status`                                               | Stream of `ReplicaStatus` values                                        |
| `exportBackup({ maxBytes })`                           | Stream of bounded canonical archive chunks                              |
| `restoreBackup(options)`                               | Clone or replace restoration from a bounded stream                      |
| `exportDocument(document, documentId)`                 | Schema encoded portable document value                                  |
| `importDocument(document, { commandId, value })`       | A new local document and fresh causal history                           |

### 5. Bind projections to SQLite

The projection table is a disposable index. Canonical Automerge history remains the source of truth.

```ts
import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Effect from "effect/Effect"
import { TaskList } from "./domain.js"

export const TaskListSql = SqlProjection.make(TaskList, {
  table: "task_list_v1",
  migrations: [{
    id: 1,
    name: "task_list_v1",
    checksum: "sha256:task-list-v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        completed INTEGER NOT NULL,
        updated_at REAL NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (
      source_document_id, title, completed, updated_at
    ) VALUES (
      ${row.sourceDocumentId}, ${row.title}, ${row.completed ? 1 : 0}, ${row.updatedAt}
    )`.pipe(Effect.asVoid)
})
```

Query handlers are also injected Layers and may depend on `SqlClient.SqlClient`.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { ListTasks } from "./domain.js"

export const QueryLive = Query.layer(ListTasks, ({ search }) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const pattern = `%${search.trim().toLocaleLowerCase()}%`
    const rows = yield* sql<{
      readonly sourceDocumentId: string
      readonly title: string
      readonly completed: number
      readonly updatedAt: number
    }>`SELECT source_document_id AS sourceDocumentId, title, completed, updated_at AS updatedAt
       FROM task_list_v1
       WHERE ${pattern} = '%%' OR LOWER(title) LIKE ${pattern}
       ORDER BY updated_at DESC`
    return rows.map((row) => ({
      ...row,
      sourceDocumentId: Identity.DocumentId.make(row.sourceDocumentId),
      completed: row.completed === 1
    }))
  }).pipe(Effect.orDie))
```

### 6. Compose the durable engine

`SqlReplica.layer` requires exactly one SQL binding per declared projection plus every mutation and query handler,
`ReplicaLimits`, and `SqlClient`. It provides `Replica`, `CommitPublisher`, `ReplicaWorkflow.WorkflowRuntime`,
`PeerSync`, `ReplicaGate`, and Effect Cluster `Sharding`. The owner side peer services stay available so
`PeerSession.make` can share the same durable runtime and fencing gate.

```ts
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import { definition, MutationLive, QueryLive, TaskListSql } from "./domain.js"

const DomainLive = Layer.mergeAll(MutationLive, QueryLive, TaskListSql.layer)

export const EngineLive = SqlReplica.layer(definition, {
  projections: [TaskListSql]
}).pipe(
  Layer.provideMerge(DomainLive),
  Layer.provideMerge(ReplicaLimits.layer(limits)),
  Layer.provideMerge(DatabaseLive)
)
```

`DatabaseLive` is any Effect v4 `SqlClient.SqlClient` Layer. In the browser it comes from `BrowserSqlite.layer` or
`BrowserSqlite.layerPort`. The complete limits object is available as `TestReplica.defaultLimits` for tests and in
[`examples/tasks/src/domain.ts`](examples/tasks/src/domain.ts) for a browser configuration.

### 7. Provide the official Effect Worker layer

`BrowserReplica.layer(definition)` intentionally requires Effect's `WorkerPlatform` and `Spawner`. It does not create
or hide a `SharedWorker`.

```ts
import { BrowserWorker } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as Layer from "effect/Layer"

const WorkerLive = BrowserWorker.layer(() => {
  const worker = new SharedWorker(new URL("./replica.shared-worker.ts", import.meta.url), {
    name: "effect-local-tasks",
    type: "module"
  })
  const channel = new MessageChannel()
  worker.port.postMessage({ _tag: "Attach", rpcPort: channel.port1 }, [channel.port1])
  worker.port.start()
  return channel.port2
})

export const BrowserLive = BrowserReplica.layer(definition).pipe(
  Layer.provide(WorkerLive)
)
```

The `Attach` message is application ownership protocol, not hidden library behavior. The production example also
handles liveness, expiring provisioning nonces, OPFS worker creation, database port transfer, and provider loss. See
[`examples/tasks/src/replica-client.ts`](examples/tasks/src/replica-client.ts) and
[`examples/tasks/src/replica.shared-worker.ts`](examples/tasks/src/replica.shared-worker.ts).

Inside the SharedWorker, compose `BrowserSqlite.layerPort(databasePort)`, domain Layers, `SqlReplica.layer`, and
`SessionManager.layer`. Serve each attached RPC port with `ReplicaOwner.layerWorker(definition)` and the official
`BrowserWorkerRunner.layerMessagePort(rpcPort)` Layer.

### 8. Build reactive state with Effect Atom

The runtime helpers use native Effect v4 reactivity APIs. Atom state is a cache over `Replica`, never the durable
authority.

```ts
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import { Atom } from "effect/unstable/reactivity"
import { BrowserLive } from "./browser.js"
import { ListTasks, RenameTask, Task } from "./domain.js"

export const runtime = Atom.runtime(BrowserLive)
export const tasks = ReplicaAtom.queryFamily(runtime, ListTasks)
export const task = ReplicaAtom.documentFamily(runtime, Task)
export const renameTask = ReplicaAtom.mutation(runtime, RenameTask)
export const replicaStatus = ReplicaAtom.status(runtime)

const taskId = Identity.makeDocumentId()
const allTasks = tasks({ search: "" })
const oneTask = task(taskId)
```

`queryFamily` canonicalizes its schema encoded payload for stable family identity. It invalidates on every declared
projection and source document key. `mutation` invalidates the mutation's document key after execution. For commands
with custom invalidation needs, use `runtime.fn` and its `reactivityKeys` option as the Tasks example does.

React applications provide an `AtomRegistry` through `@effect/atom-react` and consume the returned atoms with
`useAtomValue`, `useAtomSet`, and `useAtomRefresh`. The lower level `ReplicaAtom` service exposes a registry plus
`get`, `query`, `status`, `refresh`, and `mount` for non React integration.

### 9. Observe status and lifecycle

`Replica.status` is a stream. `ReplicaAtom.status(runtime)` wraps it as an Atom.

| Status              | Meaning                                                              |
| ------------------- | -------------------------------------------------------------------- |
| `Starting`          | Startup is in the named phase                                        |
| `Ready`             | The local replica accepts work and reports pending commands          |
| `ReadOnly`          | Reads remain available but writes are disabled for the stated reason |
| `Degraded`          | The replica is usable with a reported degradation                    |
| `ProjectionBlocked` | A projection cannot represent accepted canonical state               |
| `Restoring`         | A restore is active and reports progress                             |
| `Failed`            | Startup or runtime ownership failed                                  |

Runtime ownership is scoped. Dispose the Atom runtime and close application worker ports when the page integration is
finished. Call `replica.flush` before an intentional shutdown when the application wants pending invalidations
published. It is not a substitute for browser lifecycle guarantees.

### 10. Export backups and portable documents

Canonical backups are bounded NDJSON archives. Export is a stream so applications can preserve backpressure.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { definition, Task } from "./domain.js"

const bytes = Replica.Replica.use((replica) =>
  Stream.mkUint8Array(replica.exportBackup({ maxBytes: 32 * 1024 * 1024 }))
)

const restore = (archive: Uint8Array) =>
  Replica.Replica.use((replica) =>
    replica.restoreBackup({
      source: Stream.make(archive),
      mode: "replace",
      maxBytes: 32 * 1024 * 1024,
      expectedDefinitionHash: definition.hash
    })
  )

const duplicateDocument = (documentId: Identity.DocumentId) =>
  Replica.Replica.use((replica) =>
    replica.exportDocument(Task, documentId).pipe(
      Effect.flatMap((value) =>
        replica.importDocument(Task, {
          commandId: Identity.makeCommandId(),
          value
        })
      )
    )
  )
```

Replace restore stages and validates the archive before changing the active incarnation. Clone restore creates a new
local identity. Portable document import validates the document name and schema version, then creates fresh causal
history.

### 11. Add peer sync and presence

Applications provide `PeerTransport`. The transport owns peer identity, authentication, authorization, encryption,
discovery, and routing.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"

declare const openAuthenticatedConnection: (
  peerId: Identity.PeerId
) => Effect.Effect<PeerTransport.Connection, ReplicaError.ReplicaError, Scope.Scope>

export const TransportLive = Layer.succeed(PeerTransport.PeerTransport, {
  capabilities: { storeAndForward: false },
  connect: ({ peerId }) => openAuthenticatedConnection(peerId)
})
```

The application adapter must return the public `PeerTransport.Connection` shape with `peerId`, `capabilities`,
`receive`, `send`, and `close`. Its scope must release every transport resource. `PeerSession.make` selects whole
documents and connects the transport to durable `PeerSync` state.

`PeerSync` is the lower level durable protocol. `open` creates the local sync and outbox session for a peer. `receive`
takes that session plus the remote envelope epoch, sequence, and bytes. Its durable reply is session neutral. `enqueue`
idempotently binds the reply to the current local outbox before sending it. This separation lets Cluster replay a stored
reply after a restart without reusing a sequence from an expired connection. Most applications should use `PeerSession`,
which binds both epochs and enqueues replies automatically.

```ts
import * as PeerSession from "@lucas-barake/effect-local-browser/PeerSession"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import { Task } from "./domain.js"

const syncTask = (peerId: Identity.PeerId, documentId: Identity.DocumentId) =>
  Effect.scoped(Effect.gen(function*() {
    const session = yield* PeerSession.make({
      peerId,
      documents: [{ document: Task, documentId }]
    })
    yield* session.flush
  }))
```

Presence is intentionally ephemeral and separate from Automerge history.

```ts
import * as Presence from "@lucas-barake/effect-local-browser/Presence"
import * as Schema from "effect/Schema"

const Cursor = Schema.Struct({ documentId: Schema.String, offset: Schema.Number })
const presence = Presence.make(Cursor, { ttlMillis: 15_000 })
```

`Presence.make` returns an Effect that builds `receive`, scoped `publish`, `remove`, and `values`. Presence identity is
the authenticated transport peer. It is not a durable user identity.

### 12. Run compaction and recovery workflows

`SqlReplica.layer` provides the registered compaction workflow runtime. Handles are scoped to the replica incarnation.

```ts
import * as ReplicaWorkflow from "@lucas-barake/effect-local-sql/ReplicaWorkflow"
import * as Effect from "effect/Effect"

const compact = Effect.gen(function*() {
  const workflows = yield* ReplicaWorkflow.WorkflowRuntime
  const execution = yield* workflows.execute(
    ReplicaWorkflow.OperationId.make("scheduled-compaction-2026-07")
  )
  const current = yield* workflows.poll(execution)
  yield* workflows.resume(execution)
  return current
})
```

The registered workflow journals document listing and one compact and prune activity per document. Recovery verifies
checkpoint checksums, heads, change metadata, and tombstones. It can fall back to an older verified checkpoint and
replay accepted changes. `Compaction` and `Recovery` are public injectable services for lower level engine assembly.

`ProjectionRebuild`, `CreateBackup`, and `RestoreBackup` reserve stable Workflow definitions but are not registered
operations in the current beta. Backup and restore streams do not become durable merely because a Workflow is used.

### 13. Write deterministic tests

`TestReplica.layer` uses SQLite in memory while retaining the production command, projection, query, and recovery
composition.

```ts
import { assert, it } from "@effect/vitest"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { definition, ListTasks, MutationLive, Task, TaskListSql } from "./domain.js"

const TestDomain = Layer.mergeAll(
  MutationLive,
  Query.layer(ListTasks, () => Effect.succeed([])),
  TaskListSql.layer
)

const TestLive = TestReplica.layer(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(TestDomain)
)

it.effect("commits locally", () =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const now = Date.now()
    const outcome = yield* replica.create(Task, {
      commandId: Identity.makeCommandId(),
      value: { title: "test", completed: false, createdAt: now, updatedAt: now }
    })
    assert.strictEqual(outcome._tag, "DurablyCommittedLocal")
    yield* CommandOutcome.committedOrFail(outcome)
  }).pipe(Effect.provide(TestLive)))
```

For sync tests use `TestReplica.layerWithSync`, `TestPeer.layer`, and `FaultInjection.layerSequence`. Fault decisions
can deterministically drop, duplicate, delay, reorder, partition, heal, and flush bounded peer traffic. Effect's
`TestClock` controls delay and presence expiration without wall clock sleeps.

## State and consistency model

| State                                      | Owner                    | Durable | Replicated                          | Rebuildable                                         |
| ------------------------------------------ | ------------------------ | ------- | ----------------------------------- | --------------------------------------------------- |
| Automerge changes and verified checkpoints | SQLite canonical store   | Yes     | Yes, through selected peer sessions | No, this is the source of truth                     |
| Document heads and command receipts        | SQLite canonical store   | Yes     | Heads travel in sync messages       | Derived from committed history and protocol records |
| Projection tables                          | SQL projection bindings  | Yes     | No                                  | Yes, from canonical snapshots                       |
| Cluster mailbox and replies                | Durable Cluster runtime  | Yes     | No                                  | No, they resolve local execution ambiguity          |
| Workflow journals and activity replies     | Durable Workflow runtime | Yes     | No                                  | No, they resume local orchestration                 |
| Atom values                                | Atom registry            | No      | No                                  | Yes, from `Replica` reads and queries               |
| Presence and tab sessions                  | Browser process          | No      | Best effort transport only          | Not applicable                                      |

Consistency guarantees:

| Boundary                 | Guarantee                                                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| One document command     | Serialized through its Cluster entity and committed with canonical state, projections, receipt, sequence, and stored reply |
| Command retry            | Same command ID and same canonical request returns the durable result                                                      |
| Different command input  | Reusing a command ID for different input fails                                                                             |
| Query                    | Reads local projection state under the replica operation gate                                                              |
| Multi document invariant | Not transactional. Model one aggregate document or an explicit Workflow                                                    |
| Peer convergence         | Replicas converge after receiving the same valid Automerge change set                                                      |
| Restore                  | Exclusive, fenced, staged, schema checked, and projection rebuilding                                                       |
| Atom invalidation        | Reactive cache refresh, not a durability acknowledgement                                                                   |
| Presence                 | Expiring best effort state with no durability guarantee                                                                    |

## Limits and security

This beta deliberately does not promise a complete collaboration product.

- No backend, relay, peer discovery, or asynchronous store and forward collaboration is included.
- Authentication, authorization, peer identity, routing, and end to end encryption belong to the application transport.
- OPFS is origin scoped and may be evicted unless the browser grants persistent storage.
- Existing secondary tabs do not yet promote themselves when the provisioning tab disappears. A new attachment can
  reprovision the owner.
- Schema evolution is not implemented in place. The exact replica definition hash is pinned.
- An old backup can require a matching application build until versioned migration support exists.
- One mutation targets one document. There is no replicated transaction across documents.
- Whole document sync is the only sync granularity. Subtree sync is not implemented.
- Store and forward capability is a transport declaration, not an implementation supplied by this library.
- Conflict inspection, history browsing, sharing policy, and resolution UI belong to the application.
- Presence is not durable awareness and must not carry authorization decisions.
- Backup creation and restore Workflow definitions are reserved but not registered in the current beta.
- Limits must be selected for the product. They bound backup bytes, archive records, JSON depth, sync messages,
  dependency graphs, pending changes, peers, sessions, RPC streams, and queues.

Read [architecture](docs/architecture.md), [durability](docs/durability.md), [sync](docs/sync.md), and
[schema evolution](docs/schema-evolution.md) for the detailed contracts.

## Examples

| Example                                            | What it demonstrates                                                                                                                                                                                              | Commands                                                                                  |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [`examples/tasks`](examples/tasks)                 | Complete React application, Atom queries and commands, SharedWorker ownership, OPFS provisioning, offline shell, persistence status, backup, and restore                                                          | `pnpm --dir examples/tasks dev`, `pnpm --dir examples/tasks test:browser`                 |
| [`examples/browser-spike`](examples/browser-spike) | Internal durability proof for atomic Cluster replies, rollback, deduplication, reload persistence, Workflow recovery, and concurrent RPC streaming. It is not the recommended application template                | `pnpm --dir examples/browser-spike dev`, `pnpm --dir examples/browser-spike test:browser` |
| [`examples/api-tour`](examples/api-tour)           | Runnable Node API tour covering domain modeling, fallible mutations, Replica commands and receipts, SQL projections and queries, backup and restore, portable documents, direct peer convergence, and test Layers | `pnpm --dir examples/api-tour check`, `pnpm --dir examples/api-tour all`                  |
| [`examples/advanced-api`](examples/advanced-api)   | Focused public service examples for schema checked presence, peer session contracts, Workflow runtime, compaction, recovery, and commit subscriptions                                                             | `pnpm --dir examples/advanced-api check`, `pnpm --dir examples/advanced-api examples`     |

## Repository scripts

Run commands from the repository root.

| Script                | Purpose                                                              |
| --------------------- | -------------------------------------------------------------------- |
| `pnpm build`          | Build all publishable packages with TypeScript project references    |
| `pnpm build:examples` | Build every example package                                          |
| `pnpm check`          | Type check packages, tests, and examples                             |
| `pnpm check:tsgo`     | Type check with the TypeScript native preview                        |
| `pnpm lint`           | Run oxlint and check dprint formatting                               |
| `pnpm lint-fix`       | Apply lint fixes and dprint formatting                               |
| `pnpm test --run`     | Run the unit and integration suite once                              |
| `pnpm test:examples`  | Run every Node example                                               |
| `pnpm test:browser`   | Run the Chromium suites for `browser-spike` and `tasks`              |
| `pnpm coverage`       | Run Vitest with V8 coverage                                          |
| `pnpm circular`       | Check package sources for circular imports                           |
| `pnpm codegen`        | Regenerate package barrel modules                                    |
| `pnpm docgen`         | Compile documentation and examples through the root TypeScript graph |
| `pnpm clean`          | Remove generated build artifacts                                     |

## Public API inventory

Every root package exports module namespaces. Every module is also available through its public subpath, such as
`@lucas-barake/effect-local/Replica`. `internal/*` is explicitly private.

### `@lucas-barake/effect-local`

| Namespace           | Public API                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Backup`            | `FormatVersion`, `Header`, `ExportOptions`, `RestoreOptions`, `ExportedDocument`                                                                                                                                                                                                                                                                                                                                                                     |
| `Canonical`         | `stringify`, `hash`, `digest`                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `CommandOutcome`    | `Rejected`, `DurablyCommittedLocal`, `OutcomeUnknown`, `CommandOutcome`, `schema`, `rejected`, `durablyCommitted`, `unknown`, `match`, `committedOrFail`                                                                                                                                                                                                                                                                                             |
| `Commit`            | `Heads`, `Commit` and their inferred types                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Document`          | `WireSchema`, `AutomergeEncoded`, `DocumentSchema`, `Document`, `Any`, `make`, `isAutomergeValue`, `decode`, `encode`                                                                                                                                                                                                                                                                                                                                |
| `DocumentSet`       | `DocumentSet`, `make`, `get`                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `Identity`          | Schemas and types for `ReplicaId`, `ReplicaIncarnation`, `SessionId`, `DocumentId`, `CommandId`, `WriterGeneration`, `CommitSequence`, `PeerId`, and `ProjectionVersion`. `makeReplicaId`, `makeSessionId`, `makeDocumentId`, `makeCommandId`, `makePeerId`, `documentIdFromCommandId`                                                                                                                                                               |
| `Mutation`          | `DraftValue`, `Draft`, `SuccessResult`, `HandlerResult`, `HandlerOptions`, `Handler`, `HandlerService`, `Mutation`, `Any`, `make`, `handler`, `layer`                                                                                                                                                                                                                                                                                                |
| `PeerTransport`     | `Capabilities`, `Connection`, `ConnectOptions`, `PeerTransport` service                                                                                                                                                                                                                                                                                                                                                                              |
| `Projection`        | `Projection`, `Any`, `make`, `assertUniqueKeys`, `evaluate`                                                                                                                                                                                                                                                                                                                                                                                          |
| `Query`             | `Handler`, `HandlerService`, `Query`, `Any`, `make`, `handler`, `layer`                                                                                                                                                                                                                                                                                                                                                                              |
| `Replica`           | `Service`, `Replica` service                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `ReplicaDefinition` | `ReplicaDefinition`, `Any`, `invalidationKeys`, `make`                                                                                                                                                                                                                                                                                                                                                                                               |
| `ReplicaError`      | Cause schemas `SqlCause`, `SchemaCause`, `WorkerCause`, `RpcCause`, `AutomergeCause`, `Cause`. Reason schemas `DocumentNotFound`, `DocumentDecodeError`, `UnsupportedDocumentVersion`, `ProjectionBlocked`, `CommandIdConflict`, `StorageUnavailable`, `StorageCorrupt`, `QuotaExceeded`, `MigrationFailed`, `BackupInvalid`, `BackupTooLarge`, `RestoreBusy`, `RestoreFailed`, `ProtocolMismatch`, `ReplicaFenced`. `Reason`, tagged `ReplicaError` |
| `ReplicaLimits`     | `Values`, `ReplicaLimits` service, `make`, `layer`                                                                                                                                                                                                                                                                                                                                                                                                   |
| `ReplicaStatus`     | `Starting`, `Ready`, `ReadOnly`, `Degraded`, `ProjectionBlocked`, `Restoring`, `Failed`, `ReplicaStatus` schemas and types                                                                                                                                                                                                                                                                                                                           |
| `Snapshot`          | `ProjectionState`, `Snapshot`, `FromDocument`                                                                                                                                                                                                                                                                                                                                                                                                        |

### `@lucas-barake/effect-local-sql`

| Namespace          | Public API                                                                                                                                                                      |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BackupStore`      | `BackupStore` service, `layer`                                                                                                                                                  |
| `CommandExecutor`  | `createRequestHash`, `mutationRequestHash`, `deleteRequestHash`, `CommandExecutor` service, `MutationHandlers`, `layer`                                                         |
| `CommitPublisher`  | `CommitEvent`, `CommitSubscription`, `CommitPublisher` service, `layer`                                                                                                         |
| `Compaction`       | `PreparedCheckpoint`, `CompactResult`, `Compaction` service, `layer`                                                                                                            |
| `DocumentEntity`   | Cluster RPC definitions `Create`, `Mutate`, `Delete`, `ApplySync`, plus `ApplySyncResult`, `DocumentEntity`, `layer`                                                            |
| `DocumentStore`    | `Stored`, `DocumentStore` service, `layer`                                                                                                                                      |
| `DurableRuntime`   | `layer`                                                                                                                                                                         |
| `EntityReplica`    | `layer`                                                                                                                                                                         |
| `Migrations`       | `canonicalStoreChecksum`, `peerSyncChecksum`, `durabilityIndexesChecksum`, `loader`, `run`, `layer`                                                                             |
| `PeerSync`         | `Session`, `Outbound`, `Reply`, `Generated`, `Received`, `PeerSync` service, `layer`                                                                                            |
| `ProjectionStore`  | `ProjectionStore` service, `BindingServices`, `layer`                                                                                                                           |
| `QueryExecutor`    | `QueryExecutor` service, `QueryHandlers`, `layer`                                                                                                                               |
| `Recovery`         | `RawRecoveryExport`, `Recovery` service, `make`, `layer`                                                                                                                        |
| `ReplicaBootstrap` | `State`, `ReplicaBootstrap` service, `make`, `layer`                                                                                                                            |
| `ReplicaGate`      | `Permit`, `ReplicaGate` service, `layer`                                                                                                                                        |
| `ReplicaWorkflow`  | `OperationId`, Workflow definitions `ProjectionRebuild`, `CompactReplica`, `CreateBackup`, `RestoreBackup`, `Execution`, `WorkflowRuntime`, `registrationLayer`, `runtimeLayer` |
| `SqlProjection`    | `Migration`, `SqlProjection`, `BindingService`, `make`, `Any`                                                                                                                   |
| `SqlReplica`       | `layerFromServices`, `layer`                                                                                                                                                    |

### `@lucas-barake/effect-local-browser`

| Namespace        | Public API                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| `BrowserReplica` | `layer`                                                                                                             |
| `BrowserSqlite`  | `DatabasePort` service, `layer`, `layerPort`                                                                        |
| `PeerSession`    | `SelectedDocument`, `Service`, `SyncEnvelope`, `makeWithClient`, `make`                                             |
| `Presence`       | `Entry`, `Presence`, `make`                                                                                         |
| `ReplicaAtom`    | `Service`, `ReplicaAtom` service, `reactivityLayer`, `layer`, `documentFamily`, `queryFamily`, `mutation`, `status` |
| `ReplicaClient`  | `Service`, `ReplicaClient` service, `fromRpcClient`, `layer`                                                        |
| `ReplicaOwner`   | `handlers`, `layer`, `layerWorker`                                                                                  |
| `ReplicaRpc`     | `protocolVersion`, `Invalidation`, `InvalidationMessage`, `QueryError`, `group`                                     |
| `SessionManager` | `leaseDurationMillis`, `Service`, `SessionManager` service, `layer`                                                 |

### `@lucas-barake/effect-local-test`

| Namespace        | Public API                                                                                                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `FaultInjection` | `Packet`, `Decision`, `Service`, `FaultInjection` service, `layer`, `none`, `layerSequence`                                                                                    |
| `TestPeer`       | Tagged errors `InvalidFault`, `QueueFull`, `ConnectionClosed`, plus `TestPeerError`, `Options`, `Connection`, `Service`, `TestPeer` service, `make`, `layer`, `transportLayer` |
| `TestReplica`    | `defaultLimits`, `layerWithLimits`, `layer`, `layerWithSyncAndLimits`, `layerWithSync`                                                                                         |

## License

[MIT](LICENSE)
