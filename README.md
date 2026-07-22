# Effect Local

Effect Local is a frontend only local first engine for Effect v4. Application reads and writes complete against a
durable browser replica. Automerge owns causal history and convergence. SQLite WASM in OPFS owns durable bytes and
rebuildable query projections. Effect Cluster serializes document commands and stores their replies. Effect Workflow
resumes long running maintenance. Effect Atom exposes reactive views without becoming another source of truth.

The design follows the ownership, longevity, and offline principles in
[Local first software](https://www.inkandswitch.com/essay/local-first/). This repository contains client code only. It
does not provide a backend, relay, authentication, encryption, or a prescribed server protocol.

> **Beta:** The library targets Effect `4.0.0-beta.99` and Automerge `3.3.2`. Durable formats,
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

## Mental model

Effect Local is easiest to understand as one durable local database with several replaceable views and execution
systems around it. The canonical document history is the authority. SQL projections, Atom values, RPC sessions, and
presence are ways to use or observe that authority. They never become a second source of truth.

The engine separates four concerns that are often collapsed into one client state library:

| Concern           | Responsibility                                                                   | What it does not own                           |
| ----------------- | -------------------------------------------------------------------------------- | ---------------------------------------------- |
| Canonical storage | Automerge changes, document heads, checkpoints, tombstones, and command receipts | UI cache state or remote availability          |
| Local execution   | Serialized commands, durable replies, maintenance workflows, and recovery        | Replicated merge semantics                     |
| Derived views     | SQLite projections and Effect Atom values optimized for reads                    | Canonical history                              |
| Connectivity      | Worker RPC, peer sessions, presence, and application supplied transports         | Authentication, routing, or a backend protocol |

### Core terminology

| Term                   | Meaning                                                                                                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Replica definition** | The schema checked blueprint for one application data model. It names the documents, mutations, projections, and queries that must agree on a durable protocol hash.            |
| **Replica**            | One running local instance of a replica definition. It exposes commands, reads, queries, status, backup, and restore operations as an Effect service.                           |
| **Document**           | The unit of canonical state, causal history, merge, compaction, and peer synchronization. A document normally represents one aggregate whose invariants must change together.   |
| **Snapshot**           | A decoded point in a document's history with its identity, version, heads, tombstone state, and value. A snapshot is a read result, not a mutable store object.                 |
| **Mutation**           | A named, schema checked command that changes exactly one document. Its handler runs against an Automerge draft and can return a declared tagged domain error.                   |
| **Command ID**         | The stable identity of a create, mutate, or delete request. It turns retry from guesswork into an idempotent protocol. Reuse it only for the same logical input.                |
| **Command receipt**    | The durable record that connects a command ID to its committed or rejected result. Receipts let a caller resolve an ambiguous transport outcome later.                          |
| **Command outcome**    | Proof of a local durable commit, a declared rejection, or `OutcomeUnknown` when the caller cannot prove which occurred. It is stronger than an RPC success response.            |
| **Projection**         | A deterministic, rebuildable SQL representation of canonical snapshots. Projections make local queries efficient but can always be discarded and rebuilt.                       |
| **Query**              | A schema checked read over projections. Its declared dependencies are also the keys used to invalidate reactive Atom computations.                                              |
| **Owner**              | The single browser runtime currently allowed to open the durable SQLite replica. Other tabs are clients of that owner rather than competing database writers.                   |
| **Session**            | A leased RPC capability bound to one client and one owner epoch. Sessions fence stale tabs and force clients to reopen after an owner restart.                                  |
| **Cluster entity**     | The per document durable command executor. It serializes local commands and stores replies atomically with application state. It does not decide CRDT convergence.              |
| **Workflow**           | Durable orchestration for work that spans retries, activities, or multiple steps. Use it for maintenance and coordination, not as a replacement for a single document mutation. |
| **Peer session**       | A scoped synchronization relationship over an application supplied `PeerTransport`. It exchanges bounded Automerge changes and resets safely when either side disconnects.      |
| **Presence**           | Expiring best effort metadata about connected peers or tabs. Presence is never durable state and must never authorize an operation.                                             |

### The write path

1. The caller creates one command ID and sends a create, mutate, or delete command to `Replica`.
2. Effect Cluster routes that command to the document entity and serializes it with other commands for the same
   document.
3. The mutation handler changes an Automerge draft or returns its declared tagged domain error.
4. One SQLite transaction commits canonical changes, heads, projections, the command receipt, the commit sequence,
   and the stored Cluster reply.
5. Commit invalidations refresh dependent Atom queries. They are notifications after durability, not the durability
   boundary itself.
6. Peer sync later exchanges the canonical Automerge changes. Remote connectivity never blocks the local commit.

This ordering explains why `DurablyCommittedLocal` is precise. It proves the local transaction and durable reply. It
does not claim that another device has received the change.

### The read path

`get` decodes the canonical snapshot for one document. `query` reads rebuildable SQL projections. Effect Atom wraps
those operations with reactive caching and invalidates them from commit events. A stale or empty Atom registry can
always recover by reading the replica again because Atom is not part of the persistence model.

### Choosing the right abstraction

| Need                                      | Use                                                  |
| ----------------------------------------- | ---------------------------------------------------- |
| Change one aggregate atomically           | One document mutation                                |
| Read one aggregate with causal metadata   | `Replica.get`                                        |
| Filter, sort, or join local data          | Projection plus query                                |
| Keep React views current                  | `ReplicaAtom` builders                               |
| Retry a command after losing the response | The same command ID, then the matching lookup method |
| Coordinate several durable steps          | Effect Workflow                                      |
| Exchange changes with another replica     | `PeerSession` and an application supplied transport  |
| Show cursors or online state              | Presence                                             |
| Move or recover all local data            | Backup and restore                                   |

The main design rule is simple: durable facts flow outward from canonical storage into projections and reactive
views. They never flow back from a cache, presence record, or transport connection into canonical history without a
schema checked command.

## Installation

Install only the packages used by the application surface. All four packages are ESM.

```sh
pnpm add effect@4.0.0-beta.99 @automerge/automerge@3.3.2
pnpm add @lucas-barake/effect-local @lucas-barake/effect-local-sql
pnpm add @lucas-barake/effect-local-browser @effect/platform-browser@4.0.0-beta.99
pnpm add @effect/sql-sqlite-wasm@4.0.0-beta.99 @effect/wa-sqlite@0.2.1
pnpm add @effect/atom-react@4.0.0-beta.99
pnpm add -D @lucas-barake/effect-local-test @effect/platform-node@4.0.0-beta.99 @effect/sql-sqlite-node@4.0.0-beta.99
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

Mutation definitions expose `payloadSchema`, `successSchema`, and `errorSchema`. Payloads accept either a schema or
`Schema.Struct` fields directly, matching Effect RPC. Implementations are separate Layer services. A mutation handler
is synchronous because it runs inside an Automerge change and a durable SQL command transaction. `toLayer` accepts a
handler directly or an Effect that constructs one.

```ts
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { Task } from "./domain.js"

export class TitleEmpty extends Schema.TaggedErrorClass<TitleEmpty>()("TitleEmpty", {}) {}

export const RenameTask = Mutation.make("RenameTask", {
  document: Task,
  payload: { title: Schema.String },
  error: TitleEmpty
})

export const SetTaskCompleted = Mutation.make("SetTaskCompleted", {
  document: Task,
  payload: { completed: Schema.Boolean }
})

export const MutationLive = Layer.mergeAll(
  RenameTask.toLayer(({ draft, payload }) => {
    const title = payload.title.trim()
    if (title.length === 0) return Result.fail(new TitleEmpty())
    draft.title = title
    draft.updatedAt = Date.now()
    return Result.succeed(undefined)
  }),
  SetTaskCompleted.toLayer(({ draft, payload }) => {
    draft.completed = payload.completed
    draft.updatedAt = Date.now()
    return undefined
  })
)
```

The handler receives the mutable encoded `draft`, the decoded `payload`, and the decoded `current` value. A mutation
with a declared error schema returns `Result`. A mutation whose error is `Schema.Never` returns its success directly.
Declared domain errors must be schema backed yieldable tagged errors. Define one error with `Schema.TaggedErrorClass`,
or pass a `Schema.Union` of tagged error classes when a mutation can reject for several domain reasons. This keeps
`Effect.catchTag`, `Effect.catchTags`, `Result`, RPC encoding, and durable receipts on one discriminated error model.

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
  payload: { search: Schema.String },
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

const create = Effect.gen(function*() {
  const replica = yield* Replica.Replica
  const now = Date.now()
  const outcome = yield* replica.create(Task, {
    commandId: yield* Identity.makeCommandId,
    value: { title: "Read the paper", completed: false, createdAt: now, updatedAt: now }
  })
  return yield* CommandOutcome.committedOrFail(outcome)
})

const rename = (documentId: Identity.DocumentId) =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    return yield* replica.mutate(RenameTask, {
      commandId: yield* Identity.makeCommandId,
      documentId,
      payload: { title: "Build the engine" }
    })
  }).pipe(
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

`committedOrFail` returns the committed value, fails directly with a declared mutation error for `Rejected`, and fails
with `CommandOutcomeUnknown` when durability cannot be established. Both failure paths are tagged and work with
`Effect.catchTag`.

Query definitions expose the same `payloadSchema`, `successSchema`, and `errorSchema` metadata. Query handlers are also
injected Layers and may depend on `SqlClient.SqlClient`. `toLayer` accepts a handler directly or an Effect that
constructs one, then captures its Effect services in the installed handler Layer.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { ListTasks } from "./domain.js"

const ListTasksSql = SqlSchema.findAll({
  Request: ListTasks.payloadSchema,
  Result: Schema.Struct({
    sourceDocumentId: Identity.DocumentId,
    title: Schema.String,
    completed: Schema.BooleanFromBit,
    updatedAt: Schema.Number
  }),
  execute: ({ search }) => {
    const pattern = `%${search.trim().toLocaleLowerCase()}%`
    return SqlClient.SqlClient.use((sql) =>
      sql`SELECT source_document_id AS sourceDocumentId, title, completed, updated_at AS updatedAt
          FROM task_list_v1
          WHERE ${pattern} = '%%' OR LOWER(title) LIKE ${pattern}
          ORDER BY updated_at DESC`
    )
  }
})

export const QueryLive = ListTasks.toLayer((payload) => ListTasksSql(payload).pipe(Effect.orDie))
```

### 6. Compose the durable engine

`SqlReplica.layerWithBindings` accepts exactly one SQL binding per declared projection and provides those binding
services automatically. It still requires every mutation and query handler,
`ReplicaLimits`, `Crypto`, and `SqlClient`. It provides `Replica`, `CommitPublisher`, `ReplicaWorkflow.CompactionWorkflow`,
`PeerSync`, `ReplicaGate`, and Effect Cluster `Sharding`. The owner side peer services stay available so
`PeerSession.make` can share the same durable runtime and fencing gate.

```ts
import { BrowserCrypto } from "@effect/platform-browser"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import { definition, MutationLive, QueryLive, TaskListSql } from "./domain.js"

const DomainLive = Layer.mergeAll(MutationLive, QueryLive)

export const EngineLive = SqlReplica.layerWithBindings(definition, {
  projections: [TaskListSql]
}).pipe(
  Layer.provideMerge(DomainLive),
  Layer.provideMerge(BrowserCrypto.layer),
  Layer.provideMerge(ReplicaLimits.layer(limits)),
  Layer.provideMerge(DatabaseLive)
)
```

`DatabaseLive` is any Effect v4 `SqlClient.SqlClient` Layer. `SqlReplica.layerWithBindings` also requires the Effect `Crypto`
service. In the browser these come from `BrowserSqlite.layer` or `BrowserSqlite.layerMessagePort` and
`BrowserCrypto.layer`. Node programs provide `NodeCrypto.layer`. The complete limits object is available as
`TestReplica.defaultLimits` for tests and in
[`examples/tasks/src/domain.ts`](examples/tasks/src/domain.ts) for a browser configuration.

Use the lower level `SqlReplica.layer` when an application intentionally wants to provide or override projection
binding services itself.

### 7. Provide the official Effect Worker layer

`BrowserReplica.layer(definition)` intentionally requires Effect's `WorkerPlatform` and `Spawner`. It does not create
or hide a `SharedWorker`.

```ts
import { BrowserCrypto, BrowserWorker } from "@effect/platform-browser"
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

export const BrowserLive = BrowserReplica.layerWithReactivity(definition).pipe(
  Layer.provide(Layer.merge(WorkerLive, BrowserCrypto.layer))
)
```

The `Attach` message is application ownership protocol, not hidden library behavior. The production example also
handles liveness, expiring provisioning nonces, OPFS worker creation, database port transfer, and provider loss. See
[`examples/tasks/src/replica-client.ts`](examples/tasks/src/replica-client.ts) and
[`examples/tasks/src/replica.shared-worker.ts`](examples/tasks/src/replica.shared-worker.ts).

Inside the SharedWorker, compose `BrowserSqlite.layerMessagePort(databasePort)`, domain Layers, `SqlReplica.layer`, and
`SessionManager.layer`. Serve each attached RPC port with `ReplicaOwner.layerWorker(definition)` and the official
`BrowserWorkerRunner.layerMessagePort(rpcPort)` Layer.

### 8. Build reactive state with Effect Atom

The runtime helpers use native Effect v4 reactivity APIs. Atom state is a cache over `Replica`, never the durable
authority.

```ts
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import { Atom } from "effect/unstable/reactivity"
import { BrowserLive } from "./browser.js"
import { ListTasks, RenameTask, Task } from "./domain.js"

export const runtime = Atom.runtime(BrowserLive)
export const tasks = ReplicaAtom.queryFamily(runtime, ListTasks)
export const task = ReplicaAtom.documentFamily(runtime, Task)
export const renameTask = ReplicaAtom.mutation(runtime, RenameTask)
export const replicaStatus = ReplicaAtom.status(runtime)

const allTasks = tasks({ search: "" })
const oneTask = Effect.map(Identity.makeDocumentId, task)
```

`queryFamily` canonicalizes its schema encoded payload for stable family identity. It invalidates on every declared
projection and source document key. `mutation` invalidates the mutation's document key after execution. For commands
with custom invalidation needs, use `runtime.fn` and its `reactivityKeys` option as the Tasks example does.

React applications provide an `AtomRegistry` through `@effect/atom-react` and consume the returned atoms with
`useAtomValue`, `useAtomSet`, and `useAtomRefresh`. Non React integrations mount the same atoms directly in an
`AtomRegistry`.

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
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const value = yield* replica.exportDocument(Task, documentId)
    return yield* replica.importDocument(Task, {
      commandId: yield* Identity.makeCommandId,
      value
    })
  })
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
const presence = Presence.make(Cursor, { timeToLive: "15 seconds" })
```

`Presence.make` returns an Effect that builds `receive`, scoped `publish`, `remove`, and `values`. Presence identity is
the authenticated transport peer. It is not a durable user identity. `timeToLive` accepts any Effect `Duration.Input`
whose normalized millisecond value is positive, finite, and no greater than `Number.MAX_SAFE_INTEGER`.

### 12. Run compaction and recovery workflows

`SqlReplica.layer` provides the registered compaction workflow runtime. Handles are scoped to the replica incarnation.

```ts
import * as ReplicaWorkflow from "@lucas-barake/effect-local-sql/ReplicaWorkflow"
import * as Effect from "effect/Effect"

const compact = Effect.gen(function*() {
  const workflows = yield* ReplicaWorkflow.CompactionWorkflow
  const execution = yield* workflows.execute(
    ReplicaWorkflow.OperationId.make("scheduled-compaction-2026-07")
  )
  const current = yield* workflows.poll(execution)
  return current
})
```

The registered workflow journals document listing and one compact and prune activity per document. Recovery verifies
checkpoint checksums, heads, change metadata, and tombstones. It can fall back to an older verified checkpoint and
replay accepted changes. `Compaction` and `Recovery` are public injectable services for lower level engine assembly.

Backup and restore streams do not become durable merely because a Workflow is used.

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
  ListTasks.toLayer(() => Effect.succeed([])),
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
      commandId: yield* Identity.makeCommandId,
      value: { title: "test", completed: false, createdAt: now, updatedAt: now }
    })
    assert.strictEqual(outcome._tag, "DurablyCommittedLocal")
    yield* CommandOutcome.committedOrFail(outcome)
  }).pipe(Effect.provide(TestLive)))
```

For sync tests use `TestReplica.layerWithSync`, `TestPeer.layer`, and `FaultInjection.layerSequence`. Fault decisions
can deterministically drop, duplicate, delay, reorder, partition, heal, and flush bounded peer traffic. Effect's
`TestClock` controls delay and presence expiration without wall clock sleeps. `TestPeer.make` and `TestPeer.layer`
validate their bounds in the Effect error channel with the tagged `InvalidOptions` error.

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

| Namespace           | Public API                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Backup`            | `FormatVersion`, `Header`, `ExportOptions`, `RestoreOptions`, `ExportedDocument`                                                                                                                                                                                                                                                                                                                                                                        |
| `Canonical`         | `stringify`, `hash`, `digest`                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `CommandOutcome`    | `Rejected`, `DurablyCommittedLocal`, `OutcomeUnknown`, `CommandOutcomeUnknown`, `CommandOutcome`, `schema`, `rejected`, `durablyCommitted`, `unknown`, `match`, `committedOrFail`                                                                                                                                                                                                                                                                       |
| `Commit`            | `Heads`, `Commit` and their inferred types                                                                                                                                                                                                                                                                                                                                                                                                              |
| `Document`          | `WireSchema`, `AutomergeEncoded`, `DocumentSchema`, `Document`, `Any`, `make`, `isAutomergeValue`, `decode`, `encode`                                                                                                                                                                                                                                                                                                                                   |
| `DocumentSet`       | `DocumentSet`, `make`, `get`                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `Identity`          | Schemas and types for `ReplicaId`, `ReplicaIncarnation`, `SessionId`, `DocumentId`, `CommandId`, `WriterGeneration`, `CommitSequence`, `PeerId`, and `ProjectionVersion`. `makeReplicaId`, `makeSessionId`, `makeDocumentId`, `makeCommandId`, `makePeerId`, `documentIdFromCommandId`                                                                                                                                                                  |
| `Mutation`          | `DraftValue`, `Draft`, `SuccessResult`, `HandlerResult`, `HandlerOptions`, `Handler`, `HandlerService`, `Mutation`, `Any`, `make`; definitions expose `payloadSchema`, `successSchema`, `errorSchema`, `of`, and `toLayer`; `toLayer` accepts a handler or an Effect that builds one                                                                                                                                                                    |
| `PeerTransport`     | `Capabilities`, `Connection`, `ConnectOptions`, `PeerTransport` service                                                                                                                                                                                                                                                                                                                                                                                 |
| `Projection`        | `Projection`, `Any`, `make`, `assertUniqueKeys`, `evaluate`                                                                                                                                                                                                                                                                                                                                                                                             |
| `Query`             | `Handler`, `HandlerService`, `Query`, `Any`, `make`; definitions expose `payloadSchema`, `successSchema`, `errorSchema`, `of`, and `toLayer`; `toLayer` accepts a handler or an Effect that builds one                                                                                                                                                                                                                                                  |
| `Replica`           | `Replica` service                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ReplicaDefinition` | `ReplicaDefinition`, `Any`, `invalidationKeys`, `make`                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ReplicaError`      | Reason schemas `DocumentNotFound`, `DocumentDecodeError`, `DocumentEncodeError`, `UnsupportedDocumentVersion`, `ProjectionBlocked`, `CommandIdConflict`, `StorageUnavailable`, `StorageCorrupt`, `QuotaExceeded`, `MigrationFailed`, `BackupInvalid`, `BackupTooLarge`, `RestoreBusy`, `RestoreFailed`, `ProtocolMismatch`, `ReplicaFenced`. Causal reasons use `Schema.Defect()` for transportable arbitrary failures. `Reason`, tagged `ReplicaError` |
| `ReplicaLimits`     | `Values`, `ReplicaLimits` service, `make`, `layer`                                                                                                                                                                                                                                                                                                                                                                                                      |
| `ReplicaStatus`     | `Starting`, `Ready`, `ReadOnly`, `Degraded`, `ProjectionBlocked`, `Restoring`, `Failed`, `ReplicaStatus` schemas and types                                                                                                                                                                                                                                                                                                                              |
| `Snapshot`          | `ProjectionState`, `Snapshot`, `FromDocument`                                                                                                                                                                                                                                                                                                                                                                                                           |

### `@lucas-barake/effect-local-sql`

| Namespace          | Public API                                                                                                              |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `BackupStore`      | `BackupStore` service, `layer`                                                                                          |
| `CommandExecutor`  | `createRequestHash`, `mutationRequestHash`, `deleteRequestHash`, `CommandExecutor` service, `MutationHandlers`, `layer` |
| `CommitPublisher`  | `CommitEvent`, `CommitSubscription`, `CommitPublisher` service, `layer`                                                 |
| `Compaction`       | `PreparedCheckpoint`, `CompactResult`, `Compaction` service, `layer`                                                    |
| `DocumentEntity`   | Cluster RPC definitions `Create`, `Mutate`, `Delete`, `ApplySync`, plus `ApplySyncResult`, `DocumentEntity`, `layer`    |
| `DocumentStore`    | `Stored`, `DocumentStore` service, `layer`                                                                              |
| `DurableRuntime`   | `layer`, `layerWith`                                                                                                    |
| `EntityReplica`    | `layer`                                                                                                                 |
| `Migrations`       | `canonicalStoreChecksum`, `peerSyncChecksum`, `durabilityIndexesChecksum`, `loader`, `run`, `layer`                     |
| `PeerSync`         | `Session`, `Outbound`, `Reply`, `Generated`, `Received`, `PeerSync` service, `layer`                                    |
| `ProjectionStore`  | `ProjectionStore` service, `BindingServices`, `layer`                                                                   |
| `QueryExecutor`    | `QueryExecutor` service, `QueryHandlers`, `layer`                                                                       |
| `Recovery`         | `RawRecoveryExport`, `Recovery` service, `make`, `layer`                                                                |
| `ReplicaBootstrap` | `State`, `ReplicaBootstrap` service, `make`, `layer`                                                                    |
| `ReplicaGate`      | `Permit`, `ReplicaGate` service, `layer`                                                                                |
| `ReplicaWorkflow`  | `OperationId`, `CompactReplica`, `Execution`, `CompactionWorkflow`, `layerRegistration`, `layerRuntime`                 |
| `SqlProjection`    | `Migration`, `SqlProjection`, `BindingService`, `make`, `Any`                                                           |
| `SqlReplica`       | `layerFromServices`, `layer`, `layerWithBindings`                                                                       |

### `@lucas-barake/effect-local-browser`

| Namespace        | Public API                                                                             |
| ---------------- | -------------------------------------------------------------------------------------- |
| `BrowserReplica` | `layer`, `layerWith`, `layerWithReactivity`, `layerWithReactivityOptions`              |
| `BrowserSqlite`  | `DatabasePort` service, `layer`, `layerMessagePort`                                    |
| `PeerSession`    | `SelectedDocument`, `PeerSession`, `SyncEnvelope`, `makeTestClient`, `make`            |
| `Presence`       | `Entry`, `Presence`, `make`                                                            |
| `ReplicaAtom`    | `layerReactivity`, `documentFamily`, `queryFamily`, `mutation`, `status`               |
| `ReplicaClient`  | `ReplicaClient` service, `fromRpcClient`, `layer`                                      |
| `ReplicaOwner`   | `layerHandlers`, `layer`, `layerWorker`                                                |
| `ReplicaRpc`     | `protocolVersion`, `Invalidation`, `InvalidationMessage`, `ReplicaQueryError`, `group` |
| `SessionManager` | `leaseDurationMillis`, `SessionManager` service, `layer`                               |

### `@lucas-barake/effect-local-test`

| Namespace        | Public API                                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FaultInjection` | `Packet`, `Decision`, `FaultInjection` service, `layer`, `none`, `layerSequence`                                                                                                      |
| `TestPeer`       | Tagged errors `InvalidOptions`, `InvalidFault`, `QueueFull`, `ConnectionClosed`, plus `TestPeerError`, `Options`, `Connection`, `TestPeer` service, `make`, `layer`, `transportLayer` |
| `TestReplica`    | `defaultLimits`, `layerWithLimits`, `layer`, `layerWithSyncAndLimits`, `layerWithSync`                                                                                                |

## License

[MIT](LICENSE)
