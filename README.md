# Effect Local

Effect Local is a local first engine for browser and Node local processes built on Effect v4. Application reads and
writes complete against a local replica. Automerge defines document state, causal history, and convergence. SQLite
persists the local representation and rebuildable query projections. In browsers, SQLite WASM stores its database in
OPFS. Effect Cluster serializes document commands and stores their replies. Effect Workflow resumes long running
maintenance. Effect Atom exposes reactive views without becoming another source of truth.

The design follows the ownership, longevity, and offline principles in
[Local first software](https://www.inkandswitch.com/essay/local-first/). This repository provides local process
libraries. It does not provide a hosted backend, relay, authentication service, encryption service, or prescribed
server protocol. The browser package adds tab and worker ownership. The SQL package also supports a local Node host.

> **Beta:** The library targets Effect `4.0.0-beta.99` and Automerge `3.3.2`. Durable formats,
> worker protocols, and public APIs can still change. Read [Limits and security](#limits-and-security) before adopting
> it for user data.

## Why Effect Local

- The local replica is authoritative for interactive reads and writes.
- Every public boundary uses Effect Schema codecs.
- Mutation handlers, query handlers, SQL projections, workers, transports, and limits are Effect services or Layers.
- Command IDs make retries explicit and distinguish a durable commit from an unknown outcome.
- Canonical Automerge state is separate from rebuildable SQL projections and ephemeral Atom caches.
- Cluster and Workflow provide durable execution without replacing Automerge merge semantics.
- Production shaped in memory layers and deterministic peer faults keep tests fast and reproducible.

## Mental model

Effect Local is easiest to understand as one local database with several replaceable views and execution systems
around it. The local replica is the primary copy for interactive work, following the
[local first model](https://www.inkandswitch.com/essay/local-first/local-first.pdf). Automerge defines the logical
document state and merge rules. SQLite provides the local database commit boundary. OPFS is the browser file
storage substrate. SQL projections and Atom values let applications read the same facts in useful forms. RPC
sessions, peer sessions, and presence connect processes without becoming another source of truth.

This separates concerns that are often collapsed into one client state library:

| Concern                  | Responsibility                                                            | What it does not own                           |
| ------------------------ | ------------------------------------------------------------------------- | ---------------------------------------------- |
| Canonical recovery state | Automerge changes, heads, checkpoints, tombstones, and command receipts   | Physical storage bytes or UI cache state       |
| Physical persistence     | SQLite transactions and database bytes, stored in OPFS in the browser     | Merge semantics or permanent browser retention |
| Local execution          | Serialized commands, durable replies, maintenance workflows, and recovery | Replicated convergence                         |
| Derived durable views    | SQL projection tables optimized for application queries                   | Canonical history                              |
| Ephemeral views          | Atom caches, RPC leases, and presence                                     | Durable facts                                  |
| Connectivity             | Worker RPC, peer sessions, and application supplied transports            | Authentication, routing, or a backend protocol |

The distinction follows Automerge's separation between a
[CRDT document and its storage adapter](https://automerge.org/docs/reference/repositories/storage/) and SQLite's
[transactional guarantees](https://www.sqlite.org/transactional.html). Automerge does not persist bytes by itself.
SQLite does not decide how concurrent document changes merge. Automerge `save` encodes compressed document bytes and
`load` reconstructs a document from those bytes. Neither operation writes the bytes to durable storage.

### Core terminology

| Term                       | Meaning                                                                                                                                                                                                            |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Replica definition**     | The schema checked protocol blueprint for one application model. It names the document definitions, mutations, projections, and queries that must agree on a definition hash.                                      |
| **Replica service**        | The running Effect service for commands, canonical reads, projection queries, status, backup, and restore.                                                                                                         |
| **Document definition**    | The inert name, schema, and version descriptor returned by `Document.make`. It is not stored document data.                                                                                                        |
| **Document instance**      | One `DocumentId` with its Automerge state and causal history. It normally represents one aggregate whose invariants must change together.                                                                          |
| **Snapshot**               | Effect Local's decoded read result for one document instance. It contains identity, version, current heads, tombstone state, projection state, and value.                                                          |
| **Heads**                  | The frontier change hashes that identify a point in Automerge document history. Heads are not revision numbers, storage bytes, or acknowledgements.                                                                |
| **Mutation definition**    | A named, schema checked domain operation for one document definition. Its handler changes an Automerge draft and can return a declared tagged domain error.                                                        |
| **Command**                | One idempotent create, mutate, or delete request. Its stable command ID must be reused only for the same logical input.                                                                                            |
| **Automerge change**       | A causal group of CRDT operations produced or accepted by a document instance. A command can produce no new change, so commands and changes are not one to one.                                                    |
| **Local database commit**  | The successful SQLite transaction that makes canonical state, projections, command evidence, and sequence metadata durable together. The default Cluster runtime also stores its reply in that transaction.        |
| **Commit event**           | Postcommit metadata used to invalidate dependent reads. It reports local durable work but is not itself the durability boundary.                                                                                   |
| **Command receipt**        | Internal durable evidence that connects a command ID to a committed or rejected result. Public lookup methods expose the `CommandOutcome` proven by that evidence.                                                 |
| **Command outcome**        | `DurablyCommittedLocal`, a declared `Rejected` result, or `OutcomeUnknown` when the caller cannot prove what occurred. Operational and protocol failures remain `ReplicaError` values in the Effect error channel. |
| **Projection definition**  | A deterministic mapping from canonical snapshots to a rebuildable read model.                                                                                                                                      |
| **SQL projection binding** | The projection table name, table migrations, delete operation, and row insertion operation. A projection table is a read model. A SQLite index is a separate query planner structure on that table.                |
| **Projection state**       | `Ready`, `Blocked`, or `Rebuilding` for a snapshot. Queries require their declared projection dependencies to be ready.                                                                                            |
| **Query definition**       | A named, schema checked read whose declared projection dependencies drive readiness and reactive invalidation. SQL is the standard handler implementation, not a requirement of the core type.                     |
| **Invalidation key**       | Notification metadata for refreshing a document or projection read. It is not canonical data or a durability acknowledgement.                                                                                      |
| **Presence**               | Expiring best effort metadata about connected peers or tabs. Presence is never durable state and must never authorize an operation.                                                                                |

Automerge calls the document the unit of change and defines a change as a group of operations. See its
[concepts](https://automerge.org/docs/reference/concepts/) and
[binary format specification](https://automerge.org/automerge-binary-format-spec/). Effect Local adds definition,
identity, schema, projection, command, and transaction boundaries around that CRDT model.

### Identity and fencing

Identities answer different questions. They are not interchangeable version counters.

| Identity or generation | Meaning                                                                                                                                                           |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReplicaId`            | Identity of the currently installed logical replica. Clone restore creates a new ID. Replace restore adopts the archive ID.                                       |
| `ReplicaIncarnation`   | Generation of the canonical contents. Replace restore advances it so stale sync state cannot enter the replacement.                                               |
| `WriterGeneration`     | Persisted, increasing fencing token for the current durable writer. Old writers fail validation before they write.                                                |
| Owner epoch            | Opaque identity of one browser owner runtime. It detects restart and stale invalidation streams. It is not the durable writer fence.                              |
| `SessionId`            | Renewable RPC lease for one client attached to one owner epoch.                                                                                                   |
| `PeerId`               | Identity of an application selected synchronization peer.                                                                                                         |
| Connection epoch       | Identity of one peer sync connection. Reconnect creates a new epoch while durable outbox state remains explicitly managed.                                        |
| `DocumentId`           | Stable identity of one document instance.                                                                                                                         |
| `CommandId`            | Stable identity and idempotency key of one logical command.                                                                                                       |
| `CommitSequence`       | Local sequence used for invalidation watermarks. It increases within an installed incarnation but can change on restore. It is not an Automerge history position. |

The client session lease authorizes ordinary RPC calls. The browser owner epoch distinguishes invalidation
generations. The writer generation fences durable database writes. Only the writer generation mirrors the fencing
token pattern in the [Chubby lock service](https://storage.googleapis.com/gweb-research2023-media/pubtools/4444.pdf),
where an older acquisition count cannot authorize a newer write.

### Effect composition model

The public model forms one composition ladder:

`definitions` to `handler Layers` to `SQL bindings` to `durable runtime Layer` to `Replica service`

Definitions are inert values. Mutation and query Layers provide behavior. SQL bindings provide physical read model
operations. `SqlReplica.layerWithBindings` constructs the durable services. Application effects consume
`Replica.Replica` from the Effect environment.

Effect concepts retain their normal meanings:

| Effect concept    | Role in Effect Local                                                                                                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Service           | A typed capability such as `Replica`, `PeerTransport`, `ReplicaLimits`, or a mutation handler.                                                                                        |
| Layer             | Construction and dependency wiring for services, including scoped resources.                                                                                                          |
| Scope             | Lifetime and cleanup boundary for resources. Scope does not mean persistence.                                                                                                         |
| Cluster entity    | Addressable RPC protocol routed by shard. Effect Local's configured document entity serializes local commands and stores their replies. It is not peer discovery or CRDT replication. |
| Workflow          | Durable orchestration for retries and multiple steps. Effect Local ships compaction orchestration and lets advanced runtimes register more workflows.                                 |
| Atom and registry | Reactive view and cache lifetime. Atom values rebuild from `Replica` reads and queries.                                                                                               |

These definitions follow the Effect source contracts for
[Context](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/Context.ts),
[Layer](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/Layer.ts),
[Scope](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/Scope.ts),
[Cluster Entity](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/unstable/cluster/Entity.ts),
[Workflow](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/unstable/workflow/Workflow.ts),
and [Atom](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/unstable/reactivity/Atom.ts).

### Connectivity and browser ownership

Synchronization has three layers:

| Layer           | Responsibility                                                                                                                      |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `PeerTransport` | Application supplied connection, peer identity, routing, security, receive stream, send operation, and close operation.             |
| `PeerSync`      | Durable protocol state, bounded outbox, sequencing, replay, and received change application.                                        |
| `PeerSession`   | Scoped orchestration that binds one transport connection to one replica incarnation and a selected set of whole document instances. |

`observedByPeer` means Automerge's per peer sync state indicates that the peer has all local changes. It does not prove
remote storage durability. Effect Local therefore keeps durable confirmation separate and currently reports it as
false.

The browser topology also assigns distinct roles:

| Role                    | Responsibility                                                                          |
| ----------------------- | --------------------------------------------------------------------------------------- |
| RPC client              | Page code using `BrowserReplica` and a leased client session.                           |
| Provisioning provider   | Page that creates the dedicated OPFS worker and transfers its database port capability. |
| Replica owner runtime   | SharedWorker runtime that hosts RPC and serves one open database.                       |
| Durable database worker | Dedicated worker that owns the SQLite WASM connection and OPFS access.                  |

SharedWorker is the rendezvous and RPC host. The Web Lock elects one live browser holder. The OPFS access handle
provides file exclusivity. The persisted writer generation fences stale writes. A transferred `MessagePort` is a
capability, not a shared database handle.

### The write path

1. The caller creates one command ID and sends a create, mutate, or delete command to `Replica`.
2. Effect Cluster routes that command to the document entity and serializes it with other commands for the same
   document.
3. The mutation handler changes an Automerge draft or returns its declared tagged domain error.
4. One SQLite transaction commits any produced Automerge changes, heads, projections, the command receipt, and the
   commit sequence. The default `SqlReplica.layer` and `layerWithBindings` runtime stores the Cluster reply in the same
   transaction.
5. Commit invalidations refresh dependent Atom queries. They are notifications after durability, not the durability
   boundary itself.
6. Peer sync later exchanges the canonical Automerge changes. Remote connectivity never blocks the local commit.

This ordering explains why `DurablyCommittedLocal` is precise. It proves that SQLite reported a committed transaction
through the configured VFS and that the receipt evidence is available. The default Cluster runtime also stores its
reply in that transaction. The outcome does not prove peer delivery, remote persistence, permanent browser retention,
or protection from site data deletion.

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

The main design rule is simple: durable facts flow outward from canonical recovery state into projections and reactive
views. They never flow back from a cache, presence record, or transport connection into canonical history without a
schema checked command.

### Terms that require qualifiers

Some familiar words refer to multiple operations. The README uses the qualified form when the distinction matters.

| Qualified term               | Meaning                                                                                                                          |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Canonical encoding utilities | Deterministic `Canonical.stringify`, `Canonical.hash`, and `Canonical.digest`. This module is not the canonical storage service. |
| Storage compaction           | Publish a verified Automerge save checkpoint and prune redundant SQL change rows while preserving logical document history.      |
| History truncation           | Deliberately discard causal history. Effect Local storage compaction does not make this promise.                                 |
| Library storage migration    | Library controlled migration of the internal SQL format.                                                                         |
| Projection table migration   | Application supplied DDL for one rebuildable projection table.                                                                   |
| Document schema evolution    | Change to a document definition version and value schema. In place evolution is not implemented yet.                             |

## Installation

Install only the packages used by the application surface. All four packages are ESM. Start with the core model and
protocol types:

```sh
pnpm add @lucas-barake/effect-local effect@4.0.0-beta.99 @automerge/automerge@3.3.2
```

For a durable Node replica, add the SQL engine and Node providers:

```sh
pnpm add @lucas-barake/effect-local-sql @effect/platform-node@4.0.0-beta.99 @effect/sql-sqlite-node@4.0.0-beta.99
```

For a durable browser replica, add the SQL and browser packages plus the OPFS driver:

```sh
pnpm add @lucas-barake/effect-local-sql @lucas-barake/effect-local-browser
pnpm add @effect/platform-browser@4.0.0-beta.99 @effect/sql-sqlite-wasm@4.0.0-beta.99 @effect/wa-sqlite@0.2.1
```

Effect Atom React bindings are optional:

```sh
pnpm add @effect/atom-react@4.0.0-beta.99
```

For tests, add the production shaped test package and the Effect Vitest integration. Add the Node platform package
when tests create command or peer IDs directly.

```sh
pnpm add -D @lucas-barake/effect-local-test @effect/vitest@4.0.0-beta.99 vitest@4.1.10
pnpm add -D @effect/platform-node@4.0.0-beta.99
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
    return Result.void
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

`mutations`, `projections`, and `queries` may be omitted when they are empty. The returned definition still exposes
all three collections as readonly empty tuples.

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

The projection table is a rebuildable, query optimized read model. It is durable so reads remain fast after restart,
but canonical Automerge recovery state remains the source of truth. SQLite indexes are separate query planner
structures on this table.

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

`Identity.makeCommandId` and the other identity constructors require Effect's `Crypto` service. Browser applications
provide `BrowserCrypto.layer`. Node applications and tests provide `NodeCrypto.layer`.

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
const DomainDependencies = DomainLive.pipe(Layer.provideMerge(DatabaseLive))
const EngineDependencies = Layer.mergeAll(
  DomainDependencies,
  BrowserCrypto.layer,
  ReplicaLimits.layer(limits)
)

export const EngineLive = SqlReplica.layerWithBindings(definition, {
  projections: [TaskListSql]
}).pipe(
  Layer.provide(EngineDependencies)
)
```

`DatabaseLive` is any Effect v4 `SqlClient.SqlClient` Layer. `SqlReplica.layerWithBindings` also requires the Effect `Crypto`
service. In the browser these come from `BrowserSqlite.layer` or `BrowserSqlite.layerMessagePort` and
`BrowserCrypto.layer`. Node programs provide `NodeCrypto.layer`. The complete limits object is available as
`TestReplica.defaultLimits` for tests and in
[`examples/tasks/src/domain.ts`](examples/tasks/src/domain.ts) for a browser configuration.

Choose the constructor by assembly level:

| Constructor         | Use                                                                                       |
| ------------------- | ----------------------------------------------------------------------------------------- |
| `layerWithBindings` | Application default. Installs one declared SQL binding for every projection.              |
| `layer`             | Lower level composition that receives projection binding services from another Layer.     |
| `layerFromServices` | Framework assembly from already constructed durable services. It provides only `Replica`. |

### 7. Provide the official Effect Worker layer

`BrowserReplica.layer(definition)` intentionally requires Effect's `WorkerPlatform` and `Spawner`. It does not create
or hide a `SharedWorker`.

The following snippet shows only the RPC attachment. A durable first launch also needs the application ownership
protocol described immediately after it.

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

Runtime ownership is scoped. `Atom.runtime` does not own a dispose operation. React applications release mounted
atoms when `RegistryProvider` unmounts. Non React applications dispose their `AtomRegistry`. Applications separately
close control ports and terminate workers they created. Call `replica.flush` before an intentional shutdown when the
application wants pending invalidations published. It is not a substitute for browser lifecycle guarantees.

The current SQL composition emits `Ready` after startup. Other status variants are public protocol values for
compositions that publish richer startup, restore, or failure state.

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
import { NodeCrypto } from "@effect/platform-node"
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
  ListTasks.toLayer(() => Effect.succeed([]))
)

const TestLive = TestReplica.layer(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(TestDomain)
)

it.layer(NodeCrypto.layer)("replica", (it) => {
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
})
```

Every `TestReplica` constructor installs the SQL binding Layers passed in `projections`. Consumer Layers provide only
mutation and query handlers. `layer` and `layerWithLimits` use the full durable runtime. `layerWithSync` and
`layerWithSyncAndLimits` use the direct protocol graph for deterministic peer tests.

For sync tests use `TestReplica.layerWithSync`, `TestPeer.layer`, and `FaultInjection.layerSequence`. Fault decisions
can deterministically drop, duplicate, delay, reorder, partition, heal, and flush bounded peer traffic. Effect's
`TestClock` controls delay and presence expiration without wall clock sleeps. `TestPeer.make` and `TestPeer.layer`
validate their bounds in the Effect error channel with the tagged `InvalidOptions` error.

## State and consistency model

| State                                      | Owner                    | Durable | Replicated                          | Rebuildable                                         |
| ------------------------------------------ | ------------------------ | ------- | ----------------------------------- | --------------------------------------------------- |
| Automerge changes and verified checkpoints | Canonical recovery store | Yes     | Yes, through selected peer sessions | No, this is the logical source of truth             |
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
- OPFS is origin scoped. Browser storage starts as best effort, can be evicted unless persistence is granted, and is
  deleted when the user clears the site's storage.
- Existing secondary tabs do not yet promote themselves when the provisioning tab disappears. A new attachment can
  reprovision the owner.
- Document schema evolution is not implemented in place. The exact replica definition hash is pinned. Library storage
  migrations and projection table migrations are separate supported mechanisms.
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

| Script                  | Purpose                                                              |
| ----------------------- | -------------------------------------------------------------------- |
| `pnpm bench`            | Run reproducible library benchmarks once                             |
| `pnpm build`            | Build all publishable packages with TypeScript project references    |
| `pnpm build:examples`   | Build every example package                                          |
| `pnpm check`            | Type check packages, tests, and examples                             |
| `pnpm check:all`        | Add the native TypeScript preview check to local commit checks       |
| `pnpm check:pre-commit` | Run nonmodifying local commit checks once                            |
| `pnpm check:tsgo`       | Type check with the TypeScript native preview                        |
| `pnpm dead-code`        | Find unused private files, exports, and dependencies                 |
| `pnpm lint`             | Run oxlint and check dprint formatting                               |
| `pnpm lint-fix`         | Apply lint fixes and dprint formatting                               |
| `pnpm test --run`       | Run the unit and integration suite once                              |
| `pnpm test:examples`    | Run every Node example                                               |
| `pnpm test:browser`     | Run the Chromium suites for `browser-spike` and `tasks`              |
| `pnpm coverage`         | Run Vitest with V8 coverage                                          |
| `pnpm circular`         | Check package sources for circular imports                           |
| `pnpm codegen`          | Regenerate package barrel modules                                    |
| `pnpm docgen`           | Compile documentation and examples through the root TypeScript graph |
| `pnpm clean`            | Remove generated build artifacts                                     |

## Public API inventory

Every root package exports module namespaces. Every module is also available through its public subpath, such as
`@lucas-barake/effect-local/Replica`. `internal/*` is explicitly private.

The export surface supports several assembly levels. Most applications start with everyday domain and composition
modules. Feature and advanced services remain public for products that need direct control.

| Level                      | Modules                                                                                                                                                                  |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Everyday domain            | `Document`, `DocumentSet`, `Mutation`, `Projection`, `Query`, `ReplicaDefinition`, `Replica`, `CommandOutcome`, `Snapshot`, `Identity`, `ReplicaStatus`, `Backup`        |
| Durable runtime            | `SqlProjection`, `SqlReplica`, `BrowserReplica`, `BrowserSqlite`                                                                                                         |
| Reactive and test adapters | `ReplicaAtom`, `TestReplica`                                                                                                                                             |
| Optional features          | `PeerTransport`, `PeerSession`, `Presence`, `ReplicaWorkflow`, `TestPeer`, `FaultInjection`                                                                              |
| Advanced assembly          | `CommitPublisher`, `Compaction`, `Recovery`, `PeerSync`, `DurableRuntime`, `ReplicaClient`, `ReplicaOwner`, `SessionManager`                                             |
| Runtime internals          | `CommandExecutor`, `DocumentEntity`, `DocumentStore`, `EntityReplica`, `Migrations`, `ProjectionStore`, `QueryExecutor`, `ReplicaBootstrap`, `ReplicaGate`, `ReplicaRpc` |

Runtime internals are public for framework assembly and diagnostics. They are not required for the normal application
path shown in the cookbook.

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
