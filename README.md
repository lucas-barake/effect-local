# Effect Local

Effect Local is a local first data engine for Effect v4 applications. Automerge defines document state and
convergence. SQLite persists the canonical store and rebuildable query projections, in Node and in the browser
through OPFS. Effect Cluster serializes commands and stores their replies, Effect Workflow resumes long running
maintenance, and Effect Atom exposes reactive views. An optional RPC package synchronizes replicas through a
durable store and forward relay. Application reads and writes commit against the local replica; synchronization
never blocks a local commit.

> **Beta:** Effect Local targets Effect `4.0.0-beta.99` and Automerge `3.3.2`. Durable formats, worker protocols,
> and public APIs can still change. Read [Guarantees and limitations](#guarantees-and-limitations) before storing
> user data.

## Use Effect Local when

- The application must keep working offline and commit writes without a network.
- Several replicas (browser tabs, devices, a server process) hold the same documents and must converge.
- Reads need SQL queries over local data without making the query layer a second source of truth.
- Browser tabs must share one durable database through a SharedWorker and OPFS.
- Commands need durable idempotency: a retried command must not apply twice.
- The stack is Effect, or is being built on Effect services and Layers.

Do not use it as a managed backend, an identity or tenant system, an encryption protocol, or a replacement for
multi document transactions. Those remain application responsibilities; see
[Guarantees and limitations](#guarantees-and-limitations).

## Packages and installation

All five packages are ESM. Install only what the application surface uses.

```sh
# Core model: documents, mutations, projections, queries, Replica
pnpm add @lucas-barake/effect-local effect@4.0.0-beta.99 @automerge/automerge@3.3.2

# Durable Node replica
pnpm add @lucas-barake/effect-local-sql @effect/platform-node@4.0.0-beta.99 @effect/platform-node-shared@4.0.0-beta.99 @effect/sql-sqlite-node@4.0.0-beta.99

# Durable browser replica
pnpm add @lucas-barake/effect-local-sql @lucas-barake/effect-local-browser
pnpm add @effect/platform-browser@4.0.0-beta.99 @effect/sql-sqlite-wasm@4.0.0-beta.99 @effect/wa-sqlite@0.1.2

# Peer synchronization over Effect RPC
pnpm add @lucas-barake/effect-local-rpc

# Reactive state (optional)
pnpm add @effect/atom-react@4.0.0-beta.99

# Tests
pnpm add -D @lucas-barake/effect-local-test @effect/vitest@4.0.0-beta.99 vitest@4.1.10
pnpm add -D @effect/platform-node@4.0.0-beta.99 @effect/platform-node-shared@4.0.0-beta.99
```

| Package                              | Purpose                                                                                           |
| ------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `@lucas-barake/effect-local`         | Documents, mutations, projections, queries, command outcomes, backups, sync transport, `Replica`  |
| `@lucas-barake/effect-local-sql`     | SQLite persistence, durable execution, recovery, compaction, peer sync, relay outbox and receipts |
| `@lucas-barake/effect-local-browser` | Worker and RPC composition, OPFS ports, ownership, sessions, presence, Atom builders              |
| `@lucas-barake/effect-local-rpc`     | Durable relay protocol, injectable custody, policies, bounded server, client adapter              |
| `@lucas-barake/effect-local-test`    | Production shaped in memory replicas and deterministic bounded peer faults                        |

Every module is importable as a public subpath, for example `@lucas-barake/effect-local/Replica` and
`@lucas-barake/effect-local-sql/SqlReplica`. Paths under `internal/*` are private and unsupported.

## Quick start

This quick start builds one task application end to end on Node: a document, a mutation with a declared domain
error, a projection, a query, a durable engine, and commands with their failure handling. The complete sources
live under [`packages/local-sql/examples/`](packages/local-sql/examples/), compiled by the repository typecheck,
so an API change breaks CI instead of silently staling this guide.

Define the document and its mutations. A mutation handler runs synchronously inside an Automerge change and can
reject with a declared tagged error.

```ts
// domain.ts
import * as Document from "@lucas-barake/effect-local/Document"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

export const TaskDocument = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 1
})

export class TitleEmpty extends Schema.TaggedErrorClass<TitleEmpty>()("TitleEmpty", {}) {}

export const RenameTask = Mutation.make("RenameTask", {
  document: TaskDocument,
  payload: { title: Schema.String },
  error: TitleEmpty
})

export const SetTaskCompleted = Mutation.make("SetTaskCompleted", {
  document: TaskDocument,
  payload: { completed: Schema.Boolean }
})
```

Define a projection (a deterministic mapping from document snapshots to rows) and a query over it, then collect
everything in one replica definition.

```ts
// domain.ts (continued)
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"

export const TaskRow = Schema.Struct({
  sourceDocumentId: Identity.DocumentId,
  title: Schema.String,
  completed: Schema.Boolean,
  updatedAt: Schema.Number
})

export class ListTasksError extends Schema.TaggedErrorClass<ListTasksError>(
  "@lucas-barake/effect-local/examples/ListTasksError"
)("ListTasksError", {
  reason: Schema.Literals(["StorageUnavailable", "StorageCorrupt"])
}) {}

export const TaskList = Projection.make("TaskList", {
  document: TaskDocument,
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
  error: ListTasksError,
  dependsOn: [TaskList]
})

export const definition = ReplicaDefinition.make({
  name: "tasks",
  documents: DocumentSet.make(TaskDocument),
  mutations: [RenameTask, SetTaskCompleted],
  projections: [TaskList],
  queries: [ListTasks]
})
```

Bind the projection to a SQLite table and implement the handlers. Mutation handlers mutate a draft or return
their declared error. Query handlers are ordinary Effects and may use `SqlClient`; map its infrastructure
failures to a portable declared error rather than turning them into defects.

```ts
// domain.ts (continued)
import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

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

const ListTasksSql = SqlSchema.findAll({
  Request: ListTasks.payloadSchema,
  Result: Schema.Struct({
    ...TaskRow.fields,
    completed: Schema.BooleanFromBit
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

export const DomainLive = Layer.mergeAll(
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
  }),
  ListTasks.toLayer((payload) =>
    ListTasksSql(payload).pipe(
      Effect.catchTags({
        SqlError: () => Effect.fail(new ListTasksError({ reason: "StorageUnavailable" })),
        SchemaError: () => Effect.fail(new ListTasksError({ reason: "StorageCorrupt" }))
      })
    )
  )
)
```

Compose the durable engine. `SqlReplica.layerWithBindings` requires exactly one SQL binding per declared
projection, every mutation and query handler Layer, a `ReplicaLimits` value, Effect's `Crypto`, and a
`SqlClient`. The full limits object is defined in
[`packages/local-sql/examples/domain.ts`](packages/local-sql/examples/domain.ts) and copied verbatim from
`TestReplica.defaultLimits`, which is a starting point rather than production capacity planning.

```ts
// node.ts
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import { definition, DomainLive, limits, TaskListSql } from "./domain.js"

const DatabaseLive = SqliteClient.layer({ filename: "tasks.sqlite" })

const DependenciesLive = Layer.mergeAll(
  DatabaseLive,
  NodeCrypto.layer,
  DomainLive.pipe(Layer.provide(DatabaseLive)),
  ReplicaLimits.layer(limits)
)

export const EngineLive = SqlReplica.layerWithBindings(definition, {
  projections: [TaskListSql]
}).pipe(Layer.provideMerge(DependenciesLive))
```

Use the `Replica` service. Commands return their committed value and report every failure in the Effect error
channel: the mutation's declared error unwrapped, or `ReplicaError`. After an ambiguous outcome, look the command
ID up instead of retrying blind.

```ts
// quick-start.ts
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import { ListTasks, RenameTask, SetTaskCompleted, TaskDocument } from "./domain.js"
import { EngineLive } from "./node.js"

const program = Effect.gen(function*() {
  const replica = yield* Replica.Replica

  const now = Date.now()
  const documentId = yield* replica.create(TaskDocument, {
    commandId: yield* Identity.makeCommandId,
    value: { title: "Write the README", completed: false, createdAt: now, updatedAt: now }
  })

  // The declared domain error arrives unwrapped and is caught by its own tag.
  // `renamed` is "rejected" here, because the handler trims the title and fails on empty.
  const renamed = yield* replica.mutate(RenameTask, {
    commandId: yield* Identity.makeCommandId,
    documentId,
    payload: { title: "   " }
  }).pipe(
    Effect.catchTag("TitleEmpty", () => Effect.succeed("rejected"))
  )

  yield* replica.mutate(SetTaskCompleted, {
    commandId: yield* Identity.makeCommandId,
    documentId,
    payload: { completed: true }
  })

  const snapshot = yield* replica.get(TaskDocument, documentId)
  const tasks = yield* replica.query(ListTasks, { search: "readme" })

  // `CommandOutcomeUnknown` is never retried blind: look the command id up instead.
  const commandId = yield* Identity.makeCommandId
  const settled = yield* replica.mutate(RenameTask, {
    commandId,
    documentId,
    payload: { title: "Ship it" }
  }).pipe(
    Effect.catchReason(
      "ReplicaError",
      "CommandOutcomeUnknown",
      (reason) =>
        replica.lookupMutation(RenameTask, reason.commandId).pipe(
          Effect.flatMap(CommandOutcome.committedOrFail)
        )
    )
  )

  const outcome = yield* replica.lookupMutation(RenameTask, commandId)
  const described = CommandOutcome.match(outcome, {
    onRejected: ({ error }) => `Rejected with ${error._tag}`,
    onCommitted: () => "Committed locally",
    onUnknown: () => "Outcome unknown"
  })

  return { renamed, snapshot, tasks, settled, described }
})

export const main = program.pipe(
  Effect.provide(EngineLive),
  Effect.scoped
)
```

Running `main` with `Effect.runPromise` creates the database, commits the commands locally, and returns the
collected results. `described` is `"Committed locally"`: the mutation committed, so the lookup resolves to
`DurablyCommittedLocal` and `committedOrFail` returns its value.

From here, pick the composition that matches the deployment:

- [Durable Node composition](#durable-node-composition) for server and CLI replicas.
- [Browser composition](#browser-composition) for tabs, SharedWorker ownership, and OPFS.
- [Reactive state](#reactive-state) for Effect Atom and React.
- [Peer synchronization](#peer-synchronization) and [RPC relay](#rpc-relay) for connecting replicas.

## Core concepts

One replica is one local database with replaceable views and execution systems around it. Automerge holds the
canonical document history. SQLite is the commit boundary. Projections and Atom values are rebuildable views.
Commands are the only way in.

| Term                | Meaning                                                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Replica definition  | The schema checked blueprint of one application model: documents, mutations, projections, queries, and its `hash`      |
| Document definition | The inert name, schema, version, and migration chain returned by `Document.make`                                       |
| Document instance   | One `DocumentId` with its Automerge state and causal history. Model one aggregate per document                         |
| Snapshot            | The decoded read result for one instance: identity, version, heads, tombstone, projection state, value                 |
| Heads               | The frontier change hashes identifying a point in Automerge history                                                    |
| Command             | One create, mutate, or delete request carrying a caller supplied command ID for idempotency                            |
| Command outcome     | What a lookup answers: `DurablyCommittedLocal`, `Rejected`, or `OutcomeUnknown`                                        |
| Projection          | A deterministic mapping from canonical snapshots to a rebuildable SQL read model                                       |
| Local commit        | The SQLite transaction that commits canonical changes, projections, the command receipt, and the stored reply together |

The composition ladder is: definitions to handler Layers to SQL bindings to `SqlReplica.layerWithBindings` to
the `Replica` service. Definitions are inert values. Installing a definition does not install its handler.

## Documents and schema evolution

```ts
export const make = <Name extends string, S extends DocumentSchema>(
  name: Name,
  options: {
    readonly schema: S
    readonly version: number
    readonly migrations?: ReadonlyArray<AnyMigration>
  }
): Document<Name, S>
```

`Document.make` requires a nonempty name, a positive safe integer version, and a schema whose encoded shape is
Automerge compatible: scalars, arrays, plain records, `Date`, `Uint8Array`, counters, and immutable strings.
`Document.isAutomergeValue` is the exact check. Names and versions are durable protocol identities; renaming a
document changes the protocol.

When the encoded meaning changes, increment `version` and register one `Document.migration` per prior version
that may still be stored. Each step declares its source version, the schema that decodes stored values at that
version, and a pure `migrate` function. `Document.make` rejects duplicate, gapped, or out of range chains at
construction.

```ts
// migrations.ts, compiled under packages/local-sql/examples/
import * as Document from "@lucas-barake/effect-local/Document"
import * as Schema from "effect/Schema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

const TaskV1 = Schema.Struct({
  title: Title,
  completed: Schema.Boolean,
  createdAt: Schema.Number,
  updatedAt: Schema.Number
})

export const TaskDocumentV2 = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    priority: Schema.Int,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 2,
  migrations: [
    Document.migration({
      from: 1,
      schema: TaskV1,
      migrate: (value) => ({ ...value, priority: 0 })
    })
  ]
})
```

What happens with stored data:

- Recovery decodes every stored value with the decoder matching its stored version and applies the chain, so a
  snapshot always carries the current type. A stored version with no covering chain fails with
  `ReplicaError` / `UnsupportedDocumentVersion`.
- At startup the replica materializes each stale document by appending a normal Automerge change that rewrites
  the value to the current encoding. History is never rewritten.
- A document whose history cannot be recovered is quarantined and skipped, never silently reinterpreted.

Opening a replica under a changed definition. `ReplicaDefinition.make` computes `definition.hash` from names,
versions, encoded schemas, and query dependencies. Migrations are excluded, so registering one is not a protocol
change. Bootstrap compares the stored hash with the current one:

- Hash matches: open normally.
- Hash differs: open only when every stored document type still exists and every stored version reaches the
  current version through its migration chain. The stored hash is updated in the same bootstrap transaction and
  the startup evolution pass migrates stale documents and rebuilds changed projections before the replica
  serves requests.
- Otherwise bootstrap fails with `ReplicaError` / `ProtocolMismatch` and leaves metadata untouched, so the
  previous build still opens.

Peer synchronization, browser sessions, and restore still require an exact hash match. Two peers on different
definitions never exchange changes, and an old backup can require a matching build for restore. Portable
document import ([Backup and restore](#backup-and-restore)) is the version tolerant boundary.

The on disk layout has its own gate. `storage_format_version` describes the library's SQL format, not the
application schema, and no migration path across it exists. A mismatch fails with
`ReplicaError` / `UnsupportedStorageFormatVersion` carrying `observedVersion` and `supportedVersion`, and the
database is left exactly as the other build wrote it.

See [docs/schema-evolution.md](docs/schema-evolution.md) for the full rules, including mutation receipt
versioning.

## Mutations and domain errors

```ts
export const make = <
  Name extends string,
  D extends Document.Any,
  P extends SchemaInput.Input = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends TaggedError.Schema = typeof Schema.Never
>(
  name: Name,
  options: {
    readonly document: D
    readonly version?: number
    readonly payload?: SchemaInput.Valid<P>
    readonly success?: A
    readonly error?: E
  }
): Mutation<Name, D, SchemaInput.Wire<P>, A, E>
```

Defaults are version `1`, payload `Schema.Void`, success `Schema.Void`, and error `Schema.Never`. The payload
accepts a schema or `Schema.Struct` fields directly. Names must not start with `$`, which is reserved for
operation sentinels.

The handler runs synchronously inside one Automerge change and one durable command transaction:

```ts
export type Handler<D extends Document.Any, P, A, E,> = (
  options: {
    readonly draft: Draft<D> // mutable encoded document value
    readonly payload: P // decoded payload
    readonly current: D["schema"]["Type"] // decoded current value
  }
) => [E] extends [never] ? SuccessResult<A> : Result.Result<A, E>
```

A mutation whose error is `Schema.Never` returns its success value directly (or `undefined` for void). A
mutation with a declared error returns `Result`: `Result.fail(new TitleEmpty())` to reject, `Result.void` or a
value to accept. Declared errors must be schema backed tagged error classes built with
`Schema.TaggedErrorClass`, or a `Schema.Union` of them when one mutation rejects for several reasons. This keeps
`Effect.catchTag`, RPC encoding, and durable receipts on one discriminated model.

Implementations are Effect services generated per definition. `toLayer` accepts a handler directly or an Effect
that constructs one, preserving that Effect's requirements in the Layer.

```ts
export const RenameLive = RenameTask.toLayer(({ draft, payload }) => {
  const title = payload.title.trim()
  if (title.length === 0) return Result.fail(new TitleEmpty())
  draft.title = title
  draft.updatedAt = Date.now()
  return Result.void
})
```

At the call site the declared error arrives unwrapped in the error channel, next to `ReplicaError`:

```ts
replica.mutate(RenameTask, { commandId, documentId, payload: { title: "  " } }).pipe(
  Effect.catchTag("TitleEmpty", (error) => Effect.succeed(`Rejected: ${error._tag}`))
)
```

Keep a mutation's payload, success, and error schemas decodable for as long as one of its receipts may be
retried. A behavior change with incompatible input or output needs a new version or a new name.

## Projections and queries

```ts
export const make = <Name extends string, D extends Document.Any, R extends Document.WireSchema>(
  name: Name,
  options: {
    readonly document: D
    readonly version: number
    readonly Row: R
    readonly key: (row: R["Type"]) => string
    readonly project: (snapshot: Snapshot.FromDocument<D>) => ReadonlyArray<R["Type"]>
  }
): Projection<Name, D, R>
```

A projection is a deterministic function of a canonical snapshot. `Projection.make` validates only the name and
version; determinism is the caller's contract. Every projected row must carry `sourceDocumentId` equal to the
snapshot's document ID, keys must be unique per snapshot, and every row is decoded through `Row`. A violation
fails the commit with `ReplicaError` / `ProjectionBlocked`.

The SQL binding is `SqlProjection.make(projection, { table, migrations, deleteByDocument, insert })` from
`@lucas-barake/effect-local-sql/SqlProjection`, shown in the [quick start](#quick-start). It requires a nonempty
table name, at least one migration, and unique positive integer migration IDs. The table is a rebuildable read
model, durable only so reads stay fast after restart. A SQLite index is a separate planner structure, not a
binding.

```ts
export const make = <
  Name extends string,
  P extends SchemaInput.Input = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends TaggedError.Schema = typeof Schema.Never,
  Dependencies extends ReadonlyArray<Projection.Any> = readonly []
>(
  name: Name,
  options: {
    readonly payload?: SchemaInput.Valid<P>
    readonly version?: number
    readonly success?: A
    readonly error?: E
    readonly dependsOn: Dependencies
  }
): Query<Name, SchemaInput.Wire<P>, A, E, Dependencies>
```

A query names its payload, success, and optional declared error, and declares the projections that drive
readiness and reactive invalidation through `dependsOn` (required, unique by projection name). Handlers are
Effects, so they can run SQL through `SqlSchema`, as in the quick start. `toLayer` captures the Layer build
context and provides it to every call.

Readiness is enforced per execution. A query fails with `ReplicaError` / `ProjectionBlocked` while any declared
projection is not `Ready`: the registry entry is rebuilding, a source document is durably blocked, or a per
document projection row is not ready. Readers never observe a partially rebuilt table.

Upgrading a projection. A projection's name, row schema, and version are part of `definition.hash`, so changing
any of them changes the hash. The upgrade path is startup reconciliation, not an online swap:

1. Change the binding: bump the projection `version`, change `Row`, or point the binding at a new `table`.
2. At startup the projection registry reconciles every binding: on any table, version, or checksum mismatch it
   clears the stale rows and marks the entry `Rebuilding`.
3. Before the replica serves requests, the startup evolution pass re-projects every document of that type from
   verified canonical snapshots and marks the projection `Ready`.
4. Queries on that projection refuse with `ProjectionBlocked` until then, so no reader sees a partial table.

Because reconciliation happens while the Layer graph builds, readers never hold a `Replica` during the rebuild.
There is no shadow table and no atomic online publish; the rebuild is complete before the replica opens. A
projection only change still changes `definition.hash`, so peers and restore archives must run the same build,
and a rolled back deployment reopens the replica as long as its own documents remain decodable.

## Using `Replica`

`Replica` is the application capability, an Effect service consumed from the environment. Commands return the
committed value. Every failure is typed in the error channel.

```ts
export class Replica extends Context.Service<Replica, {
  readonly create: <D extends Document.Any,>(
    document: D,
    options: { readonly commandId: Identity.CommandId; readonly value: D["schema"]["Type"] }
  ) => Effect.Effect<Identity.DocumentId, ReplicaError.ReplicaError>
  readonly get: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Snapshot.FromDocument<D>, ReplicaError.ReplicaError>
  readonly mutate: <M extends Mutation.Any,>(
    mutation: M,
    options: { readonly commandId: Identity.CommandId; readonly documentId: Identity.DocumentId } & PayloadOf<M>
  ) => Effect.Effect<M["successSchema"]["Type"], M["errorSchema"]["Type"] | ReplicaError.ReplicaError>
  readonly delete: <D extends Document.Any,>(
    document: D,
    options: { readonly commandId: Identity.CommandId; readonly documentId: Identity.DocumentId }
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly query: <Q extends Query.Any,>(
    query: Q,
    ...payload: [Q["payloadSchema"]["Type"]] extends [void] ? readonly [] : readonly [Q["payloadSchema"]["Type"]]
  ) => Effect.Effect<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
  readonly lookupMutation: <M extends Mutation.Any,>(
    mutation: M,
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandOutcome<M["successSchema"]["Type"], M["errorSchema"]["Type"]>, ReplicaError.ReplicaError>
  readonly lookupCreate: <D extends Document.Any,>(
    document: D,
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
  readonly lookupDelete: <D extends Document.Any,>(
    document: D,
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandOutcome<void>, ReplicaError.ReplicaError>
  readonly flush: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: Stream.Stream<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
  readonly exportBackup: (options: Backup.ExportOptions) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly restoreBackup: <R,>(options: Backup.RestoreOptions<R>) => Effect.Effect<void, ReplicaError.ReplicaError, R>
  readonly exportDocument: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Backup.ExportedDocument<D["schema"]["Encoded"]>, ReplicaError.ReplicaError>
  readonly importDocument: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly value: Backup.ExportedDocument<D["schema"]["Encoded"]>
    }
  ) => Effect.Effect<Identity.DocumentId, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local/Replica") {}
```

`PayloadOf<M>` above stands for the source level conditional: the `payload` field is required only when the
mutation's payload type is not `void`.

Behavior notes:

- `create` derives the document ID from the command ID (`Identity.documentIdFromCommandId`), so an identical
  retry targets the same document.
- `get` returns the decoded snapshot: `value`, `version`, `heads`, `tombstone`, and per projection readiness.
  A missing document fails with `ReplicaError` / `DocumentNotFound`.
- `delete` commits a durable tombstone. Projections drop the document's rows.
- `flush` publishes pending commit invalidations. Call it before an intentional shutdown when subscribers
  should observe the latest commits.
- `status` is a long lived latest wins stream. Consume it with a bounded operator such as `Stream.take` or
  `Stream.runHead`, never `Stream.runCollect`.

### Command outcomes and idempotency

Within one replica incarnation, a command ID is an idempotency key:

- Repeating an identical command returns the durable result. Nothing applies twice.
- Reusing a command ID for different input fails with `ReplicaError` / `CommandIdConflict`.
- A lookup under the wrong operation type or mutation fails with `ReplicaError` / `ReceiptOperationMismatch`.

Commands report three endings: success, the declared domain error, or `ReplicaError`. The ambiguous case is
`ReplicaError` with reason `CommandOutcomeUnknown { commandId, cause }`: the command may or may not have
committed. It is never retryable. Keep the command ID and call the matching lookup method.

Lookups answer with a `CommandOutcome` value, because "what happened to this id" has three answers:

```ts
export type CommandOutcome<A, E = never,> =
  | { readonly _tag: "Rejected"; readonly commandId: Identity.CommandId; readonly error: E }
  | { readonly _tag: "DurablyCommittedLocal"; readonly commandId: Identity.CommandId; readonly value: A }
  | { readonly _tag: "OutcomeUnknown"; readonly commandId: Identity.CommandId }
```

```ts
export const match = <A, E, B>(
  self: CommandOutcome<A, E>,
  handlers: {
    readonly onRejected: (outcome: Rejected<E>) => B
    readonly onCommitted: (outcome: DurablyCommittedLocal<A>) => B
    readonly onUnknown: (outcome: OutcomeUnknown) => B
  }
): B

export const committedOrFail: <A, E>(
  self: CommandOutcome<A, E>,
  cause?: unknown
) => Effect.Effect<A, E | ReplicaError.ReplicaError>

export const toOutcome: <A, E, R>(
  commandId: Identity.CommandId,
  effect: Effect.Effect<A, E, R>
) => Effect.Effect<CommandOutcome<A, Exclude<E, ReplicaError.ReplicaError>>, ReplicaError.ReplicaError, R>
```

Every `match` handler returns the same type `B`, so the result stays one value:

```ts
const described: string = CommandOutcome.match(outcome, {
  onRejected: ({ error }) => `Rejected with ${error._tag}`,
  onCommitted: () => "Committed locally",
  onUnknown: () => "Outcome unknown"
})
```

`committedOrFail` projects a lookup into the same channels a command uses: the value on commit, the declared
error on rejection, and `CommandOutcomeUnknown` when no receipt exists. `toOutcome` is the inverse for
boundaries that must carry an outcome as a value.

Restore can advance the replica incarnation. Receipt evidence from the archived incarnation then reports
`OutcomeUnknown` through the current permit, and compaction eventually reclaims it. Application policy decides
whether a later request is a new logical command, with a new command ID.

The distinction that matters:

| Situation                                           | Action                                                         |
| --------------------------------------------------- | -------------------------------------------------------------- |
| Command returned success or a declared error        | Done. The result is durable                                    |
| `CommandOutcomeUnknown`                             | Look up the same command ID with the matching `lookup*` method |
| Lookup answers `OutcomeUnknown`                     | Application policy decides; a new attempt is a NEW command ID  |
| Lookup answers `Rejected` / `DurablyCommittedLocal` | The recorded answer is final                                   |
| Deliberate new operation                            | Mint a fresh command ID with `Identity.makeCommandId`          |

## Durable Node composition

`SqlReplica.layerWithBindings(definition, { projections })` is the standard constructor, shown in the
[quick start](#quick-start). It requires the platform `SqlClient`, `Crypto`, a `ReplicaLimits` Layer, every
generated mutation and query handler Layer, and exactly one `SqlProjection` binding per declared projection.
Passing a different set of bindings throws a `TypeError` at construction.

Choose the constructor by assembly level:

| Constructor              | Use                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------- |
| `layerWithBindings`      | Application default. Installs one declared SQL binding per projection                 |
| `layerRelayWithBindings` | Same, with relay receipt support for the store and forward client                     |
| `layer` / `layerRelay`   | Lower level composition that receives projection binding services from another Layer  |
| `layerFromServices`      | Framework assembly from already constructed durable services. Provides only `Replica` |

The four durable constructors provide `Replica`, `CommitPublisher`, `PeerSync`, `ReplicaEvolution`, `ReplicaGate`,
the compaction and history rewrite workflow services, and Effect Cluster `Sharding` over the same SQL storage. The
relay variants additionally require `PeerRelayReceiptLimits`. Building one of those Layers runs bootstrap and the
startup evolution pass, so its `Replica` opens onto migrated documents and ready projections. `layerFromServices`
only assembles `Replica` from services the caller has already constructed.

An optional `health` option tunes `ReplicaHealth` (`{ sampleInterval: Duration.Input }`, default
`"1 second"`), which backs the `Replica.status` stream. The SQL composition reports:

| Status              | Source                                                                                       |
| ------------------- | -------------------------------------------------------------------------------------------- |
| `Ready`             | `pendingCommands` is the unpublished commit outbox depth, drained by `replica.flush`         |
| `Restoring`         | Bytes consumed while a backup archive is being ingested                                      |
| `ReadOnly`          | A restore is installing under the writer claim, or another writer generation fenced this one |
| `ProjectionBlocked` | A document is durably blocked for a declared projection                                      |
| `Degraded`          | Storage was transiently unavailable on the last sample                                       |
| `Failed`            | Storage is corrupt, the metadata singleton is missing, or health sampling stopped            |

`Starting` exists in the public `ReplicaStatus` union for other compositions; the SQL composition never emits
it, because bootstrap completes before any consumer can hold a `Replica`.

## Browser composition

The durable browser topology has three roles. Page code talks to `Replica` through RPC. A SharedWorker elected
by the ownership coordinator owns the engine. A dedicated worker owns the SQLite WASM connection and OPFS file.
Chromium cannot spawn a `Worker` inside a `SharedWorkerGlobalScope`, so a page provisions the database worker
and transfers its `MessagePort` to the owner.

The page side wires `OwnershipCoordinator.layerTab`, which provides Effect's `WorkerPlatform` and `Spawner`,
plus `BrowserReplica.layerWithReactivity`:

```ts
// replica-client.ts, abridged from the checked example under
// packages/local-browser/test-browser/ownership/src/
import { BrowserCrypto } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as Layer from "effect/Layer"
import { Atom } from "effect/unstable/reactivity"
import { definition } from "./domain.js"

const OwnershipLive = OwnershipCoordinator.layerTab({
  name: "effect-local-tasks",
  sharedWorker: () =>
    new SharedWorker(new URL("./replica.shared-worker.ts", import.meta.url), {
      name: "effect-local-tasks",
      type: "module"
    }),
  databaseWorker: () =>
    new Worker(new URL("./opfs.worker.ts", import.meta.url), {
      name: "effect-local-tasks-opfs",
      type: "module"
    })
})

const ReplicaLive = Layer.merge(
  BrowserReplica.layerWithReactivity(definition).pipe(
    Layer.provide(Layer.merge(OwnershipLive, BrowserCrypto.layer))
  ),
  BrowserCrypto.layer
)

export const runtime = Atom.runtime(ReplicaLive)
```

`BrowserReplica` offers `layer`, `layerWith`, `layerWithReactivity`, and `layerWithReactivityOptions`. Each
accepts optional `ReplicaClient.Options`:

```ts
export interface Options {
  readonly sessionTimeout?: Duration.Input | undefined // default 10 seconds
  readonly operationTimeout?: Duration.Input | undefined // default 30 seconds
  readonly closeSessionOnPageHide?: boolean | undefined // default true
}
```

The session timeout bounds open, renewal, and close. The operation timeout bounds each unary RPC attempt,
including command receipt lookups. Restore uses one end to end deadline across the whole protocol. Status,
invalidation, and backup export streams are intentionally unbounded. The owner side session lease is 60 seconds
(`SessionManager.leaseDurationMillis`) and the client renews it automatically.

The SharedWorker entry delegates to the coordinator:

```ts
// replica.shared-worker.ts
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"

OwnershipCoordinator.runSharedWorker(() =>
  import("./replica.shared-worker-runtime.ts").then((module) => module.options)
)
```

The runtime module builds the engine over the transferred database port and hands
`OwnershipCoordinator.SharedWorkerOptions` back: `name`, `definition`, and `engine`, a function from
`MessagePort` to a `ManagedRuntime` containing `SqlReplica.layerWithBindings`, `SessionManager.layer`, the
domain Layers, `ReplicaLimits`, `BrowserCrypto.layer`, and `BrowserSqlite.layerMessagePort(databasePort)`.
Connects arriving while the engine module loads are buffered; a load failure is reported to every tab.

The database worker entry holds a Web Lock for the database lifetime, so a replacement provider waits for the
lock instead of opening OPFS concurrently with a slow prior owner:

```ts
// opfs.worker.ts
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"

OwnershipCoordinator.runDatabaseWorker({ name: "effect-local-tasks" })
```

The complete checked application is
[`packages/local-browser/test-browser/ownership/src/`](packages/local-browser/test-browser/ownership/src/):
domain model, engine runtime, tab client, and React views, compiled and exercised by the browser test suite.

`OwnershipCoordinator.layerSharedWorker` exposes the same state machine as a service for custom worker entry
points: election of one provisioning tab, expiring provision nonces, database round trip health verification,
takeover handoff of every attached tab, and session cleanup. `ReplicaOwner.layerWorker(definition)` serves the
page protocol inside the engine runtime.

## Reactive state

Atom state is a cache over `Replica`, never a durable authority. Build one shared runtime with `Atom.runtime`
so every atom builds under one `Layer.MemoMap`:

```ts
// atoms.ts
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import { Atom } from "effect/unstable/reactivity"
import { ListTasks, RenameTask, TaskDocument, TaskList } from "./domain.js"
import { runtime } from "./replica-client.js"

export const tasks = ReplicaAtom.queryFamily(runtime, ListTasks)
export const task = ReplicaAtom.documentFamily(runtime, TaskDocument)
export const renameTask = ReplicaAtom.mutation(runtime, RenameTask)
export const replicaStatus = ReplicaAtom.status(runtime)

const allTasks = tasks({ search: "" })
```

- `queryFamily(runtime, query)` canonicalizes the schema encoded payload for stable family identity and
  invalidates on every declared projection and source document key.
- `documentFamily(runtime, document)` invalidates on the document key.
- `mutation(runtime, mutation)` is a `runtime.fn` that invalidates the mutation's document key.
- `status(runtime)` wraps the status stream and resubscribes only after a session replacement.

For commands with custom invalidation needs, use `runtime.fn` directly with `reactivityKeys`:

```ts
// atoms.ts (continued)
export const createTask = runtime.fn<{ readonly title: string }>()(
  ({ title }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const now = Date.now()
      return yield* replica.create(TaskDocument, {
        commandId: yield* Identity.makeCommandId,
        value: { title, completed: false, createdAt: now, updatedAt: now }
      })
    }),
  { concurrent: true, reactivityKeys: [TaskList.name] }
)
```

React applications provide an `AtomRegistry` through `@effect/atom-react` and consume atoms with
`useAtomValue`, `useAtomSet`, and `useAtomRefresh`. The ownership example's
[`app.tsx`](packages/local-browser/test-browser/ownership/src/app.tsx) shows the wiring.

## Backup and restore

Canonical backups are bounded NDJSON archives with checked record schemas, checksums, and lineage on every
document. Export is a stream so applications keep backpressure. Restore requires an `installationId` identifying
the restore attempt.

```ts
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { definition, TaskDocument } from "./domain.js"

const bytes = Replica.Replica.use((replica) =>
  Stream.mkUint8Array(replica.exportBackup({ maxBytes: 32 * 1024 * 1024 }))
)

const restore = (archive: Uint8Array) =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    yield* replica.restoreBackup({
      source: Stream.make(archive),
      mode: "replace",
      maxBytes: 32 * 1024 * 1024,
      expectedDefinitionHash: definition.hash,
      installationId: yield* Identity.makeBackupInstallationId
    })
  })
```

```ts
export interface ExportOptions {
  readonly maxBytes: number
}

export interface RestoreOptions<R,> {
  readonly source: Stream.Stream<Uint8Array, ReplicaError.ReplicaError, R>
  readonly mode: "clone" | "replace"
  readonly maxBytes: number
  readonly expectedDefinitionHash: string
  readonly installationId: Identity.BackupInstallationId
}
```

- `replace` stages and validates the whole archive, then installs it under an exclusive writer claim. It
  adopts the archive's replica identity and advances the incarnation, which fences every stale operation. Use
  it when the source replica has been retired.
- `clone` creates a fresh local replica identity. Use it to duplicate data onto another device.
- Both modes rebuild every projection from installed canonical state inside the restore transaction. A
  projection failure rolls the replacement back.
- `expectedDefinitionHash` must equal the archive's hash. An old backup can require a matching application
  build after definition evolution.

Restore failures are `ReplicaError` reasons: `BackupInvalid` (envelope, checksum, bounds, definition, or
foreign key validation), `BackupTooLarge { limit, observed }`, `RestoreBusy` (another restore holds the claim),
`RestoreFailed` (the install itself), and `ProtocolMismatch` (hash mismatch).

Portable document transfer bypasses archive compatibility. `exportDocument` returns the schema encoded value,
and `importDocument` validates the document name and schema version, then writes a fresh causal history as a new
local document under a caller supplied command ID.

```ts
const duplicateDocument = (documentId: Identity.DocumentId) =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const value = yield* replica.exportDocument(TaskDocument, documentId)
    return yield* replica.importDocument(TaskDocument, {
      commandId: yield* Identity.makeCommandId,
      value
    })
  })
```

Browser storage can be evicted while it stays best effort, so a complete product exposes export, restore,
duplicate, and deletion controls regardless of OPFS persistence. In the browser the restore runs over a
dedicated `MessagePort` protocol with one outstanding pull credit, bounded staging, and a separate restore
admission limit (`maxActiveRestores`, `maxRestoresPerSession`, `maxRestoreMillis`, `maxRestorePullMillis`,
`maxRestoreCoalesceMillis`, `maxRestoreErrorBytes`). The page uses the same `replica.restoreBackup` call; the
transport protocol is internal.

## Peer synchronization

Two replicas converge by exchanging Automerge messages for selected whole documents. The application supplies
the transport; the engine supplies durable sequencing, replay, and receipts.

```ts
export interface Connection {
  readonly peerId: Identity.PeerId
  readonly relayPeerId: Identity.PeerId
  readonly capabilities: Capabilities
  readonly receive: Stream.Stream<AcknowledgedDelivery, ReplicaError.ReplicaError>
  readonly send: (message: Uint8Array) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly close: Effect.Effect<void>
}

export class PeerTransport extends Context.Service<PeerTransport, {
  readonly capabilities: Capabilities
  readonly connect: (options: ConnectOptions) => Effect.Effect<Connection, ReplicaError.ReplicaError, Scope.Scope>
}>() {}
```

The adapter returns one scoped `Connection` per connection epoch. Its scope must release every transport
resource. `capabilities.lineageAware` describes the REMOTE peer: set it only when that peer compares document
lineage before merging, because the send path refuses to emit a rewritten document to a peer that does not.

`PeerSession` binds one connection to selected whole documents and drives both directions:

```ts
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import { TaskDocument } from "./domain.js"

const syncTask = (peerId: Identity.PeerId, documentId: Identity.DocumentId) =>
  Effect.scoped(Effect.gen(function*() {
    const session = yield* PeerSession.make({
      peerId,
      documents: [{ document: TaskDocument, documentId }]
    })
    yield* session.flush
  }))
```

| Constructor                  | Behavior                                                                           |
| ---------------------------- | ---------------------------------------------------------------------------------- |
| `PeerSession.make`           | Owns transport connect and the receive loop for the scope                          |
| `PeerSession.makeSupervised` | Adds `awaitDisconnect`, the terminal replica failure without a commit subscription |
| `PeerSession.makeLive`       | Subscribes to commit events and flushes automatically on local commits             |

The session exposes `peerId`, `connectionEpoch`, `markDirty`, `flush`, `observedByPeer(documentId)`, and
`durableConfirmation(documentId)`. `observedByPeer` reports Automerge sync state only: the peer has all current
local changes. It does not prove remote storage durability, which is why `durableConfirmation` currently
returns only `false`.

Semantics to build on:

- Convergence is eventual. Two replicas converge after receiving the same valid change set, and temporary
  divergence is normal operation.
- Exact retransmission is safe. Persistent receive identity and Automerge merge semantics deduplicate it.
  Delivery is at least once at the transport, never exactly once.
- A reconnect creates a new connection epoch, fresh Automerge sync state, and a fresh outbox from canonical
  history. Never resume an old connection scope.
- Malformed peer traffic is terminal for the session, not retryable in place. Envelope, digest, and protocol
  validation failures raise `ReplicaError` / `ProtocolMismatch`, which fails the supervised session. Reconnect
  with a fresh scope; the durable outbox replays what the peer still needs.
- Selection is a nonempty set of unique whole documents. Subtree synchronization is unsupported.

Presence is a separate best effort channel with expiry, for cursors and online state:

```ts
import * as Presence from "@lucas-barake/effect-local-browser/Presence"
import * as Schema from "effect/Schema"

const Cursor = Schema.Struct({ documentId: Schema.String, offset: Schema.Number })

const presence = Presence.make(Cursor, { timeToLive: "15 seconds" })
```

`Presence.make` returns an Effect building `receive`, scoped `publish`, `remove`, and `values`. Presence
identity is the authenticated transport peer. Entries expire lazily on read, concurrent writes resolve by
arrival order at this process, and a stale write is dropped silently. Presence is never durable and must never
authorize an operation.

`PeerSync` is the lower level durable protocol service (`open`, `reset`, `generate`, `receive`, `enqueue`,
`pending`, `markSent`, `invalidateDocument`). Most applications must not call it directly; `PeerSession` owns
sequencing, lifecycle, and transport cleanup. See [docs/sync.md](docs/sync.md) for the protocol detail.

## RPC relay

The RPC package synchronizes independent canonical replicas through one durable store and forward protocol,
`PeerRpc` version `1`. Its procedures are `Open` (a stream that starts with `Opened` and then emits
`StoredMessage` deliveries), `Push`, `Acknowledge`, and `Reject`. There is no direct in memory RPC topology.

Delivery is at least once. A successful `Push` means the relay's SQL store committed the complete envelope
before replying. A lost response, expired claim, interrupted acknowledgement, or reconnect can redeliver a
message, and the recipient's durable receipts make the duplicate safe within the negotiated retention window.

The relay is a cluster of `RelayInbox` entities, one per recipient device, each the sole writer of its inbox.
The application supplies authentication, authorization, limits, a socket, serialization, and a `Sharding`; the
relay never builds one, so single process, one runner over SQL, and many sharded runners are all deployment
choices. `RelayServer.layerHandlers(options)` builds the front door for an existing RPC server.
`RelayServer.layer(options)` adds the entity behaviour and the retention singleton that makes `messageTtl` and
`terminalRetention` run.

```ts
// Server composition, abridged from docs/store-and-forward.md.
// tenantId, relayPeerId, relayLimits, authorizeRelay, shardingLive, and socketServerLive are
// application supplied values; the complete listing with every import is in docs/store-and-forward.md.
const RelayLive = RelayServer.layer({
  tenantId,
  peerId: relayPeerId,
  heartbeatInterval: Duration.seconds(30),
  entityCallTimeout: Duration.seconds(30),
  inbox: {
    maxDeliveries: 16,
    messageTtl: Duration.days(7),
    terminalRetention: Duration.days(8),
    sessionDeadline: Duration.seconds(90),
    sessionSweep: Duration.seconds(5),
    maxConcurrentChannels: 8,
    storeRetry: Duration.seconds(1),
    maxPendingMessages: 10_000,
    maxPendingBytes: 256 * 1_024 * 1_024,
    mailboxCapacity: 64,
    maxIdleTime: Duration.minutes(30)
  },
  // Required. Without it nothing expires and nothing is collected.
  maintenance: {
    interval: Duration.minutes(1),
    batchLimit: 500,
    terminalRetention: Duration.days(8),
    enabled: true
  }
}).pipe(
  Layer.provide(SqlRelayInboxStore.layer),
  Layer.provide(PeerRelayLimits.layer(relayLimits)),
  Layer.provide(PeerRelayAuthorization.layer(
    authorizeRelay,
    PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode
  )),
  Layer.provide(shardingLive)
)

const ServerLive = RpcServer.layer(PeerRpc.Rpcs).pipe(
  Layer.provide(RelayLive),
  Layer.provide(PeerAuthentication.layerServer),
  Layer.provide(RpcServer.layerProtocolSocketServer),
  Layer.provide(RpcSerialization.layerJson),
  Layer.provide(socketServerLive)
)
```

On the client, build the replica with `SqlReplica.layerRelayWithBindings`, add
`PeerRelayClientRuntime.layerSql` for the durable sender outbox, and bind the generated RPC client to a
`PeerSession` through `RpcPeerTransport.makeSession(client, options)`. Options pin the expected local
principal, relay peer, remote subject and peer, selected documents, sender replica incarnation, receipt
retention, retry horizon, and replay batch size. A restore advances the incarnation, so rebuild options from
`ReplicaGate.current` on every reconnect attempt rather than caching one value.

The full composition, custody boundaries, capacity rules, and operational requirements are in
[docs/store-and-forward.md](docs/store-and-forward.md), including the transport scoping mistake that leaves a
client waiting on a released socket.

### Relay policy

`PeerRelayAuthorization.layer(authorize, authorizeUnsafeUnboundedAutomerge3Decode)` installs two callbacks:

- `authorize(request)` receives the direction, the authenticated principal, the exact remote, and the selected
  whole documents, and returns an exact grant or fails with `AccessDenied` / `ServerUnavailable`. Partial,
  enlarged, or mismatched grants fail closed during validation.
- `authorizeUnsafeUnboundedAutomerge3Decode(request)` is a second, explicit resource trust grant. Automerge
  `3.3.2` has no allocation bounded decode API, so authentication and document authorization never imply that
  the producer's bytes are safe to decode. The default is
  `PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode`. Grant it only when the application controls and
  resource trusts the producer. The grant exactly binds principal, remote, direction, the complete document
  set, a finite `validUntil`, and an `invalidated` Effect.

### RPC failures and retry

Every procedure can fail with a fieldless `PeerRpcError`:

| Variant                   | Cause                                                                            | Classification |
| ------------------------- | -------------------------------------------------------------------------------- | -------------- |
| `AuthenticationFailure`   | Missing or rejected credential, expired grant, or any authenticator failure      | Terminal       |
| `AccessDenied`            | Authorization or the unsafe decode grant refused                                 | Terminal       |
| `UnsupportedVersion`      | Protocol version mismatch at the handshake                                       | Terminal       |
| `PeerMismatch`            | The connected peer is not the configured one                                     | Terminal       |
| `DefinitionMismatch`      | Definition hash mismatch                                                         | Terminal       |
| `InvalidRequest`          | Malformed envelope, digest, routing, or selection                                | Terminal       |
| `RequestLimitExceeded`    | A negotiated or configured limit was breached                                    | Terminal       |
| `RequestCapacityExceeded` | Authentication rate limit, verifier saturation, or inbox admission quota         | Live resource  |
| `SessionUnavailable`      | No live session, or the session belongs to another principal (one shared answer) | Live resource  |
| `SessionOverloaded`       | Per subject session cap                                                          | Live resource  |
| `ServerUnavailable`       | Backend store unavailable                                                        | Live resource  |
| `DocumentLineageChanged`  | The remote holds a superseded document lineage                                   | Terminal       |

The wire errors are fieldless by design so authorization and storage failures do not disclose message
existence. Unexpected defects encode only `{ _tag: "InternalError" }`.

`RpcPeerTransport` classifies every wire error and transport failure into the local error model. Live resource
failures and `RpcClientError` become `ReplicaError` / `StorageUnavailable`. Everything terminal becomes
`ReplicaError` / `ProtocolMismatch` carrying the wire tag in `observed`. The retry rule is one line:

```ts
export const isRetryable = (error: ReplicaError.ReplicaError) => error.reason._tag === "StorageUnavailable"
```

Retry only `StorageUnavailable`, and retry by rebuilding the complete connection scope with fresh options.
Policy and protocol mismatches are not transient; fix the cause instead. `SupervisedPeerSession.awaitDisconnect`
reports the terminal failure, and the retry schedule is the application's deployment policy, not a library
default.

Malformed peer traffic is terminal, never retried in place. Envelope, digest, endpoint, or selection validation
failures reject the exchange. On the relay the recipient settles a permanently invalid message with
`Reject` and reason `ProtocolInvalid`, which dead letters it; application rejection uses `ApplicationRejected`.
Infrastructure failure, interruption, timeout, disconnect, and claim expiry instead release the message for
retry. Reaching `RelayInbox.Options.maxDeliveries` dead letters a message so it stops blocking its channel, and
restart does not reset the count.

### Authentication semantics

The application implements `PeerAuthenticator.authenticate(credential)`, which returns the principal, a finite
`validUntil`, and an `invalidated` Effect, or fails with `AuthenticationFailure`. Credentials are
`Redacted<string>` values decoded from the RPC payload on every operation; upgrade headers and client IDs are
not principals. Client middleware overwrites any caller supplied credential from `PeerCredentials.get`, and
`PeerAuthentication.layerClient` / `layerServer` install both sides.

The wire deliberately cannot distinguish credential rejection from authenticator infrastructure failure. A
missing credential, a rejected credential, an expired or nonfinite `validUntil`, and any failure or defect
inside the authenticator all surface as the same fieldless `AuthenticationFailure`. Interruption is the only
cause propagated unchanged. An authenticator that calls a database or identity service must log its own
infrastructure failures locally, because the peer never learns the difference. Rate limiting and verifier
saturation answer separately with `RequestCapacityExceeded`, which is retryable with the same stable request
after application backoff.

### RPC environment requirements

`RelayServer.layerHandlers` requires `Crypto`, `PeerRelayAuthorization`, `PeerRelayLimits`, and a `Sharding`,
plus the application `SqlClient` for `SqlRelayInboxStore.layer` (SQLite, PostgreSQL, or MySQL).
`RelayServer.layer` additionally requires `RelayInboxStore`. `PeerAuthentication.layerServer` requires
`PeerAuthenticator` and `PeerRelayLimits`; `layerClient` requires `PeerCredentials`.
`PeerRpc.makeRpcClient` requires an `RpcClient.Protocol`, the client middleware, and a `Scope`.
`RpcPeerTransport.layer` additionally uses `PeerRelayClientRuntime`. TLS, Origin policy, ingress byte and
connection limits, credential issuance and rotation, tenant routing, and process supervision remain deployment
responsibilities and are not inferred from any Layer here.

## Compaction and history rewriting

Compaction bounds SQL change rows by publishing a verified Automerge checkpoint and pruning redundant changes.
It never discards logical history. `SqlReplica` constructors register the workflow; handles are scoped to the
replica incarnation.

```ts
import * as ReplicaWorkflow from "@lucas-barake/effect-local-sql/ReplicaWorkflow"
import * as Effect from "effect/Effect"

const compact = Effect.gen(function*() {
  const workflows = yield* ReplicaWorkflow.CompactionWorkflow
  const execution = yield* workflows.execute(
    ReplicaWorkflow.OperationId.make("scheduled-compaction-2026-07")
  )
  return yield* workflows.poll(execution)
})
```

```ts
export class CompactionWorkflow extends Context.Service<CompactionWorkflow, {
  readonly execute: (operationId: OperationId) => Effect.Effect<Execution, ReplicaError.ReplicaError>
  readonly poll: (
    execution: Execution
  ) => Effect.Effect<Option.Option<Workflow.Result<void, ReplicaError.ReplicaError>>, ReplicaError.ReplicaError>
  readonly interrupt: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly resume: (execution: Execution) => Effect.Effect<void, ReplicaError.ReplicaError>
}>() {}
```

Checkpoint publication is an optimistic compare and set against the global commit sequence, so a concurrent
commit supersedes it. Each document retries a bounded number of times. If every attempt is superseded, `poll`
completes with a failed `Exit` carrying `ReplicaError` / `CheckpointSuperseded { documentIds, attempts }`,
where `attempts` counts publish attempts per document. That failure means partial compaction: every document
not listed compacted normally. Any other failure stops the run at the document that raised it.

Retrying requires a NEW operation ID. Execution identity is derived from the replica incarnation and the
operation ID, and the result is journaled, so re-executing the same ID replays the recorded outcome instead of
compacting again.

History rewriting is the one destructive operation. `Automerge.save` encodes the whole change graph, so one
field overwritten N times keeps growing the checkpoint even after pruning. Automerge `3.3.2` offers no API to
remove history from a live document, so `HistoryRewriteWorkflow` rebuilds the document as a fresh single
change document carrying the current materialized value, and mints a new lineage (`lin_` plus a UUID).

```ts
import * as ReplicaWorkflow from "@lucas-barake/effect-local-sql/ReplicaWorkflow"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"

declare const documentId: Identity.DocumentId

const rewrite = Effect.gen(function*() {
  const workflows = yield* ReplicaWorkflow.HistoryRewriteWorkflow
  const execution = yield* workflows.execute(
    documentId,
    ReplicaWorkflow.OperationId.make("history-horizon-2026-07")
  )
  // The minted lineage is read from the Complete result, not from execute.
  return yield* workflows.poll(execution)
})
```

The rewrite is operator driven, never automatic. It is permanent: every prior change and checkpoint, the writer
provenance of every dropped change, losing register alternatives, and that document's peer receipts and outbox
rows are destroyed. The current decoded value, the tombstone marker, and command receipts are preserved.

Contract points that are easy to get wrong:

1. An operation ID is bound to the first document it rewrote. Reusing one against a different document fails
   with `ReplicaError` / `ProtocolMismatch` before anything destructive runs. A new operation ID performs a
   second rewrite and mints a second lineage.
2. A rewritten document never synchronizes with a peer holding the superseded lineage, and it cannot be seeded
   to a peer that never held the document. Peers that did not advertise `lineageAware` are never sent the
   document at all; aware but stale peers are refused with a typed non retryable error scoped to that document
   while every other selected document keeps synchronizing.
3. Lineage is an unauthenticated peer assertion on the sync envelope. It is a correctness signal against an
   honest but stale peer, not a security control. Confirm locally that a rewrite ran before acting on a
   refusal.
4. Recovery for a refused peer replaces its obsolete canonical state: export the authoritative replica after
   the rewrite and clone restore onto the refused replica. Either restore mode discards unsynced changes on the
   superseded lineage, which is why history rewrite is opt in rather than a background policy.

## Testing

`@lucas-barake/effect-local-test` runs the production composition against in memory SQLite, with deterministic
peer faults. Install it with `@effect/vitest` and the Node platform packages.

```ts
import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { definition, DomainLive, TaskDocument, TaskListSql } from "./domain.js"

const TestLive = TestReplica.layer(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(DomainLive)
)

it.layer(NodeCrypto.layer)("replica", (it) => {
  it.effect("commits locally", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const now = Date.now()
      const documentId = yield* replica.create(TaskDocument, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "test", completed: false, createdAt: now, updatedAt: now }
      })
      const snapshot = yield* replica.get(TaskDocument, documentId)
      assert.strictEqual(snapshot.value.title, "test")
    }).pipe(Effect.provide(TestLive)))
})
```

| Constructor                          | Composition                                                               |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `TestReplica.layer`                  | Full durable Cluster and Workflow path, in memory SQLite, `defaultLimits` |
| `TestReplica.layerWithLimits`        | Same, with an explicit limits value                                       |
| `TestReplica.layerWithSync`          | Lower level sync graph (`PeerSync`) for deterministic peer tests          |
| `TestReplica.layerWithSyncAndLimits` | Same, with an explicit limits value                                       |

Every constructor installs the supplied SQL bindings and still requires the domain's generated mutation and
query handler Layers.

For peer tests, `TestPeer.layer({ queueCapacity, maxCopies, maxDelay })` provides a bounded duplex transport
with `connect`, `partition`, `heal`, `flush`, and `transport(peerId)`, which adapts it to `PeerTransport`.
`FaultInjection.layerSequence` decides per packet: drop, finite copies, finite delay, reorder; the final
decision repeats. Effect's `TestClock` controls delay and presence expiry without wall clock sleeps.

Invalid bounds fail in the error channel, not as defects: `TestPeer.make` and `TestPeer.layer` fail with
`InvalidOptions { reason }`. Fault decisions outside the configured bounds fail with
`InvalidFault { sequence, reason }`. Sends against a full queue or closed connection fail with `QueueFull` or
`ConnectionClosed`. `TestReplica.defaultLimits` is the only exported limits preset and is intended for tests,
not production capacity planning.

## Error reference

Failures beside the operations that produce them are documented in each section above. This is the consolidated
reference. Four kinds of failure exist:

1. **Declared domain errors.** Defined per mutation and query with `Schema.TaggedErrorClass`. They arrive
   unwrapped in the error channel and are caught by tag: `Effect.catchTag("TitleEmpty", ...)`.
2. **`ReplicaError`.** The engine's one tagged error, `{ _tag: "ReplicaError", reason }`. Discriminate the
   reason with `Effect.catchReason("ReplicaError", "<ReasonTag>", ...)` or match on `error.reason._tag`.
   `ReplicaError.isReplicaError` narrows a mixed channel.
3. **RPC errors.** `PeerRpcError` variants on the relay protocol, mapped locally to `ReplicaError` by
   `RpcPeerTransport`; see [RPC failures and retry](#rpc-failures-and-retry). Browser page RPC failures are
   `ReplicaError`, plus `ReplicaQueryError` carrying a query's declared error as JSON.
4. **Defects and interruption.** Defects are programming errors and stay defects. Interruption is preserved
   everywhere; scope close is cleanup, not persistence.

### `ReplicaError` reasons

| Reason                            | Fields                                              | Raised when                                                                                | Handling                                                           |
| --------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `DocumentNotFound`                | `documentId`                                        | `get`, `mutate`, or `delete` targets a missing document                                    | Terminal                                                           |
| `DocumentDecodeError`             | `documentId`, `cause`                               | Stored or supplied value fails schema decode, or a migration step throws                   | Terminal; fix data or schema                                       |
| `DocumentEncodeError`             | `documentId`, `cause`                               | A supplied value fails schema encode                                                       | Terminal                                                           |
| `UnsupportedDocumentVersion`      | `documentId`, `observedVersion`, `supportedVersion` | A stored version has no covering migration chain                                           | Terminal; register the migration or upgrade the build              |
| `ProjectionBlocked`               | `projection`, `cause`                               | A projection cannot represent accepted canonical state, or a query dependency is not ready | Terminal for data causes; readiness resolves after startup rebuild |
| `CommandIdConflict`               | `commandId`                                         | A command ID was reused for different input                                                | Terminal; mint a new command ID for a new operation                |
| `ReceiptOperationMismatch`        | `commandId`, `expected`, `observed`                 | A lookup targets a receipt written by another operation or mutation                        | Terminal                                                           |
| `StorageUnavailable`              | `cause`                                             | The SQL store or transport is temporarily unavailable                                      | Retryable with backoff; commands need lookup after retry           |
| `CanonicalEncodeError`            | `cause`                                             | Canonical JSON encoding failed                                                             | Terminal                                                           |
| `StorageCorrupt`                  | `cause`                                             | One document's stored bytes or metadata fail validation                                    | Terminal for that document; recovery quarantines it                |
| `ReplicaMetadataMissing`          | `operation`                                         | The replica's metadata singleton row is gone; the replica has no identity                  | Replica wide and fatal                                             |
| `QuotaExceeded`                   | `resource`, `limit`                                 | A configured limit was breached (pending bytes, gate queue, restore admission)             | Shed load; retry only after capacity frees                         |
| `MigrationFailed`                 | `migration`, `cause`                                | A library storage migration failed                                                         | Terminal                                                           |
| `BackupInvalid`                   | `cause`                                             | An archive or portable document fails validation                                           | Terminal                                                           |
| `BackupTooLarge`                  | `limit`, `observed`                                 | An archive exceeds `maxBytes`                                                              | Terminal; raise the bound or split the data                        |
| `RestoreBusy`                     | `replica`                                           | Another restore holds the exclusive claim                                                  | Retry after the active restore finishes                            |
| `RestoreFailed`                   | `cause`                                             | The staged install failed                                                                  | Terminal; the previous incarnation is untouched                    |
| `ProtocolMismatch`                | `expected`, `observed`                              | Definition hash, protocol version, incarnation, or session mismatch                        | Terminal; fix the deployment or rebuild the session                |
| `ReplicaFenced`                   | `expectedGeneration`, `observedGeneration`          | A stale writer generation tried to commit                                                  | Terminal; this process lost ownership                              |
| `OperationTimeout`                | `operation`, `timeoutMillis`                        | A bounded operation exceeded its deadline                                                  | Same as `StorageUnavailable` for transport operations              |
| `UnsupportedStorageFormatVersion` | `observedVersion`, `supportedVersion`               | The on disk format belongs to another library build                                        | Terminal; run a matching build or re-seed                          |
| `CheckpointSuperseded`            | `documentIds`, `attempts`                           | Checkpoint publication lost the install race on every attempt                              | Partial compaction; retry with a new operation ID                  |
| `DocumentLineageChanged`          | `documentId`, `localLineage`, `remoteLineage`       | A peer holds a superseded document lineage                                                 | Terminal for that document; repair by archive restore              |
| `CommandOutcomeUnknown`           | `commandId`, `cause`                                | The command may or may not have committed                                                  | Never retry; look the command ID up                                |

Fields named `cause` use `Schema.Defect()` and carry the original failure. They stay inside the local process;
the external peer RPC never exposes them.

Handling pattern for one operation, with every branch explicit. The handler functions are illustrative
application code:

```ts
// Illustrative: showDomainError, refreshList, and retryWithBackoff are application functions.
replica.mutate(RenameTask, { commandId, documentId, payload }).pipe(
  Effect.catchTag("TitleEmpty", (error) => showDomainError(error)),
  Effect.catchReason(
    "ReplicaError",
    "CommandOutcomeUnknown",
    (reason) =>
      replica.lookupMutation(RenameTask, reason.commandId).pipe(
        Effect.flatMap(CommandOutcome.committedOrFail)
      )
  ),
  Effect.catchReason("ReplicaError", "DocumentNotFound", () => refreshList()),
  Effect.catchReason("ReplicaError", "StorageUnavailable", () => retryWithBackoff())
)
```

### Retry taxonomy

| Action                       | When                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------- |
| Retry the same command ID    | Only after `StorageUnavailable` style transport loss, when the request bytes are identical  |
| Look up an ambiguous outcome | After `CommandOutcomeUnknown`, with the same definition and command ID                      |
| Start a new logical command  | New intent, or application policy after an `OutcomeUnknown` lookup. Fresh command ID        |
| Retry a transport connection | `RpcPeerTransport.isRetryable` (`StorageUnavailable` only), with a rebuilt connection scope |
| Never retry                  | Domain rejections, policy and protocol mismatches, fencing, quota, malformed peer traffic   |

### Browser protocol errors

The page to owner protocol fails with `ReplicaError`. `Query` additionally fails with `ReplicaQueryError`,
which carries the query's declared error as JSON and is unwrapped back to the declared error by the client. A
session the owner no longer knows answers `ProtocolMismatch { expected: "active session" }`, which the client
and `ReplicaAtom.status` recover by reopening a session. Any other protocol mismatch is deployment skew and
requires a page reload. Restore admission failures use `QuotaExceeded`; restore transport failures use
`StorageUnavailable`; an owner protocol violation uses `ProtocolMismatch`.

### Test errors

`TestPeer` validates in the error channel: `InvalidOptions { reason }` for constructor bounds,
`InvalidFault { sequence, reason }` for fault decisions outside configured bounds, `QueueFull { from, to, capacity }`
and `ConnectionClosed { from, to }` on send. `TestPeerError` is the union of `InvalidFault`, `QueueFull`, and
`ConnectionClosed`.

## API reference

Every module below is a public subpath import: `@lucas-barake/effect-local/Document`,
`@lucas-barake/effect-local-sql/SqlReplica`, and so on. Everyday modules cover the normal application path.
Advanced modules support custom runtimes and diagnostics; most applications should not call them directly.

### `@lucas-barake/effect-local`

Domain model and the application capability.

- `Document`: `make`, `migration`, `decode`, `encode`, `decodeStored`, `isAutomergeValue`, `supportsStoredVersion`;
  types `Document`, `Any`, `Migration`, `WireSchema`, `AutomergeEncoded`, `DocumentSchema`.
- `DocumentSet`: `make`, `get`; type `DocumentSet`.
- `Mutation`: `make`; per definition `payloadSchema`, `successSchema`, `errorSchema`, `of`, `toLayer`; types
  `Mutation`, `Any`, `Handler`, `HandlerOptions`, `HandlerService`, `Draft`, `DraftValue`, `HandlerResult`.
- `Projection`: `make`, `evaluate`, `assertUniqueKeys`; types `Projection`, `Any`.
- `Query`: `make`; per definition `payloadSchema`, `successSchema`, `errorSchema`, `dependsOn`, `of`, `toLayer`;
  types `Query`, `Any`, `Handler`, `HandlerService`.
- `ReplicaDefinition`: `make`, `invalidationKeys`; types `ReplicaDefinition`, `Any`. Exposes `name`, `documents`,
  `mutations`, `projections`, `queries`, and `hash`.
- `Replica`: the `Replica` service, signatured in [Using `Replica`](#using-replica).
- `CommandOutcome`: `schema`, `rejected`, `durablyCommitted`, `unknown`, `match`, `committedOrFail`, `toOutcome`;
  types `CommandOutcome`, `Rejected`, `DurablyCommittedLocal`, `OutcomeUnknown`.
- `Snapshot`: types `Snapshot`, `FromDocument`, `ProjectionState`.
- `Identity`: branded schemas and types for `ReplicaId`, `ReplicaIncarnation`, `SessionId`, `DocumentId`,
  `CommandId`, `WriterGeneration`, `CommitSequence`, `PeerId`, `BackupInstallationId`, `RelayMessageId`,
  `ProjectionVersion`, `DocumentLineage`; `genesisLineage`; generators `makeReplicaId`, `makeSessionId`,
  `makeDocumentId`, `makeCommandId`, `makePeerId`, `makeBackupInstallationId`, `makeRelayMessageId`,
  `makeDocumentLineage` (each requires Effect `Crypto`); `documentIdFromCommandId`.
- `ReplicaError`: the `ReplicaError` class, every reason class, `Reason`, `isReplicaError`.
- `ReplicaStatus`: `Starting`, `Ready`, `ReadOnly`, `Degraded`, `ProjectionBlocked`, `Restoring`, `Failed` and
  the `ReplicaStatus` union.
- `ReplicaLimits`: `Values`, `minimumRestoreErrorBytes`, the `ReplicaLimits` service, `make`, `layer`.
- `Backup`: `FormatVersion`, `Header`, `MaxBytes`, `validateMaxBytes`; types `ExportOptions`, `RestoreOptions`,
  `ExportedDocument`.
- `Commit`: `Heads`, `Commit` and their inferred types.
- `Canonical`: `stringify`, `hash`, `digest`. Deterministic encoding utilities, not the canonical store.
- `PeerTransport`: the `PeerTransport` service; types `Connection`, `ConnectOptions`, `Capabilities`,
  `AcknowledgedDelivery`, `RelayDeliveryIdentity`, `PermanentRejectReason`.
- `SchemaDescriptor`: `make`. Encoded schema fingerprints used in hashes and checksums.

### `@lucas-barake/effect-local-sql`

Durable runtime. Everyday modules are `SqlReplica`, `SqlProjection`, `PeerSession`, and `ReplicaWorkflow`. The
rest are advanced assembly for custom runtimes.

- `SqlReplica`: `layerWithBindings`, `layerRelayWithBindings`, `layer`, `layerRelay`, `layerFromServices`. See
  [Durable Node composition](#durable-node-composition).
- `SqlProjection`: `make`; types `SqlProjection`, `Migration`, `BindingService`, `Any`. Each binding exposes
  `service` and `layer`.
- `PeerSession`: `make`, `makeSupervised`, `makeLive`, `makeTestClient`; types `SelectedDocument`, `PeerSession`,
  `SupervisedPeerSession`. See [Peer synchronization](#peer-synchronization).
- `ReplicaWorkflow`: `CompactionWorkflow`, `HistoryRewriteWorkflow`, `CompactReplica`, `RewriteDocumentHistory`,
  `OperationId`, `layerRegistration`, `layerRuntime`, `layerHistoryRewriteRegistration`,
  `layerHistoryRewriteRuntime`; types `Execution`, `DocumentExecution`.
- `ReplicaEvolution`: `make`, `layer`, the `ReplicaEvolution` service and its `State`. Runs at startup inside
  `SqlReplica`; report surface for diagnostics.
- `ReplicaHealth`: `layer`, `defaultOptions`; the `ReplicaHealth` service, `Options`, `Restore`. Backs
  `Replica.status`.
- Advanced assembly: `BackupStore`, `CommandExecutor`, `CommitPublisher`, `Compaction`, `DocumentEntity`,
  `DocumentStore`, `DurableRuntime`, `EntityReplica`, `Migrations`, `ProjectionStore`, `QueryExecutor`,
  `Recovery`, `ReplicaBootstrap`, `ReplicaGate`. These own bootstrap, fencing, storage, execution, and recovery
  internals behind `SqlReplica`; call them only when assembling a custom runtime. Their service surfaces are
  documented in [docs/durability.md](docs/durability.md) and [docs/architecture.md](docs/architecture.md).
- Relay client state (used by `RpcPeerTransport`, not called directly): `PeerRelayClientRuntime`
  (`layer`, `layerSql`, `makeScoped`, `ConnectionConfiguration`), `PeerRelayOutbox` (`layerSql` and its
  service), `PeerRelayOutboxLimits` (`Values`, `defaults`, `make`, `layer`, `layerDefaults`,
  `maximumRetryHorizonMillis`), `PeerRelayReceiptLimits` (same shape plus `maximumReceiptRetentionMillis`).
- `PeerSync` and `PeerSyncEnvelope`: the durable protocol service and the envelope schemas, validation,
  `maximumSyncEnvelopeBytes`, and digest functions. Advanced; `PeerSession` owns their sequencing.

### `@lucas-barake/effect-local-browser`

Browser runtime. Everyday modules are `BrowserReplica`, `BrowserSqlite`, `OwnershipCoordinator`, `ReplicaAtom`,
and `Presence`.

- `BrowserReplica`: `layer`, `layerWith`, `layerWithReactivity`, `layerWithReactivityOptions`. See
  [Browser composition](#browser-composition).
- `BrowserSqlite`: the `DatabasePort` service, `layer`, `layerMessagePort`.
- `OwnershipCoordinator`: `layerTab`, `layerSharedWorker`, `runSharedWorker`, `runDatabaseWorker`; the
  `OwnershipCoordinator` service; types `SharedWorkerOptions`, `TabOptions`, `DatabaseWorkerOptions`,
  `EngineServices`.
- `ReplicaAtom`: `documentFamily`, `queryFamily`, `mutation`, `status`, `layerReactivity`. See
  [Reactive state](#reactive-state).
- `Presence`: `make`; types `Presence`, `Entry`.
- `PeerSession`: compatibility reexport of the SQL package session API. New code should import
  `@lucas-barake/effect-local-sql/PeerSession`.
- Advanced assembly: `ReplicaClient` (`fromRpcClient`, `layer`, `Options`, `defaultSessionTimeout`,
  `defaultOperationTimeout`, the `ReplicaClient` service), `ReplicaOwner` (`layerHandlers`, `layer`,
  `layerWorker`), `ReplicaRpc` (the page to owner protocol group and its schemas), `SessionManager` (`layer`,
  `leaseDurationMillis`, the service). These are wired by `BrowserReplica` and `OwnershipCoordinator`.

### `@lucas-barake/effect-local-rpc`

Relay protocol and policies. See [RPC relay](#rpc-relay) for composition and failure semantics.

- `PeerRpc`: `protocolVersion`, `maximumNegotiatedDurationMillis`, `maximumRelayPayloadBytes`,
  `maximumRequestedDocuments`, the `Open` / `Push` / `Acknowledge` / `Reject` RPC classes, `Rpcs`,
  `makeRpcClient`; types `RpcClient`, `Opened`, `StoredMessage`, `OpenEvent`, `RequestedDocument`, `ClaimToken`,
  `RelayDigest`, `RejectReason`.
- `PeerRpcError`: the twelve fieldless error classes, the `PeerRpcError` union, and the `Defect` schema that
  encodes unexpected defects as `{ _tag: "InternalError" }`.
- `PeerAuthentication`: `layerClient`, `layerServer`, the `PeerAuthentication` middleware, `AuthenticatedPeer`,
  `PeerPrincipal`.
- `PeerAuthenticator`: the application credential verification service.
- `PeerCredentials`: the client credential provider service.
- `PeerRelayAuthorization`: `layer`, `denyUnsafeUnboundedAutomerge3Decode`,
  `unsafeUnboundedAutomerge3DecodeRisk`, `UnsafeUnboundedAutomerge3DecodeRequest`,
  `UnsafeUnboundedAutomerge3DecodeGrant`, `Direction`, `RemotePeer`; types `Request`, `Result`, `Authorize`,
  `AuthorizeUnsafeUnboundedAutomerge3Decode`.
- `PeerRelayLimits`: `Values`, `defaults`, `make`, `layer`, `layerDefaults`, `InvalidPeerRelayLimits`.
- `RelayServer`: `layerHandlers`, `layer`; type `Options`.
- `RelayInbox`: the `RelayInbox` entity, `layer`, `Options`, and its RPC classes.
- `RelayInboxMaintenance`: `layer`, `Options`.
- `RelayInboxStore`: the injectable custody contract service and its schemas (`InboxEnvelope`, `ChannelKey`,
  `AdmissionResult`, `DeliveryRecord`, `SettleResult`, `InboxState`, `TerminalOutcome`; types `AdmissionRequest`,
  `PendingMessage`, `AbandonedMessage`, `Usage`, `AdmissionQuota`).
- `SqlRelayInboxStore`: `make`, `layer`. Generic `SqlClient` implementation for SQLite, PostgreSQL, and MySQL.
- `RpcPeerTransport`: `layer`, `makeSession`, `isRetryable`; type `Options`.

### `@lucas-barake/effect-local-test`

- `TestReplica`: `layer`, `layerWithLimits`, `layerWithSync`, `layerWithSyncAndLimits`, `defaultLimits`.
- `TestPeer`: `make`, `layer`, `transportLayer`; the `TestPeer` service; errors `InvalidOptions`,
  `InvalidFault`, `QueueFull`, `ConnectionClosed`; types `Options`, `Connection`, `TestPeerError`.
- `FaultInjection`: `layer`, `none`, `layerSequence`; the service; types `Packet`, `Decision`.

## Guarantees and limitations

Guarantees:

- The local replica is authoritative for interactive reads and writes. Remote connectivity never blocks a local
  commit.
- One command commits canonical changes, projections, the command receipt, the commit sequence, and the stored
  reply in one SQLite transaction.
- Command IDs are idempotency keys within one replica incarnation. An identical retry returns the durable
  result; a conflicting reuse fails.
- Projections are rebuildable from canonical snapshots. A blocked projection blocks its queries, never serves
  partial state.
- Replicas converge after receiving the same valid Automerge change set.
- Restore is exclusive, fenced, staged, schema checked, and projection rebuilding. A failed install leaves the
  previous incarnation untouched.
- Relay delivery is at least once within configured retry, expiry, retention, authorization, and capacity
  boundaries. A successful `Push` is durable custody.
- Compaction never discards logical history.

Limitations:

- Presence is expiring best effort state. It is never durable and never authorizes anything.
- `durableConfirmation` returns only `false`; `observedByPeer` is Automerge sync state, not remote durability.
- Browser storage starts as best effort and can be evicted or cleared. Request `navigator.storage.persist()`,
  report the result, and ship backup controls.
- One mutation targets one document. There is no replicated transaction across documents; model one aggregate
  document or an explicit workflow.
- Whole document sync is the only granularity. Subtree synchronization is unsupported.
- History rewrite destroys prior changes, checkpoints, provenance, and peer state for the document, and the
  rewritten document never resynchronizes with a peer holding the superseded lineage.
- Cross lineage synchronization is refused, never merged.
- The RPC package provides no managed backend, discovery, account system, credential issuer, tenant registry,
  TLS, HTTP server, or end to end encryption.
- A replica database must not be opened by a build older than the one that wrote its workflow records: a record
  carrying an unknown `ReplicaError` reason fails to decode and becomes a defect.
- The browser suites currently exercise Chromium. Other engines are not claimed as supported until the same
  suite passes there.

## Browser and deployment requirements

The durable browser composition requires an ESM build tool with module workers and WebAssembly; `SharedWorker`,
dedicated `Worker`, transferable `MessagePort`, Web Locks, OPFS, and WebAssembly; a secure context in
production; a CSP and asset pipeline that allow the SQLite WASM files and module workers; and a page that can
provision the dedicated OPFS worker.

A relay deployment additionally owns: an Effect `RpcServer.Protocol` and `RpcClient.Protocol` with the same
`RpcSerialization` on both ends, a platform socket and HTTP server, TLS, WebSocket upgrade routing, Origin
policy, ingress byte and connection limits, credential issuance and rotation, tenant routing, process
supervision, graceful shutdown, and the database deployment behind `SqlRelayInboxStore`. The relay's in process
quotas do not replace upstream controls. Reject oversized handshakes and frames before decoding. A multi node
relay needs a real inter-runner socket transport; `Runners.layerNoop` hangs instead of erroring on a remote
shard. See [docs/store-and-forward.md](docs/store-and-forward.md) for the operational checklist.

## Repository scripts

Run commands from the repository root.

| Script                  | Purpose                                                           |
| ----------------------- | ----------------------------------------------------------------- |
| `pnpm build`            | Build all publishable packages with TypeScript project references |
| `pnpm check`            | Type check packages, examples, and tests                          |
| `pnpm check:all`        | Add the native TypeScript preview check to local commit checks    |
| `pnpm check:pre-commit` | Run nonmodifying local commit checks once                         |
| `pnpm lint`             | Run oxlint and check dprint formatting                            |
| `pnpm lint-fix`         | Apply lint fixes and dprint formatting                            |
| `pnpm test --run`       | Run the unit and integration suite once                           |
| `pnpm test:browser`     | Run browser ownership, OPFS, transfer, and restart contract tests |
| `pnpm bench`            | Run ordinary tracked library benchmarks once                      |
| `pnpm coverage`         | Run Vitest with V8 coverage                                       |
| `pnpm dead-code`        | Find unused private files, exports, and dependencies              |
| `pnpm circular`         | Check package sources for circular imports                        |
| `pnpm codegen`          | Regenerate package barrel modules                                 |
| `pnpm docgen`           | Compile documentation through the root TypeScript graph           |
| `pnpm clean`            | Remove generated build artifacts                                  |

## Further documentation

- [docs/architecture.md](docs/architecture.md): state ownership, relay topology, browser lifecycle.
- [docs/durability.md](docs/durability.md): command acknowledgement, recovery, compaction, lineage, workflows,
  backup and restore.
- [docs/schema-evolution.md](docs/schema-evolution.md): definition compatibility, document migrations, storage
  format versioning.
- [docs/sync.md](docs/sync.md): the peer sync protocol, bounds, and transport responsibilities.
- [docs/store-and-forward.md](docs/store-and-forward.md): relay composition, custody, security, capacity, and
  operations.
- [packages/local-sql/examples/](packages/local-sql/examples/): the compile checked quick start sources.
- [packages/local-browser/test-browser/ownership/src/](packages/local-browser/test-browser/ownership/src/): a
  complete checked browser application.
- Effect `4.0.0-beta.99` source:
  [Context](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/Context.ts),
  [Layer](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/Layer.ts),
  [Atom](https://github.com/Effect-TS/effect/blob/6184a7dc53cb9310e299b65ad6d6c712c2cbf202/packages/effect/src/unstable/reactivity/Atom.ts).
- [Automerge](https://automerge.org/docs/reference/concepts/) concepts and the
  [local first essay](https://www.inkandswitch.com/essay/local-first/).

## License

[MIT](LICENSE)
