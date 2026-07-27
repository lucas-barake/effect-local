# Effect Local

Effect Local is a local first engine for browser and Node replicas built on Effect v4. Application reads and writes
complete against a local replica. Automerge defines document state, causal history, and convergence. SQLite persists
the local representation and rebuildable query projections. In browsers, SQLite WASM stores its database in OPFS.
Effect Cluster serializes document commands and stores their replies. Effect Workflow resumes long running
maintenance. Effect Atom exposes reactive views without becoming another source of truth. The optional RPC package
provides the protocol, policies, bounded sessions, and transport adapter needed to host another canonical SQL replica
over an application owned Effect RPC connection.

The design follows the ownership, longevity, and offline principles in
[Local first software](https://www.inkandswitch.com/essay/local-first/). This repository provides local process
libraries and backend building blocks. It does not provide a managed service, identity issuer, tenant registry,
encryption protocol, TLS termination, HTTP server, or deployment framework. The browser package adds tab and worker
ownership. The SQL package supports browser and Node replicas. The RPC package stays platform neutral and leaves
WebSocket, serialization, routing, and server ownership to Effect and the application.

> **Beta:** The library targets Effect `4.0.0-beta.99` and Automerge `3.3.2`. Durable formats,
> worker protocols, and public APIs can still change. Read [Limits and security](#limits-and-security) before adopting
> it for user data. External RPC uses one durable version `1` relay topology with an at least once delivery contract.
> The built in SQL custody store supports SQLite, PostgreSQL, and MySQL. It is not a managed global relay service.

## Retrieval contract

This README is the public contract index for humans and language models. Headings are stable lookup keys. Normative
statements use **must**, **must not**, **requires**, **guarantees**, or **does not guarantee**. Examples show the
minimum public composition path. The [public API inventory](#public-api-inventory) lists every root namespace and its
exports. Every namespace is also a public subpath. For example, `@lucas-barake/effect-local/Replica` and
`@lucas-barake/effect-local-rpc/PeerRpc` are supported imports. Paths under `internal/*` are private.

When a transport, queue, stream, session, lease, or acknowledgement is described as successful, read the exact
boundary stated in [Durability and delivery contract](#durability-and-delivery-contract). No lower layer implies a
higher durability guarantee.

## Contract summary

- The local replica is authoritative for interactive reads and writes.
- Durable rows, domain payloads, tagged failures, backup records, and RPC wire values use Effect Schema codecs. Public
  TypeScript only control shapes such as backup options, snapshots, transport connections, and peer session handles do
  not expose codecs.
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

| Concern                  | Responsibility                                                                        | What it does not own                                                               |
| ------------------------ | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Canonical recovery state | Automerge changes, heads, checkpoints, tombstones, and command receipts               | Physical storage bytes or UI cache state                                           |
| Physical persistence     | SQLite transactions and database bytes, stored in OPFS in the browser                 | Merge semantics or permanent browser retention                                     |
| Local execution          | Serialized commands, durable replies, maintenance workflows, and recovery             | Replicated convergence                                                             |
| Derived durable views    | SQL projection tables optimized for application queries                               | Canonical history                                                                  |
| Ephemeral views          | Atom caches, RPC leases, and presence                                                 | Durable facts                                                                      |
| Connectivity             | Worker RPC, durable peer sessions, application supplied transports, and relay custody | Identity issuance, application routing policy, platform socket or server ownership |

The distinction follows Automerge's separation between a
[CRDT document and its storage adapter](https://automerge.org/docs/reference/repositories/storage/) and SQLite's
[transactional guarantees](https://www.sqlite.org/transactional.html). Automerge does not persist bytes by itself.
SQLite does not decide how concurrent document changes merge. Automerge `save` encodes compressed document bytes and
`load` reconstructs a document from those bytes. Neither operation writes the bytes to durable storage.

### Core terminology

| Term                         | Meaning                                                                                                                                                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Replica definition**       | The schema checked protocol blueprint for one application model. It names the document definitions, mutations, projections, and queries that must agree on a definition hash.                                                                 |
| **Replica service**          | The running Effect service for commands, canonical reads, projection queries, status, backup, and restore.                                                                                                                                    |
| **Document definition**      | The inert name, schema, and version descriptor returned by `Document.make`. It is not stored document data.                                                                                                                                   |
| **Document instance**        | One `DocumentId` with its Automerge state and causal history. It normally represents one aggregate whose invariants must change together.                                                                                                     |
| **Snapshot**                 | Effect Local's decoded read result for one document instance. It contains identity, version, current heads, tombstone state, projection state, and value.                                                                                     |
| **Heads**                    | The frontier change hashes that identify a point in Automerge document history. Heads are not revision numbers, storage bytes, or acknowledgements.                                                                                           |
| **Mutation definition**      | A named, schema checked domain operation for one document definition. Its handler changes an Automerge draft and can return a declared tagged domain error.                                                                                   |
| **Command**                  | One create, mutate, or delete request with incarnation scoped idempotency. Within the current replica incarnation, its stable command ID must be reused only for the same logical input.                                                      |
| **Automerge change**         | A causal group of CRDT operations produced or accepted by a document instance. A command can produce no new change, so commands and changes are not one to one.                                                                               |
| **Local database commit**    | The successful SQLite transaction that makes canonical state, projections, command evidence, and sequence metadata durable together. The default Cluster runtime also stores its reply in that transaction.                                   |
| **Commit event**             | Postcommit metadata used to invalidate dependent reads. It reports local durable work but is not itself the durability boundary.                                                                                                              |
| **Command receipt**          | Internal durable, replica incarnation scoped evidence that connects a command ID to a committed or rejected result. It is not reconstructed from document history. Public lookup methods expose the `CommandOutcome` proven by that evidence. |
| **Command outcome**          | `DurablyCommittedLocal`, a declared `Rejected` result, or `OutcomeUnknown` when the caller cannot prove what occurred. Operational and protocol failures remain `ReplicaError` values in the Effect error channel.                            |
| **Projection definition**    | A deterministic mapping from canonical snapshots to a rebuildable read model.                                                                                                                                                                 |
| **SQL projection binding**   | The projection table name, table migrations, delete operation, and row insertion operation. A projection table is a read model. A SQLite index is a separate query planner structure on that table.                                           |
| **Projection state**         | `Ready`, `Blocked`, or `Rebuilding` for a snapshot. Queries require their declared projection dependencies to be ready.                                                                                                                       |
| **Query definition**         | A named, schema checked read whose declared projection dependencies drive readiness and reactive invalidation. SQL is the standard handler implementation, not a requirement of the core type.                                                |
| **Invalidation key**         | Notification metadata for refreshing a document or projection read. It is not canonical data or a durability acknowledgement.                                                                                                                 |
| **Presence**                 | Expiring best effort metadata about connected peers or tabs. Presence is never durable state and must never authorize an operation.                                                                                                           |
| **Hosted canonical replica** | One ordinary `SqlReplica` running in a server process. It participates as a CRDT peer. It is not a transaction authority for other replicas.                                                                                                  |
| **Store and forward relay**  | The version `1` RPC topology. A stable sender outbox transfers custody to an injected durable backend store for at least once reconnect delivery.                                                                                             |
| **Relay acknowledgement**    | A fenced recipient response sent only after the production SQL sync workflow and sender scoped replay receipt commit. It can retire only the exact live claim.                                                                                |
| **Principal**                | A request scoped `tenantId`, `subjectId`, and stable `peerId` produced by `PeerAuthenticator`. It is derived from a credential on every RPC operation.                                                                                        |
| **Authorization lease**      | A bounded grant for exactly the requested whole document set. Expiry or invalidation terminates the session. It is not a distributed lock.                                                                                                    |
| **RPC session**              | One scoped live mapping between an authenticated peer and a SQL `PeerSession`. A replacement connection closes the previous incarnation.                                                                                                      |
| **RPC queue acceptance**     | Admission into a bounded in memory queue for the current live session. It is not remote SQLite durability, remote Automerge message application, peer observation, or custody.                                                                |

Automerge calls the document the unit of change and defines a change as a group of operations. See its
[concepts](https://automerge.org/docs/reference/concepts/) and
[binary format specification](https://automerge.org/automerge-binary-format-spec/). Effect Local adds definition,
identity, schema, projection, command, and transaction boundaries around that CRDT model.

### Identity and fencing

Identities answer different questions. They are not interchangeable version counters.

| Identity or generation | Meaning                                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReplicaId`            | Identity of the currently installed logical replica. Clone restore creates a new ID. Replace restore adopts the archive ID.                                          |
| `ReplicaIncarnation`   | Generation of the canonical contents. Replace restore advances it so stale sync state cannot enter the replacement.                                                  |
| `WriterGeneration`     | Persisted, increasing fencing token for the current durable writer. Old writers fail validation before they write.                                                   |
| Owner epoch            | Opaque identity of one browser owner runtime. It detects restart and stale invalidation streams. It is not the durable writer fence.                                 |
| `SessionId`            | Identity of a browser page lease or one live external peer session. The surrounding protocol defines which. A reconnect creates a new value.                         |
| `PeerId`               | Identity of an application selected synchronization peer.                                                                                                            |
| Connection epoch       | Identity of one peer sync connection. Reconnect creates a new epoch. Outbox rows and receipts belong to a specific epoch and are not resumed as the next connection. |
| `DocumentId`           | Stable identity of one document instance.                                                                                                                            |
| `CommandId`            | Stable identity and idempotency key of one logical command.                                                                                                          |
| `CommitSequence`       | Local sequence used for invalidation watermarks. It increases within an installed incarnation but can change on restore. It is not an Automerge history position.    |

The browser client session lease authorizes page to owner RPC calls. An external peer `SessionId` identifies only the
current authenticated and authorized live peer session. The browser owner epoch distinguishes invalidation
generations. The writer generation fences durable database writes. Only the writer generation mirrors the fencing
token pattern in the [Chubby lock service](https://storage.googleapis.com/gweb-research2023-media/pubtools/4444.pdf),
where an older acquisition count cannot authorize a newer write.

RPC session replacement uses a new `SessionId` and connection epoch. A reconnect never resumes the old Effect RPC
response stream, transient Automerge sync state, or prior epoch outbox. The fresh session creates new sync state and
new outbound envelopes from canonical Automerge history. Canonical history drives convergence. Connection state does
not.

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
[Context](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/Context.ts),
[Layer](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/Layer.ts),
[Scope](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/Scope.ts),
[Cluster Entity](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/unstable/cluster/Entity.ts),
[Workflow](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/unstable/workflow/Workflow.ts),
and [Atom](https://github.com/Effect-TS/effect/blob/eb9b10256c8558881b441c2fef833b7037174400/packages/effect/src/unstable/reactivity/Atom.ts).

### Connectivity and browser ownership

Transport neutral synchronization has three layers. The optional RPC package adds a contract and adapter:

| Layer              | Responsibility                                                                                                                                                     |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `PeerTransport`    | Application supplied connection, peer identity, routing, security, receive stream, send operation, and close operation.                                            |
| `PeerSync`         | Persistent receive identity, epoch scoped outbox, sequencing, replay, and received change application. Automerge `SyncState` remains in memory for one connection. |
| `PeerSession`      | Scoped orchestration that binds one transport connection to one replica incarnation and a selected set of whole document instances.                                |
| `PeerRpc`          | Durable version `1` `Open`, `Push`, `Acknowledge`, and `Reject` contract plus the generated Effect RPC client.                                                     |
| `RpcPeerTransport` | Adapter from the generated RPC client to `PeerTransport`, then to the existing SQL `PeerSession`.                                                                  |
| Relay SQL state    | Frontend SQLite outbox and receipts plus injected backend custody for at least once reconnect delivery.                                                            |

`observedByPeer` means Automerge's per peer sync state indicates that the peer has all local changes. It does not prove
remote storage durability. Effect Local therefore keeps durable confirmation separate and currently reports it as
false.

Effect RPC response acknowledgements bound streamed response chunks. They are not byte credits, durable receipts, or
proof that Automerge applied a message. `PeerRpcServer` adds item limits, byte reservations, quotas, rate limits, and
admission concurrency above the RPC stream. WebSocket ordering applies only within one live connection.

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

| Need                                      | Use                                                                   |
| ----------------------------------------- | --------------------------------------------------------------------- |
| Change one aggregate atomically           | One document mutation                                                 |
| Read one aggregate with causal metadata   | `Replica.get`                                                         |
| Filter, sort, or join local data          | Projection plus query                                                 |
| Keep React views current                  | `ReplicaAtom` builders                                                |
| Retry a command after losing the response | The same command ID, then the matching lookup method                  |
| Coordinate several durable steps          | Effect Workflow                                                       |
| Exchange changes with another replica     | `PeerSession` and an application supplied transport                   |
| Host a live authenticated replica peer    | `PeerRpcServer` plus application owned Effect RPC                     |
| Connect through the generated RPC client  | `RpcPeerTransport.makeSession`                                        |
| Bound a high churn document's checkpoint  | `ReplicaWorkflow.HistoryRewriteWorkflow`                              |
| Add asynchronous relay custody            | The opt in [store and forward](docs/store-and-forward.md) composition |
| Show cursors or online state              | Presence                                                              |
| Move or recover all local data            | Backup and restore                                                    |

The main design rule is simple: durable facts flow outward from canonical recovery state into projections and reactive
views. They never flow back from a cache, presence record, or transport connection into canonical history without a
schema checked command.

### Terms that require qualifiers

Some familiar words refer to multiple operations. The README uses the qualified form when the distinction matters.

| Qualified term               | Meaning                                                                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Canonical encoding utilities | Deterministic `Canonical.stringify`, `Canonical.hash`, and `Canonical.digest`. This module is not the canonical storage service.                                |
| Storage compaction           | Publish a verified Automerge save checkpoint and prune redundant SQL change rows while preserving logical document history.                                     |
| History truncation           | Deliberately discard causal history. Storage compaction never does this. `HistoryRewriteWorkflow` does, for one document, and only when an operator invokes it. |
| Document lineage             | The identity of one document's causal root. Genesis is the empty string. A history rewrite mints `lin_` plus a UUID.                                            |
| Library storage migration    | Library controlled migration of the internal SQL format.                                                                                                        |
| Projection table migration   | Application supplied DDL for one rebuildable projection table.                                                                                                  |
| Document schema evolution    | Change to a document definition version and value schema. Stored documents migrate in place at startup through the registered versioned decoder chain.          |

## Installation

Install only the packages used by the application surface. All five packages are ESM. Start with the core model and
protocol types:

```sh
pnpm add @lucas-barake/effect-local effect@4.0.0-beta.99 @automerge/automerge@3.3.2
```

For a durable Node replica, add the SQL engine and Node providers:

```sh
pnpm add @lucas-barake/effect-local-sql @effect/platform-node@4.0.0-beta.99 @effect/platform-node-shared@4.0.0-beta.99 @effect/sql-sqlite-node@4.0.0-beta.99
```

For live authenticated peer synchronization over Effect RPC, add the platform neutral RPC package. The application
must separately install the Effect platform package used to provide its socket and server.

```sh
pnpm add @lucas-barake/effect-local-rpc
```

For a durable browser replica, add the SQL and browser packages plus the OPFS driver:

```sh
pnpm add @lucas-barake/effect-local-sql @lucas-barake/effect-local-browser
pnpm add @effect/platform-browser@4.0.0-beta.99 @effect/sql-sqlite-wasm@4.0.0-beta.99 @effect/wa-sqlite@0.1.2
```

Effect Atom React bindings are optional:

```sh
pnpm add @effect/atom-react@4.0.0-beta.99
```

For tests, add the production shaped test package and the Effect Vitest integration. Add the Node platform package
when tests create command or peer IDs directly.

```sh
pnpm add -D @lucas-barake/effect-local-test @effect/vitest@4.0.0-beta.99 vitest@4.1.10
pnpm add -D @effect/platform-node@4.0.0-beta.99 @effect/platform-node-shared@4.0.0-beta.99
```

Package roles:

| Package                              | Purpose                                                                                                         |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `@lucas-barake/effect-local`         | Documents, mutations, projections, queries, backups, sync transport, and `Replica`                              |
| `@lucas-barake/effect-local-sql`     | SQLite persistence, durable execution, recovery, compaction, peer sync, sender relay outbox, and relay receipts |
| `@lucas-barake/effect-local-browser` | Effect Worker and RPC composition, OPFS ports, sessions, presence, and Atom builders                            |
| `@lucas-barake/effect-local-test`    | In memory production shaped replicas and deterministic bounded peer faults                                      |
| `@lucas-barake/effect-local-rpc`     | Durable relay protocol, injectable custody, policies, bounded server, and client adapter                        |

Package dependency direction is:

In this diagram, `A <- B` means package `B` depends on package `A`.

```text
@lucas-barake/effect-local
  <- @lucas-barake/effect-local-sql
       <- @lucas-barake/effect-local-browser
       <- @lucas-barake/effect-local-test
       <- @lucas-barake/effect-local-rpc
```

Browser, test, and RPC do not depend on one another. The RPC package stays platform neutral. The test package chooses
Node providers only for its production shaped in memory runtime.

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

The RPC package itself is platform neutral. A network deployment requires an Effect `RpcServer.Protocol` and
`RpcClient.Protocol`, the same `RpcSerialization` on both ends, a platform `Socket`, an HTTP server, TLS, WebSocket
upgrade routing, Origin policy, ingress byte and connection limits, credential issuance, secret rotation, tenant
routing, process supervision, and graceful shutdown. None of those responsibilities are inferred from
`PeerRpcServer.layerHandlers`.

The durable relay uses the bounded `PeerRelayIngress` protocol. The application owns TLS, routing, identity, policy,
process supervision, and the SQLite, PostgreSQL, or MySQL deployment that holds one relay shard.

## Composition recipes

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

When a document schema changes meaning, increment `version` and register a migration for every prior version that may
still be stored. Each migration decodes its source version with that version's schema and returns the next version's
value. The chain must be stepwise and gap free, which `Document.make` validates eagerly.

```ts
export const TaskV2 = Document.make("Task", {
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
      schema: Schema.Struct({
        title: Title,
        completed: Schema.Boolean,
        createdAt: Schema.Number,
        updatedAt: Schema.Number
      }),
      migrate: (value) => ({ ...value, priority: 0 })
    })
  ]
})
```

Migrations are local decode capability, not protocol surface: registering one does not change `definition.hash`. On
startup the replica migrates every stored document to the current version by appending a normal Automerge change —
history is never rewritten. See [docs/schema-evolution.md](docs/schema-evolution.md) for the full rules.

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

`definition.hash` covers names, versions, schemas, and query dependencies. Opening an existing replica under a changed
hash is allowed when every stored document version reaches the current version through its registered migration chain;
the stored hash is then updated and a startup workflow migrates stale documents and rebuilds changed projections. A
change that strands stored data — a removed document type with rows, or a version bump without a migration — still
fails bootstrap with `ProtocolMismatch`. Peer sync and restore continue to require an exact hash match.

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
for different input fails with `CommandIdConflict`. A lookup that targets a receipt written by a different operation
type or a different mutation fails with `ReceiptOperationMismatch` instead of decoding the stored outcome.

The full service surface is:

| Method                                                 | Result                                                                                        |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `create(document, { commandId, value })`               | `CommandOutcome<DocumentId>`                                                                  |
| `get(document, documentId)`                            | Decoded `Snapshot` with heads, version, tombstone, and projection state                       |
| `mutate(mutation, { commandId, documentId, payload })` | The mutation's typed `CommandOutcome`                                                         |
| `delete(document, { commandId, documentId })`          | `CommandOutcome<void>` and a durable tombstone                                                |
| `query(query, payload)`                                | The query's decoded success or declared error                                                 |
| `lookupCreate(document, commandId)`                    | Durable or unknown create outcome, `ReceiptOperationMismatch` otherwise                       |
| `lookupMutation(mutation, commandId)`                  | Durable, rejected, or unknown outcome for that mutation, `ReceiptOperationMismatch` otherwise |
| `lookupDelete(document, commandId)`                    | Durable or unknown delete outcome, `ReceiptOperationMismatch` otherwise                       |
| `flush`                                                | Publishes pending local commit invalidations                                                  |
| `status`                                               | Stream of `ReplicaStatus` values                                                              |
| `exportBackup({ maxBytes })`                           | Stream of bounded canonical archive chunks                                                    |
| `restoreBackup(options)`                               | Clone or replace restoration from a bounded stream                                            |
| `exportDocument(document, documentId)`                 | Schema encoded portable document value                                                        |
| `importDocument(document, { commandId, value })`       | A new local document and fresh causal history                                                 |

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

`Identity.makeCommandId`, the other effectful identity constructors, and `Canonical.digest` require Effect's `Crypto`
service. Browser applications provide `BrowserCrypto.layer`. Node applications and tests provide `NodeCrypto.layer`.

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
`ReplicaWorkflow.HistoryRewriteWorkflow`, `PeerSync`, `ReplicaGate`, and Effect Cluster `Sharding`. The owner side peer
services stay available so `PeerSession.make` can share the same durable runtime and fencing gate.

```ts
import { BrowserCrypto } from "@effect/platform-browser"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import { definition, MutationLive, QueryLive, TaskListSql } from "./domain.js"

const DomainLive = Layer.mergeAll(MutationLive, QueryLive)
const DomainDependencies = DomainLive.pipe(Layer.provideMerge(DatabaseLive))

const EngineCoreLive = SqlReplica.layerWithBindings(definition, {
  projections: [TaskListSql]
}).pipe(
  Layer.provide(DomainDependencies)
)

export const EngineLive = EngineCoreLive.pipe(
  Layer.provideMerge(
    Layer.merge(BrowserCrypto.layer, ReplicaLimits.layer(limits))
  )
)
```

`DatabaseLive` is any Effect v4 `SqlClient.SqlClient` Layer. `SqlReplica.layerWithBindings` also requires the Effect `Crypto`
service. In the browser these come from `BrowserSqlite.layer` or `BrowserSqlite.layerMessagePort` and
`BrowserCrypto.layer`. Node programs provide `NodeCrypto.layer`. The complete limits object is available as
`TestReplica.defaultLimits` for tests.

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

export const BrowserLive = BrowserReplica.layerWithReactivity(definition, {
  sessionTimeout: "10 seconds",
  operationTimeout: "30 seconds"
}).pipe(
  Layer.provide(Layer.merge(WorkerLive, BrowserCrypto.layer))
)
```

Session timeout applies to open, renewal, and close. Operation timeout applies independently to each unary RPC
attempt, including durable command receipt lookup. Restore is the exception. It uses one end to end operation deadline
across Begin, source transfer, Finish, cleanup, and stale session replacement. Status, invalidation, and backup export
streams are intentionally unbounded.

Browser restore protocol version 4 begins with a short unary request. The owner returns a restore nonce and dedicated
`MessagePort`. The page immediately starts a pending `FinishRestoreBackupV4` result RPC and sends `Start`. After the
owner has both signals, it drives archive input with one outstanding `Pull` credit. The page sends one bounded chunk
per credit. When the owner sends `TerminalReady`, the page accepts the authoritative Exit from the result RPC before
acknowledging the terminal. The owner then releases restore admission and sends `Released`. This lets the Begin RPC
worker slot return before the archive source is subscribed and preserves backpressure when the source uses the same
client.

Restore admission is fail fast and separate from ordinary stream, in flight, and queued RPC permits. The global limit
is `maxActiveRestores`. Each session is also bounded by `maxRestoresPerSession`. `maxRestoreMillis` bounds owner work
before cancellation begins. `maxRestorePullMillis` bounds individual protocol waits.
`maxRestoreCoalesceMillis` bounds how long a partial frame waits for more immediately available bytes.
`maxRestoreErrorBytes` bounds encoded source and result error details.

The `Attach` message is application ownership protocol, not hidden library behavior. Production applications must also
handle liveness, expiring provisioning nonces, OPFS worker creation, database port transfer, and provider loss.

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

| Status              | Meaning                                                     |
| ------------------- | ----------------------------------------------------------- |
| `Starting`          | Startup is in the named phase                               |
| `Ready`             | The local replica accepts work and reports pending commands |
| `ReadOnly`          | Writes are unavailable for the stated reason                |
| `Degraded`          | The replica is usable with a reported degradation           |
| `ProjectionBlocked` | A projection cannot represent accepted canonical state      |
| `Restoring`         | A restore is active and reports progress                    |
| `Failed`            | Startup or runtime ownership failed                         |

Runtime ownership is scoped. `Atom.runtime` does not own a dispose operation. React applications release mounted
atoms when `RegistryProvider` unmounts. Non React applications dispose their `AtomRegistry`. Applications separately
close control ports and terminate workers they created. Call `replica.flush` before an intentional shutdown when the
application wants pending invalidations published. It is not a substitute for browser lifecycle guarantees.

The SQL composition backs the stream with `ReplicaHealth`, which samples the durable tables once per second and
also receives pushed restore progress. It reports:

| Status              | Source                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------ |
| `Ready`             | `pendingCommands` is the unpublished commit-outbox depth, which `replica.flush` drains     |
| `Restoring`         | Bytes consumed while a backup is being ingested                                            |
| `ReadOnly`          | A restore is installing under the writer claim, or another writer generation has fenced it |
| `ProjectionBlocked` | A document is durably marked blocked for a projection this definition declares             |
| `Degraded`          | Storage was transiently unavailable on the last sample                                     |
| `Failed`            | Storage is corrupt, its metadata singleton is missing, or health sampling stopped          |

`Starting` is not emitted by the SQL composition: bootstrap completes while the layer graph is still building, so no
consumer can hold a `Replica` in time to observe it. It remains a public protocol value for other compositions.

The stream is long lived and never completes on its own, so consume it with a bounded operator such as
`Stream.take` or `Stream.runHead` rather than `Stream.runCollect`. It is a latest-wins level signal: subscribing
samples once so a late subscriber starts from the current value, repeated identical statuses are suppressed, and a
slow consumer converges on the newest status instead of replaying history. Closing the replica scope ends every live
subscriber. Each live subscriber holds one `maxStreamsPerSession` permit and one `maxInFlightPerSession` permit for
its lifetime, so budget for it alongside `invalidations`.

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

For conforming browser clients, each admitted restore retains at most one owner payload of `P`, one page staging buffer
of `P`, and the caller owned current emitted source batch. The client drains that batch before pulling another. `P` is
the smallest of the fixed 64 KiB page staging cap, the advertised `maxChunkBytes`, and the caller validated `maxBytes`.
Across the owner, archive payload retained by the transport is bounded by `R × P`.
`R` is the smaller of `maxActiveRestores` and `maxSessions × maxRestoresPerSession`. Active and finishing control
scopes are bounded by `2R`. Finishing scopes retain no archive payload. A hostile raw peer causes synchronous listener
removal and channel closure on the first invalid frame. Messages already queued by the browser before that closure are
outside the application bound. SQL archive staging and validation are separate and remain bounded by the existing
backup limits.

### 11. Add peer sync and presence

For a custom transport, the application provides `PeerTransport` and owns its identity, authentication, authorization,
encryption, discovery, and routing. For Effect RPC, do not implement another `PeerTransport`. `RpcPeerTransport`
supplies it from `PeerRpc.RpcClient`. `PeerAuthentication`, `PeerRelayAuthorization`, and the application owned socket
composition retain those responsibilities.

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
  capabilities: { lineageAware: true },
  connect: ({ peerId }) => openAuthenticatedConnection(peerId)
})
```

The application adapter must return the public `PeerTransport.Connection` shape with `peerId`, `capabilities`,
`receive`, `send`, and `close`. Its scope must release every transport resource. `PeerTransport.Capabilities` also
carries an optional `lineageAware`. It describes the **remote** peer, and an absent value means that peer does not
compare document lineage before it merges. `PeerSession` reads it and refuses to emit a rewritten document toward
such a peer, so an adapter must set it only when the remote end really performs that comparison.
`PeerSession.make` selects whole documents and connects the transport to durable `PeerSync` state.
`PeerSession.makeSupervised` exposes the same neutral session with `awaitDisconnect` for hosts that need the exact
terminal replica failure without a per session commit subscription. `PeerSession.makeLive` adds commit subscription
ownership.

`PeerSync` is the lower level durable protocol. `open` creates the local sync and outbox session for a peer. `receive`
takes that session plus the remote envelope epoch, sequence, and bytes. Its durable reply is session neutral. `enqueue`
idempotently binds the reply to the current local outbox before sending it. This separation lets Cluster replay a stored
reply after a restart without reusing a sequence from an expired connection. Most applications should use `PeerSession`,
which binds both epochs and enqueues replies automatically. Import the transport neutral session from the SQL package.
After `receive` commits a reply, `PeerSession` signals one scoped capacity one coalescing flush worker. The receive fiber
never waits for network send capacity. This preserves full duplex progress when the shared RPC response dispatcher is
backpressured. A session scoped map retains the exact returned outbound for live retransmission and coalesces duplicate
send sequences. Durable pending quotas bound its unique entries. Each drain merges that map with the durable outbox and
sends distinct first attempts by ascending send sequence. An exact retry may repeat an older sequence after a newer one.
The remote durable receipt identifies that retry as a duplicate. Generation drains each document before consuming the
next durable quota slot. A concurrent reply that fills the global outbox quota is drained before generation retries. An
intrinsic message or byte quota failure remains terminal. Typed failures and defects in receive, reply flush, or live
commit workers fail the supervised session. Pure scope interruption remains normal shutdown. The browser package keeps
its existing subpath as a compatibility reexport.

```ts
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
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

Concurrent writes for one peer resolve by arrival at the API boundary. Each `receive`, `publish`, and `remove` claims a
monotonic sequence when it is called, and a write is stored only when its sequence is newer than the newest call that
has already settled that peer, so a slow payload decode can no longer let an earlier call overwrite a later one. The
comparison survives the entry itself: a write that lost the race stays dropped even if the winner is removed, expires,
or is reclaimed by a `values` read before the loser finishes decoding.

Two consequences follow. A losing write is dropped silently and success is not an admission signal, so a peer should
hold at most one `publish` scope at a time; a losing `publish` holds no entry for the lifetime of its scope, and its
release removes nothing. And the ordering is over arrival at this process, not production at the remote peer: a
transport that reorders two updates from one peer still delivers the stale one last, and Presence will store it.
Ordering across a reordering transport requires a sequence carried in the payload, which is the application's
responsibility.

Expiry is enforced lazily when `values` reads. Entries stay resident until then, so a peer that only writes never
reclaims expired entries.

### 12. Host a durable relay over Effect RPC

The RPC package exposes one protocol, `PeerRpc` version `1`. Its procedures are `Open`, `Push`,
`Acknowledge`, and `Reject`. Every connection uses durable custody. There is no direct in memory RPC topology.

Applications provide `PeerAuthenticator`, `PeerRelayAuthorization`, `PeerRelayLimits`,
`PeerRelayIngress`, and `PeerRelayStore`. The semantic store is injectable. The built in
`SqlPeerRelayStore.layer` requires a generic `SqlClient` and supports SQLite, PostgreSQL, and MySQL. The frontend
replica, sender outbox, and recipient receipts remain SQLite.

`PeerRpcServer.layerHandlers({ tenantId, peerId })` builds the handlers. `PeerRpcServer.layerServer` supervises the
RPC server and bounded ingress. A successful `Push` means the backend store committed custody before replying.

### 13. Connect an RPC peer

Create `PeerRpc.RpcClient` through `PeerRpc.makeRpcClient`, then adapt it with
`RpcPeerTransport.layer` or `RpcPeerTransport.makeSession`. Options bind the expected local principal, relay peer,
remote peer, selected documents, replica incarnation, receipt retention, retry horizon, and replay batch size.

The connection always exposes the relay peer and an acknowledged delivery stream. `PeerSession` acknowledges only
after the production SQL sync workflow and sender scoped receipt commit. Lost responses or expired claims can
redeliver a message, so delivery is at least once and receipt suppression is durable.

Automerge `3.3.2` does not expose an allocation bounded semantic decode API. Relay admission therefore requires the
separate exact `UnsafeUnboundedAutomerge3DecodeGrant`. The recommended default remains
`PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode`.

See [Store and forward](docs/store-and-forward.md) for complete composition, custody, security, and failure semantics.

### 14. Run compaction and recovery workflows

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

The registered workflow journals one command receipt reclamation activity, then document listing, then one compact
activity per document. Each compact activity prunes changes only after it publishes a fresh checkpoint. Receipt
reclamation is independent of any document, so it runs first and is not skipped when a document fails to publish.
Recovery verifies checkpoint checksums, heads, change metadata, and
tombstones. It can fall back to an older verified checkpoint and replay accepted changes. `Compaction` and
`Recovery` are public injectable services for lower level engine assembly.

Installing a checkpoint is an optimistic compare and set against the global commit sequence, so a concurrent commit
anywhere in the replica can supersede it. Each document retries a bounded number of times. If every attempt is
superseded, the document is recorded and the run continues to the next document, and `poll` returns `Complete` with
a failed `Exit` carrying `ReplicaError` / `CheckpointSuperseded`. A failed exit therefore means **partial**
compaction, not zero: every document not named in `documentIds` was compacted and pruned normally. That guarantee is
specific to a superseded publish. Any other failure, such as a fenced replica or corrupt storage, still stops the
run at the document that raised it.

Retrying compaction requires a **new** `OperationId`. Execution identity is derived from the replica incarnation and
the operation id, and the result is journaled, so re-executing or resuming the same operation id replays the
recorded outcome instead of compacting again.

Backup and restore streams do not become durable merely because a Workflow is used.

#### Rewrite one document's history

Compaction bounds SQL change rows. It does not bound the checkpoint. `Automerge.save` encodes the whole change
graph, so one field overwritten N times keeps growing the saved bytes with N even after every redundant change row
is pruned. Automerge `3.3.2` exposes no API that removes history from a live document. The only way to bound such a
document is to rebuild it as a new document carrying the value the current one materializes to.
`ReplicaWorkflow.HistoryRewriteWorkflow` is that operation. It is registered by `SqlReplica.layer` alongside the
compaction workflow and it is never triggered automatically.

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
  return yield* workflows.poll(execution)
})
```

`execute` returns a durable `DocumentExecution` handle, not the new lineage. The rewrite outlives the caller, so the
minted lineage is read from the `Complete` result of `poll`. `interrupt` and `resume` behave as they do for
compaction, and every handle is rejected once the replica incarnation changes.

**Why the peers cannot simply merge the result.** Every Automerge ingestion path is a union. `merge`,
`applyChanges`, `loadIncremental`, and the sync protocol all add changes and none of them replaces content. A peer
that still holds the discarded history therefore puts it back. Its bloom filter is anchored at a change the
rewritten replica no longer has, so it sends a reset that asks for the sender's complete history, and the union
restores exactly what the rewrite removed. The rewritten value then loses, and it loses deterministically rather
than occasionally, because an Automerge register winner is ordered by Lamport operation counter first and by actor
id only on a tie. A rebuilt document starts at counter one against a surviving lineage at counter N. Measured
against Automerge `3.3.2`, the rewritten value survived 0 of 100 runs at every level of prior churn from one
upward. This is why the design refuses cross lineage synchronization instead of merging it.

**What a rewrite destroys.** All of it is permanent and none of it is recoverable from this replica.

- Every prior change and every prior checkpoint for that document.
- The writer provenance of every dropped change. The rewritten checkpoint carries provenance for exactly the one
  change the rebuild produced, attributed to this replica's current definition hash and to the schema version the
  stored document was encoded at.
- Register conflict alternatives. The rebuilt document carries the value `Automerge.toJS` exposes, which is the
  winner of each register. That winner is the only value this library has ever surfaced through `Snapshot`, so no
  value an application could read is lost. The losing alternatives that `Automerge.getConflicts` still reports on
  the old document are gone.
- That document's durable peer receipts and peer outbox rows.

**What a rewrite preserves.** The current decoded value, and the tombstone marker, so a deleted document is not
resurrected. Command receipts are deliberately retained, so a retried command that already committed is still
suppressed.

The rewrite refuses unless the document's canonical history is complete and settled. Materialized heads must equal
accepted heads, and there must be no unapplied change row, no peer receipt holding an undecoded pending message,
and no pending peer outbox row. It also refuses if the document advances between the read that prepares the rebuild
and the transaction that installs it. Every refusal happens before any destructive statement runs.

**Idempotency.** The workflow dedupes twice. `Workflow.execute` derives the execution identity from the replica
incarnation, the document ID, and the operation ID, so a repeated request resolves to the same execution and
replays its recorded result. Beneath that, the rewrite records the lineage it minted against
`(replica_incarnation, operation_id)` in the same transaction that performs the rewrite. A crash between that commit
and the journaling of the activity reply reruns the activity, and the marker makes the rerun return the first
lineage instead of minting a second one. A **new** operation ID performs a **second** rewrite and mints a second
lineage.

**Four contract points that are easy to get wrong.**

1. **An operation ID is not reusable across documents.** It is scoped to `(replica_incarnation, operation_id)` and
   is bound to the first document it rewrote. Reusing one against a different document fails with `ReplicaError` /
   `ProtocolMismatch` before any destructive work runs. Restore raises the incarnation, so a marker written before
   a restore can never satisfy a lookup after it.
2. **A rewritten document cannot be seeded to a brand new peer.** A peer that has never seen a document resolves
   its lineage as genesis, because a document it does not hold has no history for a rewrite to have discarded. A
   non genesis document therefore never matches. This is fail closed and deliberate. The alternative would be to
   adopt a lineage from whichever side spoke first.
3. **Peers are handled differently depending on what they advertise.** A peer that did not advertise
   `lineageAware` is never sent a rewritten document at all. Generation fails locally for that one document and the
   peer sees nothing, because a peer with no cross lineage check would union the rewritten document and push the
   discarded history straight back. A peer that did advertise it, and that holds a superseded lineage, is refused
   with a typed non retryable error scoped to that document. The session stays open and every other selected
   document keeps synchronizing. The refused document is dropped from the flush loop and its further inbound
   messages are discarded before they reach storage, so neither side spins.
4. **Lineage is an unauthenticated peer assertion.** It is a field on the sync envelope, it is not covered by the
   envelope's message digest, and nothing verifies that the peer is entitled to claim it. It is a correctness
   signal against an honest but stale peer. It is not a security control. A refusal must never be read as proof
   that the **local** document is stale. Confirm on this replica that a rewrite actually ran, through the workflow
   result or the document's own lineage, before acting on a refusal.

**Recovery for a refused peer replaces its obsolete canonical state.** Canonical archives carry lineage on every
document and checkpoint record, and restore preserves it. To repair a refusal, export the authoritative replica after
the rewrite and clone restore that archive onto the refused replica. Use replace restore only when the archive source
replica has been retired, because replace adopts the source replica identity. Either mode discards the refused
replica's old canonical state, including any unsynced change on the superseded lineage. Those changes have no causal
relationship to the new root and cannot be merged safely. This destructive recovery requirement is the main reason
history rewrite is opt in and operator driven rather than a background policy.

### 15. Write deterministic tests

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
`layerWithSyncAndLimits` use the lower level sync graph for deterministic peer tests.

For sync tests use `TestReplica.layerWithSync`, `TestPeer.layer`, and `FaultInjection.layerSequence`. Fault decisions
can deterministically drop, duplicate, delay, reorder, partition, heal, and flush bounded peer traffic. Effect's
`TestClock` controls delay and presence expiration without wall clock sleeps. `TestPeer.make` and `TestPeer.layer`
validate their bounds in the Effect error channel with the tagged `InvalidOptions` error.

## Durability and delivery contract

### State ownership

| State                                    | Owner                    | Durable | Replicated                                          | Rebuildable                                         |
| ---------------------------------------- | ------------------------ | ------- | --------------------------------------------------- | --------------------------------------------------- |
| Automerge changes                        | Canonical recovery store | Yes     | Yes, through selected peer sessions                 | No, this is the logical source of truth             |
| Verified checkpoint records and bytes    | Local recovery store     | Yes     | No. Their represented logical history can replicate | Not generally after prerequisite changes are pruned |
| Document heads                           | SQLite canonical store   | Yes     | Head hashes travel in sync messages                 | Yes, from retained Automerge history                |
| Command receipts                         | SQLite canonical store   | Yes     | No                                                  | No. Missing evidence yields `OutcomeUnknown`        |
| Projection tables                        | SQL projection bindings  | Yes     | No                                                  | Yes, from canonical snapshots                       |
| Cluster mailbox and replies              | Durable Cluster runtime  | Yes     | No                                                  | No, they resolve local execution ambiguity          |
| Workflow journals and activity replies   | Durable Workflow runtime | Yes     | No                                                  | No, they resume local orchestration                 |
| Atom values                              | Atom registry            | No      | No                                                  | Yes, from `Replica` reads and queries               |
| Presence and tab sessions                | Browser process          | No      | Best effort transport only                          | Not applicable                                      |
| RPC principals, leases, sessions, queues | RPC process scope        | No      | No                                                  | Recreated by authentication and synchronization     |
| Sender relay outbox                      | Sender SQLite replica    | Yes     | No                                                  | Removed after relay custody or retry horizon        |
| Relay custody and terminal evidence      | Backend relay SQL store  | Yes     | No                                                  | Active payload is not reconstructable after expiry  |
| Recipient relay receipts                 | Recipient SQLite replica | Yes     | No                                                  | Bounded replay evidence, not canonical CRDT history |

### Boundary guarantees

Consistency guarantees:

| Boundary                 | Guarantee                                                                                                                            |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| One document command     | Serialized through its Cluster entity and committed with canonical state, projections, receipt, sequence, and stored reply           |
| Command retry            | Within one replica incarnation, the same command ID and canonical request returns the durable result                                 |
| Different command input  | Within one replica incarnation, reusing a command ID for different input fails                                                       |
| Query                    | Reads local projection state under the replica operation gate                                                                        |
| Multi document invariant | Not transactional. Model one aggregate document or an explicit Workflow                                                              |
| Peer convergence         | Replicas converge after receiving the same valid Automerge change set                                                                |
| Cross lineage sync       | Refused, never merged. A rewritten document stops synchronizing with every peer that holds the superseded lineage                    |
| Restore                  | Exclusive, fenced, staged, schema checked, and projection rebuilding                                                                 |
| Atom invalidation        | Reactive cache refresh, not a durability acknowledgement                                                                             |
| Presence                 | Expiring best effort state with no durability guarantee                                                                              |
| Automerge observation    | The peer's sync state reports all current local changes observed. It does not prove remote storage durability                        |
| RPC `Open` handshake     | One authenticated, authorized, bounded durable session exists for exactly the selected whole documents                               |
| RPC `Push` success       | The backend relay store committed the complete envelope and quota reservation before replying                                        |
| Relay recipient ack      | The recipient SQL sync workflow and sender scoped receipt committed before the fenced acknowledgement was attempted                  |
| Relay delivery           | At least once within configured retry, expiry, retention, authorization, and capacity boundaries                                     |
| Relay poison bound       | Delivery attempts are durable. Reaching `RelayInbox.Options.maxDeliveries` dead letters the row, which stops it blocking its channel |
| Effect RPC stream ack    | The response chunk was acknowledged by the RPC protocol. It is neither a byte credit nor an application receipt                      |
| WebSocket frame order    | Frames are ordered within one live RFC 6455 connection. Reconnect, replay, authorization, and persistence are separate               |
| `durableConfirmation`    | Always `false` in the shipped peer transports                                                                                        |

### Retry and ambiguity

- Within the current replica incarnation, reuse a `CommandId` only for the same logical create, mutation, delete, or
  import input. Same identity plus same canonical request hash resolves the durable receipt. Different input fails
  `CommandIdConflict`. Restore can cross the incarnation boundary, after which the current permit can report
  `OutcomeUnknown` for evidence retained only by the archived incarnation. Compaction reclaims receipts from
  superseded incarnations, so that archived evidence does not survive the next compaction run and is absent from
  every later export. A permit captured before the boundary can read it until then, and reports `OutcomeUnknown`
  afterwards.
- After `OutcomeUnknown`, call the matching lookup method with the same definition and command ID. If the replica
  incarnation changed, prior receipt protection is not available through the current permit. Application policy must
  decide whether a later request is a new logical operation.
- Peer messages may duplicate. Persistent receive identity and Automerge merge semantics make exact retransmission
  safe. This is endpoint deduplication, not exactly once transport delivery.
- Retry `RpcPeerTransport` only for `StorageUnavailable`. Rebuild the complete connection scope. Policy and protocol
  mismatches are not transient.
- Retry relay admission with the same stable `RelayMessageId` and exact outer envelope. A lost custody response,
  recipient acknowledgement, reconnect, or expired claim can duplicate delivery. Sender scoped recipient receipts
  suppress repeated application during the negotiated retention window.
- A relay delivery advances the durable attempt count once the message has provably reached the transport, so a
  message prepared for a channel and then abandoned on a flaky connection costs nothing. Reaching
  `RelayInbox.Options.maxDeliveries` dead letters the message so it stops blocking its channel. Restart does not
  reset the count.
- Durable commit events are retried until publication into the bounded process local stream. Subscriber delivery is
  best effort because the stream uses sliding capacity. A later sequence gap, `FullRefreshRequired`, or refresh
  generation change instructs the subscriber to reread canonical state. If no later event arrives, no notification
  guarantee repairs a dropped event.

### Required invariants

1. A replica definition name, document schemas, operation schemas, versions, projection dependencies, and definition
   hash must match the installed canonical data.
2. Values crossing a document boundary must encode to supported Automerge values and decode through the declared
   Effect Schema.
3. Exactly one valid writer generation may commit to a canonical database. Restore advances the replica incarnation
   and fences stale operations.
4. Native Automerge documents loaded by advanced storage APIs must have deterministic ownership and exactly one
   `free`, using an acquisition bracket or a `Scope` finalizer.
5. Peer selection is a nonempty set of unique whole document identities. Subtree synchronization is unsupported.
6. Authentication is evaluated for every RPC operation from the decoded redacted payload credential. Upgrade headers,
   caller payload credentials, and Effect RPC client IDs are not principals.
7. Authorization must return exactly the requested set resolved to server owned definitions. Partial grants, enlarged
   grants, duplicate requests, duplicate results, expired grants, and nonfinite deadlines fail closed.
8. One server Layer binds one tenant ID, one stable server `PeerId`, and one canonical SQL replica. Principal tenant IDs
   must match that configured tenant.
9. Per connection, per subject, per session, per item, and total byte limits must remain finite and positive. The
   configured byte capacities must hold
   `PeerSession.maximumSyncEnvelopeBytes(maxSyncMessageBytes, maxSyncChangesPerMessage)`. The bound reserves JSON
   space for the encoded sync message and every per change writer provenance entry.
10. All long lived connections, subscriptions, streams, queues, fibers, and server sessions must be owned by an Effect
    `Scope`. Native documents may instead use a deterministic acquisition bracket. Scope close is cleanup. It is not
    persistence.
11. Relay ordering is FIFO only within one exact tenant, sender subject, sender peer, sender replica incarnation,
    recipient subject, and recipient peer channel.
12. One SQLite process and durable volume must own each relay shard. The application must not run a second authority
    or open that database through a network file system.
13. Relay authentication and ordinary document authorization must not imply unsafe Automerge resource trust. The
    explicit grant must match the principal, resolved remote, direction, complete document set, finite lease, and
    revocation Effect.
14. Relay infrastructure validates bounded opaque envelope structure, routing, hashes, digest, and provenance shape.
    Relay enabled `PeerSync` performs the one semantic Automerge decode.
15. After fresh ordinary and unsafe grants, relay policy revocation and the bounded operation contend on a local gate.
    Revocation admitted first prevents SQL mutation or payload emission. An operation admitted first may finish and
    returns its real result. Revocation drains that in flight operation and blocks later work.

### Invalid compositions

- Do not combine descriptors from different replica definitions or install projection and query dependencies not
  registered in the definition.
- Do not provide a mutation or query handler Layer for the wrong generated service, or omit a registered handler.
- Do not bind two SQL projections to the same physical table, skip binding migrations, or treat a SQLite index as a
  projection binding.
- Do not reuse one command ID for different request bytes or domain intent.
- Do not reuse one history rewrite operation ID across documents, and do not retry a rewrite under a new operation ID
  expecting the first one to be undone. A new operation ID performs a second rewrite.
- Do not use presence, Atom state, invalidation events, RPC client IDs, WebSocket headers, or transport connection state
  as authorization evidence.
- Do not share one RPC handler Layer across tenants when the underlying SQL replica has no tenant column.
- Do not treat `Push`, WebSocket, stream acknowledgement, `markSent`, or `observedByPeer` as remote durable custody.
- Do not treat store and forward as exactly once delivery. A stale claim may duplicate delivery, but its old token
  cannot retire a newer claim.
- Do not run two relay custody authorities for one shard or infer automatic failover, quorum, or split brain recovery.
- Do not grant `UnsafeUnboundedAutomerge3DecodeGrant` merely because a peer is authenticated or document authorized.
- Do not resume an `Open` stream or Automerge connection state after reconnect. Create a fresh scope and session.
- Do not keep the browser SQLite connection in page code or open the same OPFS database outside the elected owner.
- Do not add a second Automerge repository or backend materialization and call it canonical. Projections remain
  rebuildable and the SQL Automerge history remains the only source of truth.

## Limits and security

This beta deliberately provides building blocks rather than a complete collaboration product.

- The RPC package provides one durable store and forward protocol. It does not provide a managed backend, peer
  discovery, account system, credential issuer, or tenant registry.
- The application owns credential issuance and rotation, authenticator and authorization implementations, stable peer
  identity assignment, tenant routing, TLS, Origin validation, ingress controls, logging policy, and end to end
  encryption when required.
- Credentials are `Redacted<string>` values in decoded request and service APIs. Serialization necessarily encodes the
  credential for transport, so deployments must use TLS. Client middleware overwrites a caller supplied credential.
  Server middleware reads only the decoded RPC payload. Applications must not log credentials or include them in
  telemetry.
- Unexpected RPC defects encode only `{ _tag: "InternalError" }`. Credential, tenant, subject, peer, session, document
  identity, and payload content must not be attached to spans or metrics. Application logs must preserve the same rule.
- Authentication and authorization leases are upper bounds on reuse. Their invalidation Effects may terminate them
  earlier. They do not revoke data already synchronized to an authorized replica.
- Automerge `3.3.2` does not expose allocation bounded semantic decode. Relay policy must deny
  `authorizeUnsafeUnboundedAutomerge3Decode` unless the application controls and resource trusts the producer bytes.
  Ordinary authentication and document authorization are insufficient. The exact grant binds principal, remote,
  direction, documents, finite expiry, and revocation. A future allocation bounded Automerge API should replace this
  exception and remove the unsafe grant.
- Relay revocation is not atomic with SQLite. It does not retroactively cancel an operation that already won the local
  gate. That bounded operation may finish its durable commit or delivery and returns its real result. Revocation drains
  it and prevents later operations. If revocation wins the gate first, no SQL mutation or payload emission occurs.
- `AuthenticationFailure` and `AccessDenied` intentionally disclose no reason. `UnsupportedVersion`, `PeerMismatch`,
  `InvalidRequest`, and limit errors are typed protocol failures. Capacity and session availability errors are live
  resource failures.
- A missing session and a session owned by another tenant, subject, or peer both return the same fieldless
  `SessionUnavailable`. Applications must not wrap this result with ownership or existence detail because that creates
  a session enumeration oracle.
- `PeerRelayLimits.defaults` are conservative library defaults, not product capacity planning. Configure them with
  process memory, expected document size, ingress limits, database capacity, and subject quotas.
- OPFS is origin scoped. Browser storage starts as best effort, can be evicted unless persistence is granted, and is
  deleted when the user clears the site's storage.
- Existing secondary tabs do not yet promote themselves when the provisioning tab disappears. A new attachment can
  reprovision the owner.
- Document schema evolution is not implemented in place. The exact replica definition hash is pinned. Library storage
  migrations and projection table migrations are separate supported mechanisms.
- An old backup can require a matching application build until versioned migration support exists.
- One mutation targets one document. There is no replicated transaction across documents.
- Whole document sync is the only sync granularity. Subtree sync is not implemented.
- Document lineage is an unauthenticated peer assertion. It is carried on the sync envelope, it is not covered by the
  envelope's message digest, and no authorization check restricts what a peer may claim. It exists to stop an honest
  but stale peer from silently resurrecting discarded history. It is not evidence about the peer and must not be used
  as one. A refusal is not proof that the local document is stale. Confirm locally that a rewrite ran first. A
  forged lineage costs the forger that one document in that one session and nothing else.
- `RpcPeerTransport.layer` and `makeSession` expose only the durable acknowledged topology after the version `1`
  handshake.
- Conflict inspection, history browsing, sharing policy, and resolution UI belong to the application.
- Presence is not durable awareness and must not carry authorization decisions.
- Limits must be selected for the product. They bound backup bytes, archive records, JSON depth, sync messages,
  dependency graphs, pending changes, peers, sessions, RPC streams, queues, retained rate state, in flight work,
  replica gate admission, and total buffered bytes.
- `maxQueuedPermits` counts every acquirer waiting on the replica gate, including the unbounded internal ones from
  peer sync and peer sessions. Those ceil at roughly three to four per session, so size it comfortably above
  `maxSessions`. At the default of `1_024` against `maxSessions = 32` there is ample headroom; setting it near that
  internal ceiling lets peer traffic shed local operations while a restore holds the writer.

### RPC deployment responsibilities

| Owner                  | Required responsibility                                                                                           |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Application            | Credentials, authentication, relay authorization, tenant mapping, stable peer IDs, reconnect policy               |
| Effect RPC composition | `RpcClient.Protocol`, `RpcServer.Protocol`, serialization, request and stream lifecycle                           |
| Platform               | Socket and WebSocket implementation, HTTP server, TLS, DNS, process signals                                       |
| Ingress                | Origin policy, connection limits, upgrade timeout, maximum frame and request bytes, load shedding                 |
| Effect Local RPC       | Durable version `1`, required auth middleware, bounded sessions, custody adapter, and limits                      |
| SQL replica            | Canonical Automerge state, durable peer outbox and receipts, fencing, recovery, projections                       |
| Operations             | Secret rotation, telemetry redaction, capacity configuration, backup policy, incident response, graceful shutdown |

The RPC server's in process quotas do not replace upstream controls. Reject oversized WebSocket handshakes and frames
before decoding. Isolate tenant replicas structurally. Close the server Layer scope during shutdown and allow its
bounded number of concurrent session cleanups to complete before terminating the process. Cleanup duration itself is
not bounded. Operations that require a hard process deadline must enforce it outside the Layer and decide when to force
termination.

Read [architecture](docs/architecture.md), [durability](docs/durability.md), [sync](docs/sync.md),
[store and forward](docs/store-and-forward.md), and [schema evolution](docs/schema-evolution.md) for the detailed
contracts.

## Non goals

- Globally replicated relay custody, automatic failover, quorum operation, shard rebalance, and cross region routing.
- Backend owned application state outside the canonical Automerge SQL replica.
- NATS, Kafka, broker, Cluster, or Workflow orchestration for live network sessions.
- Automatic token issuance, user storage, tenant registry, policy language, peer discovery, or sharing UI.
- A package owned WebSocket client, HTTP server, router, serializer, TLS layer, or process supervisor.
- End to end encryption, key agreement, key recovery, or forward secrecy protocols.
- Replicated transactions across documents, subtree synchronization, or in place document schema evolution.
- Automatic, scheduled, or threshold triggered history truncation, and any path that lets a rewritten document
  resume synchronizing with a peer that still holds the superseded lineage.
- Remote durability confirmation. The current `durableConfirmation` API returns only `false`.
- Protocol compatibility with versions other than the declared direct and relay protocol versions.
- React APIs in the RPC package. React consumers continue to use `ReplicaAtom` over their local replica.

## Repository scripts

Run commands from the repository root.

| Script                  | Purpose                                                           |
| ----------------------- | ----------------------------------------------------------------- |
| `pnpm bench`            | Run ordinary tracked library benchmarks once                      |
| `pnpm build`            | Build all publishable packages with TypeScript project references |
| `pnpm check`            | Type check packages and tests                                     |
| `pnpm check:all`        | Add the native TypeScript preview check to local commit checks    |
| `pnpm check:pre-commit` | Run nonmodifying local commit checks once                         |
| `pnpm check:tsgo`       | Type check with the TypeScript native preview                     |
| `pnpm dead-code`        | Find unused private files, exports, and dependencies              |
| `pnpm lint`             | Run oxlint and check dprint formatting                            |
| `pnpm lint-fix`         | Apply lint fixes and dprint formatting                            |
| `pnpm test --run`       | Run the unit and integration suite once                           |
| `pnpm test:browser`     | Run browser ownership, OPFS, transfer, and restart contract tests |
| `pnpm coverage`         | Run Vitest with V8 coverage                                       |
| `pnpm circular`         | Check package sources for circular imports                        |
| `pnpm codegen`          | Regenerate package barrel modules                                 |
| `pnpm docgen`           | Compile documentation through the root TypeScript graph           |
| `pnpm clean`            | Remove generated build artifacts                                  |

`pnpm bench` excludes `PeerRpcServerPerformance.bench.ts`. That harness imports an instrumentation module created only
by `base-admission-instrumentation.patch` or `candidate-admission-instrumentation.patch` inside detached benchmark
worktrees. It must never run against or add hooks to the shipped source. After applying the matching patch, run it with
`pnpm exec vitest bench --run --config packages/local-rpc/bench/vitest.admission.config.ts`. Base and candidate must use
the same harness, dependency lock, sample count, and machine.

## Public API inventory

Every root package exports module namespaces. Every module is also available through its public subpath, such as
`@lucas-barake/effect-local/Replica`. `internal/*` is explicitly private.

The export surface supports several assembly levels. Most applications start with everyday domain and composition
modules. Feature and advanced services remain public for products that need direct control.

| Level                       | Modules                                                                                                                                                                                               |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Everyday domain             | `Document`, `DocumentSet`, `Mutation`, `Projection`, `Query`, `ReplicaDefinition`, `Replica`, `CommandOutcome`, `Snapshot`, `Identity`, `ReplicaStatus`, `Backup`                                     |
| Durable runtime             | `SqlProjection`, `SqlReplica`, `BrowserReplica`, `BrowserSqlite`                                                                                                                                      |
| Reactive and test adapters  | `ReplicaAtom`, `TestReplica`                                                                                                                                                                          |
| Optional features           | `PeerTransport`, `PeerSession`, `Presence`, `ReplicaWorkflow`, `TestPeer`, `FaultInjection`, `PeerRpc`, `RpcPeerTransport`                                                                            |
| RPC policy and server       | `PeerCredentials`, `PeerAuthenticator`, `PeerAuthentication`, `PeerRelayAuthorization`, `PeerRelayLimits`, `PeerRelayIngress`, `PeerRelayStore`, `SqlPeerRelayStore`, `PeerRpcServer`, `PeerRpcError` |
| Advanced assembly           | `CommitPublisher`, `Compaction`, `Recovery`, `PeerSync`, `DurableRuntime`, `ReplicaClient`, `ReplicaOwner`, `SessionManager`                                                                          |
| Advanced framework assembly | `CommandExecutor`, `DocumentEntity`, `DocumentStore`, `EntityReplica`, `Migrations`, `ProjectionStore`, `QueryExecutor`, `ReplicaBootstrap`, `ReplicaGate`, `ReplicaRpc`                              |

These public framework assembly modules support custom runtimes and diagnostics. Paths under `internal/*` remain
private and unsupported. The public assembly modules are not required for the normal application path shown in the
composition recipes.

### `@lucas-barake/effect-local`

| Namespace           | Public API                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Backup`            | `FormatVersion`, `Header`, `ExportOptions`, `RestoreOptions`, `ExportedDocument`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `Canonical`         | `stringify`, `hash`, `digest`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `CommandOutcome`    | `Rejected`, `DurablyCommittedLocal`, `OutcomeUnknown`, `CommandOutcomeUnknown`, `CommandOutcome`, `schema`, `rejected`, `durablyCommitted`, `unknown`, `match`, `committedOrFail`                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `Commit`            | `Heads`, `Commit` and their inferred types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Document`          | `WireSchema`, `AutomergeEncoded`, `DocumentSchema`, `Document`, `Any`, `make`, `isAutomergeValue`, `decode`, `encode`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `DocumentSet`       | `DocumentSet`, `make`, `get`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `Identity`          | Schemas and types for `ReplicaId`, `ReplicaIncarnation`, `SessionId`, `DocumentId`, `CommandId`, `RelayMessageId`, `WriterGeneration`, `CommitSequence`, `PeerId`, `ProjectionVersion`, and `DocumentLineage`. `genesisLineage`, identity generators including `makeRelayMessageId`, `makeDocumentLineage`, `documentIdFromCommandId`                                                                                                                                                                                                                                                                                |
| `Mutation`          | `DraftValue`, `Draft`, `SuccessResult`, `HandlerResult`, `HandlerOptions`, `Handler`, `HandlerService`, `Mutation`, `Any`, `make`; definitions expose `payloadSchema`, `successSchema`, `errorSchema`, `of`, and `toLayer`; `toLayer` accepts a handler or an Effect that builds one                                                                                                                                                                                                                                                                                                                                 |
| `PeerTransport`     | `Capabilities`, `PermanentRejectReason`, `RelayDeliveryIdentity`, `AcknowledgedDelivery`, `Connection`, `ConnectOptions`, `PeerTransport` service                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `Projection`        | `Projection`, `Any`, `make`, `assertUniqueKeys`, `evaluate`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Query`             | `Handler`, `HandlerService`, `Query`, `Any`, `make`; definitions expose `payloadSchema`, `successSchema`, `errorSchema`, `of`, and `toLayer`; `toLayer` accepts a handler or an Effect that builds one                                                                                                                                                                                                                                                                                                                                                                                                               |
| `Replica`           | `Replica` service                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `ReplicaDefinition` | `ReplicaDefinition`, `Any`, `invalidationKeys`, `make`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `ReplicaError`      | Reason schemas `DocumentNotFound`, `DocumentDecodeError`, `DocumentEncodeError`, `UnsupportedDocumentVersion`, `ProjectionBlocked`, `CommandIdConflict`, `ReceiptOperationMismatch`, `StorageUnavailable`, `CanonicalEncodeError`, `StorageCorrupt`, `QuotaExceeded`, `MigrationFailed`, `BackupInvalid`, `BackupTooLarge`, `RestoreBusy`, `RestoreFailed`, `ProtocolMismatch`, `ReplicaFenced`, `OperationTimeout`, `UnsupportedStorageFormatVersion`, `CheckpointSuperseded`, `DocumentLineageChanged`. Causal reasons use `Schema.Defect()` for transportable arbitrary failures. `Reason`, tagged `ReplicaError` |
| `ReplicaLimits`     | `Values`, `minimumRestoreErrorBytes`, `ReplicaLimits` service, `make`, `layer`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ReplicaStatus`     | `Starting`, `Ready`, `ReadOnly`, `Degraded`, `ProjectionBlocked`, `Restoring`, `Failed`, `ReplicaStatus` schemas and types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Snapshot`          | `ProjectionState`, `Snapshot`, `FromDocument`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

`Replica.Replica` is the application capability. Its methods are `create`, `get`, `mutate`, `delete`, `query`,
`lookupMutation`, `lookupCreate`, `lookupDelete`, `flush`, `status`, `exportBackup`, `restoreBackup`,
`exportDocument`, and `importDocument`. Create, mutate, delete, and import require caller supplied command IDs.
Commands return `CommandOutcome`. Query retains its declared error together with `ReplicaError`. Status and backup
export are Streams. Restore may require the environment of its source Stream.

Core data shapes:

| Value                             | Exact fields                                                                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `Backup.Header`                   | `formatVersion: 1`, `definitionHash`, `replicaId`, `incarnation`, `createdAt`                                                   |
| `Backup.ExportOptions`            | TypeScript only `{ maxBytes }`                                                                                                  |
| `Backup.RestoreOptions<R>`        | TypeScript only `{ source: Stream<Uint8Array, ReplicaError, R>, mode: "clone" \| "replace", maxBytes, expectedDefinitionHash }` |
| `Backup.ExportedDocument<E>`      | TypeScript only `{ documentName, schemaVersion, value }`                                                                        |
| `Commit.Commit`                   | `{ commandId, documentId, heads: ReadonlyArray<string>, sequence }`                                                             |
| `Snapshot.Snapshot<A>`            | TypeScript only `{ documentId, value, version, heads, tombstone, projection: "Ready" \| "Blocked" \| "Rebuilding" }`            |
| `ReplicaStatus.Starting`          | `{ _tag: "Starting", phase }`                                                                                                   |
| `ReplicaStatus.Ready`             | `{ _tag: "Ready", pendingCommands }`, where the count is nonnegative                                                            |
| `ReplicaStatus.ReadOnly`          | `{ _tag: "ReadOnly", reason }`                                                                                                  |
| `ReplicaStatus.Degraded`          | `{ _tag: "Degraded", reason }`                                                                                                  |
| `ReplicaStatus.ProjectionBlocked` | `{ _tag: "ProjectionBlocked", projection, reason }`                                                                             |
| `ReplicaStatus.Restoring`         | `{ _tag: "Restoring", processedBytes }`, where the count is nonnegative                                                         |
| `ReplicaStatus.Failed`            | `{ _tag: "Failed", message }`                                                                                                   |

`ReplicaError` is `{ _tag: "ReplicaError", reason }`. Exhaustive reason payloads:

| Reason tag                                                                                       | Fields after `_tag`                                 |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `DocumentNotFound`                                                                               | `documentId`                                        |
| `DocumentDecodeError`, `DocumentEncodeError`                                                     | `documentId`, `cause`                               |
| `UnsupportedDocumentVersion`                                                                     | `documentId`, `observedVersion`, `supportedVersion` |
| `ProjectionBlocked`                                                                              | `projection`, `cause`                               |
| `CommandIdConflict`                                                                              | `commandId`                                         |
| `ReceiptOperationMismatch`                                                                       | `commandId`, `expected`, `observed`                 |
| `StorageUnavailable`, `CanonicalEncodeError`, `StorageCorrupt`, `BackupInvalid`, `RestoreFailed` | `cause`                                             |
| `QuotaExceeded`                                                                                  | `resource`, `limit`                                 |
| `MigrationFailed`                                                                                | `migration`, `cause`                                |
| `BackupTooLarge`                                                                                 | `limit`, `observed`                                 |
| `RestoreBusy`                                                                                    | `replica`                                           |
| `ProtocolMismatch`                                                                               | `expected`, `observed`                              |
| `ReplicaFenced`                                                                                  | `expectedGeneration`, `observedGeneration`          |
| `OperationTimeout`                                                                               | `operation`, `timeoutMillis`                        |
| `UnsupportedStorageFormatVersion`                                                                | `observedVersion`, `supportedVersion`               |
| `CheckpointSuperseded`                                                                           | `documentIds`, `attempts`                           |
| `DocumentLineageChanged`                                                                         | `documentId`, `localLineage`, `remoteLineage`       |

`CheckpointSuperseded` carries every document whose checkpoint publication lost the install race, and `attempts` is
the number of publish attempts made **per document**, not a total across them.

`DocumentLineageChanged` reports that this replica and a peer disagree about which causal root a document has. Both
lineage fields are `Identity.DocumentLineage`, so the genesis value reads as the empty string. On the send side the
peer reported nothing, and `remoteLineage` is that genesis value rather than anything the peer claimed.

Fields named `cause` use `Schema.Defect()` and can cross the local serialization boundary. The external peer RPC does
not expose them. It maps unexpected defects to its fixed `InternalError` sentinel.

Core constructor rules:

| Constructor              | Defaults and validation                                                                                                                                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Document.make`          | Requires a nonempty durable name, an Automerge encodable schema, and a positive safe integer version                                                                                                                      |
| `DocumentSet.make`       | Variadic. Rejects duplicate document names. `get` may return `undefined`                                                                                                                                                  |
| `Mutation.make`          | Payload `Schema.Void`, success `Schema.Void`, error `Schema.Never`, version `1`. Rejects names starting with `$`. Handler is synchronous and mutates one Automerge draft                                                  |
| `Projection.make`        | Validates only a nonempty name and positive safe integer version. The caller must make `project` deterministic. `Projection.evaluate` decodes every row through `Row` and rejects duplicate `key` values                  |
| `Query.make`             | Payload `Schema.Void`, success `Schema.Void`, error `Schema.Never`, version `1`. `dependsOn` is required and unique by projection name                                                                                    |
| `ReplicaDefinition.make` | Omitted mutations, projections, and queries become empty collections. Rejects duplicate names and every unregistered cross reference                                                                                      |
| `ReplicaLimits.make`     | Decodes every configured bound as a positive integer, requires `maxRestoreErrorBytes >= minimumRestoreErrorBytes`, and requires `maxRestoreCoalesceMillis < maxRestorePullMillis`. `layer` installs the validated service |

Mutation and query descriptors each expose a generated handler service, `of`, and `toLayer`. `toLayer` accepts either
a handler or an Effect that builds one, preserving its Context requirements. Definitions are inert. Installing a
descriptor does not install its handler.

### `@lucas-barake/effect-local-sql`

| Namespace                | Public API                                                                                                                                                                                                                                                                                                                |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BackupStore`            | `BackupStore` service, `layer`                                                                                                                                                                                                                                                                                            |
| `CommandExecutor`        | `createRequestHash`, `mutationRequestHash`, `deleteRequestHash`, `CommandExecutor` service, `MutationHandlers`, `layer`                                                                                                                                                                                                   |
| `CommitPublisher`        | `CommitEvent`, `CommitSubscription`, `CommitPublisher` service, `layer`                                                                                                                                                                                                                                                   |
| `Compaction`             | `OperationId`, `PreparedCheckpoint`, `CompactResult`, `Compaction` service, `layer`                                                                                                                                                                                                                                       |
| `DocumentEntity`         | Cluster RPC definitions `Create`, `Mutate`, `Delete`, `ApplySync`, plus `ApplySyncResult`, `DocumentEntity`, `layer`                                                                                                                                                                                                      |
| `DocumentStore`          | `Stored`, `DocumentStore` service, `layer`                                                                                                                                                                                                                                                                                |
| `DurableRuntime`         | `layer`, `layerWith`, `layerRelay`                                                                                                                                                                                                                                                                                        |
| `EntityReplica`          | `layer`                                                                                                                                                                                                                                                                                                                   |
| `Migrations`             | `canonicalStoreChecksum`, `peerSyncChecksum`, `durabilityIndexesChecksum`, `projectionReadinessChecksum`, `pendingReceiptIndexesChecksum`, `peerWriterProvenanceChecksum`, `replicaHealthIndexesChecksum`, `documentLineageChecksum`, `historyRewriteMarkersChecksum`, `peerRelayStateChecksum`, `loader`, `run`, `layer` |
| `PeerRelayClientRuntime` | `ConnectionConfiguration`, `PeerRelayClientRuntime` service, `makeScoped`, `layer`                                                                                                                                                                                                                                        |
| `PeerRelayOutbox`        | `Endpoint`, `AdmitInput`, `ReplayInput`, `CustodyInput`, `Entry`, `Usage`, `PeerRelayOutbox` service, `layerSql`                                                                                                                                                                                                          |
| `PeerRelayOutboxLimits`  | `Values`, `defaults`, `PeerRelayOutboxLimits` service, `make`, `layer`, `layerDefaults`                                                                                                                                                                                                                                   |
| `PeerRelayReceiptLimits` | `Values`, `defaults`, `PeerRelayReceiptLimits` service, `make`, `layer`, `layerDefaults`                                                                                                                                                                                                                                  |
| `PeerSession`            | `SelectedDocument`, `PeerSession`, `SupervisedPeerSession`, `SyncEnvelope`, `maximumSyncEnvelopeBytes`, `makeTestClient`, `makeSupervised`, `make`, `makeLive`                                                                                                                                                            |
| `PeerSync`               | `Session`, `Outbound`, `Reply`, `Generated`, `Received`, `RelayReceipt`, `PeerSync` service, `layer`, `layerRelay`                                                                                                                                                                                                        |
| `PeerSyncEnvelope`       | Direct and relay envelope schemas, validation, encoding, canonical outer digest, and declared size bounds                                                                                                                                                                                                                 |
| `ProjectionStore`        | `ProjectionStore` service, `BindingServices`, `layer`                                                                                                                                                                                                                                                                     |
| `QueryExecutor`          | `QueryExecutor` service, `QueryHandlers`, `layer`                                                                                                                                                                                                                                                                         |
| `Recovery`               | `RawRecoveryExport`, `Recovery` service, `make`, `layer`                                                                                                                                                                                                                                                                  |
| `ReplicaBootstrap`       | `State`, `ReplicaBootstrap` service, `make`, `layer`                                                                                                                                                                                                                                                                      |
| `ReplicaGate`            | `Permit`, `ReplicaGate` service, `layer`                                                                                                                                                                                                                                                                                  |
| `ReplicaWorkflow`        | `OperationId` reexported from `Compaction`, `CompactReplica`, `RewriteDocumentHistory`, `Execution`, `DocumentExecution`, `CompactionWorkflow`, `HistoryRewriteWorkflow`, `layerRegistration`, `layerRuntime`, `layerHistoryRewriteRegistration`, `layerHistoryRewriteRuntime`                                            |
| `SqlProjection`          | `Migration`, `SqlProjection`, `BindingService`, `make`, `Any`                                                                                                                                                                                                                                                             |
| `SqlReplica`             | `layerFromServices`, `layer`, `layerRelay`, `layerWithBindings`                                                                                                                                                                                                                                                           |

Public SQL service methods:

| Service                  | Methods and effects                                                                                                                                                                                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BackupStore`            | `export(ExportOptions)` returns a byte Stream. `restore(RestoreOptions)` consumes its source environment                                                                                                                                                            |
| `CommandExecutor`        | `create`, `mutate`, `delete`, `lookupCreate`, `lookupMutation`, `lookupDelete`                                                                                                                                                                                      |
| `CommitPublisher`        | `publishPending`, `invalidate(keys)`, scoped `subscribe`                                                                                                                                                                                                            |
| `Compaction`             | `prepare(document, documentId)`, `publish(checkpoint)`, `compact(document, documentId)`, `prune(documentId)`, `pruneCommandReceipts`, `rewriteHistory(document, documentId, operationId)`                                                                           |
| `DocumentStore`          | `create`, `load`, `stage`, `tombstone`, `persist`; callers own the native document returned by `load`                                                                                                                                                               |
| `PeerRelayClientRuntime` | Stable admission, exact endpoint replay, custody retirement, receipt pruning signal, health, and fatal failure observation                                                                                                                                          |
| `PeerRelayOutbox`        | `admit`, `dueForEndpoint`, `maximumPendingHorizon`, `markCustody`, `pruneExpired`, `usage`, `validateReplicaIncarnation`                                                                                                                                            |
| `PeerSync`               | `open(peerId)`, `reset(session)`, `generate(document, documentId, session, peer)`, `receive(document, documentId, session, input)`, `enqueue(session, reply)`, `pending(session)`, `markSent(session, sendSequence, messageHash)`, `invalidateDocument(documentId)` |
| `ProjectionStore`        | `clear`, `replace(binding, snapshot, destinationTable)`, `replaceDocument(document, snapshot, commitSequence)`                                                                                                                                                      |
| `QueryExecutor`          | `execute(query, payload)` and reactive `reactive(query, payload)`                                                                                                                                                                                                   |
| `Recovery`               | `recover(document, documentId)`, `recoverWithPermit(document, documentId, permit)`, `exportRaw(documentId)`                                                                                                                                                         |
| `ReplicaGate`            | `current`, scoped `shared`, bounded scoped `admit`, exclusive `claim(use)`, `refresh`, `validate(expectedPermit)`                                                                                                                                                   |
| `CompactionWorkflow`     | `execute(operationId)`, `poll(execution)`, `interrupt(execution)`, `resume(execution)`                                                                                                                                                                              |
| `HistoryRewriteWorkflow` | `execute(documentId, operationId)`, `poll(execution)`, `interrupt(execution)`, `resume(execution)`. Handles are `DocumentExecution`, which adds `documentId` to `Execution`                                                                                         |

`DocumentEntity` is the durable Cluster protocol beneath `Replica`. All four procedures fail with `ReplicaError`, are
persisted, and run with the configured SQL transaction annotation. `Create`, `Mutate`, and `Delete` are client
uninterruptible. `ApplySync` is uninterruptible.

| Procedure   | Payload additions                                                                                              | Success                                                                                                  |
| ----------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `Create`    | Common command fields plus schema coded JSON bytes in `payload`                                                | Schema coded JSON bytes                                                                                  |
| `Mutate`    | Common command fields plus `mutationTag` and schema coded JSON bytes in `payload`                              | Schema coded JSON bytes                                                                                  |
| `Delete`    | Common command fields only                                                                                     | Schema coded JSON bytes                                                                                  |
| `ApplySync` | `replicaIncarnation`, peer and connection epochs, receive sequence, document type, message hash, message bytes | `{ reply, heads, acceptedHeads, commitSequence, observedByPeer, durableConfirmation: false, duplicate }` |

The common command fields are `replicaIncarnation`, `writerGeneration`, `commandId`, `documentType`, and
`requestHash`. Command persistence keys include the replica incarnation, command ID, and request hash. Sync
persistence keys include the incarnation, peer, connection epoch, receive sequence, and message hash.

SQL composition contracts:

| Module                               | Required services or ownership                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SqlReplica`                         | `layerWithBindings` is the normal constructor. The application supplies platform `SqlClient`, `Crypto`, `ReplicaLimits`, generated mutation and query handler Layers, and all declared `SqlProjection` bindings. `layer` accepts bindings directly. `layerFromServices` is advanced assembly                                                                                                                                                                                               |
| `ReplicaBootstrap` and `ReplicaGate` | Bootstrap runs migrations and establishes replica identity, incarnation, definition hash, and writer generation. Gate requires `ReplicaLimits` and provides shared operation permits and exclusive restore fencing. `shared` and `claim` always wait. `admit` also waits, except that while an exclusive holder such as restore holds or is waiting for the gate it sheds acquirers past `maxQueuedPermits` with `QuotaExceeded`, so a long restore cannot accumulate an unbounded backlog |
| `DocumentStore`                      | Requires `Crypto`, `SqlClient`, and `ReplicaGate`. Callers of loaded native Automerge documents own `free`                                                                                                                                                                                                                                                                                                                                                                                 |
| `ProjectionStore`                    | Requires generated binding services and `SqlClient`. It replaces and rebuilds deterministic derived tables                                                                                                                                                                                                                                                                                                                                                                                 |
| `CommandExecutor`                    | Requires stores, gate, SQL, Crypto, and every mutation handler. Canonical state, projections, receipt, and commit outbox are one transaction                                                                                                                                                                                                                                                                                                                                               |
| `QueryExecutor`                      | Requires gate, projection readiness, SQL, and every query handler. Declared query errors remain typed                                                                                                                                                                                                                                                                                                                                                                                      |
| `CommitPublisher`                    | Requires `Reactivity` and SQL. `publishPending` durably retries until publication into a bounded sliding stream. Scoped subscribers receive best effort refresh notifications and must recover from later sequence gaps by rereading canonical state                                                                                                                                                                                                                                       |
| `DurableRuntime`                     | Builds Cluster and Workflow over the same SQL storage. `layerWith` accepts additional Workflow registrations                                                                                                                                                                                                                                                                                                                                                                               |
| `EntityReplica`                      | Adapts durable Cluster commands, stores, queries, backup, status, and commit publication to core `Replica`                                                                                                                                                                                                                                                                                                                                                                                 |
| `BackupStore`                        | Streams and restores definition bound archives. Export is a snapshot. Restore validates envelope, checksum, bounds, definition, foreign keys, recovery, and projections before installation                                                                                                                                                                                                                                                                                                |
| `Compaction` and `Recovery`          | Compaction publishes only verified checkpoints and prunes changes only with retained safety evidence. It also reclaims command receipts from superseded incarnations, which no current permit can read. Recovery validates checkpoints, changes, heads, and tombstones. `Compaction.rewriteHistory` is the one destructive member. It is reached through `ReplicaWorkflow.HistoryRewriteWorkflow`, which is the only composition that owns the required permit for it                      |
| `ReplicaWorkflow`                    | Registers and executes the scoped compaction and history rewrite Workflows. Workflow durability does not make peer or backup transport durable                                                                                                                                                                                                                                                                                                                                             |

`PeerSession.PeerSession` exposes `peerId`, `connectionEpoch`, `markDirty`, `flush`, `observedByPeer`, and
`durableConfirmation`. `SupervisedPeerSession` adds `awaitDisconnect`. `make` owns transport and receive lifetime.
`makeSupervised` exposes terminal failure without a per session commit subscription. `makeLive` subscribes to
`CommitPublisher` before initial synchronization, routes relevant commit events to `markDirty`, and owns the live
flush loop. The neutral import is `@lucas-barake/effect-local-sql/PeerSession`; the browser path is a compatibility
reexport.

`SyncEnvelope` carries an optional `lineage`. It is optional so an envelope from a peer built before lineage still
decodes, and an absent key becomes the genesis lineage at the single point the envelope enters the session. The
outbound value comes from the queued outbox row rather than from the document, so a rewrite between generation and
send never relabels an already generated message. A session that refuses one document for a lineage change records
that document and keeps serving every other selected document. The record lives for one session, so a reconnect
reevaluates.

`PeerSync.PeerSync` is the lower level durable protocol service. Its methods are `open`, `reset`, `generate`,
`receive`, `enqueue`, `pending`, `markSent`, and `invalidateDocument`. `generate` takes an explicit already
normalized `{ lineageAware }` for the connected peer, so every caller decides that fact rather than defaulting to
it. `invalidateDocument` drops the in memory Automerge sync state every live session holds for one document, which
is what a history rewrite needs because nothing else evicts a state after the change graph is replaced.
`Session` binds peer, connection epoch, and replica incarnation.
`Outbound`, `Reply`, `Generated`, and `Received` expose durable protocol evidence. Most applications must not call this
service directly because `PeerSession` owns sequencing, whole document allowlists, lifecycle, and transport cleanup.

### `@lucas-barake/effect-local-browser`

| Namespace        | Public API                                                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `BrowserReplica` | `layer`, `layerWith`, `layerWithReactivity`, `layerWithReactivityOptions`                                                       |
| `BrowserSqlite`  | `DatabasePort` service, `layer`, `layerMessagePort`                                                                             |
| `PeerSession`    | Compatibility reexport of the SQL package session API                                                                           |
| `Presence`       | `Entry`, `Presence`, `make`                                                                                                     |
| `ReplicaAtom`    | `layerReactivity`, `documentFamily`, `queryFamily`, `mutation`, `status`                                                        |
| `ReplicaClient`  | `ReplicaClient` service, `TimeoutOptions`, `defaultSessionTimeout`, `defaultOperationTimeout`, `fromRpcClient`, `layer`         |
| `ReplicaOwner`   | `layerHandlers`, `layer`, `layerWorker`                                                                                         |
| `ReplicaRpc`     | `protocolVersion`, `SessionHandshake`, `MessagePortSchema`, `Invalidation`, `InvalidationMessage`, `ReplicaQueryError`, `group` |
| `SessionManager` | `leaseDurationMillis`, `RestoreLease`, `SessionManager` service, `layer`                                                        |

`ReplicaRpc.group` is the page to owner protocol. Every request carries `sessionId`. Except where shown, failures are
`ReplicaError` and success is `void`.

| Procedure               | Additional request fields                                      | Success and error differences                                                                                                             |
| ----------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `OpenSession`           | optional `protocolVersion`, `definitionHash`                   | `{ leaseMillis, protocolVersion, definitionHash, ownerEpoch, maxChunkBytes, maxRestoreCoalesceMillis, maxRestoreErrorBytes }`             |
| `RenewSession`          | None                                                           | `{ leaseMillis }`                                                                                                                         |
| `CloseSession`          | None                                                           | `void`                                                                                                                                    |
| `Create`                | `document`, `commandId`, JSON `value`                          | `CommandOutcome<DocumentId, never>`                                                                                                       |
| `Get`                   | `document`, `documentId`                                       | JSON `Snapshot`                                                                                                                           |
| `Mutate`                | `mutation`, `commandId`, `documentId`, JSON `payload`          | `CommandOutcome<Json, Json>`                                                                                                              |
| `Delete`                | `document`, `commandId`, `documentId`                          | `CommandOutcome<Json, Json>`                                                                                                              |
| `Query`                 | `query`, JSON `payload`                                        | JSON. Error is `ReplicaQueryError \| ReplicaError`                                                                                        |
| `LookupMutation`        | `mutation`, `commandId`                                        | `CommandOutcome<Json, Json>`                                                                                                              |
| `LookupCreate`          | `document`, `commandId`                                        | `CommandOutcome<DocumentId, never>`                                                                                                       |
| `LookupDelete`          | `document`, `commandId`                                        | `CommandOutcome<Json, Json>`                                                                                                              |
| `Flush`                 | None                                                           | `void`                                                                                                                                    |
| `Invalidations`         | `ownerEpoch`                                                   | Stream of `InvalidationsReady`, `Invalidation`, or `FullRefreshRequired`                                                                  |
| `Status`                | None                                                           | Stream of `ReplicaStatus`                                                                                                                 |
| `ExportBackup`          | `maxBytes`                                                     | Stream of transferable byte chunks                                                                                                        |
| `RestoreBackup`         | None                                                           | Legacy protocol rejection. Always fails with `ProtocolMismatch` after session ownership validation                                        |
| `BeginRestoreBackupV4`  | `mode`, `maxBytes`, `expectedDefinitionHash`, `installationId` | `{ nonce, port }`. The page sends Start, then the owner drives Pull, chunk, terminal readiness, acknowledgement, and release confirmation |
| `FinishRestoreBackupV4` | `nonce`                                                        | Pending authoritative restore result. The page accepts its Exit before acknowledging `TerminalReady`                                      |
| `ExportDocument`        | `document`, `documentId`                                       | `{ documentName, schemaVersion, value: Json }`                                                                                            |
| `ImportDocument`        | `document`, `commandId`, exported document value               | `CommandOutcome<DocumentId, never>`                                                                                                       |

`Invalidation` is either `{ _tag: "Invalidation", ownerEpoch, sequence, keys }` or
`{ _tag: "FullRefreshRequired", ownerEpoch, keys }`. `InvalidationMessage` also admits
`{ _tag: "InvalidationsReady", ownerEpoch, watermark, refreshGeneration }`. `ReplicaQueryError` carries JSON in
`error`.

Browser composition contracts:

| Module           | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BrowserSqlite`  | `DatabasePort` is the transferred dedicated worker capability. `layerMessagePort` installs a concrete port. Neither API opens a second database connection                                                                                                                                                                                                                                                                                                                                                     |
| `ReplicaOwner`   | `layerHandlers` serves the internal `ReplicaRpc.group`. `layer` composes ownership services. `layerWorker` hosts them in the Effect Worker environment                                                                                                                                                                                                                                                                                                                                                         |
| `SessionManager` | `open`, `renew`, `close`, `contains`, `activeCount`, `run`, and `stream` enforce client ownership, the exported 60 second lease, per session in flight limits, stream limits, and global queued RPC bounds. The exported restore limit fields, `acquireRestore`, `activeRestoreCount`, and `activeRestoreCountForSession` expose atomic restore admission and diagnostics                                                                                                                                      |
| `ReplicaClient`  | Extends the core `Replica` service with `ownerEpoch` and invalidations. `fromRpcClient` owns session open, renewal, close, transient reconnect, command ambiguity recovery, and invalidation resubscription. `layer` builds the internal generated client. Session lifecycle RPCs default to 10 second timeouts. Unary operations default to 30 seconds. Restore uses one end to end deadline across transport and session replacement. `Invalidations`, `Status`, and `ExportBackup` streams remain unbounded |
| `BrowserReplica` | `layer` is the standard page service. Every constructor accepts optional `TimeoutOptions`. `layerWith` and `layerWithReactivityOptions` also accept Worker options. Reactivity variants provide invalidation bridging                                                                                                                                                                                                                                                                                          |
| `ReplicaAtom`    | `documentFamily`, `queryFamily`, `mutation`, and `status` build reactive views over an Atom runtime. `layerReactivity` connects commit invalidations. Atom state remains rebuildable                                                                                                                                                                                                                                                                                                                           |
| `Presence`       | `make` validates a positive finite TTL and returns `receive`, scoped `publish`, `remove`, and `values`. Concurrent writes for one peer resolve by arrival sequence, and a write older than the newest settled call for that peer is dropped without an error, including after the winner was removed or expired. Ordering covers arrival at this process, not transport reordering. Expiry is applied lazily on `values`. Presence is ephemeral and never authorization state                                  |

The internal browser `ReplicaRpc` protocol and the external peer `PeerRpc` protocol are different contracts. The
first connects a page to its SharedWorker owned local replica. The second connects independent canonical replicas.
Do not expose the browser owner protocol as an internet peer API.

### `@lucas-barake/effect-local-test`

| Namespace        | Public API                                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FaultInjection` | `Packet`, `Decision`, `FaultInjection` service, `layer`, `none`, `layerSequence`                                                                                                      |
| `TestPeer`       | Tagged errors `InvalidOptions`, `InvalidFault`, `QueueFull`, `ConnectionClosed`, plus `TestPeerError`, `Options`, `Connection`, `TestPeer` service, `make`, `layer`, `transportLayer` |
| `TestReplica`    | `defaultLimits`, `layerWithLimits`, `layer`, `layerWithSyncAndLimits`, `layerWithSync`                                                                                                |

`TestReplica.layer` and `layerWithLimits` run the full Cluster and Workflow production path against in memory Node
SQLite. `layerWithSync` and `layerWithSyncAndLimits` expose the lower level sync graph. All constructors
still require the domain's generated mutation and query handler services and install the supplied SQL projection
bindings.

`FaultInjection.Decision` controls drop, finite copy count, finite nonnegative delay, and pairwise reorder. A
`layerSequence` repeats its final decision for later packets. `TestPeer` validates queue capacity, maximum copies, and
maximum delay. Its methods connect peers, partition, heal, and flush traffic. `transportLayer` adapts one peer to core
`PeerTransport` with the same acknowledged delivery shape used by production transports.

`TestReplica.defaultLimits` is the only exported `ReplicaLimits` preset. It is intended for tests, not production capacity
planning:

| Category                 | Exact defaults                                                                                                                                                                            |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backup and encoding      | `maxBackupBytes = 16 MiB`, `maxChunkBytes = 64 KiB`, `maxArchiveRecords = 10_000`, `maxJsonDepth = 64`                                                                                    |
| One sync message         | `maxSyncMessageBytes = 1 MiB`, `maxPeerSendMillis = 10_000`, `maxSyncChangesPerMessage = 1_000`, `maxSyncDependencyEdgesPerMessage = 10_000`, `maxSyncOperationsPerMessage = 100_000`     |
| Pending bytes            | Per document `16 MiB`, per peer `32 MiB`, per replica `64 MiB`, maximum age `60_000 ms`                                                                                                   |
| Pending changes          | Per document `10_000`, per peer `20_000`, per replica `50_000`                                                                                                                            |
| Pending dependency edges | Per document `100_000`, per peer `200_000`, per replica `500_000`                                                                                                                         |
| Browser owner RPC        | `maxSessions = 32`, `maxStreamsPerSession = 32`, `maxInFlightPerSession = 128`, `maxQueuedRpc = 1_024`                                                                                    |
| Replica gate admission   | `maxQueuedPermits = 1_024`                                                                                                                                                                |
| Browser restore          | `maxActiveRestores = 1_024`, `maxRestoresPerSession = 128`, `maxRestoreMillis = 30_000`, `maxRestorePullMillis = 10_000`, `maxRestoreCoalesceMillis = 25`, `maxRestoreErrorBytes = 4_096` |

### `@lucas-barake/effect-local-rpc`

| Module                   | Public contract                                                                                            |
| ------------------------ | ---------------------------------------------------------------------------------------------------------- |
| `PeerAuthentication`     | Required client and server middleware for every relay request                                              |
| `PeerAuthenticator`      | Application credential verification service                                                                |
| `PeerCredentials`        | Client credential provider                                                                                 |
| `PeerRelayAuthorization` | Exact send and receive authorization plus the explicit Automerge resource trust grant                      |
| `PeerRelayIngress`       | Bounded server and client socket protocol                                                                  |
| `PeerRelayLimits`        | Authentication, ingress, custody, session, worker, quota, retry, and maintenance limits                    |
| `PeerRpc`                | Durable protocol version `1`, `Open`, `Push`, `Acknowledge`, `Reject`, events, group, and generated client |
| `PeerRelayStore`         | Injectable semantic custody contract                                                                       |
| `SqlPeerRelayStore`      | Generic `SqlClient` implementation with `make` and `layer` for SQLite, PostgreSQL, and MySQL               |
| `PeerRpcServer`          | `layerHandlers`, `layerServer`, and `PeerRpcServerRuntime`                                                 |
| `RpcPeerTransport`       | Durable `Options`, `layer`, `makeSession`, and `isRetryable`                                               |

The `Open` stream starts with `Opened` and then emits `StoredMessage` values. `Push` succeeds only after durable
custody. `Acknowledge` and `Reject` use the exact claim token and message hash. Every procedure can fail with a
fieldless `PeerRpcError`.

`PeerAuthentication.layerClient` requires `PeerCredentials`.
`PeerAuthentication.layerServer` requires `PeerAuthenticator` and `PeerRelayLimits`.
`PeerRpcServer.layerHandlers` requires `Crypto`, `PeerRelayAuthorization`, `PeerRelayIngress`,
`PeerRelayLimits`, and `PeerRelayStore`. `SqlPeerRelayStore.layer` requires `SqlClient`, `Crypto`, and
`PeerRelayLimits`. `RpcPeerTransport.layer` additionally uses `PeerRelayClientRuntime`.

### Research basis

These works motivate the constraints. They do not expand the implementation guarantees beyond the contracts above.

| Principle               | Primary source                                                                                                                                                                                                                                                                            | Applied decision                                                                                                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local replica primacy   | Kleppmann, Wiggins, van Hardenberg, and McGranaghan, [Local First Software](https://doi.org/10.1145/3359591.3359737), Onward! 2019                                                                                                                                                        | Interactive work commits locally. A hosted process is another assisting canonical replica, not a mandatory transaction authority                                                             |
| CRDT convergence        | Shapiro, Preguica, Baquero, and Zawirski, [A Comprehensive Study of Convergent and Commutative Replicated Data Types](https://inria.hal.science/inria-00555588), 2011                                                                                                                     | Replicas converge by accepting the same valid Automerge changes rather than by backend serialization across replicas                                                                         |
| Optimistic replication  | Saito and Shapiro, [Optimistic Replication](https://doi.org/10.1145/1057977.1057980), ACM Computing Surveys 2005                                                                                                                                                                          | Temporary divergence and background propagation are normal. Recovery and delivery assumptions stay explicit                                                                                  |
| End to end delivery     | Saltzer, Reed, and Clark, [End to End Arguments in System Design](https://doi.org/10.1145/357401.357402), 1984, and Lampson, [Reliable Messages and Connection Establishment](https://www.microsoft.com/en-us/research/publication/reliable-messages-and-connection-establishment/), 1993 | Endpoint command receipts, persistent message identity, and convergence matter. Transport success is never called exactly once delivery                                                      |
| Fail safe authorization | Saltzer and Schroeder, [The Protection of Information in Computer Systems](https://doi.org/10.1109/PROC.1975.9939), 1975                                                                                                                                                                  | Complete mediation on every RPC, exact grants, least privilege, redacted failures, and structural tenant isolation                                                                           |
| Connection semantics    | Fette and Melnikov, [The WebSocket Protocol](https://www.rfc-editor.org/rfc/rfc6455.html), RFC 6455                                                                                                                                                                                       | Ordered frames exist only within one connection. Replay, authorization, application acknowledgement, and durability remain separate                                                          |
| Backpressure            | Reactive Streams, [Specification 1.0.4](https://github.com/reactive-streams/reactive-streams-jvm)                                                                                                                                                                                         | Finite demand, item capacity, byte capacity, admission concurrency, and explicit overload replace unbounded buffering                                                                        |
| Leases and fencing      | Gray and Cheriton, [Leases](https://doi.org/10.1145/74850.74870), SOSP 1989, and Burrows, [The Chubby Lock Service](https://www.usenix.org/conference/osdi-06/chubby-lock-service-loosely-coupled-distributed-systems), OSDI 2006                                                         | Time bounded policy reuse is paired with invalidation. Writer generation, replica incarnation, and connection epoch reject stale ownership                                                   |
| Safe tracing            | Sigelman and collaborators, [Dapper](https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/), 2010, and the [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)                                   | Dapper motivates common low overhead instrumentation and sampling. OWASP motivates excluding secrets and sensitive identifiers. The exact excluded fields are this repository's threat model |

## License

[MIT](LICENSE)
