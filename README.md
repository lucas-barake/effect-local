# Effect Local

Effect Local is a local first mutation log for Effect applications. A client commits mutations optimistically to
local SQLite, works while offline, and reconciles with an authoritative server assigned order when connectivity
returns. Effect Schema defines every domain, durable, and wire contract. Effect services, Layers, scopes, streams,
and Atom own the runtime.

The library targets Effect `4.0.0-beta.103`. It has not published a stable release. Durable and public contracts may
change before v1.

## Architecture

```mermaid
flowchart LR
  UI["Effect Atom"] --> R["Replica service"]
  R --> L["Local SQLite"]
  L --> P["Pending mutations"]
  L --> V["Visible state"]
  P --> F["Finite reconciliation Workflow"]
  F --> W
  W["WebSocket RPC"]
  W --> E["Cluster space entity"]
  E --> S["Server admission"]
  E --> X["Bounded ephemeral state"]
  S --> O["Authoritative total order"]
  O --> W
  W --> L
  L --> C["Scoped canonical state"]
  C --> V
```

One local transaction allocates a stable mutation identity, runs the handler, stores the pending envelope and write
set, and updates visible state. The server authenticates and authorizes the mutation, deduplicates exact retries,
executes the same handler, and either stores a terminal rejection or advances the space's dense mutation sequence.
The submitting client's result remains in its private receipt. Replication sends only the current entities selected
for that client. The client applies those view changes to canonical state and then replays its remaining pending
mutations over that state.

Effect Cluster owns deployment neutral routing and live ownership. Separate entities per space isolate mutation
admission, reads, watches, and bounded ephemera while routing them across runners. The actors do not retain mutation
payloads or replies in Cluster message history. Server SQL stores authoritative entities, bounded mutation history and
receipts, and a materialized replication view per client. Ephemeral roster, event, and state data stays in memory.
Clients retain pending mutations, a dense view cursor, the global mutation watermark, durable retractions, and
resumable scoped bootstrap staging in SQLite. Effect Workflow owns durable client scheduling through finite
reconciliation generations.

Ordinary fields store ordinary values. Applications that need concurrent intent for a specific field can use an
explicit `Field.Semantics` such as a counter or grow only set. Every other model avoids causal metadata.

See [architecture](docs/architecture.md), [durability](docs/durability.md), and [synchronization](docs/sync.md) for the
invariants and failure model.

## Packages

| Package                              | Purpose                                                           |
| ------------------------------------ | ----------------------------------------------------------------- |
| `@lucas-barake/effect-local`         | Models, mutations, queries, field semantics, protocol, and errors |
| `@lucas-barake/effect-local-sql`     | SQLite state, server log, and Workflow reconciliation             |
| `@lucas-barake/effect-local-rpc`     | WebSocket RPC, Cluster space routing, and bounded ephemera        |
| `@lucas-barake/effect-local-browser` | Browser SQLite ports and the joined Effect Atom graph             |
| `@lucas-barake/effect-local-test`    | Production shaped test layers and deterministic network faults    |

All packages are ESM. Public modules are available as subpaths such as
`@lucas-barake/effect-local/Mutation`. Paths under `internal/*` are private.

## Domain definition

```ts
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

export const Task = Model.make("Task", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    title: Schema.NonEmptyString,
    completed: Schema.Boolean
  }),
  indexes: {
    byCompletedTitle: {
      version: 1,
      partition: [{
        name: "completed",
        affinity: "integer",
        schema: Schema.Boolean,
        extract: (task) => task.completed
      }],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.NonEmptyString,
        extract: (task) => task.title
      }]
    }
  }
})

export const PutTask = Mutation.make("PutTask", {
  version: 1,
  payload: Task.schema,
  success: Task.schema
})

export const ToggleTask = Mutation.make("ToggleTask", {
  version: 1,
  payload: { id: Schema.String }
})

export const ListTasks = Query.make("ListTasks", {
  payload: { completed: Schema.Boolean, titleFrom: Schema.optional(Schema.NonEmptyString) },
  success: Schema.Array(Task.schema)
})

export const definition = Definition.make({
  version: 1,
  models: [Task],
  mutations: [PutTask, ToggleTask],
  queries: [ListTasks]
})

export const layerDomain = Layer.mergeAll(
  PutTask.toLayer(({ payload, transaction }) => transaction.set(Task, payload.id, payload).pipe(Effect.as(payload))),
  ToggleTask.toLayer(({ payload, transaction }) =>
    transaction.get(Task, payload.id).pipe(
      Effect.flatMap(Option.match({
        onNone: () => Effect.void,
        onSome: (task) => transaction.set(Task, task.id, { ...task, completed: !task.completed })
      }))
    )
  ),
  ListTasks.toLayer(({ payload, query }) =>
    query.from(Task, "byCompletedTitle")
      .where({
        completed: payload.completed,
        title: payload.titleFrom === undefined ? undefined : { gte: payload.titleFrom }
      })
      .order("asc")
      .limit(50)
      .page()
      .pipe(Effect.map((page) => page.items))
  )
)
```

Handler dependencies are ordinary Effect requirements. `toLayer` captures them once when the Layer is built. Handler
failures declared by the mutation or query Schema stay in the typed error channel. Storage, protocol, capacity,
authorization, and identity failures use the tagged classes in `ReplicaError`.

Handlers must be deterministic. Put timestamps, random values, generated identifiers, and other nondeterministic
inputs in the mutation payload before committing it.

### Indexed queries

Every multi row query names an index declared on its model. Partition components require exact values. The first sort
component accepts `gt`, `gte`, `lt`, and `lte` bounds. Ordering applies to the complete sort tuple, and the encoded
entity key is the stable final tie breaker. Unknown indexes, missing partition fields, extra filter fields, and cursors
from another model or index are type errors.

`page()` fetches at most the configured limit and returns `{ items, next }`. Pass `next` to `after()` for the following
keyset page:

```ts
const tasks = query.from(Task, "byCompletedTitle")
  .where({ completed: false, title: { gte: "A", lt: "N" } })
  .order("asc")
  .limit(25)

const first = yield * tasks.page()
const second = first.next === undefined ? undefined : yield * tasks.after(first.next).page()
```

`after()` also accepts the bare `next.token` string. A paginating query can take a `Schema.NullOr(Schema.String)`
cursor in its payload and return `page.next?.token ?? null` in its success value without remodeling the cursor object.
The token is validated against the exact query shape, so a cursor minted by a different query, direction, or limit
fails with `StorageCorrupt`.

Use `stream()` when a handler must consume every matching row without materializing the result set first. It advances
through the same bounded keyset pages and decodes only selected entities. The named query still returns a value
accepted by its success Schema, so a live Stream cannot escape through `Replica.query`.

SQLite stores each declared index in an owner qualified shadow table. The migration catalog checks its DDL checksum
and resumes bounded backfills. Local mutations, accepted sync changes, projection replay, and snapshot installation
maintain the active index generation. Index declarations are client local derived storage. Changing one rebuilds the
local layout without changing the wire definition or model schema identity.

Reactive query atoms retain the exact entity and index ranges read by the handler. A write publishes old and new index
points after commit, grouped by index partition, so even a very large batch invalidates only queries over the touched
partitions. Only mounted queries whose recorded ranges can change rerun. Reads through `query.get` use exact
space, model, and decoded key tokens. `Query.make` has no static dependency list because runtime reads are the source
of truth.

## SQLite replica

`SqlReplica.layer` assembles one public `Replica` that owns one SQLite database, one synchronization transport, and
any number of joined spaces. Supply the domain handlers, a `SqlClient`, `Crypto`, and a `SyncEngine`:

```ts
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { definition, layerDomain, ListTasks, PutTask, Task } from "./domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scope = Protocol.ReplicationScope.make({ models: [Task.name] })

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: "tasks.sqlite" }),
  NodeCrypto.layer
)

const history = {
  retainedReceipts: 256,
  maximumReceipts: 1_024,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 100_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "25 millis", maximumAttempts: 8 }
} as const

export const layerReplica = SqlReplica.layer({
  definition,
  clientId,
  defaultScope: scope,
  initialSpaces: [spaceId],
  maximumActiveSpaces: 8,
  foregroundActiveSpaces: 4,
  reconciliationConcurrency: 8,
  foregroundReconciliationConcurrency: 2,
  ...history
}).pipe(
  Layer.provide(layerDomain),
  Layer.provide(layerDatabase),
  Layer.provide(layerSync)
)

const program = Replica.Replica.use((replica) =>
  Effect.gen(function*() {
    const space = yield* replica.space(spaceId)
    const pending = yield* space.mutate(PutTask, {
      id: "task-1",
      title: "Ship the mutation log",
      completed: false
    })
    const inFlight = yield* space.pendingFor(PutTask)
    const task = yield* space.get(Task, "task-1")
    const tasks = yield* space.query(ListTasks, { completed: false })
    return { pending, inFlight, task, tasks }
  })
).pipe(Effect.provide(layerReplica), Effect.scoped)
```

Call `replica.join(spaceId)` to remember membership, `replica.leave(spaceId)` to evict that space, and `replica.spaces`
to list remembered handles. A remembered space starts inactive. `space.activate` opens its local runtime and watch,
while `space.deactivate` releases them without deleting local data. Entity, query, mutation, pending, receipt, and
settlement operations activate the addressed space as foreground work. Each handle also exposes reactive `scope`,
`setScope`, `activation`, and `status` operations.
Leaving cascades through every client table without changing the durable client identity or any other space. A stale
handle returns `SpaceUnavailable`, including after the same space is joined again with a fresh membership incarnation.

`layerSync` can be the RPC client Layer from the next section or any implementation of `SyncEngine`. Local commits do
not wait for it. `maximumActiveSpaces` bounds all live per space runtimes. `foregroundActiveSpaces` reserves an LRU
resident subset for addressed work. Reconciliation reserves `foregroundReconciliationConcurrency` from the total
`reconciliationConcurrency`, so an open chat progresses while background spaces are saturated. The remaining slots
drain pending mutations with short lived background runtimes, including for otherwise inactive spaces. Active watches
remain separate logical streams over the shared `SyncEngine` and RPC WebSocket, but their count is bounded by the
active space budget. `replica.status` is a constant size summary with a space count, total pending count, and counts by
state. Read individual `space.status` values only for rows the UI displays.
The explicit Workflow composition persists only reconciliation execution control. Application data stays in the same
SQLite tables used by the in memory composition.

`space.mutate` still completes at the local optimistic commit. It never waits for the server and its error channel
contains only failures from that local run. Use `space.pending` to inspect every in flight mutation, including its
decoded payload, submission state, and attempt count. Use `space.pendingFor(PutTask)` when the mutation specific type
matters. `space.settlements()` is a durable Stream of `{ sequence, settlement }` values, where each settlement holds
the terminal `{ pending, receipt }` pair, delivered only after rollback and pending replay have completed. The default
`from: "live"` starts at the current tail; `from: "acknowledged"` resumes from the durable acknowledgement floor, and
`from: n` replays everything after sequence `n`, so a settlement recorded while no subscriber was attached, or before
an app restart, is still observed. `space.settlementsFor(PutTask)` filters by mutation in the durable read and includes
legacy receipts. `space.acknowledgeSettlements(sequence)` advances the retention floor: pruning prefers acknowledged
settlements, but the retained receipt budget is always enforced, so an app that never subscribes or never acknowledges
keeps syncing. A replay that falls behind the prune horizon fails with `SettlementReplayTruncated` carrying the oldest
available sequence instead of silently skipping. Consumers read at their own pace from SQLite and can never
backpressure reconciliation or the local `mutate` commit. Mutation rejections from either surface are decoded through
`PutTask.rejectionSchema`; authorization, capacity, legacy, and quarantine rejections remain distinct origin tagged
JSON branches.

Use `SqlReplica.layerWorkflow` when reconciliation must recover through Effect Workflow. Supply
`ClusterWorkflowEngine.layer` and the official runner Layer separately. For one SQLite owner this is normally
`SingleRunner.layer({ runnerStorage: "sql" })`. Configure `retryDelay`, `maximumRetryDelay`, and `maximumAttempts` on
`layerWorkflow` to bound one execution's exponential retry history. A later local mutation or server wake creates a
new generation after a terminal failure. `SqlReplica.layer` remains the explicit lightweight in memory choice.

The production composition benchmark compares eager runtimes at `e999e1c` with inactive remembered spaces. It seeds
the same durable memberships, measures the median of three cold Replica restores, and runs full V8 garbage collection
before each retained heap sample:

| Spaces | Eager fibers | Lazy fibers | Eager watches | Lazy watches | Eager heap MiB | Lazy heap MiB | Eager restore ms | Lazy restore ms |
| -----: | -----------: | ----------: | ------------: | -----------: | -------------: | ------------: | ---------------: | --------------: |
|      1 |           11 |           6 |             1 |            0 |           0.28 |          0.13 |            24.17 |           14.59 |
|     64 |          144 |           6 |            64 |            0 |           3.04 |          0.49 |           371.21 |           27.90 |
|    256 |          528 |           6 |           256 |            0 |           9.63 |          0.49 |         1,031.43 |           32.45 |
|  1,000 |        2,016 |           6 |         1,000 |            0 |          34.73 |          0.17 |         4,090.48 |           98.84 |

```sh
for spaces in 1 64 256 1000; do
  EFFECT_LOCAL_BENCH_SPACES=$spaces pnpm bench packages/local-sql/bench/ReplicaScale.bench.ts
done
```

## Replication scope and read authorization

A replication scope belongs to one remembered space. `defaultScope` initializes only a newly joined membership.
`space.scope` reads the durable value and `space.setScope(next)` changes only that space. An empty scope is valid.
Changing scope advances its durable generation, restarts its active watch, and schedules foreground reconciliation.
Widening backfills newly selected entities through ordinary pull pages. Narrowing sends `Retract` changes so excluded
entities disappear locally without replacing the database or doing a full bootstrap.

A window bounds a model to the newest `count` entities per partition of one of its secondary indexes, ordered by the
index sort descending. Per-partition overrides raise the count or add a leading-sort-component range, which is how a
client pages older history in and evicts it again. Scroll-back and eviction are plain scope changes, so they reuse the
same generation fencing, pages, and retractions as any other scope transition. The client needs no window logic: slid
or evicted entities arrive as retractions. A windowed scope requires the caller to be on the current server schema;
clients mid rolling upgrade keep full model scopes until they upgrade.
One scope can contain at most 1,000 windows, 1,000 partition overrides in total, and 4 MiB of encoded data.

```ts
const scope = Protocol.ReplicationScope.make({
  models: [],
  windows: [Protocol.ReplicationWindow.make({ model: "Message", index: "byChat", count: 50 })]
})

const scrolledBack = Protocol.ReplicationScope.make({
  models: [],
  windows: [Protocol.ReplicationWindow.make({
    model: "Message",
    index: "byChat",
    count: 50,
    partitions: [Protocol.ReplicationWindowPartition.make({ key: ["chat-42"], count: 200 })]
  })]
})
```

Steady pulls are derived from the authoritative log suffix since a per-view delivered watermark: the server touches
only the entities that changed, the partitions those changes affected, and the client's acknowledged view, instead of
rescanning the space. The full re-derive remains the fallback for scope changes, schema changes, pruned history, and
read authorization invalidation.

The server computes effective visibility as the intersection of the requested scope and `authorizeRead`. It first
calls the policy with `_tag: "Scope"`, before disclosing space or schema state. It then calls the policy with
`_tag: "Entity"` for every candidate entity. The entity key and value are Schema encoded JSON. A candidate that fails
either check never enters a pull or bootstrap page. Wakes contain no entity payload.

The option callback and a Context service compose directly. The Effect returned by `authorizeRead` may require
services. Those requirements propagate to `ServerStore.layer`, where normal Layer composition supplies them:

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
  authorizeAccess: ({ clientId, principal, spaceId }) => authorizeClient({ clientId, principal, spaceId }),
  authorizeMutation: ({ mutation, principal }) => authorizeMutation({ mutation, principal }),
  authorizeRead: (input) => ReadPolicy.use((policy) => policy.authorize(input))
}).pipe(Layer.provide(layerReadPolicy))
```

`Delete` means the authoritative entity no longer exists. `Retract` means it still exists but no longer belongs to the
client's scope or visibility. A retraction removes canonical and visible state and leaves a durable fence so an old
optimistic mutation cannot resurrect revoked data. Later authorization can send an `Upsert` and clear the fence.
Watches carry only wake hints. Pull rechecks the policy. `readAuthorizationRefreshInterval` supplies periodic hints so
a policy-only revocation is eventually retracted even when no mutation changes that entity.

Every pull re-evaluates `authorizeRead` for each entity the client currently holds, so a revocation retracts on the
next pull even when the entity itself never changed. The reverse direction is not free under incremental pulls: a
policy flip that newly grants an entity untouched since the client's watermark stays invisible until something changes
it. When authorization depends on external state, call `ServerStore.invalidateReadAuthorization(spaceId)` after that
state changes; the next pull of each affected client re-derives its complete view.

## Server and WebSocket RPC

`ServerStore.layer` persists one dense mutation sequence per space, terminal receipts per client mutation, the
authoritative entity state, and principal bound client views. Its mutation path acquires the space row inside the SQL
transaction, so handler execution, materialization, sequence allocation, and hard capacity checks share one order.
The dense space sequence remains the mutation basis. A separate dense view cursor orders the subset visible to one
client.

Call `ServerStore.maintain` or provide `ServerStore.layerMaintenance` in the server scope. Maintenance prepares the
global recovery snapshot used to expire old mutation and receipt evidence, then reclaims bounded prefixes. Admission
returns `CapacityExceeded` before handler execution when a hard cap is reached, so the maintenance Layer is required
in a long lived deployment. A fresh or invalid client view receives `BootstrapRequired` and installs client, scope,
schema, and principal bound pages into durable staging before one atomic canonical replacement.

`ServerStore.layer` requires `authorizeAccess`, `authorizeMutation`, and `authorizeRead`. Access authorization runs
before retry receipt lookup. Mutation admission rejection consumes the client's local sequence and persists an exact
retry receipt, but does not consume a server sequence. `ServerStore.layerTrusted` is the explicit allow all Layer for
tests and already trusted processes.

`SyncRpc.Rpcs` multiplexes submit, pull, bootstrap, watch, and ephemera on one Effect RPC WebSocket. The server uses
`Authentication.layerServer`. The client uses `Authentication.layerClient` with an application supplied
`CredentialProvider`. Its `acquire` Effect runs for every RPC and returns a redacted bearer credential plus its
nonnegative generation. `awaitChange(rejectedGeneration)` signals when `acquire` can return a different generation.
A rejected credential changes the space to `NeedsAuthentication` and pauses that generation. Publishing a new
generation resumes synchronization through the same replica and WebSocket.

`CredentialRejected` is a credential problem. `AuthenticatorUnavailable` is a verifier outage and remains retryable.
`AuthorizationDenied` means an authenticated principal lacks permission and is terminal. `OperationTimeout` identifies
the bounded session or RPC operation that expired. Session acquisition, unary RPCs, and stream acquisition default to
10 second timeouts and accept `Duration.Input`. Established watch streams may remain idle. Transient reconciliation
failures use capped exponential backoff from `retryDelay`, default 1 second, through `maximumRetryDelay`, default 1
minute. See [synchronization](docs/sync.md) and the
[`effect-local-rpc` guide](packages/local-rpc/README.md) for the provider contract and socket retry policy.

The authenticated server facade sends all operations to the Cluster entity for the requested space. Cluster supplies
the unique live owner and cross runner stream routing. SQL stores the authoritative accepted log and terminal
receipts. The application chooses Effect's runner storage, message storage, runner transport, and deployment Layers.
It also remains responsible for its HTTP server, WebSocket path, TLS, Origin policy, credential verification, and
tenant authorization. Provide `SyncRpc.layerJson` on both sides. It bounds and sanitizes complete JSON frames. A
reverse proxy or lower level WebSocket upgrade handler must enforce the same native ingress payload limit.

The facade uses five space entities. `SpaceAdmissionEntity` serializes Submit and Discard.
`SpaceReadEntity` serves Pull and Bootstrap concurrently. A Layer wide fail fast allowance bounds Bootstrap assertion verification
and preparation. A separate per space allowance bounds immutable page reads. `SpaceWatchEntity` owns long lived sync
watches. `SpaceEphemeralJoinEntity` owns joined streams, while `SpaceEphemeralCommandEntity` owns publish and
heartbeat. The Hub applies its watcher bound after authorization. Separate command and stream lanes keep a paused
Bootstrap page or full join population from blocking mutation admission. Saturated work fails with typed
`CapacityExceeded` resource `bootstrap authorizations`, `bootstrap pages`, or `ephemeral join verifications`.

`ServerStore.maximumWatchersPerSpace` and `EphemeralHub.maximumWatchersPerSpace` independently cap active streams.
The ephemeral channel has bounded sliding history and per-subscriber revision-gap detection. Only a lagging client
resubscribes to a fresh roster and retained-state snapshot. Join establishes a private server capability for publish
and heartbeat, and periodic authorization revocation closes the established stream. Sync authorization successes
are cached by the complete normalized space, client, scope, and principal input. The refresh interval is the fail closed
revocation bound. Executing policy calls, live authorization callers and owner lookups, completed successes, and active
watchers have separate required limits. The same pending allowance also bounds per-wake visibility work. Pending overflow fails with typed
`CapacityExceeded { resource: "read authorizations", limit }`. Accepted mutations publish one shared postcommit wake.
Delivery performs no SQLite transaction or space row write per watcher.

Effect metrics cover admission and rejection classes, history and receipt depth beside their limits, sync and ephemeral
watcher populations, wake fanout duration, durable bootstrap installs, maintenance and prune volume, and client pending
depth. Labels use bounded categories and never include space or principal identifiers. The production composition
benchmark at `packages/local-rpc/bench/Fanout.bench.ts` exercises 64, 256, and 1,024 watchers.

## Effect Atom

```ts
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

export const graph = BrowserReplica.make(Layer.merge(layerReplica, EphemeralClient.layer))

export const taskAtom = graph.entity(spaceId, Task)("task-1")
export const tasksAtom = graph.query(spaceId, ListTasks)({ completed: false })
export const putTaskAtom = graph.mutation(spaceId, PutTask)
export const pendingTasksAtom = graph.pendingFor(spaceId, PutTask)
export const taskSettlementsAtom = graph.settlementsFor(spaceId, PutTask)
export const spaceStatusAtom = graph.status(spaceId)
export const spaceScopeAtom = graph.scope(spaceId)
export const setSpaceScopeAtom = graph.setScope(spaceId)
export const activationAtom = graph.activation(spaceId)
export const activateAtom = graph.activate(spaceId)
export const deactivateAtom = graph.deactivate(spaceId)
export const replicaStatusAtom = graph.aggregateStatus
export const spacesAtom = graph.spaces
export const joinAtom = graph.join
export const leaveAtom = graph.leave

const member = Protocol.EphemeralMember.make({
  clientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"))
const Typing = Ephemeral.make("Typing", {
  kind: "event",
  payload: { conversationId: ConversationId, active: Schema.Boolean }
})
const ReadPosition = Ephemeral.make("ReadPosition", {
  kind: "state",
  key: ConversationId,
  payload: { messageId: Schema.String }
})
const Presence = Ephemeral.member({ status: Schema.String })

export const sessionAtom = graph.ephemeral(Presence, {
  spaceId,
  member,
  value: { status: "online" },
  ttl: "30 seconds"
})
export const typingAtom = graph.ephemeralEvents(sessionAtom, Typing)
export const positionsAtom = graph.ephemeralState(sessionAtom, ReadPosition)
export const rosterAtom = graph.ephemeralMembers(sessionAtom)
export const publishTypingAtom = graph.publishEphemeral(Typing, { spaceId, member })
```

The graph defaults to Effect's shared `Atom.runtime`, so every graph participates in one application memo map. Entity
atoms register exact space addressed keys. Query atoms also retain the entity keys and index ranges their handler
actually read. Local commits and reconciliation batches refresh only affected mounted reads. Mutation, pending,
receipt, scope, activation, and status atoms also require a space address. A settlement atom resolves to the lazy
durable Stream at its live tail. Mounting the atom does not consume events. Materializing that Stream owns one scoped
subscription that pages the durable settlement log; use `space.settlements({ from })` directly for replay from a
cursor or the acknowledgement floor. The membership and
aggregate atoms use separate keys, so a write does not rebuild the membership list or unrelated statuses. Mutation
and lifecycle command atoms are concurrent and preserve their typed result. Ephemeral channels are declared once with
`Ephemeral.make` (an explicit event or state kind) and drive typed publish commands and typed projections. All typed
projections for one member share the session atom's single joined stream: events are live only, state and the roster
replay their current decoded view to late subscribers, and a malformed remote value fails only the projection for its
own definition with a typed decode error. Set `publishTypingAtom` with `{ payload, ttl }` and observe the command's
`AsyncResult`. Pass an application factory with `options.factory` when the application already owns a deliberate
custom runtime.

### Infinite scroll

Express a live scrolling list as one query whose payload carries the window size, and grow the size to load more. One
`page()` over the whole window registers one footprint, so every refresh is one atomically consistent read and stays
precise: an insert at the head reruns the window, a write past its last row does not.

```ts
const MessageWindow = Query.make("MessageWindow", {
  payload: { limit: Schema.Number },
  success: Schema.Struct({ items: Schema.Array(Message.schema), hasMore: Schema.Boolean })
})

const layerMessageWindow = MessageWindow.toLayer(({ payload, query }) =>
  query.from(Message, "byCreatedAt").order("desc").limit(payload.limit).page().pipe(
    Effect.map((page) => ({ items: page.items, hasMore: page.next !== undefined }))
  )
)

const windows = graph.query(spaceId, MessageWindow)
const sizeAtom = Atom.make(50)
export const windowAtom = Atom.readable((get) => get(windows({ limit: get(sizeAtom) })))
export const loadMoreAtom = Atom.writable(
  (get) => get(sizeAtom),
  (ctx) => ctx.set(sizeAtom, Math.min(ctx.get(sizeAtom) + 50, 1_000))
)
```

Do not stitch a scrolling list from per page keyset atoms. Page boundaries derive from data, so a head insert shifts
the first page's cursor while later pages correctly do not rerun, and the concatenation silently drops the row that
moved across the boundary. The growing window has no boundary between reads and cannot tear.

A query limit is capped at 1,000 rows. Past that, split the list at application chosen anchor values instead of keyset
cursors: one live head query bounded by `gte` on the anchor and one frozen query per older segment bounded by `lt` and
`gte`. Constant bounds cannot shift, so each segment invalidates independently and precisely.

## Selective field semantics

```ts
import * as Field from "@lucas-barake/effect-local/Field"

const next = yield* transaction.applyField(Field.counter, currentCount, {
  _tag: "Increment",
  delta: 1
})
```

`Field.register`, `Field.counter`, and `Field.growOnlySet` describe explicit operations. Store the returned value in
the enclosing model through the same transaction. These semantics are domain tools, not a replication substrate.

## Testing

`TestServer.layer` adapts the real authoritative store to a production shaped `SyncEngine`. `FaultInjection` can
partition and heal the link, drop the next receipt after the server commits it, and duplicate the next catch up page.
`TestReplica.layer` is the same `SqlReplica` composition used in production.

The repository runs on Node `22.22.2`, `24.15.0`, or `26+`.

```sh
pnpm install --frozen-lockfile
pnpm check:pre-commit
pnpm build
pnpm circular
pnpm bench
```

## Rolling schema and protocol deployments

A rolling deployment can keep the previous application bundle online while the new server and client bundle roll
out. Application schema compatibility and wire protocol compatibility are separate controls. The server schema window
determines which old domain definitions can still sync. Protocol negotiation determines whether two library versions
can speak the same wire format.

Deploy the compatible server first. A new client cannot negotiate with a server that does not expose the negotiation
RPC, and an old client needs the new server to project current data back to its schema.

### Define both migration directions

Forward transforms let the current server execute an old mutation. Downgrade transforms let that server return
receipts, pull entries, and bootstrap snapshots that the old client can decode. For example, suppose version 2 adds a
`completed` field to `Task` and `PutTask`:

```ts
import * as Evolution from "@lucas-barake/effect-local/Evolution"

export const evolution = Evolution.make({
  current: definitionV2,
  steps: [Evolution.step({
    id: "definition/1-to-2",
    from: definitionV1,
    to: definitionV2,
    models: [Evolution.model({
      id: "task/1-to-2",
      from: TaskV1,
      to: TaskV2,
      value: ({ value }) => ({ ...value, completed: false }),
      downgradeValue: ({ value }) => ({
        id: value.id,
        title: value.title
      })
    })],
    mutations: [Evolution.mutation({
      id: "put-task/1-to-2",
      from: PutTaskV1,
      to: PutTaskV2,
      payload: (payload) => ({ ...payload, completed: false }),
      success: (success) => ({ ...success, completed: false }),
      downgradePayload: ({ id, title }) => ({ id, title }),
      downgradeSuccess: ({ id, title }) => ({ id, title })
    })]
  })]
})
```

Pass the same `evolution` catalog to the server and the current `SqlReplica`. The client uses it to promote durable
local state and pending mutations. The server uses it to admit old callers and project current data to them.

### Open the server schema window

`acceptedSchemaVersions` counts immediately preceding definitions in the evolution chain. A value of `1` accepts the
current definition and version N minus 1. Once the evolution catalog has steps the option is required. Omitting it
fails server layer construction with `InvalidConfiguration`, so a deployment cannot silently ship with the window
closed. Without an evolution catalog the window is implicitly zero. Construction also fails with
`InvalidConfiguration` if the requested window is missing a required downgrade transform.

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"

export const layerServer = ServerStore.layer({
  ...serverHistory,
  definition: definitionV2,
  evolution,
  acceptedSchemaVersions: 1,
  authorizeAccess: ({ clientId, principal, spaceId }) => authorizeClient({ clientId, principal, spaceId }),
  authorizeMutation: ({ mutation, principal }) => authorizeMutation({ mutation, principal }),
  authorizeRead: (input) => ReadPolicy.use((policy) => policy.authorize(input))
})
```

The authorization functions above are application Effects. Their requirements propagate to the server Layer.
`ServerStore.layerTrusted` remains for tests and already trusted processes. Do not use a digest as authorization.
Client and space ownership belongs in `authorizeAccess`.

### Wake disconnected clients

Add `offlineWake` when accepted mutations should notify clients without a live Watch. The library stores, coalesces,
retries, and retires wake work. The application supplies authoritative space membership and provider delivery.

```ts
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"

export const ServerLive = ServerStore.layer({
  ...serverOptions,
  offlineWake: {
    recipients: ({ spaceId }) => Memberships.clientIds(spaceId),
    deliver: (wake) =>
      Memberships.deliverIfCurrent(
        wake,
        ({ wakeId, spaceId, clientId }) => Push.sendContentFree({ idempotencyKey: wakeId, spaceId, clientId })
      ),
    coalescingWindow: "2 seconds",
    pollInterval: "1 second",
    retryDelay: "1 second",
    maximumRetryDelay: "1 minute",
    claimLeaseDuration: "30 seconds",
    hookTimeout: "10 seconds",
    presenceLeaseDuration: "30 seconds",
    presenceHeartbeatInterval: "10 seconds",
    claimBatchSize: 128,
    maximumConcurrentRecipientResolutions: 8,
    maximumConcurrentDeliveries: 32,
    maximumRecipientsPerSpace: 10_000
  }
})
```

The delivery hook must serialize its final membership check with the provider send, then return `"Delivered"` or
`"NotRecipient"`. `"NotRecipient"` retires the current work. The hook receives routing and idempotency IDs, but no
mutation or entity content. Keep provider-visible notification content free of those IDs and all sync data. Once
delivery starts, it is at least once: a failure, defect, timeout, or database failure after the provider send can retry
the same `wakeId`, so the send must be idempotent. The dispatcher coalesces mutations behind a high water fence. A live
Watch suppresses delivery across all server runtimes sharing the database. An acknowledged Pull cursor retires covered
work. Configure the same `offlineWake` adapter on every runtime that shares the database and accepts Watch streams so
each runtime publishes its live presence. See [synchronization](docs/sync.md#offline-wake-delivery) for the full
delivery and recovery contract.

### Negotiate one protocol for sync and ephemera

During a protocol rollout, advertise the overlap on the server and share one client `ProtocolSession` between sync and
ephemera. Sharing the session gives both services one selected version and one renegotiation gate.

```ts
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as ProtocolSession from "@lucas-barake/effect-local-rpc/ProtocolSession"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as SyncServer from "@lucas-barake/effect-local-rpc/SyncServer"
import * as Layer from "effect/Layer"

export const layerServerRpc = SyncServer.layerWithOptions({
  supportedProtocolVersions: [1, 2]
})

const layerSession = ProtocolSession.layerWithOptions({
  supportedProtocolVersions: [1, 2],
  sessionAcquisitionTimeout: "10 seconds"
})

export const layerClientRpc = Layer.merge(
  SyncClient.layerFromSession({ rpcTimeout: "10 seconds" }),
  EphemeralClient.layerFromSession({ rpcTimeout: "10 seconds", heartbeatInterval: "20 seconds" })
).pipe(
  Layer.provide(layerSession),
  Layer.provide(layerRpcProtocol),
  Layer.provide(layerAuthentication)
)
```

Negotiation selects the highest shared version. A rolling peer that rejects a cached version causes one renegotiation
and retry. If there is no common version, the client receives terminal `UpgradeRequired` instead of retrying a decode
failure forever.

### Prompt compatible old clients to reload

An accepted old client continues syncing. Its space status changes to `SchemaUpdateAvailable`, which includes the
server schema identity so the application can show a reload prompt without treating the replica as failed.

```ts
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"

export const promptForReload = Replica.Replica.use((replica) =>
  Effect.gen(function*() {
    const space = yield* replica.space(spaceId)
    const status = yield* space.status

    if (status._tag === "SchemaUpdateAvailable") {
      yield* showReloadPrompt({ serverVersion: status.serverSchema.version })
    }
  })
)
```

### Recover a rejected pending mutation

If a pending mutation cannot be replayed during client schema promotion, startup still completes. The original
envelope and typed rejection move to durable quarantine. That item blocks later pending work from passing its local
sequence until the application resubmits a corrected payload or explicitly discards it.

```ts
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"

export const recoverQuarantine = Replica.Replica.use((replica) =>
  Effect.gen(function*() {
    const space = yield* replica.space(spaceId)
    const [item] = yield* space.quarantine

    if (item !== undefined) {
      const result = yield* space.resubmitQuarantined(
        item.envelope.mutationId,
        PutTaskV2,
        { id: "task-1", title: "Ship safely", completed: false }
      )

      if (result._tag === "Resubmitted") {
        yield* showRecoveredMutation(result.pending.envelope.mutationId)
      }
    }
  })
)
```

Use `space.discardQuarantined(item.envelope.mutationId)` when the operation should never run. Discard advances the
server side client sequence without executing the mutation handler. Both operations are idempotent across interruption
and restart.

The complete deployment sequence is:

1. Deploy a server that accepts the old schema and advertises both protocol versions.
2. Deploy the new client bundle with the same evolution catalog and protocol overlap.
3. Prompt compatible old clients to reload and monitor their remaining usage.
4. Reduce `acceptedSchemaVersions` only after the application deprecation horizon.
5. Remove the old protocol version only after no deployed client requires it.

## Guarantees and limits

- Local mutation commit is atomic in SQLite and does not require a network.
- Exact mutation retries are idempotent. Reusing an identity with different canonical bytes fails.
- Accepted server sequences are dense per space. Clients install only contiguous entries.
- Pull entries expose only public mutation identity and canonical changes. Payloads and success results are not in the
  shared authoritative log.
- Steady sync cost is proportional to the changed entities and the client's acknowledged view, not to space size. A
  windowed scope bounds the acknowledged view, so per-message sync stays flat as server history grows. Received pages
  and settlements apply incrementally on the client; the full projection rebuild remains for bootstrap, revocation,
  schema evolution, and crash recovery.
- A terminal rejection rolls back its optimistic write set and replays remaining pending mutations.
- Queues, mutation payloads, ephemeral payloads and state, pull pages, bootstrap pages, snapshots, receipts, and retained history
  are bounded by explicit configuration.
- Ephemeral roster, live events, and retained state are best effort, server expired, multi-space isolated, and never
  enter the durable mutation log. Persist read or delivery positions with a normal application mutation when they must
  survive server restart or the configured state TTL.
- Cluster routes each space to one live owner across runners. Entity operations are volatile. A failed submit remains
  in the client's pending SQLite outbox until exact resubmission returns the SQL backed terminal receipt. Pull and watch
  recover from the durable server sequence.
- Workflow executions are finite and generation keyed. SQLite progress repairs lost wakes and browser termination when
  a runner starts again.
- The server is an authority, not a peer. Conflict behavior is arrival order unless a handler explicitly applies field
  semantics.
- SQL storage schemas advance through an ordered checksum validated migration catalog. A lifecycle migration still
  requires old server writers to stop before they can issue a legacy SQL write shape. This is separate from the
  supported mixed application schema and wire protocol window. There is no backward SQL migration, multi writer
  browser ownership coordinator, encryption layer, or stable v1 compatibility promise yet.
