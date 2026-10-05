# Architecture

Effect Local has two authority domains.

The client owns local availability. It assigns `(spaceId, clientId, membershipIncarnation, localSequence)`, generates an immutable
`mutationId`, computes a digest over the canonical envelope, executes the mutation optimistically, and commits the
pending mutation plus visible writes to SQLite. None of those steps require the server.

The server owns shared order. It verifies the digest, authenticates the principal, serializes admission per space,
deduplicates the mutation identity, checks the next client sequence, authorizes and executes the handler, and stores a
terminal receipt. Accepted mutations alone receive the next dense server sequence and enter the authoritative log.

## State model

Client SQLite contains one durable `clientId` plus a membership row for every joined space. All remaining client rows
are partitioned by `space_id`. Each membership contains:

- replica identity, definition hash, next local sequence, dense server watermark, visible revision, and requested and
  completed reconciliation generations
- normalized desired replication scope, scope generation, principal bound view cursor, and durable retractions
- canonical entities from accepted server entries
- visible entities after optimistic pending mutations
- pending envelopes, results, write sets, submission states, and attempt counts ordered by local sequence
- a bounded accepted server suffix ordered by server sequence
- bounded terminal accepted, rejected, and expired receipts
- one resumable scoped bootstrap stage and its verified authorized entities
- the installed snapshot identity, accepted sequence, and terminal fence

Server SQL contains:

- a definition hash, accepted and terminal sequence heads, retained floors, exact row counters, and snapshot pointer per
  space
- a last processed and expired local sequence per `(spaceId, clientId, membershipIncarnation)`
- retained immutable receipts keyed by both mutation identity and client sequence
- a retained dense accepted suffix keyed by server sequence
- authoritative materialized entities with exact snapshot byte accounting
- immutable snapshot manifests and deterministically ordered snapshot entities
- one principal bound materialized replication view per client, with a dense view cursor and at most one immutable
  outstanding page
- immutable client and scope bound snapshot manifests and their authorized current entity rows

Workflow storage contains reconciliation execution control only. It does not contain mutation payloads, receipts, log
entries, or canonical state. Cluster messages and publications are routed wake and bounded ephemeral signals. They are never
application data authority.

## Mutation lifecycle

1. The client reads its durable cursor as the mutation basis.
2. One SQLite transaction allocates the next local sequence, runs the handler, stores the pending envelope and write
   set, and changes visible entities.
3. The same transaction increments the requested reconciliation generation. A finite Workflow coalesces durable
   generations and runs an idempotent reconciliation pass. Retry always uses the same envelope bytes and identity.
4. The server locks the space order row inside its transaction. It returns an existing exact receipt or processes the
   next client sequence.
5. A rejection is stored without advancing the accepted sequence. An acceptance appends public mutation identity and
   its exact write set at the next sequence. The success result remains only in the submitter's receipt.
6. The client pulls from its durable view cursor. The server diffs the acknowledged principal bound view against the
   current scope and entity authorization. Bounded `Upsert`, `Delete`, and `Retract` pages advance a dense view
   revision. Final pages also advance the separate dense server watermark used as the next mutation basis.
7. A missing, rotated, or schema invalidated view receives `BootstrapRequired`. Bootstrap pages repeat the immutable
   scoped manifest. The client durably verifies identity, order, Schema values, entity bytes, and the chained digest.
   Completion replaces canonical state, locally retracts prior canonical identities absent from the authorized
   snapshot, preserves pending only optimistic identities, and advances both cursors in one transaction.
8. The client removes settled pending mutations, restores touched visible entities from canonical state, and replays
   remaining pending mutations in local order. Only after that projection is visible does it publish the terminal
   settlement to current subscribers. Mutation rejections are decoded through the originating mutation schema.
9. Successful reconciliation advances the completed generation idempotently. A newer requested generation starts a
   new finite Workflow.
10. Effect `Reactivity` invalidates affected models, pending inspection, receipts, and status after the SQL transaction
    commits. Every invalidation is delivered one key at a time, after the state it announces is applied and before
    the operation's waiters resume. A subscriber that throws is logged at error level with its key and never fails
    the operation that notified it. Subscribers registered after it on the same key miss that one notification,
    because Effect `Reactivity` stops iterating a key's handlers at the first throw. A notification that a custom
    `Reactivity` ends with an interruption or a failure never skips bookkeeping or strands a waiter. The operation
    finishes its state changes and then ends with that cause, and a commit logs it and returns the committed
    mutation. Notifications raised on the fiber that called `join`, `activate`, `deactivate`, `setScope`, or an
    operation that activates a space join that caller's `Reactivity.withBatch` and run when the batch ends.
    Notifications raised on a fiber the library owns, including a commit's, a leave's, and a background turn's, are
    delivered immediately. Delivery is interruptible, so a custom `Reactivity` whose `invalidate` never returns
    cannot hold back a deactivation, a leave, or the replica's shutdown. Each waiter of an operation is resumed on
    its own, so a completion callback that throws is logged and does not keep the other waiters from resuming.

The local commit and server settlement contracts are separate. `mutate` returns after the optimistic SQLite commit and
never widens its error channel with a later server outcome. Durable pending inspection exposes the decoded payload,
submission state, and attempt count. Settlement is durable: the transaction that removes a settled pending row also
stamps its receipt with a pending snapshot and a monotonic settlement sequence, so the receipts table is the
settlement log. Subscribers page that log from a cursor and wait on a per space coalescing wake signal that carries no
data, so a slow or absent consumer can never backpressure settlement recording or reconciliation, and a settlement
recorded across a process restart is replayable. Duplicate receipt delivery cannot record twice because only the
transition that removes an existing pending row creates a settlement. Acknowledging a sequence sets the retention
floor; pruning prefers acknowledged settlements but always enforces the retained receipt budget, and a replay behind
the prune horizon fails with a typed truncation error.

## Handler contract

Mutation handlers receive only their decoded payload and a constrained `Transaction`. Query handlers receive their
decoded payload and a read only `Query` capability. `Mutation.toLayer` and `Query.toLayer` capture ordinary Effect
dependencies when their Layer is built.

Handlers must be deterministic across client and server execution. Time, randomness, server generated values, and
environment dependent decisions belong in the payload or admission policy. A handler may use explicit
`Field.Semantics` when a field needs operation semantics. The normal model value remains plain Schema encoded JSON.

## Ordering and identity

Mutation identity and accepted order are separate.

- `(spaceId, clientId, membershipIncarnation, localSequence)` provides a monotonic membership order.
- `membershipIncarnation` allows a fully evicted space to rejoin with a fresh local sequence without reusing server lineage.
- `mutationId` provides a stable random retry identity.
- `digest` rejects reuse of either identity with different bytes.
- `basis` records the accepted cursor observed when the mutation was created.
- `serverSequence` is dense per space and exists only for accepted mutations.
- `(viewId, revision)` is dense per principal bound client view and can advance without an entity change.

A terminal rejection advances the server's client sequence watermark but does not create a hole in the accepted log.
Receipt reclamation advances an expired local sequence watermark atomically with deletion. A retry at or below that
watermark returns `Expired` bound to a published snapshot fence and never executes again.

## Effect runtime

Public capabilities are `Context.Service` values. Implementations are scoped Layers. SQL transactions and errors stay
in Effects. Callers select either the lightweight in memory reconciliation Layer or the finite Workflow Layer. The
Workflow payload contains only the schema identity, space, client, membership incarnation, replication scope and scope
generation, and reconciliation generation. Activities call the same idempotent reconciliation operation as the in
memory scheduler.

The server front door is an authenticated WebSocket RPC facade. It routes every operation by space to one Effect
Cluster entity, `EffectLocal/Space`, so an active space costs one resident entity. The entity runs SubmitBatch and Discard
sequentially behind one admission permit and serves Pull, immutable snapshot Bootstrap pages, sync watches, joined
ephemeral streams, publications, and heartbeats concurrently. Join authorization precedes the Hub watcher bound.
Because only admission is serialized, a full join population or a paused Bootstrap page cannot occupy mutation
admission.

An accepted admission publishes a shared in memory wake after its SQL transaction commits. Subscribers read that
publication from the per space hub. Fanout does not acquire a SQLite transaction or write a space row for each watcher.
Pull remains the durable repair path when a wake is lost.

Entity operations are volatile. Client SQLite owns a mutation until admission returns its exact receipt. Server SQL then
owns the authoritative mutation log, terminal receipt, canonical changes, and total sequence. A runner failure before
the SQL commit leaves the client mutation pending. A lost reply after commit is repaired by exact idempotent
resubmission. Cluster provides ownership and routing without retaining a second permanent copy of every mutation payload
and private reply.

Applications provide Effect's Cluster runner, storage, and transport Layers. A single process can use
`SingleRunner`. Sharded deployments can use shared runner storage and the Node runner transport, with one shared
`assertionSecret` so each runner verifies the principal assertions another runner's gateway signed. The authoritative
server SQL database must remain reachable after shard reassignment. Pod-local SQLite is valid only when deployment
placement keeps that database with its space owner.

In the browser, the tabs of one origin form their own Effect Cluster. Each tab is a runner. Holding a Web Lock named after the runner is liveness. Only the leader tab reports itself ready, and only once its replica is open, so every shard, and with it the replica entity, lives on the leader tab, which alone opens the SQLite database. Tabs exchange Cluster
frames over one `BroadcastChannel` inbox per tab. When the leader tab closes its locks are released, the transport
fails the calls addressed to it, Cluster resends them to the next leader, and caller-minted mutation ids keep resent
mutations from running twice. Cluster fibers dispatch through a private `MessageChannel`, because hidden tabs throttle
timers and the leader is often hidden.

The Atom graph defaults to Effect's shared `Atom.runtime`. The replica Layer and the graph's atoms therefore share one
application memo map and one memoized `Reactivity` service, on which the graph registers its invalidation keys. Every
atom and invalidation key includes its space.
Entity keys invalidate exact records. Query dependencies invalidate once per space and model.

## Capacity

Protocol limits bound a mutation, a pull page, a bootstrap page, and ephemeral requests by count and encoded bytes.
Server options set hard history, receipt, entity, snapshot byte, and bootstrap byte limits. Admission cross checks
space counters against trigger maintained shadow counters under the lock before executing a handler and checks
resulting snapshot capacity inside the handler savepoint. The client bounds pending mutations, retained receipts,
accepted evidence, staged entities, staged bytes, and incoming page bytes. The in memory reconciler uses one keyed
dispatcher with independent watches and turns. Workflow generations coalesce durable requests. Streams and
publications carry only notifications.

Every `SpaceEntity.HandlerOptions` field is optional. One `mailboxCapacity` (a positive safe integer or `"unbounded"`)
bounds the active requests of a space entity, open `Watch` and `JoinEphemeral` streams included. When it is omitted,
Cluster's `ShardingConfig.entityMailboxCapacity` applies. `maximumConcurrentBootstrapAuthorizations` (64),
`maximumConcurrentBootstrapPagesPerSpace` (4), `maximumConcurrentEphemeralJoinVerificationsPerSpace` (64), and
`maximumConcurrentEphemeralRequestsPerSpace` (64) default to the values shown and must be positive safe integers. Join
verification and ephemeral publish and heartbeat work use separate per space allowances. Bootstrap assertion
verification and preparation share one fail fast Layer wide allowance. Published page reads use a separate per space
allowance. Saturation reports `CapacityExceeded` with resource `bootstrap authorizations`, `bootstrap pages`, or
`ephemeral join verifications`.

`ServerStore.maximumWatchersPerSpace` is the active sync watcher allowance. `EphemeralHub.maximumWatchersPerSpace` is a
separate joined-stream allowance. `ServerStore.wakeCapacity` is the sliding sync hint depth. `EphemeralHub.capacity`
bounds shared sliding delta history. A subscriber that observes a revision gap rejoins from a fresh roster and
retained-state snapshot without reconnecting healthy subscribers. Excess watchers fail with typed `CapacityExceeded` resources `sync watchers` or `ephemeral
watchers`. Interrupted, denied, and revoked streams release their watcher allowance.

Both stores also cap the watchers one authenticated principal holds in a space with `maximumWatchersPerPrincipal`,
checked in the same admission step as the space allowance. The key is the canonical principal because client IDs are
chosen by clients and are not bound to a principal, so a per client cap could be evaded by rotating client IDs over one
socket. Excess watchers fail with `sync watchers per principal` or `ephemeral watchers per principal`.

Sync watch authorization successes share one structural `(spaceId, clientId, normalized scope, principal)` lookup and expire after
`readAuthorizationRefreshInterval`. `maximumConcurrentReadAuthorizations` bounds policy work and
`maximumPendingReadAuthorizations` independently bounds all live authorization callers and distinct owner lookups. It
also bounds distinct per-wake visibility evaluations waiting for policy execution. `readAuthorizationCacheCapacity`
bounds completed successes. These limits are independent of the active watcher allowance. Equal lookups share one result
within the caller allowance and denials are not cached. Overflow fails with typed
`CapacityExceeded { resource: "read authorizations", limit }`. Each watch begins refresh halfway
through the interval. If a fresh success is not available at the existing success expiry, the watcher scope closes and
the stream fails with `AuthorizationDenied`. The configured interval is therefore the fail closed revocation bound even
when policy work hangs or the client stops pulling. Pull and Bootstrap perform uncached one shot authorization checks.

Capacity failures use the Schema tagged `CapacityExceeded { resource, limit }` error across SQL, ephemera, and RPC
boundaries. `resource` is the closed union `ReplicaError.CapacityResource`, so reconciliation can decide per resource
whether a limit is worth retrying. Full Cluster mailboxes map to `ServerUnavailable` because the request did not enter
its domain handler.

Operational metrics report admission outcome and rejection class, history and receipt depth beside their limits, sync
and ephemeral watcher populations, wake fanout duration, durable bootstrap installs, maintenance outcomes and prune
volumes, and pending client mutation population. Labels are bounded categories. They do not contain resource
identifiers. The production fanout benchmark is `packages/local-rpc/bench/Fanout.bench.ts`.

History maintenance is an explicit service lifecycle. It reads one consistent bounded materialized state, builds an
immutable manifest in memory, then locks the space and compares both sequence heads. Only an unchanged candidate is
inserted and published. The same transaction advances logical floors before deleting bounded prefixes. A crash can
leave surplus rows, but it cannot publish a floor without a complete recovery snapshot.

Schema promotion changes the active entity generation in one constant work transaction. That flip logically fences
old replication views by schema identity. Scoped snapshot entries, manifests, pages, view members, and views are then
deleted child first in bounded resumable phases before old entity generations are reclaimed.
