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

- replica identity, definition hash, next local sequence, server cursor, visible revision, and requested and completed
  reconciliation generations
- canonical entities from accepted server entries
- visible entities after optimistic pending mutations
- pending envelopes, results, and write sets ordered by local sequence
- a bounded accepted server suffix ordered by server sequence
- bounded terminal accepted, rejected, and expired receipts
- one resumable bootstrap stage and its verified entities
- the installed snapshot identity, accepted sequence, and terminal fence

Server SQL contains:

- a definition hash, accepted and terminal sequence heads, retained floors, exact row counters, and snapshot pointer per
  space
- a last processed and expired local sequence per `(spaceId, clientId, membershipIncarnation)`
- retained immutable receipts keyed by both mutation identity and client sequence
- a retained dense accepted suffix keyed by server sequence
- authoritative materialized entities with exact snapshot byte accounting
- immutable snapshot manifests and deterministically ordered snapshot entities

Workflow storage contains reconciliation execution control only. It does not contain mutation payloads, receipts, log
entries, or canonical state. Cluster messages and publications are routed wake and presence signals. They are never
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
6. The client pulls from its durable cursor. It installs only contiguous entries into canonical state. A cursor below
   the retained floor receives `BootstrapRequired` instead of a false gap or an unbounded replay.
7. Bootstrap pages repeat the immutable manifest and are count and byte bounded. The client durably verifies identity,
   order, Schema values, entity bytes, and the chained digest. Completion replaces canonical state and advances the
   cursor in one transaction.
8. The client removes settled pending mutations, restores touched visible entities from canonical state, and replays
   remaining pending mutations in local order.
9. Successful reconciliation advances the completed generation idempotently. A newer requested generation starts a
   new finite Workflow.
10. Effect `Reactivity` invalidates affected models, receipts, and status after the SQL transaction commits.

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

A terminal rejection advances the server's client sequence watermark but does not create a hole in the accepted log.
Receipt reclamation advances an expired local sequence watermark atomically with deletion. A retry at or below that
watermark returns `Expired` bound to a published snapshot fence and never executes again.

## Effect runtime

Public capabilities are `Context.Service` values. Implementations are scoped Layers. SQL transactions and errors stay
in Effects. Callers select either the lightweight in memory reconciliation Layer or the finite Workflow Layer. The
Workflow payload contains only definition, space, client, membership incarnation, and generation identity. Activities call the same
idempotent reconciliation operation as the in memory scheduler.

The server front door is an authenticated WebSocket RPC facade. It routes every operation by space through four Effect
Cluster entity types. `SpaceAdmissionEntity` runs Submit and Discard sequentially. `SpaceReadEntity` forks Pull and
immutable snapshot Bootstrap pages. `SpaceWatchEntity` forks long lived sync and presence streams.
`SpacePresencePublishEntity` runs bounded concurrent presence publications. Each entity has its own required finite
mailbox, so a paused Bootstrap page or a full watch lane cannot occupy mutation admission.

An accepted mutation publishes one in memory wake after its SQL transaction commits. Subscribers read that publication
from the per space hub. Fanout does not acquire a SQLite transaction or write a space row for each watcher. Pull remains
the durable repair path when a wake is lost.

Entity operations are volatile. Client SQLite owns a mutation until admission returns its exact receipt. Server SQL then
owns the authoritative mutation log, terminal receipt, canonical changes, and total sequence. A runner failure before
the SQL commit leaves the client mutation pending. A lost reply after commit is repaired by exact idempotent
resubmission. Cluster provides ownership and routing without retaining a second permanent copy of every mutation payload
and private reply.

Applications provide Effect's Cluster runner, storage, and transport Layers. A single process can use
`SingleRunner`. Sharded deployments can use shared runner storage and the Node runner transport. The authoritative
server SQL database must remain reachable after shard reassignment. Pod-local SQLite is valid only when deployment
placement keeps that database with its space owner.

The browser graph defaults to Effect's shared `Atom.runtime`. The replica Layer and `factory.withReactivity` therefore
share one application memo map and memoized `Reactivity` service. Every atom and invalidation key includes its space.
Entity keys invalidate exact records. Query dependencies invalidate once per space and model.

## Capacity

Protocol limits bound a mutation, a pull page, a bootstrap page, and a presence update by count and encoded bytes.
Server options set hard history, receipt, entity, snapshot byte, and bootstrap byte limits. Admission cross checks
space counters against trigger maintained shadow counters under the lock before executing a handler and checks
resulting snapshot capacity inside the handler savepoint. The client bounds pending mutations, retained receipts,
accepted evidence, staged entities, staged bytes, and incoming page bytes. The in memory reconciler uses one keyed
dispatcher with independent watches and turns. Workflow generations coalesce durable requests. Streams and
publications carry only notifications.

`SpaceEntity.HandlerOptions` requires positive finite `admissionMailboxCapacity`, `readMailboxCapacity`,
`watchMailboxCapacity`, and `presencePublicationMailboxCapacity` values. It also requires
`maximumConcurrentBootstrapPagesPerSpace` and `maximumConcurrentPresencePublicationsPerSpace`. Bootstrap fails fast
with `CapacityExceeded { resource: "bootstrap pages", limit }` when its work allowance is full.

`ServerStore.maximumWatchersPerSpace` is the active sync watcher allowance. `PresenceHub.maximumWatchersPerSpace` is a
separate active presence watcher allowance. `ServerStore.wakeCapacity` and `PresenceHub.capacity` are sliding
publication queue depths, not watcher limits. Excess watchers fail with typed `CapacityExceeded` resources `sync
watchers` or `presence watchers`. Interrupted, denied, and revoked streams release their watcher allowance.

Sync watch authorization successes share one structural `(spaceId, principal)` lookup and expire after
`readAuthorizationRefreshInterval`. `maximumConcurrentReadAuthorizations` bounds policy work and
`readAuthorizationCacheCapacity` bounds completed successes. Pending equal lookups share one result and denials are not
cached. Each watch begins refresh halfway through the interval. If a fresh success is not available at the existing
success expiry, the watcher scope closes and the stream fails with `AuthorizationDenied`. The configured interval is
therefore the fail closed revocation bound even when policy work hangs or the client stops pulling. Pull and Bootstrap
perform uncached one shot authorization checks.

Capacity failures use the Schema tagged `CapacityExceeded { resource, limit }` error across SQL, presence, and RPC
boundaries. Full Cluster mailboxes map to `ServerUnavailable` because the request did not enter its domain handler.

Operational metrics report admission outcome and rejection class, history and receipt depth beside their limits, sync
and presence watcher populations, wake fanout duration, durable bootstrap installs, maintenance outcomes and prune
volumes, and pending client mutation population. Labels are bounded categories. They do not contain resource
identifiers. The production fanout benchmark is `packages/local-rpc/bench/Fanout.bench.ts`.

History maintenance is an explicit service lifecycle. It reads one consistent bounded materialized state, builds an
immutable manifest in memory, then locks the space and compares both sequence heads. Only an unchanged candidate is
inserted and published. The same transaction advances logical floors before deleting bounded prefixes. A crash can
leave surplus rows, but it cannot publish a floor without a complete recovery snapshot.
