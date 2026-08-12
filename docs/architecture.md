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
    commits.

The local commit and server settlement contracts are separate. `mutate` returns after the optimistic SQLite commit and
never widens its error channel with a later server outcome. Durable pending inspection exposes the decoded payload,
submission state, and attempt count. Settlement notifications use a bounded, replay free PubSub per space, exposed as
a Stream. The space scope owns and shuts down that PubSub. Its configured capacity bounds the live feed, so a slow
subscriber backpressures settlement publication and reconciliation completion without delaying the local mutation
commit. Publication occurs after durable pending removal, projection rebuild, and reactive invalidation. Duplicate
receipt delivery cannot publish twice because only the transition that removes an existing pending row creates a
settlement. A process crash may lose the live notification. Retained receipts remain the durable lookup path.

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
Workflow payload contains only definition, space, client, membership incarnation, and generation identity. Activities call the same
idempotent reconciliation operation as the in memory scheduler.

The server front door is an authenticated WebSocket RPC facade. It routes every operation by space through four Effect
Cluster entity types. `SpaceAdmissionEntity` runs Submit and Discard sequentially. `SpaceReadEntity` forks Pull and
immutable snapshot Bootstrap pages. `SpaceWatchEntity` forks long lived sync and presence streams.
`SpacePresencePublishEntity` runs bounded concurrent presence publications. Each entity has its own required finite
mailbox, so a paused Bootstrap page or a full watch lane cannot occupy mutation admission.

An accepted admission publishes a shared in memory wake after its SQL transaction commits. Subscribers read that
publication from the per space hub. Fanout does not acquire a SQLite transaction or write a space row for each watcher.
Pull remains the durable repair path when a wake is lost.

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
`maximumConcurrentBootstrapAuthorizations`, `maximumConcurrentBootstrapPagesPerSpace`, and
`maximumConcurrentPresencePublicationsPerSpace`. Bootstrap assertion verification and preparation share one fail fast
Layer wide allowance. Published page reads use a separate per space allowance. Saturation reports `CapacityExceeded`
with resource `bootstrap authorizations` or `bootstrap pages`.

`ServerStore.maximumWatchersPerSpace` is the active sync watcher allowance. `PresenceHub.maximumWatchersPerSpace` is a
separate active presence watcher allowance. `ServerStore.wakeCapacity` and `PresenceHub.capacity` are sliding
publication queue depths, not watcher limits. Excess watchers fail with typed `CapacityExceeded` resources `sync
watchers` or `presence watchers`. Interrupted, denied, and revoked streams release their watcher allowance.

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

Schema promotion changes the active entity generation in one constant work transaction. That flip logically fences
old replication views by schema identity. Scoped snapshot entries, manifests, pages, view members, and views are then
deleted child first in bounded resumable phases before old entity generations are reclaimed.
