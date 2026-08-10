# Architecture

Effect Local has two authority domains.

The client owns local availability. It assigns `(spaceId, clientId, localSequence)`, generates an immutable
`mutationId`, computes a digest over the canonical envelope, executes the mutation optimistically, and commits the
pending mutation plus visible writes to SQLite. None of those steps require the server.

The server owns shared order. It verifies the digest, authenticates the principal, serializes admission per space,
deduplicates the mutation identity, checks the next client sequence, authorizes and executes the handler, and stores a
terminal receipt. Accepted mutations alone receive the next dense server sequence and enter the authoritative log.

## State model

Client SQLite contains:

- replica identity, definition hash, next local sequence, server cursor, visible revision, and requested and completed
  reconciliation generations
- canonical entities from accepted server entries
- visible entities after optimistic pending mutations
- pending envelopes, results, and write sets ordered by local sequence
- accepted server entries ordered by server sequence
- terminal accepted and rejected receipts

Server SQL contains:

- a definition hash and next accepted sequence per space
- a last terminal local sequence per `(spaceId, clientId)`
- immutable receipts keyed by both mutation identity and client sequence
- accepted entries keyed by server sequence
- authoritative materialized entities

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
6. The client pulls from its durable cursor. It installs only contiguous entries into canonical state.
7. The client removes settled pending mutations, restores touched visible entities from canonical state, and replays
   remaining pending mutations in local order.
8. Successful reconciliation advances the completed generation idempotently. A newer requested generation starts a
   new finite Workflow.
9. Effect `Reactivity` invalidates affected models, receipts, and status after the SQL transaction commits.

## Handler contract

Mutation handlers receive only their decoded payload and a constrained `Transaction`. Query handlers receive their
decoded payload and a read only `Query` capability. `Mutation.toLayer` and `Query.toLayer` capture ordinary Effect
dependencies when their Layer is built.

Handlers must be deterministic across client and server execution. Time, randomness, server generated values, and
environment dependent decisions belong in the payload or admission policy. A handler may use explicit
`Field.Semantics` when a field needs operation semantics. The normal model value remains plain Schema encoded JSON.

## Ordering and identity

Mutation identity and accepted order are separate.

- `(spaceId, clientId, localSequence)` provides a monotonic client session order.
- `mutationId` provides a stable random retry identity.
- `digest` rejects reuse of either identity with different bytes.
- `basis` records the accepted cursor observed when the mutation was created.
- `serverSequence` is dense per space and exists only for accepted mutations.

A terminal rejection advances the server's client sequence watermark but does not create a hole in the accepted log.

## Effect runtime

Public capabilities are `Context.Service` values. Implementations are scoped Layers. SQL transactions and errors stay
in Effects. Callers select either the lightweight in memory reconciliation Layer or the finite Workflow Layer. The
Workflow payload contains only definition, space, client, and generation identity. Activities call the same
idempotent reconciliation operation as the in memory scheduler.

The server front door is an authenticated WebSocket RPC facade. It routes submit, pull, watch, presence publish, and
presence watch through one volatile Effect Cluster entity keyed by space. That owner holds the per-space wake and
presence hubs, so a client connected to one runner observes work admitted through another. Cluster does not wrap the
server SQL transaction. This keeps durable receipt insertion, canonical writes, log insertion, sequence allocation,
and post-commit wake ordering under one transaction owner.

Applications provide Effect's Cluster runner, storage, and transport Layers. A single process can use
`SingleRunner`. Sharded deployments can use shared runner storage and the Node runner transport. The authoritative
server SQL database must remain reachable after shard reassignment. Pod-local SQLite is valid only when deployment
placement keeps that database with its space owner.

The browser graph defaults to Effect's shared `Atom.runtime`. The replica Layer and `factory.withReactivity` therefore
share one application memo map and memoized `Reactivity` service. Entity keys invalidate exact records. Query
dependencies invalidate once per model.

## Capacity

Protocol limits bound a mutation, a pull page by count and encoded bytes, and a presence update. Local pending count,
server wake publication, presence publication, and in memory reconciler wakeup are bounded or sliding. Workflow
generations coalesce durable requests. Pull pages contain authoritative entries. Streams and publications carry only
notifications.
