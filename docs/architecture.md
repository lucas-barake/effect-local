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

- replica identity, definition hash, next local sequence, server cursor, and visible revision
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

In memory queues and publications are wake signals. They are never authority.

## Mutation lifecycle

1. The client reads its durable cursor as the mutation basis.
2. One SQLite transaction allocates the next local sequence, runs the handler, stores the pending envelope and write
   set, and changes visible entities.
3. The reconciler submits pending envelopes in local sequence order. Retry always uses the same bytes and identity.
4. The server locks the space order row inside its transaction. It returns an existing exact receipt or processes the
   next client sequence.
5. A rejection is stored without advancing the accepted sequence. An acceptance appends its exact write set and
   result at the next sequence.
6. The client pulls from its durable cursor. It installs only contiguous entries into canonical state.
7. The client removes settled pending mutations, restores touched visible entities from canonical state, and replays
   remaining pending mutations in local order.
8. Effect `Reactivity` invalidates affected models, receipts, and status after the SQL transaction commits.

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
in Effects. The reconciler owns bounded wake queues, a single permit, a watch fiber, and a retrying worker fiber. Scope
closure interrupts workers and releases subscriptions.

The browser graph uses one `Atom.context` and memo map. The replica Layer and `factory.withReactivity` therefore share
one memoized `Reactivity` service. Entity keys invalidate exact records. Query dependencies invalidate by model.

## Capacity

Protocol limits bound a mutation, a pull page by count and encoded bytes, and a presence update. Local pending count,
server wake publication, presence publication, and reconciler wakeup are bounded or sliding. Pull pages contain
authoritative entries. Streams and publications carry only notifications.
