import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"

export interface Entry<A,> {
  readonly peerId: Identity.PeerId
  readonly value: A
  readonly expiresAtMillis: number
  readonly identity: "transport-peer"
}

export interface Presence<A,> {
  readonly receive: (peerId: Identity.PeerId, value: unknown) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly publish: (
    peerId: Identity.PeerId,
    value: unknown
  ) => Effect.Effect<void, ReplicaError.ReplicaError, Scope.Scope>
  readonly remove: (peerId: Identity.PeerId) => Effect.Effect<void>
  readonly values: Effect.Effect<ReadonlyArray<Entry<A>>>
}

export const make = <A,>(schema: Schema.Decoder<A>, options: { readonly timeToLive: Duration.Input }) =>
  Effect.gen(function*() {
    const timeToLiveMillis = Duration.toMillis(options.timeToLive)
    if (
      !Number.isFinite(timeToLiveMillis) ||
      timeToLiveMillis <= 0 ||
      timeToLiveMillis > Number.MAX_SAFE_INTEGER
    ) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "schema-valid presence payload",
          observed: "timeToLive must resolve to positive finite milliseconds within the safe integer range"
        })
      })
    }
    // `entries` is closure private and is mutated in place inside the Ref callbacks below, so a single
    // callback is the whole critical section. The map must never escape: no accessor may return it, the
    // Ref, or a stored entry, `values` must keep copying entries field by field, and `Ref.getUnsafe` and
    // `Ref.updateSome` must not be used on it.
    const entries = yield* Ref.make(new Map<Identity.PeerId, Entry<A> & { readonly token: number }>())
    // Monotonic arrival sequence. Plain mutable state on purpose: each mint happens inside a synchronous
    // callback with no suspension point, so two mints can never interleave and the counter needs no Ref. A
    // mint is deliberately not tied to the settle: `claim` mints on arrival and settles only after the
    // decode, which is exactly what `admitted` below exists to order.
    let nextToken = 0
    // Ordering state for the peers that currently have a write in flight. `admitted` is the highest token
    // that has settled the peer's slot, by storing a value or by removing it, and `pending` counts the
    // writes still in flight. The stored entry cannot carry this alone: `remove`, `removeToken`, and the
    // `values` prune all delete it, and an empty slot would readmit a write that already lost. A record is
    // dropped once nothing is in flight for the peer, which is safe because no later arrival can then hold
    // a smaller token, and it bounds this map by concurrency rather than by peer count. Like `entries` it
    // is closure private, and it is only ever mutated inside one synchronous step, which is atomic.
    const claims = new Map<Identity.PeerId, { admitted: number; pending: number }>()

    // Claim the ordering token on arrival, before decoding. Claimed after the decode it would order
    // publications by decode completion, so a slow decode could overwrite a value that arrived later. The
    // record is handed back with the token: its own `pending` keeps it in the map for as long as this call
    // runs, so the steps below hold it directly rather than looking it up again and defending against a
    // record that cannot be missing.
    const claim = (peerId: Identity.PeerId) =>
      Effect.sync(() => {
        let claimed = claims.get(peerId)
        if (claimed === undefined) {
          claimed = { admitted: 0, pending: 0 }
          claims.set(peerId, claimed)
        }
        claimed.pending += 1
        return { claimed, token: ++nextToken }
      })

    const set = (peerId: Identity.PeerId, value: unknown) =>
      Effect.acquireUseRelease(claim(peerId), ({ claimed, token }) =>
        Effect.gen(function*() {
          const decoded = yield* Schema.decodeUnknownEffect(schema)(value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProtocolMismatch({
                  expected: "schema-valid presence payload",
                  observed: String(cause)
                })
              })
            )
          )
          const expiresAtMillis = (yield* Clock.currentTimeMillis) + timeToLiveMillis
          const supersededBy = yield* Ref.modify(entries, (current) => {
            if (claimed.admitted > token) {
              return [claimed.admitted, current] satisfies readonly [
                number,
                Map<Identity.PeerId, Entry<A> & { readonly token: number }>
              ]
            }
            current.set(peerId, { peerId, value: decoded, expiresAtMillis, identity: "transport-peer", token })
            claimed.admitted = token
            return [undefined, current] satisfies readonly [
              undefined,
              Map<Identity.PeerId, Entry<A> & { readonly token: number }>
            ]
          })
          if (supersededBy !== undefined) {
            yield* Effect.logDebug("presence write superseded by a newer publication").pipe(
              Effect.annotateLogs({ peerId, supersededBy, token })
            )
          }
          return token
        }), ({ claimed }) =>
        Effect.sync(() => {
          claimed.pending -= 1
          if (claimed.pending === 0) claims.delete(peerId)
        }))

    // The release of a scoped publication. It retires only the entry this publication created, and
    // deliberately does not claim a sequence: ending a publication says nothing about writes that arrived
    // from elsewhere for the same peer, so it must never suppress them. `remove` is the call that means
    // "this peer is gone" and it does claim one.
    const removeToken = (peerId: Identity.PeerId, token: number) =>
      Ref.update(entries, (current) => {
        if (current.get(peerId)?.token !== token) return current
        current.delete(peerId)
        return current
      })

    // Removal settles the peer's slot by emptying it, so a write still in flight that arrived earlier is
    // dropped instead of resurrecting the peer. The token is minted here and so outranks every token minted
    // before it. Only an existing claim is touched, so a removal with nothing in flight adds no state.
    const remove = (peerId: Identity.PeerId) =>
      Ref.update(entries, (current) => {
        current.delete(peerId)
        const token = ++nextToken
        const claimed = claims.get(peerId)
        if (claimed !== undefined) claimed.admitted = token
        return current
      })

    return {
      receive: (peerId, value) => set(peerId, value).pipe(Effect.asVoid),
      publish: (peerId, value) =>
        Effect.acquireRelease(set(peerId, value), (token) => removeToken(peerId, token)).pipe(
          Effect.asVoid
        ),
      remove,
      values: Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        return yield* Ref.modify(entries, (current) => {
          const active: Array<Entry<A>> = []
          for (const [peerId, entry] of current) {
            if (entry.expiresAtMillis <= now) {
              current.delete(peerId)
              continue
            }
            active.push({
              peerId: entry.peerId,
              value: entry.value,
              expiresAtMillis: entry.expiresAtMillis,
              identity: entry.identity
            })
          }
          return [active, current]
        })
      })
    } satisfies Presence<A>
  })
