import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
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

export const make = <A,>(schema: Schema.Decoder<A>, options: { readonly ttlMillis: number }) =>
  Effect.gen(function*() {
    if (!Number.isSafeInteger(options.ttlMillis) || options.ttlMillis <= 0) {
      return yield* new ReplicaError.ReplicaError({
        reason: {
          _tag: "ProtocolMismatch",
          expected: "schema-valid presence payload",
          observed: "ttlMillis must be a positive safe integer"
        }
      })
    }
    const entries = yield* Ref.make(new Map<Identity.PeerId, Entry<A> & { readonly token: number }>())
    const tokens = yield* Ref.make(0)

    const set = (peerId: Identity.PeerId, value: unknown) =>
      Effect.gen(function*() {
        const decoded = yield* Schema.decodeUnknownEffect(schema)(value).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.ReplicaError({
              reason: {
                _tag: "ProtocolMismatch",
                expected: "schema-valid presence payload",
                observed: String(cause)
              }
            })
          )
        )
        const token = yield* Ref.updateAndGet(tokens, (current) => current + 1)
        const expiresAtMillis = (yield* Clock.currentTimeMillis) + options.ttlMillis
        yield* Ref.update(entries, (current) => {
          const next = new Map(current)
          next.set(peerId, { peerId, value: decoded, expiresAtMillis, identity: "transport-peer", token })
          return next
        })
        return token
      })

    const removeToken = (peerId: Identity.PeerId, token: number) =>
      Ref.update(entries, (current) => {
        if (current.get(peerId)?.token !== token) return current
        const next = new Map(current)
        next.delete(peerId)
        return next
      })

    const remove = (peerId: Identity.PeerId) =>
      Ref.update(entries, (current) => {
        if (!current.has(peerId)) return current
        const next = new Map(current)
        next.delete(peerId)
        return next
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
          const next = new Map<Identity.PeerId, Entry<A> & { readonly token: number }>()
          for (const [peerId, entry] of current) {
            if (entry.expiresAtMillis <= now) continue
            next.set(peerId, entry)
            active.push({
              peerId: entry.peerId,
              value: entry.value,
              expiresAtMillis: entry.expiresAtMillis,
              identity: entry.identity
            })
          }
          return [active, next]
        })
      })
    } satisfies Presence<A>
  })
