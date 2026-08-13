import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"

export interface Entry<A,> {
  readonly clientId: Identity.ClientId
  readonly value: A
  readonly expiresAtMillis: number
}

export interface Presence<A,> {
  readonly receive: (clientId: Identity.ClientId, value: unknown) => Effect.Effect<void, ReplicaError.ProtocolInvalid>
  readonly publish: (
    clientId: Identity.ClientId,
    value: unknown
  ) => Effect.Effect<void, ReplicaError.ProtocolInvalid, Scope.Scope>
  readonly remove: (clientId: Identity.ClientId) => Effect.Effect<void>
  readonly values: Effect.Effect<ReadonlyArray<Entry<A>>>
}

interface StoredEntry<A,> extends Entry<A> {
  readonly token: number
}

interface ClientState<A,> {
  readonly barrier: number
  readonly inFlight: ReadonlySet<number>
  readonly entry: StoredEntry<A> | undefined
}

interface State<A,> {
  readonly nextToken: number
  readonly clients: Map<Identity.ClientId, ClientState<A>>
}

export const make = <A,>(schema: Schema.Decoder<A>, options: { readonly timeToLive: Duration.Input }) =>
  Effect.gen(function*() {
    const duration = Duration.fromInput(options.timeToLive)
    if (duration._tag === "None") {
      return yield* new ReplicaError.ProtocolInvalid({ message: "Presence timeToLive is invalid" })
    }
    const timeToLiveMillis = Duration.toMillis(duration.value)
    if (!Number.isFinite(timeToLiveMillis) || timeToLiveMillis <= 0) {
      return yield* new ReplicaError.ProtocolInvalid({ message: "Presence timeToLive must be positive and finite" })
    }
    // The map never escapes and is mutated only inside synchronous Ref callbacks. Each callback is one
    // atomic state transition without copying every resident for an unrelated client's update.
    const state = yield* Ref.make<State<A>>({ nextToken: 0, clients: new Map() })
    const removeInFlight = (client: ClientState<A>, token: number) => {
      if (!client.inFlight.has(token)) return client
      const inFlight = new Set(client.inFlight)
      inFlight.delete(token)
      return { ...client, inFlight }
    }
    const updateClient = (
      current: State<A>,
      clientId: Identity.ClientId,
      update: (client: ClientState<A>) => ClientState<A>
    ): State<A> => {
      const client = update(current.clients.get(clientId) ?? { barrier: 0, inFlight: new Set(), entry: undefined })
      if (client.entry === undefined && client.inFlight.size === 0) current.clients.delete(clientId)
      else current.clients.set(clientId, client)
      return current
    }
    const begin = (clientId: Identity.ClientId) =>
      Ref.modify(state, (current) => {
        const token = current.nextToken + 1
        const next = updateClient({ ...current, nextToken: token }, clientId, (client) => ({
          ...client,
          inFlight: new Set(client.inFlight).add(token)
        }))
        return [token, next]
      })
    const finish = (clientId: Identity.ClientId, token: number) =>
      Ref.update(
        state,
        (current) => updateClient(current, clientId, (client) => removeInFlight(client, token))
      )
    const set = (clientId: Identity.ClientId, value: unknown) =>
      Effect.gen(function*() {
        const token = yield* begin(clientId)
        return yield* Effect.gen(function*() {
          const decoded = yield* Schema.decodeUnknownEffect(schema)(value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ProtocolInvalid({
                message: "Presence payload failed Schema decoding",
                cause
              })
            )
          )
          const expiresAtMillis = (yield* Clock.currentTimeMillis) + timeToLiveMillis
          yield* Ref.update(state, (current) =>
            updateClient(current, clientId, (client) => {
              const settled = removeInFlight(client, token)
              if (token < settled.barrier) return settled
              return {
                ...settled,
                barrier: token,
                entry: { clientId, value: decoded, expiresAtMillis, token }
              }
            }))
          return token
        }).pipe(Effect.ensuring(finish(clientId, token)))
      })
    const removeToken = (clientId: Identity.ClientId, token: number) =>
      Ref.update(
        state,
        (current) =>
          updateClient(current, clientId, (client) => {
            if (client.entry?.token === token) return { ...client, entry: undefined }
            return client
          })
      )
    return {
      receive: (clientId, value) => set(clientId, value).pipe(Effect.asVoid),
      publish: (clientId, value) =>
        set(clientId, value).pipe(
          (acquire) => Effect.acquireRelease(acquire, (token) => removeToken(clientId, token)),
          Effect.asVoid
        ),
      remove: (clientId) =>
        Ref.update(state, (current) => {
          const token = current.nextToken + 1
          return updateClient({ ...current, nextToken: token }, clientId, (client) => ({
            ...client,
            barrier: token,
            entry: undefined
          }))
        }),
      values: Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        return yield* Ref.modify(state, (current) => {
          const active: Array<Entry<A>> = []
          for (const [clientId, client] of current.clients) {
            const entry = client.entry
            if (entry === undefined) continue
            if (entry.expiresAtMillis <= now) {
              if (client.inFlight.size === 0) current.clients.delete(clientId)
              else current.clients.set(clientId, { ...client, entry: undefined })
            } else {
              active.push({ clientId, value: entry.value, expiresAtMillis: entry.expiresAtMillis })
            }
          }
          return [active, current]
        })
      })
    } satisfies Presence<A>
  })
