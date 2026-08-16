import type * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import type * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Query from "@lucas-barake/effect-local/Query"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import type * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as replicaWire from "./replicaWire.js"
import { decodeWith, encodeJson, type Json } from "./wireCodec.js"

const durationMillis = (input: Duration.Input): Effect.Effect<number> => Effect.sync(() => Duration.toMillis(input))

const ownerLost = new ReplicaError.OwnerUnavailable({ reason: "transport" })

export type WireClient = RpcClient.RpcClient<
  RpcGroup.Rpcs<typeof replicaWire.ReplicaRpcs>,
  RpcClientError.RpcClientError
>

export interface ProxyOptions {
  readonly definition: Definition.Any
  readonly profileNames: ReadonlyMap<Ephemeral.AnyMember, string>
  readonly client: WireClient
  readonly makeHandle: Effect.Effect<string>
  readonly reconnectDelay: Duration.Input
}

export interface ProxyRegistrations {
  readonly knownSpaces: ReadonlySet<Identity.SpaceId>
  readonly reregister: Effect.Effect<void>
}

export interface ReplicaProxy {
  readonly replica: Replica.Service
  readonly queryReactivity: QueryReactivity.Service
  readonly ephemeral: EphemeralClient.Service
  readonly registrations: ProxyRegistrations
}

const mapTransport = <A, E extends { readonly _tag: string },>(
  effect: Effect.Effect<A, E | RpcClientError.RpcClientError>
): Effect.Effect<A, E | ReplicaError.OwnerUnavailable> =>
  effect.pipe(
    Effect.catchTag("RpcClientError", () => Effect.fail(ownerLost))
  )

const dieUnknownDefinition = <A, E extends { readonly _tag: string },>(
  effect: Effect.Effect<A, E | replicaWire.WireUnknownDefinition | replicaWire.WireUnknownSession>
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.catchTag("WireUnknownDefinition", (error) => Effect.die(error)),
    Effect.catchTag("WireUnknownSession", (error) => Effect.die(error))
  )

export const decodePending = Effect.fnUntraced(function*(
  definition: Definition.Any,
  pending: Protocol.PendingMutation
) {
  const mutation = definition.mutationByName.get(pending.envelope.name)
  if (mutation === undefined) {
    return yield* Effect.die(new Error(`Unknown mutation on the wire: ${pending.envelope.name}`))
  }
  const payload = yield* decodeWith(mutation.payloadSchema, pending.envelope.payload)
  const decoded: Replica.PendingMutation = {
    envelope: pending.envelope,
    changes: pending.changes,
    submissionState: pending.submissionState,
    attempts: pending.attempts,
    payload
  }
  return decoded
})

export const decodeReceipt = Effect.fnUntraced(function*(
  definition: Definition.Any,
  receipt: Protocol.Receipt
) {
  switch (receipt._tag) {
    case "Accepted": {
      const mutation = definition.mutationByName.get(receipt.name)
      if (mutation === undefined) {
        return yield* Effect.die(new Error(`Unknown mutation on the wire: ${receipt.name}`))
      }
      const result = yield* decodeWith(mutation.successSchema, receipt.result)
      const decoded: Replica.Receipt<Mutation.Any> = { ...receipt, result }
      return decoded
    }
    case "Rejected": {
      if (receipt.origin !== "Mutation") return receipt
      const mutation = definition.mutationByName.get(receipt.name)
      if (mutation === undefined) {
        return yield* Effect.die(new Error(`Unknown mutation on the wire: ${receipt.name}`))
      }
      const rejection = yield* decodeWith(mutation.rejectionSchema, receipt.rejection)
      const decoded: Replica.Receipt<Mutation.Any> = {
        ...receipt,
        origin: "Mutation",
        // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The rejection was decoded with this exact mutation's schema; Mutation.Any erases the rejection type.
        rejection: rejection as never
      }
      return decoded
    }
    default: {
      return receipt
    }
  }
})

export const decodeSettlement = Effect.fnUntraced(function*(
  definition: Definition.Any,
  wire: replicaWire.WireSettlement
) {
  const pending = yield* decodePending(definition, wire.pending)
  const receipt = yield* decodeReceipt(definition, wire.receipt)
  const settled: Replica.SettledMutation = {
    sequence: wire.sequence,
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Receipt variants were decoded against the mutation named in the envelope; the generic settlement type erases that link.
    settlement: { pending, receipt } as Replica.MutationSettlement
  }
  return settled
})

export const makeProxy = (options: ProxyOptions): ReplicaProxy => {
  const definition = options.definition
  const reconnectMillis = Duration.toMillis(options.reconnectDelay)
  const client = options.client
  const knownSpaces = new Set<Identity.SpaceId>()
  const retainCounts = new Map<string, number>()
  const lastAcks = new Map<Identity.SpaceId, number>()
  interface OpenSession {
    readonly request: {
      readonly handle: string
      readonly name: string
      readonly spaceId: Identity.SpaceId
      readonly member: Protocol.EphemeralMember
      readonly value: Json
      readonly ttlMillis: number
    }
  }
  const openSessions = new Map<string, OpenSession>()

  const rememberSpace = (spaceId: Identity.SpaceId): void => {
    knownSpaces.add(spaceId)
  }

  const settlementsStream = (
    spaceId: Identity.SpaceId,
    streamOptions: Replica.SettlementOptions | undefined,
    name: string | undefined
  ): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> => {
    let cursor: number | undefined
    const page = (from: Replica.SettlementStart | undefined) =>
      client.Settlements({ spaceId, from, name }).pipe(
        Stream.mapEffect((wire) => decodeSettlement(definition, wire)),
        Stream.tap((settled) =>
          Effect.sync(() => {
            cursor = settled.sequence
          })
        ),
        Stream.catchTag("WireUnknownDefinition", (error) => Stream.fromEffect(Effect.die(error)))
      )
    const resume = (): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> =>
      Stream.suspend(() => {
        if (cursor === undefined) return page(streamOptions?.from)
        return page(cursor)
      }).pipe(
        Stream.catchTag("RpcClientError", () =>
          Stream.fromEffect(Effect.sleep(reconnectMillis)).pipe(
            Stream.flatMap(() => resume())
          ))
      )
    return resume()
  }

  const makeSpace = (spaceId: Identity.SpaceId): Replica.Space => {
    rememberSpace(spaceId)
    const space: Replica.Space = {
      spaceId,
      scope: mapTransport(client.SpaceScope({ spaceId })),
      setScope: (scope) => mapTransport(client.SetScope({ spaceId, scope })),
      activation: mapTransport(client.Activation({ spaceId })),
      activate: mapTransport(client.Activate({ spaceId })),
      deactivate: mapTransport(client.Deactivate({ spaceId })),
      status: mapTransport(client.SpaceStatus({ spaceId })),
      mutate: (mutation, payload) =>
        encodeJson(mutation.payloadSchema, payload).pipe(
          Effect.flatMap((encoded) =>
            client.Mutate({ spaceId, name: mutation.name, payload: encoded }).pipe(
              mapTransport,
              dieUnknownDefinition,
              Effect.catchTag("WireMutationRejection", (wire) =>
                decodeWith(mutation.rejectionSchema, wire.rejection).pipe(
                  Effect.flatMap((rejection) =>
                    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The rejection was decoded with this exact mutation's schema; the generic mutate signature erases it.
                    Effect.fail(rejection as never)
                  )
                ))
            )
          )
        ),
      get: (model: Model.Any, key) =>
        encodeJson(model.key, key).pipe(
          Effect.flatMap((encoded) =>
            client.GetEntity({ spaceId, name: model.name, key: encoded }).pipe(mapTransport, dieUnknownDefinition)
          ),
          Effect.flatMap(Option.match({
            onNone: () => Effect.succeedNone,
            onSome: (value) =>
              decodeWith(model.schema, value).pipe(
                // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The value was decoded with this exact model's schema; Model.Any erases the value type.
                Effect.map((decoded) => Option.some(decoded as never))
              )
          }))
        ),
      query: (query: Query.Any, payload) =>
        encodeJson(query.payloadSchema, payload).pipe(
          Effect.flatMap((encoded) =>
            client.Query({ spaceId, name: query.name, payload: encoded }).pipe(
              mapTransport,
              dieUnknownDefinition,
              Effect.catchTag("WireQueryError", (wire) =>
                decodeWith(query.errorSchema, wire.error).pipe(
                  // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The error was decoded with this exact query's schema; the generic query signature erases it.
                  Effect.flatMap((error) => Effect.fail(error as never))
                ))
            )
          ),
          Effect.flatMap((result) => decodeWith(query.successSchema, result))
        ),
      receipt: (mutation, mutationId) =>
        client.ReceiptOf({ spaceId, name: mutation.name, mutationId }).pipe(
          mapTransport,
          dieUnknownDefinition,
          Effect.flatMap(Option.match({
            onNone: () => Effect.succeedNone,
            onSome: (receipt) =>
              decodeReceipt(definition, receipt).pipe(
                // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The receipt was decoded against this exact mutation's schemas; the generic receipt signature erases it.
                Effect.map((decoded) => Option.some(decoded as never))
              )
          }))
        ),
      pending: mapTransport(client.Pending({ spaceId })).pipe(
        Effect.flatMap(Effect.forEach((entry) => decodePending(definition, entry)))
      ),
      pendingFor: (mutation) =>
        client.PendingFor({ spaceId, name: mutation.name }).pipe(
          mapTransport,
          dieUnknownDefinition,
          Effect.flatMap(Effect.forEach((entry) =>
            decodePending(definition, entry).pipe(
              // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact mutation's schema; the generic pendingFor signature erases it.
              Effect.map((decoded) => decoded as never)
            )
          ))
        ),
      settlements: (streamOptions) => settlementsStream(spaceId, streamOptions, undefined),
      settlementsFor: (mutation, streamOptions) =>
        // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The host filters by this mutation's name and payloads decode with its schema; the generic signature erases that.
        settlementsStream(spaceId, streamOptions, mutation.name) as Stream.Stream<
          Replica.SettledMutation<never>,
          ReplicaError.ReplicaError
        >,
      acknowledgeSettlements: (sequence) =>
        mapTransport(client.AcknowledgeSettlements({ spaceId, sequence })).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              const previous = lastAcks.get(spaceId) ?? 0
              if (sequence > previous) lastAcks.set(spaceId, sequence)
            })
          )
        ),
      quarantine: mapTransport(client.QuarantineList({ spaceId })),
      discardQuarantined: (mutationId) => mapTransport(client.DiscardQuarantined({ spaceId, mutationId })),
      resubmitQuarantined: (mutationId, mutation, payload) =>
        encodeJson(mutation.payloadSchema, payload).pipe(
          Effect.flatMap((encoded) =>
            client.ResubmitQuarantined({ spaceId, mutationId, name: mutation.name, payload: encoded }).pipe(
              mapTransport,
              dieUnknownDefinition,
              Effect.catchTag("WireMutationRejection", (wire) =>
                decodeWith(mutation.rejectionSchema, wire.rejection).pipe(
                  Effect.flatMap((rejection) =>
                    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The rejection was decoded with this exact mutation's schema; the generic signature erases it.
                    Effect.fail(rejection as never)
                  )
                ))
            )
          )
        )
    }
    return space
  }

  const replica: Replica.Service = {
    join: (spaceId) => mapTransport(client.Join({ spaceId })).pipe(Effect.map(() => makeSpace(spaceId))),
    leave: (spaceId) => mapTransport(client.Leave({ spaceId })),
    spaces: mapTransport(client.Spaces({})).pipe(
      Effect.map((spaceIds) => spaceIds.map(makeSpace))
    ),
    space: (spaceId) =>
      mapTransport(client.SpaceScope({ spaceId })).pipe(
        Effect.as(makeSpace(spaceId))
      ),
    status: mapTransport(client.AggregateStatus({}))
  }

  const queryReactivity: QueryReactivity.Service = {
    retain: (key) =>
      Effect.suspend(() => {
        const count = retainCounts.get(key) ?? 0
        retainCounts.set(key, count + 1)
        const release = Effect.suspend(() => {
          const current = retainCounts.get(key) ?? 0
          if (current <= 1) {
            retainCounts.delete(key)
            return client.Release({ key }).pipe(Effect.ignore)
          }
          retainCounts.set(key, current - 1)
          return Effect.void
        })
        if (count === 0) {
          return client.Retain({ key }).pipe(Effect.ignore, Effect.as(release))
        }
        return Effect.succeed(release)
      }),
    record: () => Effect.void,
    affected: () => Effect.succeed([])
  }

  const sessionStreamsFor = (
    handle: string,
    profile: Ephemeral.AnyMember,
    request: OpenSession["request"]
  ): EphemeralClient.Session<Ephemeral.AnyMember> => {
    const reopen = (): Effect.Effect<void, ReplicaError.ReplicaError> =>
      client.EphemeralOpen(request).pipe(
        Effect.catchTag("WireUnknownDefinition", (error) => Effect.die(error)),
        Effect.catchTag("WireUnknownSession", (error) => Effect.die(error)),
        Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error)),
        Effect.catchTag("RpcClientError", () =>
          Effect.sleep(reconnectMillis).pipe(
            Effect.andThen(reopen())
          ))
      )
    const withReconnect = <A, E,>(
      make: () => Stream.Stream<
        A,
        | E
        | replicaWire.WireUnknownSession
        | RpcClientError.RpcClientError
      >
    ): Stream.Stream<A, E | ReplicaError.ReplicaError> =>
      make().pipe(
        Stream.catchTag("WireUnknownSession", () =>
          Stream.fromEffect(reopen()).pipe(Stream.flatMap(() => withReconnect(make)))),
        Stream.catchTag("RpcClientError", () =>
          Stream.fromEffect(
            Effect.sleep(reconnectMillis).pipe(Effect.andThen(reopen()))
          ).pipe(Stream.flatMap(() =>
            withReconnect(make)
          )))
      )

    return {
      spaceId: request.spaceId,
      member: request.member,
      events: (definitionArg) =>
        withReconnect(() =>
          client.EphemeralEvents({ handle, name: definitionArg.name }).pipe(
            Stream.mapEffect((frame) =>
              decodeWith(definitionArg.payloadSchema, frame.payload).pipe(
                // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact definition's schema; the generic events signature erases it.
                Effect.map((payload) => ({ member: frame.member, payload }) as never)
              )
            ),
            Stream.catchTag("WireUnknownDefinition", (error) => Stream.fromEffect(Effect.die(error))),
            Stream.catchTag("WireEphemeralEncodeError", (error) => Stream.fromEffect(Effect.die(error)))
          )
        ),
      state: (definitionArg) =>
        withReconnect(() =>
          client.EphemeralState({ handle, name: definitionArg.name }).pipe(
            Stream.catchTag("WireUnknownDefinition", (error) => Stream.fromEffect(Effect.die(error))),
            Stream.catchTag("WireEphemeralEncodeError", (error) => Stream.fromEffect(Effect.die(error))),
            Stream.mapEffect(Effect.forEach((frame) =>
              Effect.all({
                key: decodeWith(definitionArg.keySchema, frame.key),
                value: decodeWith(definitionArg.payloadSchema, frame.value)
              }).pipe(
                Effect.map(({ key, value }) =>
                  // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Key and value were decoded with this exact definition's schemas; the generic state signature erases them.
                  ({
                    member: frame.member,
                    key,
                    value,
                    expiresAtMillis: frame.expiresAtMillis
                  }) as never
                )
              )
            ))
          )
        ),
      members: withReconnect(() =>
        client.EphemeralMembers({ handle }).pipe(
          Stream.catchTag("WireUnknownDefinition", (error) => Stream.fromEffect(Effect.die(error))),
          Stream.catchTag("WireEphemeralEncodeError", (error) => Stream.fromEffect(Effect.die(error))),
          Stream.mapEffect(Effect.forEach((frame) =>
            decodeWith(profile.payloadSchema, frame.value).pipe(
              Effect.map((value) =>
                // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The value was decoded with this session's profile schema; AnyMember erases the value type.
                ({
                  member: frame.member,
                  value,
                  expiresAtMillis: frame.expiresAtMillis
                }) as never
              )
            )
          ))
        )
      ),
      updateMember: (value) =>
        encodeJson(profile.payloadSchema, value).pipe(
          Effect.flatMap((encoded) =>
            client.EphemeralUpdateMember({ handle, value: encoded }).pipe(mapTransport, dieUnknownDefinition)
          ),
          Effect.catchTag(
            "WireEphemeralEncodeError",
            (error) => Effect.die(error)
          )
        )
    }
  }

  const ephemeral: EphemeralClient.Service = {
    session: Effect.fnUntraced(function*(profile, sessionOptions) {
      const name = options.profileNames.get(profile)
      if (name === undefined) {
        return yield* Effect.fail(
          new ReplicaError.InvalidConfiguration({
            option: "profiles",
            message: "Ephemeral profile is not registered in the MultiTab profiles record"
          })
        )
      }
      const handle = yield* options.makeHandle
      const value = yield* encodeJson(profile.payloadSchema, sessionOptions.value)
      const ttlMillis = yield* durationMillis(sessionOptions.ttl)
      const request: OpenSession["request"] = {
        handle,
        name,
        spaceId: sessionOptions.spaceId,
        member: sessionOptions.member,
        value,
        ttlMillis
      }
      yield* client.EphemeralOpen(request).pipe(
        mapTransport,
        dieUnknownDefinition,
        Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
      )
      openSessions.set(handle, { request })
      yield* Effect.addFinalizer(() =>
        Effect.suspend(() => {
          openSessions.delete(handle)
          return client.EphemeralClose({ handle }).pipe(Effect.ignore)
        })
      )
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The session was opened for this exact profile; AnyMember erases the member payload type.
      return sessionStreamsFor(handle, profile, request) as never
    }),
    publish: (definitionArg: Ephemeral.Any, publishOptions: {
      readonly spaceId: Identity.SpaceId
      readonly member: Protocol.EphemeralMember
      readonly payload?: unknown
      readonly key?: unknown
      readonly ttl: Parameters<typeof durationMillis>[0]
    }) =>
      Effect.suspend(() => durationMillis(publishOptions.ttl)).pipe(Effect.flatMap((ttlMillis) => {
        if (definitionArg.kind === "event") {
          return encodeJson(definitionArg.payloadSchema, publishOptions.payload).pipe(
            Effect.flatMap((payload) =>
              client.EphemeralPublishEvent({
                name: definitionArg.name,
                spaceId: publishOptions.spaceId,
                member: publishOptions.member,
                payload,
                ttlMillis
              }).pipe(
                mapTransport,
                dieUnknownDefinition,
                Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
              )
            )
          )
        }
        return Effect.all({
          key: encodeJson(definitionArg.keySchema, publishOptions.key),
          payload: encodeJson(definitionArg.payloadSchema, publishOptions.payload)
        }).pipe(
          Effect.flatMap(({ key, payload }) =>
            client.EphemeralPublishState({
              name: definitionArg.name,
              spaceId: publishOptions.spaceId,
              member: publishOptions.member,
              key,
              payload,
              ttlMillis
            }).pipe(
              mapTransport,
              dieUnknownDefinition,
              Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
            )
          )
        )
      })),
    clear: (definitionArg, target) =>
      client.EphemeralClear({
        name: definitionArg.name,
        spaceId: target.spaceId,
        member: target.member
      }).pipe(
        mapTransport,
        dieUnknownDefinition,
        Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
      ),
    remove: (definitionArg, removeOptions) =>
      encodeJson(definitionArg.keySchema, removeOptions.key).pipe(
        Effect.flatMap((key) =>
          client.EphemeralRemove({
            name: definitionArg.name,
            spaceId: removeOptions.spaceId,
            member: removeOptions.member,
            key
          }).pipe(
            mapTransport,
            dieUnknownDefinition,
            Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
          )
        )
      )
  }

  const reregister = Effect.gen(function*() {
    yield* Effect.forEach(
      retainCounts.keys(),
      (key) => client.Retain({ key }).pipe(Effect.ignore),
      { discard: true }
    )
    yield* Effect.forEach(
      openSessions.values(),
      (session) => client.EphemeralOpen(session.request).pipe(Effect.ignore),
      { discard: true }
    )
    yield* Effect.forEach(
      lastAcks.entries(),
      ([spaceId, sequence]) => client.AcknowledgeSettlements({ spaceId, sequence }).pipe(Effect.ignore),
      { discard: true }
    )
  })

  return {
    replica,
    queryReactivity,
    ephemeral,
    registrations: {
      knownSpaces,
      reregister
    }
  }
}
