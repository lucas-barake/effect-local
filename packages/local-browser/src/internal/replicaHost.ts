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
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type * as broadcastRpc from "./broadcastRpc.js"
import type * as Wire from "./multiTabWire.js"
import * as replicaWire from "./replicaWire.js"
import { decodeWith, encodeJson } from "./wireCodec.js"

const isReplicaError = Schema.is(ReplicaError.ReplicaError)

export interface HostOptions {
  readonly definition: Definition.Any
  readonly ephemerals: ReadonlyArray<Ephemeral.Any>
  readonly profiles: ReadonlyMap<string, Ephemeral.AnyMember>
  readonly replica: Replica.Service
  readonly queryReactivity: QueryReactivity.Service
  readonly ephemeral: EphemeralClient.Service
  readonly server: broadcastRpc.ServerProtocol
  readonly disconnectGrace: Duration.Input
}

interface EphemeralSessionEntry {
  readonly tabId: Wire.TabId
  readonly profileName: string
  readonly profile: Ephemeral.AnyMember
  readonly session: EphemeralClient.Session<Ephemeral.AnyMember>
  readonly close: Effect.Effect<void>
}

export const encodeReceipt = Effect.fnUntraced(function*(
  definition: Definition.Any,
  receipt: Replica.Receipt<Mutation.Any>
) {
  switch (receipt._tag) {
    case "Accepted": {
      const mutation = definition.mutationByName.get(receipt.name)
      if (mutation === undefined) {
        return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "mutation", name: receipt.name }))
      }
      const result = yield* encodeJson(mutation.successSchema, receipt.result)
      const encoded: Protocol.Receipt = { ...receipt, result }
      return encoded
    }
    case "Rejected": {
      if (receipt.origin !== "Mutation") return receipt
      const mutation = definition.mutationByName.get(receipt.name)
      if (mutation === undefined) {
        return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "mutation", name: receipt.name }))
      }
      const rejection = yield* encodeJson(mutation.rejectionSchema, receipt.rejection)
      const encoded: Protocol.Receipt = { ...receipt, rejection }
      return encoded
    }
    default: {
      return receipt
    }
  }
})

export const encodeSettlement = Effect.fnUntraced(function*(
  definition: Definition.Any,
  settled: Replica.SettledMutation
) {
  const pending = settled.settlement.pending
  const wirePending: Protocol.PendingMutation = {
    envelope: pending.envelope,
    optimisticResult: null,
    changes: pending.changes,
    submissionState: pending.submissionState,
    attempts: pending.attempts
  }
  const wireReceipt = yield* encodeReceipt(definition, settled.settlement.receipt)
  const settlement: replicaWire.WireSettlement = {
    sequence: settled.sequence,
    pending: wirePending,
    receipt: wireReceipt
  }
  return settlement
})

export const makeHost = Effect.fn("localBrowser.replicaHost")(function*(options: HostOptions) {
  const definition = options.definition
  const graceMillis = Duration.toMillis(options.disconnectGrace)
  const ephemeralByName = new Map<string, Ephemeral.Any>()
  for (const entry of options.ephemerals) {
    ephemeralByName.set(entry.name, entry)
  }

  const retains = new Map<Wire.TabId, Map<string, Effect.Effect<void>>>()
  const sessions = new Map<string, EphemeralSessionEntry>()
  const acks = new Map<Identity.SpaceId, Map<string, number>>()
  const applied = new Map<Identity.SpaceId, number>()

  yield* Effect.addFinalizer(
    Effect.fnUntraced(function*() {
      const retainReleases = Array.from(retains.values()).flatMap((entries) => Array.from(entries.values()))
      const sessionCloses = Array.from(sessions.values(), (entry) => entry.close)
      retains.clear()
      sessions.clear()
      acks.clear()
      applied.clear()
      const exits = yield* Effect.forEach(
        [...retainReleases, ...sessionCloses],
        (release) => Effect.exit(release)
      )
      const failure = exits.find((exit) => exit._tag === "Failure")
      if (failure !== undefined && failure._tag === "Failure") {
        yield* Effect.failCause(failure.cause)
      }
    })
  )

  const mutationFor = (name: string) =>
    Effect.suspend(() => {
      const mutation = definition.mutationByName.get(name)
      if (mutation === undefined) {
        return Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "mutation", name }))
      }
      return Effect.succeed(mutation)
    })

  const queryFor = (name: string) =>
    Effect.suspend(() => {
      const query = definition.queryByName.get(name)
      if (query === undefined) {
        return Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "query", name }))
      }
      return Effect.succeed(query)
    })

  const modelFor = (name: string) =>
    Effect.suspend(() => {
      const model = definition.modelByName.get(name)
      if (model === undefined) {
        return Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "model", name }))
      }
      return Effect.succeed(model)
    })

  const ephemeralFor = (name: string) =>
    Effect.suspend(() => {
      const entry = ephemeralByName.get(name)
      if (entry === undefined) {
        return Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name }))
      }
      return Effect.succeed(entry)
    })

  const profileFor = (name: string) =>
    Effect.suspend(() => {
      const profile = options.profiles.get(name)
      if (profile === undefined) {
        return Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name }))
      }
      return Effect.succeed(profile)
    })

  const tabFor = (clientId: number): Wire.TabId | undefined => options.server.tabIdOf(clientId)

  const sessionFor = (handle: string, clientId: number) =>
    Effect.suspend(() => {
      const entry = sessions.get(handle)
      const tabId = tabFor(clientId)
      if (entry === undefined || tabId === undefined || entry.tabId !== tabId) {
        return Effect.fail(new replicaWire.WireUnknownSession({ handle }))
      }
      return Effect.succeed(entry)
    })

  const applyAck = Effect.fnUntraced(function*(spaceId: Identity.SpaceId) {
    const perSpace = acks.get(spaceId)
    if (perSpace === undefined || perSpace.size === 0) return
    let minimum = Number.POSITIVE_INFINITY
    for (const value of perSpace.values()) {
      if (value < minimum) minimum = value
    }
    const current = applied.get(spaceId) ?? 0
    if (minimum <= current) return
    const space = yield* options.replica.space(spaceId)
    yield* space.acknowledgeSettlements(minimum)
    applied.set(spaceId, minimum)
  })

  const ackFrom = Effect.fnUntraced(function*(
    source: string,
    spaceId: Identity.SpaceId,
    sequence: number
  ) {
    let perSpace = acks.get(spaceId)
    if (perSpace === undefined) {
      perSpace = new Map()
      acks.set(spaceId, perSpace)
    }
    const previous = perSpace.get(source) ?? 0
    if (sequence > previous) perSpace.set(source, sequence)
    yield* applyAck(spaceId)
  })

  const cleanupTab = Effect.fnUntraced(function*(tabId: Wire.TabId) {
    const tabRetains = retains.get(tabId)
    if (tabRetains !== undefined) {
      retains.delete(tabId)
      yield* Effect.forEach(tabRetains.values(), (release) => release, { discard: true })
    }
    const closing: Array<Effect.Effect<void>> = []
    for (const [handle, entry] of sessions) {
      if (entry.tabId === tabId) {
        sessions.delete(handle)
        closing.push(entry.close)
      }
    }
    yield* Effect.forEach(closing, (close) => close, { discard: true })
    const touched: Array<Identity.SpaceId> = []
    for (const [spaceId, perSpace] of acks) {
      if (perSpace.delete(tabId)) touched.push(spaceId)
    }
    yield* Effect.forEach(touched, (spaceId) => applyAck(spaceId), { discard: true })
  })

  yield* Queue.take(options.server.tabDisconnects).pipe(
    Effect.flatMap((tabId) =>
      Effect.sleep(graceMillis).pipe(
        Effect.andThen(Effect.suspend(() => {
          if (options.server.isConnected(tabId)) return Effect.void
          return cleanupTab(tabId)
        })),
        Effect.forkScoped
      )
    ),
    Effect.forever,
    Effect.forkScoped
  )

  const spaceFor = (spaceId: Identity.SpaceId) => options.replica.space(spaceId)

  const failMutation = (
    mutation: Mutation.Any,
    error: unknown
  ): Effect.Effect<
    never,
    ReplicaError.ReplicaError | replicaWire.WireMutationRejection
  > => {
    if (isReplicaError(error)) return Effect.fail(error)
    return encodeJson(mutation.rejectionSchema, error).pipe(
      Effect.flatMap((rejection) =>
        Effect.fail(new replicaWire.WireMutationRejection({ name: mutation.name, rejection }))
      )
    )
  }

  const failQuery = (
    query: Query.Any,
    error: unknown
  ): Effect.Effect<never, ReplicaError.ReplicaError | replicaWire.WireQueryError> => {
    if (isReplicaError(error)) return Effect.fail(error)
    return encodeJson(query.errorSchema, error).pipe(
      Effect.flatMap((encoded) => Effect.fail(new replicaWire.WireQueryError({ name: query.name, error: encoded })))
    )
  }

  const layerHandlers = replicaWire.ReplicaRpcs.toLayer(Effect.succeed({
    Join: (request: { readonly spaceId: Identity.SpaceId }) =>
      options.replica.join(request.spaceId).pipe(Effect.asVoid),
    Leave: (request: { readonly spaceId: Identity.SpaceId }) => options.replica.leave(request.spaceId),
    Spaces: () =>
      options.replica.spaces.pipe(
        Effect.map((spaces) => spaces.map((space) => space.spaceId))
      ),
    AggregateStatus: () => options.replica.status,
    SpaceScope: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.scope)),
    SetScope: (request: { readonly spaceId: Identity.SpaceId; readonly scope: Protocol.ReplicationScope }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.setScope(request.scope))),
    Activation: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.activation)),
    Activate: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.activate)),
    Deactivate: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.deactivate)),
    SpaceStatus: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.status)),
    Mutate: Effect.fnUntraced(
      function*(request: {
        readonly spaceId: Identity.SpaceId
        readonly name: string
        readonly payload: unknown
      }) {
        const mutation = yield* mutationFor(request.name)
        const payload = yield* decodeWith(mutation.payloadSchema, request.payload)
        const space = yield* spaceFor(request.spaceId)
        // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact mutation's schema; Mutation.Any erases the payload type.
        return yield* space.mutate(mutation, payload as never).pipe(
          Effect.catch((error) => failMutation(mutation, error))
        )
      }
    ),
    GetEntity: (request: {
      readonly spaceId: Identity.SpaceId
      readonly name: string
      readonly key: unknown
    }) =>
      modelFor(request.name).pipe(
        Effect.flatMap((model: Model.Any) =>
          decodeWith(model.key, request.key).pipe(
            Effect.flatMap((key) =>
              spaceFor(request.spaceId).pipe(
                Effect.flatMap((space) => space.get(model, key)),
                Effect.flatMap(Option.match({
                  onNone: () => Effect.succeedNone,
                  onSome: (value) => encodeJson(model.schema, value).pipe(Effect.map(Option.some))
                }))
              )
            )
          )
        )
      ),
    Query: Effect.fnUntraced(
      function*(request: {
        readonly spaceId: Identity.SpaceId
        readonly name: string
        readonly payload: unknown
      }) {
        const query = yield* queryFor(request.name)
        const payload = yield* decodeWith(query.payloadSchema, request.payload)
        const space = yield* spaceFor(request.spaceId)
        // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact query's schema; Query.Any erases the payload type.
        const result = yield* space.query(query, payload as never).pipe(
          Effect.catch((error) => failQuery(query, error))
        )
        return yield* encodeJson(query.successSchema, result)
      }
    ),
    ReceiptOf: (request: {
      readonly spaceId: Identity.SpaceId
      readonly name: string
      readonly mutationId: Identity.MutationId
    }) =>
      mutationFor(request.name).pipe(
        Effect.flatMap((mutation) =>
          spaceFor(request.spaceId).pipe(
            Effect.flatMap((space) => space.receipt(mutation, request.mutationId)),
            Effect.flatMap(Option.match({
              onNone: () => Effect.succeedNone,
              onSome: (receipt) => encodeReceipt(definition, receipt).pipe(Effect.map(Option.some))
            }))
          )
        )
      ),
    Pending: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(
        Effect.flatMap((space) => space.pending),
        Effect.map((pending) =>
          pending.map((entry): Protocol.PendingMutation => ({
            envelope: entry.envelope,
            optimisticResult: null,
            changes: entry.changes,
            submissionState: entry.submissionState,
            attempts: entry.attempts
          }))
        )
      ),
    PendingFor: (request: { readonly spaceId: Identity.SpaceId; readonly name: string }) =>
      mutationFor(request.name).pipe(
        Effect.flatMap((mutation) =>
          spaceFor(request.spaceId).pipe(
            Effect.flatMap((space) => space.pendingFor(mutation)),
            Effect.map((pending) =>
              pending.map((entry): Protocol.PendingMutation => ({
                envelope: entry.envelope,
                optimisticResult: null,
                changes: entry.changes,
                submissionState: entry.submissionState,
                attempts: entry.attempts
              }))
            )
          )
        )
      ),
    Settlements: (request: {
      readonly spaceId: Identity.SpaceId
      readonly from?: Replica.SettlementStart | undefined
      readonly name?: string | undefined
    }) => {
      const settlementOptions: Replica.SettlementOptions = { from: request.from }
      const name = request.name
      if (name === undefined) {
        return Stream.unwrap(
          spaceFor(request.spaceId).pipe(
            Effect.map((space) => space.settlements(settlementOptions))
          )
        ).pipe(
          Stream.mapEffect((settled) => encodeSettlement(definition, settled))
        )
      }
      return Stream.unwrap(
        Effect.all([mutationFor(name), spaceFor(request.spaceId)]).pipe(
          Effect.map(([mutation, space]) => space.settlementsFor(mutation, settlementOptions))
        )
      ).pipe(
        Stream.mapEffect((settled) => encodeSettlement(definition, settled))
      )
    },
    AcknowledgeSettlements: (
      request: { readonly spaceId: Identity.SpaceId; readonly sequence: number },
      context: { readonly client: { readonly id: number } }
    ) =>
      Effect.suspend(() => {
        const tabId = tabFor(context.client.id)
        if (tabId === undefined) return Effect.void
        return ackFrom(tabId, request.spaceId, request.sequence)
      }),
    QuarantineList: (request: { readonly spaceId: Identity.SpaceId }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.quarantine)),
    DiscardQuarantined: (request: {
      readonly spaceId: Identity.SpaceId
      readonly mutationId: Identity.MutationId
    }) =>
      spaceFor(request.spaceId).pipe(
        Effect.flatMap((space) => space.discardQuarantined(request.mutationId))
      ),
    ResubmitQuarantined: Effect.fnUntraced(
      function*(request: {
        readonly spaceId: Identity.SpaceId
        readonly mutationId: Identity.MutationId
        readonly name: string
        readonly payload: unknown
      }) {
        const mutation = yield* mutationFor(request.name)
        const payload = yield* decodeWith(mutation.payloadSchema, request.payload)
        const space = yield* spaceFor(request.spaceId)
        // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact mutation's schema; Mutation.Any erases the payload type.
        return yield* space.resubmitQuarantined(request.mutationId, mutation, payload as never).pipe(
          Effect.catch((error) => failMutation(mutation, error))
        )
      }
    ),
    Retain: (
      request: { readonly key: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      Effect.suspend(() => {
        const tabId = tabFor(context.client.id)
        if (tabId === undefined) return Effect.void
        let tabRetains = retains.get(tabId)
        if (tabRetains === undefined) {
          tabRetains = new Map()
          retains.set(tabId, tabRetains)
        }
        if (tabRetains.has(request.key)) return Effect.void
        const target = tabRetains
        return options.queryReactivity.retain(request.key).pipe(
          Effect.map((release) => {
            target.set(request.key, release)
          })
        )
      }),
    Release: (
      request: { readonly key: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      Effect.suspend(() => {
        const tabId = tabFor(context.client.id)
        if (tabId === undefined) return Effect.void
        const tabRetains = retains.get(tabId)
        const release = tabRetains?.get(request.key)
        if (tabRetains === undefined || release === undefined) return Effect.void
        tabRetains.delete(request.key)
        return release
      }),
    EphemeralOpen: Effect.fnUntraced(
      function*(
        request: {
          readonly handle: string
          readonly name: string
          readonly spaceId: Identity.SpaceId
          readonly member: Protocol.EphemeralMember
          readonly value: unknown
          readonly ttlMillis: number
        },
        context: { readonly client: { readonly id: number } }
      ) {
        if (sessions.has(request.handle)) return
        const tabId = tabFor(context.client.id)
        if (tabId === undefined) return
        const profile = yield* profileFor(request.name)
        const value = yield* decodeWith(profile.payloadSchema, request.value)
        const scope = yield* Scope.make()
        const session = yield* options.ephemeral.session(profile, {
          spaceId: request.spaceId,
          member: request.member,
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The profile's payload type is erased by AnyMember; the value was decoded with this exact profile's schema.
          value: value as never,
          ttl: request.ttlMillis
        }).pipe(
          Scope.provide(scope),
          Effect.catchTag(
            "EphemeralEncodeError",
            () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
          ),
          Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause)))
        )
        sessions.set(request.handle, {
          tabId,
          profileName: request.name,
          profile,
          session,
          close: Scope.close(scope, Exit.void)
        })
      }
    ),
    EphemeralClose: (
      request: { readonly handle: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      sessionFor(request.handle, context.client.id).pipe(
        Effect.flatMap((entry) => {
          sessions.delete(request.handle)
          return entry.close
        })
      ),
    EphemeralUpdateMember: (
      request: { readonly handle: string; readonly value: unknown },
      context: { readonly client: { readonly id: number } }
    ) =>
      sessionFor(request.handle, context.client.id).pipe(
        Effect.flatMap((entry) =>
          decodeWith(entry.profile.payloadSchema, request.value).pipe(
            Effect.flatMap((value) =>
              // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The profile's payload type is erased by AnyMember; the value was decoded with this exact profile's schema.
              entry.session.updateMember(value as never).pipe(
                Effect.catchTag(
                  "EphemeralEncodeError",
                  () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: entry.profileName }))
                )
              )
            )
          )
        )
      ),
    EphemeralEvents: (
      request: { readonly handle: string; readonly name: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      Stream.unwrap(
        Effect.all([sessionFor(request.handle, context.client.id), ephemeralFor(request.name)]).pipe(
          Effect.map(([entry, event]) => {
            if (event.kind !== "event") {
              return Stream.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
            }
            return entry.session.events(event).pipe(
              Stream.mapEffect((envelope) =>
                encodeJson(event.payloadSchema, envelope.payload).pipe(
                  Effect.map((payload) => ({ member: envelope.member, payload }))
                )
              ),
              Stream.catchTag(
                "EphemeralDecodeError",
                () => Stream.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
              )
            )
          })
        )
      ),
    EphemeralState: (
      request: { readonly handle: string; readonly name: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      Stream.unwrap(
        Effect.all([sessionFor(request.handle, context.client.id), ephemeralFor(request.name)]).pipe(
          Effect.map(([entry, state]) => {
            if (state.kind !== "state") {
              return Stream.fail(
                new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })
              )
            }
            return entry.session.state(state).pipe(
              Stream.mapEffect(Effect.forEach((item) =>
                Effect.all({
                  key: encodeJson(state.keySchema, item.key),
                  value: encodeJson(state.payloadSchema, item.value)
                }).pipe(
                  Effect.map(({ key, value }) => ({
                    member: item.member,
                    key,
                    value,
                    expiresAtMillis: item.expiresAtMillis
                  }))
                )
              )),
              Stream.catchTag("EphemeralDecodeError", () =>
                Stream.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })))
            )
          })
        )
      ),
    EphemeralMembers: (
      request: { readonly handle: string },
      context: { readonly client: { readonly id: number } }
    ) =>
      Stream.unwrap(
        sessionFor(request.handle, context.client.id).pipe(
          Effect.map((entry) =>
            entry.session.members.pipe(
              Stream.mapEffect(Effect.forEach((item) =>
                encodeJson(entry.profile.payloadSchema, item.value).pipe(
                  Effect.map((value) => ({
                    member: item.member,
                    value,
                    expiresAtMillis: item.expiresAtMillis
                  }))
                )
              )),
              Stream.catchTag("EphemeralDecodeError", () =>
                Stream.fail(
                  new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: entry.profileName })
                ))
            )
          )
        )
      ),
    EphemeralPublishEvent: Effect.fnUntraced(
      function*(request: {
        readonly name: string
        readonly spaceId: Identity.SpaceId
        readonly member: Protocol.EphemeralMember
        readonly payload: unknown
        readonly ttlMillis: number
      }) {
        const event = yield* ephemeralFor(request.name)
        if (event.kind !== "event") {
          return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
        }
        const payload = yield* decodeWith(event.payloadSchema, request.payload)
        return yield* options.ephemeral.publish(event, {
          spaceId: request.spaceId,
          member: request.member,
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact definition's schema; AnyEvent erases the payload type.
          payload: payload as never,
          ttl: request.ttlMillis
        }).pipe(
          Effect.catchTag(
            "EphemeralEncodeError",
            () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
          )
        )
      }
    ),
    EphemeralPublishState: Effect.fnUntraced(
      function*(request: {
        readonly name: string
        readonly spaceId: Identity.SpaceId
        readonly member: Protocol.EphemeralMember
        readonly key: unknown
        readonly payload: unknown
        readonly ttlMillis: number
      }) {
        const state = yield* ephemeralFor(request.name)
        if (state.kind !== "state") {
          return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
        }
        const key = yield* decodeWith(state.keySchema, request.key)
        const payload = yield* decodeWith(state.payloadSchema, request.payload)
        return yield* options.ephemeral.publish(state, {
          spaceId: request.spaceId,
          member: request.member,
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The key was decoded with this exact definition's key schema; AnyState erases the key type.
          key: key as never,
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The payload was decoded with this exact definition's schema; AnyState erases the payload type.
          payload: payload as never,
          ttl: request.ttlMillis
        }).pipe(
          Effect.catchTag(
            "EphemeralEncodeError",
            () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
          )
        )
      }
    ),
    EphemeralClear: Effect.fnUntraced(
      function*(request: {
        readonly name: string
        readonly spaceId: Identity.SpaceId
        readonly member: Protocol.EphemeralMember
      }) {
        const event = yield* ephemeralFor(request.name)
        if (event.kind !== "event") {
          return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
        }
        return yield* options.ephemeral.clear(event, { spaceId: request.spaceId, member: request.member })
      }
    ),
    EphemeralRemove: Effect.fnUntraced(
      function*(request: {
        readonly name: string
        readonly spaceId: Identity.SpaceId
        readonly member: Protocol.EphemeralMember
        readonly key: unknown
      }) {
        const state = yield* ephemeralFor(request.name)
        if (state.kind !== "state") {
          return yield* Effect.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name }))
        }
        const key = yield* decodeWith(state.keySchema, request.key)
        return yield* options.ephemeral.remove(state, {
          spaceId: request.spaceId,
          member: request.member,
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The key was decoded with this exact definition's key schema; AnyState erases the key type.
          key: key as never
        }).pipe(
          Effect.catchTag(
            "EphemeralEncodeError",
            () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
          )
        )
      }
    )
  }))

  return {
    layerHandlers,
    acknowledgeLocal: (spaceId: Identity.SpaceId, sequence: number) => ackFrom("$local", spaceId, sequence)
  }
})
