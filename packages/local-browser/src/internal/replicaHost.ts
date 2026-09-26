import type * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import type * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Query from "@lucas-barake/effect-local/Query"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type * as InvalidationHub from "./invalidationHub.js"
import * as replicaWire from "./replicaWire.js"
import { decodeWith, encodeJson } from "./wireCodec.js"

const isReplicaError = Schema.is(ReplicaError.ReplicaError)

export interface OwnerResources {
  readonly session: string
  readonly replica: Replica.Service
  readonly queryReactivity: QueryReactivity.Service
  readonly ephemeral: EphemeralClient.Service
  readonly invalidations: InvalidationHub.InvalidationHub
}

export interface HostOptions {
  readonly definition: Definition.Any
  readonly ephemerals: ReadonlyArray<Ephemeral.Any>
  readonly profiles: ReadonlyMap<string, Ephemeral.AnyMember>
  readonly resources: OwnerResources
}

interface EphemeralSessionEntry {
  readonly profileName: string
  readonly profile: Ephemeral.AnyMember
  readonly session: EphemeralClient.Session<Ephemeral.AnyMember>
}

interface SettlementConsumer {
  streams: number
  sequence: number
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

export const makeHandlers = Effect.fn("localBrowser.replicaHost")(function*(options: HostOptions) {
  const definition = options.definition
  const resources = options.resources
  const ephemeralByName = new Map<string, Ephemeral.Any>()
  for (const entry of options.ephemerals) {
    ephemeralByName.set(entry.name, entry)
  }

  const sessions = new Map<string, EphemeralSessionEntry>()
  const consumers = new Map<Identity.SpaceId, Map<string, SettlementConsumer>>()
  const applied = new Map<Identity.SpaceId, number>()
  let nextHandle = 0

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

  const sessionFor = (handle: string) =>
    Effect.suspend(() => {
      const entry = sessions.get(handle)
      if (entry === undefined) return Effect.fail(new replicaWire.WireUnknownSession({ handle }))
      return Effect.succeed(entry)
    })

  const applyAck = Effect.fnUntraced(function*(spaceId: Identity.SpaceId, floor: number) {
    let minimum = floor
    for (const consumer of consumers.get(spaceId)?.values() ?? []) {
      if (consumer.sequence < minimum) minimum = consumer.sequence
    }
    const current = applied.get(spaceId) ?? 0
    if (!Number.isFinite(minimum) || minimum <= current) return
    const space = yield* resources.replica.space(spaceId)
    yield* space.acknowledgeSettlements(minimum)
    applied.set(spaceId, minimum)
  })

  const openConsumer = (spaceId: Identity.SpaceId, consumer: string, start: number) =>
    Effect.sync(() => {
      let perSpace = consumers.get(spaceId)
      if (perSpace === undefined) {
        perSpace = new Map()
        consumers.set(spaceId, perSpace)
      }
      const existing = perSpace.get(consumer)
      const floor = Math.max(start, applied.get(spaceId) ?? 0)
      if (existing === undefined) {
        perSpace.set(consumer, { streams: 1, sequence: floor })
      } else {
        existing.streams += 1
        existing.sequence = Math.min(existing.sequence, floor)
      }
    })

  const closeConsumer = (spaceId: Identity.SpaceId, consumer: string) =>
    Effect.suspend(() => {
      const perSpace = consumers.get(spaceId)
      const existing = perSpace?.get(consumer)
      if (perSpace === undefined || existing === undefined) return Effect.void
      existing.streams -= 1
      if (existing.streams > 0) return Effect.void
      perSpace.delete(consumer)
      return applyAck(spaceId, Number.POSITIVE_INFINITY).pipe(Effect.ignore)
    })

  const spaceFor = (spaceId: Identity.SpaceId) => resources.replica.space(spaceId)

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

  const wirePending = (entry: Replica.PendingMutation): Protocol.PendingMutation => ({
    envelope: entry.envelope,
    optimisticResult: null,
    changes: entry.changes,
    submissionState: entry.submissionState,
    attempts: entry.attempts
  })

  const sessionFrames = (entry: EphemeralSessionEntry): Stream.Stream<
    replicaWire.EphemeralSessionFrame,
    ReplicaError.ReplicaError | replicaWire.WireUnknownDefinition
  > => {
    const members = entry.session.members.pipe(
      Stream.mapEffect(Effect.forEach((item) =>
        encodeJson(entry.profile.payloadSchema, item.value).pipe(
          Effect.map((value) => ({ member: item.member, value, expiresAtMillis: item.expiresAtMillis }))
        )
      )),
      Stream.map((entries): replicaWire.EphemeralSessionFrame => ({ _tag: "Members", entries }))
    )
    const projections = options.ephemerals.map((ephemeralDefinition) => {
      if (ephemeralDefinition.kind === "event") {
        return entry.session.events(ephemeralDefinition).pipe(
          Stream.mapEffect((envelope) =>
            encodeJson(ephemeralDefinition.payloadSchema, envelope.payload).pipe(
              Effect.map((payload): replicaWire.EphemeralSessionFrame => ({
                _tag: "Event",
                name: ephemeralDefinition.name,
                member: envelope.member,
                payload
              }))
            )
          )
        )
      }
      return entry.session.state(ephemeralDefinition).pipe(
        Stream.mapEffect(Effect.forEach((item) =>
          Effect.all({
            key: encodeJson(ephemeralDefinition.keySchema, item.key),
            value: encodeJson(ephemeralDefinition.payloadSchema, item.value)
          }).pipe(
            Effect.map(({ key, value }) => ({
              member: item.member,
              key,
              value,
              expiresAtMillis: item.expiresAtMillis
            }))
          )
        )),
        Stream.map((entries): replicaWire.EphemeralSessionFrame => ({
          _tag: "State",
          name: ephemeralDefinition.name,
          entries
        }))
      )
    })
    return Stream.mergeAll([members, ...projections], { concurrency: "unbounded" }).pipe(
      Stream.catchTag(
        "EphemeralDecodeError",
        () => Stream.fail(new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: entry.profileName }))
      )
    )
  }

  const retainedLease = Stream.concat(Stream.succeed(undefined), Stream.never)

  return replicaWire.ReplicaEntity.of({
    Join: ({ payload }) => resources.replica.join(payload.spaceId).pipe(Effect.asVoid),
    Leave: ({ payload }) => resources.replica.leave(payload.spaceId),
    Spaces: () => resources.replica.spaces.pipe(Effect.map((spaces) => spaces.map((space) => space.spaceId))),
    AggregateStatus: () => resources.replica.status,
    SpaceScope: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.scope)),
    SetScope: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.setScope(payload.scope))),
    Activation: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.activation)),
    Activate: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.activate)),
    Deactivate: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.deactivate)),
    SpaceStatus: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.status)),
    Mutate: Effect.fnUntraced(function*({ payload: request }) {
      const mutation = yield* mutationFor(request.name)
      const payload = yield* decodeWith(mutation.payloadSchema, request.payload)
      const space = yield* spaceFor(request.spaceId)
      return yield* space.mutate(mutation, payload, { mutationId: request.mutationId }).pipe(
        Effect.catch((error) => failMutation(mutation, error))
      )
    }),
    GetEntity: Effect.fnUntraced(function*({ payload: request }) {
      const model = yield* modelFor(request.name)
      const key = yield* decodeWith(model.key, request.key)
      const space = yield* spaceFor(request.spaceId)
      const value = yield* space.get(model, key)
      if (Option.isNone(value)) return Option.none()
      return Option.some(yield* encodeJson(model.schema, value.value))
    }),
    Query: Effect.fnUntraced(function*({ payload: request }) {
      const query = yield* queryFor(request.name)
      const payload = yield* decodeWith(query.payloadSchema, request.payload)
      const space = yield* spaceFor(request.spaceId)
      const result = yield* space.query(query, payload).pipe(
        Effect.catch((error) => failQuery(query, error))
      )
      return yield* encodeJson(query.successSchema, result)
    }),
    ReceiptOf: Effect.fnUntraced(function*({ payload: request }) {
      const mutation = yield* mutationFor(request.name)
      const space = yield* spaceFor(request.spaceId)
      const receipt = yield* space.receipt(mutation, request.mutationId)
      if (Option.isNone(receipt)) return Option.none()
      return Option.some(yield* encodeReceipt(definition, receipt.value))
    }),
    Pending: ({ payload }) =>
      spaceFor(payload.spaceId).pipe(
        Effect.flatMap((space) => space.pending),
        Effect.map((pending) => pending.map(wirePending))
      ),
    PendingFor: Effect.fnUntraced(function*({ payload: request }) {
      const mutation = yield* mutationFor(request.name)
      const space = yield* spaceFor(request.spaceId)
      const pending = yield* space.pendingFor(mutation)
      return pending.map(wirePending)
    }),
    ResolveSettlementStart: ({ payload: request }) =>
      spaceFor(request.spaceId).pipe(Effect.flatMap((space) => space.resolveSettlementStart(request.from))),
    Settlements: ({ payload: request }) => {
      const settlementOptions: Replica.SettlementOptions = { from: request.after }
      const name = request.name
      const settled = Stream.unwrap(Effect.gen(function*() {
        const space = yield* spaceFor(request.spaceId)
        if (name === undefined) return space.settlements(settlementOptions)
        const mutation = yield* mutationFor(name)
        return space.settlementsFor(mutation, settlementOptions)
      }))
      return Stream.unwrap(
        Effect.acquireRelease(
          openConsumer(request.spaceId, request.consumer, request.start),
          () => closeConsumer(request.spaceId, request.consumer)
        ).pipe(Effect.as(settled))
      ).pipe(Stream.mapEffect((entry) => encodeSettlement(definition, entry)))
    },
    AcknowledgeSettlements: ({ payload: request }) =>
      Effect.suspend(() => {
        const consumer = consumers.get(request.spaceId)?.get(request.consumer)
        if (consumer === undefined) return applyAck(request.spaceId, request.sequence)
        if (request.sequence > consumer.sequence) consumer.sequence = request.sequence
        return applyAck(request.spaceId, Number.POSITIVE_INFINITY)
      }),
    QuarantineList: ({ payload }) => spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.quarantine)),
    DiscardQuarantined: ({ payload }) =>
      spaceFor(payload.spaceId).pipe(Effect.flatMap((space) => space.discardQuarantined(payload.mutationId))),
    ResubmitQuarantined: Effect.fnUntraced(function*({ payload: request }) {
      const mutation = yield* mutationFor(request.name)
      const payload = yield* decodeWith(mutation.payloadSchema, request.payload)
      const space = yield* spaceFor(request.spaceId)
      return yield* space.resubmitQuarantined(request.mutationId, mutation, payload).pipe(
        Effect.catch((error) => failMutation(mutation, error))
      )
    }),
    Retain: ({ payload }) =>
      Stream.unwrap(
        Effect.acquireRelease(resources.queryReactivity.retain(payload.key), (release) => release).pipe(
          Effect.as(retainedLease)
        )
      ),
    Invalidations: () =>
      Stream.unwrap(
        resources.invalidations.subscribe.pipe(
          Effect.map((batches) =>
            Stream.concat(
              Stream.succeed<replicaWire.InvalidationFrame>({ _tag: "Subscribed" }),
              batches
            )
          )
        )
      ),
    EphemeralSession: ({ payload: request }) =>
      Stream.unwrap(Effect.gen(function*() {
        const profile = yield* profileFor(request.name)
        const value = yield* decodeWith(profile.payloadSchema, request.value)
        const session = yield* resources.ephemeral.session(profile, {
          spaceId: request.spaceId,
          member: request.member,
          value,
          ttl: request.ttlMillis
        }).pipe(
          Effect.catchTag(
            "EphemeralEncodeError",
            () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
          )
        )
        const handle = `${resources.session}:${nextHandle++}`
        const entry: EphemeralSessionEntry = { profileName: request.name, profile, session }
        yield* Effect.acquireRelease(
          Effect.sync(() => sessions.set(handle, entry)),
          () => Effect.sync(() => sessions.delete(handle))
        )
        return Stream.concat(
          Stream.succeed<replicaWire.EphemeralSessionFrame>({ _tag: "Opened", handle }),
          sessionFrames(entry)
        )
      })),
    EphemeralUpdateMember: Effect.fnUntraced(function*({ payload: request }) {
      const entry = yield* sessionFor(request.handle)
      const value = yield* decodeWith(entry.profile.payloadSchema, request.value)
      return yield* entry.session.updateMember(value).pipe(
        Effect.catchTag(
          "EphemeralEncodeError",
          () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: entry.profileName }))
        )
      )
    }),
    EphemeralPublishEvent: Effect.fnUntraced(function*({ payload: request }) {
      const event = yield* ephemeralFor(request.name)
      if (event.kind !== "event") {
        return yield* new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })
      }
      const payload = yield* decodeWith(event.payloadSchema, request.payload)
      return yield* resources.ephemeral.publish(event, {
        spaceId: request.spaceId,
        member: request.member,
        payload,
        ttl: request.ttlMillis
      }).pipe(
        Effect.catchTag(
          "EphemeralEncodeError",
          () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
        )
      )
    }),
    EphemeralPublishState: Effect.fnUntraced(function*({ payload: request }) {
      const state = yield* ephemeralFor(request.name)
      if (state.kind !== "state") {
        return yield* new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })
      }
      const key = yield* decodeWith(state.keySchema, request.key)
      const payload = yield* decodeWith(state.payloadSchema, request.payload)
      return yield* resources.ephemeral.publish(state, {
        spaceId: request.spaceId,
        member: request.member,
        key,
        payload,
        ttl: request.ttlMillis
      }).pipe(
        Effect.catchTag(
          "EphemeralEncodeError",
          () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
        )
      )
    }),
    EphemeralClear: Effect.fnUntraced(function*({ payload: request }) {
      const event = yield* ephemeralFor(request.name)
      if (event.kind !== "event") {
        return yield* new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })
      }
      return yield* resources.ephemeral.clear(event, { spaceId: request.spaceId, member: request.member })
    }),
    EphemeralRemove: Effect.fnUntraced(function*({ payload: request }) {
      const state = yield* ephemeralFor(request.name)
      if (state.kind !== "state") {
        return yield* new replicaWire.WireUnknownDefinition({ kind: "ephemeral", name: request.name })
      }
      const key = yield* decodeWith(state.keySchema, request.key)
      return yield* resources.ephemeral.remove(state, {
        spaceId: request.spaceId,
        member: request.member,
        key
      }).pipe(
        Effect.catchTag(
          "EphemeralEncodeError",
          () => Effect.fail(new replicaWire.WireEphemeralEncodeError({ name: request.name }))
        )
      )
    })
  })
})

export type TermHandlers = Effect.Success<ReturnType<typeof makeHandlers>>

interface Tagged {
  readonly _tag: string
}

export interface FencedOptions {
  readonly lease: Effect.Effect<OwnerResources, never, Scope.Scope>
  readonly handlersFor: (resources: OwnerResources) => Effect.Effect<TermHandlers>
}

export const makeFencedHandlers = (options: FencedOptions) => {
  const unary = <A, E extends Tagged, R,>(f: (handlers: TermHandlers) => Effect.Effect<A, E, R>) =>
    options.lease.pipe(
      Effect.flatMap(options.handlersFor),
      Effect.flatMap(f),
      Effect.scoped
    )
  const streaming = <A, E extends Tagged, R,>(f: (handlers: TermHandlers) => Stream.Stream<A, E, R>) =>
    Stream.unwrap(options.lease.pipe(Effect.flatMap(options.handlersFor), Effect.map(f)))
  return replicaWire.ReplicaEntity.of({
    Join: (request) => unary((handlers) => handlers.Join(request)),
    Leave: (request) => unary((handlers) => handlers.Leave(request)),
    Spaces: () => unary((handlers) => handlers.Spaces()),
    AggregateStatus: () => unary((handlers) => handlers.AggregateStatus()),
    SpaceScope: (request) => unary((handlers) => handlers.SpaceScope(request)),
    SetScope: (request) => unary((handlers) => handlers.SetScope(request)),
    Activation: (request) => unary((handlers) => handlers.Activation(request)),
    Activate: (request) => unary((handlers) => handlers.Activate(request)),
    Deactivate: (request) => unary((handlers) => handlers.Deactivate(request)),
    SpaceStatus: (request) => unary((handlers) => handlers.SpaceStatus(request)),
    Mutate: (request) => unary((handlers) => handlers.Mutate(request)),
    GetEntity: (request) => unary((handlers) => handlers.GetEntity(request)),
    Query: (request) => unary((handlers) => handlers.Query(request)),
    ReceiptOf: (request) => unary((handlers) => handlers.ReceiptOf(request)),
    Pending: (request) => unary((handlers) => handlers.Pending(request)),
    PendingFor: (request) => unary((handlers) => handlers.PendingFor(request)),
    ResolveSettlementStart: (request) => unary((handlers) => handlers.ResolveSettlementStart(request)),
    Settlements: (request) => streaming((handlers) => handlers.Settlements(request)),
    AcknowledgeSettlements: (request) => unary((handlers) => handlers.AcknowledgeSettlements(request)),
    QuarantineList: (request) => unary((handlers) => handlers.QuarantineList(request)),
    DiscardQuarantined: (request) => unary((handlers) => handlers.DiscardQuarantined(request)),
    ResubmitQuarantined: (request) => unary((handlers) => handlers.ResubmitQuarantined(request)),
    Retain: (request) => streaming((handlers) => handlers.Retain(request)),
    Invalidations: () => streaming((handlers) => handlers.Invalidations()),
    EphemeralSession: (request) => streaming((handlers) => handlers.EphemeralSession(request)),
    EphemeralUpdateMember: (request) => unary((handlers) => handlers.EphemeralUpdateMember(request)),
    EphemeralPublishEvent: (request) => unary((handlers) => handlers.EphemeralPublishEvent(request)),
    EphemeralPublishState: (request) => unary((handlers) => handlers.EphemeralPublishState(request)),
    EphemeralClear: (request) => unary((handlers) => handlers.EphemeralClear(request)),
    EphemeralRemove: (request) => unary((handlers) => handlers.EphemeralRemove(request))
  })
}
