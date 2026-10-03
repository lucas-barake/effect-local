import type * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import type * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Quarantine from "@lucas-barake/effect-local/Quarantine"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import type * as ClusterError from "effect/cluster/ClusterError"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import type * as Queue from "effect/Queue"
import type * as Reactivity from "effect/reactivity/Reactivity"
import type * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcGroup from "effect/rpc/RpcGroup"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { boundedTtlMillis } from "./configuration.js"
import { invalidConfiguration } from "./errors.js"
import * as LosslessQueue from "./losslessQueue.js"
import type * as replicaWire from "./replicaWire.js"
import { decodeWith, encodeJson, type Json } from "./wireCodec.js"

type TransportError =
  | ClusterError.MailboxFull
  | ClusterError.AlreadyProcessingMessage
  | ClusterError.PersistenceError
  | ClusterError.EntityNotAssignedToRunner

export type ReplicaClient = RpcClient.RpcClient.From<RpcGroup.Rpcs<typeof replicaWire.ReplicaRpcs>, TransportError>

export interface ProxyOptions {
  readonly definition: Definition.Any
  readonly profileNames: ReadonlyMap<Ephemeral.AnyMember, string>
  readonly client: ReplicaClient
  readonly consumer: string
  readonly reactivity: Reactivity.Reactivity
  readonly crypto: Crypto.Crypto
  readonly retryDelayMillis: number
  readonly awaitRouted: Effect.Effect<boolean>
  readonly superseded: Deferred.Deferred<never, ReplicaError.BuildSuperseded>
}

export interface ReplicaProxy {
  readonly replica: Replica.Service
  readonly queryReactivity: QueryReactivity.Service
  readonly ephemeral: EphemeralClient.Service
}

const ownerUnavailable = new ReplicaError.OwnerUnavailable({ reason: "transport" })

interface Tagged {
  readonly _tag: string
}

function mapTransport<A, E extends Tagged,>(
  effect: Effect.Effect<A, E | TransportError>
): Effect.Effect<A, Exclude<E, TransportError> | ReplicaError.OwnerUnavailable>
function mapTransport(
  effect: Effect.Effect<unknown, Tagged | TransportError>
): Effect.Effect<unknown, Tagged | ReplicaError.OwnerUnavailable> {
  return effect.pipe(
    Effect.catchTag("MailboxFull", () => Effect.fail(ownerUnavailable)),
    Effect.catchTag("AlreadyProcessingMessage", () => Effect.fail(ownerUnavailable)),
    Effect.catchTag("PersistenceError", () => Effect.fail(ownerUnavailable)),
    Effect.catchTag("EntityNotAssignedToRunner", () => Effect.fail(ownerUnavailable))
  )
}

function dieUnknownDefinition<A, E extends Tagged,>(
  effect: Effect.Effect<A, E | replicaWire.WireUnknownDefinition | replicaWire.WireUnknownSession>
): Effect.Effect<A, Exclude<E, replicaWire.WireUnknownDefinition | replicaWire.WireUnknownSession>>
function dieUnknownDefinition(
  effect: Effect.Effect<unknown, Tagged | replicaWire.WireUnknownDefinition | replicaWire.WireUnknownSession>
): Effect.Effect<unknown, Tagged> {
  return effect.pipe(
    Effect.catchTag("WireUnknownDefinition", (error) => Effect.die(error)),
    Effect.catchTag("WireUnknownSession", (error) => Effect.die(error))
  )
}

const wireMutation = (definition: Definition.Any, name: string) =>
  Effect.suspend(() => {
    const mutation = definition.mutationByName.get(name)
    if (mutation === undefined) return Effect.die(`Unknown mutation on the wire: ${name}`)
    return Effect.succeed(mutation)
  })

export const decodePending = Effect.fnUntraced(function*(
  definition: Definition.Any,
  pending: Protocol.PendingMutation
) {
  const mutation = yield* wireMutation(definition, pending.envelope.name)
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

function decodeReceipt<M extends Mutation.Any,>(
  definition: Definition.Any,
  receipt: Protocol.Receipt
): Effect.Effect<Replica.Receipt<M>, ReplicaError.StorageCorrupt>
function decodeReceipt(
  definition: Definition.Any,
  receipt: Protocol.Receipt
): Effect.Effect<Replica.Receipt<Mutation.Any>, ReplicaError.StorageCorrupt> {
  switch (receipt._tag) {
    case "Accepted": {
      return wireMutation(definition, receipt.name).pipe(
        Effect.flatMap((mutation) => decodeWith(mutation.successSchema, receipt.result)),
        Effect.map((result): Replica.Receipt<Mutation.Any> => ({ ...receipt, result }))
      )
    }
    case "Rejected": {
      if (receipt.origin !== "Mutation") return Effect.succeed({ ...receipt, origin: receipt.origin })
      return wireMutation(definition, receipt.name).pipe(
        Effect.flatMap((mutation) => decodeWith(mutation.rejectionSchema, receipt.rejection)),
        Effect.map((rejection): Replica.Receipt<Mutation.Any> => ({ ...receipt, origin: "Mutation", rejection }))
      )
    }
    default: {
      return Effect.succeed(receipt)
    }
  }
}

const decodeAnySettlement = Effect.fnUntraced(function*(
  definition: Definition.Any,
  wire: replicaWire.WireSettlement
) {
  const pending = yield* decodePending(definition, wire.pending)
  const receipt = yield* decodeReceipt(definition, wire.receipt)
  const settled: Replica.SettledMutation = { sequence: wire.sequence, settlement: { pending, receipt } }
  return settled
})

function decodeSettlement<M extends Mutation.Any,>(
  definition: Definition.Any,
  wire: replicaWire.WireSettlement
): Effect.Effect<Replica.SettledMutation<M>, ReplicaError.StorageCorrupt>
function decodeSettlement(
  definition: Definition.Any,
  wire: replicaWire.WireSettlement
): Effect.Effect<Replica.SettledMutation, ReplicaError.StorageCorrupt> {
  return decodeAnySettlement(definition, wire)
}

const streamFrom = <A, E extends { readonly _tag: string },>(
  queue: Effect.Effect<Queue.Dequeue<A, E>, never, Scope.Scope>
): Stream.Stream<A, Exclude<E, Cause.Done>> => Stream.unwrap(Effect.map(queue, LosslessQueue.stream))

const failureOutsideHandover = <A, E extends Tagged,>(exit: Exit.Exit<A, E>): Cause.Cause<E> | undefined => {
  if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return undefined
  return exit.cause
}

export const makeProxy = Effect.fnUntraced(function*(options: ProxyOptions) {
  const proxyScope = yield* Effect.scope

  const awaitRoutedOrUnavailable = options.awaitRouted.pipe(
    Effect.flatMap((routed) => {
      if (routed) return Effect.void
      return Effect.fail(ownerUnavailable)
    })
  )

  const retryHandover = <A, E extends Tagged,>(
    effect: Effect.Effect<A, E>
  ): Effect.Effect<A, E | ReplicaError.OwnerUnavailable> =>
    Effect.exit(effect).pipe(
      Effect.flatMap((exit) => {
        if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
          return awaitRoutedOrUnavailable.pipe(Effect.andThen(retryHandover(effect)))
        }
        return exit
      })
    )

  const awaitSuperseded = Deferred.await(options.superseded)
  const supersededSignal = awaitSuperseded.pipe(Effect.ignore)

  const ensureCurrent = Deferred.isDone(options.superseded).pipe(
    Effect.flatMap((done) => {
      if (done) return awaitSuperseded
      return Effect.void
    })
  )

  const guard = <A, E extends Tagged,>(effect: Effect.Effect<A, E>) =>
    ensureCurrent.pipe(Effect.andThen(Effect.raceFirst(effect, awaitSuperseded)))

  const call = <A, E extends Tagged,>(
    effect: Effect.Effect<A, E | TransportError | replicaWire.WireUnknownDefinition | replicaWire.WireUnknownSession>
  ) => guard(retryHandover(effect)).pipe(mapTransport, dieUnknownDefinition)

  const resubscribeAfterHandover = <A, E extends Tagged,>(effect: Effect.Effect<A, E>) =>
    Effect.exit(effect).pipe(
      Effect.flatMap((exit) => {
        if (failureOutsideHandover(exit) === undefined) return options.awaitRouted
        return Effect.sleep(options.retryDelayMillis).pipe(Effect.as(true))
      }),
      Effect.repeat({ while: (routed) => routed }),
      Effect.asVoid
    )

  const definition = options.definition
  const client = options.client
  const handles = new Map<Identity.SpaceId, Replica.Space>()
  const known = new Set<Identity.SpaceId>()
  const joined = new Set<Identity.SpaceId>()
  let membershipEpoch = 0
  let invalidationsLive = false
  const mintMutationId = Identity.makeMutationId.pipe(Effect.provideService(Crypto.Crypto, options.crypto))

  const fullRefreshKeys = (): Array<string> => {
    const keys: Array<string> = [ReactivityKey.spaces, ReactivityKey.aggregateStatus]
    for (const spaceId of known) keys.push(ReactivityKey.membership(spaceId))
    return keys
  }

  const dropMemberships = () => {
    membershipEpoch += 1
    joined.clear()
  }

  const forgetMemberships = () => {
    invalidationsLive = false
    dropMemberships()
  }

  const forgetInvalidatedMemberships = (keys: ReadonlyArray<string>) => {
    const invalidated = new Set(keys)
    for (const spaceId of known) {
      if (!invalidated.has(ReactivityKey.membership(spaceId))) continue
      membershipEpoch += 1
      joined.delete(spaceId)
    }
  }

  const liveEpoch = (): number | undefined => {
    if (!invalidationsLive) return undefined
    return membershipEpoch
  }

  const rememberJoined = (epoch: number | undefined, spaceIds: ReadonlyArray<Identity.SpaceId>) => {
    if (epoch === undefined || epoch !== membershipEpoch) return
    for (const spaceId of spaceIds) joined.add(spaceId)
  }

  const subscribed = Deferred.makeUnsafe<void>()
  let resubscribing = false
  yield* streamFrom(client.Invalidations({}, { asQueue: true })).pipe(
    Stream.runForEach((frame) => {
      if (frame._tag === "Keys") {
        forgetInvalidatedMemberships(frame.keys)
        return options.reactivity.invalidate(frame.keys)
      }
      if (frame._tag === "Overflow") {
        dropMemberships()
        return options.reactivity.invalidate(fullRefreshKeys())
      }
      invalidationsLive = true
      if (resubscribing) {
        dropMemberships()
        return options.reactivity.invalidate(fullRefreshKeys())
      }
      resubscribing = true
      return Deferred.succeed(subscribed, undefined)
    }),
    Effect.ensuring(Effect.sync(forgetMemberships)),
    resubscribeAfterHandover,
    Effect.raceFirst(supersededSignal),
    Effect.forkIn(proxyScope)
  )
  yield* supersededSignal.pipe(
    Effect.andThen(Effect.suspend(() => {
      forgetMemberships()
      return options.reactivity.invalidate(fullRefreshKeys())
    })),
    Effect.forkIn(proxyScope)
  )
  yield* Effect.raceFirst(Deferred.await(subscribed), supersededSignal)

  const settlementsStream = (
    spaceId: Identity.SpaceId,
    streamOptions: Replica.SettlementOptions | undefined,
    name: string | undefined
  ): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> => {
    let start: number | undefined
    let cursor = 0
    const resolveStart = Effect.suspend(() => {
      if (start !== undefined) return Effect.succeed(start)
      return client.ResolveSettlementStart({ spaceId, from: streamOptions?.from ?? "live" }).pipe(
        Effect.tap((resolved) =>
          Effect.sync(() => {
            start = resolved
            cursor = resolved
          })
        )
      )
    })
    const session = (): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> =>
      Stream.unwrap(
        resolveStart.pipe(
          Effect.map((resolved) =>
            streamFrom(
              client.Settlements({ spaceId, consumer: options.consumer, start: resolved, after: cursor, name }, {
                asQueue: true
              })
            )
          )
        )
      ).pipe(
        Stream.catchTag("WireUnknownDefinition", (error) => Stream.die(error)),
        Stream.catchTags({
          MailboxFull: () => Stream.fail(ownerUnavailable),
          AlreadyProcessingMessage: () => Stream.fail(ownerUnavailable),
          PersistenceError: () => Stream.fail(ownerUnavailable),
          EntityNotAssignedToRunner: () => Stream.fail(ownerUnavailable)
        }),
        Stream.filter((wire) => wire.sequence > cursor),
        Stream.mapEffect((wire) => decodeSettlement(definition, wire)),
        Stream.tap((settled) =>
          Effect.sync(() => {
            cursor = settled.sequence
          })
        ),
        Stream.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Stream.empty
          return Stream.failCause(cause)
        })
      )
    const routed = Stream.concat(Stream.succeed(undefined), Stream.fromEffectRepeat(awaitRoutedOrUnavailable))
    return routed.pipe(Stream.flatMap(session), Stream.interruptWhen(awaitSuperseded))
  }

  const spaceHandle = (spaceId: Identity.SpaceId): Replica.Space => {
    const existing = handles.get(spaceId)
    if (existing !== undefined) return existing
    known.add(spaceId)
    const created = makeSpace(spaceId)
    handles.set(spaceId, created)
    return created
  }

  const makeSpace = (spaceId: Identity.SpaceId): Replica.Space => {
    const mutateAny = Effect.fnUntraced(function*(
      mutation: Mutation.Any,
      payload: unknown,
      mutateOptions: Replica.MutateOptions | undefined
    ) {
      const mutationId = mutateOptions?.mutationId ?? (yield* mintMutationId)
      const encoded = yield* encodeJson(mutation.payloadSchema, payload)
      return yield* call(client.Mutate({ spaceId, name: mutation.name, payload: encoded, mutationId })).pipe(
        Effect.catchTag(
          "WireMutationRejection",
          (wire) => decodeWith(mutation.rejectionSchema, wire.rejection).pipe(Effect.flatMap(Effect.fail))
        )
      )
    })

    function mutate<M extends Mutation.Any,>(
      mutation: M,
      payload: Mutation.Payload<M>,
      mutateOptions?: Replica.MutateOptions
    ): Effect.Effect<Protocol.PendingMutation, ReplicaError.ReplicaError | Mutation.Rejection<M>>
    function mutate(
      mutation: Mutation.Any,
      payload: unknown,
      mutateOptions?: Replica.MutateOptions
    ): Effect.Effect<Protocol.PendingMutation, ReplicaError.ReplicaError | Mutation.Rejection<Mutation.Any>> {
      return mutateAny(mutation, payload, mutateOptions)
    }

    function get<M extends Model.Any,>(
      model: M,
      key: Model.Key<M>
    ): Effect.Effect<Option.Option<Model.Value<M>>, ReplicaError.ReplicaError>
    function get(model: Model.Any, key: unknown): Effect.Effect<Option.Option<unknown>, ReplicaError.ReplicaError> {
      return encodeJson(model.key, key).pipe(
        Effect.flatMap((encoded) => call(client.GetEntity({ spaceId, name: model.name, key: encoded }))),
        Effect.flatMap(Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (value) => decodeWith(model.schema, value).pipe(Effect.map(Option.some))
        }))
      )
    }

    function query<Q extends Query.Any,>(
      definitionArg: Q,
      payload: Q["payloadSchema"]["Type"]
    ): Effect.Effect<
      Q["successSchema"]["Type"],
      ReplicaError.ReplicaError | ReplicaError.QueryFailed | Q["errorSchema"]["Type"]
    >
    function query(
      definitionArg: Query.Any,
      payload: unknown
    ): Effect.Effect<unknown, ReplicaError.ReplicaError | ReplicaError.QueryFailed | Query.TaggedError> {
      return encodeJson(definitionArg.payloadSchema, payload).pipe(
        Effect.flatMap((encoded) =>
          call(client.Query({ spaceId, name: definitionArg.name, payload: encoded })).pipe(
            Effect.catchTag("WireQueryError", (wire) =>
              decodeWith(definitionArg.errorSchema, wire.error).pipe(Effect.flatMap(Effect.fail)))
          )
        ),
        Effect.flatMap((result) => decodeWith(definitionArg.successSchema, result))
      )
    }

    function receipt<M extends Mutation.Any,>(
      mutation: M,
      mutationId: Identity.MutationId
    ): Effect.Effect<Option.Option<Replica.Receipt<M>>, ReplicaError.ReplicaError>
    function receipt(
      mutation: Mutation.Any,
      mutationId: Identity.MutationId
    ): Effect.Effect<Option.Option<Replica.Receipt<Mutation.Any>>, ReplicaError.ReplicaError> {
      return call(client.ReceiptOf({ spaceId, name: mutation.name, mutationId })).pipe(
        Effect.flatMap(Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (wire) => decodeReceipt(definition, wire).pipe(Effect.map(Option.some))
        }))
      )
    }

    function pendingFor<M extends Mutation.Any,>(
      mutation: M
    ): Effect.Effect<ReadonlyArray<Replica.PendingMutation<M>>, ReplicaError.ReplicaError>
    function pendingFor(
      mutation: Mutation.Any
    ): Effect.Effect<ReadonlyArray<Replica.PendingMutation>, ReplicaError.ReplicaError> {
      return call(client.PendingFor({ spaceId, name: mutation.name })).pipe(
        Effect.flatMap(Effect.forEach((entry) => decodePending(definition, entry)))
      )
    }

    function settlementsFor<M extends Mutation.Any,>(
      mutation: M,
      streamOptions?: Replica.SettlementOptions
    ): Stream.Stream<Replica.SettledMutation<M>, ReplicaError.ReplicaError>
    function settlementsFor(
      mutation: Mutation.Any,
      streamOptions?: Replica.SettlementOptions
    ): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> {
      return settlementsStream(spaceId, streamOptions, mutation.name)
    }

    function resubmitQuarantined<M extends Mutation.Any,>(
      mutationId: Identity.MutationId,
      mutation: M,
      payload: Mutation.Payload<M>
    ): Effect.Effect<
      Quarantine.ResubmitResult,
      ReplicaError.ReplicaError | Mutation.Rejection<M>
    >
    function resubmitQuarantined(
      mutationId: Identity.MutationId,
      mutation: Mutation.Any,
      payload: unknown
    ): Effect.Effect<
      Quarantine.ResubmitResult,
      ReplicaError.ReplicaError | Mutation.Rejection<Mutation.Any>
    > {
      return encodeJson(mutation.payloadSchema, payload).pipe(
        Effect.flatMap((encoded) =>
          call(client.ResubmitQuarantined({ spaceId, mutationId, name: mutation.name, payload: encoded })).pipe(
            Effect.catchTag(
              "WireMutationRejection",
              (wire) => decodeWith(mutation.rejectionSchema, wire.rejection).pipe(Effect.flatMap(Effect.fail))
            )
          )
        )
      )
    }

    return {
      spaceId,
      scope: call(client.SpaceScope({ spaceId })),
      setScope: (scope) => call(client.SetScope({ spaceId, scope })),
      activation: call(client.Activation({ spaceId })),
      activate: call(client.Activate({ spaceId })),
      deactivate: call(client.Deactivate({ spaceId })),
      status: call(client.SpaceStatus({ spaceId })),
      mutate,
      get,
      query,
      receipt,
      pending: call(client.Pending({ spaceId })).pipe(
        Effect.flatMap(Effect.forEach((entry) => decodePending(definition, entry)))
      ),
      pendingFor,
      settlements: (streamOptions) => settlementsStream(spaceId, streamOptions, undefined),
      settlementsFor,
      resolveSettlementStart: (from) => call(client.ResolveSettlementStart({ spaceId, from })),
      acknowledgeSettlements: (sequence) =>
        call(client.AcknowledgeSettlements({ spaceId, consumer: options.consumer, sequence })),
      quarantine: call(client.QuarantineList({ spaceId })),
      discardQuarantined: (mutationId) => call(client.DiscardQuarantined({ spaceId, mutationId })),
      resubmitQuarantined
    }
  }

  const replica: Replica.Service = {
    join: (spaceId) => call(client.Join({ spaceId })).pipe(Effect.map(() => spaceHandle(spaceId))),
    leave: (spaceId) =>
      call(client.Leave({ spaceId })).pipe(
        Effect.ensuring(Effect.sync(() => forgetInvalidatedMemberships([ReactivityKey.membership(spaceId)])))
      ),
    spaces: Effect.suspend(() => {
      const epoch = liveEpoch()
      return call(client.Spaces({})).pipe(
        Effect.map((spaceIds) => {
          rememberJoined(epoch, spaceIds)
          return spaceIds.map(spaceHandle)
        })
      )
    }),
    space: (spaceId) =>
      Effect.suspend(() => {
        known.add(spaceId)
        if (joined.has(spaceId)) return Effect.succeed(spaceHandle(spaceId))
        const epoch = liveEpoch()
        return call(client.SpaceScope({ spaceId })).pipe(
          Effect.map(() => {
            rememberJoined(epoch, [spaceId])
            return spaceHandle(spaceId)
          })
        )
      }),
    status: call(client.AggregateStatus({}))
  }

  interface Lease {
    count: number
    readonly scope: Scope.Closeable
    readonly acquired: Deferred.Deferred<void>
  }
  const leases = new Map<string, Lease>()

  const releaseLease = (key: string) =>
    Effect.suspend(() => {
      const lease = leases.get(key)
      if (lease === undefined) return Effect.void
      lease.count -= 1
      if (lease.count > 0) return Effect.void
      leases.delete(key)
      return Scope.close(lease.scope, Exit.void)
    })

  const retain = Effect.fnUntraced(function*(key: string) {
    const existing = leases.get(key)
    if (existing !== undefined) {
      existing.count += 1
      yield* Effect.raceFirst(Deferred.await(existing.acquired), supersededSignal)
      return releaseLease(key)
    }
    const acquired = Deferred.makeUnsafe<void>()
    const scope = Scope.forkUnsafe(proxyScope)
    leases.set(key, { count: 1, scope, acquired })
    let acquisitions = 0
    yield* streamFrom(client.Retain({ key }, { asQueue: true })).pipe(
      Stream.runForEach(() =>
        Effect.suspend(() => {
          acquisitions += 1
          if (acquisitions === 1) return Deferred.succeed(acquired, undefined)
          return options.reactivity.invalidate([key])
        })
      ),
      resubscribeAfterHandover,
      Effect.raceFirst(supersededSignal),
      Effect.forkIn(scope)
    )
    yield* Effect.raceFirst(Deferred.await(acquired), supersededSignal)
    return releaseLease(key)
  })

  const queryReactivity: QueryReactivity.Service = {
    retain,
    record: () => Effect.void,
    affected: () => Effect.succeed([])
  }

  const openSession = Effect.fnUntraced(function*(
    profile: Ephemeral.AnyMember,
    sessionOptions: EphemeralClient.SessionOptions<Ephemeral.AnyMember>
  ) {
    const sessionScope = yield* Effect.scope
    const name = options.profileNames.get(profile)
    if (name === undefined) {
      return yield* invalidConfiguration("profiles", "Ephemeral profile is not registered with the browser replica")
    }
    const initialValue = yield* encodeJson(profile.payloadSchema, sessionOptions.value)
    const ttlMillis = yield* boundedTtlMillis(
      sessionOptions.ttl,
      Protocol.minimumEphemeralMemberTtlMillis,
      Protocol.maximumEphemeralMemberTtlMillis
    )
    let latestValue: Json = initialValue
    let openedValue: Json = initialValue
    const handle = yield* SubscriptionRef.make(Option.none<string>())
    const makeSessionRef = <A,>(initial: A) =>
      Effect.acquireRelease(SubscriptionRef.make(initial), (ref) => PubSub.shutdown(ref.pubsub)).pipe(
        Scope.provide(sessionScope)
      )
    const members = yield* makeSessionRef(
      Option.none<ReadonlyArray<typeof replicaWire.EphemeralMemberFrame.Type>>()
    )
    const states = new Map<
      string,
      SubscriptionRef.SubscriptionRef<Option.Option<ReadonlyArray<typeof replicaWire.EphemeralStateFrame.Type>>>
    >()
    const stateRef = (stateName: string) =>
      Effect.suspend(() => {
        const existing = states.get(stateName)
        if (existing !== undefined) return Effect.succeed(existing)
        return makeSessionRef(
          Option.none<ReadonlyArray<typeof replicaWire.EphemeralStateFrame.Type>>()
        ).pipe(Effect.tap((created) => Effect.sync(() => states.set(stateName, created))))
      })
    const events = yield* Effect.acquireRelease(
      PubSub.unbounded<Extract<replicaWire.EphemeralSessionFrame, { readonly _tag: "Event" }>>(),
      PubSub.shutdown
    )
    const opened = yield* Deferred.make<void, ReplicaError.ReplicaError | Ephemeral.EncodeError>()

    const updateRemote = (value: Json) =>
      ensureCurrent.pipe(
        Effect.andThen(SubscriptionRef.get(handle)),
        Effect.flatMap(Option.match({
          onNone: () => Effect.void,
          onSome: (current) =>
            retryHandover(client.EphemeralUpdateMember({ handle: current, value })).pipe(
              guard,
              Effect.catchTag("WireUnknownSession", () => Effect.void),
              mapTransport,
              dieUnknownDefinition
            )
        }))
      )

    const onFrame = (frame: replicaWire.EphemeralSessionFrame): Effect.Effect<void, ReplicaError.ReplicaError> => {
      if (frame._tag === "Opened") {
        return SubscriptionRef.set(handle, Option.some(frame.handle)).pipe(
          Effect.andThen(Deferred.succeed(opened, undefined)),
          Effect.andThen(Effect.suspend(() => {
            if (latestValue === openedValue) return Effect.void
            return updateRemote(latestValue).pipe(
              Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
            )
          }))
        )
      }
      if (frame._tag === "Members") return SubscriptionRef.set(members, Option.some(frame.entries))
      if (frame._tag === "Event") return PubSub.publish(events, frame).pipe(Effect.asVoid)
      return stateRef(frame.name).pipe(
        Effect.flatMap((ref) => SubscriptionRef.set(ref, Option.some(frame.entries)))
      )
    }

    const openRemote = Stream.suspend(() => {
      openedValue = latestValue
      return streamFrom(client.EphemeralSession({
        name,
        spaceId: sessionOptions.spaceId,
        member: sessionOptions.member,
        value: openedValue,
        ttlMillis
      }, { asQueue: true }))
    })

    yield* openRemote.pipe(
      Stream.runForEach(onFrame),
      Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error)),
      Effect.catchTag("WireUnknownDefinition", (error) => Effect.die(error)),
      Effect.catchTag("WireUnknownSession", (error) => Effect.die(error)),
      mapTransport,
      Effect.exit,
      Effect.tap(() => SubscriptionRef.set(handle, Option.none())),
      Effect.flatMap((exit) => {
        const cause = failureOutsideHandover(exit)
        if (cause === undefined) {
          return options.awaitRouted.pipe(
            Effect.flatMap((routed) => {
              if (routed) return Effect.succeed(false)
              return Deferred.fail(opened, ownerUnavailable).pipe(Effect.as(true))
            })
          )
        }
        return Deferred.failCause(opened, cause).pipe(
          Effect.flatMap((openFailed) => {
            if (openFailed) return Effect.succeed(true)
            return Effect.sleep(options.retryDelayMillis).pipe(Effect.as(false))
          })
        )
      }),
      Effect.repeat({ until: (openFailed) => openFailed }),
      Effect.raceFirst(supersededSignal),
      Effect.forkIn(sessionScope)
    )
    yield* Effect.raceFirst(Deferred.await(opened), awaitSuperseded)

    function eventsOf<D extends Ephemeral.AnyEvent,>(
      definitionArg: D
    ): Stream.Stream<EphemeralClient.EventEnvelope<D>, Ephemeral.DecodeError | ReplicaError.ReplicaError>
    function eventsOf(
      definitionArg: Ephemeral.AnyEvent
    ): Stream.Stream<
      EphemeralClient.EventEnvelope<Ephemeral.AnyEvent>,
      Ephemeral.DecodeError | ReplicaError.ReplicaError
    > {
      return Stream.fromPubSub(events).pipe(
        Stream.filter((frame) => frame.name === definitionArg.name),
        Stream.mapEffect((frame) =>
          decodeWith(definitionArg.payloadSchema, frame.payload).pipe(
            Effect.map((payload) => ({ member: frame.member, payload }))
          )
        ),
        Stream.interruptWhen(awaitSuperseded)
      )
    }

    function stateOf<D extends Ephemeral.AnyState,>(
      definitionArg: D
    ): Stream.Stream<ReadonlyArray<EphemeralClient.StateEntry<D>>, Ephemeral.DecodeError | ReplicaError.ReplicaError>
    function stateOf(
      definitionArg: Ephemeral.AnyState
    ): Stream.Stream<
      ReadonlyArray<EphemeralClient.StateEntry<Ephemeral.AnyState>>,
      Ephemeral.DecodeError | ReplicaError.ReplicaError
    > {
      return Stream.unwrap(stateRef(definitionArg.name).pipe(Effect.map(SubscriptionRef.changes))).pipe(
        Stream.filter(Option.isSome),
        Stream.map((entries) => entries.value),
        Stream.mapEffect(Effect.forEach((frame) =>
          Effect.all({
            key: decodeWith(definitionArg.keySchema, frame.key),
            value: decodeWith(definitionArg.payloadSchema, frame.value)
          }).pipe(
            Effect.map(({ key, value }) => ({
              member: frame.member,
              key,
              value,
              expiresAtMillis: frame.expiresAtMillis
            }))
          )
        )),
        Stream.interruptWhen(awaitSuperseded)
      )
    }

    const result: EphemeralClient.Session<Ephemeral.AnyMember> = {
      spaceId: sessionOptions.spaceId,
      member: sessionOptions.member,
      events: eventsOf,
      state: stateOf,
      members: SubscriptionRef.changes(members).pipe(
        Stream.filter(Option.isSome),
        Stream.map((entries) => entries.value),
        Stream.mapEffect(Effect.forEach((frame) =>
          decodeWith(profile.payloadSchema, frame.value).pipe(
            Effect.map((value) => ({ member: frame.member, value, expiresAtMillis: frame.expiresAtMillis }))
          )
        )),
        Stream.interruptWhen(awaitSuperseded)
      ),
      updateMember: (value) =>
        encodeJson(profile.payloadSchema, value).pipe(
          Effect.tap((encoded) =>
            Effect.sync(() => {
              latestValue = encoded
            })
          ),
          Effect.flatMap(updateRemote),
          Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
        )
    }
    return result
  })

  function session<M extends Ephemeral.AnyMember,>(
    profile: M,
    sessionOptions: EphemeralClient.SessionOptions<M>
  ): Effect.Effect<EphemeralClient.Session<M>, ReplicaError.ReplicaError | Ephemeral.EncodeError, Scope.Scope>
  function session(
    profile: Ephemeral.AnyMember,
    sessionOptions: EphemeralClient.SessionOptions<Ephemeral.AnyMember>
  ): Effect.Effect<
    EphemeralClient.Session<Ephemeral.AnyMember>,
    ReplicaError.ReplicaError | Ephemeral.EncodeError,
    Scope.Scope
  > {
    return openSession(profile, sessionOptions)
  }

  const publish: EphemeralClient.Service["publish"] = Effect.fnUntraced(function*(
    definitionArg: Ephemeral.Any,
    publishOptions: {
      readonly spaceId: Identity.SpaceId
      readonly member: Protocol.EphemeralMember
      readonly payload: unknown
      readonly key?: unknown
      readonly ttl: Duration.Input
    }
  ) {
    if (definitionArg.kind === "event") {
      const payload = yield* encodeJson(definitionArg.payloadSchema, publishOptions.payload)
      const ttlMillis = yield* boundedTtlMillis(publishOptions.ttl, 1, Protocol.maximumEphemeralEventTtlMillis)
      return yield* call(client.EphemeralPublishEvent({
        name: definitionArg.name,
        spaceId: publishOptions.spaceId,
        member: publishOptions.member,
        payload,
        ttlMillis
      })).pipe(Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error)))
    }
    const key = yield* encodeJson(definitionArg.keySchema, publishOptions.key)
    const payload = yield* encodeJson(definitionArg.payloadSchema, publishOptions.payload)
    const ttlMillis = yield* boundedTtlMillis(publishOptions.ttl, 1, Protocol.maximumEphemeralStateTtlMillis)
    return yield* call(client.EphemeralPublishState({
      name: definitionArg.name,
      spaceId: publishOptions.spaceId,
      member: publishOptions.member,
      key,
      payload,
      ttlMillis
    })).pipe(Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error)))
  })

  const ephemeral: EphemeralClient.Service = {
    session,
    publish,
    clear: (definitionArg, target) =>
      call(client.EphemeralClear({ name: definitionArg.name, spaceId: target.spaceId, member: target.member })).pipe(
        Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
      ),
    remove: (definitionArg, removeOptions) =>
      encodeJson(definitionArg.keySchema, removeOptions.key).pipe(
        Effect.flatMap((key) =>
          call(client.EphemeralRemove({
            name: definitionArg.name,
            spaceId: removeOptions.spaceId,
            member: removeOptions.member,
            key
          }))
        ),
        Effect.catchTag("WireEphemeralEncodeError", (error) => Effect.die(error))
      )
  }

  const proxy: ReplicaProxy = { replica, queryReactivity, ephemeral }
  return proxy
})
