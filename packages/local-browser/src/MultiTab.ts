import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as broadcastRpc from "./internal/broadcastRpc.js"
import * as leadership from "./internal/leadership.js"
import * as Wire from "./internal/multiTabWire.js"
import * as platform from "./internal/platform.js"
import * as replicaHost from "./internal/replicaHost.js"
import * as replicaProxy from "./internal/replicaProxy.js"
import * as replicaWire from "./internal/replicaWire.js"

export interface OwnerContext {
  readonly clientId: Identity.ClientId
}

export interface Options<E extends { readonly _tag: string },> {
  readonly name: string
  readonly definition: Definition.Any
  readonly owner: (context: OwnerContext) => EffectLayer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient,
    E,
    Reactivity.Reactivity
  >
  readonly ephemerals?: ReadonlyArray<Ephemeral.Any>
  readonly profiles?: Readonly<Record<string, Ephemeral.AnyMember>>
  readonly requestPersistence?: boolean
  readonly heartbeatInterval?: Duration.Input
  readonly pingInterval?: Duration.Input
  readonly pingTimeout?: Duration.Input
  readonly clientTimeout?: Duration.Input
  readonly sweepInterval?: Duration.Input
  readonly disconnectGrace?: Duration.Input
  readonly retryDelay?: Duration.Input
  readonly platform?: EffectLayer.Layer<
    platform.WebLocks | platform.TabChannel | platform.EpochStore | platform.ClientIdentityStore
  >
}

export const layerPlatformBrowser: EffectLayer.Layer<
  platform.WebLocks | platform.TabChannel | platform.EpochStore | platform.ClientIdentityStore
> = EffectLayer.mergeAll(
  platform.layerWebLocksNavigator,
  platform.layerTabChannelBroadcast,
  platform.layerEpochStoreLocalStorage,
  platform.layerClientIdentityStoreLocalStorage
)

export interface StorageEstimate {
  readonly usage: number | undefined
  readonly quota: number | undefined
}

export const storageEstimate: Effect.Effect<StorageEstimate> = Effect.suspend(() => {
  if (typeof navigator !== "object" || navigator.storage === undefined) {
    return Effect.succeed({ usage: undefined, quota: undefined })
  }
  return Effect.promise(() => navigator.storage.estimate()).pipe(
    Effect.map((estimate) => ({ usage: estimate.usage, quota: estimate.quota })),
    Effect.catchCause(() => Effect.succeed({ usage: undefined, quota: undefined }))
  )
})

const requestPersistence = Effect.suspend(() => {
  if (typeof navigator !== "object" || navigator.storage === undefined) return Effect.void
  return Effect.promise(() => navigator.storage.persist()).pipe(
    Effect.flatMap((persisted) =>
      Effect.logDebug("storage persistence request").pipe(Effect.annotateLogs({ persisted }))
    ),
    Effect.catchCause(() => Effect.void)
  )
})

interface Backend {
  readonly epoch: Wire.Epoch | undefined
  readonly local: boolean
  readonly replica: Replica.Service
  readonly queryReactivity: QueryReactivity.Service
  readonly ephemeral: EphemeralClient.Service
  readonly acknowledge: (
    spaceId: Identity.SpaceId,
    sequence: number
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

const clientIdKey = (name: string): string => `@lucas-barake/effect-local-browser:${name}:client-id`

// oxlint-disable-next-line effect/noGlobals -- This package is the browser platform adapter; tab and client identifiers come from the browser's own crypto.
const makeUuid = Effect.sync(() => crypto.randomUUID())

export const layer = <E extends { readonly _tag: string },>(options: Options<E>): EffectLayer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient,
  never,
  Reactivity.Reactivity
> =>
  EffectLayer.effectContext(Effect.gen(function*() {
    const baseReactivity = yield* Reactivity.Reactivity
    const platformContext = yield* EffectLayer.build(options.platform ?? layerPlatformBrowser)
    const channels = Context.get(platformContext, platform.TabChannel)
    const locks = Context.get(platformContext, platform.WebLocks)
    const identities = Context.get(platformContext, platform.ClientIdentityStore)

    const definition = options.definition
    const fingerprint = `${Wire.protocolVersion}:${definition.hash}`
    const heartbeatInterval = options.heartbeatInterval ?? Duration.seconds(2)
    const pingInterval = options.pingInterval ?? Duration.seconds(3)
    const pingTimeout = options.pingTimeout ?? Duration.seconds(6)
    const clientTimeout = options.clientTimeout ?? Duration.seconds(15)
    const sweepInterval = options.sweepInterval ?? Duration.seconds(5)
    const disconnectGrace = options.disconnectGrace ?? Duration.seconds(10)
    const retryDelay = options.retryDelay ?? Duration.seconds(1)

    const profiles = new Map<string, Ephemeral.AnyMember>()
    const profileNames = new Map<Ephemeral.AnyMember, string>()
    for (const [name, profile] of Object.entries(options.profiles ?? {})) {
      profiles.set(name, profile)
      profileNames.set(profile, name)
    }

    const tabId = Wire.TabId.make(yield* makeUuid)
    const clientId = yield* Effect.scoped(Effect.gen(function*() {
      yield* locks.acquire(`${leadership.lockName(options.name)}:client-identity`)
      const storedClientId = yield* identities.load(clientIdKey(options.name))
      if (storedClientId !== undefined) return Identity.ClientId.make(storedClientId)
      const generated = Identity.ClientId.make(`cli_${yield* makeUuid}`)
      yield* identities.store(clientIdKey(options.name), generated)
      return generated
    }))
    const connection = yield* channels.open(`@lucas-barake/effect-local-browser:${options.name}`)

    const layerScope = yield* Effect.scope
    let backendChanged = yield* Deferred.make<void>()
    let promoted = yield* Deferred.make<void>()
    let localOwner = false
    let currentBackend: Backend

    interface Retention {
      count: number
      readonly leases: Map<Backend, Effect.Effect<void>>
    }
    const retentions = new Map<string, Retention>()
    const retentionLock = yield* Semaphore.make(1)

    const ensureRetentions = (backend: Backend): Effect.Effect<void> => {
      const entries = retentions.entries()
      return retentionLock.withPermit(
        Effect.forEach(entries, ([key, retention]) => {
          if (retention.leases.has(backend)) return Effect.void
          return backend.queryReactivity.retain(key).pipe(
            Effect.map((release) => {
              retention.leases.set(backend, release)
            })
          )
        }, { discard: true })
      )
    }

    const releaseBackendRetentions = (backend: Backend): Effect.Effect<void> => {
      const entries = retentions.values()
      return retentionLock.withPermit(
        Effect.forEach(entries, (retention) => {
          const release = retention.leases.get(backend)
          if (release === undefined) return Effect.void
          retention.leases.delete(backend)
          return release
        }, { discard: true })
      )
    }

    const releaseRetention = (key: string): Effect.Effect<void> =>
      retentionLock.withPermit(Effect.suspend(() => {
        const retention = retentions.get(key)
        if (retention === undefined) return Effect.void
        retention.count -= 1
        if (retention.count > 0) return Effect.void
        retentions.delete(key)
        return Effect.forEach(retention.leases.entries(), ([backend, release]) => {
          if (backend === currentBackend) return release
          return Effect.forkIn(release, layerScope).pipe(Effect.asVoid)
        }, { discard: true })
      }))

    const notifyBackendChanged = Effect.gen(function*() {
      const previous = backendChanged
      backendChanged = yield* Deferred.make<void>()
      yield* Deferred.succeed(previous, undefined)
    })

    const fullRefreshKeys = (): Array<string> => {
      const keys: Array<string> = [ReactivityKey.spaces, ReactivityKey.aggregateStatus]
      for (const spaceId of proxy.registrations.knownSpaces) {
        keys.push(ReactivityKey.membership(spaceId))
      }
      return keys
    }

    const fullRefresh = Effect.suspend(() => baseReactivity.invalidate(fullRefreshKeys()))

    const leaderControl = yield* Deferred.make<leadership.Leadership>()

    const onLeaderSilent = Effect.gen(function*() {
      const control = yield* Deferred.await(leaderControl)
      yield* control.requestSteal
    })

    const clientProtocol = yield* broadcastRpc.makeClientProtocol({
      tabId,
      fingerprint,
      connection,
      heartbeatInterval,
      pingInterval,
      pingTimeout,
      onLeaderSilent,
      isParked: () => localOwner,
      parkInterrupt: Effect.suspend(() => Deferred.await(promoted)),
      onLeaderReady: Effect.suspend(() =>
        ensureRetentions(remoteBackend).pipe(
          Effect.andThen(proxy.registrations.reregister),
          Effect.andThen(fullRefresh)
        )
      )
    })
    const wireClient = yield* RpcClient.make(replicaWire.ReplicaRpcs).pipe(
      Effect.provideService(RpcClient.Protocol, clientProtocol)
    )

    const proxy = replicaProxy.makeProxy({
      definition,
      profileNames,
      client: wireClient,
      makeHandle: makeUuid,
      reconnectDelay: retryDelay
    })

    const remoteBackend: Backend = {
      epoch: undefined,
      local: false,
      replica: proxy.replica,
      queryReactivity: proxy.queryReactivity,
      ephemeral: proxy.ephemeral,
      acknowledge: (spaceId, sequence) =>
        proxy.replica.space(spaceId).pipe(
          Effect.flatMap((space) => space.acknowledgeSettlements(sequence))
        )
    }
    currentBackend = remoteBackend

    const invalidationWatermark: { epoch: Wire.Epoch | undefined; seq: number } = { epoch: undefined, seq: 0 }
    yield* broadcastRpc.subscribeFrames(connection, {
      onFrame: (frame) => {
        if (frame._tag !== "Invalidation") return Effect.void
        return Effect.suspend(() => {
          if (currentBackend.local) return Effect.void
          if (invalidationWatermark.epoch !== frame.epoch) {
            invalidationWatermark.epoch = frame.epoch
            invalidationWatermark.seq = frame.seq
            if (frame.seq === 1) return baseReactivity.invalidate(frame.keys)
            return baseReactivity.invalidate([...frame.keys, ...fullRefreshKeys()])
          }
          if (frame.seq === invalidationWatermark.seq + 1) {
            invalidationWatermark.seq = frame.seq
            return baseReactivity.invalidate(frame.keys)
          }
          if (frame.seq <= invalidationWatermark.seq) return Effect.void
          invalidationWatermark.seq = frame.seq
          return baseReactivity.invalidate([...frame.keys, ...fullRefreshKeys()])
        })
      }
    })

    const whileLeader = Effect.fnUntraced(function*(epoch: Wire.Epoch) {
      const grantScope = yield* Effect.scope
      const invalidationSeq = { value: 0 }
      const decoratedReactivity: Reactivity.Reactivity["Service"] = {
        ...baseReactivity,
        invalidate: (keys) =>
          baseReactivity.invalidate(keys).pipe(
            Effect.andThen(Effect.suspend(() => {
              const flat: Array<string> = []
              if (Array.isArray(keys)) {
                for (const key of keys) {
                  if (typeof key === "string") flat.push(key)
                }
              } else {
                for (const key of Object.keys(keys)) flat.push(key)
              }
              if (flat.length === 0) return Effect.void
              invalidationSeq.value += 1
              return broadcastRpc.postFrame(
                connection,
                Wire.Invalidation.make({ epoch, seq: invalidationSeq.value, keys: flat })
              )
            }))
          )
      }
      const buildOwner = EffectLayer.build(
        options.owner({ clientId }).pipe(
          EffectLayer.provide(EffectLayer.succeed(Reactivity.Reactivity, decoratedReactivity))
        )
      ).pipe(Effect.exit)
      const ownerExit = yield* buildOwner
      if (ownerExit._tag === "Failure") {
        yield* Effect.logWarning("multi-tab owner stack failed to build").pipe(
          Effect.annotateLogs({ cause: String(ownerExit.cause) })
        )
        return false
      }
      const ownerContext = ownerExit.value
      const replica = Context.get(ownerContext, Replica.Replica)
      const queryReactivity = Context.get(ownerContext, QueryReactivity.QueryReactivity)
      const ephemeral = Context.get(ownerContext, EphemeralClient.EphemeralClient)

      const server = yield* broadcastRpc.makeServerProtocol({
        epoch,
        leaderId: tabId,
        fingerprint,
        connection,
        clientTimeout,
        sweepInterval
      })
      const host = yield* replicaHost.makeHost({
        definition,
        ephemerals: options.ephemerals ?? [],
        profiles,
        replica,
        queryReactivity,
        ephemeral,
        server,
        disconnectGrace
      })
      yield* EffectLayer.build(
        RpcServer.layer(replicaWire.ReplicaRpcs).pipe(
          EffectLayer.provide(host.layerHandlers),
          EffectLayer.provide(EffectLayer.succeed(RpcServer.Protocol, server.protocol))
        )
      )

      if (options.requestPersistence !== false) {
        yield* Effect.forkScoped(requestPersistence)
      }

      const localBackend: Backend = {
        epoch,
        local: true,
        replica,
        queryReactivity,
        ephemeral,
        acknowledge: (spaceId, sequence) => host.acknowledgeLocal(spaceId, sequence)
      }
      yield* Scope.addFinalizer(
        grantScope,
        Effect.gen(function*() {
          localOwner = false
          currentBackend = remoteBackend
          yield* notifyBackendChanged
          const releaseExit = yield* Effect.exit(releaseBackendRetentions(localBackend))
          const refreshExit = yield* Effect.exit(fullRefresh)
          if (releaseExit._tag === "Failure") yield* Effect.failCause(releaseExit.cause)
          if (refreshExit._tag === "Failure") yield* Effect.failCause(refreshExit.cause)
        })
      )
      localOwner = true
      const parking = promoted
      promoted = yield* Deferred.make<void>()
      yield* Deferred.succeed(parking, undefined)
      currentBackend = localBackend
      yield* notifyBackendChanged
      yield* ensureRetentions(localBackend)
      yield* fullRefresh
      return true
    })

    const control = yield* leadership.makeLeadership({
      name: options.name,
      tabId,
      connection,
      retryDelay,
      whileLeader
    }).pipe(Effect.provide(platformContext))
    yield* Deferred.succeed(leaderControl, control)

    const backendStream = <A,>(
      make: (backend: Backend) => Stream.Stream<A, ReplicaError.ReplicaError>
    ): Stream.Stream<A, ReplicaError.ReplicaError> => {
      const run = (): Stream.Stream<A, ReplicaError.ReplicaError> =>
        Stream.suspend(() => {
          const backend = currentBackend
          const changed = Deferred.await(backendChanged)
          return make(backend).pipe(
            Stream.interruptWhen(changed),
            Stream.concat(Stream.suspend(run))
          )
        })
      return run()
    }

    const facadeSpace = (
      spaceId: Identity.SpaceId,
      initial?: { readonly backend: Backend; readonly space: Replica.Space }
    ): Replica.Space => {
      let cached: typeof initial
      if (initial?.backend.local === false) cached = initial
      const resolveSpace = (backend: Backend): Effect.Effect<Replica.Space, ReplicaError.ReplicaError> => {
        if (cached?.backend === backend) return Effect.succeed(cached.space)
        return backend.replica.space(spaceId).pipe(
          Effect.map((space) => {
            if (!backend.local) cached = { backend, space }
            return space
          })
        )
      }
      const withSpace = <A, EX extends { readonly _tag: string },>(
        use: (space: Replica.Space) => Effect.Effect<A, EX>
      ): Effect.Effect<A, EX | ReplicaError.ReplicaError> =>
        Effect.suspend(() => resolveSpace(currentBackend).pipe(Effect.flatMap(use)))
      const settlementCursor = (
        streamOptions: Replica.SettlementOptions | undefined,
        make: (space: Replica.Space, from: Replica.SettlementOptions | undefined) => Stream.Stream<
          Replica.SettledMutation,
          ReplicaError.ReplicaError
        >
      ): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> => {
        let cursor: number | undefined
        return backendStream((backend) =>
          Stream.unwrap(
            resolveSpace(backend).pipe(
              Effect.map((space) => {
                let from = streamOptions
                if (cursor !== undefined) from = { from: cursor }
                return make(space, from).pipe(
                  Stream.tap((settled) =>
                    Effect.sync(() => {
                      cursor = settled.sequence
                    })
                  )
                )
              })
            )
          )
        )
      }
      const facade: Replica.Space = {
        spaceId,
        scope: withSpace((space) => space.scope),
        setScope: (scope) => withSpace((space) => space.setScope(scope)),
        activation: withSpace((space) => space.activation),
        activate: withSpace((space) => space.activate),
        deactivate: withSpace((space) => space.deactivate),
        status: withSpace((space) => space.status),
        mutate: (mutation, payload) => withSpace((space) => space.mutate(mutation, payload)),
        get: (model, key) => withSpace((space) => space.get(model, key)),
        query: (query, payload) => withSpace((space) => space.query(query, payload)),
        receipt: (mutation, mutationId) => withSpace((space) => space.receipt(mutation, mutationId)),
        pending: withSpace((space) => space.pending),
        pendingFor: (mutation) => withSpace((space) => space.pendingFor(mutation)),
        settlements: (streamOptions) => settlementCursor(streamOptions, (space, from) => space.settlements(from)),
        settlementsFor: (mutation, streamOptions) =>
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The underlying settlementsFor filters by this mutation; the cursor wrapper erases the generic.
          settlementCursor(streamOptions, (space, from) => space.settlementsFor(mutation, from)) as never,
        acknowledgeSettlements: (sequence) => Effect.suspend(() => currentBackend.acknowledge(spaceId, sequence)),
        quarantine: withSpace((space) => space.quarantine),
        discardQuarantined: (mutationId) => withSpace((space) => space.discardQuarantined(mutationId)),
        resubmitQuarantined: (mutationId, mutation, payload) =>
          withSpace((space) => space.resubmitQuarantined(mutationId, mutation, payload))
      }
      return facade
    }

    const facadeReplica: Replica.Service = {
      join: (spaceId) =>
        Effect.suspend(() => {
          const backend = currentBackend
          return backend.replica.join(spaceId).pipe(
            Effect.map((space) => facadeSpace(spaceId, { backend, space }))
          )
        }),
      leave: (spaceId) => Effect.suspend(() => currentBackend.replica.leave(spaceId)),
      spaces: Effect.suspend(() => {
        const backend = currentBackend
        return backend.replica.spaces.pipe(
          Effect.map((spaces) => spaces.map((space) => facadeSpace(space.spaceId, { backend, space })))
        )
      }),
      space: (spaceId) =>
        Effect.suspend(() => {
          const backend = currentBackend
          return backend.replica.space(spaceId).pipe(
            Effect.map((space) => facadeSpace(spaceId, { backend, space }))
          )
        }),
      status: Effect.suspend(() => currentBackend.replica.status)
    }

    const facadeQueryReactivity: QueryReactivity.Service = {
      retain: (key) =>
        retentionLock.withPermit(Effect.gen(function*() {
          const existing = retentions.get(key)
          if (existing !== undefined) {
            existing.count += 1
            return releaseRetention(key)
          }
          const backend = currentBackend
          const release = yield* backend.queryReactivity.retain(key)
          retentions.set(key, { count: 1, leases: new Map([[backend, release]]) })
          return releaseRetention(key)
        })),
      record: (key, reads) => Effect.suspend(() => currentBackend.queryReactivity.record(key, reads)),
      affected: (changes) => Effect.suspend(() => currentBackend.queryReactivity.affected(changes))
    }

    const facadeEphemeral: EphemeralClient.Service = {
      session: (profile, sessionOptions) =>
        Effect.suspend(() => currentBackend.ephemeral.session(profile, sessionOptions)),
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The overloaded publish signature cannot be implemented by a single delegating function without erasing the overloads.
      publish: ((definitionArg: Ephemeral.Any, publishOptions: never) =>
        Effect.suspend(() =>
          // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The overload target is chosen by the definition's kind at runtime; the single delegating function erases it.
          currentBackend.ephemeral.publish(definitionArg as never, publishOptions)
        )) as EphemeralClient.Service["publish"],
      clear: (definitionArg, target) => Effect.suspend(() => currentBackend.ephemeral.clear(definitionArg, target)),
      remove: (definitionArg, removeOptions) =>
        Effect.suspend(() => currentBackend.ephemeral.remove(definitionArg, removeOptions))
    }

    return Context.empty().pipe(
      Context.add(Replica.Replica, facadeReplica),
      Context.add(QueryReactivity.QueryReactivity, facadeQueryReactivity),
      Context.add(EphemeralClient.EphemeralClient, facadeEphemeral)
    )
  }))
