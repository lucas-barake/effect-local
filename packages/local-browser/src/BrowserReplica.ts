import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import type * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import type * as QueryExecutor from "@lucas-barake/effect-local-sql/QueryExecutor"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Sharding from "effect/cluster/Sharding"
import type * as ShardingConfig from "effect/cluster/ShardingConfig"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/sql/SqlClient"
import { BrowserStorageError } from "./BrowserStorageError.js"
import * as BuildGate from "./internal/buildGate.js"
import * as BuildIdentity from "./internal/buildIdentity.js"
import * as configuration from "./internal/configuration.js"
import * as lockNames from "./internal/lockNames.js"
import * as platform from "./internal/platform.js"
import * as replicaHost from "./internal/replicaHost.js"
import * as ReplicaOwner from "./internal/replicaOwner.js"
import * as replicaProxy from "./internal/replicaProxy.js"
import * as replicaWire from "./internal/replicaWire.js"
import * as TabCluster from "./internal/tabCluster.js"
import * as TabScheduler from "./internal/tabScheduler.js"

export { BrowserStorageError } from "./BrowserStorageError.js"

export interface Options<D extends Definition.Any,> {
  readonly name: string
  readonly definition: D
  readonly spaces?: Iterable<Identity.SpaceId> | undefined
  readonly replica?: Omit<SqlReplica.Options<D>, "definition" | "clientId" | "initialSpaces"> | undefined
  readonly ephemerals?: ReadonlyArray<Ephemeral.Any> | undefined
  readonly profiles?: Readonly<Record<string, Ephemeral.AnyMember>> | undefined
  readonly requestPersistence?: boolean | undefined
  readonly retryDelay?: Duration.Input | undefined
  readonly eventCapacity?: number | undefined
  readonly sharding?: Partial<ShardingConfig.ShardingConfig["Service"]> | undefined
}

interface Tagged {
  readonly _tag: string
}

export type Platform =
  | platform.WebLocks
  | platform.TabChannel
  | platform.TabVisibility
  | platform.ClientIdentityStore
  | Crypto.Crypto

export const layerPlatformBrowser: Layer.Layer<Platform> = Layer.mergeAll(
  platform.layerWebLocksNavigator,
  platform.layerTabChannelBroadcast,
  platform.layerTabVisibilityDocument,
  platform.layerClientIdentityStoreLocalStorage,
  BrowserCrypto.layer
)

const requestPersistence = Effect.suspend(() => {
  if (typeof navigator !== "object" || navigator.storage === undefined) return Effect.void
  return Effect.promise(() => navigator.storage.persist()).pipe(
    Effect.flatMap((persisted) =>
      Effect.logDebug("storage persistence request").pipe(Effect.annotateLogs({ persisted }))
    )
  )
})

const layerRequestPersistence = Layer.effectDiscard(Effect.forkScoped(requestPersistence))

const randomUuid = (crypto: Crypto.Crypto) =>
  crypto.randomUUIDv4.pipe(Effect.catchTag("PlatformError", (error) => Effect.die(error)))

const loadClientId = Effect.fnUntraced(function*(
  names: lockNames.LockNames,
  key: string,
  locks: platform.WebLocksService,
  identities: platform.ClientIdentityStoreService,
  crypto: Crypto.Crypto
) {
  yield* locks.acquire(names.clientIdentity)
  const stored = yield* identities.load(key)
  if (stored !== undefined) {
    return yield* Schema.decodeUnknownEffect(Identity.ClientId)(stored).pipe(
      Effect.mapError((cause) => new BrowserStorageError({ operation: "decode", key, cause }))
    )
  }
  const generated = Identity.ClientId.make(`cli_${yield* randomUuid(crypto)}`)
  yield* identities.store(key, generated)
  return generated
}, Effect.scoped)

export const layer = <D extends Definition.Any, E extends Tagged, R,>(
  layerOwner: Layer.Layer<SqlClient.SqlClient | SyncEngine.SyncEngine | EphemeralClient.EphemeralClient, E, R>,
  options: Options<D>
): Layer.Layer<
  | Replica.Replica
  | QueryReactivity.QueryReactivity
  | EphemeralClient.EphemeralClient
  | Sharding.Sharding
  | Crypto.Crypto,
  BrowserStorageError | ReplicaError.InvalidConfiguration,
  Reactivity.Reactivity | MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D> | Platform | R
> =>
  Layer.effectContext(Effect.gen(function*() {
    const retryDelayMillis = yield* configuration.positiveFiniteDurationMillis(
      "retryDelay",
      options.retryDelay ?? Duration.seconds(1)
    )
    const eventCapacity = yield* configuration.positiveSafeInteger("eventCapacity", options.eventCapacity ?? 1_024)
    const scheduler = yield* TabScheduler.make
    return yield* build(layerOwner, options, retryDelayMillis, eventCapacity).pipe(
      Effect.provideService(Scheduler.Scheduler, scheduler)
    )
  }))

const build = Effect.fnUntraced(function*<D extends Definition.Any, E extends Tagged, R,>(
  layerOwner: Layer.Layer<SqlClient.SqlClient | SyncEngine.SyncEngine | EphemeralClient.EphemeralClient, E, R>,
  options: Options<D>,
  retryDelayMillis: number,
  eventCapacity: number
) {
  const reactivity = yield* Reactivity.Reactivity
  const handlers = Context.pick(
    ...options.definition.mutations.map((mutation) => mutation.handler),
    ...options.definition.queries.map((query) => query.handler)
  )(yield* Effect.context<MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D>>())
  const crypto = yield* Crypto.Crypto
  const locks = yield* platform.WebLocks
  const channels = yield* platform.TabChannel
  const visibility = yield* platform.TabVisibility
  const identities = yield* platform.ClientIdentityStore
  const profiles = new Map<string, Ephemeral.AnyMember>(Object.entries(options.profiles ?? {}))
  const profileNames = new Map<Ephemeral.AnyMember, string>()
  for (const [name, profile] of profiles) profileNames.set(profile, name)
  const ephemerals = options.ephemerals ?? []
  const identity = BuildIdentity.make({ definition: options.definition, ephemerals, profiles })
  const names = lockNames.make(options.name, identity.fingerprint)
  const host = yield* randomUuid(crypto)
  const clientId = yield* loadClientId(
    names,
    `@lucas-barake/effect-local-browser:${options.name}:client-id`,
    locks,
    identities,
    crypto
  )

  const layerStack = SqlReplica.layer({
    ...options.replica,
    definition: options.definition,
    clientId,
    initialSpaces: options.spaces ?? []
  }).pipe(
    Layer.provide(Layer.succeedContext(handlers)),
    Layer.provideMerge(layerOwner)
  )
  let layerTerm = layerStack
  if (options.requestPersistence !== false) {
    layerTerm = Layer.merge(layerStack, layerRequestPersistence)
  }

  const gate = yield* BuildGate.make({ host, build: identity, names, locks, channels })
  const owner = yield* ReplicaOwner.make(layerTerm, {
    host,
    names,
    locks,
    channels,
    visibility,
    retryDelayMillis,
    gate
  })
  const cluster = yield* TabCluster.make({
    host,
    names,
    locks,
    channels,
    isDraining: owner.isDraining,
    shardingConfig: options.sharding
  })
  const termHandlers = new WeakMap<replicaHost.OwnerResources, replicaHost.TermHandlers>()
  const handlersFor = (resources: replicaHost.OwnerResources) =>
    Effect.sync(() => {
      const known = termHandlers.get(resources)
      if (known !== undefined) return known
      const built = replicaHost.makeHandlers({ definition: options.definition, ephemerals, profiles, resources })
      termHandlers.set(resources, built)
      return built
    })
  const layerEntity = replicaWire.ReplicaEntity.toLayer(
    Effect.sync(() =>
      replicaHost.makeFencedHandlers({ lease: owner.lease, drainingLease: owner.drainingLease, handlersFor })
    ),
    { concurrency: "unbounded", mailboxCapacity: "unbounded" }
  )
  const shardingContext = yield* Layer.build(layerEntity.pipe(Layer.provideMerge(cluster.layer)))
  const sharding = Context.get(shardingContext, Sharding.Sharding)
  yield* cluster.registered
  const makeClient = yield* replicaWire.ReplicaEntity.client.pipe(
    Effect.provideService(Sharding.Sharding, sharding)
  )
  const proxy = yield* replicaProxy.makeProxy({
    definition: options.definition,
    profileNames,
    client: makeClient(options.name),
    consumer: host,
    reactivity,
    crypto,
    retryDelayMillis,
    eventCapacity,
    awaitRouted: cluster.awaitRouted,
    superseded: gate.superseded
  })
  return Context.make(Replica.Replica, proxy.replica).pipe(
    Context.add(QueryReactivity.QueryReactivity, proxy.queryReactivity),
    Context.add(EphemeralClient.EphemeralClient, proxy.ephemeral),
    Context.add(Sharding.Sharding, sharding),
    Context.add(Crypto.Crypto, crypto)
  )
})
