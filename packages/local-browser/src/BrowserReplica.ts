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
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as Sharding from "effect/unstable/cluster/Sharding"
import type * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import { BrowserStorageError } from "./BrowserStorageError.js"
import * as lockNames from "./internal/lockNames.js"
import * as platform from "./internal/platform.js"
import * as replicaHost from "./internal/replicaHost.js"
import * as ReplicaOwner from "./internal/replicaOwner.js"
import * as replicaProxy from "./internal/replicaProxy.js"
import * as replicaWire from "./internal/replicaWire.js"
import * as TabCluster from "./internal/tabCluster.js"
import * as TabScheduler from "./internal/tabScheduler.js"

export { BrowserStorageError } from "./BrowserStorageError.js"

export interface Options<D extends Definition.Any, ED extends Tagged, ES extends Tagged,> {
  readonly name: string
  readonly definition: D
  readonly layerDatabase: Layer.Layer<SqlClient.SqlClient, ED>
  readonly layerSync: Layer.Layer<SyncEngine.SyncEngine | EphemeralClient.EphemeralClient, ES>
  readonly spaces?: Iterable<Identity.SpaceId> | undefined
  readonly replica?: Omit<SqlReplica.Options<D>, "definition" | "clientId" | "initialSpaces"> | undefined
  readonly ephemerals?: ReadonlyArray<Ephemeral.Any> | undefined
  readonly profiles?: Readonly<Record<string, Ephemeral.AnyMember>> | undefined
  readonly requestPersistence?: boolean | undefined
  readonly retryDelay?: Duration.Input | undefined
  readonly sharding?: Partial<ShardingConfig.ShardingConfig["Service"]> | undefined
  readonly layerPlatform?:
    | Layer.Layer<platform.WebLocks | platform.TabChannel | platform.TabVisibility | platform.ClientIdentityStore>
    | undefined
}

interface Tagged {
  readonly _tag: string
}

export const layerPlatformBrowser: Layer.Layer<
  platform.WebLocks | platform.TabChannel | platform.TabVisibility | platform.ClientIdentityStore
> = Layer.mergeAll(
  platform.layerWebLocksNavigator,
  platform.layerTabChannelBroadcast,
  platform.layerTabVisibilityDocument,
  platform.layerClientIdentityStoreLocalStorage
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

const randomUuid = Crypto.Crypto.use((crypto) => crypto.randomUUIDv4).pipe(
  Effect.catchTag("PlatformError", (error) => Effect.die(error))
)

const loadClientId = Effect.fnUntraced(function*(
  names: lockNames.LockNames,
  key: string,
  locks: platform.WebLocksService,
  identities: platform.ClientIdentityStoreService
) {
  yield* locks.acquire(names.clientIdentity)
  const stored = yield* identities.load(key)
  if (stored !== undefined) {
    return yield* Schema.decodeUnknownEffect(Identity.ClientId)(stored).pipe(
      Effect.mapError((cause) => new BrowserStorageError({ operation: "decode", key, cause }))
    )
  }
  const generated = Identity.ClientId.make(`cli_${yield* randomUuid}`)
  yield* identities.store(key, generated)
  return generated
}, Effect.scoped)

export const layer = <D extends Definition.Any, ED extends Tagged, ES extends Tagged,>(
  options: Options<D, ED, ES>
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient | Sharding.Sharding,
  BrowserStorageError,
  Reactivity.Reactivity | MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D>
> =>
  Layer.effectContext(Effect.gen(function*() {
    const scheduler = yield* TabScheduler.make
    return yield* build(options).pipe(Effect.provideService(Scheduler.Scheduler, scheduler))
  })).pipe(Layer.provide(BrowserCrypto.layer))

const build = Effect.fnUntraced(function*<D extends Definition.Any, ED extends Tagged, ES extends Tagged,>(
  options: Options<D, ED, ES>
) {
  const reactivity = yield* Reactivity.Reactivity
  const handlers = Context.pick(
    ...options.definition.mutations.map((mutation) => mutation.handler),
    ...options.definition.queries.map((query) => query.handler)
  )(yield* Effect.context<MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D>>())
  const crypto = yield* Crypto.Crypto
  const platformContext = yield* Layer.build(options.layerPlatform ?? layerPlatformBrowser)
  const locks = Context.get(platformContext, platform.WebLocks)
  const channels = Context.get(platformContext, platform.TabChannel)
  const visibility = Context.get(platformContext, platform.TabVisibility)
  const identities = Context.get(platformContext, platform.ClientIdentityStore)
  const names = lockNames.make(options.name)
  const host = yield* randomUuid
  const clientId = yield* loadClientId(
    names,
    `@lucas-barake/effect-local-browser:${options.name}:client-id`,
    locks,
    identities
  )
  const retryDelay = options.retryDelay ?? Duration.seconds(1)
  const profiles = new Map<string, Ephemeral.AnyMember>(Object.entries(options.profiles ?? {}))
  const profileNames = new Map<Ephemeral.AnyMember, string>()
  for (const [name, profile] of profiles) profileNames.set(profile, name)
  const ephemerals = options.ephemerals ?? []

  const layerStack = SqlReplica.layer({
    ...options.replica,
    definition: options.definition,
    clientId,
    initialSpaces: options.spaces ?? []
  }).pipe(
    Layer.provide(Layer.succeedContext(handlers)),
    Layer.provide(options.layerDatabase),
    Layer.provide(BrowserCrypto.layer),
    Layer.provideMerge(options.layerSync)
  )
  let layerOwner = layerStack
  if (options.requestPersistence !== false) {
    layerOwner = Layer.merge(layerStack, layerRequestPersistence)
  }

  const owner = yield* ReplicaOwner.make({
    host,
    names,
    locks,
    channels,
    visibility,
    retryDelay,
    layerOwner
  })
  const cluster = yield* TabCluster.make({
    name: options.name,
    host,
    names,
    locks,
    channels,
    isDraining: owner.isDraining,
    shardingConfig: options.sharding
  })
  const termHandlers = new WeakMap<replicaHost.OwnerResources, replicaHost.TermHandlers>()
  const handlersFor = (resources: replicaHost.OwnerResources) =>
    Effect.suspend(() => {
      const known = termHandlers.get(resources)
      if (known !== undefined) return Effect.succeed(known)
      return replicaHost.makeHandlers({ definition: options.definition, ephemerals, profiles, resources }).pipe(
        Effect.tap((built) => Effect.sync(() => termHandlers.set(resources, built)))
      )
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
    retryDelay: Duration.fromInputUnsafe(retryDelay),
    awaitRouted: cluster.awaitRouted
  })
  return Context.make(Replica.Replica, proxy.replica).pipe(
    Context.add(QueryReactivity.QueryReactivity, proxy.queryReactivity),
    Context.add(EphemeralClient.EphemeralClient, proxy.ephemeral),
    Context.add(Sharding.Sharding, sharding)
  )
})
