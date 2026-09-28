import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerServer from "effect/unstable/cluster/RunnerServer"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as Snowflake from "effect/unstable/cluster/Snowflake"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import type * as lockNames from "./lockNames.js"
import type * as platform from "./platform.js"
import * as TabRunnerStorage from "./tabRunnerStorage.js"
import * as TabTransport from "./tabTransport.js"

export const shardingDefaults: Partial<ShardingConfig.ShardingConfig["Service"]> = {
  shardsPerGroup: 16,
  shardLockRefreshInterval: Duration.seconds(1),
  refreshAssignmentsInterval: Duration.zero,
  entityMessagePollInterval: Duration.millis(250),
  sendRetryInterval: Duration.millis(50),
  entityTerminationTimeout: Duration.seconds(1),
  simulateRemoteSerialization: false
}

const layerRunners = Layer.effect(
  Runners.Runners,
  Runners.makeRpc.pipe(
    Effect.map((runners) =>
      Runners.Runners.of({
        ...runners,
        send: (options) => runners.send(options).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true))
      })
    )
  )
).pipe(Layer.provide(Snowflake.layerGenerator))

export interface Options {
  readonly host: string
  readonly names: lockNames.LockNames
  readonly locks: platform.WebLocksService
  readonly channels: platform.TabChannelService
  readonly isDraining: () => boolean
  readonly shardingConfig?: Partial<ShardingConfig.ShardingConfig["Service"]> | undefined
}

export interface TabCluster {
  readonly layer: Layer.Layer<Sharding.Sharding | Runners.Runners>
  readonly registered: Effect.Effect<void>
  readonly awaitRouted: Effect.Effect<boolean>
}

export const make = Effect.fnUntraced(function*(options: Options) {
  const self = RunnerAddress.make(options.host, 0)
  const config: ShardingConfig.ShardingConfig["Service"] = {
    ...ShardingConfig.defaults,
    ...shardingDefaults,
    ...options.shardingConfig,
    runnerAddress: Option.some(self),
    runnerListenAddress: Option.none()
  }
  const transport = yield* TabTransport.make({
    names: options.names,
    self,
    locks: options.locks,
    channels: options.channels
  })
  const storage = yield* TabRunnerStorage.make({
    self,
    channels: options.channels,
    names: options.names,
    locks: options.locks,
    groups: config.assignedShardGroups,
    weight: config.runnerShardWeight,
    isDraining: options.isDraining
  })
  const layer = RunnerServer.layer.pipe(
    Layer.provideMerge(Sharding.layer),
    Layer.provideMerge(layerRunners),
    Layer.provide([
      Layer.succeed(RpcServer.Protocol, transport.server),
      Layer.succeed(Runners.RpcClientProtocol, transport.clients),
      Layer.succeed(RunnerStorage.RunnerStorage, storage.storage),
      RunnerHealth.layerNoop,
      MessageStorage.layerMemory
    ]),
    Layer.provide(Layer.succeed(ShardingConfig.ShardingConfig, config))
  )
  const cluster: TabCluster = { layer, registered: storage.registered, awaitRouted: storage.awaitRouted }
  return cluster
})
