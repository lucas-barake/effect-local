import * as Layer from "effect/Layer"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"

export const messagePrefix = "effect_local_cluster"
export const runnerPrefix = "effect_local_runner"

export const layer = Sharding.layer.pipe(
  Layer.provideMerge(Runners.layerNoop),
  Layer.provideMerge(SqlMessageStorage.layerWith({ prefix: messagePrefix })),
  Layer.provide([
    Layer.orDie(SqlRunnerStorage.layerWith({ prefix: runnerPrefix })),
    RunnerHealth.layerNoop
  ]),
  Layer.provide(ShardingConfig.layerFromEnv())
)
