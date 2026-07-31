import * as Effect from "effect/Effect"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"

export const messagePrefix = "effect_local_cluster"
export const runnerPrefix = "effect_local_runner"

// A command sent before this runner has acquired its shards is routed to storage instead of the
// local entity, and its reply is then only observed on `entityReplyPollInterval` wall-clock
// ticks: extra real latency in production and a deadlock under TestClock. A singleton starts
// exactly when its shard is acquired, so awaiting one during construction guarantees that by
// the time the layer is built, entity commands take the shard-local reply path.
const awaitShardsAcquired = Layer.effectDiscard(Effect.gen(function*() {
  const sharding = yield* Sharding.Sharding
  const acquired = yield* Latch.make(false)
  yield* sharding.registerSingleton("EffectLocal/ShardsAcquired", acquired.open)
  yield* acquired.await
}))

export const layer = awaitShardsAcquired.pipe(
  Layer.provideMerge(Sharding.layer),
  Layer.provideMerge(Runners.layerNoop),
  Layer.provideMerge(SqlMessageStorage.layerWith({ prefix: messagePrefix })),
  Layer.provide([
    Layer.orDie(SqlRunnerStorage.layerWith({ prefix: runnerPrefix })),
    RunnerHealth.layerNoop
  ]),
  Layer.provide(ShardingConfig.layerFromEnv())
)
