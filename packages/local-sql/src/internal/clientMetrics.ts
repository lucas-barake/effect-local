import * as Effect from "effect/Effect"
import * as Metric from "effect/Metric"

export const make = Effect.gen(function*() {
  const bootstrapInstall = Metric.counter("effect_local_client_bootstrap_install", {
    description: "Durable client bootstrap installations",
    incremental: true
  })
  const pendingMutationCount = Metric.counter("effect_local_client_pending_mutation_count", {
    description: "Pending client mutations across active local stores",
    incremental: false
  })
  let pendingContribution = 0

  yield* Effect.addFinalizer(() => {
    const contribution = pendingContribution
    pendingContribution = 0
    return Metric.update(pendingMutationCount, -contribution)
  })

  return {
    recordBootstrapInstall: Metric.update(bootstrapInstall, 1),
    refreshPending: <E, R,>(read: Effect.Effect<number, E, R>) =>
      Effect.gen(function*() {
        const count = yield* read
        const delta = count - pendingContribution
        yield* Metric.update(pendingMutationCount, delta)
        pendingContribution = count
      })
  }
})
