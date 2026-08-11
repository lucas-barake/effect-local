import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Evolution from "@lucas-barake/effect-local/Evolution"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as LocalStore from "./LocalStore.js"
import * as MutationRuntime from "./MutationRuntime.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as Reconciler from "./Reconciler.js"
import * as ReconciliationWorkflow from "./ReconciliationWorkflow.js"

export interface Options<D extends Definition.Any,> {
  readonly definition: D
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly maximumPendingMutations?: number
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly pageSize?: number
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
  readonly maximumAttempts?: number
}

export const layerFromServices: Layer.Layer<
  Replica.Replica,
  never,
  LocalStore.Store | QueryExecutor.QueryExecutor | Reconciler.Reconciler
> = Layer.effect(
  Replica.Replica,
  Effect.gen(function*() {
    const local = yield* LocalStore.Store
    const queries = yield* QueryExecutor.QueryExecutor
    const reconciler = yield* Reconciler.Reconciler
    return Replica.Replica.of({
      mutate: (mutation, payload) => local.mutate(mutation, payload).pipe(Effect.tap(() => reconciler.notify)),
      get: local.get,
      query: queries.execute,
      receipt: local.receipt,
      status: reconciler.status
    })
  })
)

const makeLayer = <D extends Definition.Any, E, R,>(
  options: Options<D>,
  reconcilerLayer: Layer.Layer<Reconciler.Reconciler, E, LocalStore.Store | R>
) => {
  const mutationRuntime = MutationRuntime.layer(options.definition, options.evolution)
  const local = LocalStore.layer(options).pipe(Layer.provide(mutationRuntime))
  const queries = QueryExecutor.layer(options.definition)
  const reconciler = reconcilerLayer.pipe(Layer.provide(local))
  return layerFromServices.pipe(
    Layer.provideMerge(local),
    Layer.provideMerge(queries),
    Layer.provideMerge(reconciler)
  )
}

export const layer = <D extends Definition.Any,>(options: Options<D>) => makeLayer(options, Reconciler.layer(options))

export const layerWorkflow = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer(options, ReconciliationWorkflow.layer(options))
