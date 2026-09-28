import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"

export interface Service {
  readonly generation: Effect.Effect<number>
  readonly waitForChange: (observedGeneration: number) => Effect.Effect<void>
}

export class Transport extends Context.Service<Transport, Service>()("@lucas-barake/effect-local-rpc/Transport") {}
