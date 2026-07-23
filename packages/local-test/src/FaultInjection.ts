import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

export interface Packet {
  readonly sequence: number
  readonly from: Identity.PeerId
  readonly to: Identity.PeerId
  readonly payload: Uint8Array
}

export interface Decision {
  readonly drop: boolean
  readonly copies: number
  readonly delay: Duration.Input
  readonly reorder: boolean
}

export class FaultInjection extends Context.Service<FaultInjection, {
  readonly decide: (packet: Packet) => Effect.Effect<Decision>
}>()(
  "@lucas-barake/effect-local-test/FaultInjection"
) {}

export const layer = (decide: FaultInjection["Service"]["decide"]) => Layer.succeed(FaultInjection, { decide })

export const none = layer(() => Effect.succeed({ drop: false, copies: 1, delay: 0, reorder: false }))

export const layerSequence = (decisions: readonly [Decision, ...ReadonlyArray<Decision>]) =>
  layer((packet) => {
    const index = Math.max(0, Math.min(Math.trunc(packet.sequence), decisions.length - 1))
    return Effect.succeed(decisions[index] ?? decisions[0])
  })
