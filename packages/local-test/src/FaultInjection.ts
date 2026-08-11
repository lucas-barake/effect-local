import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"

export interface State {
  readonly online: boolean
  readonly dropNextReceipt: boolean
  readonly duplicateNextPage: boolean
}

export interface Service {
  readonly state: Effect.Effect<State>
  readonly partition: Effect.Effect<void>
  readonly heal: Effect.Effect<void>
  readonly dropNextReceipt: Effect.Effect<void>
  readonly duplicateNextPage: Effect.Effect<void>
  readonly takeDroppedReceipt: Effect.Effect<boolean>
  readonly takeDuplicatePage: Effect.Effect<boolean>
}

export class FaultInjection extends Context.Service<FaultInjection, Service>()(
  "@lucas-barake/effect-local-test/FaultInjection"
) {}

export const layer: Layer.Layer<FaultInjection> = Layer.effect(
  FaultInjection,
  Effect.gen(function*() {
    const state = yield* Ref.make<State>({ online: true, dropNextReceipt: false, duplicateNextPage: false })
    const set = (patch: Partial<State>) => Ref.update(state, (current) => ({ ...current, ...patch }))
    const take = (key: "dropNextReceipt" | "duplicateNextPage") =>
      Ref.modify(state, (current) => [
        current[key],
        { ...current, [key]: false }
      ])
    return FaultInjection.of({
      state: Ref.get(state),
      partition: set({ online: false }),
      heal: set({ online: true }),
      dropNextReceipt: set({ dropNextReceipt: true }),
      duplicateNextPage: set({ duplicateNextPage: true }),
      takeDroppedReceipt: take("dropNextReceipt"),
      takeDuplicatePage: take("duplicateNextPage")
    })
  })
)
