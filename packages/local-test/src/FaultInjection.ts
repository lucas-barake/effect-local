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
  readonly state: (spaceId: Identity.SpaceId) => Effect.Effect<State>
  readonly partition: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly heal: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly dropNextReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly duplicateNextPage: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly takeDroppedReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly takeDuplicatePage: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
}

export class FaultInjection extends Context.Service<FaultInjection, Service>()(
  "@lucas-barake/effect-local-test/FaultInjection"
) {}

export const layer: Layer.Layer<FaultInjection> = Layer.effect(
  FaultInjection,
  Effect.gen(function*() {
    const initial = (): State => ({ online: true, dropNextReceipt: false, duplicateNextPage: false })
    const state = yield* Ref.make(new Map<Identity.SpaceId, State>())
    const get = (spaceId: Identity.SpaceId) =>
      Ref.get(state).pipe(Effect.map((spaces) => spaces.get(spaceId) ?? initial()))
    const set = (spaceId: Identity.SpaceId, patch: Partial<State>) =>
      Ref.update(state, (spaces) => {
        const next = new Map(spaces)
        next.set(spaceId, { ...(spaces.get(spaceId) ?? initial()), ...patch })
        return next
      })
    const take = (spaceId: Identity.SpaceId, key: "dropNextReceipt" | "duplicateNextPage") =>
      Ref.modify(state, (spaces) => {
        const current = spaces.get(spaceId) ?? initial()
        const next = new Map(spaces)
        next.set(spaceId, { ...current, [key]: false })
        return [current[key], next]
      })
    return FaultInjection.of({
      state: get,
      partition: (spaceId) => set(spaceId, { online: false }),
      heal: (spaceId) => set(spaceId, { online: true }),
      dropNextReceipt: (spaceId) => set(spaceId, { dropNextReceipt: true }),
      duplicateNextPage: (spaceId) => set(spaceId, { duplicateNextPage: true }),
      takeDroppedReceipt: (spaceId) => take(spaceId, "dropNextReceipt"),
      takeDuplicatePage: (spaceId) => take(spaceId, "duplicateNextPage")
    })
  })
)
import type * as Identity from "@lucas-barake/effect-local/Identity"
