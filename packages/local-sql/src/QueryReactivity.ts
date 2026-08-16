import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"

export type Read =
  | { readonly _tag: "Entity"; readonly spaceId: Identity.SpaceId; readonly key: string }
  | { readonly _tag: "Model"; readonly spaceId: Identity.SpaceId; readonly model: string }

export interface Changes {
  readonly spaceId: Identity.SpaceId
  readonly entityKeys: ReadonlySet<string>
  readonly models: ReadonlySet<string>
}

export interface Service {
  readonly retain: (key: string) => Effect.Effect<Effect.Effect<void>>
  readonly record: (key: string, reads: ReadonlyArray<Read>) => Effect.Effect<void>
  readonly affected: (changes: Changes) => Effect.Effect<ReadonlyArray<string>>
}

export class QueryReactivity extends Context.Service<QueryReactivity, Service>()(
  "@lucas-barake/effect-local-sql/QueryReactivity"
) {}

const make = (): Service => {
  const retained = new Map<string, {
    count: number
    spaceId?: Identity.SpaceId
    reads?: ReadonlyArray<Read>
  }>()
  const keysBySpace = new Map<Identity.SpaceId, Set<string>>()
  return {
    retain: (key) => {
      const cleanup = Effect.sync(() => {
        const current = retained.get(key)
        if (current === undefined) return
        current.count--
        if (current.count > 0) return
        retained.delete(key)
        const spaceId = current.spaceId
        if (spaceId === undefined) return
        const keys = keysBySpace.get(spaceId)
        if (keys === undefined) return
        keys.delete(key)
        if (keys.size === 0) keysBySpace.delete(spaceId)
      })
      return Effect.sync(() => {
        const retainedQuery = retained.get(key)
        if (retainedQuery === undefined) retained.set(key, { count: 1 })
        else retainedQuery.count++
      }).pipe(Effect.as(cleanup))
    },
    record: (key, reads) =>
      Effect.sync(() => {
        const retainedQuery = retained.get(key)
        if (retainedQuery === undefined || reads.length === 0) return
        const spaceId = reads[0].spaceId
        const previousSpaceId = retainedQuery.spaceId
        if (previousSpaceId !== undefined && previousSpaceId !== spaceId) {
          const previous = keysBySpace.get(previousSpaceId)
          previous?.delete(key)
          if (previous?.size === 0) keysBySpace.delete(previousSpaceId)
        }
        retainedQuery.spaceId = spaceId
        retainedQuery.reads = reads
        const keys = keysBySpace.get(spaceId)
        if (keys === undefined) keysBySpace.set(spaceId, new Set([key]))
        else keys.add(key)
      }),
    affected: (changes) =>
      Effect.sync(() => {
        if (changes.entityKeys.size === 0 && changes.models.size === 0) return []
        const affected: Array<string> = []
        const keys = keysBySpace.get(changes.spaceId)
        if (keys === undefined) return affected
        for (const key of keys) {
          const reads = retained.get(key)?.reads
          if (reads === undefined) continue
          const matches = reads.some((read) => {
            if (read._tag === "Entity") return changes.entityKeys.has(read.key)
            return changes.models.has(read.model)
          })
          if (matches) affected.push(key)
        }
        return affected
      })
  }
}

export const makeLayer = (): EffectLayer.Layer<QueryReactivity> => EffectLayer.sync(QueryReactivity, make)

export const layer: EffectLayer.Layer<QueryReactivity> = makeLayer()
