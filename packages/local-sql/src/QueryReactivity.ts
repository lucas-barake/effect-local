import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import type * as IndexStore from "./IndexStore.js"

export type Read =
  | { readonly _tag: "Entity"; readonly spaceId: Identity.SpaceId; readonly key: string }
  | { readonly _tag: "Index"; readonly footprint: IndexStore.Footprint }

export interface Changes {
  readonly spaceId: Identity.SpaceId
  readonly entityKeys: ReadonlySet<string>
  readonly points: ReadonlyArray<IndexStore.Point>
  readonly broadModels: ReadonlySet<string>
}

export interface Service {
  readonly retain: (key: string) => Effect.Effect<Effect.Effect<void>>
  readonly record: (key: string, reads: ReadonlyArray<Read>) => Effect.Effect<void>
  readonly affected: (changes: Changes) => Effect.Effect<ReadonlyArray<string>>
}

export class QueryReactivity extends Context.Service<QueryReactivity, Service>()(
  "@lucas-barake/effect-local-sql/QueryReactivity"
) {}

const compareValue = (left: string | number, right: string | number): number => {
  if (typeof left === "number" && typeof right === "number") return left - right
  const leftText = String(left)
  const rightText = String(right)
  if (leftText < rightText) return -1
  if (leftText > rightText) return 1
  return 0
}

const compareTuple = (
  left: ReadonlyArray<string | number>,
  right: ReadonlyArray<string | number>
): number => {
  const length = Math.min(left.length, right.length)
  for (let position = 0; position < length; position++) {
    const compared = compareValue(left[position], right[position])
    if (compared !== 0) return compared
  }
  return left.length - right.length
}

const sameTuple = (
  left: ReadonlyArray<string | number>,
  right: ReadonlyArray<string | number>
): boolean => compareTuple(left, right) === 0

const pointMatches = (footprint: IndexStore.Footprint, point: IndexStore.Point): boolean => {
  if (point.spaceId !== footprint.spaceId) return false
  if (!sameTuple(point.partition, footprint.partition)) return false
  const ranged = point.sort[0]
  if (ranged !== undefined) {
    if (footprint.lower !== undefined) {
      const compared = compareValue(ranged, footprint.lower)
      if (compared < 0 || (compared === 0 && !footprint.lowerInclusive)) return false
    }
    if (footprint.upper !== undefined) {
      const compared = compareValue(ranged, footprint.upper)
      if (compared > 0 || (compared === 0 && !footprint.upperInclusive)) return false
    }
  }
  const tuple = [...point.sort, point.entityKey]
  if (footprint.cursor !== undefined) {
    const compared = compareTuple(tuple, footprint.cursor)
    if (footprint.direction === "asc" && compared <= 0) return false
    if (footprint.direction === "desc" && compared >= 0) return false
  }
  if (footprint.boundary !== undefined && (footprint.hasMore || footprint.full)) {
    const compared = compareTuple(tuple, footprint.boundary)
    if (footprint.direction === "asc" && compared > 0) return false
    if (footprint.direction === "desc" && compared < 0) return false
  }
  return true
}

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
        const first = reads[0]
        let spaceId: Identity.SpaceId
        if (first._tag === "Entity") spaceId = first.spaceId
        else spaceId = first.footprint.spaceId
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
        if (
          changes.entityKeys.size === 0 && changes.points.length === 0 && changes.broadModels.size === 0
        ) return []
        const pointsByDescriptor = new Map<string, Array<IndexStore.Point>>()
        for (const point of changes.points) {
          const points = pointsByDescriptor.get(point.descriptor)
          if (points === undefined) pointsByDescriptor.set(point.descriptor, [point])
          else points.push(point)
        }
        const affected: Array<string> = []
        const keys = keysBySpace.get(changes.spaceId)
        if (keys === undefined) return affected
        for (const key of keys) {
          const reads = retained.get(key)?.reads
          if (reads === undefined) continue
          const matches = reads.some((read) => {
            if (read._tag === "Entity") return changes.entityKeys.has(read.key)
            if (changes.broadModels.has(read.footprint.model)) return true
            return pointsByDescriptor.get(read.footprint.descriptor)?.some((point) =>
              pointMatches(read.footprint, point)
            ) ?? false
          })
          if (matches) affected.push(key)
        }
        return affected
      })
  }
}

export const makeLayer = (): EffectLayer.Layer<QueryReactivity> => EffectLayer.sync(QueryReactivity, make)

export const layer: EffectLayer.Layer<QueryReactivity> = makeLayer()
