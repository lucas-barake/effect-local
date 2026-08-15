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

const maximumExactPointsPerGroup = 256

interface PointGroup {
  points: Array<IndexStore.Point> | undefined
  sortMin: string | number | undefined
  sortMax: string | number | undefined
  rangeAlways: boolean
  tupleMin: ReadonlyArray<string | number>
  tupleMax: ReadonlyArray<string | number>
}

const groupKey = (
  spaceId: Identity.SpaceId,
  descriptor: string,
  partition: ReadonlyArray<string | number>
): string => {
  let key = `${spaceId.length}:${spaceId}${descriptor.length}:${descriptor}`
  for (const value of partition) {
    const text = String(value)
    key += `${text.length}:${text}`
  }
  return key
}

const widenGroup = (group: PointGroup, point: IndexStore.Point): void => {
  const ranged = point.sort[0]
  if (ranged === undefined) group.rangeAlways = true
  else {
    if (group.sortMin === undefined || compareValue(ranged, group.sortMin) < 0) group.sortMin = ranged
    if (group.sortMax === undefined || compareValue(ranged, group.sortMax) > 0) group.sortMax = ranged
  }
  const tuple = [...point.sort, point.entityKey]
  if (group.tupleMin.length === 0 || compareTuple(tuple, group.tupleMin) < 0) group.tupleMin = tuple
  if (group.tupleMax.length === 0 || compareTuple(tuple, group.tupleMax) > 0) group.tupleMax = tuple
}

const addToGroup = (group: PointGroup, point: IndexStore.Point): void => {
  if (group.points === undefined) {
    widenGroup(group, point)
    return
  }
  if (group.points.length < maximumExactPointsPerGroup) {
    group.points.push(point)
    return
  }
  for (const existing of group.points) {
    widenGroup(group, existing)
  }
  group.points = undefined
  widenGroup(group, point)
}

const groupMatches = (footprint: IndexStore.Footprint, group: PointGroup): boolean => {
  if (!group.rangeAlways) {
    if (footprint.lower !== undefined && group.sortMax !== undefined) {
      const compared = compareValue(group.sortMax, footprint.lower)
      if (compared < 0 || (compared === 0 && !footprint.lowerInclusive)) return false
    }
    if (footprint.upper !== undefined && group.sortMin !== undefined) {
      const compared = compareValue(group.sortMin, footprint.upper)
      if (compared > 0 || (compared === 0 && !footprint.upperInclusive)) return false
    }
  }
  if (footprint.cursor !== undefined) {
    if (footprint.direction === "asc" && compareTuple(group.tupleMax, footprint.cursor) <= 0) return false
    if (footprint.direction === "desc" && compareTuple(group.tupleMin, footprint.cursor) >= 0) return false
  }
  if (footprint.boundary !== undefined && (footprint.hasMore || footprint.full)) {
    if (footprint.direction === "asc" && compareTuple(group.tupleMin, footprint.boundary) > 0) return false
    if (footprint.direction === "desc" && compareTuple(group.tupleMax, footprint.boundary) < 0) return false
  }
  return true
}

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
  const keyByFootprint = new WeakMap<IndexStore.Footprint, string>()
  const footprintGroupKey = (footprint: IndexStore.Footprint): string => {
    const cached = keyByFootprint.get(footprint)
    if (cached !== undefined) return cached
    const key = groupKey(footprint.spaceId, footprint.descriptor, footprint.partition)
    keyByFootprint.set(footprint, key)
    return key
  }
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
        if (changes.entityKeys.size === 0 && changes.points.length === 0) return []
        const groups = new Map<string, PointGroup>()
        for (const point of changes.points) {
          const key = groupKey(point.spaceId, point.descriptor, point.partition)
          const group = groups.get(key)
          if (group === undefined) {
            groups.set(key, {
              points: [point],
              sortMin: undefined,
              sortMax: undefined,
              rangeAlways: false,
              tupleMin: [],
              tupleMax: []
            })
          } else addToGroup(group, point)
        }
        const affected: Array<string> = []
        const keys = keysBySpace.get(changes.spaceId)
        if (keys === undefined) return affected
        for (const key of keys) {
          const reads = retained.get(key)?.reads
          if (reads === undefined) continue
          const matches = reads.some((read) => {
            if (read._tag === "Entity") return changes.entityKeys.has(read.key)
            const group = groups.get(footprintGroupKey(read.footprint))
            if (group === undefined) return false
            if (group.points === undefined) return groupMatches(read.footprint, group)
            return group.points.some((point) => pointMatches(read.footprint, point))
          })
          if (matches) affected.push(key)
        }
        return affected
      })
  }
}

export const makeLayer = (): EffectLayer.Layer<QueryReactivity> => EffectLayer.sync(QueryReactivity, make)

export const layer: EffectLayer.Layer<QueryReactivity> = makeLayer()
