import type * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as IndexStore from "./IndexStore.js"

export type Read =
  | { readonly _tag: "Entity"; readonly key: string }
  | { readonly _tag: "Model"; readonly model: string }
  | { readonly _tag: "Index"; readonly footprint: IndexStore.Footprint }

export interface Changes {
  readonly entityKeys: ReadonlySet<string>
  readonly models: ReadonlySet<string>
  readonly points: ReadonlyArray<IndexStore.Point>
  readonly broadModels: ReadonlySet<string>
}

export interface Registry {
  readonly record: (key: string, reads: ReadonlyArray<Read>) => void
  readonly affected: (changes: Changes) => ReadonlyArray<string>
}

interface InternalRegistry extends Registry {
  readonly delete: (key: string) => void
}

const registries = new WeakMap<Reactivity.Reactivity["Service"], InternalRegistry>()
const liveRegistries = new Set<WeakRef<InternalRegistry>>()
const retained = new Map<string, number>()

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
  if (point.descriptor !== footprint.descriptor || !sameTuple(point.partition, footprint.partition)) return false
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

const readMatches = (read: Read, changes: Changes): boolean => {
  if (read._tag === "Entity") return changes.entityKeys.has(read.key)
  if (read._tag === "Model") return changes.models.has(read.model)
  if (changes.broadModels.has(read.footprint.model)) return true
  return changes.points.some((point) => pointMatches(read.footprint, point))
}

const make = (): InternalRegistry => {
  const readsByKey = new Map<string, ReadonlyArray<Read>>()
  return {
    record: (key, reads) => {
      if (!retained.has(key)) return
      readsByKey.set(key, reads)
    },
    delete: (key) => {
      readsByKey.delete(key)
    },
    affected: (changes) => {
      if (
        changes.entityKeys.size === 0 && changes.models.size === 0 && changes.points.length === 0 &&
        changes.broadModels.size === 0
      ) return []
      const affected: Array<string> = []
      for (const [key, reads] of readsByKey) {
        if (reads.some((read) => readMatches(read, changes))) affected.push(key)
      }
      return affected
    }
  }
}

export const get = (reactivity: Reactivity.Reactivity["Service"]): Registry => {
  const existing = registries.get(reactivity)
  if (existing !== undefined) return existing
  const created = make()
  registries.set(reactivity, created)
  liveRegistries.add(new WeakRef(created))
  return created
}

export const retain = (key: string): () => void => {
  retained.set(key, (retained.get(key) ?? 0) + 1)
  return () => {
    const remaining = (retained.get(key) ?? 1) - 1
    if (remaining > 0) {
      retained.set(key, remaining)
      return
    }
    retained.delete(key)
    for (const reference of liveRegistries) {
      const registry = reference.deref()
      if (registry === undefined) liveRegistries.delete(reference)
      else registry.delete(key)
    }
  }
}
