import * as Automerge from "@automerge/automerge/slim"
import * as DateTime from "effect/DateTime"
import * as Encoding from "effect/Encoding"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as SchemaTransformation from "effect/SchemaTransformation"
import type * as Document from "./Document.js"
import * as Identity from "./Identity.js"
import {
  maxConflictAlternativesHardLimit,
  maxConflictDepthHardLimit,
  maxConflictNodesHardLimit,
  maxConflictPathSegmentsHardLimit,
  maxConflictValueBytesHardLimit
} from "./ReplicaLimits.js"
import type * as Snapshot from "./Snapshot.js"

const nonNegativeSafeInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const prototypeKeys = new Set([...Object.getOwnPropertyNames(Object.prototype), "prototype"])
const textEncoder = new TextEncoder()
const JsonString = Schema.fromJsonString(Schema.Unknown)

export const isSupportedKey = (key: string): boolean => !prototypeKeys.has(key)

export const compareCodeUnits = (left: string, right: string): number => {
  if (left < right) return -1
  if (left > right) return 1
  return 0
}

const ConflictKey = Schema.String.check(
  Schema.makeFilter(isSupportedKey, { expected: "a key that is safe for Automerge object traversal" })
)

export const AlternativeId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@lucas-barake/effect-local/Conflict/AlternativeId")
)
export type AlternativeId = typeof AlternativeId.Type

export const maxHeadsHardLimit = 1_024
export const maxHeadBytesHardLimit = 64 * 1_024
export const maxHeadsBytesHardLimit = 64 * 1_024

const utf8Bytes = (value: string): number => textEncoder.encode(value).byteLength

export const Head = Schema.String.check(
  Schema.makeFilter(
    (head) => utf8Bytes(head) <= maxHeadBytesHardLimit,
    { expected: `a head using at most ${maxHeadBytesHardLimit} UTF8 bytes` }
  )
)
export type Head = typeof Head.Type

export const normalizeHeads = (heads: ReadonlyArray<Head>): ReadonlyArray<Head> => [...new Set(heads)].toSorted()

const BoundedHeads = Schema.Array(Head).check(
  Schema.isMaxLength(maxHeadsHardLimit),
  Schema.makeFilter(
    (heads) => {
      let bytes = 0
      for (const head of heads) {
        bytes += utf8Bytes(head)
        if (bytes > maxHeadsBytesHardLimit) return false
      }
      return true
    },
    { expected: `heads using at most ${maxHeadsBytesHardLimit} aggregate UTF8 bytes` }
  )
)

export const Heads = BoundedHeads.pipe(
  Schema.decode(
    SchemaTransformation.transform({
      decode: normalizeHeads,
      encode: normalizeHeads
    })
  )
)
export type Heads = typeof Heads.Type

export const ParentSegment = Schema.TaggedUnion({
  Key: {
    key: ConflictKey,
    alternative: Schema.optionalKey(AlternativeId)
  },
  Index: {
    index: nonNegativeSafeInteger,
    alternative: Schema.optionalKey(AlternativeId)
  }
})
export type ParentSegment = typeof ParentSegment.Type

export const TargetSegment = Schema.TaggedUnion({
  Key: { key: ConflictKey },
  Index: { index: nonNegativeSafeInteger }
})
export type TargetSegment = typeof TargetSegment.Type

export const Path = Schema.Struct({
  parents: Schema.Array(ParentSegment),
  target: TargetSegment
}).check(
  Schema.makeFilter(
    (path) => path.parents.length + 1 <= maxConflictPathSegmentsHardLimit,
    { expected: `a conflict path with at most ${maxConflictPathSegmentsHardLimit} segments` }
  )
)
export type Path = typeof Path.Type

export interface NullValue {
  readonly _tag: "Null"
}

export interface BooleanValue {
  readonly _tag: "Boolean"
  readonly value: boolean
}

export interface NumberValue {
  readonly _tag: "Number"
  readonly value: number
}

export interface TextValue {
  readonly _tag: "Text"
  readonly value: string
}

export interface ImmutableStringValue {
  readonly _tag: "ImmutableString"
  readonly value: string
}

export interface DateValue {
  readonly _tag: "Date"
  readonly value: string
}

export interface BytesValue {
  readonly _tag: "Bytes"
  readonly value: string
}

export interface CounterValue {
  readonly _tag: "Counter"
  readonly value: number
}

export interface MapEntry {
  readonly key: string
  readonly value: PortableValue
}

export interface MapValue {
  readonly _tag: "Map"
  readonly entries: ReadonlyArray<MapEntry>
}

export interface ListValue {
  readonly _tag: "List"
  readonly values: ReadonlyArray<PortableValue>
}

export type PortableValue =
  | NullValue
  | BooleanValue
  | NumberValue
  | TextValue
  | ImmutableStringValue
  | DateValue
  | BytesValue
  | CounterValue
  | MapValue
  | ListValue

export interface PreflightLimits {
  readonly maxConflictDepth: number
  readonly maxConflictNodes: number
  readonly maxConflictAlternatives: number
  readonly maxConflictPathSegments: number
  readonly maxConflictValueBytes: number
}

export const hardPreflightLimits: PreflightLimits = {
  maxConflictDepth: maxConflictDepthHardLimit,
  maxConflictNodes: maxConflictNodesHardLimit,
  maxConflictAlternatives: maxConflictAlternativesHardLimit,
  maxConflictPathSegments: maxConflictPathSegmentsHardLimit,
  maxConflictValueBytes: maxConflictValueBytesHardLimit
}

export type PreflightIssue =
  | {
    readonly _tag: "UnsupportedValue"
    readonly pathDepth: number
    readonly kind: string
  }
  | {
    readonly _tag: "UnsupportedKey"
    readonly pathDepth: number
  }
  | {
    readonly _tag: "Cycle"
    readonly pathDepth: number
  }
  | {
    readonly _tag: "Depth"
    readonly limit: number
  }
  | {
    readonly _tag: "Nodes"
    readonly limit: number
  }
  | {
    readonly _tag: "Alternatives"
    readonly limit: number
  }
  | {
    readonly _tag: "PathSegments"
    readonly limit: number
  }
  | {
    readonly _tag: "Bytes"
    readonly limit: number
  }

const unsupportedKind = (value: unknown): string => {
  if (value === null) return "null"
  if (Array.isArray(value)) return "array"
  if (value instanceof Date) return "date"
  if (value instanceof Uint8Array) return "bytes"
  return typeof value
}

const jsonByteBudgetState: unique symbol = Symbol()

export interface JsonByteBudget {
  readonly limit: number
  readonly [jsonByteBudgetState]: {
    bytes: number
    readonly limit: number
  }
}

export const createJsonByteBudget = (limit: number): JsonByteBudget => ({
  limit,
  [jsonByteBudgetState]: { bytes: 0, limit }
})

export const addJsonBytes = (budget: JsonByteBudget, bytes: number): boolean => {
  const state = budget[jsonByteBudgetState]
  state.bytes += bytes
  return state.bytes > state.limit
}

export const addJsonStringBytes = (budget: JsonByteBudget, value: string): boolean => {
  if (addJsonBytes(budget, 2)) return true
  for (let index = 0; index < value.length; index++) {
    const codeUnit = value.charCodeAt(index)
    let bytes: number
    if (codeUnit === 0x22 || codeUnit === 0x5c) {
      bytes = 2
    } else if (codeUnit <= 0x1f) {
      bytes = 6
      if (
        codeUnit === 0x08 ||
        codeUnit === 0x09 ||
        codeUnit === 0x0a ||
        codeUnit === 0x0c ||
        codeUnit === 0x0d
      ) {
        bytes = 2
      }
    } else if (codeUnit <= 0x7f) {
      bytes = 1
    } else if (codeUnit <= 0x7ff) {
      bytes = 2
    } else if (codeUnit >= 0xd800 && codeUnit <= 0xdfff) {
      if (value.codePointAt(index)! > 0xffff) {
        bytes = 4
        index++
      } else {
        bytes = 6
      }
    } else {
      bytes = 3
    }
    if (addJsonBytes(budget, bytes)) return true
  }
  return false
}

export const jsonByteBudgetRemaining = (budget: JsonByteBudget): number => {
  const state = budget[jsonByteBudgetState]
  return Math.max(0, state.limit - state.bytes)
}

export const jsonByteBudgetExceeded = (budget: JsonByteBudget): boolean => {
  const state = budget[jsonByteBudgetState]
  return state.bytes > state.limit
}

const bytesIssue = (limit: number): PreflightIssue => ({ _tag: "Bytes", limit })

const denseArrayValues = (value: ReadonlyArray<unknown>): ReadonlyArray<unknown> | undefined => {
  const values = Array.from<unknown>({ length: value.length })
  for (let index = 0; index < value.length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, index)
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) return undefined
    values[index] = descriptor.value
  }
  return values
}

type BoundedArrayValues =
  | { readonly _tag: "Values"; readonly values: ReadonlyArray<unknown> }
  | { readonly _tag: "Sparse" }
  | { readonly _tag: "Nodes" }

const boundedDenseArrayValues = (
  value: ReadonlyArray<unknown>,
  remainingNodes: number
): BoundedArrayValues => {
  const inspected = Math.min(value.length, Math.max(0, remainingNodes))
  const values: Array<unknown> = []
  for (let index = 0; index < inspected; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, index)
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return { _tag: "Sparse" }
    }
    values.push(descriptor.value)
  }
  if (value.length > remainingNodes) return { _tag: "Nodes" }
  return { _tag: "Values", values }
}

type BoundedObjectEntries =
  | {
    readonly _tag: "Entries"
    readonly entries: ReadonlyArray<readonly [string, unknown]>
  }
  | { readonly _tag: "Unsupported" }
  | { readonly _tag: "Nodes" }

const boundedObjectEntries = (
  value: object,
  remainingNodes: number
): BoundedObjectEntries => {
  const keys = Reflect.ownKeys(value)
  const inspected = Math.min(keys.length, Math.max(0, remainingNodes))
  const entries: Array<readonly [string, unknown]> = []
  for (let index = 0; index < inspected; index++) {
    const key = keys[index]
    if (typeof key !== "string") return { _tag: "Unsupported" }
    const descriptor = Object.getOwnPropertyDescriptor(value, key)
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return { _tag: "Unsupported" }
    }
    entries.push([key, descriptor.value])
  }
  if (keys.length > remainingNodes) return { _tag: "Nodes" }
  return { _tag: "Entries", entries }
}

const isPlainObject = (value: object): value is { readonly [key: string]: unknown } => {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

const hasOnlyDataProperties = (value: object): boolean =>
  Object.values(Object.getOwnPropertyDescriptors(value)).every(
    (descriptor) => "value" in descriptor && descriptor.enumerable
  )

const isValidPreflightLimit = (value: number, maximum: number): boolean =>
  Number.isSafeInteger(value) && value > 0 && value <= maximum

const valueLimitsIssue = (
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes">
): PreflightIssue | undefined => {
  if (!isValidPreflightLimit(limits.maxConflictDepth, maxConflictDepthHardLimit)) {
    return { _tag: "Depth", limit: maxConflictDepthHardLimit }
  }
  if (!isValidPreflightLimit(limits.maxConflictNodes, maxConflictNodesHardLimit)) {
    return { _tag: "Nodes", limit: maxConflictNodesHardLimit }
  }
  if (!isValidPreflightLimit(limits.maxConflictValueBytes, maxConflictValueBytesHardLimit)) {
    return { _tag: "Bytes", limit: maxConflictValueBytesHardLimit }
  }
  return undefined
}

const preflightLimitsIssue = (limits: PreflightLimits): PreflightIssue | undefined => {
  const valueIssue = valueLimitsIssue(limits)
  if (valueIssue !== undefined) return valueIssue
  if (!isValidPreflightLimit(limits.maxConflictAlternatives, maxConflictAlternativesHardLimit)) {
    return { _tag: "Alternatives", limit: maxConflictAlternativesHardLimit }
  }
  if (!isValidPreflightLimit(limits.maxConflictPathSegments, maxConflictPathSegmentsHardLimit)) {
    return { _tag: "PathSegments", limit: maxConflictPathSegmentsHardLimit }
  }
  return undefined
}

const nativeValueIssue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes">
): PreflightIssue | undefined => {
  const seen = new WeakSet<object>()
  const stack: Array<{ readonly value: unknown; readonly depth: number }> = [{ value: input, depth: 0 }]
  const budget = createJsonByteBudget(limits.maxConflictValueBytes)
  let nodes = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }

    if (value === null) {
      if (addJsonBytes(budget, `{"_tag":"Null"}`.length)) return bytesIssue(budget.limit)
      continue
    }
    if (typeof value === "string") {
      if (
        addJsonBytes(budget, `{"_tag":"Text","value":`.length) ||
        addJsonStringBytes(budget, value) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (typeof value === "boolean") {
      let booleanBytes = 5
      if (value) booleanBytes = 4
      if (
        addJsonBytes(budget, `{"_tag":"Boolean","value":`.length) ||
        addJsonBytes(budget, booleanBytes) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value) || value >= Number.MAX_SAFE_INTEGER) {
        let kind = "nonfinite number"
        if (Number.isFinite(value)) kind = "nonportable number"
        return {
          _tag: "UnsupportedValue",
          pathDepth: depth,
          kind
        }
      }
      if (
        addJsonBytes(budget, `{"_tag":"Number","value":`.length) ||
        addJsonBytes(budget, String(value).length) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (typeof value !== "object") {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    const array = Array.isArray(value)
    if (array || isPlainObject(value)) {
      if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
      seen.add(value)
      const remainingNodes = limits.maxConflictNodes - nodes - stack.length

      if (array) {
        const bounded = boundedDenseArrayValues(value, remainingNodes)
        if (bounded._tag === "Nodes") {
          return { _tag: "Nodes", limit: limits.maxConflictNodes }
        }
        if (bounded._tag === "Sparse") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "sparse array" }
        }
        const values = bounded.values
        if (
          addJsonBytes(budget, `{"_tag":"List","values":[`.length) ||
          addJsonBytes(budget, Math.max(0, values.length - 1)) ||
          addJsonBytes(budget, 2)
        ) {
          return bytesIssue(budget.limit)
        }
        for (let index = values.length - 1; index >= 0; index--) {
          stack.push({ value: values[index], depth: depth + 1 })
        }
        continue
      }

      const bounded = boundedObjectEntries(value, remainingNodes)
      if (bounded._tag === "Nodes") {
        return { _tag: "Nodes", limit: limits.maxConflictNodes }
      }
      if (bounded._tag === "Unsupported") {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
      }
      const entries = bounded.entries
      if (
        addJsonBytes(budget, `{"_tag":"Map","entries":[`.length) ||
        addJsonBytes(budget, Math.max(0, entries.length - 1)) ||
        addJsonBytes(budget, 2)
      ) {
        return bytesIssue(budget.limit)
      }
      for (const [key] of entries) {
        if (!isSupportedKey(key)) return { _tag: "UnsupportedKey", pathDepth: depth + 1 }
        if (
          addJsonBytes(budget, `{"key":`.length) ||
          addJsonStringBytes(budget, key) ||
          addJsonBytes(budget, `,"value":`.length + 1)
        ) {
          return bytesIssue(budget.limit)
        }
      }
      for (let index = entries.length - 1; index >= 0; index--) {
        const [, child] = entries[index]
        stack.push({ value: child, depth: depth + 1 })
      }
      continue
    }
    if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "invalid date" }
      }
      if (
        addJsonBytes(budget, `{"_tag":"Date","value":`.length) ||
        addJsonStringBytes(budget, value.toISOString()) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (Automerge.isCounter(value)) {
      if (!Number.isSafeInteger(value.value) || value.value >= Number.MAX_SAFE_INTEGER) {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "unsafe counter" }
      }
      if (
        addJsonBytes(budget, `{"_tag":"Counter","value":`.length) ||
        addJsonBytes(budget, String(value.value).length) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (Automerge.isImmutableString(value)) {
      if (
        addJsonBytes(budget, `{"_tag":"ImmutableString","value":`.length) ||
        addJsonStringBytes(budget, value.val) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (value instanceof Uint8Array) {
      const base64Bytes = 4 * Math.ceil(value.byteLength / 3)
      if (
        addJsonBytes(budget, `{"_tag":"Bytes","value":`.length) ||
        addJsonBytes(budget, base64Bytes + 2) ||
        addJsonBytes(budget, 1)
      ) {
        return bytesIssue(budget.limit)
      }
      continue
    }
    if (Automerge.isAutomerge(value)) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Automerge document" }
    }
    return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
  }
  return undefined
}

const exactKeys = (value: { readonly [key: string]: unknown }, expected: ReadonlyArray<string>): boolean => {
  const actual = Object.keys(value).toSorted()
  return actual.length === expected.length && actual.every((key, index) => key === expected[index])
}

const portableValueIssue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes">
): PreflightIssue | undefined => {
  const seen = new WeakSet<object>()
  const stack: Array<{ readonly value: unknown; readonly depth: number }> = [{ value: input, depth: 0 }]
  const budget = createJsonByteBudget(limits.maxConflictValueBytes)
  let nodes = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }
    if (typeof value !== "object" || value === null || Array.isArray(value) || !isPlainObject(value)) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    if (!hasOnlyDataProperties(value) || Object.getOwnPropertySymbols(value).length > 0) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
    }
    if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
    seen.add(value)
    const record = value
    const tag = record._tag
    const remainingNodes = limits.maxConflictNodes - nodes - stack.length

    switch (tag) {
      case "Null":
        if (!exactKeys(record, ["_tag"])) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Null" }
        }
        if (addJsonBytes(budget, `{"_tag":"Null"}`.length)) return bytesIssue(budget.limit)
        break
      case "Boolean":
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "boolean") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Boolean" }
        }
        let booleanBytes = 5
        if (record.value) booleanBytes = 4
        if (
          addJsonBytes(budget, `{"_tag":"Boolean","value":`.length) ||
          addJsonBytes(budget, booleanBytes) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        break
      case "Number":
        if (
          !exactKeys(record, ["_tag", "value"]) ||
          typeof record.value !== "number" ||
          !Number.isFinite(record.value) ||
          record.value >= Number.MAX_SAFE_INTEGER
        ) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Number" }
        }
        if (
          addJsonBytes(budget, `{"_tag":"Number","value":`.length) ||
          addJsonBytes(budget, String(record.value).length) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        break
      case "Text":
      case "ImmutableString":
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: tag }
        }
        if (
          addJsonBytes(budget, `{"_tag":"${tag}","value":`.length) ||
          addJsonStringBytes(budget, record.value) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        break
      case "Date": {
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Date" }
        }
        if (
          addJsonBytes(budget, `{"_tag":"Date","value":`.length) ||
          addJsonStringBytes(budget, record.value) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        const date = DateTime.make(record.value)
        if (Option.isNone(date) || DateTime.formatIso(date.value) !== record.value) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Date" }
        }
        break
      }
      case "Bytes": {
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Bytes" }
        }
        if (
          addJsonBytes(budget, `{"_tag":"Bytes","value":`.length) ||
          addJsonStringBytes(budget, record.value) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        const decoded = Encoding.decodeBase64(record.value)
        if (Result.isFailure(decoded) || Encoding.encodeBase64(decoded.success) !== record.value) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Bytes" }
        }
        break
      }
      case "Counter":
        if (
          !exactKeys(record, ["_tag", "value"]) ||
          typeof record.value !== "number" ||
          !Number.isSafeInteger(record.value) ||
          record.value >= Number.MAX_SAFE_INTEGER
        ) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Counter" }
        }
        if (
          addJsonBytes(budget, `{"_tag":"Counter","value":`.length) ||
          addJsonBytes(budget, String(record.value).length) ||
          addJsonBytes(budget, 1)
        ) {
          return bytesIssue(budget.limit)
        }
        break
      case "Map": {
        const entries = record.entries
        if (!exactKeys(record, ["_tag", "entries"]) || !Array.isArray(entries)) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Map" }
        }
        const bounded = boundedDenseArrayValues(entries, remainingNodes)
        if (bounded._tag === "Nodes") {
          return { _tag: "Nodes", limit: limits.maxConflictNodes }
        }
        if (bounded._tag === "Sparse") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Map" }
        }
        const entryValues = bounded.values
        if (
          addJsonBytes(budget, `{"_tag":"Map","entries":[`.length) ||
          addJsonBytes(budget, Math.max(0, entryValues.length - 1)) ||
          addJsonBytes(budget, 2)
        ) {
          return bytesIssue(budget.limit)
        }
        const keys = new Set<string>()
        for (const entry of entryValues) {
          if (
            typeof entry !== "object" ||
            entry === null ||
            Array.isArray(entry) ||
            !isPlainObject(entry) ||
            !hasOnlyDataProperties(entry) ||
            Object.getOwnPropertySymbols(entry).length > 0 ||
            !exactKeys(entry, ["key", "value"]) ||
            typeof entry.key !== "string"
          ) {
            return { _tag: "UnsupportedValue", pathDepth: depth + 1, kind: "Map entry" }
          }
          if (!isSupportedKey(entry.key)) return { _tag: "UnsupportedKey", pathDepth: depth + 1 }
          if (keys.has(entry.key)) {
            return { _tag: "UnsupportedValue", pathDepth: depth + 1, kind: "duplicate map key" }
          }
          keys.add(entry.key)
          if (
            addJsonBytes(budget, `{"key":`.length) ||
            addJsonStringBytes(budget, entry.key) ||
            addJsonBytes(budget, `,"value":`.length + 1)
          ) {
            return bytesIssue(budget.limit)
          }
        }
        for (let index = entryValues.length - 1; index >= 0; index--) {
          const entry = entryValues[index]
          if (
            typeof entry !== "object" ||
            entry === null ||
            Array.isArray(entry) ||
            !isPlainObject(entry) ||
            !("value" in entry)
          ) {
            return { _tag: "UnsupportedValue", pathDepth: depth + 1, kind: "Map entry" }
          }
          stack.push({ value: entry.value, depth: depth + 1 })
        }
        break
      }
      case "List": {
        const values = record.values
        if (!exactKeys(record, ["_tag", "values"]) || !Array.isArray(values)) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "List" }
        }
        const bounded = boundedDenseArrayValues(values, remainingNodes)
        if (bounded._tag === "Nodes") {
          return { _tag: "Nodes", limit: limits.maxConflictNodes }
        }
        if (bounded._tag === "Sparse") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "List" }
        }
        const childValues = bounded.values
        if (
          addJsonBytes(budget, `{"_tag":"List","values":[`.length) ||
          addJsonBytes(budget, Math.max(0, childValues.length - 1)) ||
          addJsonBytes(budget, 2)
        ) {
          return bytesIssue(budget.limit)
        }
        for (let index = childValues.length - 1; index >= 0; index--) {
          stack.push({ value: childValues[index], depth: depth + 1 })
        }
        break
      }
      default:
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "unknown tag" }
    }
  }
  return undefined
}

export const preflightNativeValue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes"> = hardPreflightLimits
): PreflightIssue | undefined => valueLimitsIssue(limits) ?? nativeValueIssue(input, limits)

export const preflightPortableValue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes"> = hardPreflightLimits
): PreflightIssue | undefined => valueLimitsIssue(limits) ?? portableValueIssue(input, limits)

const isPathSegmentShape = (value: unknown, parent: boolean): boolean => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    !isPlainObject(value) ||
    !hasOnlyDataProperties(value) ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    return false
  }
  const record = value
  if (record._tag === "Key") {
    if (typeof record.key !== "string") return false
    const expected = ["_tag", "key"]
    if (parent && record.alternative !== undefined) expected.splice(1, 0, "alternative")
    return exactKeys(record, expected)
  }
  if (record._tag === "Index") {
    if (!Number.isSafeInteger(record.index)) return false
    const expected = ["_tag", "index"]
    if (parent && record.alternative !== undefined) expected.splice(1, 0, "alternative")
    return exactKeys(record, expected)
  }
  return false
}

const pathSegments = (value: unknown): number | undefined => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    !isPlainObject(value) ||
    !hasOnlyDataProperties(value) ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    return undefined
  }
  const record = value
  if (!exactKeys(record, ["parents", "target"]) || !Array.isArray(record.parents)) return undefined
  const parents = denseArrayValues(record.parents)
  if (
    parents === undefined ||
    !parents.every((segment) => isPathSegmentShape(segment, true)) ||
    !isPathSegmentShape(record.target, false)
  ) {
    return undefined
  }
  return parents.length + 1
}

const alternativeCount = (value: { readonly [key: string]: unknown }): number | undefined => {
  if (
    !exactKeys(value, ["alternatives", "path", "visible"]) ||
    typeof value.visible !== "string" ||
    pathSegments(value.path) === undefined ||
    !Array.isArray(value.alternatives) ||
    denseArrayValues(value.alternatives) === undefined
  ) {
    return undefined
  }
  return value.alternatives.length
}

export const preflightUnknown = (
  input: unknown,
  limits: PreflightLimits = hardPreflightLimits
): PreflightIssue | undefined => {
  const limitsIssue = preflightLimitsIssue(limits)
  if (limitsIssue !== undefined) return limitsIssue
  const seen = new WeakSet<object>()
  const stack: Array<{ readonly value: unknown; readonly depth: number }> = [{ value: input, depth: 0 }]
  const budget = createJsonByteBudget(limits.maxConflictValueBytes)
  let nodes = 0
  let alternatives = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }
    if (value === null) {
      if (addJsonBytes(budget, 4)) return bytesIssue(budget.limit)
      continue
    }
    if (typeof value === "string") {
      if (addJsonStringBytes(budget, value)) return bytesIssue(budget.limit)
      continue
    }
    if (typeof value === "boolean") {
      let booleanBytes = 5
      if (value) booleanBytes = 4
      if (addJsonBytes(budget, booleanBytes)) return bytesIssue(budget.limit)
      continue
    }
    if (typeof value === "number") {
      if (Number.isFinite(value)) {
        if (addJsonBytes(budget, String(value).length)) return bytesIssue(budget.limit)
        continue
      }
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "nonfinite number" }
    }
    if (typeof value !== "object") {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
    seen.add(value)
    const remainingNodes = limits.maxConflictNodes - nodes - stack.length
    if (Array.isArray(value)) {
      const bounded = boundedDenseArrayValues(value, remainingNodes)
      if (bounded._tag === "Nodes") {
        return { _tag: "Nodes", limit: limits.maxConflictNodes }
      }
      if (bounded._tag === "Sparse") {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "sparse array" }
      }
      const values = bounded.values
      if (
        addJsonBytes(budget, 2) ||
        addJsonBytes(budget, Math.max(0, values.length - 1))
      ) {
        return bytesIssue(budget.limit)
      }
      for (let index = values.length - 1; index >= 0; index--) {
        stack.push({ value: values[index], depth: depth + 1 })
      }
      continue
    }
    if (!isPlainObject(value)) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
    }
    const bounded = boundedObjectEntries(value, remainingNodes)
    if (bounded._tag === "Nodes") {
      return { _tag: "Nodes", limit: limits.maxConflictNodes }
    }
    if (bounded._tag === "Unsupported") {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
    }
    const entries = bounded.entries
    const record = value
    const recordAlternatives = alternativeCount(record)
    if (recordAlternatives !== undefined) {
      alternatives += recordAlternatives
      if (alternatives > limits.maxConflictAlternatives) {
        return { _tag: "Alternatives", limit: limits.maxConflictAlternatives }
      }
    }
    const segments = pathSegments(record)
    if (segments !== undefined) {
      if (segments > limits.maxConflictPathSegments) {
        return { _tag: "PathSegments", limit: limits.maxConflictPathSegments }
      }
    }
    if (
      addJsonBytes(budget, 2) ||
      addJsonBytes(budget, Math.max(0, entries.length - 1))
    ) {
      return bytesIssue(budget.limit)
    }
    for (const [key] of entries) {
      if (!isSupportedKey(key)) return { _tag: "UnsupportedKey", pathDepth: depth + 1 }
      if (addJsonStringBytes(budget, key) || addJsonBytes(budget, 1)) {
        return bytesIssue(budget.limit)
      }
    }
    for (let index = entries.length - 1; index >= 0; index--) {
      const [, child] = entries[index]
      stack.push({ value: child, depth: depth + 1 })
    }
  }
  return undefined
}

const nativeToPortableUnchecked = (input: Automerge.AutomergeValue): PortableValue => {
  let output: PortableValue = { _tag: "Null" }
  const stack: Array<{
    readonly input: Automerge.AutomergeValue
    readonly assign: (value: PortableValue) => void
  }> = [{ input, assign: (value) => output = value }]

  while (stack.length > 0) {
    const current = stack.pop()!
    const value = current.input
    if (value === null) {
      current.assign({ _tag: "Null" })
    } else if (typeof value === "string") {
      current.assign({ _tag: "Text", value })
    } else if (typeof value === "boolean") {
      current.assign({ _tag: "Boolean", value })
    } else if (typeof value === "number") {
      current.assign({ _tag: "Number", value })
    } else if (value instanceof Date) {
      current.assign({ _tag: "Date", value: value.toISOString() })
    } else if (Automerge.isCounter(value)) {
      current.assign({ _tag: "Counter", value: value.value })
    } else if (Automerge.isImmutableString(value)) {
      current.assign({ _tag: "ImmutableString", value: value.val })
    } else if (value instanceof Uint8Array) {
      current.assign({ _tag: "Bytes", value: Encoding.encodeBase64(value) })
    } else if (Array.isArray(value)) {
      const values = Array.from<PortableValue>({ length: value.length })
      current.assign({ _tag: "List", values })
      for (let index = value.length - 1; index >= 0; index--) {
        stack.push({
          input: value[index],
          assign: (child) => {
            values[index] = child
          }
        })
      }
    } else {
      const entries = Object.entries(value).toSorted(([left], [right]) => compareCodeUnits(left, right))
      const portableEntries = Array.from<MapEntry>({ length: entries.length })
      current.assign({ _tag: "Map", entries: portableEntries })
      for (let index = entries.length - 1; index >= 0; index--) {
        const [key, child] = entries[index]
        stack.push({
          input: child,
          assign: (portable) => {
            portableEntries[index] = { key, value: portable }
          }
        })
      }
    }
  }

  return output
}

const NativeValueSchema = Schema.declare<Automerge.AutomergeValue>(
  (input): input is Automerge.AutomergeValue => nativeValueIssue(input, hardPreflightLimits) === undefined,
  { expected: "a bounded native Automerge conflict value" }
)

const isPortableValue = (value: unknown): value is PortableValue =>
  portableValueIssue(value, hardPreflightLimits) === undefined

const BoundedPortableValue = Schema.declare<PortableValue>(
  (input): input is PortableValue => portableValueIssue(input, hardPreflightLimits) === undefined,
  {
    expected: "a bounded portable Automerge conflict value",
    toCodecJson: () =>
      Schema.link<PortableValue>()(
        Schema.Json,
        {
          decode: SchemaGetter.transform<PortableValue, Schema.Json>(
            (value) => {
              if (isPortableValue(value)) return value
              return Schema.decodeUnknownSync(Schema.Never)(value)
            }
          ),
          encode: SchemaGetter.transform<Schema.Json, PortableValue>(
            (value) => Schema.encodeUnknownSync(Schema.Json)(value)
          )
        }
      )
  }
)

const portableToNativeUnchecked = (input: PortableValue): Automerge.AutomergeValue => {
  let output: Automerge.AutomergeValue = null
  const stack: Array<{
    readonly input: PortableValue
    readonly assign: (value: Automerge.AutomergeValue) => void
  }> = [{ input, assign: (value) => output = value }]

  while (stack.length > 0) {
    const current = stack.pop()!
    const value = current.input
    switch (value._tag) {
      case "Null":
        current.assign(null)
        break
      case "Boolean":
      case "Number":
      case "Text":
        current.assign(value.value)
        break
      case "ImmutableString":
        current.assign(new Automerge.ImmutableString(value.value))
        break
      case "Date":
        current.assign(DateTime.toDate(DateTime.makeUnsafe(value.value)))
        break
      case "Bytes": {
        const decoded = Encoding.decodeBase64(value.value)
        if (Result.isSuccess(decoded)) current.assign(decoded.success)
        else current.assign(new Uint8Array())
        break
      }
      case "Counter":
        current.assign(new Automerge.Counter(value.value))
        break
      case "Map": {
        const decoded: { [key: string]: Automerge.AutomergeValue } = {}
        current.assign(decoded)
        for (let index = value.entries.length - 1; index >= 0; index--) {
          const entry = value.entries[index]
          stack.push({
            input: entry.value,
            assign: (child) => {
              decoded[entry.key] = child
            }
          })
        }
        break
      }
      case "List": {
        const decoded = Array.from<Automerge.AutomergeValue>({ length: value.values.length })
        current.assign(decoded)
        for (let index = value.values.length - 1; index >= 0; index--) {
          stack.push({
            input: value.values[index],
            assign: (child) => {
              decoded[index] = child
            }
          })
        }
        break
      }
    }
  }

  return output
}

export const Value: Schema.Codec<Automerge.AutomergeValue, PortableValue> = NativeValueSchema.pipe(
  Schema.encodeTo(
    BoundedPortableValue,
    SchemaTransformation.transform({
      decode: portableToNativeUnchecked,
      encode: nativeToPortableUnchecked
    })
  )
)
export type Value = typeof Value.Type
export type EncodedValue = typeof Value.Encoded

export const Choice = Schema.TaggedUnion({
  SelectAlternative: { alternativeId: AlternativeId },
  ReplaceValue: { value: Value },
  DeleteValue: {}
})
export type Choice = typeof Choice.Type

export const Resolution = Schema.Struct({
  heads: Heads,
  path: Path,
  choice: Choice
})
export type Resolution = typeof Resolution.Type

export const Alternative = Schema.Struct({
  id: AlternativeId,
  value: Value
})
export type Alternative = typeof Alternative.Type

export const normalizeAlternatives = (alternatives: ReadonlyArray<Alternative>): ReadonlyArray<Alternative> =>
  alternatives.toSorted((left, right) => compareCodeUnits(left.id, right.id))

const BoundedAlternatives = Schema.Array(Alternative).check(
  Schema.isMaxLength(maxConflictAlternativesHardLimit)
)

const Alternatives = BoundedAlternatives.pipe(
  Schema.decode(
    SchemaTransformation.transform({
      decode: normalizeAlternatives,
      encode: normalizeAlternatives
    })
  )
)

export const Record = Schema.Struct({
  path: Path,
  visible: AlternativeId,
  alternatives: Alternatives
}).check(
  Schema.makeFilter(
    (record) => {
      const ids = record.alternatives.map((alternative) => alternative.id)
      return ids.length >= 2 &&
        new Set(ids).size === ids.length &&
        ids.filter((id) => id === record.visible).length === 1
    },
    { expected: "unique conflict alternatives containing the visible alternative exactly once" }
  )
)
export type Record = typeof Record.Type

const pathOrderKey = (path: Path): string => {
  const parents = path.parents.map((segment) => {
    if (segment._tag === "Key") return ["Key", segment.key, segment.alternative ?? ""]
    return ["Index", segment.index, segment.alternative ?? ""]
  })
  let target: ReadonlyArray<string | number>
  if (path.target._tag === "Key") target = ["Key", path.target.key]
  else target = ["Index", path.target.index]
  return Schema.encodeSync(JsonString)([...parents, target])
}

export const normalizeRecords = (records: ReadonlyArray<Record>): ReadonlyArray<Record> =>
  records
    .map((record) => ({ record, key: pathOrderKey(record.path) }))
    .toSorted((left, right) => compareCodeUnits(left.key, right.key))
    .map(({ record }) => record)

const BoundedRecords = Schema.Array(Record).check(
  Schema.makeFilter(
    (records) => {
      const paths = records.map((record) => pathOrderKey(record.path))
      let alternatives = 0
      for (const record of records) {
        alternatives += record.alternatives.length
        if (alternatives > maxConflictAlternativesHardLimit) return false
      }
      return new Set(paths).size === paths.length
    },
    {
      expected: `unique conflict paths using at most ${maxConflictAlternativesHardLimit} aggregate alternatives`
    }
  )
)

export const Records = BoundedRecords.pipe(
  Schema.decode(
    SchemaTransformation.transform({
      decode: normalizeRecords,
      encode: normalizeRecords
    })
  )
)
export type Records = typeof Records.Type

export interface Inspection<A,> {
  readonly snapshot: Snapshot.Snapshot<A>
  readonly conflicts: ReadonlyArray<Record>
}

export const inspection = <A extends Document.WireSchema,>(value: A) =>
  Schema.Struct({
    snapshot: Schema.Struct({
      documentId: Identity.DocumentId,
      value,
      version: Schema.Int,
      heads: Heads,
      tombstone: Schema.Boolean,
      projection: Schema.Literals(["Ready", "Blocked", "Rebuilding"])
    }),
    conflicts: Records
  })

export const preflightResolution = (
  input: Resolution,
  limits: PreflightLimits = hardPreflightLimits
): PreflightIssue | undefined => {
  let structural: unknown = input
  if (input.choice._tag === "ReplaceValue") {
    structural = {
      heads: input.heads,
      path: input.path,
      choice: { _tag: input.choice._tag, value: null }
    }
  }
  const structuralIssue = preflightUnknown(structural, limits)
  if (structuralIssue !== undefined) return structuralIssue
  if (input.choice._tag === "ReplaceValue") return preflightNativeValue(input.choice.value, limits)
  return undefined
}

export const preflightInspection = <A,>(
  input: Inspection<A>,
  limits: PreflightLimits = hardPreflightLimits
): PreflightIssue | undefined => {
  const structural = {
    snapshot: {
      documentId: input.snapshot.documentId,
      value: null,
      version: input.snapshot.version,
      heads: input.snapshot.heads,
      tombstone: input.snapshot.tombstone,
      projection: input.snapshot.projection
    },
    conflicts: input.conflicts.map((record) => ({
      path: record.path,
      visible: record.visible,
      alternatives: record.alternatives.map((alternative) => ({
        id: alternative.id,
        value: null
      }))
    }))
  }
  const structuralIssue = preflightUnknown(structural, limits)
  if (structuralIssue !== undefined) return structuralIssue
  const snapshotIssue = preflightNativeValue(input.snapshot.value, limits)
  if (snapshotIssue !== undefined) return snapshotIssue
  for (const record of input.conflicts) {
    for (const alternative of record.alternatives) {
      const issue = preflightNativeValue(alternative.value, limits)
      if (issue !== undefined) return issue
    }
  }
  return undefined
}

export class UnsupportedConflictValue extends Schema.TaggedErrorClass<UnsupportedConflictValue>(
  "@lucas-barake/effect-local/Conflict/UnsupportedConflictValue"
)("UnsupportedConflictValue", {
  pathDepth: nonNegativeSafeInteger,
  kind: Schema.String
}) {}

export class UnsupportedConflictKey extends Schema.TaggedErrorClass<UnsupportedConflictKey>(
  "@lucas-barake/effect-local/Conflict/UnsupportedConflictKey"
)("UnsupportedConflictKey", {
  pathDepth: nonNegativeSafeInteger
}) {}

export class StaleConflictResolution extends Schema.TaggedErrorClass<StaleConflictResolution>(
  "@lucas-barake/effect-local/Conflict/StaleConflictResolution"
)("StaleConflictResolution", {
  expectedHeads: Heads,
  observedHeads: Heads
}) {}

export class ConflictPathNotFound extends Schema.TaggedErrorClass<ConflictPathNotFound>(
  "@lucas-barake/effect-local/Conflict/ConflictPathNotFound"
)("ConflictPathNotFound", {
  path: Path,
  segmentIndex: nonNegativeSafeInteger
}) {}

export class ConflictPathTypeMismatch extends Schema.TaggedErrorClass<ConflictPathTypeMismatch>(
  "@lucas-barake/effect-local/Conflict/ConflictPathTypeMismatch"
)("ConflictPathTypeMismatch", {
  path: Path,
  segmentIndex: nonNegativeSafeInteger,
  expected: Schema.Literals(["Map", "List"]),
  observed: Schema.Literals(["Map", "List", "Scalar"])
}) {}

export class ConflictNotFound extends Schema.TaggedErrorClass<ConflictNotFound>(
  "@lucas-barake/effect-local/Conflict/ConflictNotFound"
)("ConflictNotFound", { path: Path }) {}

export class ConflictAlternativeNotFound extends Schema.TaggedErrorClass<ConflictAlternativeNotFound>(
  "@lucas-barake/effect-local/Conflict/ConflictAlternativeNotFound"
)("ConflictAlternativeNotFound", {
  path: Path,
  alternativeId: AlternativeId
}) {}

export class CompositeAlternativeRequiresReplacement
  extends Schema.TaggedErrorClass<CompositeAlternativeRequiresReplacement>(
    "@lucas-barake/effect-local/Conflict/CompositeAlternativeRequiresReplacement"
  )("CompositeAlternativeRequiresReplacement", {
    path: Path,
    alternativeId: AlternativeId,
    composite: Schema.Literals(["Map", "List"])
  })
{}

export class ConflictResolutionSchemaError extends Schema.TaggedErrorClass<ConflictResolutionSchemaError>(
  "@lucas-barake/effect-local/Conflict/ConflictResolutionSchemaError"
)("ConflictResolutionSchemaError", {
  path: Path,
  cause: Schema.Defect()
}) {}

export const InspectionError = Schema.Union([
  UnsupportedConflictValue,
  UnsupportedConflictKey
])
export type InspectionError = typeof InspectionError.Type

export const ResolutionError = Schema.Union([
  UnsupportedConflictValue,
  UnsupportedConflictKey,
  StaleConflictResolution,
  ConflictPathNotFound,
  ConflictPathTypeMismatch,
  ConflictNotFound,
  ConflictAlternativeNotFound,
  CompositeAlternativeRequiresReplacement,
  ConflictResolutionSchemaError
])
export type ResolutionError = typeof ResolutionError.Type

export const sameHeads = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean => {
  const leftNormalized = normalizeHeads(left)
  const rightNormalized = normalizeHeads(right)
  return leftNormalized.length === rightNormalized.length &&
    leftNormalized.every((head, index) => head === rightNormalized[index])
}
