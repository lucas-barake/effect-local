import * as Automerge from "@automerge/automerge"
import * as Encoding from "effect/Encoding"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
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

export const isSupportedKey = (key: string): boolean => !prototypeKeys.has(key)

const ConflictKey = Schema.String.check(
  Schema.makeFilter(isSupportedKey, { expected: "a key that is safe for Automerge object traversal" })
)

export const AlternativeId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@lucas-barake/effect-local/Conflict/AlternativeId")
)
export type AlternativeId = typeof AlternativeId.Type

export const Head = Schema.String
export type Head = typeof Head.Type

export const normalizeHeads = (heads: ReadonlyArray<Head>): ReadonlyArray<Head> => [...new Set(heads)].toSorted()

export const Heads = Schema.Array(Head).pipe(
  Schema.decodeTo(
    Schema.Array(Head),
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

const byteLength = (value: unknown): number | undefined => {
  try {
    const encoded = JSON.stringify(value)
    return encoded === undefined ? undefined : textEncoder.encode(encoded).byteLength
  } catch {
    return undefined
  }
}

const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index++) {
    if (!Object.hasOwn(value, index)) return false
  }
  return true
}

const isPlainObject = (value: object): value is { readonly [key: string]: unknown } => {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

const hasOnlyDataProperties = (value: object): boolean =>
  Object.values(Object.getOwnPropertyDescriptors(value)).every(
    (descriptor) => "value" in descriptor && descriptor.enumerable
  )

const nativeValueIssue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes">
): PreflightIssue | undefined => {
  const seen = new WeakSet<object>()
  const stack: Array<{ readonly value: unknown; readonly depth: number }> = [{ value: input, depth: 0 }]
  let nodes = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }

    if (value === null || typeof value === "string" || typeof value === "boolean") continue
    if (typeof value === "number") {
      if (Number.isFinite(value)) continue
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "nonfinite number" }
    }
    if (typeof value !== "object") {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    if (value instanceof Date) {
      if (!Number.isNaN(value.getTime())) continue
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "invalid date" }
    }
    if (Automerge.isCounter(value)) {
      if (Number.isSafeInteger(value.value)) continue
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "unsafe counter" }
    }
    if (Automerge.isImmutableString(value) || value instanceof Uint8Array) continue
    if (Automerge.isAutomerge(value)) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Automerge document" }
    }
    if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
    seen.add(value)

    if (Array.isArray(value)) {
      if (!isDenseArray(value)) {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "sparse array" }
      }
      for (let index = value.length - 1; index >= 0; index--) {
        stack.push({ value: value[index], depth: depth + 1 })
      }
      continue
    }
    if (!isPlainObject(value) || !hasOnlyDataProperties(value) || Object.getOwnPropertySymbols(value).length > 0) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
    }
    const entries = Object.entries(value)
    for (let index = entries.length - 1; index >= 0; index--) {
      const [key, child] = entries[index]!
      if (!isSupportedKey(key)) return { _tag: "UnsupportedKey", pathDepth: depth + 1 }
      stack.push({ value: child, depth: depth + 1 })
    }
  }

  const portable = nativeToPortableUnchecked(input as Automerge.AutomergeValue)
  const bytes = byteLength(portable)
  return bytes === undefined || bytes > limits.maxConflictValueBytes
    ? { _tag: "Bytes", limit: limits.maxConflictValueBytes }
    : undefined
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
  let nodes = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }
    if (typeof value !== "object" || value === null || Array.isArray(value) || !isPlainObject(value)) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
    seen.add(value)
    const record = value as { readonly [key: string]: unknown }
    const tag = record._tag

    switch (tag) {
      case "Null":
        if (!exactKeys(record, ["_tag"])) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Null" }
        }
        break
      case "Boolean":
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "boolean") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Boolean" }
        }
        break
      case "Number":
        if (
          !exactKeys(record, ["_tag", "value"]) || typeof record.value !== "number" || !Number.isFinite(record.value)
        ) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Number" }
        }
        break
      case "Text":
      case "ImmutableString":
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: String(tag) }
        }
        break
      case "Date": {
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Date" }
        }
        const date = new Date(record.value)
        if (Number.isNaN(date.getTime()) || date.toISOString() !== record.value) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Date" }
        }
        break
      }
      case "Bytes": {
        if (!exactKeys(record, ["_tag", "value"]) || typeof record.value !== "string") {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Bytes" }
        }
        const decoded = Encoding.decodeBase64(record.value)
        if (Result.isFailure(decoded) || Encoding.encodeBase64(decoded.success) !== record.value) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Bytes" }
        }
        break
      }
      case "Counter":
        if (!exactKeys(record, ["_tag", "value"]) || !Number.isSafeInteger(record.value)) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Counter" }
        }
        break
      case "Map": {
        if (
          !exactKeys(record, ["_tag", "entries"]) || !Array.isArray(record.entries) || !isDenseArray(record.entries)
        ) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "Map" }
        }
        const keys = new Set<string>()
        for (let index = record.entries.length - 1; index >= 0; index--) {
          const entry = record.entries[index]
          if (
            typeof entry !== "object" ||
            entry === null ||
            Array.isArray(entry) ||
            !isPlainObject(entry) ||
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
          stack.push({ value: entry.value, depth: depth + 1 })
        }
        break
      }
      case "List":
        if (!exactKeys(record, ["_tag", "values"]) || !Array.isArray(record.values) || !isDenseArray(record.values)) {
          return { _tag: "UnsupportedValue", pathDepth: depth, kind: "List" }
        }
        for (let index = record.values.length - 1; index >= 0; index--) {
          stack.push({ value: record.values[index], depth: depth + 1 })
        }
        break
      default:
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "unknown tag" }
    }
  }

  const bytes = byteLength(input)
  return bytes === undefined || bytes > limits.maxConflictValueBytes
    ? { _tag: "Bytes", limit: limits.maxConflictValueBytes }
    : undefined
}

export const preflightNativeValue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes"> = hardPreflightLimits
): PreflightIssue | undefined => nativeValueIssue(input, limits)

export const preflightPortableValue = (
  input: unknown,
  limits: Pick<PreflightLimits, "maxConflictDepth" | "maxConflictNodes" | "maxConflictValueBytes"> = hardPreflightLimits
): PreflightIssue | undefined => portableValueIssue(input, limits)

export const preflightUnknown = (
  input: unknown,
  limits: PreflightLimits = hardPreflightLimits
): PreflightIssue | undefined => {
  const seen = new WeakSet<object>()
  const stack: Array<{ readonly value: unknown; readonly depth: number }> = [{ value: input, depth: 0 }]
  let nodes = 0
  let alternatives = 0

  while (stack.length > 0) {
    const { value, depth } = stack.pop()!
    nodes++
    if (nodes > limits.maxConflictNodes) return { _tag: "Nodes", limit: limits.maxConflictNodes }
    if (depth > limits.maxConflictDepth) return { _tag: "Depth", limit: limits.maxConflictDepth }
    if (value === null || typeof value === "string" || typeof value === "boolean") continue
    if (typeof value === "number") {
      if (Number.isFinite(value)) continue
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "nonfinite number" }
    }
    if (typeof value !== "object") {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: unsupportedKind(value) }
    }
    if (seen.has(value)) return { _tag: "Cycle", pathDepth: depth }
    seen.add(value)
    if (Array.isArray(value)) {
      if (!isDenseArray(value)) {
        return { _tag: "UnsupportedValue", pathDepth: depth, kind: "sparse array" }
      }
      for (let index = value.length - 1; index >= 0; index--) {
        stack.push({ value: value[index], depth: depth + 1 })
      }
      continue
    }
    if (!isPlainObject(value) || !hasOnlyDataProperties(value) || Object.getOwnPropertySymbols(value).length > 0) {
      return { _tag: "UnsupportedValue", pathDepth: depth, kind: "object" }
    }
    const record = value as { readonly [key: string]: unknown }
    if (Array.isArray(record.alternatives)) {
      alternatives += record.alternatives.length
      if (alternatives > limits.maxConflictAlternatives) {
        return { _tag: "Alternatives", limit: limits.maxConflictAlternatives }
      }
    }
    if (Array.isArray(record.parents) && Object.hasOwn(record, "target")) {
      if (record.parents.length + 1 > limits.maxConflictPathSegments) {
        return { _tag: "PathSegments", limit: limits.maxConflictPathSegments }
      }
    }
    const entries = Object.entries(value)
    for (let index = entries.length - 1; index >= 0; index--) {
      const [key, child] = entries[index]!
      if (!isSupportedKey(key)) return { _tag: "UnsupportedKey", pathDepth: depth + 1 }
      stack.push({ value: child, depth: depth + 1 })
    }
  }

  const bytes = byteLength(input)
  return bytes === undefined || bytes > limits.maxConflictValueBytes
    ? { _tag: "Bytes", limit: limits.maxConflictValueBytes }
    : undefined
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
          input: value[index]!,
          assign: (child) => {
            values[index] = child
          }
        })
      }
    } else {
      const entries = Object.entries(value).toSorted(([left], [right]) => left.localeCompare(right))
      const portableEntries = Array.from<MapEntry>({ length: entries.length })
      current.assign({ _tag: "Map", entries: portableEntries })
      for (let index = entries.length - 1; index >= 0; index--) {
        const [key, child] = entries[index]!
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

const StrictDateString = Schema.String.check(
  Schema.makeFilter(
    (value) => {
      const date = new Date(value)
      return !Number.isNaN(date.getTime()) && date.toISOString() === value
    },
    { expected: "a canonical ISO date string" }
  )
)

const CanonicalBase64 = Schema.String.check(
  Schema.makeFilter(
    (value) => {
      const decoded = Encoding.decodeBase64(value)
      return Result.isSuccess(decoded) && Encoding.encodeBase64(decoded.success) === value
    },
    { expected: "canonical Base64" }
  )
)

const SafeCounter = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(Number.MIN_SAFE_INTEGER),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)

const RecursiveValue = Schema.suspend(
  (): Schema.Codec<Automerge.AutomergeValue, PortableValue, never, never> => Value
)

const MapEntries = Schema.Array(Schema.Struct({
  key: ConflictKey,
  value: RecursiveValue
})).check(
  Schema.makeFilter(
    (entries) => new Set(entries.map((entry) => entry.key)).size === entries.length,
    { expected: "unique map entry keys" }
  )
)

const TaggedValue = Schema.TaggedUnion({
  Null: {},
  Boolean: { value: Schema.Boolean },
  Number: { value: Schema.Finite },
  Text: { value: Schema.String },
  ImmutableString: { value: Schema.String },
  Date: { value: StrictDateString },
  Bytes: { value: CanonicalBase64 },
  Counter: { value: SafeCounter },
  Map: { entries: MapEntries },
  List: { values: Schema.Array(RecursiveValue) }
})

type TaggedValue = typeof TaggedValue.Type

const decodeTaggedValue = (input: TaggedValue): Automerge.AutomergeValue => {
  switch (input._tag) {
    case "Null":
      return null
    case "Boolean":
    case "Number":
    case "Text":
      return input.value
    case "ImmutableString":
      return new Automerge.ImmutableString(input.value)
    case "Date":
      return new Date(input.value)
    case "Bytes": {
      const decoded = Encoding.decodeBase64(input.value)
      return Result.isSuccess(decoded) ? decoded.success : new Uint8Array()
    }
    case "Counter":
      return new Automerge.Counter(input.value)
    case "Map": {
      const value = Object.create(null) as { [key: string]: Automerge.AutomergeValue }
      for (const entry of input.entries) value[entry.key] = entry.value
      return value
    }
    case "List":
      return [...input.values]
  }
}

const encodeTaggedValue = (input: Automerge.AutomergeValue): TaggedValue => {
  if (input === null) return { _tag: "Null" }
  if (typeof input === "string") return { _tag: "Text", value: input }
  if (typeof input === "boolean") return { _tag: "Boolean", value: input }
  if (typeof input === "number") return { _tag: "Number", value: input }
  if (input instanceof Date) return { _tag: "Date", value: input.toISOString() }
  if (Automerge.isCounter(input)) return { _tag: "Counter", value: input.value }
  if (Automerge.isImmutableString(input)) return { _tag: "ImmutableString", value: input.val }
  if (input instanceof Uint8Array) return { _tag: "Bytes", value: Encoding.encodeBase64(input) }
  if (Array.isArray(input)) return { _tag: "List", values: input }
  return {
    _tag: "Map",
    entries: Object.entries(input)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => ({ key, value }))
  }
}

export const Value: Schema.Codec<Automerge.AutomergeValue, PortableValue, never, never> = TaggedValue.pipe(
  Schema.decodeTo(
    NativeValueSchema,
    SchemaTransformation.transform({
      decode: decodeTaggedValue,
      encode: encodeTaggedValue
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
  alternatives.toSorted((left, right) => left.id.localeCompare(right.id))

const Alternatives = Schema.Array(Alternative)

export const Record = Schema.Struct({
  path: Path,
  visible: AlternativeId,
  alternatives: Alternatives
}).check(
  Schema.makeFilter(
    (record) => {
      const ids = record.alternatives.map((alternative) => alternative.id)
      return ids.length <= maxConflictAlternativesHardLimit &&
        new Set(ids).size === ids.length &&
        ids.filter((id) => id === record.visible).length === 1
    },
    { expected: "unique conflict alternatives containing the visible alternative exactly once" }
  )
)
export type Record = typeof Record.Type

const pathOrderKey = (path: Path): string =>
  JSON.stringify([
    ...path.parents.map((segment) =>
      segment._tag === "Key"
        ? ["Key", segment.key, segment.alternative ?? ""]
        : ["Index", segment.index, segment.alternative ?? ""]
    ),
    path.target._tag === "Key" ? ["Key", path.target.key] : ["Index", path.target.index]
  ])

export const normalizeRecords = (records: ReadonlyArray<Record>): ReadonlyArray<Record> =>
  records.toSorted((left, right) => pathOrderKey(left.path).localeCompare(pathOrderKey(right.path)))

export const Records = Schema.Array(Record)
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
  if (left.length !== right.length) return false
  const leftSorted = left.toSorted()
  const rightSorted = right.toSorted()
  return leftSorted.every((head, index) => head === rightSorted[index])
}
