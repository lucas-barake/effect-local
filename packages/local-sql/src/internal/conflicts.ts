import * as Automerge from "@automerge/automerge"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as InternalAutomerge from "./automerge.js"
import * as HistoryCounters from "./historyCounters.js"
import * as NativeError from "./nativeError.js"
import { quotaExceeded } from "./quotaExceeded.js"

type Backend = ReturnType<typeof Automerge.getBackend>
type BackendValue = ReturnType<Backend["getAll"]>[number]
type ConflictFailure = Conflict.InspectionError | ReplicaError.ReplicaError
type ResolutionFailure = Conflict.ResolutionError | ReplicaError.ReplicaError
type Container = Record<string, Automerge.AutomergeValue> | Array<Automerge.AutomergeValue>
type TraversalLimits = Pick<
  ReplicaLimits.Values,
  | "maxConflictAlternatives"
  | "maxConflictDepth"
  | "maxConflictNodes"
  | "maxConflictPathSegments"
  | "maxConflictValueBytes"
>

interface Budget {
  readonly limits: TraversalLimits
  readonly jsonBytes: Conflict.JsonByteBudget
  nodes: number
  alternatives: number
  failure: ResolutionFailure | undefined
}

interface BranchFrame {
  readonly parent: Container
  readonly property: string | number
  readonly selected: Container
}

interface Located {
  readonly backend: Backend
  readonly parent: Container
  readonly property: string | number
  readonly values: ReadonlyArray<BackendValue>
  readonly conflicts: Record<string, Automerge.AutomergeValue>
  readonly frames: ReadonlyArray<BranchFrame>
}

export interface PreparedResolution {
  readonly resolution: Conflict.Resolution
  readonly backend: Backend
}

export type EncodedResolution = typeof Conflict.Resolution.Encoded

export const encodeResolutionCanonical = (
  resolution: Conflict.Resolution
): Effect.Effect<
  EncodedResolution,
  Conflict.UnsupportedConflictValue
> =>
  Schema.encodeEffect(Conflict.Resolution)(resolution).pipe(
    Effect.mapError(() =>
      new Conflict.UnsupportedConflictValue({
        pathDepth: 0,
        kind: "resolution"
      })
    )
  )

export const requireSourceBudget = (
  source: {
    readonly historyBytes: number | null
    readonly historyChanges: number | null
    readonly historyOperations: number | null
  },
  limits: ReplicaLimits.Values
): Effect.Effect<void, ReplicaError.ReplicaError> => {
  if (source.historyChanges === null || source.historyOperations === null || source.historyBytes === null) {
    return Effect.fail(quotaExceeded("unmeasured conflict source history", limits.maxConflictSourceChanges))
  }
  return HistoryCounters.check({
    changes: source.historyChanges,
    operations: source.historyOperations,
    bytes: source.historyBytes
  }, limits).pipe(Effect.asVoid)
}

const preflightFailure = (
  issue: Conflict.PreflightIssue
): Conflict.UnsupportedConflictValue | Conflict.UnsupportedConflictKey | ReplicaError.ReplicaError => {
  switch (issue._tag) {
    case "UnsupportedKey":
      return new Conflict.UnsupportedConflictKey({ pathDepth: issue.pathDepth })
    case "UnsupportedValue":
      return new Conflict.UnsupportedConflictValue({
        pathDepth: issue.pathDepth,
        kind: issue.kind
      })
    case "Cycle":
      return new Conflict.UnsupportedConflictValue({
        pathDepth: issue.pathDepth,
        kind: "cycle"
      })
    case "Depth":
      return quotaExceeded("conflict depth", issue.limit)
    case "Nodes":
      return quotaExceeded("conflict nodes", issue.limit)
    case "Alternatives":
      return quotaExceeded("conflict alternatives", issue.limit)
    case "PathSegments":
      return quotaExceeded("conflict path segments", issue.limit)
    case "Bytes":
      return quotaExceeded("conflict value bytes", issue.limit)
  }
  return NativeError.throwError("Unreachable conflict preflight issue")
}

export const encodeResolution = (
  resolution: Conflict.Resolution,
  limits: ReplicaLimits.Values
): Effect.Effect<
  EncodedResolution,
  Conflict.UnsupportedConflictValue | Conflict.UnsupportedConflictKey | ReplicaError.ReplicaError
> => {
  if (resolution.path.parents.length + 1 > limits.maxConflictPathSegments) {
    return Effect.fail(quotaExceeded("conflict path segments", limits.maxConflictPathSegments))
  }
  for (let index = 0; index < resolution.path.parents.length; index++) {
    const segment = resolution.path.parents[index]
    if (segment._tag === "Key" && !Conflict.isSupportedKey(segment.key)) {
      return Effect.fail(new Conflict.UnsupportedConflictKey({ pathDepth: index + 1 }))
    }
  }
  if (resolution.path.target._tag === "Key" && !Conflict.isSupportedKey(resolution.path.target.key)) {
    return Effect.fail(
      new Conflict.UnsupportedConflictKey({ pathDepth: resolution.path.parents.length + 1 })
    )
  }
  if (resolution.choice._tag === "ReplaceValue") {
    const issue = Conflict.preflightNativeValue(resolution.choice.value, limits)
    if (issue !== undefined) return Effect.fail(preflightFailure(issue))
  }
  return encodeResolutionCanonical(resolution)
}

const consumeNode = (budget: Budget, depth: number): boolean => {
  if (depth > budget.limits.maxConflictDepth) {
    budget.failure = quotaExceeded("conflict depth", budget.limits.maxConflictDepth)
    return false
  }
  budget.nodes++
  if (budget.nodes > budget.limits.maxConflictNodes) {
    budget.failure = quotaExceeded("conflict nodes", budget.limits.maxConflictNodes)
    return false
  }
  return true
}

const consumeAlternatives = (budget: Budget, count: number): boolean => {
  budget.alternatives += count
  if (budget.alternatives > budget.limits.maxConflictAlternatives) {
    budget.failure = quotaExceeded("conflict alternatives", budget.limits.maxConflictAlternatives)
    return false
  }
  return true
}

const alternativeId = (value: BackendValue): string =>
  (() => {
    if (
      value[0] === "map" || value[0] === "list" ||
      value[0] === "text" || value[0] === "table"
    ) return (value[1])
    return (value[2])
  })()

const isComposite = (value: BackendValue): value is Extract<BackendValue, ["map" | "list", string]> =>
  value[0] === "map" || value[0] === "list"

const containerKind = (value: unknown): "Map" | "List" | "Scalar" =>
  (() => {
    if (Array.isArray(value)) return ("List")
    return ((() => {
      if (typeof value === "object" && value !== null) return ("Map")
      return ("Scalar")
    })())
  })()

const asDocument = (container: Container): Automerge.Doc<Record<string, Automerge.AutomergeValue>> =>
  Schema.decodeUnknownSync(Schema.Any)(container)

const conflictsAt = (
  container: Container,
  property: string | number
): Record<string, Automerge.AutomergeValue> | undefined => Automerge.getConflicts(asDocument(container), property)

const getProperty = (container: Container, property: string | number): Automerge.AutomergeValue | undefined =>
  Reflect.get(container, property)

const setProperty = (container: Container, property: string | number, value: Automerge.AutomergeValue): void => {
  Reflect.set(container, property, value)
}

const unsupportedValue = (depth: number, kind: string) =>
  new Conflict.UnsupportedConflictValue({ pathDepth: depth, kind })

const copyBackendValue = (
  backend: Backend,
  tuple: BackendValue,
  proxy: Automerge.AutomergeValue | undefined,
  budget: Budget,
  depth: number
): { readonly value: Automerge.AutomergeValue } | undefined => {
  if (!consumeNode(budget, depth)) return undefined
  switch (tuple[0]) {
    case "null": {
      if (Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Null"}`.length)) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: null }
    }
    case "boolean": {
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Boolean","value":`.length) ||
        Conflict.addJsonBytes(
          budget.jsonBytes,
          (() => {
            if (tuple[1]) return (4)
            return (5)
          })()
        ) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: tuple[1] }
    }
    case "str": {
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"ImmutableString","value":`.length) ||
        Conflict.addJsonStringBytes(budget.jsonBytes, tuple[1]) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: new Automerge.ImmutableString(tuple[1]) }
    }
    case "int":
    case "uint":
    case "f64": {
      if (typeof tuple[1] !== "number" || !Number.isFinite(tuple[1])) {
        budget.failure = unsupportedValue(depth, "number")
        return undefined
      }
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Number","value":`.length) ||
        Conflict.addJsonBytes(budget.jsonBytes, String(tuple[1]).length) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: tuple[1] }
    }
    case "timestamp": {
      const value = DateTime.make(tuple[1])
      if (Option.isNone(value)) {
        budget.failure = unsupportedValue(depth, "date")
        return undefined
      }
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Date","value":`.length) ||
        Conflict.addJsonStringBytes(budget.jsonBytes, DateTime.formatIso(value.value)) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: DateTime.toDateUtc(value.value) }
    }
    case "counter": {
      if (typeof tuple[1] !== "number" || !Number.isSafeInteger(tuple[1])) {
        budget.failure = unsupportedValue(depth, "counter")
        return undefined
      }
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Counter","value":`.length) ||
        Conflict.addJsonBytes(budget.jsonBytes, String(tuple[1]).length) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: new Automerge.Counter(tuple[1]) }
    }
    case "bytes": {
      const base64Bytes = 4 * Math.ceil(tuple[1].byteLength / 3)
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Bytes","value":`.length) ||
        Conflict.addJsonBytes(budget.jsonBytes, base64Bytes + 2) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value: new Uint8Array(tuple[1]) }
    }
    case "text": {
      if (
        Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Text","value":`.length) ||
        backend.length(tuple[1]) + 3 > Conflict.jsonByteBudgetRemaining(budget.jsonBytes)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      const value = backend.text(tuple[1])
      if (
        Conflict.addJsonStringBytes(budget.jsonBytes, value) ||
        Conflict.addJsonBytes(budget.jsonBytes, 1)
      ) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return undefined
      }
      return { value }
    }
    case "map":
    case "list":
      if (proxy === undefined || typeof proxy !== "object" || proxy === null) {
        budget.failure = unsupportedValue(depth, tuple[0])
        return undefined
      }
      return copyContainer(backend, Schema.decodeUnknownSync(Schema.Any)(proxy), budget, depth)
    case "table":
      budget.failure = unsupportedValue(depth, "table")
      return undefined
  }
  return undefined
}

const copyContainer = (
  backend: Backend,
  container: Container,
  budget: Budget,
  depth: number
): { readonly value: Automerge.AutomergeValue } | undefined => {
  const objectId = Automerge.getObjectId(container)
  if (objectId === null) {
    budget.failure = unsupportedValue(depth, "object")
    return undefined
  }
  if (Array.isArray(container)) {
    if (
      Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"List","values":[`.length) ||
      Conflict.addJsonBytes(budget.jsonBytes, Math.max(0, container.length - 1)) ||
      Conflict.addJsonBytes(budget.jsonBytes, 2)
    ) {
      budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
      return undefined
    }
    const output = Array.from<Automerge.AutomergeValue>({ length: container.length })
    for (let index = 0; index < container.length; index++) {
      const values = backend.getAll(objectId, index)
      const visible = values.at(-1)
      if (visible === undefined) {
        budget.failure = unsupportedValue(depth + 1, "missing list value")
        return undefined
      }
      const copied = copyBackendValue(backend, visible, container[index], budget, depth + 1)
      if (copied === undefined) return undefined
      output[index] = copied.value
    }
    return { value: output }
  }

  const keys = Object.keys(container).toSorted()
  if (
    Conflict.addJsonBytes(budget.jsonBytes, `{"_tag":"Map","entries":[`.length) ||
    Conflict.addJsonBytes(budget.jsonBytes, Math.max(0, keys.length - 1)) ||
    Conflict.addJsonBytes(budget.jsonBytes, 2)
  ) {
    budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
    return undefined
  }
  const output: Record<string, Automerge.AutomergeValue> = {}
  for (const key of keys) {
    if (!Conflict.isSupportedKey(key)) {
      budget.failure = new Conflict.UnsupportedConflictKey({ pathDepth: depth + 1 })
      return undefined
    }
    if (
      Conflict.addJsonBytes(budget.jsonBytes, `{"key":`.length) ||
      Conflict.addJsonStringBytes(budget.jsonBytes, key) ||
      Conflict.addJsonBytes(budget.jsonBytes, `,"value":`.length + 1)
    ) {
      budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
      return undefined
    }
    const values = backend.getAll(objectId, key)
    const visible = values.at(-1)
    if (visible === undefined) {
      budget.failure = unsupportedValue(depth + 1, "missing map value")
      return undefined
    }
    const copied = copyBackendValue(backend, visible, container[key], budget, depth + 1)
    if (copied === undefined) return undefined
    output[key] = copied.value
  }
  return { value: output }
}

const addParentSegmentJsonBytes = (
  budget: Conflict.JsonByteBudget,
  segment: Conflict.ParentSegment
): boolean => {
  if (segment._tag === "Key") {
    return Conflict.addJsonBytes(budget, `{"_tag":"Key","key":`.length) ||
      Conflict.addJsonStringBytes(budget, segment.key) ||
      (segment.alternative !== undefined && (
        Conflict.addJsonBytes(budget, `,"alternative":`.length) ||
        Conflict.addJsonStringBytes(budget, segment.alternative)
      )) ||
      Conflict.addJsonBytes(budget, 1)
  }
  return Conflict.addJsonBytes(budget, `{"_tag":"Index","index":`.length) ||
    Conflict.addJsonBytes(budget, String(segment.index).length) ||
    (segment.alternative !== undefined && (
      Conflict.addJsonBytes(budget, `,"alternative":`.length) ||
      Conflict.addJsonStringBytes(budget, segment.alternative)
    )) ||
    Conflict.addJsonBytes(budget, 1)
}

const addTargetSegmentJsonBytes = (
  budget: Conflict.JsonByteBudget,
  segment: Conflict.TargetSegment
): boolean =>
  (() => {
    if (segment._tag === "Key") {
      return (Conflict.addJsonBytes(budget, `{"_tag":"Key","key":`.length) ||
        Conflict.addJsonStringBytes(budget, segment.key) ||
        Conflict.addJsonBytes(budget, 1))
    }
    return (Conflict.addJsonBytes(budget, `{"_tag":"Index","index":`.length) ||
      Conflict.addJsonBytes(budget, String(segment.index).length) ||
      Conflict.addJsonBytes(budget, 1))
  })()

const addRecordMetadataJsonBytes = (
  budget: Conflict.JsonByteBudget,
  path: Conflict.Path,
  visible: Conflict.AlternativeId,
  ids: ReadonlyArray<Conflict.AlternativeId>,
  separator: boolean
): boolean => {
  if (
    (separator && Conflict.addJsonBytes(budget, 1)) ||
    Conflict.addJsonBytes(budget, `{"path":{"parents":[`.length) ||
    Conflict.addJsonBytes(budget, Math.max(0, path.parents.length - 1))
  ) {
    return true
  }
  for (const segment of path.parents) {
    if (addParentSegmentJsonBytes(budget, segment)) return true
  }
  if (
    Conflict.addJsonBytes(budget, `],"target":`.length) ||
    addTargetSegmentJsonBytes(budget, path.target) ||
    Conflict.addJsonBytes(budget, `},"visible":`.length) ||
    Conflict.addJsonStringBytes(budget, visible) ||
    Conflict.addJsonBytes(budget, `,"alternatives":[`.length) ||
    Conflict.addJsonBytes(budget, Math.max(0, ids.length - 1))
  ) {
    return true
  }
  for (const id of ids) {
    if (
      Conflict.addJsonBytes(budget, `{"id":`.length) ||
      Conflict.addJsonStringBytes(budget, id) ||
      Conflict.addJsonBytes(budget, `,"value":`.length + 1)
    ) {
      return true
    }
  }
  return Conflict.addJsonBytes(budget, 2)
}

const pathOrderKey = (path: Conflict.Path): string =>
  Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))([
    ...path.parents.map((segment) =>
      (() => {
        if (segment._tag === "Key") return ["Key", segment.key, segment.alternative ?? ""]
        return ["Index", segment.index, segment.alternative ?? ""]
      })()
    ),
    (() => {
      if (path.target._tag === "Key") return ["Key", path.target.key]
      return ["Index", path.target.index]
    })()
  ])

interface OrderedRecord {
  readonly key: string
  readonly record: Conflict.Record
}

const inspectContainer = (
  backend: Backend,
  container: Container,
  parents: ReadonlyArray<Conflict.ParentSegment>,
  budget: Budget,
  records: Array<OrderedRecord>
): void => {
  if (budget.failure !== undefined) return
  if (parents.length + 1 > budget.limits.maxConflictPathSegments) {
    budget.failure = quotaExceeded("conflict path segments", budget.limits.maxConflictPathSegments)
    return
  }
  if (!consumeNode(budget, parents.length)) return
  const objectId = Automerge.getObjectId(container)
  if (objectId === null) {
    budget.failure = unsupportedValue(parents.length, "object")
    return
  }
  const properties: ReadonlyArray<string | number> = (() => {
    if (Array.isArray(container)) return (Array.from({ length: container.length }, (_, index) => index))
    return (Object.keys(container).toSorted())
  })()

  for (const property of properties) {
    if (budget.failure !== undefined) return
    if (typeof property === "string" && !Conflict.isSupportedKey(property)) {
      budget.failure = new Conflict.UnsupportedConflictKey({ pathDepth: parents.length + 1 })
      return
    }
    const values = backend.getAll(objectId, property)
    if (!consumeNode(budget, parents.length + 1)) return
    if (values.length === 0) continue
    const visibleId = Conflict.AlternativeId.make(alternativeId(values[values.length - 1]))
    let conflicts: Record<string, Automerge.AutomergeValue> | undefined
    if (values.length > 1) {
      if (!consumeAlternatives(budget, values.length)) return
      const ids = values.map((value) => Conflict.AlternativeId.make(alternativeId(value)))
      const path: Conflict.Path = {
        parents,
        target: (() => {
          if (typeof property === "number") return (Conflict.TargetSegment.cases.Index.make({ index: property }))
          return (Conflict.TargetSegment.cases.Key.make({ key: property }))
        })()
      }
      if (values.some(isComposite)) {
        conflicts = conflictsAt(container, property)
        if (conflicts === undefined) {
          budget.failure = unsupportedValue(parents.length + 1, "conflict")
          return
        }
      }
      const alternatives: Array<Conflict.Alternative> = []
      for (let index = 0; index < values.length; index++) {
        const value = values[index]
        const id = ids[index]
        const copied = copyBackendValue(backend, value, conflicts?.[id], budget, parents.length + 1)
        if (copied === undefined) return
        alternatives.push({ id, value: copied.value })
      }
      // Metadata bytes are charged after the alternatives so an exhausted traversal budget is
      // reported before the encoded size guard.
      if (addRecordMetadataJsonBytes(budget.jsonBytes, path, visibleId, ids, records.length > 0)) {
        budget.failure = quotaExceeded("conflict value bytes", budget.limits.maxConflictValueBytes)
        return
      }
      const record: Conflict.Record = {
        path,
        visible: visibleId,
        alternatives
      }
      alternatives.sort((left, right) => Conflict.compareCodeUnits(left.id, right.id))
      records.push({ key: pathOrderKey(record.path), record })
    }

    for (const value of values) {
      if (!isComposite(value)) continue
      const id = Conflict.AlternativeId.make(alternativeId(value))
      const child = (() => {
        if (values.length > 1) return (conflicts?.[id])
        return (getProperty(container, property))
      })()
      if (child === undefined || typeof child !== "object" || child === null) {
        budget.failure = unsupportedValue(parents.length + 1, value[0])
        return
      }
      const alternative = (() => {
        if (id === visibleId) return undefined
        return id
      })()
      inspectContainer(
        backend,
        Schema.decodeUnknownSync(Schema.Any)(child),
        [
          ...parents,
          (() => {
            if (typeof property === "number") {
              return (Conflict.ParentSegment.cases.Index.make({
                index: property,
                ...((() => {
                  if (alternative === undefined) return ({})
                  return ({ alternative })
                })())
              }))
            }
            return (Conflict.ParentSegment.cases.Key.make({
              key: property,
              ...((() => {
                if (alternative === undefined) return ({})
                return ({ alternative })
              })())
            }))
          })()
        ],
        budget,
        records
      )
    }
  }
}

export const inspect = <E,>(
  document: Automerge.Doc<InternalAutomerge.Root<E>>,
  limits: ReplicaLimits.Values
): Effect.Effect<ReadonlyArray<Conflict.Record>, ConflictFailure> =>
  Effect.suspend(() => {
    const budget: Budget = {
      limits,
      jsonBytes: Conflict.createJsonByteBudget(limits.maxConflictValueBytes),
      nodes: 0,
      alternatives: 0,
      failure: undefined
    }
    const records: Array<OrderedRecord> = []
    return Effect.sync(() => {
      Automerge.change(document, (draft) => {
        const root = draft.value
        if (typeof root === "object" && root !== null) {
          inspectContainer(
            Automerge.getBackend(draft),
            Schema.decodeUnknownSync(Schema.Any)(root),
            [],
            budget,
            records
          )
        }
      })
    }).pipe(
      Effect.flatMap(() => {
        if (budget.failure !== undefined) return Effect.fail(Schema.decodeUnknownSync(Schema.Any)(budget.failure))
        records.sort((left, right) => Conflict.compareCodeUnits(left.key, right.key))
        const result = records.map(({ record }) => record)
        return Schema.encodeEffect(Conflict.Records)(result).pipe(
          Effect.orDie,
          Effect.flatMap((encoded) => {
            const issue = Conflict.preflightUnknown(encoded, limits)
            return (() => {
              if (issue?._tag === "Bytes") {
                return (Effect.fail(quotaExceeded("conflict value bytes", limits.maxConflictValueBytes)))
              }
              return (Effect.succeed(result))
            })()
          })
        )
      })
    )
  })

type LocateResult =
  | { readonly _tag: "Success"; readonly value: Located }
  | { readonly _tag: "Failure"; readonly error: Conflict.ResolutionError }

const pathProperty = (segment: Conflict.ParentSegment | Conflict.TargetSegment): string | number =>
  (() => {
    if (segment._tag === "Key") return (segment.key)
    return (segment.index)
  })()

const locate = (
  root: unknown,
  resolution: Conflict.Resolution,
  backend: Backend
): LocateResult => {
  let current: unknown = root
  const frames: Array<BranchFrame> = []

  for (let index = 0; index < resolution.path.parents.length; index++) {
    const segment = resolution.path.parents[index]
    const expected = (() => {
      if (segment._tag === "Key") return ("Map")
      return ("List")
    })()
    const observed = containerKind(current)
    if (observed !== expected) {
      return {
        _tag: "Failure",
        error: new Conflict.ConflictPathTypeMismatch({
          path: resolution.path,
          segmentIndex: index,
          expected,
          observed
        })
      }
    }
    const parent: Container = Schema.decodeUnknownSync(Schema.Any)(current)
    const property = pathProperty(segment)
    const missing = (() => {
      if (Array.isArray(parent)) {
        return (typeof property !== "number" || property >= parent.length || !Object.hasOwn(parent, property))
      }
      return (typeof property !== "string" || !Object.hasOwn(parent, property))
    })()
    if (missing) {
      return {
        _tag: "Failure",
        error: new Conflict.ConflictPathNotFound({
          path: resolution.path,
          segmentIndex: index
        })
      }
    }
    const alternative = segment.alternative
    if (alternative === undefined) {
      current = getProperty(parent, property)
      continue
    }
    const conflicts = conflictsAt(parent, property)
    const selected = conflicts?.[alternative]
    if (selected === undefined) {
      return {
        _tag: "Failure",
        error: new Conflict.ConflictAlternativeNotFound({
          path: resolution.path,
          alternativeId: alternative
        })
      }
    }
    if (typeof selected !== "object" || selected === null) {
      current = selected
      continue
    }
    frames.push({ parent, property, selected: Schema.decodeUnknownSync(Schema.Any)(selected) })
    current = selected
  }

  const index = resolution.path.parents.length
  const target = resolution.path.target
  const expected = (() => {
    if (target._tag === "Key") return ("Map")
    return ("List")
  })()
  const observed = containerKind(current)
  if (observed !== expected) {
    return {
      _tag: "Failure",
      error: new Conflict.ConflictPathTypeMismatch({
        path: resolution.path,
        segmentIndex: index,
        expected,
        observed
      })
    }
  }
  const parent: Container = Schema.decodeUnknownSync(Schema.Any)(current)
  const property = pathProperty(target)
  const missing = (() => {
    if (Array.isArray(parent)) {
      return (typeof property !== "number" || property >= parent.length || !Object.hasOwn(parent, property))
    }
    return (typeof property !== "string" || !Object.hasOwn(parent, property))
  })()
  if (missing) {
    return {
      _tag: "Failure",
      error: new Conflict.ConflictPathNotFound({
        path: resolution.path,
        segmentIndex: index
      })
    }
  }
  const objectId = Automerge.getObjectId(parent)
  if (objectId === null) {
    return {
      _tag: "Failure",
      error: new Conflict.ConflictPathTypeMismatch({
        path: resolution.path,
        segmentIndex: index,
        expected,
        observed: "Scalar"
      })
    }
  }
  const values = backend.getAll(objectId, property)
  const conflicts = conflictsAt(parent, property)
  return (() => {
    if (values.length <= 1 || conflicts === undefined) {
      return ({
        _tag: "Failure",
        error: new Conflict.ConflictNotFound({ path: resolution.path })
      })
    }
    return ({
      _tag: "Success",
      value: { backend, parent, property, values, conflicts, frames }
    })
  })()
}

const validateSelection = (
  located: Located,
  resolution: Conflict.Resolution,
  budget: Budget
): Conflict.ResolutionError | undefined => {
  const choice = resolution.choice
  if (choice._tag !== "SelectAlternative") return undefined
  const tuple = located.values.find((value) => alternativeId(value) === choice.alternativeId)
  if (tuple === undefined) {
    return new Conflict.ConflictAlternativeNotFound({
      path: resolution.path,
      alternativeId: choice.alternativeId
    })
  }
  if (tuple[0] === "map" || tuple[0] === "list") {
    return new Conflict.CompositeAlternativeRequiresReplacement({
      path: resolution.path,
      alternativeId: choice.alternativeId,
      composite: (() => {
        if (tuple[0] === "map") return ("Map")
        return ("List")
      })()
    })
  }
  const copied = copyBackendValue(
    located.backend,
    tuple,
    located.conflicts[choice.alternativeId],
    budget,
    resolution.path.parents.length + 1
  )
  return (() => {
    if (copied === undefined && budget.failure !== undefined && budget.failure._tag !== "ReplicaError") {
      return (budget.failure)
    }
    return undefined
  })()
}

export const prepareResolution = <E,>(
  document: Automerge.Doc<InternalAutomerge.Root<E>>,
  resolution: Conflict.Resolution,
  limits: ReplicaLimits.Values
): Effect.Effect<PreparedResolution, ResolutionFailure> =>
  Effect.suspend(() => {
    const budget: Budget = {
      limits,
      jsonBytes: Conflict.createJsonByteBudget(limits.maxConflictValueBytes),
      nodes: 0,
      alternatives: 0,
      failure: undefined
    }
    const backend = Automerge.getBackend(document)
    let located: LocateResult | undefined
    return Effect.sync(() => {
      Automerge.change(document, (draft) => {
        located = locate(draft.value, resolution, backend)
        if (located._tag === "Success") {
          if (!consumeAlternatives(budget, located.value.values.length)) return
          const error = validateSelection(located.value, resolution, budget)
          if (error !== undefined) budget.failure = error
        }
      })
    }).pipe(
      Effect.flatMap(() => {
        if (budget.failure !== undefined) return Effect.fail(budget.failure)
        if (located === undefined) return Effect.die(NativeError.nativeError("Resolution validation did not run"))
        return (() => {
          if (located._tag === "Failure") return (Effect.fail(located.error))
          return (Effect.succeed({ resolution, backend }))
        })()
      })
    )
  })

const cloneNative = (value: Automerge.AutomergeValue): Automerge.AutomergeValue => {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
    return value
  }
  if (value instanceof Date) return DateTime.toDateUtc(DateTime.fromDateUnsafe(value))
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (Array.isArray(value)) return Array.from(value, cloneNative)
  if (Automerge.isCounter(value)) return new Automerge.Counter(value.value)
  if (Automerge.isImmutableString(value)) return new Automerge.ImmutableString(value.val)
  const output: Record<string, Automerge.AutomergeValue> = {}
  for (const [key, child] of Object.entries(value)) output[key] = cloneNative(child)
  return output
}

const assignFresh = (
  parent: Container,
  property: string | number,
  value: Automerge.AutomergeValue
): void => {
  setProperty(parent, property, {})
  setProperty(parent, property, cloneNative(value))
}

const deleteValue = (parent: Container, property: string | number): void => {
  if (Array.isArray(parent)) {
    parent.splice(Schema.decodeUnknownSync(Schema.Any)(property), 1)
  } else {
    Reflect.deleteProperty(parent, property)
  }
}

export const applyResolution = (
  root: unknown,
  prepared: PreparedResolution,
  options: { readonly promoteParents: boolean }
): void => {
  const located = locate(root, prepared.resolution, prepared.backend)
  if (located._tag === "Failure") return NativeError.throwDefect(located.error)
  const { choice } = prepared.resolution
  switch (choice._tag) {
    case "DeleteValue":
      deleteValue(located.value.parent, located.value.property)
      break
    case "ReplaceValue":
      assignFresh(located.value.parent, located.value.property, choice.value)
      break
    case "SelectAlternative": {
      const tuple = located.value.values.find((value) => alternativeId(value) === choice.alternativeId)
      if (tuple === undefined || isComposite(tuple)) {
        return NativeError.throwError("Prepared conflict alternative is no longer selectable")
      }
      const budget: Budget = {
        limits: Conflict.hardPreflightLimits,
        jsonBytes: Conflict.createJsonByteBudget(Conflict.hardPreflightLimits.maxConflictValueBytes),
        nodes: 0,
        alternatives: 0,
        failure: undefined
      }
      const selected = copyBackendValue(
        located.value.backend,
        tuple,
        located.value.conflicts[choice.alternativeId],
        budget,
        prepared.resolution.path.parents.length + 1
      )
      if (selected === undefined) {
        return NativeError.throwDefect(
          budget.failure ?? NativeError.nativeError("Prepared conflict value is unavailable")
        )
      }
      assignFresh(located.value.parent, located.value.property, selected.value)
      break
    }
  }

  if (!options.promoteParents) return
  for (let index = located.value.frames.length - 1; index >= 0; index--) {
    const frame = located.value.frames[index]
    assignFresh(frame.parent, frame.property, cloneNative(frame.selected))
  }
}
