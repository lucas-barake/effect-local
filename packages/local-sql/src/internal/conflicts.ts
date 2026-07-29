import * as Automerge from "@automerge/automerge"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as InternalAutomerge from "./automerge.js"
import * as HistoryCounters from "./historyCounters.js"

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

const quota = (resource: string, limit: number) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.QuotaExceeded({ resource, limit })
  })

export const requireSourceBudget = (
  source: {
    readonly historyBytes: number | null
    readonly historyChanges: number | null
    readonly historyOperations: number | null
  },
  limits: ReplicaLimits.Values
): Effect.Effect<void, ReplicaError.ReplicaError> => {
  if (source.historyChanges === null || source.historyOperations === null || source.historyBytes === null) {
    return Effect.fail(quota("unmeasured conflict source history", limits.maxConflictSourceChanges))
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
      return quota("conflict depth", issue.limit)
    case "Nodes":
      return quota("conflict nodes", issue.limit)
    case "Alternatives":
      return quota("conflict alternatives", issue.limit)
    case "PathSegments":
      return quota("conflict path segments", issue.limit)
    case "Bytes":
      return quota("conflict value bytes", issue.limit)
  }
}

export const encodeResolution = (
  resolution: Conflict.Resolution,
  limits: ReplicaLimits.Values
): Effect.Effect<
  EncodedResolution,
  Conflict.UnsupportedConflictValue | Conflict.UnsupportedConflictKey | ReplicaError.ReplicaError
> => {
  if (resolution.path.parents.length + 1 > limits.maxConflictPathSegments) {
    return Effect.fail(quota("conflict path segments", limits.maxConflictPathSegments))
  }
  for (let index = 0; index < resolution.path.parents.length; index++) {
    const segment = resolution.path.parents[index]!
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
  return Schema.encodeEffect(Conflict.Resolution)(resolution).pipe(
    Effect.mapError(() =>
      new Conflict.UnsupportedConflictValue({
        pathDepth: 0,
        kind: "resolution"
      })
    )
  )
}

const consumeNode = (budget: Budget, depth: number): boolean => {
  if (depth > budget.limits.maxConflictDepth) {
    budget.failure = quota("conflict depth", budget.limits.maxConflictDepth)
    return false
  }
  budget.nodes++
  if (budget.nodes > budget.limits.maxConflictNodes) {
    budget.failure = quota("conflict nodes", budget.limits.maxConflictNodes)
    return false
  }
  return true
}

const consumeAlternatives = (budget: Budget, count: number): boolean => {
  budget.alternatives += count
  if (budget.alternatives > budget.limits.maxConflictAlternatives) {
    budget.failure = quota("conflict alternatives", budget.limits.maxConflictAlternatives)
    return false
  }
  return true
}

const alternativeId = (value: BackendValue): string =>
  value[0] === "map" || value[0] === "list" ||
    value[0] === "text" || value[0] === "table"
    ? value[1]
    : value[2]

const isComposite = (value: BackendValue): value is Extract<BackendValue, ["map" | "list", string]> =>
  value[0] === "map" || value[0] === "list"

const containerKind = (value: unknown): "Map" | "List" | "Scalar" =>
  Array.isArray(value) ? "List" : typeof value === "object" && value !== null ? "Map" : "Scalar"

const asDocument = (container: Container): Automerge.Doc<Record<string, Automerge.AutomergeValue>> =>
  container as Automerge.Doc<Record<string, Automerge.AutomergeValue>>

const conflictsAt = (
  container: Container,
  property: string | number
): Record<string, Automerge.AutomergeValue> | undefined => Automerge.getConflicts(asDocument(container), property)

const getProperty = (container: Container, property: string | number): Automerge.AutomergeValue | undefined =>
  Array.isArray(container) ? container[property as number] : container[property as string]

const setProperty = (container: Container, property: string | number, value: Automerge.AutomergeValue): void => {
  if (Array.isArray(container)) container[property as number] = value
  else container[property as string] = value
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
    case "null":
      return { value: null }
    case "boolean":
      return { value: tuple[1] }
    case "str":
      return { value: new Automerge.ImmutableString(tuple[1]) }
    case "int":
    case "uint":
    case "f64":
      if (typeof tuple[1] !== "number" || !Number.isFinite(tuple[1])) {
        budget.failure = unsupportedValue(depth, "number")
        return undefined
      }
      return { value: tuple[1] }
    case "timestamp": {
      const value = new Date(tuple[1])
      if (Number.isNaN(value.getTime())) {
        budget.failure = unsupportedValue(depth, "date")
        return undefined
      }
      return { value }
    }
    case "counter":
      if (typeof tuple[1] !== "number" || !Number.isSafeInteger(tuple[1])) {
        budget.failure = unsupportedValue(depth, "counter")
        return undefined
      }
      return { value: new Automerge.Counter(tuple[1]) }
    case "bytes":
      return { value: new Uint8Array(tuple[1]) }
    case "text":
      return { value: backend.text(tuple[1]) }
    case "map":
    case "list":
      if (proxy === undefined || typeof proxy !== "object" || proxy === null) {
        budget.failure = unsupportedValue(depth, tuple[0])
        return undefined
      }
      return copyContainer(backend, proxy as Container, budget, depth)
    case "table":
      budget.failure = unsupportedValue(depth, "table")
      return undefined
  }
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

  const output = Object.create(null) as Record<string, Automerge.AutomergeValue>
  const keys = Object.keys(container).toSorted()
  for (const key of keys) {
    if (!Conflict.isSupportedKey(key)) {
      budget.failure = new Conflict.UnsupportedConflictKey({ pathDepth: depth + 1 })
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

const parentSegment = (
  property: string | number,
  alternative: Conflict.AlternativeId | undefined
): Conflict.ParentSegment =>
  typeof property === "number"
    ? {
      _tag: "Index",
      index: property,
      ...(alternative === undefined ? {} : { alternative })
    }
    : {
      _tag: "Key",
      key: property,
      ...(alternative === undefined ? {} : { alternative })
    }

const targetSegment = (property: string | number): Conflict.TargetSegment =>
  typeof property === "number" ? { _tag: "Index", index: property } : { _tag: "Key", key: property }

const pathOrderKey = (path: Conflict.Path): string =>
  JSON.stringify([
    ...path.parents.map((segment) =>
      segment._tag === "Key"
        ? ["Key", segment.key, segment.alternative ?? ""]
        : ["Index", segment.index, segment.alternative ?? ""]
    ),
    path.target._tag === "Key" ? ["Key", path.target.key] : ["Index", path.target.index]
  ])

const inspectContainer = (
  backend: Backend,
  container: Container,
  parents: ReadonlyArray<Conflict.ParentSegment>,
  budget: Budget,
  records: Array<Conflict.Record>
): void => {
  if (budget.failure !== undefined) return
  if (parents.length + 1 > budget.limits.maxConflictPathSegments) {
    budget.failure = quota("conflict path segments", budget.limits.maxConflictPathSegments)
    return
  }
  if (!consumeNode(budget, parents.length)) return
  const objectId = Automerge.getObjectId(container)
  if (objectId === null) {
    budget.failure = unsupportedValue(parents.length, "object")
    return
  }
  const properties: ReadonlyArray<string | number> = Array.isArray(container)
    ? Array.from({ length: container.length }, (_, index) => index)
    : Object.keys(container).toSorted()

  for (const property of properties) {
    if (budget.failure !== undefined) return
    if (typeof property === "string" && !Conflict.isSupportedKey(property)) {
      budget.failure = new Conflict.UnsupportedConflictKey({ pathDepth: parents.length + 1 })
      return
    }
    const values = backend.getAll(objectId, property)
    if (!consumeNode(budget, parents.length + 1)) return
    if (values.length === 0) continue
    const visibleId = Conflict.AlternativeId.make(alternativeId(values[values.length - 1]!))
    const conflicts = values.length > 1 ? conflictsAt(container, property) : undefined
    if (values.length > 1) {
      if (!consumeAlternatives(budget, values.length)) return
      if (conflicts === undefined) {
        budget.failure = unsupportedValue(parents.length + 1, "conflict")
        return
      }
      const alternatives: Array<Conflict.Alternative> = []
      for (const value of values) {
        const id = Conflict.AlternativeId.make(alternativeId(value))
        const copied = copyBackendValue(backend, value, conflicts[id], budget, parents.length + 1)
        if (copied === undefined) return
        alternatives.push({ id, value: copied.value })
      }
      alternatives.sort((left, right) => left.id.localeCompare(right.id))
      records.push({
        path: { parents, target: targetSegment(property) },
        visible: visibleId,
        alternatives
      })
    }

    for (const value of values) {
      if (!isComposite(value)) continue
      const id = Conflict.AlternativeId.make(alternativeId(value))
      const child = values.length > 1 ? conflicts?.[id] : getProperty(container, property)
      if (child === undefined || typeof child !== "object" || child === null) {
        budget.failure = unsupportedValue(parents.length + 1, value[0])
        return
      }
      inspectContainer(
        backend,
        child as Container,
        [...parents, parentSegment(property, id === visibleId ? undefined : id)],
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
    const budget: Budget = { limits, nodes: 0, alternatives: 0, failure: undefined }
    const records: Array<Conflict.Record> = []
    return Effect.sync(() => {
      Automerge.change(document, (draft) => {
        const root = draft.value
        if (typeof root === "object" && root !== null) {
          inspectContainer(
            Automerge.getBackend(draft),
            root as unknown as Container,
            [],
            budget,
            records
          )
        }
      })
    }).pipe(
      Effect.flatMap(() => {
        if (budget.failure !== undefined) return Effect.fail(budget.failure as ConflictFailure)
        const bytes = new TextEncoder().encode(JSON.stringify(records)).byteLength
        if (bytes > limits.maxConflictValueBytes) {
          return Effect.fail(quota("conflict value bytes", limits.maxConflictValueBytes))
        }
        records.sort((left, right) => pathOrderKey(left.path).localeCompare(pathOrderKey(right.path)))
        return Effect.succeed(records)
      })
    )
  })

type LocateResult =
  | { readonly _tag: "Success"; readonly value: Located }
  | { readonly _tag: "Failure"; readonly error: Conflict.ResolutionError }

const pathProperty = (segment: Conflict.ParentSegment | Conflict.TargetSegment): string | number =>
  segment._tag === "Key" ? segment.key : segment.index

const locate = (
  root: unknown,
  resolution: Conflict.Resolution,
  backend: Backend
): LocateResult => {
  let current: unknown = root
  const frames: Array<BranchFrame> = []

  for (let index = 0; index < resolution.path.parents.length; index++) {
    const segment = resolution.path.parents[index]!
    const expected = segment._tag === "Key" ? "Map" : "List"
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
    const parent = current as Container
    const property = pathProperty(segment)
    const missing = Array.isArray(parent)
      ? typeof property !== "number" || property >= parent.length || !Object.hasOwn(parent, property)
      : typeof property !== "string" || !Object.hasOwn(parent, property)
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
    frames.push({ parent, property, selected: selected as Container })
    current = selected
  }

  const index = resolution.path.parents.length
  const target = resolution.path.target
  const expected = target._tag === "Key" ? "Map" : "List"
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
  const parent = current as Container
  const property = pathProperty(target)
  const missing = Array.isArray(parent)
    ? typeof property !== "number" || property >= parent.length || !Object.hasOwn(parent, property)
    : typeof property !== "string" || !Object.hasOwn(parent, property)
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
  return values.length <= 1 || conflicts === undefined
    ? {
      _tag: "Failure",
      error: new Conflict.ConflictNotFound({ path: resolution.path })
    }
    : {
      _tag: "Success",
      value: { backend, parent, property, values, conflicts, frames }
    }
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
      composite: tuple[0] === "map" ? "Map" : "List"
    })
  }
  const copied = copyBackendValue(
    located.backend,
    tuple,
    located.conflicts[choice.alternativeId],
    budget,
    resolution.path.parents.length + 1
  )
  return copied === undefined && budget.failure !== undefined && budget.failure._tag !== "ReplicaError"
    ? budget.failure
    : undefined
}

export const prepareResolution = <E,>(
  document: Automerge.Doc<InternalAutomerge.Root<E>>,
  resolution: Conflict.Resolution,
  limits: ReplicaLimits.Values
): Effect.Effect<PreparedResolution, ResolutionFailure> =>
  Effect.suspend(() => {
    const budget: Budget = { limits, nodes: 0, alternatives: 0, failure: undefined }
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
        if (located === undefined) return Effect.die(new Error("Resolution validation did not run"))
        return located._tag === "Failure"
          ? Effect.fail(located.error)
          : Effect.succeed({ resolution, backend })
      })
    )
  })

const cloneNative = (value: Automerge.AutomergeValue): Automerge.AutomergeValue => {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
    return value
  }
  if (value instanceof Date) return new Date(value)
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (Automerge.isCounter(value)) return new Automerge.Counter(value.value)
  if (Automerge.isImmutableString(value)) return new Automerge.ImmutableString(value.val)
  if (Array.isArray(value)) return value.map(cloneNative)
  const output = Object.create(null) as Record<string, Automerge.AutomergeValue>
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
    parent.splice(property as number, 1)
  } else {
    delete parent[property as string]
  }
}

export const applyResolution = (
  root: unknown,
  prepared: PreparedResolution,
  options: { readonly promoteParents: boolean }
): void => {
  const located = locate(root, prepared.resolution, prepared.backend)
  if (located._tag === "Failure") throw located.error
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
        throw new Error("Prepared conflict alternative is no longer selectable")
      }
      const budget: Budget = {
        limits: Conflict.hardPreflightLimits,
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
      if (selected === undefined) throw budget.failure ?? new Error("Prepared conflict value is unavailable")
      assignFresh(located.value.parent, located.value.property, selected.value)
      break
    }
  }

  if (!options.promoteParents) return
  const backend = located.value.backend
  for (let index = located.value.frames.length - 1; index >= 0; index--) {
    const frame = located.value.frames[index]!
    const budget: Budget = {
      limits: Conflict.hardPreflightLimits,
      nodes: 0,
      alternatives: 0,
      failure: undefined
    }
    const copied = copyContainer(backend, frame.selected, budget, index + 1)
    if (copied === undefined) throw budget.failure ?? new Error("Prepared conflict branch is unavailable")
    assignFresh(frame.parent, frame.property, copied.value)
  }
}
