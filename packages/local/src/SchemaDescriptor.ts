import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as Canonical from "./Canonical.js"

export type Descriptor =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<Descriptor>
  | { readonly [key: string]: Descriptor }

interface InternedNode {
  readonly canonical: string
  readonly descriptor: Descriptor
}

interface State {
  readonly stack: Map<SchemaAST.AST, number>
  readonly cyclic: WeakSet<SchemaAST.AST>
  readonly suspends: Map<SchemaAST.Suspend, number>
  readonly completed: WeakMap<SchemaAST.AST, Descriptor>
  readonly trustedCompleted: WeakMap<SchemaAST.AST, Descriptor>
  readonly nodes: Map<string, InternedNode>
  readonly includeConstructorDefaults: boolean
  readonly unknownAncestors: WeakMap<object, number>
  readonly unknownCyclic: WeakSet<object>
  readonly unknownCompleted: WeakMap<object, Descriptor>
  readonly unknownStack: Array<object>
}

const wellKnownSymbols = new Map<symbol, string>(
  Object.getOwnPropertyNames(Symbol).flatMap((name) => {
    const value = Reflect.get(Symbol, name)
    return typeof value === "symbol" ? [[value, name] as const] : []
  })
)

const structuralAnnotationKey = "~structural"

const fromSymbol = (value: symbol): Descriptor => {
  const globalKey = Symbol.keyFor(value)
  if (globalKey !== undefined) return { _tag: "GlobalSymbol", key: globalKey }
  const wellKnown = wellKnownSymbols.get(value)
  if (wellKnown !== undefined) return { _tag: "WellKnownSymbol", name: wellKnown }
  throw new TypeError("Schema descriptors cannot represent local symbols")
}

const fromPropertyKey = (value: PropertyKey): Descriptor =>
  typeof value === "symbol"
    ? fromSymbol(value)
    : { _tag: "StringKey", value: String(value) }

const compareDescriptor = (left: Descriptor, right: Descriptor): number => {
  const a = Canonical.stringify(left)
  const b = Canonical.stringify(right)
  return a < b ? -1 : a > b ? 1 : 0
}

const fromUnknown = (value: unknown, state: State): Descriptor => {
  if (value === null) return null
  switch (typeof value) {
    case "undefined":
      return { _tag: "Undefined" }
    case "string":
    case "boolean":
      return value
    case "number":
      if (Object.is(value, -0)) return { _tag: "Number", value: "-0" }
      return Number.isFinite(value) ? value : { _tag: "Number", value: String(value) }
    case "bigint":
      return { _tag: "BigInt", value: value.toString() }
    case "symbol":
      return fromSymbol(value)
    case "function":
      throw new TypeError("Schema descriptors cannot represent functions")
  }
  if (SchemaAST.isAST(value)) return fromAST(value, state, false)
  if (value instanceof RegExp) {
    if (value.global || value.sticky) {
      throw new TypeError("Schema descriptors cannot represent stateful regular expressions")
    }
    return { _tag: "RegExp", source: value.source, flags: value.flags }
  }
  if (value instanceof Date) return { _tag: "Date", value: value.toISOString() }
  const ancestor = state.unknownAncestors.get(value)
  if (ancestor !== undefined) {
    for (let index = ancestor; index < state.unknownStack.length; index++) {
      state.unknownCyclic.add(state.unknownStack[index]!)
    }
    return { _tag: "Circular", back: state.unknownStack.length - ancestor }
  }
  const completed = state.unknownCompleted.get(value)
  if (completed !== undefined) return completed
  state.unknownAncestors.set(value, state.unknownStack.length)
  state.unknownStack.push(value)
  let descriptor: Descriptor
  try {
    if (Array.isArray(value)) {
      descriptor = { _tag: "Array", items: value.map((item) => fromUnknown(item, state)) }
    } else {
      const prototype = Object.getPrototypeOf(value)
      if (prototype !== null && prototype !== Object.prototype) {
        throw new TypeError("Schema descriptors cannot represent non-plain objects")
      }
      const properties = Reflect.ownKeys(value).flatMap((key) => {
        const property = Object.getOwnPropertyDescriptor(value, key)
        if (property === undefined || property.enumerable !== true) return []
        if (!("value" in property)) {
          throw new TypeError("Schema descriptors cannot represent accessor properties")
        }
        return [[fromPropertyKey(key), property.value] as const]
      }).toSorted(([left], [right]) => compareDescriptor(left, right))
      const entries = properties.map(([key, propertyValue]) => [key, fromUnknown(propertyValue, state)] as const)
      descriptor = { _tag: "Object", entries }
    }
  } finally {
    state.unknownStack.pop()
    state.unknownAncestors.delete(value)
  }
  const reference = intern(descriptor, state)
  if (!state.unknownCyclic.has(value)) state.unknownCompleted.set(value, reference)
  return reference
}

type SupportedAnnotations =
  | Schema.Annotations.Annotations
  | Schema.Annotations.Filter
  | Schema.Annotations.Key<unknown>

const hasStableMetadata = (annotations: SupportedAnnotations | undefined): boolean =>
  annotations !== undefined &&
  (
    typeof annotations.identifier === "string" ||
    ("meta" in annotations && annotations.meta !== undefined) ||
    ("typeConstructor" in annotations && annotations.typeConstructor !== undefined)
  )

const hasSemanticAnnotations = (annotations: SupportedAnnotations | undefined): boolean =>
  annotations !== undefined &&
  (
    typeof annotations.identifier === "string" ||
    ("brands" in annotations && Array.isArray(annotations.brands)) ||
    ("meta" in annotations && annotations.meta !== undefined) ||
    ("typeConstructor" in annotations && annotations.typeConstructor !== undefined) ||
    ("parseOptions" in annotations && annotations.parseOptions !== undefined) ||
    typeof Reflect.get(annotations, structuralAnnotationKey) === "boolean"
  )

const hasSemanticContext = (
  context: SchemaAST.Context | undefined,
  includeConstructorDefaults: boolean
): boolean =>
  context !== undefined &&
  (
    context.isOptional ||
    context.isMutable ||
    hasSemanticAnnotations(context.annotations) ||
    (includeConstructorDefaults && context.defaultValue !== undefined)
  )

const fromAnnotations = (
  annotations: SupportedAnnotations | undefined,
  state: State
): Descriptor | undefined => {
  if (annotations === undefined) return undefined
  const result: Record<string, Descriptor> = {}
  if (typeof annotations.identifier === "string") result.identifier = annotations.identifier
  if ("brands" in annotations && Array.isArray(annotations.brands)) {
    result.brands = Array.from(new Set(annotations.brands.map(String))).toSorted()
  }
  if ("meta" in annotations && annotations.meta !== undefined) {
    result.meta = fromUnknown(annotations.meta, state)
  }
  if ("typeConstructor" in annotations && annotations.typeConstructor !== undefined) {
    result.typeConstructor = fromUnknown(annotations.typeConstructor, state)
  }
  if ("parseOptions" in annotations && annotations.parseOptions !== undefined) {
    result.parseOptions = fromUnknown(annotations.parseOptions, state)
  }
  const structural = Reflect.get(annotations, structuralAnnotationKey)
  if (typeof structural === "boolean") result.structural = structural
  return Object.keys(result).length === 0 ? undefined : result
}

const fromCheck = (
  check: SchemaAST.Check<any>,
  state: State,
  trustedBehavior: boolean
): Descriptor => {
  const result: Record<string, Descriptor> = { _tag: check._tag }
  const annotations = fromAnnotations(check.annotations, state)
  if (annotations !== undefined) result.annotations = annotations
  if (check._tag === "Filter") {
    if (!trustedBehavior && !hasStableMetadata(check.annotations)) {
      throw new TypeError("Opaque schema checks require an identifier or meta annotation")
    }
    result.aborted = check.aborted
  } else {
    result.checks = check.checks.map((inner) => fromCheck(inner, state, trustedBehavior))
  }
  return result
}

const fromChecks = (
  checks: SchemaAST.Checks | undefined,
  state: State,
  trustedBehavior: boolean
): Descriptor | undefined =>
  checks === undefined ? undefined : checks.map((check) => fromCheck(check, state, trustedBehavior))

interface BuiltInTransformation {
  readonly transformation: SchemaAST.Link["transformation"]
  readonly identity: string
}

let builtInTransformations: ReadonlyArray<BuiltInTransformation> | undefined

const getBuiltInTransformations = (): ReadonlyArray<BuiltInTransformation> => {
  if (builtInTransformations !== undefined) return builtInTransformations
  const result = new Map<SchemaAST.Link["transformation"], string>()
  const visit = (ast: SchemaAST.AST, owner: string, seen: WeakSet<object>): void => {
    if (seen.has(ast)) return
    seen.add(ast)
    for (const link of ast.encoding ?? []) {
      const current = result.get(link.transformation)
      if (current === undefined || owner < current) result.set(link.transformation, owner)
      visit(link.to, owner, seen)
    }
    switch (ast._tag) {
      case "Declaration":
        ast.typeParameters.forEach((parameter) => visit(parameter, owner, seen))
        break
      case "TemplateLiteral":
        ast.parts.forEach((part) => visit(part, owner, seen))
        break
      case "Arrays":
        ast.elements.forEach((element) => visit(element, owner, seen))
        ast.rest.forEach((element) => visit(element, owner, seen))
        break
      case "Objects":
        ast.propertySignatures.forEach((property) => visit(property.type, owner, seen))
        ast.indexSignatures.forEach((signature) => {
          visit(signature.parameter, owner, seen)
          visit(signature.type, owner, seen)
        })
        break
      case "Union":
        ast.types.forEach((member) => visit(member, owner, seen))
        break
    }
  }
  for (
    const [name, value] of Object.entries(Schema).toSorted(([left], [right]) =>
      left < right ? -1 : left > right ? 1 : 0
    )
  ) {
    if (
      value !== null &&
      (typeof value === "object" || typeof value === "function") &&
      "ast" in value &&
      SchemaAST.isAST(value.ast)
    ) {
      visit(value.ast, name, new WeakSet())
    }
  }
  builtInTransformations = Array.from(result, ([transformation, identity]) => ({
    transformation,
    identity
  }))
  return builtInTransformations
}

const explicitBehavior = (ast: SchemaAST.AST): boolean => hasStableMetadata(ast.annotations)

const fromEncoding = (
  owner: SchemaAST.AST,
  encoding: SchemaAST.Encoding,
  state: State,
  trustedBehavior: boolean
): Descriptor =>
  encoding.map((link) => {
    const builtIn = getBuiltInTransformations().find(({ transformation }) =>
      transformation === link.transformation ||
      (
        transformation._tag === link.transformation._tag &&
        transformation.decode === link.transformation.decode &&
        transformation.encode === link.transformation.encode
      )
    )?.identity
    const identified = trustedBehavior || explicitBehavior(owner)
    if (builtIn === undefined && !identified) {
      throw new TypeError("Opaque schema transformations require an identifier or meta annotation")
    }
    return {
      to: fromAST(link.to, state, identified),
      transformation: builtIn === undefined
        ? { _tag: link.transformation._tag, identity: "Annotated" }
        : { _tag: link.transformation._tag, identity: builtIn }
    }
  })

const intern = (descriptor: Descriptor, state: State): Descriptor => {
  const canonical = Canonical.stringify(descriptor)
  const id = Canonical.hash(descriptor)
  const existing = state.nodes.get(id)
  if (existing !== undefined && existing.canonical !== canonical) {
    throw new TypeError(`Schema descriptor node hash collision: ${id}`)
  }
  if (existing === undefined) state.nodes.set(id, { canonical, descriptor })
  return { _tag: "Reference", id }
}

const fromAST = (
  ast: SchemaAST.AST,
  state: State,
  trustedBehavior: boolean
): Descriptor => {
  if (
    ast._tag === "Suspend" &&
    !hasSemanticAnnotations(ast.annotations) &&
    ast.checks === undefined &&
    ast.encoding === undefined &&
    !hasSemanticContext(ast.context, state.includeConstructorDefaults)
  ) {
    const ancestor = state.suspends.get(ast)
    if (ancestor !== undefined) {
      for (const [node, depth] of state.stack) {
        if (depth >= ancestor) state.cyclic.add(node)
      }
      return { _tag: "Cycle", back: Math.max(1, state.stack.size - ancestor) }
    }
    state.suspends.set(ast, state.stack.size)
    try {
      return fromAST(ast.thunk(), state, trustedBehavior)
    } finally {
      state.suspends.delete(ast)
    }
  }
  const ancestor = state.stack.get(ast)
  if (ancestor !== undefined) {
    for (const [node, depth] of state.stack) {
      if (depth >= ancestor) state.cyclic.add(node)
    }
    return { _tag: "Cycle", back: state.stack.size - ancestor }
  }
  const completed = (trustedBehavior ? state.trustedCompleted : state.completed).get(ast)
  if (completed !== undefined) return completed
  state.stack.set(ast, state.stack.size)
  try {
    const identifiedBehavior = trustedBehavior || hasStableMetadata(ast.annotations)
    const node: Record<string, Descriptor> = { _tag: ast._tag }
    const annotations = fromAnnotations(ast.annotations, state)
    if (annotations !== undefined) node.annotations = annotations
    const checks = fromChecks(ast.checks, state, identifiedBehavior)
    if (checks !== undefined) node.checks = checks
    if (ast.encoding !== undefined) {
      node.encoding = fromEncoding(ast, ast.encoding, state, identifiedBehavior)
    }
    if (ast.context !== undefined) {
      const context: Record<string, Descriptor> = {}
      if (ast.context.isOptional) context.isOptional = true
      if (ast.context.isMutable) context.isMutable = true
      const contextAnnotations = fromAnnotations(ast.context.annotations, state)
      if (contextAnnotations !== undefined) context.annotations = contextAnnotations
      if (ast.context.defaultValue !== undefined && state.includeConstructorDefaults) {
        if (ast._tag === "Literal") {
          context.defaultValue = { _tag: "LiteralDefault", value: fromUnknown(ast.literal, state) }
        } else {
          if (!hasStableMetadata(ast.annotations) && !hasStableMetadata(ast.context.annotations)) {
            throw new TypeError("Opaque constructor defaults require an identifier or meta annotation")
          }
          context.defaultValue = fromEncoding(
            ast,
            ast.context.defaultValue,
            state,
            identifiedBehavior
          )
        }
      }
      if (Object.keys(context).length > 0) node.context = context
    }
    switch (ast._tag) {
      case "Declaration": {
        if (!identifiedBehavior) {
          throw new TypeError("Opaque schema declarations require an identifier or meta annotation")
        }
        node.typeParameters = ast.typeParameters.map((parameter) => fromAST(parameter, state, trustedBehavior))
        const encodingChecks = fromChecks(ast.encodingChecks, state, identifiedBehavior)
        if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
        break
      }
      case "Literal":
        node.literal = fromUnknown(ast.literal, state)
        break
      case "UniqueSymbol":
        node.symbol = fromSymbol(ast.symbol)
        break
      case "Enum":
        node.enums = ast.enums.map(([name, value]) => [name, value] as Descriptor)
          .toSorted(compareDescriptor)
        break
      case "TemplateLiteral":
        node.parts = ast.parts.map((part) => fromAST(part, state, trustedBehavior))
        break
      case "Arrays": {
        node.isMutable = ast.isMutable
        node.elements = ast.elements.map((element) => fromAST(element, state, trustedBehavior))
        node.rest = ast.rest.map((element) => fromAST(element, state, trustedBehavior))
        const encodingChecks = fromChecks(ast.encodingChecks, state, identifiedBehavior)
        if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
        break
      }
      case "Objects": {
        node.propertySignatures = ast.propertySignatures.map((property) => [
          fromPropertyKey(property.name),
          fromAST(property.type, state, trustedBehavior)
        ]).toSorted(([leftName, leftType], [rightName, rightType]) => {
          const byName = compareDescriptor(leftName, rightName)
          return byName === 0 ? compareDescriptor(leftType, rightType) : byName
        })
        if (ast.indexSignatures.length > 0) {
          node.indexSignatures = ast.indexSignatures.map((signature) => {
            const descriptor: Record<string, Descriptor> = {
              parameter: fromAST(signature.parameter, state, trustedBehavior),
              type: fromAST(signature.type, state, trustedBehavior)
            }
            if (signature.merge !== undefined) {
              if (!identifiedBehavior) {
                throw new TypeError("Opaque index combiners require an identifier or meta annotation")
              }
              descriptor.merge = {
                decode: signature.merge.decode !== undefined,
                encode: signature.merge.encode !== undefined,
                identity: "Annotated"
              }
            }
            return descriptor
          })
        }
        const encodingChecks = fromChecks(ast.encodingChecks, state, identifiedBehavior)
        if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
        break
      }
      case "Union": {
        node.mode = ast.mode
        node.types = ast.types.map((member) => fromAST(member, state, trustedBehavior))
        const encodingChecks = fromChecks(ast.encodingChecks, state, identifiedBehavior)
        if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
        break
      }
      case "Suspend":
        node.suspended = fromAST(ast.thunk(), state, trustedBehavior)
        break
    }
    const reference = intern(node, state)
    if (!state.cyclic.has(ast)) {
      ;(trustedBehavior ? state.trustedCompleted : state.completed).set(ast, reference)
    }
    return reference
  } finally {
    state.stack.delete(ast)
  }
}

export interface MakeOptions {
  readonly includeConstructorDefaults?: boolean | undefined
}

/**
 * Builds a deterministic structural descriptor.
 *
 * Executable schema behavior must be a recognized built-in or carry a stable
 * `identifier`, `meta`, or `typeConstructor` annotation.
 */
export const make = (schema: Schema.Constraint, options?: MakeOptions): Descriptor => {
  const state: State = {
    stack: new Map(),
    cyclic: new WeakSet(),
    suspends: new Map(),
    completed: new WeakMap(),
    trustedCompleted: new WeakMap(),
    nodes: new Map(),
    includeConstructorDefaults: options?.includeConstructorDefaults ?? true,
    unknownAncestors: new WeakMap(),
    unknownCyclic: new WeakSet(),
    unknownCompleted: new WeakMap(),
    unknownStack: []
  }
  const root = fromAST(schema.ast, state, false)
  return {
    version: 1,
    root,
    nodes: Object.fromEntries(
      Array.from(state.nodes)
        .toSorted(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
        .map(([id, node]) => [id, node.descriptor])
    )
  }
}
