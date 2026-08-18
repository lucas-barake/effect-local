import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Schema from "effect/Schema"
import { readFileSync } from "node:fs"
import { dirname, relative, resolve, sep } from "node:path"
import { Diagnostic, Plugin, Rule, RuleContext } from "oxlint-plugin-effect/rule-bindings"
import { makeEffectTypePolicyChecker } from "./tagged-effect-errors.mjs"

// oxlint-disable-next-line effect-local/noManualEffectBoundary -- Oxlint loads plugin configuration synchronously before any Effect runtime exists.
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))

const findVariable = (context, node) => {
  let scope = context.sourceCode.getScope(node)
  while (scope !== null) {
    const variable = scope.set.get(node.name)
    if (variable !== undefined) return variable
    scope = scope.upper
  }
  return undefined
}

const transparentExpressionTypes = new Set([
  "ParenthesizedExpression",
  "ChainExpression",
  "TSNonNullExpression",
  "TSAsExpression",
  "TSSatisfiesExpression",
  "TSTypeAssertion",
  "TSInstantiationExpression"
])

const unwrapExpression = (node) => {
  let expression = node
  while (expression !== undefined && transparentExpressionTypes.has(expression.type)) {
    expression = expression.expression
  }
  return expression
}

const getStaticMemberName = (member) => {
  if (!member.computed && member.property.type === "Identifier") return member.property.name
  if (!member.computed) return undefined
  const property = unwrapExpression(member.property)
  if (property.type === "Literal" && typeof property.value === "string") return property.value
  if (
    property.type === "TemplateLiteral" &&
    property.expressions.length === 0 &&
    property.quasis.length === 1
  ) {
    return property.quasis[0].value.cooked
  }
  return undefined
}

const getStaticMember = (node) => {
  const expression = unwrapExpression(node)
  if (expression?.type !== "MemberExpression") return undefined
  const name = getStaticMemberName(expression)
  if (name === undefined) return undefined
  return { expression, name }
}

const importedVariable = (context, node, localName) =>
  context.sourceCode.getDeclaredVariables(node).find((variable) => variable.name === localName)

const reportDiagnostic = (context, node, message) => context.report(Diagnostic.make({ node, message }))

const isNonReferencePosition = (context, node) => {
  const parent = context.sourceCode.getAncestors(node).at(-1)
  if (parent === undefined) return false
  if (parent.type === "ImportSpecifier") return true
  if (
    parent.type === "MemberExpression" && !parent.computed &&
    parent.property.start === node.start && parent.property.end === node.end
  ) {
    return true
  }
  return (parent.type === "Property" || parent.type === "PropertyDefinition" ||
    parent.type === "MethodDefinition" || parent.type === "TSPropertySignature" ||
    parent.type === "TSMethodSignature") &&
    !parent.computed && parent.shorthand !== true &&
    parent.key.start === node.start && parent.key.end === node.end
}

const isUnshadowedGlobal = (context, node, name) => {
  const expression = unwrapExpression(node)
  if (expression.type !== "Identifier" || expression.name !== name) return false
  const variable = findVariable(context, expression)
  return variable === undefined || variable.defs.length === 0
}

export const jsonParseStringifyMessage =
  "Do not use JSON.parse or JSON.stringify. Define a JSON codec with Schema.fromJsonString and decode or encode through Effect Schema."

export const noJsonParseStringify = Rule.define({
  name: "no-json-parse-stringify",
  meta: Rule.meta({
    type: "problem",
    description: "Require Effect Schema codecs for JSON parsing and serialization."
  }),
  create: function*() {
    const context = yield* RuleContext
    const jsonAliases = new Set()
    const isJsonObject = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "Identifier") {
        return isUnshadowedGlobal(context, expression, "JSON") ||
          jsonAliases.has(findVariable(context, expression))
      }
      const member = getStaticMember(expression)
      if (member?.name !== "JSON") return false
      const root = unwrapExpression(member.expression.object)
      if (root.type !== "Identifier") return false
      if (root.name !== "globalThis" && root.name !== "window" && root.name !== "self") return false
      return isUnshadowedGlobal(context, root, root.name)
    }
    const forbiddenMember = (node) => {
      const member = getStaticMember(node)
      if (member === undefined || !isJsonObject(member.expression.object)) return undefined
      if (member.name !== "parse" && member.name !== "stringify") return undefined
      return member.expression
    }
    return {
      VariableDeclarator: (node) => {
        const declaration = context.sourceCode.getAncestors(node).findLast(
          (ancestor) => ancestor.type === "VariableDeclaration"
        )
        if (declaration?.kind !== "const" || node.init === null || !isJsonObject(node.init)) {
          return Effect.void
        }
        if (node.id.type === "Identifier") {
          const variable = context.sourceCode.getDeclaredVariables(node).find(
            (candidate) => candidate.name === node.id.name
          )
          if (variable !== undefined) jsonAliases.add(variable)
          return Effect.void
        }
        if (node.id.type !== "ObjectPattern") return Effect.void
        const forbiddenProperties = []
        for (const property of node.id.properties) {
          if (property.type !== "Property") continue
          let name
          if (property.computed && property.key.type === "Literal") name = property.key.value
          if (!property.computed && property.key.type === "Identifier") name = property.key.name
          if (!property.computed && property.key.type !== "Identifier") name = property.key.value
          if (name === "parse" || name === "stringify") forbiddenProperties.push(property)
        }
        return Effect.forEach(
          forbiddenProperties,
          (property) => reportDiagnostic(context, property, jsonParseStringifyMessage),
          { discard: true }
        )
      },
      MemberExpression: (node) => {
        const member = forbiddenMember(node)
        if (member === undefined) return Effect.void
        return reportDiagnostic(context, member, jsonParseStringifyMessage)
      }
    }
  }
})

const rootEffectModuleNames = new Map([
  ["Data", "effect/Data"],
  ["Effect", "effect/Effect"],
  ["Function", "effect/Function"],
  ["ManagedRuntime", "effect/ManagedRuntime"],
  ["Runtime", "effect/Runtime"],
  ["Schedule", "effect/Schedule"],
  ["Schema", "effect/Schema"],
  ["SchemaParser", "effect/SchemaParser"]
])

const makeOfficialBindingResolver = (context, configuredModules) => {
  let modules = configuredModules
  if (configuredModules instanceof Set) {
    modules = new Map([
      ["effect/Effect", configuredModules],
      ["effect/Function", new Set(["pipe"])]
    ])
  }
  const moduleBindings = new Map()
  const exportBindings = new Map()
  const fallbackModuleBindings = new Map()
  const fallbackExportBindings = new Map()
  const rootNamespaces = new Set()
  const importBindingNodes = new WeakSet()
  const aliasBindingNodes = new WeakSet()

  const resolveModule = (node) => {
    const expression = unwrapExpression(node)
    if (expression.type === "Identifier") {
      const variable = findVariable(context, expression)
      if (variable === undefined) return fallbackModuleBindings.get(expression.name)
      return moduleBindings.get(variable)
    }
    const member = getStaticMember(expression)
    if (member === undefined) return undefined
    const root = unwrapExpression(member.expression.object)
    if (root.type !== "Identifier" || !rootNamespaces.has(findVariable(context, root))) return undefined
    const moduleName = rootEffectModuleNames.get(member.name)
    if (moduleName !== undefined && modules.has(moduleName)) return moduleName
    return undefined
  }

  const resolveExport = (node) => {
    const expression = unwrapExpression(node)
    if (expression.type === "Identifier") {
      const variable = findVariable(context, expression)
      if (variable === undefined) return fallbackExportBindings.get(expression.name)
      return exportBindings.get(variable)
    }
    const member = getStaticMember(expression)
    if (member === undefined) return undefined
    const moduleName = resolveModule(member.expression.object)
    if (moduleName !== undefined && modules.get(moduleName)?.has(member.name) === true) {
      return { moduleName, name: member.name }
    }
    return undefined
  }

  const registerImport = (node) => {
    if (node.importKind === "type") return
    const source = node.source.value
    for (const specifier of node.specifiers) importBindingNodes.add(specifier.local)
    if (modules.has(source)) {
      for (const specifier of node.specifiers) {
        const variable = importedVariable(context, node, specifier.local.name)
        if (specifier.type === "ImportNamespaceSpecifier") {
          if (variable === undefined) fallbackModuleBindings.set(specifier.local.name, source)
          else moduleBindings.set(variable, source)
        } else if (
          specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
          specifier.imported.type === "Identifier" && modules.get(source).has(specifier.imported.name)
        ) {
          if (variable === undefined) {
            fallbackExportBindings.set(specifier.local.name, {
              moduleName: source,
              name: specifier.imported.name
            })
          } else exportBindings.set(variable, { moduleName: source, name: specifier.imported.name })
        }
      }
      return
    }
    if (source !== "effect") return
    for (const specifier of node.specifiers) {
      const variable = importedVariable(context, node, specifier.local.name)
      if (specifier.type === "ImportNamespaceSpecifier") {
        if (variable !== undefined) rootNamespaces.add(variable)
      } else if (
        specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
        specifier.imported.type === "Identifier"
      ) {
        const moduleName = rootEffectModuleNames.get(specifier.imported.name)
        if (moduleName !== undefined && modules.has(moduleName)) {
          if (variable === undefined) fallbackModuleBindings.set(specifier.local.name, moduleName)
          else moduleBindings.set(variable, moduleName)
        } else if (modules.get("effect/Function")?.has(specifier.imported.name) === true) {
          // The barrel re-exports effect/Function helpers directly:
          // `import { pipe } from "effect"`.
          const binding = { moduleName: "effect/Function", name: specifier.imported.name }
          if (variable === undefined) fallbackExportBindings.set(specifier.local.name, binding)
          else exportBindings.set(variable, binding)
        }
      }
    }
  }

  const registerConstAlias = (node) => {
    const declaration = context.sourceCode.getAncestors(node).findLast(
      (ancestor) => ancestor.type === "VariableDeclaration"
    )
    if (declaration?.kind !== "const" || node.init === null || node.init === undefined) return
    if (node.id.type === "Identifier") {
      const variable = context.sourceCode.getDeclaredVariables(node).find(
        (candidate) => candidate.name === node.id.name
      )
      if (variable === undefined) return
      const moduleName = resolveModule(node.init)
      if (moduleName !== undefined) moduleBindings.set(variable, moduleName)
      const resolvedExport = resolveExport(node.init)
      if (resolvedExport !== undefined) exportBindings.set(variable, resolvedExport)
      aliasBindingNodes.add(node.id)
      return
    }
    if (node.id.type !== "ObjectPattern") return
    const moduleName = resolveModule(node.init)
    if (moduleName === undefined) return
    const moduleExports = modules.get(moduleName)
    for (const property of node.id.properties) {
      if (property.type !== "Property" || property.computed) continue
      let key = property.key.value
      if (property.key.type === "Identifier") key = property.key.name
      if (typeof key !== "string" || moduleExports.has(key) !== true) continue
      let value = property.value
      if (value.type === "AssignmentPattern") value = value.left
      if (value.type !== "Identifier") continue
      const variable = context.sourceCode.getDeclaredVariables(node).find(
        (candidate) => candidate.name === value.name
      )
      if (variable !== undefined) exportBindings.set(variable, { moduleName, name: key })
      aliasBindingNodes.add(value)
    }
  }

  return {
    aliasBindingNodes,
    importBindingNodes,
    registerConstAlias,
    registerImport,
    resolveExport,
    resolveModule
  }
}

const packageMetadataCache = new Map()
const getPackageSource = (filename) => {
  const normalized = resolve(filename).split(sep).join("/")
  const match = /^(.*\/packages\/[^/]+)\/src\/(.+)$/.exec(normalized)
  if (match === null) return undefined
  return { packageRoot: match[1], sourcePath: match[2] }
}

const collectExportTargets = (value, targets = []) => {
  if (typeof value === "string") targets.push(value)
  else if (value !== null && typeof value === "object") {
    for (const nested of Object.values(value)) collectExportTargets(nested, targets)
  }
  return targets
}

const getPackageExports = (packageRoot) => {
  if (packageMetadataCache.has(packageRoot)) return packageMetadataCache.get(packageRoot)
  let exports
  try {
    const packagePath = resolve(packageRoot, "package.json")
    exports = decodeJson(readFileSync(packagePath, "utf8")).exports
  } catch {
    exports = undefined
  }
  packageMetadataCache.set(packageRoot, exports)
  return exports
}

const isPublicPackageEntrypoint = (filename) => {
  const source = getPackageSource(filename)
  if (source === undefined) return false
  const exports = getPackageExports(source.packageRoot)
  if (exports === undefined) return false
  const relativeSource = `./src/${source.sourcePath}`
  for (const [specifier, value] of Object.entries(exports)) {
    if (specifier === "./internal/*" || specifier === "./package.json") continue
    for (const target of collectExportTargets(value)) {
      if (target === relativeSource) return true
      const star = target.indexOf("*")
      if (star < 0 || specifier.indexOf("*") < 0) continue
      const prefix = target.slice(0, star)
      const suffix = target.slice(star + 1)
      if (relativeSource.startsWith(prefix) && relativeSource.endsWith(suffix)) return true
    }
  }
  return false
}

const effectCodecExports = new Set(["decodeEffect", "encodeEffect"])
export const exportedSchemaCodecAliasMessage =
  "Do not export an alias of an Effect Schema codec. Call Schema.decodeEffect or Schema.encodeEffect directly at each use site so the schema and operation remain visible at the boundary."

export const noExportedSchemaCodecAlias = Rule.define({
  name: "no-exported-schema-codec-alias",
  meta: Rule.meta({
    type: "problem",
    description: "Keep Effect Schema codec operations visible at each use site."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(
      context,
      new Map([
        ["effect/Schema", effectCodecExports],
        ["effect/SchemaParser", effectCodecExports]
      ])
    )
    const codecVariables = new Set()

    const resolveCodecValue = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "CallExpression") return bindings.resolveExport(expression.callee)
      const direct = bindings.resolveExport(expression)
      if (direct !== undefined) return direct
      if (expression.type === "Identifier" && codecVariables.has(findVariable(context, expression))) {
        return { moduleName: "alias", name: "codec" }
      }
      return undefined
    }

    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      VariableDeclarator: (node) =>
        Effect.flatMap(
          Effect.sync(() => {
            bindings.registerConstAlias(node)
            if (node.id.type === "ObjectPattern") {
              if (node.init === null) return undefined
              const moduleName = bindings.resolveModule(node.init)
              if (moduleName === "effect/Schema" || moduleName === "effect/SchemaParser") {
                const exported = context.sourceCode.getAncestors(node).some(
                  (ancestor) => ancestor.type === "ExportNamedDeclaration"
                )
                if (exported) {
                  return node.id.properties.filter((property) => {
                    if (property.type !== "Property" || property.computed) return false
                    let name = property.key.value
                    if (property.key.type === "Identifier") name = property.key.name
                    return typeof name === "string" && effectCodecExports.has(name)
                  })
                }
              }
              return undefined
            }
            if (node.id.type !== "Identifier" || node.init === null || resolveCodecValue(node.init) === undefined) {
              return undefined
            }
            const variable = context.sourceCode.getDeclaredVariables(node).find(
              (candidate) => candidate.name === node.id.name
            )
            if (variable !== undefined) codecVariables.add(variable)
            const exported = context.sourceCode.getAncestors(node).some(
              (ancestor) => ancestor.type === "ExportNamedDeclaration" || ancestor.type === "ExportDefaultDeclaration"
            )
            if (exported) return node.id
            return undefined
          }),
          (reportNode) => {
            if (reportNode === undefined) return Effect.void
            if (Array.isArray(reportNode)) {
              return Effect.forEach(
                reportNode,
                (property) => reportDiagnostic(context, property.value, exportedSchemaCodecAliasMessage),
                { discard: true }
              )
            }
            return reportDiagnostic(context, reportNode, exportedSchemaCodecAliasMessage)
          }
        ),
      ExportNamedDeclaration: (node) => {
        if (node.exportKind === "type") return Effect.void
        if (node.source !== null && node.source !== undefined) {
          if (node.source.value !== "effect/Schema" && node.source.value !== "effect/SchemaParser") {
            return Effect.void
          }
          return Effect.forEach(
            node.specifiers.filter((specifier) =>
              specifier.type === "ExportSpecifier" && specifier.local.type === "Identifier" &&
              effectCodecExports.has(specifier.local.name)
            ),
            (specifier) => reportDiagnostic(context, specifier.exported, exportedSchemaCodecAliasMessage),
            { discard: true }
          )
        }
        return Effect.forEach(
          node.specifiers.filter((specifier) =>
            specifier.type === "ExportSpecifier" && specifier.local.type === "Identifier" &&
            (codecVariables.has(findVariable(context, specifier.local)) ||
              bindings.resolveExport(specifier.local) !== undefined)
          ),
          (specifier) => reportDiagnostic(context, specifier.exported, exportedSchemaCodecAliasMessage),
          { discard: true }
        )
      },
      ExportDefaultDeclaration: (node) => {
        if (resolveCodecValue(node.declaration) === undefined) return Effect.void
        return reportDiagnostic(context, node.declaration, exportedSchemaCodecAliasMessage)
      }
    }
  }
})

export const detachedForkMessage =
  "Do not use Effect.forkDetach. Use forkChild, forkScoped, or forkIn so lifetime, shutdown, and failure observation have an explicit owner."

export const noDetachedFork = Rule.define({
  name: "no-detached-fork",
  meta: Rule.meta({
    type: "problem",
    description: "Require an explicit owner for forked Effect fibers."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(context, new Set(["forkDetach"]))
    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      VariableDeclarator: (node) => Effect.sync(() => bindings.registerConstAlias(node)),
      ExportNamedDeclaration: (node) => {
        if (node.source?.value !== "effect/Effect" || node.exportKind === "type") return Effect.void
        return Effect.forEach(
          node.specifiers.filter((specifier) =>
            specifier.type === "ExportSpecifier" && specifier.local.type === "Identifier" &&
            specifier.local.name === "forkDetach"
          ),
          (specifier) => reportDiagnostic(context, specifier.local, detachedForkMessage),
          { discard: true }
        )
      },
      Identifier: (node) => {
        if (bindings.importBindingNodes.has(node) || bindings.aliasBindingNodes.has(node)) return Effect.void
        if (bindings.resolveExport(node)?.name !== "forkDetach") return Effect.void
        if (isNonReferencePosition(context, node)) return Effect.void
        return reportDiagnostic(context, node, detachedForkMessage)
      },
      MemberExpression: (node) => {
        if (bindings.resolveExport(node)?.name !== "forkDetach") return Effect.void
        return reportDiagnostic(context, node, detachedForkMessage)
      }
    }
  }
})

export const internalEntrypointExportMessage =
  "Do not export an internal module from a package entrypoint. Keep src/internal implementation details unreachable from public package exports."

export const noInternalEntrypointExport = Rule.define({
  name: "no-internal-entrypoint-export",
  meta: Rule.meta({
    type: "problem",
    description: "Keep package internal modules out of every public entrypoint."
  }),
  create: function*() {
    const context = yield* RuleContext
    const inspect = (node) => {
      if (!isPublicPackageEntrypoint(context.filename)) return Effect.void
      if (node.source === null || node.source === undefined || typeof node.source.value !== "string") {
        return Effect.void
      }
      const target = resolve(dirname(context.filename), node.source.value)
      const source = getPackageSource(context.filename)
      if (source === undefined) return Effect.void
      const internalRoot = resolve(source.packageRoot, "src/internal")
      const targetRelative = relative(internalRoot, target)
      if (targetRelative.startsWith("..")) return Effect.void
      return reportDiagnostic(context, node.source, internalEntrypointExportMessage)
    }
    return {
      ExportAllDeclaration: inspect,
      ExportNamedDeclaration: inspect
    }
  }
})

export const testWallClockWaitMessage =
  "Do not synchronize a test on wall clock time. Use TestClock for Effect time, or rendezvous on observable state with a Deferred, Latch, Queue, callback probe, or fiber join."

export const noTestWallClockWait = Rule.define({
  name: "no-test-wall-clock-wait",
  meta: Rule.meta({
    type: "problem",
    description: "Require deterministic test synchronization."
  }),
  create: function*() {
    const context = yield* RuleContext
    const waits = makeOfficialBindingResolver(
      context,
      new Map([
        ["effect/Effect", new Set(["sleep"])],
        ["effect/Schedule", new Set(["spaced", "fixed", "exponential"])]
      ])
    )
    const testNamespaces = new Map()
    const testBindings = new Map()
    const timerBindings = new Set()
    const timerNamespaces = new Set()
    const testModifiers = new Set(["each", "fails", "only", "runIf", "skip", "skipIf", "todo"])

    const resolveTest = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "CallExpression") return resolveTest(expression.callee)
      if (expression.type === "Identifier") return testBindings.get(findVariable(context, expression))
      const member = getStaticMember(expression)
      if (member === undefined) return undefined
      const object = unwrapExpression(member.expression.object)
      if (testModifiers.has(member.name)) return resolveTest(object)
      if (object.type === "Identifier") {
        const variable = findVariable(context, object)
        const source = testNamespaces.get(variable)
        if (source !== undefined && (member.name === "it" || member.name === "test")) return "plain"
        if (source === "@effect/vitest" && (member.name === "effect" || member.name === "live")) {
          return member.name
        }
      }
      const direct = resolveTest(object)
      if (direct === "plain" && (member.name === "effect" || member.name === "live")) return member.name
      return undefined
    }

    const registerTestImport = (node) => {
      const source = node.source.value
      if (source === "node:timers/promises") {
        for (const specifier of node.specifiers) {
          const variable = importedVariable(context, node, specifier.local.name)
          if (specifier.type === "ImportNamespaceSpecifier") {
            if (variable !== undefined) timerNamespaces.add(variable)
            continue
          }
          if (
            specifier.type !== "ImportSpecifier" || specifier.importKind === "type" ||
            specifier.imported.type !== "Identifier" ||
            (specifier.imported.name !== "setTimeout" && specifier.imported.name !== "setInterval")
          ) continue
          if (variable !== undefined) timerBindings.add(variable)
        }
        return
      }
      if (source !== "vitest" && source !== "@effect/vitest") return
      for (const specifier of node.specifiers) {
        const variable = importedVariable(context, node, specifier.local.name)
        if (variable === undefined) continue
        if (specifier.type === "ImportNamespaceSpecifier") {
          testNamespaces.set(variable, source)
          continue
        }
        if (
          specifier.type !== "ImportSpecifier" || specifier.importKind === "type" ||
          specifier.imported.type !== "Identifier"
        ) continue
        const name = specifier.imported.name
        if (name === "it" || name === "test") testBindings.set(variable, "plain")
        else if (source === "@effect/vitest" && (name === "effect" || name === "live")) {
          testBindings.set(variable, name)
        }
      }
    }

    const registerTestAlias = (node) => {
      const declaration = context.sourceCode.getAncestors(node).findLast(
        (ancestor) => ancestor.type === "VariableDeclaration"
      )
      if (declaration?.kind !== "const" || node.id.type !== "Identifier" || node.init == null) return
      const kind = resolveTest(node.init)
      if (kind === undefined) return
      const variable = context.sourceCode.getDeclaredVariables(node).find(
        (candidate) => candidate.name === node.id.name
      )
      if (variable !== undefined) testBindings.set(variable, kind)
    }

    const collectWaits = (root, testKind) => {
      const found = []
      const seen = new Set()
      const visit = (value) => {
        if (value === null || typeof value !== "object" || seen.has(value)) return
        seen.add(value)
        if (value.type === "CallExpression") {
          const callee = unwrapExpression(value.callee)
          let wallClock = false
          if (callee.type === "Identifier") {
            const variable = findVariable(context, callee)
            // Some scope managers expose ambient globals as variables with
            // empty defs, so undefined alone under-detects (same pattern as
            // isUnshadowedGlobal).
            wallClock = timerBindings.has(variable) ||
              ((variable === undefined || variable.defs.length === 0) &&
                (callee.name === "setTimeout" || callee.name === "setInterval"))
          }
          const member = getStaticMember(callee)
          if (member !== undefined && (member.name === "setTimeout" || member.name === "setInterval")) {
            const object = unwrapExpression(member.expression.object)
            if (object.type === "Identifier") {
              wallClock = timerNamespaces.has(findVariable(context, object)) ||
                isUnshadowedGlobal(context, object, "globalThis") ||
                isUnshadowedGlobal(context, object, "window") ||
                isUnshadowedGlobal(context, object, "self")
            }
          }
          const official = waits.resolveExport(callee)
          if (official?.moduleName === "effect/Effect" && official.name === "sleep") {
            wallClock = testKind !== "effect"
          }
          if (official?.moduleName === "effect/Schedule") wallClock = testKind !== "effect"
          if (wallClock) found.push(value)
        }
        for (const [key, child] of Object.entries(value)) {
          if (key === "parent" || key === "loc" || key === "range") continue
          if (Array.isArray(child)) child.forEach(visit)
          else visit(child)
        }
      }
      visit(root)
      return found
    }
    const resolveCallbackBody = (node, seen = new Set()) => {
      const callback = unwrapExpression(node)
      if (callback.type === "ArrowFunctionExpression" || callback.type === "FunctionExpression") {
        return callback.body
      }
      if (callback.type !== "Identifier") return undefined
      const variable = findVariable(context, callback)
      if (variable === undefined || seen.has(variable)) return undefined
      seen.add(variable)
      for (const definition of variable.defs) {
        if (definition.type === "FunctionName" && definition.node?.body !== undefined) {
          return definition.node.body
        }
        if (definition.type !== "Variable") continue
        const initializer = definition.node?.init
        if (initializer === null || initializer === undefined) continue
        const body = resolveCallbackBody(initializer, seen)
        if (body !== undefined) return body
      }
      return undefined
    }

    return {
      ImportDeclaration: (node) =>
        Effect.sync(() => {
          waits.registerImport(node)
          registerTestImport(node)
        }),
      VariableDeclarator: (node) =>
        Effect.sync(() => {
          waits.registerConstAlias(node)
          registerTestAlias(node)
        }),
      CallExpression: (node) => {
        const kind = resolveTest(node.callee)
        if (kind === undefined) return Effect.void
        const callbackBody = node.arguments.map((argument) => resolveCallbackBody(argument)).find(
          (body) => body !== undefined
        )
        if (callbackBody === undefined) return Effect.void
        return Effect.forEach(
          collectWaits(callbackBody, kind),
          (wait) => reportDiagnostic(context, wait, testWallClockWaitMessage),
          { discard: true }
        )
      }
    }
  }
})

export const noYieldEffectSync = Rule.define({
  name: "no-yield-effect-sync",
  meta: Rule.meta({
    type: "problem",
    description: "Call synchronous code directly inside Effect.gen instead of yielding Effect.sync."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(context, new Set(["sync"]))
    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      VariableDeclarator: (node) => Effect.sync(() => bindings.registerConstAlias(node)),
      YieldExpression: (node) => {
        if (!node.delegate || node.argument === null || node.argument === undefined) return Effect.void
        const argument = unwrapExpression(node.argument)
        if (argument?.type !== "CallExpression") return Effect.void
        if (bindings.resolveExport(argument.callee)?.name !== "sync") return Effect.void
        return context.report(Diagnostic.make({
          node,
          message: "Do not yield Effect.sync inside Effect.gen. Execute the synchronous code directly."
        }))
      }
    }
  }
})

export const noSemaphoreEffectSync = Rule.define({
  name: "no-semaphore-effect-sync",
  meta: Rule.meta({
    type: "problem",
    description: "Do not acquire a semaphore solely around Effect.sync."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(context, new Set(["sync"]))
    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      VariableDeclarator: (node) => Effect.sync(() => bindings.registerConstAlias(node)),
      CallExpression: (node) => {
        const callee = node.callee
        if (callee.type !== "MemberExpression" || callee.computed) return Effect.void
        if (callee.property.type !== "Identifier" || callee.property.name !== "withPermit") return Effect.void
        const argument = node.arguments[0]
        if (argument?.type !== "CallExpression") return Effect.void
        if (bindings.resolveExport(argument.callee)?.name !== "sync") return Effect.void
        return context.report(Diagnostic.make({
          node,
          message: "Do not acquire a semaphore around Effect.sync. Synchronous JavaScript cannot interleave."
        }))
      }
    }
  }
})

export const nestedCallMessage =
  "Do not nest more than two calls in one direct argument chain. Keep f(g(x)), but compose deeper calls with one .pipe(...) or pipe(...) from effect/Function pipeline."

export const chainedPipeMessage =
  "Do not chain .pipe(...) calls. Move every operator from the later .pipe(...) call into the first .pipe(...) call so the value has one receiver pipeline."

export const noNestedCalls = Rule.define({
  name: "no-nested-calls",
  meta: Rule.meta({
    type: "problem",
    description: "Compose nested calls with a receiver pipe or pipe from effect/Function."
  }),
  create: function*() {
    const context = yield* RuleContext
    const importedPipeBindings = new Set()
    const importedFunctionNamespaces = new Set()

    const isStaticPipeMemberCall = (node) => {
      const callee = unwrapExpression(node.callee)
      return callee.type === "MemberExpression" && getStaticMemberName(callee) === "pipe"
    }

    const isImportedNamespacePipeCall = (node) => {
      const callee = unwrapExpression(node.callee)
      if (!isStaticPipeMemberCall(node)) return false
      const namespace = unwrapExpression(callee.object)
      if (namespace.type !== "Identifier") return false
      return importedFunctionNamespaces.has(findVariable(context, namespace))
    }

    const isReceiverPipeCall = (node) => isStaticPipeMemberCall(node) && !isImportedNamespacePipeCall(node)

    const isImportedPipeCall = (node) => {
      const callee = unwrapExpression(node.callee)
      if (callee.type === "Identifier") {
        return importedPipeBindings.has(findVariable(context, callee))
      }
      return isImportedNamespacePipeCall(node)
    }

    const isPipeCall = (node) => isReceiverPipeCall(node) || isImportedPipeCall(node)

    const directArgumentDepth = (node) => {
      let depth = 1
      let child = node
      const ancestors = context.sourceCode.getAncestors(node)
      for (let index = ancestors.length - 1; index >= 0; index--) {
        const ancestor = ancestors[index]
        if (transparentExpressionTypes.has(ancestor.type) && ancestor.expression === child) {
          child = ancestor
          continue
        }
        if (ancestor.type !== "CallExpression") break
        if (isPipeCall(ancestor)) break
        if (!ancestor.arguments.includes(child)) break
        depth++
        child = ancestor
      }
      return depth
    }

    return {
      ImportDeclaration: (node) => {
        // The barrel re-exports pipe directly: `import { pipe } from "effect"`.
        const source = node.source.value
        if ((source !== "effect/Function" && source !== "effect") || node.importKind === "type") {
          return Effect.void
        }
        return Effect.sync(() => {
          for (const specifier of node.specifiers) {
            if (specifier.type === "ImportNamespaceSpecifier") {
              if (source !== "effect/Function") continue
              const variable = importedVariable(context, node, specifier.local.name)
              if (variable !== undefined) importedFunctionNamespaces.add(variable)
              continue
            }
            if (
              specifier.type !== "ImportSpecifier" ||
              specifier.importKind === "type" ||
              specifier.imported.type !== "Identifier" ||
              specifier.imported.name !== "pipe"
            ) {
              continue
            }
            const variable = importedVariable(context, node, specifier.local.name)
            if (variable !== undefined) importedPipeBindings.add(variable)
          }
        })
      },
      CallExpression: (node) => {
        const callee = unwrapExpression(node.callee)
        let receiver
        if (callee.type === "MemberExpression") receiver = unwrapExpression(callee.object)
        if (
          isReceiverPipeCall(node) && receiver?.type === "CallExpression" &&
          isReceiverPipeCall(receiver)
        ) {
          return reportDiagnostic(context, node, chainedPipeMessage)
        }
        if (isPipeCall(node) || directArgumentDepth(node) !== 3) return Effect.void
        return reportDiagnostic(context, node, nestedCallMessage)
      }
    }
  }
})

const dataLastEffectExports = new Map([
  ["effect/Context", new Map([["get", 0]])],
  ["effect/Effect", new Map([["forEach", 0]])],
  [
    "effect/Layer",
    new Map([
      ["buildWithScope", 0],
      ["effect", 1],
      ["merge", 0],
      ["succeed", 1]
    ])
  ],
  ["effect/Metric", new Map([["update", 0]])],
  ["effect/Option", new Map([["match", 0]])],
  [
    "effect/Semaphore",
    new Map([
      ["withPermit", 1],
      ["withPermitsIfAvailable", 2]
    ])
  ]
])

const rootEffectModules = new Map([
  ["Context", "effect/Context"],
  ["Effect", "effect/Effect"],
  ["Layer", "effect/Layer"],
  ["Metric", "effect/Metric"],
  ["Option", "effect/Option"],
  ["Semaphore", "effect/Semaphore"]
])

export const unnecessaryEffectForwardingMessage = (canonicalName, replacement) =>
  `Remove this forwarding closure. ${canonicalName} is verified data-last in Effect 4.0.0-beta.103 at this exact argument position. Pass ${replacement} directly to pipe.`

export const unnecessaryPipeForwardingMessage = (callee, source) =>
  `Remove this exact unary forwarding pipe operator. Call ${callee} directly with ${source} so the receiver and data flow stay visible.`

export const noUnnecessaryEffectForwarding = Rule.define({
  name: "no-unnecessary-effect-forwarding",
  meta: Rule.meta({
    type: "problem",
    description: "Use verified data-last Effect combinators directly instead of exact forwarding closures."
  }),
  create: function*() {
    const context = yield* RuleContext
    const directBindings = new Map()
    const moduleNamespaces = new Map()
    const rootNamespaces = new Set()
    const importedPipeBindings = new Set()
    const importedFunctionNamespaces = new Set()

    const staticMember = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type !== "MemberExpression" || expression.computed) return undefined
      if (expression.property.type !== "Identifier") return undefined
      return { expression, name: expression.property.name }
    }

    const resolveModule = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "Identifier") {
        return moduleNamespaces.get(findVariable(context, expression))
      }
      const member = staticMember(expression)
      if (member === undefined) return undefined
      const root = unwrapExpression(member.expression.object)
      if (root.type !== "Identifier") return undefined
      const rootVariable = findVariable(context, root)
      if (!rootNamespaces.has(rootVariable)) return undefined
      return rootEffectModules.get(member.name)
    }

    const resolveCombinator = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "Identifier") {
        return directBindings.get(findVariable(context, expression))
      }
      const member = staticMember(expression)
      if (member === undefined) return undefined
      const moduleName = resolveModule(member.expression.object)
      if (moduleName === undefined) return undefined
      const dataIndex = dataLastEffectExports.get(moduleName)?.get(member.name)
      if (dataIndex === undefined) return undefined
      return { canonicalName: `${moduleName.slice("effect/".length)}.${member.name}`, dataIndex }
    }

    const containsThis = (value, seen = new Set()) => {
      if (value === null || typeof value !== "object" || seen.has(value)) return false
      seen.add(value)
      if (value.type === "ThisExpression") return true
      for (const [key, child] of Object.entries(value)) {
        if (key === "parent" || key === "loc" || key === "range") continue
        if (Array.isArray(child)) {
          if (child.some((element) => containsThis(element, seen))) return true
        } else if (containsThis(child, seen)) return true
      }
      return false
    }

    const registerImport = (node) => {
      if (node.importKind === "type") return
      const source = node.source.value
      const exports = dataLastEffectExports.get(source)
      if (exports !== undefined) {
        for (const specifier of node.specifiers) {
          const variable = importedVariable(context, node, specifier.local.name)
          if (variable === undefined) continue
          if (specifier.type === "ImportNamespaceSpecifier") {
            moduleNamespaces.set(variable, source)
          } else if (
            specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
            specifier.imported.type === "Identifier"
          ) {
            const dataIndex = exports.get(specifier.imported.name)
            if (dataIndex !== undefined) {
              directBindings.set(variable, {
                canonicalName: `${source.slice("effect/".length)}.${specifier.imported.name}`,
                dataIndex
              })
            }
          }
        }
        return
      }
      if (source === "effect/Function") {
        for (const specifier of node.specifiers) {
          const variable = importedVariable(context, node, specifier.local.name)
          if (variable === undefined) continue
          if (specifier.type === "ImportNamespaceSpecifier") {
            importedFunctionNamespaces.add(variable)
          } else if (
            specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
            specifier.imported.type === "Identifier" && specifier.imported.name === "pipe"
          ) {
            importedPipeBindings.add(variable)
          }
        }
        return
      }
      if (source !== "effect") return
      for (const specifier of node.specifiers) {
        const variable = importedVariable(context, node, specifier.local.name)
        if (variable === undefined) continue
        if (specifier.type === "ImportNamespaceSpecifier") {
          rootNamespaces.add(variable)
        } else if (
          specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
          specifier.imported.type === "Identifier"
        ) {
          const moduleName = rootEffectModules.get(specifier.imported.name)
          if (moduleName !== undefined) moduleNamespaces.set(variable, moduleName)
          else if (specifier.imported.name === "pipe") importedPipeBindings.add(variable)
        }
      }
    }

    const isImportedPipeCall = (node) => {
      const callee = unwrapExpression(node.callee)
      if (callee.type === "Identifier") {
        return importedPipeBindings.has(findVariable(context, callee))
      }
      const member = getStaticMember(callee)
      if (member?.name !== "pipe") return false
      const namespace = unwrapExpression(member.expression.object)
      return namespace.type === "Identifier" &&
        importedFunctionNamespaces.has(findVariable(context, namespace))
    }

    const getPipeOperator = (node) => {
      let child = node
      const ancestors = context.sourceCode.getAncestors(node)
      for (let index = ancestors.length - 1; index >= 0; index--) {
        const ancestor = ancestors[index]
        if (transparentExpressionTypes.has(ancestor.type) && ancestor.expression === child) {
          child = ancestor
          continue
        }
        if (ancestor.type !== "CallExpression") return undefined
        if (isImportedPipeCall(ancestor)) {
          if (ancestor.arguments.indexOf(child) < 1) return undefined
          return { source: ancestor.arguments[0] }
        }
        const member = getStaticMember(ancestor.callee)
        if (member?.name !== "pipe" || ancestor.arguments.indexOf(child) < 0) return undefined
        return { source: member.expression.object }
      }
      return undefined
    }

    const inspectExactUnaryPipeForwarder = (node) => {
      const pipeOperator = getPipeOperator(node)
      if (pipeOperator === undefined || node.async || node.generator || node.params.length !== 1) {
        return Effect.void
      }
      const parameter = node.params[0]
      if (parameter.type !== "Identifier") return Effect.void
      const body = unwrapExpression(node.body)
      if (body.type !== "CallExpression" || body.optional || body.arguments.length !== 1) return Effect.void
      if (body.arguments[0].type === "SpreadElement") return Effect.void
      const callee = unwrapExpression(body.callee)
      if (callee.type === "MemberExpression" && (callee.computed || callee.optional)) return Effect.void
      if (callee.type !== "Identifier" && callee.type !== "MemberExpression") return Effect.void
      const argument = unwrapExpression(body.arguments[0])
      if (argument.type !== "Identifier") return Effect.void
      const parameterVariable = context.sourceCode.getDeclaredVariables(node).find(
        (variable) => variable.name === parameter.name
      )
      if (parameterVariable === undefined || findVariable(context, argument) !== parameterVariable) {
        return Effect.void
      }
      if (
        parameterVariable.references.length !== 1 ||
        parameterVariable.references[0].identifier.start !== argument.start ||
        parameterVariable.references[0].identifier.end !== argument.end
      ) {
        return Effect.void
      }
      const calleeText = context.sourceCode.getText(body.callee)
      const sourceText = context.sourceCode.getText(pipeOperator.source)
      return reportDiagnostic(context, node, unnecessaryPipeForwardingMessage(calleeText, sourceText))
    }

    const inspectForwarder = (node) => {
      const exactUnary = inspectExactUnaryPipeForwarder(node)
      if (exactUnary !== Effect.void) return exactUnary
      if (node.async || node.generator || node.params.length !== 1) return Effect.void
      const parameter = node.params[0]
      if (parameter.type !== "Identifier") return Effect.void
      const body = unwrapExpression(node.body)
      if (body.type !== "CallExpression" || body.arguments.some((argument) => argument.type === "SpreadElement")) {
        return Effect.void
      }
      if (containsThis(body)) return Effect.void
      const combinator = resolveCombinator(body.callee)
      if (combinator === undefined) return Effect.void
      const dataArgument = body.arguments[combinator.dataIndex]
      if (dataArgument === undefined || unwrapExpression(dataArgument).type !== "Identifier") return Effect.void
      const identifier = unwrapExpression(dataArgument)
      const parameterVariable = context.sourceCode.getDeclaredVariables(node).find(
        (variable) => variable.name === parameter.name
      )
      if (parameterVariable === undefined || findVariable(context, identifier) !== parameterVariable) return Effect.void
      if (
        parameterVariable.references.length !== 1 ||
        parameterVariable.references[0].identifier.start !== identifier.start ||
        parameterVariable.references[0].identifier.end !== identifier.end
      ) {
        return Effect.void
      }
      const remainingArguments = body.arguments.filter((_, index) => index !== combinator.dataIndex)
      const calleeText = context.sourceCode.getText(body.callee)
      let replacement = calleeText
      if (remainingArguments.length > 0) {
        replacement = `${calleeText}(${
          remainingArguments.map((argument) => context.sourceCode.getText(argument)).join(", ")
        })`
      }
      return reportDiagnostic(
        context,
        node,
        unnecessaryEffectForwardingMessage(combinator.canonicalName, replacement)
      )
    }

    return {
      ImportDeclaration: (node) => Effect.sync(() => registerImport(node)),
      ArrowFunctionExpression: inspectForwarder
    }
  }
})

export const functionEffectGenMessage =
  "Do not return Effect.gen from a function. Move the generator body into Effect.fnUntraced(function* () { ... }). If this function intentionally defines a telemetry boundary, use Effect.fn(\"spanName\")(function* () { ... }). In Effect 4.0.0-beta.103, the first argument is the generator body and arguments after it act as sequential pipe operators, so migrate a returned pipeline as Effect.fnUntraced(function* () {}, x, y, z) or Effect.fn(\"spanName\")(function* () {}, x, y, z). Each operator receives the previous result and the original function arguments."

export const noFunctionEffectGen = Rule.define({
  name: "no-function-effect-gen",
  meta: Rule.meta({
    type: "problem",
    description: "Define Effect returning functions with Effect.fnUntraced or Effect.fn."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(context, new Set(["gen", "fn", "fnUntraced"]))

    const effectGenRoot = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type !== "CallExpression") return undefined
      const resolved = bindings.resolveExport(expression.callee)
      if (resolved?.name === "gen") return expression
      if (resolved?.moduleName === "effect/Function") return effectGenRoot(expression.arguments[0])
      const member = getStaticMember(expression.callee)
      if (member?.name === "pipe") return effectGenRoot(member.expression.object)
      return undefined
    }

    const isOrdinaryFunction = (node) =>
      (node.type === "ArrowFunctionExpression" ||
        node.type === "FunctionExpression" ||
        node.type === "FunctionDeclaration") &&
      !node.async && !node.generator

    const inspectReturnedExpression = (functionNode, expression) => {
      if (!isOrdinaryFunction(functionNode)) return Effect.void
      const root = effectGenRoot(expression)
      if (root === undefined) return Effect.void
      return reportDiagnostic(context, root, functionEffectGenMessage)
    }

    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      VariableDeclarator: (node) => Effect.sync(() => bindings.registerConstAlias(node)),
      ArrowFunctionExpression: (node) => {
        if (node.body.type === "BlockStatement") return Effect.void
        return inspectReturnedExpression(node, node.body)
      },
      ReturnStatement: (node) => {
        if (node.argument === null) return Effect.void
        const ancestors = context.sourceCode.getAncestors(node)
        const owner = ancestors.findLast((ancestor) =>
          ancestor.type === "ArrowFunctionExpression" ||
          ancestor.type === "FunctionExpression" ||
          ancestor.type === "FunctionDeclaration"
        )
        if (owner === undefined) return Effect.void
        return inspectReturnedExpression(owner, node.argument)
      }
    }
  }
})

export const effectOrDieMessage =
  "Do not use Effect.orDie. Catch each intended tagged failure with Effect.catchTag, Effect.catchTags, Effect.catchReason, or Effect.catchReasons, and call Effect.die(error) in each selected handler so defect conversion stays visible and exhaustive."

export const effectCatchDieMessage =
  "Do not convert the complete Effect error channel to defects with Effect.catch. Catch each intended tag or nested reason with Effect.catchTag, Effect.catchTags, Effect.catchReason, or Effect.catchReasons, and call Effect.die(error) in each selected handler so newly added failures remain typed until reviewed."

export const noImplicitDefectConversion = Rule.define({
  name: "no-implicit-defect-conversion",
  meta: Rule.meta({
    type: "problem",
    description: "Require explicit tag or reason selection before converting typed failures to defects."
  }),
  create: function*() {
    const context = yield* RuleContext
    const bindings = makeOfficialBindingResolver(context, new Set(["orDie", "catch", "die"]))

    const isCatchDieForwarder = (node) => {
      if (bindings.resolveExport(node.callee)?.name !== "catch") return false
      if (node.arguments.some((argument) => argument.type === "SpreadElement")) return false
      let handlerIndex = -1
      if (node.arguments.length === 1) handlerIndex = 0
      else if (node.arguments.length === 2) handlerIndex = 1
      if (handlerIndex < 0) return false
      const handler = unwrapExpression(node.arguments[handlerIndex])
      if (
        (handler.type !== "ArrowFunctionExpression" && handler.type !== "FunctionExpression") ||
        handler.async || handler.generator || handler.params.length !== 1 ||
        handler.params[0].type !== "Identifier"
      ) return false
      let returned = unwrapExpression(handler.body)
      if (returned.type === "BlockStatement") {
        if (returned.body.length !== 1 || returned.body[0].type !== "ReturnStatement") return false
        if (returned.body[0].argument === null) return false
        returned = unwrapExpression(returned.body[0].argument)
      }
      if (
        returned.type !== "CallExpression" || returned.arguments.length !== 1 ||
        returned.arguments[0].type === "SpreadElement" ||
        bindings.resolveExport(returned.callee)?.name !== "die"
      ) return false
      const argument = unwrapExpression(returned.arguments[0])
      if (argument.type !== "Identifier") return false
      const parameter = handler.params[0]
      const variable = context.sourceCode.getDeclaredVariables(handler).find(
        (candidate) => candidate.name === parameter.name
      )
      return variable !== undefined && findVariable(context, argument) === variable &&
        variable.references.length === 1 &&
        variable.references[0].identifier.start === argument.start &&
        variable.references[0].identifier.end === argument.end
    }

    return {
      ImportDeclaration: (node) => Effect.sync(() => bindings.registerImport(node)),
      ExportNamedDeclaration: (node) => {
        if (node.source?.value !== "effect/Effect" || node.exportKind === "type") return Effect.void
        return Effect.forEach(
          node.specifiers.filter((specifier) =>
            specifier.type === "ExportSpecifier" &&
            specifier.local.type === "Identifier" && specifier.local.name === "orDie"
          ),
          (specifier) => reportDiagnostic(context, specifier.local, effectOrDieMessage),
          { discard: true }
        )
      },
      VariableDeclarator: (node) => Effect.sync(() => bindings.registerConstAlias(node)),
      Identifier: (node) => {
        if (bindings.importBindingNodes.has(node) || bindings.aliasBindingNodes.has(node)) return Effect.void
        if (bindings.resolveExport(node)?.name !== "orDie") return Effect.void
        if (isNonReferencePosition(context, node)) return Effect.void
        return reportDiagnostic(context, node, effectOrDieMessage)
      },
      MemberExpression: (node) => {
        if (bindings.resolveExport(node)?.name !== "orDie") return Effect.void
        return reportDiagnostic(context, node, effectOrDieMessage)
      },
      CallExpression: (node) => {
        if (!isCatchDieForwarder(node)) return Effect.void
        return reportDiagnostic(context, node, effectCatchDieMessage)
      }
    }
  }
})

export const manualBoundaryMessage =
  "Keep execution inside Effect. Yield or compose this Effect value, and use the Effect returning Schema codec so typed failures, defects, interruption, services, scopes, and finalizers remain in the current Effect. Manual runners and Sync or Promise codec adapters are allowed only at a genuine non Effect host or unavoidable synchronous exception boundary. Move the call to that exact boundary and add a one line oxlint-disable-next-line effect-local/noManualEffectBoundary with a concrete reason. Do not suppress code that can remain inside Effect or a shared codec factory that also feeds Effect code."

const schemaBoundaries = new Set([
  "decodeUnknownSync",
  "decodeSync",
  "encodeUnknownSync",
  "encodeSync",
  "decodeUnknownPromise",
  "decodePromise",
  "encodeUnknownPromise",
  "encodePromise",
  "asserts"
])

const boundaryModules = new Map([
  [
    "effect/Effect",
    new Set([
      "runFork",
      "runForkWith",
      "runCallback",
      "runCallbackWith",
      "runPromise",
      "runPromiseWith",
      "runPromiseExit",
      "runPromiseExitWith",
      "runSync",
      "runSyncWith",
      "runSyncExit",
      "runSyncExitWith"
    ])
  ],
  ["effect/Schema", schemaBoundaries],
  ["effect/SchemaParser", schemaBoundaries],
  ["effect/Runtime", new Set(["makeRunMain"])]
])

const managedRuntimeBoundaries = new Set([
  "runFork",
  "runCallback",
  "runSyncExit",
  "runSync",
  "runPromiseExit",
  "runPromise",
  "context",
  "dispose"
])

const typePolicyCheckers = new Map()
const getTypePolicyChecker = (cwd) => {
  let checker = typePolicyCheckers.get(cwd)
  if (checker === undefined) {
    checker = makeEffectTypePolicyChecker({ cwd })
    typePolicyCheckers.set(cwd, checker)
  }
  return checker
}
const closeTypePolicyCheckers = () => {
  for (const checker of typePolicyCheckers.values()) checker.close()
  typePolicyCheckers.clear()
}
process.once("exit", closeTypePolicyCheckers)
process.once("SIGINT", closeTypePolicyCheckers)
process.once("SIGTERM", closeTypePolicyCheckers)

export const noManualEffectBoundary = Rule.define({
  name: "no-manual-effect-boundary",
  meta: Rule.meta({
    type: "problem",
    description: "Keep Effect execution and Schema decoding inside the current Effect."
  }),
  create: function*() {
    const context = yield* RuleContext
    const moduleBindings = new Map()
    const directBoundaryBindings = new Set()
    const managedRuntimeFactories = new Set()
    const managedRuntimeValues = new Set()
    const importBindingNodes = new WeakSet()
    const aliasBindingNodes = new WeakSet()
    const reportedRanges = new Set()
    let typedRuntimeViolations = []
    const typedRuntimeRanges = new Set()

    const getModule = (node) => {
      if (node.type === "Identifier") {
        const variable = findVariable(context, node)
        return moduleBindings.get(variable)
      }
      const member = getStaticMember(node)
      if (member === undefined) return undefined
      const object = unwrapExpression(member.expression.object)
      if (object.type !== "Identifier") return undefined
      const variable = findVariable(context, object)
      if (moduleBindings.get(variable) !== "effect") return undefined
      return rootEffectModuleNames.get(member.name)
    }

    const getMemberName = (node) => {
      const staticName = getStaticMemberName(node)
      if (staticName !== undefined) return staticName
      if (
        node.computed &&
        node.property.type === "MemberExpression" &&
        !node.property.computed &&
        node.property.object.type === "Identifier" &&
        node.property.object.name === "Symbol" &&
        node.property.property.type === "Identifier" &&
        node.property.property.name === "asyncDispose"
      ) {
        return "asyncDispose"
      }
      return undefined
    }

    const isManagedRuntimeMake = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type !== "CallExpression") return false
      const callee = unwrapExpression(expression.callee)
      if (callee.type === "Identifier") {
        const variable = findVariable(context, callee)
        return managedRuntimeFactories.has(variable)
      }
      if (callee.type !== "MemberExpression") return false
      const memberName = getMemberName(callee)
      return memberName === "make" && getModule(callee.object) === "effect/ManagedRuntime"
    }

    const containsManagedRuntimeMake = (node) => {
      if (node === null || node === undefined) return false
      node = unwrapExpression(node)
      if (isManagedRuntimeMake(node)) return true
      if (node.type === "Identifier") {
        const variable = findVariable(context, node)
        return managedRuntimeValues.has(variable)
      }
      if (node.type === "ArrowFunctionExpression" || node.type === "FunctionExpression") {
        return containsManagedRuntimeMake(node.body)
      }
      if (node.type !== "CallExpression") return false
      const callee = node.callee
      if (callee.type === "MemberExpression" && getMemberName(callee) === "pipe") {
        if (containsManagedRuntimeMake(callee.object)) return true
        return node.arguments.some(containsManagedRuntimeMake)
      }
      return false
    }

    const resolveBoundary = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "Identifier") {
        return directBoundaryBindings.has(findVariable(context, expression))
      }
      const member = getStaticMember(expression)
      if (member === undefined) return false
      const moduleName = getModule(member.expression.object)
      return boundaryModules.get(moduleName)?.has(member.name) === true
    }

    const getPatternProperty = (property) => {
      if (
        property.type !== "Property" || property.computed &&
          property.key.type !== "Literal" && property.key.type !== "TemplateLiteral"
      ) return undefined
      let name
      if (property.key.type === "Identifier") name = property.key.name
      else if (property.key.type === "Literal") name = property.key.value
      else if (property.key.expressions.length === 0 && property.key.quasis.length === 1) {
        name = property.key.quasis[0].value.cooked
      }
      if (typeof name !== "string") return undefined
      let value = property.value
      if (value.type === "AssignmentPattern") value = value.left
      if (value.type !== "Identifier") return undefined
      return { name, value }
    }

    const registerConstAlias = (node) => {
      const declaration = context.sourceCode.getAncestors(node).findLast(
        (ancestor) => ancestor.type === "VariableDeclaration"
      )
      if (declaration?.kind !== "const" || node.init === null) return
      if (node.id.type === "Identifier") {
        const variable = context.sourceCode.getDeclaredVariables(node).find(
          (candidate) => candidate.name === node.id.name
        )
        if (variable === undefined) return
        const moduleName = getModule(node.init)
        if (moduleName !== undefined) moduleBindings.set(variable, moduleName)
        const initializer = unwrapExpression(node.init)
        if (resolveBoundary(initializer)) directBoundaryBindings.add(variable)
        if (
          initializer.type === "Identifier" &&
          managedRuntimeFactories.has(findVariable(context, initializer))
        ) {
          managedRuntimeFactories.add(variable)
        }
        if (containsManagedRuntimeMake(initializer)) managedRuntimeValues.add(variable)
        aliasBindingNodes.add(node.id)
        return
      }
      if (node.id.type !== "ObjectPattern") return
      const moduleName = getModule(node.init)
      const runtimeValue = containsManagedRuntimeMake(node.init)
      for (const property of node.id.properties) {
        const binding = getPatternProperty(property)
        if (binding === undefined) continue
        const variable = context.sourceCode.getDeclaredVariables(node).find(
          (candidate) => candidate.name === binding.value.name
        )
        if (variable === undefined) continue
        if (
          boundaryModules.get(moduleName)?.has(binding.name) === true ||
          runtimeValue && managedRuntimeBoundaries.has(binding.name) ||
          typedRuntimeRanges.has(`${binding.value.start}:${binding.value.end}`)
        ) {
          directBoundaryBindings.add(variable)
        }
        if (moduleName === "effect/ManagedRuntime" && binding.name === "make") {
          managedRuntimeFactories.add(variable)
        }
        aliasBindingNodes.add(binding.value)
      }
    }

    const report = (node) => {
      const range = `${node.start}:${node.end}`
      if (reportedRanges.has(range)) return Effect.void
      reportedRanges.add(range)
      return context.report(Diagnostic.make({ node, message: manualBoundaryMessage }))
    }

    return {
      Program: (node) => {
        const isTypeScript = context.filename.endsWith(".ts") || context.filename.endsWith(".tsx")
        if (!isTypeScript) return Effect.void
        try {
          typedRuntimeViolations = getTypePolicyChecker(context.cwd).analyze({
            filename: context.filename,
            sourceText: context.sourceCode.text
          }).manualRuntimeBoundaries
          for (const violation of typedRuntimeViolations) {
            typedRuntimeRanges.add(`${violation.start}:${violation.end}`)
          }
          return Effect.void
        } catch (cause) {
          return context.report({
            node,
            message: `Could not verify ManagedRuntime boundaries. The type aware rule must run successfully: ${
              String(cause)
            }`
          })
        }
      },
      ImportDeclaration: (node) => {
        if (node.importKind === "type") return Effect.void
        const source = node.source.value
        if (source !== "effect" && source !== "effect/ManagedRuntime" && !boundaryModules.has(source)) {
          return Effect.void
        }
        return Effect.sync(() => {
          const variables = context.sourceCode.getDeclaredVariables(node)
          const variableByName = new Map(variables.map((variable) => [variable.name, variable]))
          for (const specifier of node.specifiers) {
            if (specifier.importKind === "type") continue
            importBindingNodes.add(specifier.local)
            const variable = variableByName.get(specifier.local.name)
            if (variable === undefined) continue
            if (specifier.type === "ImportNamespaceSpecifier") {
              moduleBindings.set(variable, source)
              continue
            }
            if (specifier.type !== "ImportSpecifier" || specifier.imported.type !== "Identifier") continue
            const importedName = specifier.imported.name
            if (source === "effect") {
              const moduleName = rootEffectModuleNames.get(importedName)
              if (moduleName !== undefined) moduleBindings.set(variable, moduleName)
              continue
            }
            if (source === "effect/ManagedRuntime" && importedName === "make") {
              managedRuntimeFactories.add(variable)
              continue
            }
            if (boundaryModules.get(source)?.has(importedName) === true) {
              directBoundaryBindings.add(variable)
            }
          }
        })
      },
      VariableDeclarator: (node) => {
        return Effect.sync(() => registerConstAlias(node))
      },
      Identifier: (node) => {
        if (typedRuntimeRanges.has(`${node.start}:${node.end}`)) return report(node)
        if (importBindingNodes.has(node) || aliasBindingNodes.has(node)) return Effect.void
        const variable = findVariable(context, node)
        if (
          !directBoundaryBindings.has(variable) ||
          variable.references.every((reference) => reference.identifier !== node)
        ) return Effect.void
        return report(node)
      },
      MemberExpression: (node) => {
        if (typedRuntimeRanges.has(`${node.start}:${node.end}`)) return report(node)
        const memberName = getMemberName(node)
        if (memberName === undefined) return Effect.void
        const moduleName = getModule(node.object)
        if (moduleName !== undefined && boundaryModules.get(moduleName)?.has(memberName) === true) {
          return report(node)
        }
        const runtimeBoundary = managedRuntimeBoundaries.has(memberName) || memberName === "asyncDispose"
        if (!runtimeBoundary) return Effect.void
        if (containsManagedRuntimeMake(node.object)) return report(node)
        if (node.object.type !== "Identifier") return Effect.void
        const variable = findVariable(context, node.object)
        if (managedRuntimeValues.has(variable)) return report(node)
        return Effect.void
      },
      "Program:exit": (node) => {
        const isTypeScript = context.filename.endsWith(".ts") || context.filename.endsWith(".tsx")
        if (!isTypeScript) return Effect.void
        return Effect.forEach(
          typedRuntimeViolations.filter((violation) => !reportedRanges.has(`${violation.start}:${violation.end}`)),
          (violation) => {
            const deepestNode = context.sourceCode.getNodeByRangeIndex(violation.start)
            let reportNode = deepestNode ?? node
            if (deepestNode !== null) {
              const ancestors = context.sourceCode.getAncestors(deepestNode)
              for (const ancestor of ancestors) {
                if (ancestor.start === violation.start && ancestor.end === violation.end) reportNode = ancestor
              }
              const violationText = context.sourceCode.text.slice(violation.start, violation.end)
              if (violationText.endsWith("[Symbol.asyncDispose]")) {
                const call = ancestors.findLast((ancestor) =>
                  ancestor.type === "CallExpression" && ancestor.start === violation.start
                )
                if (call !== undefined) reportNode = call
              }
            }
            return report(reportNode)
          },
          { discard: true }
        )
      }
    }
  }
})

const taggedEffectErrorMessage = (errorTypes) => {
  const displayedTypes = errorTypes.join(" | ")
  return `Effect error channel contains an untagged type: ${displayedTypes}. Every non never Effect error must have a required _tag. Define library errors with Schema.TaggedErrorClass and propagate or handle them by _tag. Do not hide the error with a cast, any, or an unconstrained generic.`
}

const layerNameMessage = (name) =>
  `Layer value ${name} must use layer or layerX with PascalCase descriptive suffix. xLayer is invalid. Functions returning Layer remain camelCase.`

const serviceTagMapMessage =
  "Do not access a Context service by mapping its key. Use Effect.gen and yield the service explicitly with const service = yield* Service. When defining a reusable function, use Effect.fnUntraced with the generator body as its first argument."

const makeTypePolicyRule = ({ description, failure, select, message }) => ({
  meta: Rule.meta({ type: "problem", description }),
  createOnce(context) {
    return {
      before: () => {
        const isTypeScript = context.filename.endsWith(".ts") || context.filename.endsWith(".tsx")
        if (!isTypeScript) return false
        return true
      },
      "Program:exit": (node) => {
        let violations
        try {
          violations = select(
            getTypePolicyChecker(context.cwd).analyze({
              filename: context.filename,
              sourceText: context.sourceCode.text
            })
          )
        } catch (cause) {
          const detail = String(cause)
          context.report({ node, message: `${failure}: ${detail}` })
          return
        }
        for (const violation of violations) {
          const deepestNode = context.sourceCode.getNodeByRangeIndex(violation.start)
          let reportNode = deepestNode ?? node
          if (deepestNode !== null) {
            const ancestors = context.sourceCode.getAncestors(deepestNode)
            for (const ancestor of ancestors) {
              if (ancestor.start === violation.start && ancestor.end === violation.end) reportNode = ancestor
            }
          }
          context.report({ node: reportNode, message: message(violation) })
        }
      }
    }
  }
})

export const requireTaggedEffectError = makeTypePolicyRule({
  description: "Require every non never member of an Effect error channel to have a _tag.",
  failure: "Could not verify Effect error tags. The type aware rule must run successfully",
  select: (analysis) => analysis.taggedEffectErrors,
  message: (violation) => taggedEffectErrorMessage(violation.errorTypes)
})

export const noUnknownEffectChannelsMessage = (channels) => {
  let channelLabel = "channel"
  if (channels.length !== 1) channelLabel = "channels"
  return `Effect ${
    channels.join(" and ")
  } ${channelLabel} must not be unknown. Use a specific tagged error type and explicit Context services. The success channel may be unknown.`
}
export const noUnknownEffectChannels = makeTypePolicyRule({
  description: "Reject unknown Effect error and requirements channels.",
  failure: "Could not verify Effect error and requirements channels",
  select: (analysis) => analysis.unknownEffectChannels,
  message: (violation) => noUnknownEffectChannelsMessage(violation.channels)
})

export const requireLayerName = makeTypePolicyRule({
  description: "Require every stored Effect Layer value to use layer or layerX naming.",
  failure: "Could not verify Layer value names. The type aware rule must run successfully",
  select: (analysis) => analysis.layerNameViolations,
  message: (violation) => layerNameMessage(violation.name)
})

export const noServiceTagMap = makeTypePolicyRule({
  description: "Require Context services to be yielded explicitly instead of mapped from their key.",
  failure: "Could not verify Context service access. The type aware rule must run successfully",
  select: (analysis) => analysis.serviceTagMaps,
  message: () => serviceTagMapMessage
})

export const bareSqlRowTypeMessage =
  "Do not assert SQL row shapes with a bare sql<T> type argument in library code. Decode request and result rows with SqlSchema at the database boundary."
export const noBareSqlRowType = makeTypePolicyRule({
  description: "Require SqlSchema decoding for library SQL reads.",
  failure: "Could not verify SQL row type arguments",
  select: (analysis) => analysis.bareSqlRowTypes,
  message: () => bareSqlRowTypeMessage
})

export const effectSyncReturningEffectMessage =
  "Do not return an Effect from an Effect.sync thunk. Use Effect.suspend when the thunk itself produces an Effect, or compose the synchronous mutation and return the Effect with Effect.as."
export const noEffectSyncReturningEffect = makeTypePolicyRule({
  description: "Keep Effect.sync thunks synchronous and non Effect valued.",
  failure: "Could not verify Effect.sync return types",
  select: (analysis) => analysis.effectSyncReturnsEffect,
  message: () => effectSyncReturningEffectMessage
})

export const effectTestsUseEffectMessage =
  "This test callback returns an Effect. Use @effect/vitest it.effect, it.live, or effect so the Effect runtime, scope, failures, and TestClock are managed by the test harness."
export const effectTestsUseEffect = makeTypePolicyRule({
  description: "Run Effect valued tests through @effect/vitest.",
  failure: "Could not verify Effect valued test callbacks",
  select: (analysis) => analysis.effectValuedPlainTests,
  message: () => effectTestsUseEffectMessage
})

export const requireSchemaTaggedErrorMessage =
  "Define every library owned typed error with Schema.TaggedErrorClass<Self>(identifier)(\"Tag\", fields). Do not use Data.TaggedError or a custom tagged Error class."
export const requireSchemaTaggedError = makeTypePolicyRule({
  description: "Require Schema.TaggedErrorClass for library owned typed errors.",
  failure: "Could not verify tagged error declarations",
  select: (analysis) => analysis.schemaTaggedErrorViolations,
  message: () => requireSchemaTaggedErrorMessage
})

export const taggedErrorInstanceofMessage =
  "Do not use instanceof to discriminate a tagged error. Match its stable _tag with Effect.catchTag, catchTags, catchReason, catchReasons, or an explicit _tag branch."
export const noInstanceofTaggedError = makeTypePolicyRule({
  description: "Discriminate tagged errors by _tag.",
  failure: "Could not verify tagged error instanceof checks",
  select: (analysis) => analysis.taggedErrorInstanceof,
  message: () => taggedErrorInstanceofMessage
})

export const noClassInstanceFactoryMessage =
  "Instantiate this class directly where the value is needed. Do not hide construction in a mapper or factory function. Prefer local verbosity so every construction site stays explicit."
export const noClassInstanceFactory = makeTypePolicyRule({
  description: "Instantiate classes directly instead of defining single purpose factories.",
  failure: "Could not verify class instance factories",
  select: (analysis) => analysis.classInstanceFactories,
  message: () => noClassInstanceFactoryMessage
})

export const durationInputAtConfigBoundaryMessage =
  "Express configurable durations as Duration.Input. Do not expose a bare number with a unit suffix. Convert the input once when the Layer or service is constructed."
export const durationInputAtConfigBoundary = makeTypePolicyRule({
  description: "Require Duration.Input at consumer configuration boundaries.",
  failure: "Could not verify configurable duration inputs",
  select: (analysis) => analysis.durationInputViolations,
  message: (violation) => {
    if (violation.property === undefined) return durationInputAtConfigBoundaryMessage
    return `${durationInputAtConfigBoundaryMessage} Imported property: ${violation.property}.`
  }
})

export const noModuleErrorHelperMessage =
  "Do not hide tagged error construction or translation in a module helper. Construct the error inline at each raising call site, even when that is more verbose. When multiple modules raise the same error, use exactly one shared constructor under src/internal."
export const noModuleErrorHelper = makeTypePolicyRule({
  description: "Keep tagged error construction and translation at the raising call site.",
  failure: "Could not verify module error helpers",
  select: (analysis) => analysis.moduleErrorHelpers,
  message: () => noModuleErrorHelperMessage
})

export default Plugin.define({
  name: "effect-local",
  rules: {
    noYieldEffectSync,
    noSemaphoreEffectSync,
    noNestedCalls,
    noJsonParseStringify,
    noUnnecessaryEffectForwarding,
    noFunctionEffectGen,
    noImplicitDefectConversion,
    noExportedSchemaCodecAlias,
    noDetachedFork,
    noInternalEntrypointExport,
    noTestWallClockWait,
    noManualEffectBoundary,
    noBareSqlRowType,
    noEffectSyncReturningEffect,
    effectTestsUseEffect,
    requireSchemaTaggedError,
    noInstanceofTaggedError,
    noClassInstanceFactory,
    durationInputAtConfigBoundary,
    noModuleErrorHelper,
    requireTaggedEffectError,
    noUnknownEffectChannels,
    requireLayerName,
    noServiceTagMap
  }
})
