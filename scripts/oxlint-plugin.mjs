import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import { Diagnostic, Plugin, Rule, RuleContext } from "oxlint-plugin-effect/rule-bindings"
import { makeEffectTypePolicyChecker } from "./tagged-effect-errors.mjs"

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

export const noYieldEffectSync = Rule.define({
  name: "no-yield-effect-sync",
  meta: Rule.meta({
    type: "problem",
    description: "Call synchronous code directly inside Effect.gen instead of yielding Effect.sync."
  }),
  create: function*() {
    const context = yield* RuleContext
    const effectNamespaces = new Set()
    return {
      ImportDeclaration: (node) => {
        if (node.source.value !== "effect/Effect") return Effect.void
        return Effect.sync(() => {
          for (const specifier of node.specifiers) {
            if (specifier.type === "ImportNamespaceSpecifier") effectNamespaces.add(specifier.local.name)
          }
        })
      },
      YieldExpression: (node) => {
        if (!node.delegate || node.argument?.type !== "CallExpression") return Effect.void
        const callee = node.argument.callee
        if (callee.type !== "MemberExpression" || callee.computed) return Effect.void
        if (callee.object.type !== "Identifier" || !effectNamespaces.has(callee.object.name)) return Effect.void
        if (callee.property.type !== "Identifier" || callee.property.name !== "sync") return Effect.void
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
    const effectNamespaces = new Set()
    return {
      ImportDeclaration: (node) => {
        if (node.source.value !== "effect/Effect") return Effect.void
        return Effect.sync(() => {
          for (const specifier of node.specifiers) {
            if (specifier.type === "ImportNamespaceSpecifier") effectNamespaces.add(specifier.local.name)
          }
        })
      },
      CallExpression: (node) => {
        const callee = node.callee
        if (callee.type !== "MemberExpression" || callee.computed) return Effect.void
        if (callee.property.type !== "Identifier" || callee.property.name !== "withPermit") return Effect.void
        const argument = node.arguments[0]
        if (argument?.type !== "CallExpression") return Effect.void
        const innerCallee = argument.callee
        if (innerCallee.type !== "MemberExpression" || innerCallee.computed) return Effect.void
        if (innerCallee.object.type !== "Identifier" || !effectNamespaces.has(innerCallee.object.name)) {
          return Effect.void
        }
        if (innerCallee.property.type !== "Identifier" || innerCallee.property.name !== "sync") return Effect.void
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
        if (node.source.value !== "effect/Function" || node.importKind === "type") return Effect.void
        return Effect.sync(() => {
          for (const specifier of node.specifiers) {
            if (specifier.type === "ImportNamespaceSpecifier") {
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
      if (parameterVariable.references.length !== 1 || parameterVariable.references[0].identifier !== argument) {
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
      if (parameterVariable.references.length !== 1 || parameterVariable.references[0].identifier !== identifier) {
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

const makeOfficialBindingResolver = (context, effectExports) => {
  const moduleBindings = new Map()
  const exportBindings = new Map()
  const rootNamespaces = new Set()
  const importBindingNodes = new WeakSet()
  const aliasBindingNodes = new WeakSet()

  const resolveModule = (node) => {
    const expression = unwrapExpression(node)
    if (expression.type === "Identifier") return moduleBindings.get(findVariable(context, expression))
    const member = getStaticMember(expression)
    if (member?.name !== "Effect") return undefined
    const root = unwrapExpression(member.expression.object)
    if (root.type !== "Identifier" || !rootNamespaces.has(findVariable(context, root))) return undefined
    return "effect/Effect"
  }

  const resolveExport = (node) => {
    const expression = unwrapExpression(node)
    if (expression.type === "Identifier") return exportBindings.get(findVariable(context, expression))
    const member = getStaticMember(expression)
    if (member === undefined) return undefined
    const moduleName = resolveModule(member.expression.object)
    if (moduleName === "effect/Effect" && effectExports.has(member.name)) {
      return { moduleName, name: member.name }
    }
    if (moduleName === "effect/Function" && member.name === "pipe") {
      return { moduleName, name: "pipe" }
    }
    return undefined
  }

  const registerImport = (node) => {
    if (node.importKind === "type") return
    const source = node.source.value
    for (const specifier of node.specifiers) importBindingNodes.add(specifier.local)
    if (source === "effect/Effect" || source === "effect/Function") {
      for (const specifier of node.specifiers) {
        const variable = importedVariable(context, node, specifier.local.name)
        if (variable === undefined) continue
        if (specifier.type === "ImportNamespaceSpecifier") {
          moduleBindings.set(variable, source)
        } else if (
          specifier.type === "ImportSpecifier" && specifier.importKind !== "type" &&
          specifier.imported.type === "Identifier"
        ) {
          const name = specifier.imported.name
          if (
            (source === "effect/Effect" && effectExports.has(name)) ||
            (source === "effect/Function" && name === "pipe")
          ) {
            exportBindings.set(variable, { moduleName: source, name })
          }
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
        specifier.imported.type === "Identifier" && specifier.imported.name === "Effect"
      ) {
        moduleBindings.set(variable, "effect/Effect")
      }
    }
  }

  const registerConstAlias = (node) => {
    const declaration = context.sourceCode.getAncestors(node).findLast(
      (ancestor) => ancestor.type === "VariableDeclaration"
    )
    if (declaration?.kind !== "const") return
    if (node.init === null || node.init === undefined) return
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
    if (moduleName !== "effect/Effect") return
    for (const property of node.id.properties) {
      if (property.type !== "Property" || property.computed) continue
      let key = property.key.value
      if (property.key.type === "Identifier") key = property.key.name
      if (typeof key !== "string" || !effectExports.has(key)) continue
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
        variable.references.length === 1 && variable.references[0].identifier === argument
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

const effectRootModules = new Map([
  ["Effect", "effect/Effect"],
  ["ManagedRuntime", "effect/ManagedRuntime"],
  ["Runtime", "effect/Runtime"],
  ["Schema", "effect/Schema"],
  ["SchemaParser", "effect/SchemaParser"]
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
      return effectRootModules.get(member.name)
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
      Program: (node) =>
        Effect.sync(() => {
          const isTypeScript = context.filename.endsWith(".ts") || context.filename.endsWith(".tsx")
          if (!isTypeScript) return
          try {
            typedRuntimeViolations = getTypePolicyChecker(context.cwd).analyze({
              filename: context.filename,
              sourceText: context.sourceCode.text
            }).manualRuntimeBoundaries
            for (const violation of typedRuntimeViolations) {
              typedRuntimeRanges.add(`${violation.start}:${violation.end}`)
            }
          } catch (cause) {
            context.report({
              node,
              message: `Could not verify ManagedRuntime boundaries. The type aware rule must run successfully: ${
                String(cause)
              }`
            })
          }
        }),
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
              const moduleName = effectRootModules.get(importedName)
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
        return Effect.sync(() => {
          for (const violation of typedRuntimeViolations) {
            if (reportedRanges.has(`${violation.start}:${violation.end}`)) continue
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
            report(reportNode)
          }
        })
      }
    }
  }
})

const taggedEffectErrorMessage = (errorTypes) => {
  const displayedTypes = errorTypes.join(" | ")
  return `Effect error channel contains an untagged type: ${displayedTypes}. Every non never Effect error must have a required _tag. Define library errors with Schema.TaggedErrorClass and propagate or handle them by _tag. Do not hide the error with a cast, unknown, any, or an unconstrained generic.`
}

const layerPascalCaseMessage = (name) =>
  `Layer value ${name} must use PascalCase. Functions that return a Layer remain camelCase.`

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

export const requireLayerPascalCase = makeTypePolicyRule({
  description: "Require every Effect Layer value binding to use PascalCase.",
  failure: "Could not verify Layer value names. The type aware rule must run successfully",
  select: (analysis) => analysis.layerNames,
  message: (violation) => layerPascalCaseMessage(violation.name)
})

export const noServiceTagMap = makeTypePolicyRule({
  description: "Require Context services to be yielded explicitly instead of mapped from their key.",
  failure: "Could not verify Context service access. The type aware rule must run successfully",
  select: (analysis) => analysis.serviceTagMaps,
  message: () => serviceTagMapMessage
})

export default Plugin.define({
  name: "effect-local",
  rules: {
    noYieldEffectSync,
    noSemaphoreEffectSync,
    noNestedCalls,
    noUnnecessaryEffectForwarding,
    noFunctionEffectGen,
    noImplicitDefectConversion,
    noManualEffectBoundary,
    requireTaggedEffectError,
    requireLayerPascalCase,
    noServiceTagMap
  }
})
