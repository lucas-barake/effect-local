import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import { Diagnostic, Plugin, Rule, RuleContext } from "oxlint-plugin-effect/rule-bindings"
import { makeTaggedEffectErrorChecker } from "./tagged-effect-errors.mjs"

const findVariable = (context, node) => {
  let scope = context.sourceCode.getScope(node)
  while (scope !== null) {
    const variable = scope.set.get(node.name)
    if (variable !== undefined) return variable
    scope = scope.upper
  }
  return undefined
}

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
        return pipe(
          Diagnostic.make({
            node,
            message: "Do not yield Effect.sync inside Effect.gen. Execute the synchronous code directly."
          }),
          (diagnostic) => context.report(diagnostic)
        )
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
        return pipe(
          Diagnostic.make({
            node,
            message: "Do not acquire a semaphore around Effect.sync. Synchronous JavaScript cannot interleave."
          }),
          (diagnostic) => context.report(diagnostic)
        )
      }
    }
  }
})

export const nestedCallMessage =
  "Do not pass a call directly to another call. Compose it with .pipe(...) or pipe(...) from effect/Function."

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
      while (transparentExpressionTypes.has(expression.type)) expression = expression.expression
      return expression
    }

    const getStaticMemberName = (member) => {
      if (!member.computed && member.property.type === "Identifier") return member.property.name
      if (!member.computed) return undefined
      const property = unwrapExpression(member.property)
      if (property.type === "Literal" && property.value === "pipe") return "pipe"
      if (
        property.type === "TemplateLiteral" &&
        property.expressions.length === 0 &&
        property.quasis.length === 1 &&
        property.quasis[0].value.cooked === "pipe"
      ) {
        return "pipe"
      }
      return undefined
    }

    const isStaticPipeMemberCall = (node) => {
      const callee = unwrapExpression(node.callee)
      return callee.type === "MemberExpression" && getStaticMemberName(callee) === "pipe"
    }

    const isImportedNamespacePipeCall = (node) => {
      const callee = unwrapExpression(node.callee)
      if (!isStaticPipeMemberCall(node)) return false
      const namespace = unwrapExpression(callee.object)
      if (namespace.type !== "Identifier") return false
      return pipe(
        findVariable(context, namespace),
        (variable) => importedFunctionNamespaces.has(variable)
      )
    }

    const isReceiverPipeCall = (node) => isStaticPipeMemberCall(node) && !isImportedNamespacePipeCall(node)

    const isImportedPipeCall = (node) => {
      const callee = unwrapExpression(node.callee)
      if (callee.type === "Identifier") {
        return pipe(
          findVariable(context, callee),
          (variable) => importedPipeBindings.has(variable)
        )
      }
      return isImportedNamespacePipeCall(node)
    }

    const isPipeCall = (node) => isReceiverPipeCall(node) || isImportedPipeCall(node)

    const isInsidePipeOperator = (node) => {
      let child = node
      const ancestors = context.sourceCode.getAncestors(node)
      for (let index = ancestors.length - 1; index >= 0; index--) {
        const ancestor = ancestors[index]
        if (ancestor.type === "CallExpression" && isPipeCall(ancestor)) {
          let firstOperatorIndex = 1
          if (isReceiverPipeCall(ancestor)) firstOperatorIndex = 0
          if (ancestor.arguments.slice(firstOperatorIndex).includes(child)) return true
        }
        child = ancestor
      }
      return false
    }

    const report = (node, message) =>
      pipe(
        Diagnostic.make({ node, message }),
        (diagnostic) => context.report(diagnostic)
      )

    const importedVariable = (node, localName) =>
      context.sourceCode.getDeclaredVariables(node).find((variable) => variable.name === localName)

    return {
      ImportDeclaration: (node) => {
        if (node.source.value !== "effect/Function" || node.importKind === "type") return Effect.void
        return Effect.sync(() => {
          for (const specifier of node.specifiers) {
            if (specifier.type === "ImportNamespaceSpecifier") {
              const variable = importedVariable(node, specifier.local.name)
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
            const variable = importedVariable(node, specifier.local.name)
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
          return report(node, chainedPipeMessage)
        }
        if (isInsidePipeOperator(node) || isPipeCall(node)) return Effect.void
        const nestedCalls = node.arguments
          .map(unwrapExpression)
          .filter((argument) => argument.type === "CallExpression")
        if (nestedCalls.length === 0) return Effect.void
        return Effect.forEach(
          nestedCalls,
          (nestedCall) => report(nestedCall, nestedCallMessage),
          { discard: true }
        )
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
      while (transparentExpressionTypes.has(expression.type)) expression = expression.expression
      return expression
    }

    const importedVariable = (node, localName) =>
      context.sourceCode.getDeclaredVariables(node).find((variable) => variable.name === localName)

    const staticMember = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type !== "MemberExpression" || expression.computed) return undefined
      if (expression.property.type !== "Identifier") return undefined
      return { expression, name: expression.property.name }
    }

    const resolveModule = (node) => {
      const expression = unwrapExpression(node)
      if (expression.type === "Identifier") {
        return pipe(findVariable(context, expression), (variable) => moduleNamespaces.get(variable))
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
        return pipe(findVariable(context, expression), (variable) => directBindings.get(variable))
      }
      const member = staticMember(expression)
      if (member === undefined) return undefined
      const moduleName = resolveModule(member.expression.object)
      if (moduleName === undefined) return undefined
      const dataIndex = pipe(dataLastEffectExports.get(moduleName), (exports) => exports?.get(member.name))
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
          const variable = importedVariable(node, specifier.local.name)
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
      if (source !== "effect") return
      for (const specifier of node.specifiers) {
        const variable = importedVariable(node, specifier.local.name)
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

    const inspectForwarder = (node) => {
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
      return pipe(
        Diagnostic.make({
          node,
          message: unnecessaryEffectForwardingMessage(combinator.canonicalName, replacement)
        }),
        (diagnostic) => context.report(diagnostic)
      )
    }

    return {
      ImportDeclaration: (node) => Effect.sync(() => registerImport(node)),
      ArrowFunctionExpression: inspectForwarder
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

    const getModule = (node) => {
      if (node.type === "Identifier") {
        const variable = findVariable(context, node)
        return moduleBindings.get(variable)
      }
      if (node.type !== "MemberExpression" || node.computed) return undefined
      if (node.object.type !== "Identifier" || node.property.type !== "Identifier") return undefined
      const variable = findVariable(context, node.object)
      if (moduleBindings.get(variable) !== "effect") return undefined
      return effectRootModules.get(node.property.name)
    }

    const getMemberName = (node) => {
      if (!node.computed && node.property.type === "Identifier") return node.property.name
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
      if (node.type !== "CallExpression") return false
      const callee = node.callee
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

    const report = (node) =>
      pipe(
        Diagnostic.make({ node, message: manualBoundaryMessage }),
        (diagnostic) => context.report(diagnostic)
      )

    return {
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
        if (!containsManagedRuntimeMake(node.init)) return Effect.void
        return Effect.sync(() => {
          for (const variable of context.sourceCode.getDeclaredVariables(node)) {
            managedRuntimeValues.add(variable)
          }
        })
      },
      Identifier: (node) => {
        if (importBindingNodes.has(node)) return Effect.void
        const variable = findVariable(context, node)
        if (!directBoundaryBindings.has(variable)) return Effect.void
        return report(node)
      },
      MemberExpression: (node) => {
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
      }
    }
  }
})

const taggedEffectErrorMessage = (errorTypes) => {
  const displayedTypes = errorTypes.join(" | ")
  return `Effect error channel contains an untagged type: ${displayedTypes}. Every non never Effect error must have a required _tag. Define library errors with Schema.TaggedErrorClass and propagate or handle them by _tag. Do not hide the error with a cast, unknown, any, or an unconstrained generic.`
}

export const requireTaggedEffectError = {
  meta: Rule.meta({
    type: "problem",
    description: "Require every non never member of an Effect error channel to have a _tag."
  }),
  createOnce(context) {
    let checker
    let activeFiles = 0
    let closeRequested = false
    const closeChecker = () => {
      if (checker === undefined) return
      if (activeFiles > 0) {
        closeRequested = true
        return
      }
      checker.close()
      checker = undefined
    }
    const handleSignal = () => {
      closeRequested = true
      closeChecker()
    }
    process.once("exit", closeChecker)
    process.once("SIGINT", handleSignal)
    process.once("SIGTERM", handleSignal)
    return {
      before: () => {
        const isTypeScript = context.filename.endsWith(".ts") || context.filename.endsWith(".tsx")
        if (!isTypeScript) return false
        if (checker === undefined) checker = makeTaggedEffectErrorChecker({ cwd: context.cwd })
        activeFiles++
        return true
      },
      "Program:exit": (node) => {
        let violations
        try {
          violations = checker.find({
            filename: context.filename,
            sourceText: context.sourceCode.text
          })
        } catch (cause) {
          const detail = String(cause)
          context.report({
            node,
            message: `Could not verify Effect error tags. The type aware rule must run successfully: ${detail}`
          })
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
          context.report({ node: reportNode, message: taggedEffectErrorMessage(violation.errorTypes) })
        }
      },
      after: () => {
        activeFiles--
        if (closeRequested) closeChecker()
      }
    }
  }
}

export default Plugin.define({
  name: "effect-local",
  rules: {
    noYieldEffectSync,
    noSemaphoreEffectSync,
    noNestedCalls,
    noUnnecessaryEffectForwarding,
    noManualEffectBoundary,
    requireTaggedEffectError
  }
})
