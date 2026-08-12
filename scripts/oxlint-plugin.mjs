import { Diagnostic, Plugin, Rule, RuleContext } from "oxlint-plugin-effect/rule-bindings"
import * as Effect from "effect/Effect"

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

export default Plugin.define({
  name: "effect-local",
  rules: { noYieldEffectSync, noSemaphoreEffectSync }
})
