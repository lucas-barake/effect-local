import { Testing } from "oxlint-plugin-effect/rule-bindings"
import { noSemaphoreEffectSync, noYieldEffectSync } from "./oxlint-plugin.mjs"

const effectImport = Testing.importDeclWithSpecifiers(
  "effect/Effect",
  [Testing.importNamespaceSpecifier("Fx")]
)
const syncCall = Testing.callOfMember("Fx", "sync")

Testing.expectDiagnostics(
  Testing.runRuleMulti(noYieldEffectSync, [
    ["ImportDeclaration", effectImport],
    ["YieldExpression", Testing.yieldExpr(syncCall, true)]
  ]),
  [{ message: "Do not yield Effect.sync inside Effect.gen. Execute the synchronous code directly." }]
)

Testing.expectNoDiagnostics(
  Testing.runRuleMulti(noYieldEffectSync, [
    ["ImportDeclaration", effectImport],
    ["YieldExpression", Testing.yieldExpr(syncCall, false)],
    ["YieldExpression", Testing.yieldExpr(Testing.callOfMember("Other", "sync"), true)]
  ])
)

Testing.expectDiagnostics(
  Testing.runRuleMulti(noSemaphoreEffectSync, [
    ["ImportDeclaration", effectImport],
    ["CallExpression", {
      type: "CallExpression",
      callee: Testing.memberExpr("gate", "withPermit"),
      arguments: [syncCall],
      optional: false
    }]
  ]),
  [{ message: "Do not acquire a semaphore around Effect.sync. Synchronous JavaScript cannot interleave." }]
)

Testing.expectNoDiagnostics(
  Testing.runRuleMulti(noSemaphoreEffectSync, [
    ["ImportDeclaration", effectImport],
    ["CallExpression", {
      type: "CallExpression",
      callee: Testing.memberExpr("gate", "withPermit"),
      arguments: [Testing.callOfMember("Fx", "gen")],
      optional: false
    }]
  ])
)
