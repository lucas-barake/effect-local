import { pipe } from "effect/Function"
import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { existsSync, mkdtempSync, rmSync, unlinkSync, writeFileSync } from "node:fs"
import { resolve } from "node:path"
import { Testing } from "oxlint-plugin-effect/rule-bindings"
import {
  chainedPipeMessage,
  manualBoundaryMessage,
  nestedCallMessage,
  noNestedCalls,
  noSemaphoreEffectSync,
  noUnnecessaryEffectForwarding,
  noYieldEffectSync,
  unnecessaryEffectForwardingMessage
} from "./oxlint-plugin.mjs"
import { makeTaggedEffectErrorChecker } from "./tagged-effect-errors.mjs"

const effectImport = Testing.importDeclWithSpecifiers(
  "effect/Effect",
  [Testing.importNamespaceSpecifier("Fx")]
)
const syncCall = Testing.callOfMember("Fx", "sync")

pipe(
  Testing.runRuleMulti(noYieldEffectSync, [
    ["ImportDeclaration", effectImport],
    ["YieldExpression", Testing.yieldExpr(syncCall, true)]
  ]),
  (diagnostics) =>
    Testing.expectDiagnostics(
      diagnostics,
      [{ message: "Do not yield Effect.sync inside Effect.gen. Execute the synchronous code directly." }]
    )
)

pipe(
  Testing.callOfMember("Other", "sync"),
  (otherSyncCall) =>
    pipe(
      Testing.runRuleMulti(noYieldEffectSync, [
        ["ImportDeclaration", effectImport],
        ["YieldExpression", Testing.yieldExpr(syncCall, false)],
        ["YieldExpression", Testing.yieldExpr(otherSyncCall, true)]
      ]),
      (diagnostics) => Testing.expectNoDiagnostics(diagnostics)
    )
)

const innermostCall = Testing.callExpr("h")
const innerCall = Testing.callExpr("g", [innermostCall])

pipe(
  Testing.runRuleMulti(noNestedCalls, [
    ["CallExpression", Testing.callExpr("f", [innerCall])],
    ["CallExpression", innerCall]
  ]),
  (diagnostics) =>
    Testing.expectDiagnostics(
      diagnostics,
      [{ message: nestedCallMessage }, { message: nestedCallMessage }]
    )
)

pipe(
  Testing.callExpr("source"),
  (sourceCall) =>
    pipe(
      Testing.callExpr("pipe", [sourceCall]),
      (pipeCall) => Testing.runRule(noNestedCalls, "CallExpression", pipeCall)
    ),
  (diagnostics) => Testing.expectDiagnostics(diagnostics, [{ message: nestedCallMessage }])
)

pipe(
  Testing.runRuleMulti(noNestedCalls, [
    ["CallExpression", Testing.callOfMember("source", "pipe", [Testing.callExpr("transform")])],
    ["CallExpression", Testing.callExpr("standalone")]
  ]),
  (diagnostics) => Testing.expectNoDiagnostics(diagnostics)
)

const importedPipe = Testing.variable("compose")
const importedPipeScope = Testing.scope({ type: "module", variables: [importedPipe] })
const importedPipeTest = Testing.createMockContext()
importedPipeTest.context.sourceCode.getDeclaredVariables = () => [importedPipe]
importedPipeTest.context.sourceCode.getScope = () => importedPipeScope
const importedPipeVisitors = noNestedCalls.create(importedPipeTest.context)
const pipeImport = Testing.importDeclWithSpecifiers(
  "effect/Function",
  [Testing.importSpecifier("pipe", "compose")]
)
importedPipeVisitors.ImportDeclaration(pipeImport)
pipe(
  Testing.callExpr("source"),
  (sourceCall) => Testing.callExpr("compose", [sourceCall]),
  (node) => importedPipeVisitors.CallExpression(node)
)
Testing.expectNoDiagnostics(importedPipeTest.diagnostics)

const importedFunction = Testing.variable("Function")
const importedFunctionScope = Testing.scope({ type: "module", variables: [importedFunction] })
const importedFunctionTest = Testing.createMockContext()
importedFunctionTest.context.sourceCode.getDeclaredVariables = () => [importedFunction]
importedFunctionTest.context.sourceCode.getScope = () => importedFunctionScope
const importedFunctionVisitors = noNestedCalls.create(importedFunctionTest.context)
const importedFunctionSpecifier = Testing.importNamespaceSpecifier("Function")
const importedFunctionDeclaration = Testing.importDeclWithSpecifiers(
  "effect/Function",
  [importedFunctionSpecifier]
)
importedFunctionVisitors.ImportDeclaration(importedFunctionDeclaration)
const importedSourceCall = Testing.callExpr("source")
const importedFunctionPipeCall = Testing.callOfMember("Function", "pipe", [importedSourceCall])
importedFunctionVisitors.CallExpression(importedFunctionPipeCall)
Testing.expectNoDiagnostics(importedFunctionTest.diagnostics)

const computedNamespaceData = Testing.callExpr("f", [Testing.callExpr("g")])
const computedNamespaceOperator = Testing.callExpr("operator", [Testing.callExpr("finish")])
const computedNamespacePipeCall = {
  type: "CallExpression",
  callee: {
    type: "MemberExpression",
    object: { type: "Identifier", name: "Function" },
    property: { type: "Literal", value: "pipe", raw: "\"pipe\"" },
    computed: true,
    optional: false
  },
  arguments: [computedNamespaceData, computedNamespaceOperator],
  optional: false
}
importedFunctionTest.context.sourceCode.getAncestors = (node) => {
  if (node === computedNamespaceData || node === computedNamespaceOperator) return [computedNamespacePipeCall]
  return []
}
importedFunctionVisitors.CallExpression(computedNamespacePipeCall)
importedFunctionVisitors.CallExpression(computedNamespaceData)
importedFunctionVisitors.CallExpression(computedNamespaceOperator)
Testing.expectDiagnostics(importedFunctionTest.diagnostics, [{ message: nestedCallMessage }])

const firstReceiverPipe = Testing.callOfMember("value", "pipe", [Testing.callExpr("first")])
const chainedReceiverPipe = {
  type: "CallExpression",
  callee: {
    type: "MemberExpression",
    object: firstReceiverPipe,
    property: { type: "Identifier", name: "pipe" },
    computed: false,
    optional: false
  },
  arguments: [Testing.callExpr("second")],
  optional: false
}
pipe(
  Testing.runRule(noNestedCalls, "CallExpression", chainedReceiverPipe),
  (diagnostics) => Testing.expectDiagnostics(diagnostics, [{ message: chainedPipeMessage }])
)

const wrappedPipeProperty = {
  type: "TSAsExpression",
  expression: { type: "Literal", value: "pipe", raw: "\"pipe\"" }
}
const computedFirstPipe = {
  type: "CallExpression",
  callee: {
    type: "MemberExpression",
    object: { type: "Identifier", name: "value" },
    property: wrappedPipeProperty,
    computed: true,
    optional: false
  },
  arguments: [],
  optional: false
}
const wrappedComputedChain = {
  type: "CallExpression",
  callee: {
    type: "MemberExpression",
    object: computedFirstPipe,
    property: wrappedPipeProperty,
    computed: true,
    optional: false
  },
  arguments: [],
  optional: false
}
pipe(
  Testing.runRule(noNestedCalls, "CallExpression", wrappedComputedChain),
  (diagnostics) => Testing.expectDiagnostics(diagnostics, [{ message: chainedPipeMessage }])
)

const transparentReceivers = [
  { type: "ParenthesizedExpression", expression: firstReceiverPipe },
  { type: "ChainExpression", expression: firstReceiverPipe },
  { type: "TSNonNullExpression", expression: firstReceiverPipe },
  { type: "TSAsExpression", expression: firstReceiverPipe },
  { type: "TSSatisfiesExpression", expression: firstReceiverPipe },
  { type: "TSTypeAssertion", expression: firstReceiverPipe },
  { type: "TSInstantiationExpression", expression: firstReceiverPipe }
]
const transparentReceiverPairs = transparentReceivers.map((receiver) => [
  "CallExpression",
  {
    type: "CallExpression",
    callee: {
      type: "MemberExpression",
      object: receiver,
      property: { type: "Identifier", name: "pipe" },
      computed: false,
      optional: false
    },
    arguments: [],
    optional: false
  }
])
const transparentReceiverDiagnostics = Testing.runRuleMulti(noNestedCalls, transparentReceiverPairs)
const transparentReceiverExpected = transparentReceivers.map(() => ({ message: chainedPipeMessage }))
Testing.expectDiagnostics(
  transparentReceiverDiagnostics,
  transparentReceiverExpected
)

pipe(
  Testing.runRuleMulti(noSemaphoreEffectSync, [
    ["ImportDeclaration", effectImport],
    ["CallExpression", {
      type: "CallExpression",
      callee: Testing.memberExpr("gate", "withPermit"),
      arguments: [syncCall],
      optional: false
    }]
  ]),
  (diagnostics) =>
    Testing.expectDiagnostics(
      diagnostics,
      [{ message: "Do not acquire a semaphore around Effect.sync. Synchronous JavaScript cannot interleave." }]
    )
)

pipe(
  Testing.runRuleMulti(noSemaphoreEffectSync, [
    ["ImportDeclaration", effectImport],
    ["CallExpression", {
      type: "CallExpression",
      callee: Testing.memberExpr("gate", "withPermit"),
      arguments: [Testing.callOfMember("Fx", "gen")],
      optional: false
    }]
  ]),
  (diagnostics) => Testing.expectNoDiagnostics(diagnostics)
)

const worktree = resolve(import.meta.dirname, "..")

const runOxlintFixture = (name, source, rules) => {
  const directory = pipe(worktree, (root) => resolve(root, "oxlint-fixture-"), mkdtempSync)
  let fixtureName = name
  if (fixtureName.startsWith(".")) fixtureName = fixtureName.slice(1)
  const filename = resolve(directory, fixtureName)
  const configFilename = resolve(directory, "oxlint.json")
  writeFileSync(filename, source)
  const pluginFilename = resolve(worktree, "scripts/oxlint-plugin.mjs")
  const ruleEntries = rules.map((rule) => [`effect-local/${rule}`, "error"])
  const fixtureConfig = pipe(ruleEntries, Object.fromEntries, (configuredRules) => ({
    jsPlugins: [pluginFilename],
    rules: configuredRules
  }))
  pipe(fixtureConfig, JSON.stringify, (configuredSource) => writeFileSync(configFilename, configuredSource))
  try {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "oxlint",
        "--config",
        configFilename,
        "--disable-nested-config",
        filename,
        "--format",
        "json",
        "--threads",
        "1"
      ],
      { cwd: worktree, encoding: "utf8" }
    )
    if (result.error !== undefined) throw result.error
    assert.equal(result.status, 1, result.stderr)
    const output = JSON.parse(result.stdout)
    assert.equal(output.number_of_files, 1, result.stdout)
    return output.diagnostics
  } finally {
    rmSync(directory, { force: true, recursive: true })
  }
}

const nestedCallDiagnostics = runOxlintFixture(
  "no-nested-calls-fixture.mjs",
  `import * as Effect from "effect/Effect"
import { pipe, pipe as compose } from "effect/Function"
import * as Function from "effect/Function"

const value = null
const token = null
const dynamicPipe = "pipe"
const f = null
const g = null
const operator = null
const finish = null

const _nested = f(g(value))
const _wrappedArgument = f((g(value)))
const _receiverInput = f(g(value)).pipe(operator)
const _directInput = pipe(f(g(value)), operator)
const _wrappedDirectInput = (pipe)(f(g(value)), operator)
const _aliasedInput = compose(f(g(value)), operator)
const _namespaceInput = Function.pipe(f(g(value)), operator)
const _wrappedNamespaceInput = (Function).pipe(f(g(value)), operator)
const _computedReceiverInput = f(g(value))["pipe"](operator)
const _dynamicMember = value[dynamicPipe](f(value))
const _chained = value.pipe(operator).pipe(operator)
const _deeper = value.pipe(operator).pipe(operator).pipe(operator)
const _computedChain = value["pipe"](operator)["pipe"](operator)
const _templateChain = value[\`pipe\`](operator)[\`pipe\`](operator)
const _parenthesizedChain = (value.pipe(operator)).pipe(operator)
const _optionalPropertyChain = value?.pipe(operator)?.pipe(operator)
const _optionalCallChain = value.pipe?.(operator).pipe?.(operator)

const _receiverOperator = value.pipe(Effect.ensuring(finish(g(value), token)))
const _computedReceiverOperator = value["pipe"](Effect.ensuring(finish(g(value), token)))
const _directOperator = pipe(value, Effect.ensuring(finish(g(value), token)))
const _wrappedDirectOperator = (pipe)(value, Effect.ensuring(finish(g(value), token)))
const _aliasedOperator = compose(value, Effect.ensuring(finish(g(value), token)))
const _namespaceOperator = Function.pipe(value, Effect.ensuring(finish(g(value), token)))
const _wrappedNamespaceOperator = (Function).pipe(value, Effect.ensuring(finish(g(value), token)))
`,
  ["noNestedCalls"]
)
const onlyNestedCallDiagnostics = nestedCallDiagnostics.every(
  (diagnostic) => diagnostic.code === "effect-local(noNestedCalls)"
)
const nestedCallDetail = JSON.stringify(nestedCallDiagnostics)
assert.equal(onlyNestedCallDiagnostics, true, nestedCallDetail)
const nestedCompositionDiagnostics = nestedCallDiagnostics.filter(
  (diagnostic) => diagnostic.message === nestedCallMessage
)
const chainedReceiverDiagnostics = nestedCallDiagnostics.filter(
  (diagnostic) => diagnostic.message === chainedPipeMessage
)
assert.equal(nestedCompositionDiagnostics.length, 10, nestedCallDetail)
assert.equal(chainedReceiverDiagnostics.length, 8, nestedCallDetail)

const shadowedPipeDiagnostics = runOxlintFixture(
  "shadowed-pipe-fixture.mjs",
  `import { pipe as compose } from "effect/Function"

const value = null
const f = null
const operator = null

const _composed = compose(value, operator)
const _shadowed = (pipe) => pipe(f(value), operator)
`,
  ["noNestedCalls"]
)
const shadowedPipeDetail = JSON.stringify(shadowedPipeDiagnostics)
assert.equal(shadowedPipeDiagnostics.length, 1, shadowedPipeDetail)
assert.equal(shadowedPipeDiagnostics[0].code, "effect-local(noNestedCalls)")
assert.equal(shadowedPipeDiagnostics[0].message, nestedCallMessage)

const forwardingDiagnostics = runOxlintFixture(
  "unnecessary-effect-forwarding-fixture.mjs",
  `import * as Context from "effect/Context"
import { get as lookup } from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Semaphore from "effect/Semaphore"
import * as Root from "effect"
import { Layer as RootLayer } from "effect"

const Tag = null
const OtherTag = null
const value = null
const other = null
const release = null
const use = null
const runtime = null
const Fake = null
const condition = null

const _context = value.pipe((self) => Context.get(self, Tag))
const _directAlias = value.pipe((self) => lookup(self, Tag))
const _rootNamespace = value.pipe((self) => Root.Context.get(self, Tag))
const _rootNamedModule = value.pipe((self) => RootLayer.succeed(Tag, self))
const _layerSucceed = value.pipe((self) => Layer.succeed(Tag, self))
const _layerEffect = value.pipe((self) => Layer.effect(Tag, self))
const _layerMerge = value.pipe((self) => Layer.merge(self, other))
const _layerBuild = value.pipe((self) => Layer.buildWithScope(self, other))
const _metric = value.pipe((self) => Metric.update(self, 1))
const _option = value.pipe((self) => Option.match(self, { onNone: () => null, onSome: (some) => some }))
const _semaphore = value.pipe((self) => Semaphore.withPermit(other, self))
const _semaphoreAvailable = value.pipe((self) => Semaphore.withPermitsIfAvailable(other, 1, self))
const _forEach = value.pipe((self) => Effect.forEach(self, use, { discard: true }))

const _acquireRelease = value.pipe((self) => Effect.acquireRelease(self, release))
const _acquireUseRelease = value.pipe((self) => Effect.acquireUseRelease(self, use, release))
const _memberMethod = value.pipe((self) => runtime.atom(self))
const _shadowedNamespace = ((Context) => value.pipe((self) => Context.get(self, Tag)))(Fake)
const _shadowedDirect = ((lookup) => value.pipe((self) => lookup(self, Tag)))(Fake)
const _repeated = value.pipe((self) => Context.get(self, self))
const _wrongPosition = value.pipe((self) => Layer.succeed(self, Tag))
const _blockBody = value.pipe((self) => { return Context.get(self, Tag) })
const _async = value.pipe(async (self) => Context.get(self, Tag))
const _default = value.pipe((self = value) => Context.get(self, Tag))
const _rest = value.pipe((...self) => Context.get(self, Tag))
const _destructured = value.pipe(({ self }) => Context.get(self, Tag))
const _spread = value.pipe((self) => Effect.forEach(...self))
const _this = value.pipe((self) => Context.get(self, this.Tag))
const _control = value.pipe((self) => condition ? Context.get(self, Tag) : OtherTag)
const _computed = value.pipe((self) => Context["get"](self, Tag))
const _unofficial = value.pipe((self) => Fake.get(self, Tag))
const _functionExpression = value.pipe(function(self) { return Context.get(self, Tag) })
`,
  ["noUnnecessaryEffectForwarding"]
)
const forwardingDetail = JSON.stringify(forwardingDiagnostics)
assert.equal(forwardingDiagnostics.length, 13, forwardingDetail)
const forwardingCodes = new Set(forwardingDiagnostics.map((diagnostic) => diagnostic.code))
const expectedContextMessage = unnecessaryEffectForwardingMessage("Context.get", "Context.get(Tag)")
const expectedSemaphoreMessage = unnecessaryEffectForwardingMessage(
  "Semaphore.withPermitsIfAvailable",
  "Semaphore.withPermitsIfAvailable(other, 1)"
)
const forwardingMessages = new Set(forwardingDiagnostics.map((diagnostic) => diagnostic.message))
assert.deepEqual(forwardingCodes, new Set(["effect-local(noUnnecessaryEffectForwarding)"]), forwardingDetail)
pipe(forwardingMessages.has(expectedContextMessage), (found) => assert.equal(found, true, forwardingDetail))
pipe(forwardingMessages.has(expectedSemaphoreMessage), (found) => assert.equal(found, true, forwardingDetail))

const boundaryDiagnostics = runOxlintFixture(
  "manual-boundary-fixture.mjs",
  `import * as Effect from "effect/Effect"
import { runPromise as execute } from "effect/Effect"
import { make as makeRuntime } from "effect/ManagedRuntime"
import * as SchemaParser from "effect/SchemaParser"
import * as Root from "effect"

const layer = null

Effect.runSync(Effect.void)
void execute(Effect.void)
void Root.Effect.runPromise(Effect.void)
Root.Schema.decodeUnknownSync(Root.Schema.String)("value")
const _decode = SchemaParser.decodePromise(Root.Schema.String)
Root.Runtime.makeRunMain()
const runtime = Root.ManagedRuntime.make(layer)
void runtime.runPromise(Effect.void)
const pipedRuntime = layer.pipe((value) => makeRuntime(value))
void pipedRuntime.dispose()
// A process entry point must state why it owns execution and lifecycle.
// oxlint-disable-next-line effect-local/noManualEffectBoundary
Effect.runSync(Effect.void)
`,
  ["noManualEffectBoundary"]
)

const manualDiagnostics = boundaryDiagnostics.filter(
  (diagnostic) => diagnostic.code === "effect-local(noManualEffectBoundary)"
)
assert.equal(manualDiagnostics.length, 8)
const onlyManualDiagnostics = boundaryDiagnostics.every(
  (diagnostic) => diagnostic.code === "effect-local(noManualEffectBoundary)"
)
assert.equal(onlyManualDiagnostics, true)
const hasGuidance = boundaryDiagnostics.every((diagnostic) => diagnostic.message === manualBoundaryMessage)
assert.equal(hasGuidance, true)

const taggedFixtureSource = `import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type { unhandled as EffectUnhandled } from "effect/Types"
import * as Socket from "effect/unstable/socket/Socket"

class Tagged extends Schema.TaggedErrorClass<Tagged>()("Tagged", {}) {}
interface OptionalTag { readonly _tag?: "OptionalTag" }
interface unhandled { readonly _: unique symbol }
interface PublicService {
  readonly run: Effect.Effect<void, string>
  readonly execute: () => Effect.Effect<void, number>
}

declare const tagged: Tagged
declare const optional: OptionalTag
declare const unknownError: unknown
declare const anyError: any
declare const mixedError: Tagged | string
declare const widen: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E | number>
declare const effectUnhandled: EffectUnhandled
declare const userUnhandled: unhandled
declare const localMake: (options: {
  readonly runRaw: <A, E, R>(
    handler: (input: string | Uint8Array) => Effect.Effect<A, E, R> | void
  ) => Effect.Effect<void, E, R>
}) => void

const _emoji = "😀"
const _good = Effect.fail(tagged)
const _bad = Effect.fail("bad")
const _optionalTag = Effect.fail(optional)
const _unknown = Effect.fail(unknownError)
const _any = Effect.fail(anyError)
const _mixed = Effect.fail(mixedError)
const _generic = <E>(error: E) => Effect.fail(error)
const _constrained = <E extends { readonly _tag: string }>(error: E) => Effect.fail(error)
const _nested = Effect.succeed("start").pipe(Effect.flatMap(() => Effect.fail("nested")))
const _genericCascade = <E>(effect: Effect.Effect<void, E>) => Effect.suspend(() => effect)
const _partiallyNested = widen(Effect.fail("partial"))
const _effectUnhandled = Effect.fail(effectUnhandled)
const _userUnhandled = Effect.fail(userUnhandled)
const _socket = Socket.make({
  runRaw: (handler) => {
    const _concrete = Effect.fail("socket concrete")
    return Effect.gen(function*() {
      const socketResult = handler("input")
      if (Effect.isEffect(socketResult)) yield* socketResult
    })
  },
  writer: Effect.succeed(() => Effect.void)
})
localMake({
  runRaw: (handler) =>
    Effect.gen(function*() {
      const localResult = handler("input")
      if (Effect.isEffect(localResult)) yield* localResult
    })
})
const _never: Effect.Effect<void, never> = Effect.void
`

const taggedDiagnostics = runOxlintFixture(
  "tagged-effect-error-fixture.ts",
  taggedFixtureSource,
  ["requireTaggedEffectError"]
)

const repeatedFixtureName = ".oxlint-tagged-repeated-fixture.ts"
const repeatedFixturePath = resolve(worktree, repeatedFixtureName)
const taggedChecker = makeTaggedEffectErrorChecker({ cwd: worktree })
writeFileSync(repeatedFixturePath, taggedFixtureSource)
try {
  const firstDiagnostics = taggedChecker.find({
    filename: repeatedFixtureName,
    sourceText: taggedFixtureSource
  })
  assert.notEqual(firstDiagnostics.length, 0)
  const correctedSource = taggedFixtureSource.replace(
    "const _bad = Effect.fail(\"bad\")",
    "const _bad = Effect.fail(tagged)"
  )
  const secondDiagnostics = taggedChecker.find({
    filename: repeatedFixtureName,
    sourceText: correctedSource
  })
  assert.equal(secondDiagnostics.length, firstDiagnostics.length - 1)
} finally {
  taggedChecker.close()
  if (existsSync(repeatedFixturePath)) unlinkSync(repeatedFixturePath)
}

const untaggedDiagnostics = taggedDiagnostics.filter(
  (diagnostic) => diagnostic.code === "effect-local(requireTaggedEffectError)"
)
const taggedDetail = JSON.stringify(untaggedDiagnostics)
assert.equal(untaggedDiagnostics.length, 20, taggedDetail)

const taggedOffsets = new Set(untaggedDiagnostics.map((diagnostic) => diagnostic.labels[0].span.offset))
const taggedMessageByOffset = new Map(
  untaggedDiagnostics.map((diagnostic) => [diagnostic.labels[0].span.offset, diagnostic.message])
)
const utf8Offset = (text, position) => pipe(text.slice(0, position), (value) => Buffer.byteLength(value))
const offsetOf = (needle, fromEnd = false) => {
  let position = taggedFixtureSource.indexOf(needle)
  if (fromEnd) position = taggedFixtureSource.lastIndexOf(needle)
  return utf8Offset(taggedFixtureSource, position)
}
const nestedOuter = offsetOf("Effect.succeed(\"start\")")
const nestedInner = offsetOf("Effect.fail(\"nested\")")
const genericOuter = offsetOf("Effect.suspend(() => effect)")
const genericInner = offsetOf("effect)", true)
const partialOuter = offsetOf("widen(Effect.fail(\"partial\"))")
const partialInner = offsetOf("Effect.fail(\"partial\")")
const effectUnhandledOffset = offsetOf("Effect.fail(effectUnhandled)")
const userUnhandledOffset = offsetOf("Effect.fail(userUnhandled)")
const socketGenericOffset = offsetOf("socketResult\n", true)
const socketConcreteOffset = offsetOf("Effect.fail(\"socket concrete\")")
const localGenericOffset = offsetOf("localResult\n", true)
const declaredStringOffset = offsetOf("Effect.Effect<void, string>")
const declaredNumberOffset = offsetOf("Effect.Effect<void, number>")
const hasNestedOuter = taggedOffsets.has(nestedOuter)
const hasNestedInner = taggedOffsets.has(nestedInner)
const hasGenericOuter = taggedOffsets.has(genericOuter)
const hasGenericInner = taggedOffsets.has(genericInner)
const partialOuterMessage = taggedMessageByOffset.get(partialOuter)
const partialInnerMessage = taggedMessageByOffset.get(partialInner)
const hasEffectUnhandled = taggedOffsets.has(effectUnhandledOffset)
const userUnhandledMessage = taggedMessageByOffset.get(userUnhandledOffset)
const hasSocketGeneric = taggedOffsets.has(socketGenericOffset)
const socketConcreteMessage = taggedMessageByOffset.get(socketConcreteOffset)
const localGenericMessage = taggedMessageByOffset.get(localGenericOffset)
const declaredStringMessage = taggedMessageByOffset.get(declaredStringOffset)
const declaredNumberMessage = taggedMessageByOffset.get(declaredNumberOffset)
assert.equal(hasNestedOuter, false, taggedDetail)
assert.equal(hasNestedInner, true, taggedDetail)
assert.equal(hasGenericOuter, false, taggedDetail)
assert.equal(hasGenericInner, true, taggedDetail)
assert.match(partialOuterMessage, /untagged type: number\./)
assert.match(partialInnerMessage, /untagged type: string\./)
assert.equal(hasEffectUnhandled, false, taggedDetail)
assert.match(userUnhandledMessage, /untagged type: unhandled\./)
assert.equal(hasSocketGeneric, false, taggedDetail)
assert.match(socketConcreteMessage, /untagged type: string\./)
assert.match(localGenericMessage, /untagged type: E\./)
assert.match(declaredStringMessage, /untagged type: string\./)
assert.match(declaredNumberMessage, /untagged type: number\./)
