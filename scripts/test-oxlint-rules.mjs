import { pipe } from "effect/Function"
import * as Schema from "effect/Schema"
import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, rmSync, unlinkSync, writeFileSync } from "node:fs"
import { resolve } from "node:path"
import { Testing } from "oxlint-plugin-effect/rule-bindings"
import {
  chainedPipeMessage,
  effectCatchDieMessage,
  effectOrDieMessage,
  functionEffectGenMessage,
  manualBoundaryMessage,
  nestedCallMessage,
  noFunctionEffectGen,
  noImplicitDefectConversion,
  noNestedCalls,
  noSemaphoreEffectSync,
  noUnknownEffectChannelsMessage,
  noUnnecessaryEffectForwarding,
  noYieldEffectSync,
  unnecessaryEffectForwardingMessage,
  unnecessaryPipeForwardingMessage
} from "./oxlint-plugin.mjs"
import { makeEffectTypePolicyChecker } from "./tagged-effect-errors.mjs"

const jsonStringCodec = Schema.fromJsonString(Schema.Unknown)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- The CLI fixture harness must synchronously decode completed Oxlint process output.
const decodeJson = Schema.decodeUnknownSync(jsonStringCodec)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- The CLI fixture harness must synchronously serialize configuration and assertion details.
const encodeJson = Schema.encodeUnknownSync(jsonStringCodec)

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
    Testing.expectNoDiagnostics(Testing.runRuleMulti(noYieldEffectSync, [
      ["ImportDeclaration", effectImport],
      ["YieldExpression", Testing.yieldExpr(syncCall, false)],
      ["YieldExpression", Testing.yieldExpr(otherSyncCall, true)]
    ]))
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
const composedSource = Testing.callExpr("source")
importedPipeVisitors.CallExpression(Testing.callExpr("compose", [composedSource]))
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
Testing.expectNoDiagnostics(importedFunctionTest.diagnostics)

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

Testing.expectNoDiagnostics(Testing.runRuleMulti(noSemaphoreEffectSync, [
  ["ImportDeclaration", effectImport],
  ["CallExpression", {
    type: "CallExpression",
    callee: Testing.memberExpr("gate", "withPermit"),
    arguments: [Testing.callOfMember("Fx", "gen")],
    optional: false
  }]
]))

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
  pipe(fixtureConfig, encodeJson, (configuredSource) => writeFileSync(configFilename, configuredSource))
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
    const output = decodeJson(result.stdout)
    assert.equal(output.number_of_files, 1, result.stdout)
    return output.diagnostics
  } finally {
    rmSync(directory, { force: true, recursive: true })
  }
}

const runOxlintPackageFixture = (name, source, rules, extraFiles = {}, expectedStatus = 1) => {
  const directory = mkdtempSync(resolve(worktree, "packages/oxlint-fixture-"))
  const sourceDirectory = resolve(directory, "src")
  mkdirSync(sourceDirectory)
  const filename = resolve(sourceDirectory, name)
  mkdirSync(resolve(filename, ".."), { recursive: true })
  writeFileSync(filename, source)
  for (const [relativeName, contents] of Object.entries(extraFiles)) {
    const extraFilename = resolve(directory, relativeName)
    mkdirSync(resolve(extraFilename, ".."), { recursive: true })
    writeFileSync(extraFilename, contents)
  }
  writeFileSync(
    resolve(directory, "package.json"),
    encodeJson({
      name: "@effect-local/oxlint-fixture",
      type: "module",
      exports: { ".": "./src/index.ts", "./*": "./src/*.ts", "./internal/*": null }
    })
  )
  const configFilename = resolve(directory, "oxlint.json")
  const pluginFilename = resolve(worktree, "scripts/oxlint-plugin.mjs")
  writeFileSync(
    configFilename,
    encodeJson({
      jsPlugins: [pluginFilename],
      rules: Object.fromEntries(rules.map((rule) => [`effect-local/${rule}`, "error"]))
    })
  )
  try {
    const result = spawnSync(
      "pnpm",
      [
        "exec",
        "oxlint",
        "--config",
        configFilename,
        "--disable-nested-config",
        sourceDirectory,
        "--format",
        "json",
        "--threads",
        "1"
      ],
      { cwd: worktree, encoding: "utf8" }
    )
    if (result.error !== undefined) throw result.error
    assert.equal(result.status, expectedStatus, `${result.stderr}\n${result.stdout}`)
    return decodeJson(result.stdout).diagnostics
  } finally {
    rmSync(directory, { force: true, recursive: true })
  }
}

const utf8Offset = (text, position) => Buffer.byteLength(text.slice(0, position))
const offsetOf = (source, needle, from = 0) => {
  const position = source.indexOf(needle, from)
  assert.notEqual(position, -1, `Missing fixture marker: ${needle}`)
  return utf8Offset(source, position)
}
const offsetAfter = (source, marker, needle) => offsetOf(source, needle, source.indexOf(marker))
const diagnosticOffsets = (diagnostics) =>
  diagnostics.map((diagnostic) => diagnostic.labels[0].span.offset).toSorted((left, right) => left - right)
const diagnosticSpans = (diagnostics) =>
  diagnostics
    .map((diagnostic) => {
      const { length, offset } = diagnostic.labels[0].span
      return { length, offset }
    })
    .toSorted((left, right) => left.offset - right.offset || left.length - right.length)
const fixtureSpan = (source, marker, expression) => ({
  length: Buffer.byteLength(expression),
  offset: offsetAfter(source, marker, expression)
})

const nestedCallFixtureSource = `import * as Effect from "effect/Effect"
import { pipe, pipe as compose } from "effect/Function"
import * as Function from "effect/Function"

const value = null
const token = null
const dynamicPipe = "pipe"
const f = null
const g = null
const operator = null
const finish = null

const _allowedNested = f(g(value))
const _wrappedArgument = f((g(value)))
const _receiverInput = f(g(value)).pipe(operator)
const _directInput = pipe(f(g(value)), operator)
const _wrappedDirectInput = (pipe)(f(g(value)), operator)
const _aliasedInput = compose(f(g(value)), operator)
const _namespaceInput = Function.pipe(f(g(value)), operator)
const _wrappedNamespaceInput = (Function).pipe(f(g(value)), operator)
const _computedReceiverInput = f(g(value))["pipe"](operator)
const _dynamicMember = value[dynamicPipe](f(value))
const _tooDeep = f(g(f(value)))
const _receiverTooDeep = f(g(f(value))).pipe(operator)
const _directTooDeep = pipe(f(g(f(value))), operator)
const _chained = value.pipe(operator).pipe(operator)
const _deeper = value.pipe(operator).pipe(operator).pipe(operator)
const _computedChain = value["pipe"](operator)["pipe"](operator)
const _templateChain = value[\`pipe\`](operator)[\`pipe\`](operator)
const _parenthesizedChain = (value.pipe(operator)).pipe(operator)
const _optionalPropertyChain = value?.pipe(operator)?.pipe(operator)
const _optionalCallChain = value.pipe?.(operator).pipe?.(operator)

const _receiverOperator = value.pipe(Effect.ensuring(finish(value, token)))
const _computedReceiverOperator = value["pipe"](Effect.ensuring(finish(value, token)))
const _directOperator = pipe(value, Effect.ensuring(finish(value, token)))
const _wrappedDirectOperator = (pipe)(value, Effect.ensuring(finish(value, token)))
const _aliasedOperator = compose(value, Effect.ensuring(finish(value, token)))
const _namespaceOperator = Function.pipe(value, Effect.ensuring(finish(value, token)))
const _wrappedNamespaceOperator = (Function).pipe(value, Effect.ensuring(finish(value, token)))
const _operatorTooDeep = value.pipe(Effect.ensuring(finish(g(value), token)))
`
const nestedCallDiagnostics = runOxlintFixture(
  "no-nested-calls-fixture.mjs",
  nestedCallFixtureSource,
  ["noNestedCalls"]
)
const onlyNestedCallDiagnostics = nestedCallDiagnostics.every(
  (diagnostic) => diagnostic.code === "effect-local(noNestedCalls)"
)
const nestedCallDetail = encodeJson(nestedCallDiagnostics)
assert.equal(onlyNestedCallDiagnostics, true, nestedCallDetail)
const nestedCompositionDiagnostics = nestedCallDiagnostics.filter(
  (diagnostic) => diagnostic.message === nestedCallMessage
)
const chainedReceiverDiagnostics = nestedCallDiagnostics.filter(
  (diagnostic) => diagnostic.message === chainedPipeMessage
)
assert.equal(nestedCompositionDiagnostics.length, 4, nestedCallDetail)
assert.equal(chainedReceiverDiagnostics.length, 8, nestedCallDetail)
assert.deepEqual(
  diagnosticOffsets(nestedCompositionDiagnostics),
  [
    offsetOf(nestedCallFixtureSource, "f(value)", nestedCallFixtureSource.indexOf("const _tooDeep")),
    offsetOf(nestedCallFixtureSource, "f(value)", nestedCallFixtureSource.indexOf("const _receiverTooDeep")),
    offsetOf(nestedCallFixtureSource, "f(value)", nestedCallFixtureSource.indexOf("const _directTooDeep")),
    offsetOf(nestedCallFixtureSource, "g(value)", nestedCallFixtureSource.indexOf("const _operatorTooDeep"))
  ].toSorted((left, right) => left - right),
  nestedCallDetail
)
assert.deepEqual(
  diagnosticSpans(chainedReceiverDiagnostics),
  [
    fixtureSpan(nestedCallFixtureSource, "const _chained", "value.pipe(operator).pipe(operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _deeper", "value.pipe(operator).pipe(operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _deeper", "value.pipe(operator).pipe(operator).pipe(operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _computedChain", "value[\"pipe\"](operator)[\"pipe\"](operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _templateChain", "value[`pipe`](operator)[`pipe`](operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _parenthesizedChain", "(value.pipe(operator)).pipe(operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _optionalPropertyChain", "value?.pipe(operator)?.pipe(operator)"),
    fixtureSpan(nestedCallFixtureSource, "const _optionalCallChain", "value.pipe?.(operator).pipe?.(operator)")
  ].toSorted((left, right) => left.offset - right.offset || left.length - right.length),
  nestedCallDetail
)

const shadowedPipeFixtureSource = `import { pipe as compose } from "effect/Function"

const value = null
const f = null
const operator = null

const _composed = compose(value, operator)
const _shadowed = (pipe) => pipe(f(g(value)), operator)
`
const shadowedPipeDiagnostics = runOxlintFixture(
  "shadowed-pipe-fixture.mjs",
  shadowedPipeFixtureSource,
  ["noNestedCalls"]
)
const shadowedPipeDetail = encodeJson(shadowedPipeDiagnostics)
assert.equal(shadowedPipeDiagnostics.length, 1, shadowedPipeDetail)
assert.equal(shadowedPipeDiagnostics[0].code, "effect-local(noNestedCalls)")
assert.equal(shadowedPipeDiagnostics[0].message, nestedCallMessage)
assert.deepEqual(
  diagnosticOffsets(shadowedPipeDiagnostics),
  [offsetAfter(shadowedPipeFixtureSource, "const _shadowed", "g(value)")],
  shadowedPipeDetail
)

const forwardingFixtureSource = `import * as Context from "effect/Context"
import { get as lookup } from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Semaphore from "effect/Semaphore"
import * as Root from "effect"
import { Layer as RootLayer } from "effect"
import { pipe } from "effect/Function"

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
const _genericReceiver = value.pipe((self) => use(self))
const _genericImported = pipe(value, (self) => runtime.atom(self))
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
const _nonPipe = (self) => runtime.atom(self)
const _optionalCall = value.pipe((self) => runtime.atom?.(self))
const _computedMember = value.pipe((self) => runtime["atom"](self))
const _extraArgument = value.pipe((self) => runtime.atom(self, other))
`
const forwardingDiagnostics = runOxlintFixture(
  "unnecessary-effect-forwarding-fixture.mjs",
  forwardingFixtureSource,
  ["noUnnecessaryEffectForwarding"]
)
const forwardingDetail = encodeJson(forwardingDiagnostics)
assert.equal(forwardingDiagnostics.length, 16, forwardingDetail)
assert.deepEqual(
  diagnosticOffsets(forwardingDiagnostics),
  [
    "_context",
    "_directAlias",
    "_rootNamespace",
    "_rootNamedModule",
    "_layerSucceed",
    "_layerEffect",
    "_layerMerge",
    "_layerBuild",
    "_metric",
    "_option",
    "_semaphore",
    "_semaphoreAvailable",
    "_forEach",
    "_memberMethod",
    "_genericReceiver",
    "_genericImported"
  ].map((marker) => offsetAfter(forwardingFixtureSource, `const ${marker}`, "(self) =>"))
    .toSorted((left, right) => left - right),
  forwardingDetail
)
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
pipe(
  forwardingMessages.has(unnecessaryPipeForwardingMessage("runtime.atom", "value")),
  (found) => assert.equal(found, true, forwardingDetail)
)

const functionEffectGenFixtureSource = `import * as Effect from "effect/Effect"
import { gen as generate } from "effect/Effect"
import { Effect as RootEffect } from "effect"
import * as Root from "effect"
import { pipe as compose } from "effect/Function"
import * as Function from "effect/Function"

const operator = Effect.asVoid
const _arrow = () => Effect.gen(function*() {})
function block() { return generate(function*() {}) }
const _rootNamed = () => RootEffect.gen(function*() {})
const _rootNamespace = () => Root.Effect.gen(function*() {})
const _receiver = () => Effect.gen(function*() {}).pipe(operator)
const _functionPipe = () => compose(Effect.gen(function*() {}), operator)
const _namespacePipe = () => Function.pipe(Effect.gen(function*() {}), operator)
const _callback = [1].map(() => Effect.gen(function*() {}))
const localGenerate = Effect.gen
const _localAlias = () => localGenerate(function*() {})
const localGenerateAgain = localGenerate
const _localAliasChain = () => localGenerateAgain(function*() {})
const { gen: destructuredGenerate } = Effect
const _destructuredAlias = () => destructuredGenerate(function*() {})
let mutableGenerate = Effect.gen
const _mutableAlias = () => mutableGenerate(function*() {})

const _value = Effect.gen(function*() {})
const _async = async () => Effect.gen(function*() {})
function* generator() { return Effect.gen(function*() {}) }
const _notRooted = () => Effect.succeed(Effect.gen(function*() {}))
const _shadowed = ((Effect) => () => Effect.gen(function*() {}))({ gen: () => Effect.void })
`
const functionEffectGenFixtureDiagnostics = runOxlintFixture(
  "function-effect-gen-fixture.ts",
  functionEffectGenFixtureSource,
  ["noFunctionEffectGen"]
)
const functionEffectGenDiagnostics = functionEffectGenFixtureDiagnostics.filter(
  (diagnostic) => diagnostic.code === "effect-local(noFunctionEffectGen)"
)
const functionEffectGenDetail = encodeJson(functionEffectGenDiagnostics)
assert.equal(functionEffectGenDiagnostics.length, 11, functionEffectGenDetail)
assert.equal(
  functionEffectGenDiagnostics.every((diagnostic) => diagnostic.message === functionEffectGenMessage),
  true,
  functionEffectGenDetail
)
assert.match(functionEffectGenMessage, /Effect\.fnUntraced\(function\* \(\) \{\}, x, y, z\)/)
assert.match(functionEffectGenMessage, /Effect\.fn\("spanName"\)\(function\* \(\) \{\}, x, y, z\)/)
assert.match(functionEffectGenMessage, /first argument is the generator body/)
assert.deepEqual(
  diagnosticOffsets(functionEffectGenDiagnostics),
  [
    offsetAfter(functionEffectGenFixtureSource, "const _arrow", "Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "function block", "generate"),
    offsetAfter(functionEffectGenFixtureSource, "const _rootNamed", "RootEffect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _rootNamespace", "Root.Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _receiver", "Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _functionPipe", "Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _namespacePipe", "Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _callback", "Effect.gen"),
    offsetAfter(functionEffectGenFixtureSource, "const _localAlias", "localGenerate"),
    offsetAfter(functionEffectGenFixtureSource, "const _localAliasChain", "localGenerateAgain"),
    offsetAfter(functionEffectGenFixtureSource, "const _destructuredAlias", "destructuredGenerate")
  ].toSorted((left, right) => left - right),
  functionEffectGenDetail
)

const implicitDefectFixtureSource = `import * as Effect from "effect/Effect"
import { catch as catchAll, die as terminate, orDie as fatal } from "effect/Effect"
import * as Root from "effect"
import * as Layer from "effect/Layer"

const _pipe = Effect.void.pipe(Effect.orDie)
const _dataFirst = Effect.orDie(Effect.void)
const _named = fatal(Effect.void)
const _root = Root.Effect["orDie"](Effect.void)
const extracted = Effect.orDie
const _extractedUse = extracted(Effect.void)
const extractedAgain = extracted
const _aliasChain = extractedAgain(Effect.void)
const { orDie: destructured } = Effect
const _destructuredUse = destructured(Effect.void)
const _catchArrow = Effect.void.pipe(Effect.catch((failure) => Effect.die(failure)))
const _catchBlock = catchAll(Effect.void, (cause) => { return terminate(cause) })

const _standaloneDie = Effect.die("defect")
const _tagged = Effect.void.pipe(Effect.catchTag("Tagged", (error) => Effect.die(error)))
const _transformed = Effect.void.pipe(Effect.catch((error) => Effect.die(error.cause)))
const _extra = Effect.void.pipe(Effect.catch((error) => { void error; return Effect.die(error) }))
const _layer = Layer.orDie
const _shadowed = ((Effect) => Effect.orDie)({ orDie: null })
`
const implicitDefectDiagnostics = runOxlintFixture(
  "implicit-defect-fixture.ts",
  implicitDefectFixtureSource,
  ["noImplicitDefectConversion"]
)
const implicitDetail = encodeJson(implicitDefectDiagnostics)
const orDieDiagnostics = implicitDefectDiagnostics.filter(
  (diagnostic) => diagnostic.message === effectOrDieMessage
)
const catchDieDiagnostics = implicitDefectDiagnostics.filter(
  (diagnostic) => diagnostic.message === effectCatchDieMessage
)
assert.equal(orDieDiagnostics.length, 9, implicitDetail)
assert.equal(catchDieDiagnostics.length, 2, implicitDetail)
assert.deepEqual(
  diagnosticOffsets(orDieDiagnostics),
  [
    ["_pipe", "Effect.orDie"],
    ["_dataFirst", "Effect.orDie"],
    ["_named", "fatal"],
    ["_root", "Root.Effect"],
    ["const extracted", "Effect.orDie"],
    ["_extractedUse", "extracted("],
    ["const extractedAgain", "extracted\n"],
    ["_aliasChain", "extractedAgain("],
    ["_destructuredUse", "destructured("]
  ].map(([marker, needle]) => offsetAfter(implicitDefectFixtureSource, marker, needle))
    .toSorted((left, right) => left - right),
  implicitDetail
)
assert.deepEqual(
  diagnosticOffsets(catchDieDiagnostics),
  [
    offsetAfter(implicitDefectFixtureSource, "_catchArrow", "Effect.catch"),
    offsetAfter(implicitDefectFixtureSource, "_catchBlock", "catchAll")
  ].toSorted((left, right) => left - right),
  implicitDetail
)

const implicitDefectReexportSource = `export { orDie, orDie as fatal } from "effect/Effect"`
const implicitDefectReexportDiagnostics = runOxlintFixture(
  "implicit-defect-reexport-fixture.ts",
  implicitDefectReexportSource,
  ["noImplicitDefectConversion"]
).filter((diagnostic) => diagnostic.code === "effect-local(noImplicitDefectConversion)")
assert.equal(implicitDefectReexportDiagnostics.length, 2, encodeJson(implicitDefectReexportDiagnostics))
assert.deepEqual(
  diagnosticOffsets(implicitDefectReexportDiagnostics),
  [offsetOf(implicitDefectReexportSource, "orDie"), offsetAfter(implicitDefectReexportSource, ", orDie", "orDie")],
  encodeJson(implicitDefectReexportDiagnostics)
)

const layerFixtureSource = `import * as Layer from "effect/Layer"
import { Layer as RootLayer } from "effect"

declare const tag: any
declare const value: any
const layer = Layer.succeed(tag, value)
const layerTest: Layer.Layer<any> = Layer.empty
const LayerValue = RootLayer.empty
const GoodLayer = RootLayer.empty
const testLayer = RootLayer.empty
const lower = RootLayer.empty
const layerlower = RootLayer.empty
const object = { testLayer: RootLayer.empty, layerTest: RootLayer.empty }
const tupleProperty = { partition: [] }
const [, omittedBinding] = [undefined, undefined]
const layerShorthand = RootLayer.empty
const shorthandObject = { layerShorthand }
const { testLayer: testLayerBinding } = object
class Holder {
  readonly testLayer = RootLayer.empty
  readonly layerTest = RootLayer.empty
}
class ParameterHolder {
  constructor(
    readonly testLayer: Layer.Layer<never>,
    readonly layerTest: Layer.Layer<never>,
    ordinaryLayer: Layer.Layer<never>
  ) {}
}
declare const dynamicName: string
const namedObject = {
  "testLayer": RootLayer.empty,
  ["otherLayer"]: RootLayer.empty,
  [\`anotherLayer\`]: RootLayer.empty,
  "layerTest": RootLayer.empty,
  [dynamicName]: RootLayer.empty
}
class NamedHolder {
  readonly "testLayer" = RootLayer.empty
  readonly [\`otherLayer\`] = RootLayer.empty
  readonly #anotherLayer = RootLayer.empty
  readonly "layerTest" = RootLayer.empty
  readonly [\`layerOther\`] = RootLayer.empty
  readonly #layerPrivate = RootLayer.empty
}
const makeLayer = () => Layer.empty
function buildLayer(): Layer.Layer<never> { return Layer.empty }
const factoryExpression = function() { return Layer.empty }
const mixed: Layer.Layer<never> | undefined = undefined
declare const anyLayer: Layer.Any
declare const anyLayerValue: any
declare const unknownLayerValue: unknown
const structural: { readonly "~effect/Layer": unknown } = { "~effect/Layer": undefined }
interface TypeOnlyProperties { readonly testLayer: Layer.Layer<never> }
const { empty: destructured } = Layer
`
const layerDiagnostics = runOxlintFixture(
  "layer-name-fixture.ts",
  layerFixtureSource,
  ["requireLayerName"]
).filter((diagnostic) => diagnostic.code === "effect-local(requireLayerName)")
const layerDetail = encodeJson(layerDiagnostics)
assert.equal(layerDiagnostics.length, 16, layerDetail)
assert.ok(
  layerDiagnostics.every((diagnostic) =>
    diagnostic.message.endsWith(
      "must use layer or layerX with PascalCase descriptive suffix. xLayer is invalid. Functions returning Layer remain camelCase."
    )
  ),
  layerDetail
)
assert.deepEqual(
  diagnosticOffsets(layerDiagnostics),
  [
    offsetAfter(layerFixtureSource, "const LayerValue", "LayerValue"),
    offsetAfter(layerFixtureSource, "const GoodLayer", "GoodLayer"),
    offsetAfter(layerFixtureSource, "const testLayer", "testLayer"),
    offsetAfter(layerFixtureSource, "const lower", "lower"),
    offsetAfter(layerFixtureSource, "const layerlower", "layerlower"),
    offsetAfter(layerFixtureSource, "{ testLayer", "testLayer"),
    offsetAfter(layerFixtureSource, "testLayer:", "testLayerBinding"),
    offsetAfter(layerFixtureSource, "readonly testLayer", "testLayer"),
    offsetAfter(layerFixtureSource, "constructor(\n    readonly testLayer", "testLayer"),
    offsetAfter(layerFixtureSource, "namedObject = {\n  ", "\"testLayer\""),
    offsetAfter(layerFixtureSource, "[\"otherLayer\"]", "\"otherLayer\""),
    offsetAfter(layerFixtureSource, "[`anotherLayer`]", "`anotherLayer`"),
    offsetAfter(layerFixtureSource, "class NamedHolder {\n  readonly ", "\"testLayer\""),
    offsetAfter(layerFixtureSource, "readonly [`otherLayer`]", "`otherLayer`"),
    offsetAfter(layerFixtureSource, "readonly #anotherLayer", "#anotherLayer"),
    offsetAfter(layerFixtureSource, "empty: destructured", "destructured")
  ],
  layerDetail
)

const serviceMapFixtureSource = `import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { map as transform } from "effect/Effect"
import { pipe } from "effect/Function"

const Service = Context.Service<{ readonly value: number }>("Service")
const Other = Context.Service<{ readonly value: number }>("Other")
declare const ordinary: Effect.Effect<number>
declare const callback: (service: { readonly value: number }) => number
const localMap = Effect.map
const localMapAgain = localMap

const _receiver = Service.pipe(Effect.map(callback))
const _computed = Service["pipe"](Effect.map(callback))
const _function = pipe(Service, transform(callback), Effect.asVoid)
const _dataFirst = Effect.map(Service, callback)
const _twoMaps = Other.pipe(Effect.map(callback), Effect.map(String))
const _localAlias = Service.pipe(localMap(callback))
const _localAliasChain = Service.pipe(localMapAgain(callback))
const _ordinary = ordinary.pipe(Effect.map(String))
const _flatMap = Service.pipe(Effect.flatMap((service) => Effect.succeed(service.value)))
const _laterMap = Service.pipe(Effect.asVoid, Effect.map(String))
const _use = Service.use((service) => Effect.succeed(service.value))
`
const serviceMapDiagnostics = runOxlintFixture(
  "service-map-fixture.ts",
  serviceMapFixtureSource,
  ["noServiceTagMap"]
).filter((diagnostic) => diagnostic.code === "effect-local(noServiceTagMap)")
const serviceMapDetail = encodeJson(serviceMapDiagnostics)
assert.equal(serviceMapDiagnostics.length, 7, serviceMapDetail)
const serviceMapTarget = new Map([
  ["_dataFirst", "Service"],
  ["_twoMaps", "Other"]
])
assert.deepEqual(
  diagnosticOffsets(serviceMapDiagnostics),
  ["_receiver", "_computed", "_function", "_dataFirst", "_twoMaps", "_localAlias", "_localAliasChain"]
    .map((marker) => offsetAfter(serviceMapFixtureSource, `const ${marker}`, serviceMapTarget.get(marker) ?? "Service"))
    .toSorted((left, right) => left - right),
  serviceMapDetail
)

const manualBoundaryFixtureSource = `import * as Effect from "effect/Effect"
import { runPromise as execute } from "effect/Effect"
import * as Schema from "effect/Schema"
import { make as makeRuntime } from "effect/ManagedRuntime"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as SchemaParser from "effect/SchemaParser"
import * as Layer from "effect/Layer"
import * as Root from "effect"

const layer = null
const Fx = Effect
const Parser = SchemaParser
const directRunner = Effect["runCallback"]
const directRunnerAgain = directRunner
const directDecoder = Schema.decodeSync
const directParserEncoder = SchemaParser.encodePromise
const { runSyncExit } = Effect
const { ["runPromiseExit"]: computedRunner } = Effect
const { decodeUnknownSync } = Schema
const { decodePromise } = SchemaParser
const { [\`encodeSync\`]: computedEncoder } = SchemaParser
declare const typedRuntime: ManagedRuntime.ManagedRuntime<never, never>
declare const holder: { readonly runtime: ManagedRuntime.ManagedRuntime<never, never> }
declare const getRuntime: () => ManagedRuntime.ManagedRuntime<never, never>
declare const acceptsRuntime: (runtime: ManagedRuntime.ManagedRuntime<never, never>) => void

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
Fx.runFork(Effect.void)
void directRunner(Effect.void)
void directRunnerAgain(Effect.void)
directDecoder(Schema.String)("value")
directParserEncoder(Schema.String)("value")
runSyncExit(Effect.void)
void computedRunner(Effect.void)
decodeUnknownSync(Schema.String)("value")
Schema[\`decodeSync\`](Schema.String)("value")
Parser["encodeSync"](Schema.String)("value")
decodePromise(Schema.String)("value")
computedEncoder(Schema.String)("value")
void typedRuntime.runPromise(Effect.void)
holder.runtime["runSync"](Effect.void)
getRuntime().runFork(Effect.void)
acceptsRuntime(typedRuntime)
const _consumeRuntime = (runtime: ManagedRuntime.ManagedRuntime<never, never>) => runtime.runCallback(Effect.void)
const { runPromiseExit } = typedRuntime
void runPromiseExit(Effect.void)
void typedRuntime[Symbol.asyncDispose]()
const { dispose } = ManagedRuntime.make(Layer.empty)
void dispose()
// A process entry point must state why it owns execution and lifecycle.
// oxlint-disable-next-line effect-local/noManualEffectBoundary
Effect.runSync(Effect.void)
`
const boundaryDiagnostics = runOxlintFixture(
  "manual-boundary-fixture.ts",
  manualBoundaryFixtureSource,
  ["noManualEffectBoundary"]
)

const manualDiagnostics = boundaryDiagnostics.filter(
  (diagnostic) => diagnostic.code === "effect-local(noManualEffectBoundary)"
)
assert.equal(manualDiagnostics.length, 33, encodeJson(manualDiagnostics))
const onlyManualDiagnostics = boundaryDiagnostics.every(
  (diagnostic) => diagnostic.code === "effect-local(noManualEffectBoundary)"
)
assert.equal(onlyManualDiagnostics, true, encodeJson(boundaryDiagnostics))
const hasGuidance = boundaryDiagnostics.every((diagnostic) => diagnostic.message === manualBoundaryMessage)
assert.equal(hasGuidance, true)
assert.deepEqual(
  diagnosticOffsets(manualDiagnostics),
  [
    ["const directRunner", "Effect[\"runCallback\"]"],
    ["const directRunnerAgain =", "directRunner\n"],
    ["const directDecoder", "Schema.decodeSync"],
    ["const directParserEncoder", "SchemaParser.encodePromise"],
    ["Effect.runSync", "Effect.runSync"],
    ["void execute", "execute"],
    ["void Root.Effect", "Root.Effect.runPromise"],
    ["Root.Schema.decodeUnknownSync", "Root.Schema.decodeUnknownSync"],
    ["const _decode", "SchemaParser.decodePromise"],
    ["Root.Runtime.makeRunMain", "Root.Runtime.makeRunMain"],
    ["void runtime.runPromise", "runtime.runPromise"],
    ["void pipedRuntime.dispose", "pipedRuntime.dispose"],
    ["Fx.runFork", "Fx.runFork"],
    ["void directRunner(", "directRunner"],
    ["void directRunnerAgain", "directRunnerAgain"],
    ["directDecoder(", "directDecoder"],
    ["directParserEncoder(", "directParserEncoder"],
    ["runSyncExit(", "runSyncExit"],
    ["void computedRunner", "computedRunner"],
    ["\ndecodeUnknownSync(", "decodeUnknownSync"],
    ["Schema[`decodeSync`]", "Schema[`decodeSync`]"],
    ["Parser[\"encodeSync\"]", "Parser[\"encodeSync\"]"],
    ["\ndecodePromise(", "decodePromise"],
    ["computedEncoder(", "computedEncoder"],
    ["void typedRuntime.runPromise", "typedRuntime.runPromise"],
    ["holder.runtime[\"runSync\"]", "holder.runtime[\"runSync\"]"],
    ["getRuntime().runFork", "getRuntime().runFork"],
    ["runtime: ManagedRuntime.ManagedRuntime", "runtime.runCallback"],
    ["const { runPromiseExit } = typedRuntime", "runPromiseExit"],
    ["void runPromiseExit", "runPromiseExit"],
    ["typedRuntime[Symbol.asyncDispose]", "typedRuntime[Symbol.asyncDispose]"],
    ["const { dispose }", "dispose"],
    ["void dispose()", "dispose"]
  ].map(([marker, needle]) => offsetAfter(manualBoundaryFixtureSource, marker, needle))
    .toSorted((left, right) => left - right),
  encodeJson(manualDiagnostics)
)

const missingDependencyDirectory = mkdtempSync(resolve(worktree, "oxlint-missing-dependency-"))
try {
  const missingDependencySource = resolve(missingDependencyDirectory, "fixture.ts")
  const missingDependencyConfig = resolve(missingDependencyDirectory, "oxlint.json")
  writeFileSync(missingDependencySource, "export const value = 1\n")
  writeFileSync(
    missingDependencyConfig,
    encodeJson({
      jsPlugins: [resolve(worktree, "scripts/oxlint-plugin.mjs")],
      rules: { "effect-local/noManualEffectBoundary": "error" }
    })
  )
  const missingDependencyResult = spawnSync(
    resolve(worktree, "node_modules/.bin/oxlint"),
    [
      "--config",
      missingDependencyConfig,
      "--disable-nested-config",
      missingDependencySource,
      "--format",
      "json",
      "--threads",
      "1"
    ],
    { cwd: missingDependencyDirectory, encoding: "utf8" }
  )
  if (missingDependencyResult.error !== undefined) throw missingDependencyResult.error
  assert.equal(missingDependencyResult.status, 1, missingDependencyResult.stderr)
  const diagnostics = decodeJson(missingDependencyResult.stdout).diagnostics
  assert.equal(diagnostics.length, 1, missingDependencyResult.stdout)
  assert.match(diagnostics[0].message, /^Could not verify ManagedRuntime boundaries\./)
} finally {
  rmSync(missingDependencyDirectory, { force: true, recursive: true })
}

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
const taggedChecker = makeEffectTypePolicyChecker({ cwd: worktree })
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
const taggedDetail = encodeJson(untaggedDiagnostics)
assert.equal(untaggedDiagnostics.length, 19, taggedDetail)

const taggedOffsets = new Set(untaggedDiagnostics.map((diagnostic) => diagnostic.labels[0].span.offset))
const taggedMessageByOffset = new Map(
  untaggedDiagnostics.map((diagnostic) => [diagnostic.labels[0].span.offset, diagnostic.message])
)
const taggedOffsetOf = (needle, fromEnd = false) => {
  let position = taggedFixtureSource.indexOf(needle)
  if (fromEnd) position = taggedFixtureSource.lastIndexOf(needle)
  return utf8Offset(taggedFixtureSource, position)
}
const nestedOuter = taggedOffsetOf("Effect.succeed(\"start\")")
const nestedInner = taggedOffsetOf("Effect.fail(\"nested\")")
const genericOuter = taggedOffsetOf("Effect.suspend(() => effect)")
const genericInner = taggedOffsetOf("effect)", true)
const partialOuter = taggedOffsetOf("widen(Effect.fail(\"partial\"))")
const partialInner = taggedOffsetOf("Effect.fail(\"partial\")")
const effectUnhandledOffset = taggedOffsetOf("Effect.fail(effectUnhandled)")
const userUnhandledOffset = taggedOffsetOf("Effect.fail(userUnhandled)")
const socketGenericOffset = taggedOffsetOf("socketResult\n", true)
const socketConcreteOffset = taggedOffsetOf("Effect.fail(\"socket concrete\")")
const localGenericOffset = taggedOffsetOf("localResult\n", true)
const declaredStringOffset = taggedOffsetOf("Effect.Effect<void, string>")
const declaredNumberOffset = taggedOffsetOf("Effect.Effect<void, number>")
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

const classFactorySource = `class Value {}
const makeValue = () => new Value()
function createValue() { return new Value() }
const expression = function() { return new Value() }
const stored = { make: () => new Value() }
const Alias = Value
const aliasFactory = () => new Alias()
const Assigned = class {}
const assignedFactory = () => new Assigned()
const methodFactory = { make() { return new Value() } }
declare const consume: (factory: () => Value) => void
declare const consumeOptions: (options: { readonly make: () => Value }) => void
consume(() => new Value())
consumeOptions({ make: () => new Value() })
consume({ make: () => new Value() }.make)
function prepared() { const input = 1; void input; return new Value() }
const proxy = () => new Proxy({}, {})
`
const classFactoryDiagnostics = runOxlintFixture(
  "class-instance-factory-fixture.ts",
  classFactorySource,
  ["noClassInstanceFactory"]
).filter((diagnostic) => diagnostic.code === "effect-local(noClassInstanceFactory)")
assert.equal(classFactoryDiagnostics.length, 8, encodeJson(classFactoryDiagnostics))
assert.deepEqual(
  diagnosticOffsets(classFactoryDiagnostics),
  [
    offsetOf(classFactorySource, "() => new Value()"),
    offsetOf(classFactorySource, "function createValue"),
    offsetOf(classFactorySource, "function() { return new Value() }"),
    offsetAfter(classFactorySource, "const stored", "() => new Value()"),
    offsetAfter(classFactorySource, "aliasFactory", "() => new Alias()"),
    offsetAfter(classFactorySource, "assignedFactory", "() => new Assigned()"),
    offsetAfter(classFactorySource, "methodFactory", "make()"),
    offsetAfter(classFactorySource, "consume({", "() => new Value()")
  ].toSorted((left, right) => left - right),
  encodeJson(classFactoryDiagnostics)
)
assert.ok(
  classFactoryDiagnostics.every((diagnostic) =>
    diagnostic.message ===
      "Instantiate this class directly where the value is needed. Do not hide construction in a mapper or factory function. Prefer local verbosity so every construction site stays explicit."
  )
)

const newPolicySource = `import * as Effect from "effect/Effect"
import { sync as suspendSync } from "effect/Effect"
import { it } from "vitest"
import { it as effectIt } from "@effect/vitest"
import * as EffectTest from "@effect/vitest"
import * as timers from "node:timers/promises"
import * as Schema from "effect/Schema"
import { forkDetach as detach } from "effect/Effect"
import { forkDetach } from "effect/Effect"

class Tagged { readonly _tag = "Tagged" as const }
declare const tagged: Tagged
const _instance = tagged instanceof Tagged
const cleanup = Effect.void
const _badSync = Effect.sync(() => cleanup)
const _badAlias = suspendSync(() => cleanup)
declare const flag: boolean
const _badUnion = Effect.sync(() => flag ? Effect.void : 0)
const _goodSync = Effect.sync(() => 1).pipe(Effect.as(cleanup))
it("plain effect", () => Effect.void)
effectIt("plain effect", () => Effect.void)
const runTest = it
runTest("aliased effect", () => Effect.void)
it.skip("conditional effect", () => flag ? Effect.void : undefined)
const effectCallback = () => Effect.void
it("named effect", effectCallback)
const namedTimer = () => new Promise((resolve) => setTimeout(resolve, 1))
it("named timer", namedTimer)
function declaredTimer() { return new Promise((resolve) => setTimeout(resolve, 1)) }
const aliasedTimer = declaredTimer
it("declared timer", declaredTimer)
it("aliased declared timer", aliasedTimer)
effectIt.effect("virtual time", () => Effect.sleep("1 second"))
effectIt.live("wall time", () => Effect.sleep("1 second"))
effectIt.live.skip("modified wall time", () => Effect.sleep("1 second"))
EffectTest.it.live.skip("namespace wall time", () => Effect.sleep("1 second"))
it.skip("modified timer", () => new Promise((resolve) => setTimeout(resolve, 1)))
runTest("aliased timer", () => new Promise((resolve) => globalThis.setTimeout(resolve, 1)))
it("timer", () => new Promise((resolve) => setTimeout(resolve, 1)))
it("timer namespace", () => timers.setTimeout(1))
const decode = Schema.decodeEffect(Schema.String)
export { decode }
export const { decodeEffect: destructuredDecode } = Schema
import { decodeEffect as importedDecode } from "effect/Schema"
export { importedDecode }
export default Schema.encodeEffect(Schema.String)
void detach(Effect.void)
void forkDetach(Effect.void)
declare const detachHost: { forkDetach: (value: unknown) => unknown }
void detachHost.forkDetach(Effect.void)
const detachFlags = { forkDetach: false }
void detachFlags
it("window timer", () => new Promise((resolve) => window.setTimeout(resolve, 1)))
it("self timer", () => new Promise((resolve) => self.setTimeout(resolve, 1)))
`
const newPolicyDiagnostics = runOxlintFixture(
  "new-policy-fixture.ts",
  newPolicySource,
  [
    "noEffectSyncReturningEffect",
    "effectTestsUseEffect",
    "noInstanceofTaggedError",
    "noTestWallClockWait",
    "noExportedSchemaCodecAlias",
    "noDetachedFork"
  ]
)
const policyCounts = new Map()
for (const diagnostic of newPolicyDiagnostics) {
  policyCounts.set(diagnostic.code, (policyCounts.get(diagnostic.code) ?? 0) + 1)
}
const assertPolicyOffsets = (rule, offsets) => {
  const diagnostics = newPolicyDiagnostics.filter((diagnostic) => diagnostic.code === `effect-local(${rule})`)
  assert.deepEqual(
    diagnosticOffsets(diagnostics),
    offsets.toSorted((left, right) => left - right),
    encodeJson(newPolicyDiagnostics)
  )
}
assert.equal(policyCounts.get("effect-local(noEffectSyncReturningEffect)"), 3, encodeJson(newPolicyDiagnostics))
assert.equal(policyCounts.get("effect-local(effectTestsUseEffect)"), 5, encodeJson(newPolicyDiagnostics))
assert.equal(policyCounts.get("effect-local(noInstanceofTaggedError)"), 1, encodeJson(newPolicyDiagnostics))
assert.equal(policyCounts.get("effect-local(noTestWallClockWait)"), 12, encodeJson(newPolicyDiagnostics))
assert.equal(policyCounts.get("effect-local(noExportedSchemaCodecAlias)"), 4, encodeJson(newPolicyDiagnostics))
assert.equal(policyCounts.get("effect-local(noDetachedFork)"), 2, encodeJson(newPolicyDiagnostics))
assertPolicyOffsets("noEffectSyncReturningEffect", [
  offsetOf(newPolicySource, "Effect.sync(() => cleanup)"),
  offsetOf(newPolicySource, "suspendSync(() => cleanup)"),
  offsetOf(newPolicySource, "Effect.sync(() => flag")
])
assertPolicyOffsets("effectTestsUseEffect", [
  offsetOf(newPolicySource, "it(\"plain effect\""),
  offsetOf(newPolicySource, "effectIt(\"plain effect\""),
  offsetOf(newPolicySource, "runTest(\"aliased effect\""),
  offsetOf(newPolicySource, "it.skip(\"conditional effect\""),
  offsetOf(newPolicySource, "it(\"named effect\"")
])
assertPolicyOffsets("noTestWallClockWait", [
  offsetAfter(newPolicySource, "effectIt.live(\"wall time\"", "Effect.sleep"),
  offsetAfter(newPolicySource, "effectIt.live.skip", "Effect.sleep"),
  offsetAfter(newPolicySource, "EffectTest.it.live.skip", "Effect.sleep"),
  offsetAfter(newPolicySource, "it.skip(\"modified timer\"", "setTimeout"),
  offsetAfter(newPolicySource, "runTest(\"aliased timer\"", "globalThis.setTimeout"),
  offsetAfter(newPolicySource, "it(\"timer\"", "setTimeout"),
  offsetAfter(newPolicySource, "it(\"timer namespace\"", "timers.setTimeout"),
  offsetAfter(newPolicySource, "const namedTimer", "setTimeout"),
  offsetAfter(newPolicySource, "function declaredTimer", "setTimeout"),
  offsetAfter(newPolicySource, "function declaredTimer", "setTimeout"),
  offsetAfter(newPolicySource, "it(\"window timer\"", "window.setTimeout"),
  offsetAfter(newPolicySource, "it(\"self timer\"", "self.setTimeout")
])
assertPolicyOffsets("noExportedSchemaCodecAlias", [
  offsetAfter(newPolicySource, "export { decode", "decode"),
  offsetAfter(newPolicySource, "export { importedDecode", "importedDecode"),
  offsetAfter(newPolicySource, "export default", "Schema.encodeEffect"),
  offsetAfter(newPolicySource, "destructuredDecode", "destructuredDecode")
])

const barrelPipeSource = `import { Effect, pipe } from "effect"
declare const value: number
declare const inner: (input: number) => number
declare const outer: (input: number) => number
declare const step: (input: number) => number
const makeBarrel = () => pipe(
  Effect.gen(function*() {
    yield* Effect.void
  }),
  (self) => self
)
const composed = pipe(outer(inner(value)), step)
const forwarded = pipe(value, (input) => step(input))
void makeBarrel
void composed
void forwarded
`
const barrelPipeDiagnostics = runOxlintFixture(
  "barrel-pipe-fixture.ts",
  barrelPipeSource,
  ["noFunctionEffectGen", "noNestedCalls", "noUnnecessaryEffectForwarding"]
)
const barrelPipeCounts = new Map()
for (const diagnostic of barrelPipeDiagnostics) {
  barrelPipeCounts.set(diagnostic.code, (barrelPipeCounts.get(diagnostic.code) ?? 0) + 1)
}
assert.equal(barrelPipeCounts.get("effect-local(noFunctionEffectGen)"), 1, encodeJson(barrelPipeDiagnostics))
assert.equal(barrelPipeCounts.get("effect-local(noNestedCalls)"), undefined, encodeJson(barrelPipeDiagnostics))
assert.equal(
  barrelPipeCounts.get("effect-local(noUnnecessaryEffectForwarding)"),
  1,
  encodeJson(barrelPipeDiagnostics)
)
const barrelPipeGenDiagnostics = barrelPipeDiagnostics.filter((diagnostic) =>
  diagnostic.code === "effect-local(noFunctionEffectGen)"
)
assert.deepEqual(
  diagnosticOffsets(barrelPipeGenDiagnostics),
  [offsetOf(barrelPipeSource, "Effect.gen(function*()")],
  encodeJson(barrelPipeDiagnostics)
)

const packagePolicySource = `import * as Data from "effect/Data"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
class Bad extends Data.TaggedError("Bad")<{}> {}
const AssignedBad = class extends Data.TaggedError("AssignedBad")<{}> {}
declare const sql: SqlClient
const rows = sql<{ readonly id: string }>\`select id from rows\`
void rows
`
const packagePolicyDiagnostics = runOxlintPackageFixture(
  "Policy.ts",
  packagePolicySource,
  ["requireSchemaTaggedError", "noBareSqlRowType"]
)
assert.equal(
  packagePolicyDiagnostics.filter((diagnostic) => diagnostic.code === "effect-local(requireSchemaTaggedError)").length,
  2,
  encodeJson(packagePolicyDiagnostics)
)
assert.equal(
  packagePolicyDiagnostics.filter((diagnostic) => diagnostic.code === "effect-local(noBareSqlRowType)").length,
  1,
  encodeJson(packagePolicyDiagnostics)
)

const internalExportDiagnostics = runOxlintPackageFixture(
  "index.ts",
  `export * from "./internal/secret.js"\n`,
  ["noInternalEntrypointExport"],
  { "src/internal/secret.ts": "export const secret = true\n" }
).filter((diagnostic) => diagnostic.code === "effect-local(noInternalEntrypointExport)")
assert.equal(internalExportDiagnostics.length, 1, encodeJson(internalExportDiagnostics))

const boundaryPolicySource = `import * as Effect from "effect/Effect"
class Tagged extends Error { readonly _tag = "Tagged" as const }
export interface HandlerOptions { readonly retryMillis: number }
export interface GoodOptions { readonly retry: import("effect/Duration").Input }
export interface Metrics { readonly latencyMillis: number }
interface PrivateInput { readonly idleSeconds: number }
export const configure = (_input: PrivateInput) => undefined
export const configureInline = (_input: { readonly pauseMillis: number }) => undefined
type ExtraConfig = { readonly delayMillis: number }
type CombinedConfig = ExtraConfig & { readonly retrySeconds: number }
export const configureCombined = (_input: CombinedConfig) => undefined
export const translate = (cause: unknown): Tagged => { void cause; return new Tagged() }
const helpers = { make() { return new Tagged() } }
const failTagged = () => Effect.fail(new Tagged())
const failTaggedBlock = (cause: unknown) => { void cause; return Effect.fail(new Tagged()) }
`
const boundaryPolicyDiagnostics = runOxlintPackageFixture(
  "Boundary.ts",
  boundaryPolicySource,
  ["durationInputAtConfigBoundary", "noModuleErrorHelper", "noClassInstanceFactory"]
)
const boundaryRuleOffsets = (rule) =>
  diagnosticOffsets(
    boundaryPolicyDiagnostics.filter((diagnostic) => diagnostic.code === `effect-local(${rule})`)
  )
assert.equal(
  boundaryPolicyDiagnostics.filter(
    (diagnostic) => diagnostic.code === "effect-local(durationInputAtConfigBoundary)"
  ).length,
  4,
  encodeJson(boundaryPolicyDiagnostics)
)
assert.deepEqual(
  boundaryRuleOffsets("durationInputAtConfigBoundary"),
  ["idleSeconds", "pauseMillis", "delayMillis", "retrySeconds"]
    .map((name) => offsetOf(boundaryPolicySource, name))
    .toSorted((left, right) => left - right),
  encodeJson(boundaryPolicyDiagnostics)
)
assert.equal(
  boundaryPolicyDiagnostics.filter((diagnostic) => diagnostic.code === "effect-local(noModuleErrorHelper)").length,
  4,
  encodeJson(boundaryPolicyDiagnostics)
)
assert.equal(
  boundaryPolicyDiagnostics.filter((diagnostic) => diagnostic.code === "effect-local(noClassInstanceFactory)").length,
  0,
  encodeJson(boundaryPolicyDiagnostics)
)

const sharedConstructorFiles = {
  "src/SharedError.ts": `export class SharedError extends Error { readonly _tag = "SharedError" as const }\n`,
  "src/First.ts": `import { make } from "./internal/shared.js"\nexport const first = make("first")\n`,
  "src/Second.ts": `import * as Shared from "./internal/shared.js"\nexport const second = Shared.make("second")\n`
}
const sharedConstructorDiagnostics = runOxlintPackageFixture(
  "internal/shared.ts",
  `import { SharedError } from "../SharedError.js"\nexport const make = (message: string): SharedError => new SharedError(message)\n`,
  ["noModuleErrorHelper"],
  sharedConstructorFiles,
  0
)
assert.equal(sharedConstructorDiagnostics.length, 0, encodeJson(sharedConstructorDiagnostics))

const duplicateConstructorDiagnostics = runOxlintPackageFixture(
  "internal/first.ts",
  `import { SharedError } from "../SharedError.js"\nexport const make = (message: string): SharedError => new SharedError(message)\n`,
  ["noModuleErrorHelper"],
  {
    ...sharedConstructorFiles,
    "src/internal/second.ts":
      `import { SharedError } from "../SharedError.js"\nexport const makeOther = (message: string): SharedError => new SharedError(message)\n`,
    "src/First.ts": `import { make } from "./internal/first.js"\nexport const first = make("first")\n`,
    "src/Second.ts": `import { make } from "./internal/first.js"\nexport const second = make("second")\n`
  }
).filter((diagnostic) => diagnostic.code === "effect-local(noModuleErrorHelper)")
assert.equal(duplicateConstructorDiagnostics.length, 2, encodeJson(duplicateConstructorDiagnostics))

const importedDurationDiagnostics = runOxlintPackageFixture(
  "Boundary.ts",
  `import type { ExternalOptions } from "./Options.js"
interface ReusedOptions { readonly timeoutMillis: number }
const normalize = (input: ReusedOptions): ReusedOptions => input
const configureLater = (_input: ReusedOptions) => undefined
export const configureExternal = (_input: ExternalOptions) => undefined
export { configureLater }
void normalize
`,
  ["durationInputAtConfigBoundary"],
  { "src/Options.ts": "export interface ExternalOptions { readonly retryMillis: number }\n" }
).filter((diagnostic) => diagnostic.code === "effect-local(durationInputAtConfigBoundary)")
assert.equal(importedDurationDiagnostics.length, 2, encodeJson(importedDurationDiagnostics))
assert.ok(importedDurationDiagnostics.some((diagnostic) => diagnostic.message.includes("retryMillis")))

const mappedSharedConstructorDiagnostics = runOxlintPackageFixture(
  "internal/shared.ts",
  `import { SharedError } from "../SharedError.js"\nexport const make = (message: string): SharedError => new SharedError(message)\n`,
  ["noModuleErrorHelper"],
  {
    "src/SharedError.ts": `export class SharedError extends Error { readonly _tag = "SharedError" as const }\n`,
    "src/First.ts":
      `import { make } from "./internal/shared.js"\nimport * as Effect from "effect/Effect"\nexport const first = Effect.fail("x").pipe(Effect.mapError(make))\n`,
    "src/Second.ts":
      `import * as Shared from "./internal/shared.js"\nimport * as Effect from "effect/Effect"\nexport const second = Effect.fail("x").pipe(Effect.mapError(Shared.make))\n`
  },
  0
)
assert.equal(mappedSharedConstructorDiagnostics.length, 0, encodeJson(mappedSharedConstructorDiagnostics))

const unknownChannelSource = `import * as Effect from "effect/Effect"
interface Tagged { readonly _tag: "Tagged" }
declare const badError: Effect.Effect<string, unknown>
declare const badRequirements: Effect.Effect<string, Tagged, unknown>
declare const badBoth: Effect.Effect<string, unknown, unknown>
declare const inferredUnknown: unknown
const inferred = Effect.fail(inferredUnknown)
declare const goodSuccess: Effect.Effect<unknown, Tagged>
declare const goodSpecific: Effect.Effect<string, Tagged, never>
declare const goodAny: Effect.Effect<string, any, any>
`
const unknownChannelDiagnostics = runOxlintFixture(
  "unknown-effect-channels-fixture.ts",
  unknownChannelSource,
  ["noUnknownEffectChannels"]
).filter((diagnostic) => diagnostic.code === "effect-local(noUnknownEffectChannels)")
assert.equal(unknownChannelDiagnostics.length, 4, encodeJson(unknownChannelDiagnostics))
assert.deepEqual(
  diagnosticOffsets(unknownChannelDiagnostics),
  [
    offsetAfter(unknownChannelSource, "badError", "Effect.Effect"),
    offsetAfter(unknownChannelSource, "badRequirements", "Effect.Effect"),
    offsetAfter(unknownChannelSource, "badBoth", "Effect.Effect"),
    offsetOf(unknownChannelSource, "Effect.fail")
  ].toSorted((left, right) => left - right),
  encodeJson(unknownChannelDiagnostics)
)
assert.deepEqual(
  new Map(unknownChannelDiagnostics.map((diagnostic) => [diagnostic.labels[0].span.offset, diagnostic.message])),
  new Map([
    [offsetAfter(unknownChannelSource, "badError", "Effect.Effect"), noUnknownEffectChannelsMessage(["error"])],
    [
      offsetAfter(unknownChannelSource, "badRequirements", "Effect.Effect"),
      noUnknownEffectChannelsMessage(["requirements"])
    ],
    [
      offsetAfter(unknownChannelSource, "badBoth", "Effect.Effect"),
      noUnknownEffectChannelsMessage(["error", "requirements"])
    ],
    [offsetOf(unknownChannelSource, "Effect.fail"), noUnknownEffectChannelsMessage(["error"])]
  ])
)

const unknownChannelOwnershipSource = `import type * as Effect from "effect/Effect"
declare const _value: Effect.Effect<void, unknown>
`
const unknownChannelOwnershipDiagnostics = runOxlintFixture(
  "unknown-effect-channel-ownership-fixture.ts",
  unknownChannelOwnershipSource,
  ["noUnknownEffectChannels", "requireTaggedEffectError"]
)
assert.deepEqual(
  unknownChannelOwnershipDiagnostics.map((diagnostic) => diagnostic.code),
  ["effect-local(noUnknownEffectChannels)"],
  encodeJson(unknownChannelOwnershipDiagnostics)
)

const jsonPolicySource = `const parsed = JSON.parse("{}")
const encoded = JSON["stringify"]({})
const globalParsed = globalThis.JSON.parse("{}")
const windowParsed = window.JSON.parse("{}")
const selfEncoded = self.JSON.stringify({})
const JsonAlias = JSON
const aliasEncoded = JsonAlias.stringify({})
const { parse: parseJson, ["stringify"]: stringifyJson } = JSON
const shadowed = (JSON: { parse: (value: string) => unknown }) => JSON.parse("{}")
const shadowedGlobal = (globalThis: { JSON: { stringify: (value: unknown) => string } }) =>
  globalThis.JSON.stringify({})
void parsed
void encoded
void globalParsed
void windowParsed
void selfEncoded
void aliasEncoded
void parseJson
void stringifyJson
void shadowed
void shadowedGlobal
`
const jsonPolicyDiagnostics = runOxlintFixture(
  "json-codec-policy-fixture.ts",
  jsonPolicySource,
  ["noJsonParseStringify"]
).filter((diagnostic) => diagnostic.code === "effect-local(noJsonParseStringify)")
assert.equal(jsonPolicyDiagnostics.length, 8, encodeJson(jsonPolicyDiagnostics))
assert.deepEqual(
  diagnosticOffsets(jsonPolicyDiagnostics),
  [
    offsetOf(jsonPolicySource, "JSON.parse"),
    offsetOf(jsonPolicySource, "JSON[\"stringify\"]"),
    offsetOf(jsonPolicySource, "globalThis.JSON.parse"),
    offsetOf(jsonPolicySource, "window.JSON.parse"),
    offsetOf(jsonPolicySource, "self.JSON.stringify"),
    offsetOf(jsonPolicySource, "JsonAlias.stringify"),
    offsetOf(jsonPolicySource, "parse: parseJson"),
    offsetOf(jsonPolicySource, "[\"stringify\"]: stringifyJson")
  ].toSorted((left, right) => left - right),
  encodeJson(jsonPolicyDiagnostics)
)
assert.ok(jsonPolicyDiagnostics.every((diagnostic) =>
  diagnostic.message ===
    "Do not use JSON.parse or JSON.stringify. Define a JSON codec with Schema.fromJsonString and decode or encode through Effect Schema."
))
