import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- This test exercises real package installation and a child-process-backed local registry boundary.
import { execFileSync, spawn } from "node:child_process"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The temporary consumer and package archive paths must use the host filesystem directly.
import * as fs from "node:fs"
import * as NodeModule from "node:module"
import * as os from "node:os"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- The package installation harness must construct host paths for real npm and pnpm processes.
import * as path from "node:path"
import { fileURLToPath } from "node:url"

interface Pkg {
  readonly version?: string | undefined
  readonly dependencies?: Record<string, string> | undefined
  readonly peerDependencies?: Record<string, string> | undefined
  readonly devDependencies?: Record<string, string> | undefined
  readonly pnpm?: { readonly overrides?: Record<string, string> | undefined } | undefined
}

interface PackedPackage {
  readonly filename: string
}

interface ConsumerRecipe {
  readonly name: string
  readonly commands: ReadonlyArray<ReadonlyArray<string>>
  readonly internalPackages: ReadonlyArray<string>
  readonly entrypoint: string
  readonly expectedExport: string
}

const PkgSchema = Schema.Struct({
  version: Schema.optional(Schema.String),
  dependencies: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  peerDependencies: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  devDependencies: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  pnpm: Schema.optional(Schema.Struct({
    overrides: Schema.optional(Schema.Record(Schema.String, Schema.String))
  }))
})
const PackedPackageSchema = Schema.Struct({ filename: Schema.String })
const ChildProcessFailureSchema = Schema.Struct({
  stderr: Schema.optional(Schema.String),
  stdout: Schema.optional(Schema.String)
})
const require_ = NodeModule.createRequire(import.meta.url)
const die = (message: string): never => Effect.runSync(Effect.die(new Error(message)))
const runSyncThrowing = <A,>(thunk: () => A): A =>
  Effect.runSync(Effect.try({ try: thunk, catch: (cause) => cause }).pipe(Effect.orDie))
const readPkg = (file: string): Pkg =>
  runSyncThrowing(() => Schema.decodeUnknownSync(Schema.fromJsonString(PkgSchema))(fs.readFileSync(file, "utf8")))
const resolvePkg = (specifier: string): Pkg & { readonly version: string } => {
  const pkg = readPkg(require_.resolve(specifier))
  if (pkg.version === undefined) return die(`Missing version in ${specifier}`)
  return { ...pkg, version: pkg.version }
}

const repoRoot = runSyncThrowing(() => fileURLToPath(new URL("../../..", import.meta.url)))
const nodeProcess = globalThis.process
const readme = runSyncThrowing(() => fs.readFileSync(path.join(repoRoot, "README.md"), "utf8"))
const rootPkg = readPkg(path.join(repoRoot, "package.json"))
const browserPkg = readPkg(path.join(repoRoot, "packages/local-browser/package.json"))
const testPkg = readPkg(path.join(repoRoot, "packages/local-test/package.json"))

const effect = resolvePkg("effect/package.json")
const vite = resolvePkg("vite/package.json")
const waSqlite = resolvePkg("@effect/wa-sqlite/package.json")
const sqlSqliteWasm = resolvePkg("@effect/sql-sqlite-wasm/package.json")
const platformNodeSharedRequire = NodeModule.createRequire(
  require_.resolve("@effect/platform-node/package.json")
)
const platformNodeShared = readPkg(platformNodeSharedRequire.resolve("@effect/platform-node-shared/package.json"))

const entry = (record: Record<string, string> | undefined, key: string): string => {
  const value = record?.[key]
  if (value === undefined) return die(`Missing ${key}`)
  return value
}

const packageSpec = (token: string): readonly [name: string, version: string] => {
  const separator = token.lastIndexOf("@")
  if (separator > 0) return [token.slice(0, separator), token.slice(separator + 1)]
  return [token, "*"]
}

const match1 = (source: string, pattern: RegExp): string => {
  const matched = source.match(pattern)
  if (matched === null) return die(`No match for ${pattern}`)
  return matched[1]
}

const satisfiesCaret = (version: string, range: string): boolean => {
  const [vMajor, vMinor, vPatch] = version.split(".").map(Number)
  const [fMajor, fMinor, fPatch] = range.slice(1).split(".").map(Number)
  let upper: Array<number>
  if (fMajor > 0) upper = [fMajor + 1, 0, 0]
  else if (fMinor > 0) upper = [0, fMinor + 1, 0]
  else upper = [0, 0, fPatch + 1]
  const compare = (a: ReadonlyArray<number>, b: ReadonlyArray<number>): number =>
    a[0] - b[0] || a[1] - b[1] || a[2] - b[2]
  return compare([vMajor, vMinor, vPatch], [fMajor, fMinor, fPatch]) >= 0 &&
    compare([vMajor, vMinor, vPatch], upper) < 0
}

const documentedInstallCommands: ReadonlyArray<ReadonlyArray<string>> = [
  ...readme.matchAll(/^pnpm add(?: -D)? (.+)$/gm)
].map((match): ReadonlyArray<string> => match[1].split(" "))

const commandContaining = (token: string): ReadonlyArray<string> => {
  const command = documentedInstallCommands.find((candidate) => candidate.includes(token))
  if (command === undefined) return die(`No install command contains ${token}`)
  return command
}

const baseCommand = [
  "@lucas-barake/effect-local",
  "effect@4.0.0-beta.103",
  "@automerge/automerge@3.3.2"
]
const nodeCommand = [
  "@lucas-barake/effect-local-sql",
  "@effect/platform-node@4.0.0-beta.103",
  "@effect/platform-node-shared@4.0.0-beta.103",
  "@effect/sql-sqlite-node@4.0.0-beta.103"
]
const browserPackagesCommand = [
  "@lucas-barake/effect-local-sql",
  "@lucas-barake/effect-local-browser"
]
const browserProvidersCommand = [
  "@effect/platform-browser@4.0.0-beta.103",
  "@effect/sql-sqlite-wasm@4.0.0-beta.103",
  "@effect/wa-sqlite@0.1.2"
]
const testPackagesCommand = [
  "@lucas-barake/effect-local-test",
  "@effect/vitest@4.0.0-beta.103",
  "vitest@4.1.10"
]
const testNodeCommand = [
  "@effect/platform-node@4.0.0-beta.103",
  "@effect/platform-node-shared@4.0.0-beta.103"
]

const consumerRecipes: ReadonlyArray<ConsumerRecipe> = [
  {
    name: "node",
    commands: [baseCommand, nodeCommand],
    internalPackages: ["@lucas-barake/effect-local", "@lucas-barake/effect-local-sql"],
    entrypoint: "@lucas-barake/effect-local-sql",
    expectedExport: "SqlReplica"
  },
  {
    name: "browser",
    commands: [baseCommand, browserPackagesCommand, browserProvidersCommand],
    internalPackages: [
      "@lucas-barake/effect-local",
      "@lucas-barake/effect-local-sql",
      "@lucas-barake/effect-local-browser"
    ],
    entrypoint: "@lucas-barake/effect-local-browser",
    expectedExport: "BrowserSqlite"
  },
  {
    name: "test",
    commands: [baseCommand, testPackagesCommand, testNodeCommand],
    internalPackages: [
      "@lucas-barake/effect-local",
      "@lucas-barake/effect-local-sql",
      "@lucas-barake/effect-local-test"
    ],
    entrypoint: "@lucas-barake/effect-local-test",
    expectedExport: "TestReplica"
  }
]

const packPackage = (packageDirectory: string, destination: string): PackedPackage =>
  runSyncThrowing(() =>
    Schema.decodeUnknownSync(Schema.fromJsonString(PackedPackageSchema))(
      execFileSync(
        "pnpm",
        ["--dir", packageDirectory, "pack", "--json", "--pack-destination", destination],
        { cwd: repoRoot, encoding: "utf8", maxBuffer: 10 * 1024 * 1024 }
      )
    )
  )

const startLocalRegistry = (
  registryDirectory: string
): Promise<{ readonly close: () => Promise<void>; readonly url: string }> =>
  Effect.runPromise(Effect.callback((resume) => {
    const registry = spawn(
      process.execPath,
      [
        fileURLToPath(new URL("fixtures/local-package-registry.mjs", import.meta.url)),
        repoRoot,
        registryDirectory
      ],
      { stdio: ["ignore", "pipe", "pipe"] }
    )
    let stderr = ""
    registry.stderr.setEncoding("utf8")
    registry.stderr.on("data", (chunk: string) => {
      stderr += chunk
    })
    registry.once("error", (error: unknown) => resume(Effect.die(error)))
    registry.once("exit", (code: number | null) => {
      resume(Effect.die(new Error(`Local package registry exited with code ${code}\n${stderr}`)))
    })
    registry.stdout.setEncoding("utf8")
    registry.stdout.once("data", (chunk: string) => {
      const url = chunk.trim()
      resume(Effect.succeed({
        url,
        close: () =>
          Effect.runPromise(Effect.callback((resumeClose) => {
            registry.once("exit", (code: number | null) => {
              if (code === 0) resumeClose(Effect.void)
              else resumeClose(Effect.die(new Error(`Local package registry exited with code ${code}\n${stderr}`)))
            })
            registry.kill("SIGTERM")
          }))
      }))
    })
  }))

const resolvedSharedVersion = (consumerDirectory: string): string => {
  const consumerRequire = NodeModule.createRequire(path.join(consumerDirectory, "package.json"))
  const testRequire = NodeModule.createRequire(
    consumerRequire.resolve("@lucas-barake/effect-local-test/package.json")
  )
  const platformNodeRequire = NodeModule.createRequire(
    testRequire.resolve("@effect/platform-node/package.json")
  )
  const shared = readPkg(platformNodeRequire.resolve("@effect/platform-node-shared/package.json"))
  return entry({ version: shared.version ?? "" }, "version")
}

const assertEntrypointLoads = (
  consumerDirectory: string,
  entrypoint: string,
  expectedExport: string
): void => {
  const entrypointJson = Schema.encodeSync(Schema.fromJsonString(Schema.String))(entrypoint)
  const expectedExportJson = Schema.encodeSync(Schema.fromJsonString(Schema.String))(expectedExport)
  runSyncThrowing(() =>
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `const module = await import(${entrypointJson});` +
        `if (module[${expectedExportJson}] === undefined) process.exit(1)`
      ],
      { cwd: consumerDirectory, encoding: "utf8" }
    )
  )
}

describe("dependency peer invariants", () => {
  it("pins @effect/platform-node-shared to a version whose effect peer the resolved effect satisfies", () => {
    const override = entry(rootPkg.pnpm?.overrides, "@effect/platform-node-shared")
    expect(platformNodeShared.version).toBe(override)
    expect(entry(testPkg.dependencies, "@effect/platform-node-shared")).toBe(override)
    expect(entry(platformNodeShared.peerDependencies, "effect")).toBe(`^${effect.version}`)
    const documentedPins = documentedInstallCommands
      .filter((command) => command.some((token) => token.startsWith("@effect/platform-node@")))
      .map((command) => command.find((token) => token.startsWith("@effect/platform-node-shared@")))
    expect(documentedPins).toEqual([
      `@effect/platform-node-shared@${override}`,
      `@effect/platform-node-shared@${override}`
    ])
  })

  it("resolves and documents @effect/wa-sqlite within @effect/sql-sqlite-wasm's peer range", () => {
    const range = entry(sqlSqliteWasm.peerDependencies, "@effect/wa-sqlite")
    const documented = match1(readme, /@effect\/wa-sqlite@(\S+)/)
    expect(documented).toBe(entry(browserPkg.devDependencies, "@effect/wa-sqlite"))
    expect(documented).toBe(waSqlite.version)
    expect(satisfiesCaret(waSqlite.version, range)).toBe(true)
  })

  it("documents the exact Node, browser, and test install recipes", () => {
    expect(commandContaining("effect@4.0.0-beta.103")).toEqual(baseCommand)
    expect(commandContaining("@effect/sql-sqlite-node@4.0.0-beta.103")).toEqual(nodeCommand)
    expect(commandContaining("@lucas-barake/effect-local-browser")).toEqual(browserPackagesCommand)
    expect(commandContaining("@effect/sql-sqlite-wasm@4.0.0-beta.103")).toEqual(browserProvidersCommand)
    expect(commandContaining("@effect/vitest@4.0.0-beta.103")).toEqual(testPackagesCommand)
    expect(commandContaining("@effect/platform-node-shared@4.0.0-beta.103")).toEqual(nodeCommand)
    expect(documentedInstallCommands.filter((command) =>
      command.includes(
        "@effect/platform-node-shared@4.0.0-beta.103"
      )
    )).toEqual([nodeCommand, testNodeCommand])
  })

  it.effect("resolves each documented packed package graph with npm and pnpm", () =>
    Effect.gen(function*() {
      const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "effect-local-peer-invariants-"))
      yield* Effect.acquireUseRelease(
        Effect.promise(() => startLocalRegistry(path.join(temporaryDirectory, "registry"))),
        (registry) =>
          Effect.gen(function*() {
            const packDirectory = path.join(temporaryDirectory, "packs")
            fs.mkdirSync(packDirectory)
            const artifacts = Object.fromEntries(
              [
                ["@lucas-barake/effect-local", "packages/local"],
                ["@lucas-barake/effect-local-sql", "packages/local-sql"],
                ["@lucas-barake/effect-local-browser", "packages/local-browser"],
                ["@lucas-barake/effect-local-rpc", "packages/local-rpc"],
                ["@lucas-barake/effect-local-test", "packages/local-test"]
              ].map(([name, directory]) => [
                name,
                `file:${packPackage(path.join(repoRoot, directory), packDirectory).filename}`
              ])
            )
            const expectedShared = entry(rootPkg.pnpm?.overrides, "@effect/platform-node-shared")
            const packedTestManifest = Schema.decodeUnknownSync(Schema.fromJsonString(PkgSchema))(
              execFileSync(
                "tar",
                ["-xOf", artifacts["@lucas-barake/effect-local-test"].slice("file:".length), "package/package.json"],
                { encoding: "utf8" }
              )
            )
            expect(entry(packedTestManifest.dependencies, "@effect/platform-node-shared")).toBe(expectedShared)

            for (const recipe of consumerRecipes) {
              const recipeArtifacts = Object.fromEntries(
                recipe.internalPackages.map((name) => [name, artifacts[name]])
              )
              const documented = Object.fromEntries(recipe.commands.flat().map(packageSpec))
              for (const packageManager of ["npm", "pnpm"]) {
                const consumerDirectory = path.join(temporaryDirectory, `${recipe.name}-${packageManager}`)
                fs.mkdirSync(consumerDirectory)
                fs.writeFileSync(
                  path.join(consumerDirectory, "package.json"),
                  Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))({
                    private: true,
                    type: "module",
                    packageManager: "pnpm@10.18.1",
                    dependencies: { ...documented, ...recipeArtifacts },
                    // vite 8's rolldown wasm binding currently ships an optional peer chain that
                    // strict peer resolution refuses in both npm and pnpm
                    // (@rolldown/binding-wasm32-wasi -> @napi-rs/wasm-runtime -> @emnapi/core). The
                    // repository's own dev tooling resolves on vite 7, so the consumer graph pins the
                    // same line in both managers until the upstream chain resolves cleanly again.
                    overrides: { vite: vite.version },
                    pnpm: { overrides: { ...recipeArtifacts, vite: vite.version } }
                  })
                )
                let args: ReadonlyArray<string>
                if (packageManager === "npm") {
                  args = ["install", "--ignore-scripts", "--strict-peer-deps", "--no-audit", "--no-fund"]
                } else {
                  args = ["install", "--ignore-scripts", "--strict-peer-dependencies"]
                }
                const install = Effect.runSyncExit(
                  Effect.try({
                    try: () =>
                      execFileSync(packageManager, args, {
                        cwd: consumerDirectory,
                        encoding: "utf8",
                        maxBuffer: 10 * 1024 * 1024,
                        stdio: "pipe",
                        env: {
                          ...nodeProcess.env,
                          HTTP_PROXY: "http://127.0.0.1:9",
                          HTTPS_PROXY: "http://127.0.0.1:9",
                          NO_PROXY: "127.0.0.1,localhost",
                          http_proxy: "http://127.0.0.1:9",
                          https_proxy: "http://127.0.0.1:9",
                          no_proxy: "127.0.0.1,localhost",
                          npm_config_audit: "false",
                          npm_config_cache: path.join(temporaryDirectory, `cache-${recipe.name}-${packageManager}`),
                          npm_config_fund: "false",
                          npm_config_registry: registry.url,
                          npm_config_update_notifier: "false"
                        }
                      }),
                    catch: (cause) => cause
                  }).pipe(Effect.orDie)
                )
                if (Exit.isFailure(install)) {
                  const cause = Cause.squash(install.cause)
                  const failure = Option.getOrElse(
                    Schema.decodeUnknownOption(ChildProcessFailureSchema)(cause),
                    () => ({ stderr: undefined, stdout: undefined })
                  )
                  yield* Effect.die(
                    new Error(
                      `${recipe.name} ${packageManager} install failed\n${failure.stdout ?? ""}\n${
                        failure.stderr ?? ""
                      }`,
                      { cause }
                    )
                  )
                }
                assertEntrypointLoads(
                  consumerDirectory,
                  recipe.entrypoint,
                  recipe.expectedExport
                )
                if (recipe.name === "test") {
                  expect(resolvedSharedVersion(consumerDirectory)).toBe(expectedShared)
                }
              }
            }
          }),
        (registry) => Effect.promise(() => registry.close())
      ).pipe(Effect.ensuring(
        Effect.try({
          try: () => fs.rmSync(temporaryDirectory, { recursive: true, force: true }),
          catch: (cause) => cause
        }).pipe(Effect.orDie)
      ))
    }), { timeout: 0 })
})
