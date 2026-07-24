import { describe, expect, it } from "@effect/vitest"
import { execFileSync } from "node:child_process"
import * as fs from "node:fs"
import { createRequire } from "node:module"
import * as os from "node:os"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

interface Pkg {
  readonly version?: string
  readonly dependencies?: Record<string, string>
  readonly peerDependencies?: Record<string, string>
  readonly devDependencies?: Record<string, string>
  readonly pnpm?: { readonly overrides?: Record<string, string> }
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

const require_ = createRequire(import.meta.url)
const readPkg = (file: string): Pkg => JSON.parse(fs.readFileSync(file, "utf8"))
const resolvePkg = (specifier: string): Pkg & { readonly version: string } => {
  const pkg = readPkg(require_.resolve(specifier))
  if (pkg.version === undefined) throw new Error(`Missing version in ${specifier}`)
  return pkg as Pkg & { readonly version: string }
}

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url))
const readme = fs.readFileSync(path.join(repoRoot, "README.md"), "utf8")
const rootPkg = readPkg(path.join(repoRoot, "package.json"))
const browserPkg = readPkg(path.join(repoRoot, "packages/local-browser/package.json"))
const testPkg = readPkg(path.join(repoRoot, "packages/local-test/package.json"))

const effect = resolvePkg("effect/package.json")
const waSqlite = resolvePkg("@effect/wa-sqlite/package.json")
const sqlSqliteWasm = resolvePkg("@effect/sql-sqlite-wasm/package.json")
const platformNodeSharedRequire = createRequire(require_.resolve("@effect/platform-node/package.json"))
const platformNodeShared = readPkg(platformNodeSharedRequire.resolve("@effect/platform-node-shared/package.json"))

const entry = (record: Record<string, string> | undefined, key: string): string => {
  const value = record?.[key]
  if (value === undefined) throw new Error(`Missing ${key}`)
  return value
}

const packageSpec = (token: string): readonly [name: string, version: string] => {
  const separator = token.lastIndexOf("@")
  return separator > 0 ? [token.slice(0, separator), token.slice(separator + 1)] : [token, "*"]
}

const match1 = (source: string, pattern: RegExp): string => {
  const matched = source.match(pattern)
  if (matched === null) throw new Error(`No match for ${pattern}`)
  return matched[1]
}

const satisfiesCaret = (version: string, range: string): boolean => {
  const [vMajor, vMinor, vPatch] = version.split(".").map(Number)
  const [fMajor, fMinor, fPatch] = range.slice(1).split(".").map(Number)
  const upper = fMajor > 0 ? [fMajor + 1, 0, 0] : fMinor > 0 ? [0, fMinor + 1, 0] : [0, 0, fPatch + 1]
  const compare = (a: ReadonlyArray<number>, b: ReadonlyArray<number>): number =>
    a[0] - b[0] || a[1] - b[1] || a[2] - b[2]
  return compare([vMajor, vMinor, vPatch], [fMajor, fMinor, fPatch]) >= 0 &&
    compare([vMajor, vMinor, vPatch], upper) < 0
}

const documentedInstallCommands = [...readme.matchAll(/^pnpm add(?: -D)? (.+)$/gm)].map((match) => match[1].split(" "))

const commandContaining = (token: string): ReadonlyArray<string> => {
  const command = documentedInstallCommands.find((candidate) => candidate.includes(token))
  if (command === undefined) throw new Error(`No install command contains ${token}`)
  return command
}

const baseCommand = [
  "@lucas-barake/effect-local",
  "effect@4.0.0-beta.99",
  "@automerge/automerge@3.3.2"
]
const nodeCommand = [
  "@lucas-barake/effect-local-sql",
  "@effect/platform-node@4.0.0-beta.99",
  "@effect/platform-node-shared@4.0.0-beta.99",
  "@effect/sql-sqlite-node@4.0.0-beta.99"
]
const browserPackagesCommand = [
  "@lucas-barake/effect-local-sql",
  "@lucas-barake/effect-local-browser"
]
const browserProvidersCommand = [
  "@effect/platform-browser@4.0.0-beta.99",
  "@effect/sql-sqlite-wasm@4.0.0-beta.99",
  "@effect/wa-sqlite@0.1.2"
]
const testPackagesCommand = [
  "@lucas-barake/effect-local-test",
  "@effect/vitest@4.0.0-beta.99",
  "vitest@4.1.10"
]
const testNodeCommand = [
  "@effect/platform-node@4.0.0-beta.99",
  "@effect/platform-node-shared@4.0.0-beta.99"
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
  JSON.parse(
    execFileSync(
      "pnpm",
      ["--dir", packageDirectory, "pack", "--json", "--pack-destination", destination],
      { cwd: repoRoot, encoding: "utf8", maxBuffer: 10 * 1024 * 1024 }
    )
  )

const resolvedSharedVersion = (consumerDirectory: string): string => {
  const consumerRequire = createRequire(path.join(consumerDirectory, "package.json"))
  const testRequire = createRequire(consumerRequire.resolve("@lucas-barake/effect-local-test/package.json"))
  const platformNodeRequire = createRequire(testRequire.resolve("@effect/platform-node/package.json"))
  const shared = readPkg(platformNodeRequire.resolve("@effect/platform-node-shared/package.json"))
  return entry({ version: shared.version ?? "" }, "version")
}

const assertEntrypointLoads = (
  consumerDirectory: string,
  entrypoint: string,
  expectedExport: string
): void => {
  execFileSync(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      `const module = await import(${JSON.stringify(entrypoint)});` +
      `if (module[${JSON.stringify(expectedExport)}] === undefined) process.exit(1)`
    ],
    { cwd: consumerDirectory, encoding: "utf8" }
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
    expect(commandContaining("effect@4.0.0-beta.99")).toEqual(baseCommand)
    expect(commandContaining("@effect/sql-sqlite-node@4.0.0-beta.99")).toEqual(nodeCommand)
    expect(commandContaining("@lucas-barake/effect-local-browser")).toEqual(browserPackagesCommand)
    expect(commandContaining("@effect/sql-sqlite-wasm@4.0.0-beta.99")).toEqual(browserProvidersCommand)
    expect(commandContaining("@effect/vitest@4.0.0-beta.99")).toEqual(testPackagesCommand)
    expect(commandContaining("@effect/platform-node-shared@4.0.0-beta.99")).toEqual(nodeCommand)
    expect(documentedInstallCommands.filter((command) =>
      command.includes(
        "@effect/platform-node-shared@4.0.0-beta.99"
      )
    )).toEqual([nodeCommand, testNodeCommand])
  })

  it("resolves each documented packed package graph with npm and pnpm", { timeout: 180_000 }, () => {
    const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "effect-local-peer-invariants-"))
    try {
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
      const packedTestManifest = JSON.parse(
        execFileSync(
          "tar",
          ["-xOf", artifacts["@lucas-barake/effect-local-test"].slice("file:".length), "package/package.json"],
          { encoding: "utf8" }
        )
      ) as Pkg
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
            JSON.stringify({
              private: true,
              type: "module",
              packageManager: "pnpm@10.18.1",
              dependencies: { ...documented, ...recipeArtifacts },
              pnpm: { overrides: recipeArtifacts }
            })
          )
          const args = packageManager === "npm"
            ? ["install", "--ignore-scripts", "--engine-strict", "--strict-peer-deps", "--no-audit", "--no-fund"]
            : ["install", "--ignore-scripts", "--strict-peer-dependencies", "--config.engine-strict=true"]
          try {
            execFileSync(packageManager, args, {
              cwd: consumerDirectory,
              encoding: "utf8",
              maxBuffer: 10 * 1024 * 1024,
              stdio: "pipe"
            })
          } catch (error) {
            const failure = error as { readonly stderr?: string; readonly stdout?: string }
            throw new Error(
              `${recipe.name} ${packageManager} install failed\n${failure.stdout ?? ""}\n${failure.stderr ?? ""}`,
              { cause: error }
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
    } finally {
      fs.rmSync(temporaryDirectory, { recursive: true, force: true })
    }
  })
})
