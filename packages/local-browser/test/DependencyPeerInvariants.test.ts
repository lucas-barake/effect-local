import { describe, expect, it } from "@effect/vitest"
import * as fs from "node:fs"
import { createRequire } from "node:module"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

interface Pkg {
  readonly version: string
  readonly peerDependencies?: Record<string, string>
  readonly devDependencies?: Record<string, string>
  readonly pnpm?: { readonly overrides?: Record<string, string> }
}

const require_ = createRequire(import.meta.url)
const readPkg = (file: string): Pkg => JSON.parse(fs.readFileSync(file, "utf8"))
const resolvePkg = (specifier: string): Pkg => readPkg(require_.resolve(specifier))

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url))
const readme = fs.readFileSync(path.join(repoRoot, "README.md"), "utf8")
const rootPkg = readPkg(path.join(repoRoot, "package.json"))
const browserPkg = readPkg(path.join(repoRoot, "packages/local-browser/package.json"))

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

describe("dependency peer invariants", () => {
  it("pins @effect/platform-node-shared to a version whose effect peer the resolved effect satisfies", () => {
    const override = entry(rootPkg.pnpm?.overrides, "@effect/platform-node-shared")
    expect(platformNodeShared.version).toBe(override)
    expect(entry(platformNodeShared.peerDependencies, "effect")).toBe(`^${effect.version}`)
  })

  it("resolves and documents @effect/wa-sqlite within @effect/sql-sqlite-wasm's peer range", () => {
    const range = entry(sqlSqliteWasm.peerDependencies, "@effect/wa-sqlite")
    const documented = match1(readme, /@effect\/wa-sqlite@(\S+)/)
    expect(documented).toBe(entry(browserPkg.devDependencies, "@effect/wa-sqlite"))
    expect(documented).toBe(waSqlite.version)
    expect(satisfiesCaret(waSqlite.version, range)).toBe(true)
  })
})
