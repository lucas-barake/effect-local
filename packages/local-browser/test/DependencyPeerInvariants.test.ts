import { describe, expect, it } from "@effect/vitest"
import * as fs from "node:fs"
import { createRequire } from "node:module"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url))
const lockfile = fs.readFileSync(path.join(repoRoot, "pnpm-lock.yaml"), "utf8")
const readme = fs.readFileSync(path.join(repoRoot, "README.md"), "utf8")

interface Version {
  readonly major: number
  readonly minor: number
  readonly patch: number
  readonly prerelease: ReadonlyArray<string>
}

const parseVersion = (raw: string): Version => {
  const [core, prerelease] = raw.split("-", 2)
  const [major, minor, patch] = core!.split(".").map(Number) as [number, number, number]
  return { major, minor, patch, prerelease: prerelease === undefined ? [] : prerelease.split(".") }
}

const compareVersions = (a: Version, b: Version): number => {
  if (a.major !== b.major) return a.major - b.major
  if (a.minor !== b.minor) return a.minor - b.minor
  if (a.patch !== b.patch) return a.patch - b.patch
  if (a.prerelease.length === 0 && b.prerelease.length > 0) return 1
  if (a.prerelease.length > 0 && b.prerelease.length === 0) return -1
  const length = Math.max(a.prerelease.length, b.prerelease.length)
  for (let i = 0; i < length; i++) {
    const left = a.prerelease[i]
    const right = b.prerelease[i]
    if (left === undefined) return -1
    if (right === undefined) return 1
    const leftNumber = Number(left)
    const rightNumber = Number(right)
    if (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber)) {
      if (leftNumber !== rightNumber) return leftNumber - rightNumber
    } else if (left !== right) {
      return left < right ? -1 : 1
    }
  }
  return 0
}

const satisfiesCaret = (version: string, range: string): boolean => {
  if (!range.startsWith("^")) throw new Error(`Unsupported range operator: ${range}`)
  const min = parseVersion(range.slice(1))
  const v = parseVersion(version)
  const upper: Version = min.major > 0
    ? { major: min.major + 1, minor: 0, patch: 0, prerelease: [] }
    : min.minor > 0
    ? { major: 0, minor: min.minor + 1, patch: 0, prerelease: [] }
    : { major: 0, minor: 0, patch: min.patch + 1, prerelease: [] }
  return compareVersions(v, min) >= 0 && compareVersions(v, upper) < 0
}

const extractBlocks = (section: string, namePrefix: string): Array<{ version: string; block: string }> => {
  const pattern = new RegExp(` {2}'${namePrefix}@([^']+)':\\n([\\s\\S]*?)(?=\\n {2}\\S|$)`, "g")
  return [...section.matchAll(pattern)].map(([, version, block]) => ({ version: version!, block: block! }))
}

const namedSection = (name: string): string => {
  const lines = lockfile.split("\n")
  const startIndex = lines.findIndex((line) => line === `${name}:`)
  if (startIndex === -1) throw new Error(`Lockfile section not found: ${name}`)
  const endIndex = lines.findIndex((line, i) => i > startIndex && /^\S/.test(line))
  return lines.slice(startIndex + 1, endIndex === -1 ? undefined : endIndex).join("\n")
}

const packagesSection = namedSection("packages")
const snapshotsSection = namedSection("snapshots")
const importersSection = namedSection("importers")

describe("pnpm-lock.yaml peer dependency invariants", () => {
  it("resolves @effect/wa-sqlite to a version @effect/sql-sqlite-wasm's declared peer range allows", () => {
    const [declared] = extractBlocks(packagesSection, "@effect/sql-sqlite-wasm")
    expect(declared).toBeDefined()
    const declaredRangeMatch = declared!.block.match(/'@effect\/wa-sqlite': (\S+)/)
    expect(declaredRangeMatch).not.toBeNull()

    const [resolved] = extractBlocks(snapshotsSection, "@effect/sql-sqlite-wasm")
    expect(resolved).toBeDefined()
    const resolvedWaSqliteMatch = resolved!.version.match(/@effect\/wa-sqlite@([^)]+)\)/)
    expect(resolvedWaSqliteMatch).not.toBeNull()

    expect(satisfiesCaret(resolvedWaSqliteMatch![1]!, declaredRangeMatch![1]!)).toBe(true)
  })

  it("resolves @effect/platform-node-shared to a version whose effect peer the resolved effect version satisfies", () => {
    const resolvedEffectMatch = importersSection.match(
      / {2}\.:\n[\s\S]*?\n {6}effect:\n {8}specifier: [^\n]+\n {8}version: (\S+)/
    )
    expect(resolvedEffectMatch).not.toBeNull()
    const resolvedEffect = resolvedEffectMatch![1]!

    const declaredEntries = extractBlocks(packagesSection, "@effect/platform-node-shared")
    expect(declaredEntries.length).toBeGreaterThan(0)

    for (const entry of declaredEntries) {
      const effectPeerMatch = entry.block.match(/\n {6}effect: (\S+)/)
      expect(effectPeerMatch).not.toBeNull()
      expect(satisfiesCaret(resolvedEffect, effectPeerMatch![1]!)).toBe(true)
    }
  })

  it("documents a browser install wa-sqlite version that satisfies the installed sql-sqlite-wasm peer range", () => {
    const require_ = createRequire(import.meta.url)
    const sqlSqliteWasmPkgPath = require_.resolve("@effect/sql-sqlite-wasm/package.json")
    const sqlSqliteWasmPkg = JSON.parse(fs.readFileSync(sqlSqliteWasmPkgPath, "utf8")) as {
      peerDependencies?: Record<string, string>
    }
    const installedRange = sqlSqliteWasmPkg.peerDependencies?.["@effect/wa-sqlite"]
    expect(installedRange).toBeDefined()

    const readmeMatch = readme.match(/@effect\/wa-sqlite@(\S+)/)
    expect(readmeMatch).not.toBeNull()

    expect(satisfiesCaret(readmeMatch![1]!, installedRange!)).toBe(true)
  })
})
