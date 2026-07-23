import { assert, describe, it } from "@effect/vitest"
import { readdirSync, readFileSync, statSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../../..")

const workspaceGlobs = (): ReadonlyArray<string> => {
  const workspaceConfig = readFileSync(join(repoRoot, "pnpm-workspace.yaml"), "utf8")
  const packagesBlock = workspaceConfig.match(/^packages:\n((?:[ \t]+-.*\n?)+)/m)
  if (packagesBlock === null) return []
  return [...packagesBlock[1]!.matchAll(/-\s*(\S+)/g)].map((match) => match[1]!)
}

interface WorkspacePackage {
  readonly name: string
  readonly directory: string
  readonly json: Record<string, unknown>
}

const discoverWorkspacePackages = (): ReadonlyArray<WorkspacePackage> =>
  workspaceGlobs().flatMap((glob) => {
    const prefix = glob.endsWith("/*") ? glob.slice(0, -2) : glob
    const parentDirectory = join(repoRoot, prefix)
    const candidateDirectories = glob.endsWith("/*")
      ? readdirSync(parentDirectory).map((entry) => join(parentDirectory, entry))
      : [parentDirectory]
    return candidateDirectories.filter((directory) => statSync(directory).isDirectory()).flatMap((directory) => {
      const manifestPath = join(directory, "package.json")
      try {
        const json = JSON.parse(readFileSync(manifestPath, "utf8")) as Record<string, unknown>
        return [{ name: json.name as string, directory, json }]
      } catch {
        return []
      }
    })
  })

const discoverPublishedPackages = (): ReadonlyArray<WorkspacePackage> =>
  discoverWorkspacePackages().filter((workspacePackage) => workspacePackage.json.private !== true)

const parseVersion = (version: string): readonly [number, number, number] => {
  const [major = 0, minor = 0, patch = 0] = version.split(".").map(Number)
  return [major, minor, patch]
}

const compareVersions = (left: readonly [number, number, number], right: readonly [number, number, number]) => {
  for (let index = 0; index < 3; index++) {
    if (left[index] !== right[index]) return left[index]! - right[index]!
  }
  return 0
}

const rangeAllows = (range: string, version: string): boolean => {
  const match = range.trim().match(/^>=\s*(\d+(?:\.\d+){0,2})$/)
  return match !== null && compareVersions(parseVersion(version), parseVersion(match[1]!)) >= 0
}

describe("published package engines", () => {
  const packages = discoverPublishedPackages()

  it("discovers the published workspace packages", () => {
    assert.isAbove(packages.length, 0)
  })

  for (const workspacePackage of packages) {
    it(`declares a Node engines floor that excludes 18 and 19 for ${workspacePackage.name}`, () => {
      const engines = workspacePackage.json.engines as { node?: unknown } | undefined
      assert.isString(engines?.node, `${workspacePackage.name} is missing an "engines.node" field`)
      const range = engines!.node as string
      assert.isFalse(rangeAllows(range, "18.0.0"), `${workspacePackage.name} engines.node ${range} allows Node 18`)
      assert.isFalse(rangeAllows(range, "19.9.9"), `${workspacePackage.name} engines.node ${range} allows Node 19`)
      assert.isTrue(rangeAllows(range, "20.0.0"), `${workspacePackage.name} engines.node ${range} rejects Node 20`)
    })
  }
})
