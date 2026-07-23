import { assert, describe, it } from "@effect/vitest"
import { readdirSync, readFileSync, statSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../../..")

const expectedPublishedPackages = [
  "@lucas-barake/effect-local",
  "@lucas-barake/effect-local-browser",
  "@lucas-barake/effect-local-rpc",
  "@lucas-barake/effect-local-sql",
  "@lucas-barake/effect-local-test"
]

const expectedEnginesNode = ">=20.0.0"

const workspaceGlobs = (): ReadonlyArray<string> => {
  const workspaceConfig = readFileSync(join(repoRoot, "pnpm-workspace.yaml"), "utf8")
  const packagesBlock = workspaceConfig.match(/^packages:\n((?:[ \t]+-.*\n?)+)/m)
  if (packagesBlock === null) return []
  return [...packagesBlock[1].matchAll(/-\s*(\S+)/g)].map((match) => match[1])
}

interface WorkspacePackage {
  readonly name: string
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
        return [{ name: json.name as string, json }]
      } catch {
        return []
      }
    })
  })

const publishedPackages = discoverWorkspacePackages().filter((workspacePackage) =>
  workspacePackage.json.private !== true
)

describe("published package engines", () => {
  it("discovers exactly the published workspace packages", () => {
    assert.deepStrictEqual(
      publishedPackages.map((workspacePackage) => workspacePackage.name).toSorted(),
      expectedPublishedPackages
    )
  })

  for (const workspacePackage of publishedPackages) {
    it(`declares the Node engines floor for ${workspacePackage.name}`, () => {
      const engines = workspacePackage.json.engines as { node?: unknown } | undefined
      assert.strictEqual(engines?.node, expectedEnginesNode)
    })
  }
})
