import { assert, describe, it } from "@effect/vitest"
import { execFileSync } from "node:child_process"
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const repoRoot = join(import.meta.dirname, "../../..")
const packagesDirectory = join(repoRoot, "packages")

const expectedPublishedPackages = [
  "@lucas-barake/effect-local",
  "@lucas-barake/effect-local-browser",
  "@lucas-barake/effect-local-rpc",
  "@lucas-barake/effect-local-sql",
  "@lucas-barake/effect-local-test"
]

const expectedEnginesNode = "^22.22.2 || ^24.15.0 || >=26.0.0"

interface WorkspacePackage {
  readonly name?: string
  readonly path: string
  readonly private: boolean
}

interface PackageManifest {
  readonly engines?: { readonly node?: unknown }
  readonly name?: unknown
  readonly private?: unknown
}

interface PublishedPackage extends WorkspacePackage {
  readonly name: string
  readonly manifest: PackageManifest
}

const workspacePackages = JSON.parse(
  execFileSync("pnpm", ["list", "--recursive", "--depth", "-1", "--json"], {
    cwd: repoRoot,
    encoding: "utf8"
  })
) as ReadonlyArray<WorkspacePackage>

const packageDirectories = readdirSync(packagesDirectory, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => join(packagesDirectory, entry.name))

const packageDirectoryManifests = packageDirectories.map((directory) => ({
  directory,
  manifest: JSON.parse(readFileSync(join(directory, "package.json"), "utf8")) as PackageManifest
}))

const publishedPackages = workspacePackages.flatMap((workspacePackage): ReadonlyArray<PublishedPackage> => {
  if (workspacePackage.private) return []
  const manifest = JSON.parse(
    readFileSync(join(workspacePackage.path, "package.json"), "utf8")
  ) as PackageManifest
  if (typeof workspacePackage.name !== "string") {
    throw new TypeError(`Published workspace at ${workspacePackage.path} has no package name`)
  }
  assert.strictEqual(manifest.name, workspacePackage.name)
  return [{ ...workspacePackage, name: workspacePackage.name, manifest }]
})

describe("published package engines", () => {
  it("uses the pnpm workspace graph without omitting package directories", () => {
    assert.isTrue(
      workspacePackages.some((workspacePackage) => workspacePackage.path === repoRoot && workspacePackage.private)
    )
    assert.deepStrictEqual(
      workspacePackages.filter((workspacePackage) => workspacePackage.path !== repoRoot)
        .map((workspacePackage) => workspacePackage.path)
        .toSorted(),
      packageDirectoryManifests.map(({ directory }) => directory).toSorted()
    )
    assert.deepStrictEqual(
      publishedPackages.map((workspacePackage) => workspacePackage.name).toSorted(),
      expectedPublishedPackages
    )
  })

  for (const workspacePackage of publishedPackages) {
    it(`declares the Node engines floor for ${workspacePackage.name}`, () => {
      assert.strictEqual(workspacePackage.manifest.engines?.node, expectedEnginesNode)
    })

    it(`packs the Node engines floor for ${workspacePackage.name}`, () => {
      const packDirectory = mkdtempSync(join(tmpdir(), "effect-local-pack-"))
      try {
        const packed = JSON.parse(
          execFileSync(
            "pnpm",
            ["--dir", workspacePackage.path, "pack", "--json", "--pack-destination", packDirectory],
            { cwd: repoRoot, encoding: "utf8", maxBuffer: 10 * 1024 * 1024 }
          )
        ) as { readonly filename: string; readonly name: string }
        const packedManifest = JSON.parse(
          execFileSync("tar", ["-xOf", packed.filename, "package/package.json"], {
            encoding: "utf8"
          })
        ) as PackageManifest
        assert.strictEqual(packed.name, workspacePackage.name)
        assert.strictEqual(packedManifest.name, workspacePackage.name)
        assert.strictEqual(packedManifest.engines?.node, expectedEnginesNode)
      } finally {
        rmSync(packDirectory, { recursive: true, force: true })
      }
    })
  }
})
