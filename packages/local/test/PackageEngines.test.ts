import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

const { execFileSync } = globalThis.process.getBuiltinModule("node:child_process")
const { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } = globalThis.process.getBuiltinModule("node:fs")
const { tmpdir } = globalThis.process.getBuiltinModule("node:os")
const { join } = globalThis.process.getBuiltinModule("node:path")

const repoRoot = join(import.meta.dirname, "../../..")
const packagesDirectory = join(repoRoot, "packages")
const examplesDirectory = join(repoRoot, "examples")

const expectedPublishedPackages = [
  "@lucas-barake/effect-local",
  "@lucas-barake/effect-local-browser",
  "@lucas-barake/effect-local-rpc",
  "@lucas-barake/effect-local-sql",
  "@lucas-barake/effect-local-test"
]

const expectedEnginesNode = "^22.22.2 || ^24.15.0 || >=26.0.0"

interface PackageManifest {
  readonly engines?: { readonly node?: unknown }
  readonly name?: unknown
  readonly private?: unknown
}

interface PublishedPackage extends WorkspacePackage {
  readonly name: string
  readonly manifest: PackageManifest
}

const JsonString = Schema.fromJsonString(Schema.Unknown)
const WorkspacePackage = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  path: Schema.String,
  private: Schema.Boolean
})
const PackageManifest = Schema.Struct({
  engines: Schema.optionalKey(Schema.Struct({ node: Schema.optionalKey(Schema.Unknown) })),
  name: Schema.optionalKey(Schema.Unknown),
  private: Schema.optionalKey(Schema.Unknown)
})
const Packed = Schema.Struct({ filename: Schema.String, name: Schema.String })
const workspacePackages = Schema.decodeUnknownSync(Schema.Array(WorkspacePackage))(
  Schema.decodeUnknownSync(JsonString)(
    execFileSync("pnpm", ["list", "--recursive", "--depth", "-1", "--json"], {
      cwd: repoRoot,
      encoding: "utf8"
    })
  )
)

const packageDirectories = [packagesDirectory, examplesDirectory]
  .filter(existsSync)
  .flatMap((parent) =>
    readdirSync(parent, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => join(parent, entry.name))
  )

const packageDirectoryManifests = packageDirectories.map((directory) => ({
  directory,
  manifest: Schema.decodeUnknownSync(PackageManifest)(
    Schema.decodeUnknownSync(JsonString)(readFileSync(join(directory, "package.json"), "utf8"))
  )
}))

const publishedPackages = workspacePackages.flatMap((workspacePackage): ReadonlyArray<PublishedPackage> => {
  if (workspacePackage.private) return []
  const manifest = Schema.decodeUnknownSync(PackageManifest)(
    Schema.decodeUnknownSync(JsonString)(readFileSync(join(workspacePackage.path, "package.json"), "utf8"))
  )
  if (typeof workspacePackage.name !== "string") {
    return Effect.runSync(
      Effect.die(new TypeError(`Published workspace at ${workspacePackage.path} has no package name`))
    )
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
        .toSorted((left, right) => left.localeCompare(right)),
      packageDirectoryManifests.map(({ directory }) => directory).toSorted((left, right) => left.localeCompare(right))
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
      Effect.runSync(Effect.acquireUseRelease(
        Effect.succeed(packDirectory),
        (currentPackDirectory) =>
          Effect.sync(() => {
            const packed = Schema.decodeUnknownSync(Packed)(
              Schema.decodeUnknownSync(JsonString)(
                execFileSync(
                  "pnpm",
                  ["--dir", workspacePackage.path, "pack", "--json", "--pack-destination", currentPackDirectory],
                  { cwd: repoRoot, encoding: "utf8", maxBuffer: 10 * 1024 * 1024 }
                )
              )
            )
            const packedManifest = Schema.decodeUnknownSync(PackageManifest)(
              Schema.decodeUnknownSync(JsonString)(
                execFileSync("tar", ["-xOf", packed.filename, "package/package.json"], {
                  encoding: "utf8"
                })
              )
            )
            assert.strictEqual(packed.name, workspacePackage.name)
            assert.strictEqual(packedManifest.name, workspacePackage.name)
            assert.strictEqual(packedManifest.engines?.node, expectedEnginesNode)
          }),
        (currentPackDirectory) => Effect.sync(() => rmSync(currentPackDirectory, { recursive: true, force: true }))
      ))
    })
  }
})
