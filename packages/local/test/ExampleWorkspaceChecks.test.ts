import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

const { execFileSync } = globalThis.process.getBuiltinModule("node:child_process")
const { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } = globalThis.process
  .getBuiltinModule("node:fs")
const { join } = globalThis.process.getBuiltinModule("node:path")

const repoRoot = join(import.meta.dirname, "../../..")
const JsonString = Schema.fromJsonString(Schema.Unknown)
const RootManifest = Schema.Struct({
  packageManager: Schema.String,
  scripts: Schema.Struct({ "check:examples": Schema.String })
})
const rootManifest = Schema.decodeUnknownSync(RootManifest)(
  Schema.decodeUnknownSync(JsonString)(readFileSync(join(repoRoot, "package.json"), "utf8"))
)

const withFixture = (run: (directory: string) => void) => {
  const directory = mkdtempSync("effect-local-examples-")
  Effect.runSync(Effect.acquireUseRelease(
    Effect.succeed(directory),
    (currentDirectory) =>
      Effect.sync(() => {
        writeFileSync(
          join(currentDirectory, "package.json"),
          Schema.encodeSync(JsonString)({
            private: true,
            packageManager: rootManifest.packageManager,
            scripts: { "check:examples": rootManifest.scripts["check:examples"] }
          })
        )
        copyFileSync(join(repoRoot, "pnpm-workspace.yaml"), join(currentDirectory, "pnpm-workspace.yaml"))
        run(currentDirectory)
      }),
    (currentDirectory) => Effect.sync(() => rmSync(currentDirectory, { recursive: true, force: true }))
  ))
}

const addExample = (directory: string, name: string) => {
  const exampleDirectory = join(directory, "examples", name)
  mkdirSync(exampleDirectory, { recursive: true })
  writeFileSync(
    join(exampleDirectory, "package.json"),
    Schema.encodeSync(JsonString)({
      name: `@fixture/${name}`,
      private: true,
      scripts: { check: "node check.mjs" }
    })
  )
  writeFileSync(
    join(exampleDirectory, "check.mjs"),
    `import { writeFileSync } from "node:fs"
writeFileSync(new URL("./checked", import.meta.url), "ok")
`
  )
  return exampleDirectory
}

describe("example workspace checks", () => {
  it("succeeds when the examples directory is absent", { timeout: 15_000 }, () =>
    withFixture((directory) => {
      execFileSync("pnpm", ["check:examples"], { cwd: directory, stdio: "pipe" })
    }))

  it("runs the check script for every discovered example", { timeout: 15_000 }, () =>
    withFixture((directory) => {
      const alpha = addExample(directory, "alpha")
      const beta = addExample(directory, "beta")

      execFileSync("pnpm", ["check:examples"], { cwd: directory, stdio: "pipe" })

      assert.strictEqual(readFileSync(join(alpha, "checked"), "utf8"), "ok")
      assert.strictEqual(readFileSync(join(beta, "checked"), "utf8"), "ok")
    }))
})
