import { assert, describe, it } from "@effect/vitest"
import { execFileSync } from "node:child_process"
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const repoRoot = join(import.meta.dirname, "../../..")
const rootManifest = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8")) as {
  readonly packageManager: string
  readonly scripts: { readonly "check:examples": string }
}

const withFixture = (run: (directory: string) => void) => {
  const directory = mkdtempSync(join(tmpdir(), "effect-local-examples-"))
  try {
    writeFileSync(
      join(directory, "package.json"),
      JSON.stringify({
        private: true,
        packageManager: rootManifest.packageManager,
        scripts: { "check:examples": rootManifest.scripts["check:examples"] }
      })
    )
    copyFileSync(join(repoRoot, "pnpm-workspace.yaml"), join(directory, "pnpm-workspace.yaml"))
    run(directory)
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
}

const addExample = (directory: string, name: string) => {
  const exampleDirectory = join(directory, "examples", name)
  mkdirSync(exampleDirectory, { recursive: true })
  writeFileSync(
    join(exampleDirectory, "package.json"),
    JSON.stringify({
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
