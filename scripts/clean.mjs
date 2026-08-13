import { pipe } from "effect/Function"
import { glob } from "glob"
import { rm } from "node:fs/promises"

const paths = await glob(["**/dist", "**/*.tsbuildinfo", "coverage", "test-results", "playwright-report"], {
  ignore: ["node_modules/**"]
})

await pipe(
  paths.map((path) => rm(path, { force: true, recursive: true })),
  (removals) => Promise.all(removals)
)
