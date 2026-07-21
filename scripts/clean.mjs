import { glob } from "glob"
import { rm } from "node:fs/promises"

const paths = await glob(["**/dist", "**/*.tsbuildinfo", "coverage", "test-results", "playwright-report"], {
  ignore: ["node_modules/**"]
})

await Promise.all(paths.map((path) => rm(path, { force: true, recursive: true })))
