import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem"
import { runMain } from "@effect/platform-node/NodeRuntime"

const patterns = ["**/dist", "**/*.tsbuildinfo", "coverage", "test-results", "playwright-report"]

const program = Effect.gen(function*() {
  const fileSystem = yield* FileSystem.FileSystem
  const matches = yield* Effect.forEach(
    patterns,
    (pattern) => fileSystem.glob(pattern, { exclude: ["node_modules/**"] }),
    { concurrency: "unbounded" }
  )
  const paths = matches.flat()
  yield* Effect.forEach(
    paths,
    (path) => fileSystem.remove(path, { force: true, recursive: true }),
    { concurrency: "unbounded", discard: true }
  )
}).pipe(Effect.provide(fileSystemLayer))

runMain(program)
