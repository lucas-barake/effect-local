import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Path from "effect/Path"
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem"
import { layer as pathLayer } from "@effect/platform-node/NodePath"
import { runMain } from "@effect/platform-node/NodeRuntime"

const directories = ["local", "local-sql", "local-browser", "local-rpc", "local-test"]

const program = Effect.gen(function*() {
  const fileSystem = yield* FileSystem.FileSystem
  const path = yield* Path.Path

  yield* Effect.forEach(directories, (directory) =>
    Effect.gen(function*() {
      const source = path.join("packages", directory, "src")
      const files = (yield* fileSystem.readDirectory(source))
        .filter((file) => file.endsWith(".ts") && file !== "index.ts" && !file.startsWith("internal"))
        .toSorted()
      let output = files.map((file) => {
        const name = file.slice(0, -3)
        return `export * as ${name} from "./${name}.js"`
      }).join("\n")
      if (output.length === 0) {
        output = "export {}\n"
      } else {
        output = `${output}\n`
      }
      yield* fileSystem.writeFileString(path.join(source, "index.ts"), output)
    }),
    { concurrency: "unbounded", discard: true }
  )
}).pipe(Effect.provide(Layer.mergeAll(fileSystemLayer, pathLayer)))

runMain(program)
