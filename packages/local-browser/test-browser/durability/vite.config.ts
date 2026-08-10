import { NodeFileSystem } from "@effect/platform-node"
import { Effect, FileSystem } from "effect"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

const wasmPath = fileURLToPath(
  new URL("../../node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)
const readWasm = (path: string) =>
  Effect.runPromise(
    Effect.gen(function*() {
      const fileSystem = yield* FileSystem.FileSystem
      return yield* fileSystem.readFile(path)
    }).pipe(Effect.provide(NodeFileSystem.layer))
  )

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  plugins: [{
    name: "sqlite-wasm-development-asset",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        if (!request.url?.split("?")[0]?.endsWith("/wa-sqlite.wasm")) {
          next()
          return
        }
        response.setHeader("Content-Type", "application/wasm")
        void readWasm(wasmPath).then((contents) => response.end(contents), next)
      })
    }
  }]
})
