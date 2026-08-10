import { NodeFileSystem } from "@effect/platform-node"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import wasm from "vite-plugin-wasm"

const wasmPath = fileURLToPath(
  new URL("../../node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  // Crawl both entries at startup. Without this the dev server discovers the worker's dependencies
  // only after the first page has loaded, re-optimizes, and forces a reload that destroys the
  // execution context a test is already talking to.
  optimizeDeps: { entries: ["src/main.tsx", "src/opfs.worker.ts"] },
  worker: { format: "es", plugins: () => [wasm()] },
  plugins: [wasm(), {
    name: "sqlite-wasm-development-asset",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        if (!request.url?.split("?")[0]?.endsWith("/wa-sqlite.wasm")) {
          next()
          return
        }
        void Effect.runPromise(
          Effect.gen(function*() {
            const fileSystem = yield* FileSystem.FileSystem
            return yield* fileSystem.readFile(wasmPath)
          }).pipe(Effect.provide(NodeFileSystem.layer))
        ).then((bytes) => {
          response.setHeader("Content-Type", "application/wasm")
          response.end(bytes)
        }, next)
      })
    }
  }]
})
