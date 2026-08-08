import { readFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import wasm from "vite-plugin-wasm"

const wasmPath = fileURLToPath(
  new URL("./node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  // Stamped once per vite process, this names the SharedWorker generation: every dev-server
  // restart or production build moves tabs onto fresh workers instead of letting them reattach
  // to survivors running an older module graph.
  define: {
    __ENGINE_GENERATION__: JSON.stringify(Date.now().toString(36))
  },
  // Crawl every entry at startup. Without this the dev server discovers worker dependencies only
  // after the first page load, re-optimizes, and forces a reload that tears down the SharedWorker.
  optimizeDeps: {
    entries: [
      "src/main.tsx",
      "src/opfs.worker.ts",
      "src/replica.shared-worker.ts",
      "src/replica.shared-worker-runtime.ts"
    ],
    // The workspace packages are consumed as TypeScript source; pre-bundling them would freeze
    // them into dep chunks that survive library edits until a forced re-optimization.
    exclude: [
      "@lucas-barake/effect-local",
      "@lucas-barake/effect-local-sql",
      "@lucas-barake/effect-local-browser",
      "@lucas-barake/effect-local-rpc"
    ]
  },
  worker: { format: "es", plugins: () => [wasm()] },
  plugins: [wasm(), {
    name: "sqlite-wasm-development-asset",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        if (!request.url?.split("?")[0]?.endsWith("/wa-sqlite.wasm")) {
          next()
          return
        }
        void readFile(wasmPath).then((bytes) => {
          response.setHeader("Content-Type", "application/wasm")
          response.end(bytes)
        }, next)
      })
    }
  }]
})
