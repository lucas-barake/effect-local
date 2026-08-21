import { readFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import wasm from "vite-plugin-wasm"

const wasmPath = fileURLToPath(
  new URL("./node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

const serverTarget = process.env.CHAT_SERVER_URL ?? "http://localhost:4100"

export default defineConfig({
  // The workspace packages are consumed as TypeScript source; pre-bundling them would freeze
  // them into dep chunks that survive library edits until a forced re-optimization.
  optimizeDeps: {
    entries: ["src/main.tsx", "src/sqlite.worker.ts"],
    exclude: [
      "@lucas-barake/effect-local",
      "@lucas-barake/effect-local-sql",
      "@lucas-barake/effect-local-browser",
      "@lucas-barake/effect-local-rpc"
    ]
  },
  worker: { format: "es", plugins: () => [wasm()] },
  build: {
    // Keep the bundle out of `dist`: that directory holds the composite
    // project's declaration output, and vite's emptyOutDir would wipe it and
    // desync the incremental build state of the TypeScript project references.
    outDir: "dist-web"
  },
  server: {
    proxy: {
      "/sync": { target: serverTarget, ws: true },
      "/login": { target: serverTarget }
    }
  },
  preview: {
    proxy: {
      "/sync": { target: serverTarget, ws: true },
      "/login": { target: serverTarget }
    }
  },
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
