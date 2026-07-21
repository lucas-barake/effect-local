import { readFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

const wasmPath = fileURLToPath(
  new URL("../../node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

export default defineConfig({
  plugins: [{
    name: "sqlite-wasm-development-asset",
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        if (!request.url?.split("?")[0]?.endsWith("/wa-sqlite.wasm")) {
          next()
          return
        }
        response.setHeader("Content-Type", "application/wasm")
        response.end(await readFile(wasmPath))
      })
    }
  }]
})
