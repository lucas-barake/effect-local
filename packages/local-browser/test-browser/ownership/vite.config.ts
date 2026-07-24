import { readFile } from "node:fs/promises"
import type { ServerResponse } from "node:http"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import type { PreviewServer } from "vite"
import wasm from "vite-plugin-wasm"

const wasmPath = fileURLToPath(
  new URL("../../node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

const runtimeGate = () => {
  if (process.env.EFFECT_LOCAL_RUNTIME_GATE !== "1") return

  type WaitingResponse = {
    readonly next: () => void
    readonly response: ServerResponse
  }

  let mode: "failed" | "held" | "released" = "released"
  let waiting: Array<WaitingResponse> = []

  const release = (failed: boolean) => {
    const current = waiting
    waiting = []
    for (const item of current) {
      if (!failed) {
        item.next()
        continue
      }
      item.response.statusCode = 503
      item.response.end("ownership runtime unavailable")
    }
  }

  return {
    name: "ownership-runtime-preview-gate",
    configurePreviewServer(server: PreviewServer) {
      server.middlewares.use((request, response, next) => {
        const pathname = new URL(request.url ?? "/", "http://localhost").pathname
        if (pathname === "/__effect-local-runtime/hold") {
          mode = "held"
          response.statusCode = 204
          response.end()
          return
        }
        if (pathname === "/__effect-local-runtime/release") {
          mode = "released"
          release(false)
          response.statusCode = 204
          response.end()
          return
        }
        if (pathname === "/__effect-local-runtime/fail") {
          mode = "failed"
          release(true)
          response.statusCode = 204
          response.end()
          return
        }
        if (!pathname.includes("replica.shared-worker-runtime-")) {
          next()
          return
        }
        if (mode === "released") {
          next()
          return
        }
        if (mode === "failed") {
          response.statusCode = 503
          response.end("ownership runtime unavailable")
          return
        }
        waiting.push({ next, response })
      })
    }
  }
}

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  worker: { format: "es", plugins: () => [wasm()] },
  plugins: [wasm(), runtimeGate(), {
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
