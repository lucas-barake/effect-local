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
  type Gate = {
    mode: "failed" | "held" | "released"
    waiting: Array<WaitingResponse>
  }

  const gates: Record<"coordinator" | "runtime", Gate> = {
    coordinator: { mode: "released", waiting: [] },
    runtime: { mode: "released", waiting: [] }
  }

  const release = (gate: Gate, failed: boolean) => {
    const current = gate.waiting
    gate.waiting = []
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
        const control = /^\/__effect-local-(coordinator|runtime)\/(fail|hold|release|waiting)$/.exec(pathname)
        if (control !== null) {
          const gate = gates[control[1] as "coordinator" | "runtime"]
          const operation = control[2]!
          if (operation === "waiting") {
            response.statusCode = 200
            response.end(String(gate.waiting.length))
            return
          }
          gate.mode = operation === "hold" ? "held" : operation === "fail" ? "failed" : "released"
          if (operation !== "hold") release(gate, operation === "fail")
          response.statusCode = 204
          response.end()
          return
        }
        const gate = pathname.includes("OwnershipCoordinator-")
          ? gates.coordinator
          : pathname.includes("replica.shared-worker-runtime-")
          ? gates.runtime
          : undefined
        if (gate === undefined) {
          next()
          return
        }
        if (gate.mode === "released") {
          next()
          return
        }
        if (gate.mode === "failed") {
          response.statusCode = 503
          response.end("ownership runtime unavailable")
          return
        }
        gate.waiting.push({ next, response })
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
