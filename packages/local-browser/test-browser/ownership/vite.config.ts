import { NodeFileSystem } from "@effect/platform-node"
import * as Config from "effect/Config"
import * as ConfigProvider from "effect/ConfigProvider"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Option from "effect/Option"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"
import type { Connect, PreviewServer } from "vite"
import wasm from "vite-plugin-wasm"

const wasmPath = fileURLToPath(
  new URL("../../node_modules/@effect/wa-sqlite/dist/wa-sqlite.wasm", import.meta.url)
)

const readWasm = () =>
  Effect.gen(function*() {
    const fileSystem = yield* FileSystem.FileSystem
    return yield* fileSystem.readFile(wasmPath)
  }).pipe(Effect.provide(NodeFileSystem.layer))

const runtimeGate = () => {
  const gate = Effect.runSync(Config.option(Config.string("EFFECT_LOCAL_RUNTIME_GATE")).parse(ConfigProvider.fromEnv()))
  if (Option.isNone(gate) || gate.value !== "1") return undefined

  type WaitingResponse = {
    readonly next: () => void
    readonly response: Parameters<Connect.NextHandleFunction>[1]
  }
  type Gate = {
    mode: "failed" | "held" | "released"
    waiting: Array<WaitingResponse>
  }

  const gates: Record<"coordinator" | "runtime", Gate> = {
    coordinator: { mode: "released", waiting: [] },
    runtime: { mode: "released", waiting: [] }
  }

  const release = (targetGate: Gate, failed: boolean) => {
    const current = targetGate.waiting
    targetGate.waiting = []
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
          const gateName = control[1]
          const operation = control[2]
          if (gateName === undefined || operation === undefined) {
            next()
            return
          }
          let targetGate = gates.runtime
          if (gateName === "coordinator") targetGate = gates.coordinator
          if (operation === "waiting") {
            response.statusCode = 200
            response.end(String(targetGate.waiting.length))
            return
          }
          if (operation === "hold") targetGate.mode = "held"
          if (operation === "fail") targetGate.mode = "failed"
          if (operation === "release") targetGate.mode = "released"
          if (operation !== "hold") release(targetGate, operation === "fail")
          response.statusCode = 204
          response.end()
          return
        }
        let routeGate: Gate | undefined
        if (pathname.includes("OwnershipCoordinator-")) routeGate = gates.coordinator
        if (pathname.includes("replica.shared-worker-runtime-")) routeGate = gates.runtime
        if (routeGate === undefined) {
          next()
          return
        }
        if (routeGate.mode === "released") {
          next()
          return
        }
        if (routeGate.mode === "failed") {
          response.statusCode = 503
          response.end("ownership runtime unavailable")
          return
        }
        routeGate.waiting.push({ next, response })
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
        void Effect.runPromise(readWasm()).then((bytes) => {
          response.setHeader("Content-Type", "application/wasm")
          response.end(bytes)
        }, next)
      })
    }
  }]
})
