// Starts the sync server and the Vite dev server together. Any CLI arguments
// (for example --host / --port from the preview runner) are forwarded to Vite.
import { spawn } from "node:child_process"

const server = spawn("pnpm", ["-C", "server", "dev"], { stdio: "inherit" })
const client = spawn("pnpm", ["-C", "client", "dev", ...process.argv.slice(2)], { stdio: "inherit" })

let exiting = false
const shutdown = (code) => {
  if (exiting) return
  exiting = true
  server.kill("SIGTERM")
  client.kill("SIGTERM")
  process.exit(code)
}

server.on("exit", (code) => {
  if (!exiting) {
    console.error(`[chat] sync server exited (code ${code})`)
    shutdown(code ?? 1)
  }
})
client.on("exit", (code) => shutdown(code ?? 0))
process.on("SIGINT", () => shutdown(0))
process.on("SIGTERM", () => shutdown(0))
