import { spawn } from "node:child_process"
import { once } from "node:events"
import { fileURLToPath } from "node:url"
import { makeViteTest } from "../playwright.ts"

const startRelay = async () => {
  const child = spawn(
    process.execPath,
    ["--import", "tsx", fileURLToPath(new URL("./relay-server.ts", import.meta.url))],
    {
      cwd: fileURLToPath(new URL("../..", import.meta.url)),
      env: { ...process.env, EFFECT_LOCAL_RELAY_PORT: "0" },
      stdio: ["ignore", "pipe", "pipe", "ipc"]
    }
  )
  let output = ""
  child.stdout?.on("data", (chunk) => {
    output += String(chunk)
  })
  child.stderr?.on("data", (chunk) => {
    output += String(chunk)
  })
  const killOnParentExit = () => child.kill("SIGTERM")
  process.once("exit", killOnParentExit)
  let url: string
  try {
    url = await new Promise<string>((resolve, reject) => {
      const cleanup = () => {
        child.off("error", onError)
        child.off("exit", onExit)
        child.off("message", onMessage)
      }
      const onError = (error: Error) => {
        cleanup()
        reject(error)
      }
      const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
        cleanup()
        reject(new Error(`Relay exited before readiness (${code ?? signal ?? "unknown"})\n${output}`))
      }
      const onMessage = (message: unknown) => {
        if (
          typeof message !== "object" || message === null ||
          Reflect.get(message, "_tag") !== "RelayReady" ||
          typeof Reflect.get(message, "url") !== "string"
        ) return
        cleanup()
        resolve(Reflect.get(message, "url"))
      }
      child.on("error", onError)
      child.on("exit", onExit)
      child.on("message", onMessage)
    })
  } catch (error) {
    process.off("exit", killOnParentExit)
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM")
      await once(child, "exit")
    }
    throw error
  }
  return {
    env: { VITE_RELAY_PORT: new URL(url).port },
    close: async () => {
      process.off("exit", killOnParentExit)
      if (child.exitCode !== null || child.signalCode !== null) return
      child.kill("SIGTERM")
      await once(child, "exit")
    }
  }
}

export const { expect, test } = makeViteTest({
  configFile: fileURLToPath(new URL("./vite.config.ts", import.meta.url)),
  mode: "development",
  dependency: startRelay
})
