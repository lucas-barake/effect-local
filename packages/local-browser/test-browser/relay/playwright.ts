import { NodeServices } from "@effect/platform-node"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as ChildProcess from "effect/unstable/process/ChildProcess"
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner"
import process from "node:process"
import { fileURLToPath } from "node:url"
import { makeViteTest } from "../playwright.ts"

const runtime = ManagedRuntime.make(NodeServices.layer)
const readyPrefix = "RelayReady "

const startRelay = () => {
  const scope = Scope.makeUnsafe("parallel")
  const command = ChildProcess.make(
    process.execPath,
    ["--import", "tsx", fileURLToPath(new URL("./relay-server.ts", import.meta.url))],
    {
      cwd: fileURLToPath(new URL("../..", import.meta.url)),
      env: { EFFECT_LOCAL_RELAY_PORT: "0" },
      extendEnv: true,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe"
    }
  )
  const program = Effect.gen(function*() {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const child = yield* Effect.provideService(spawner.spawn(command), Scope.Scope, scope)
    const firstLine = yield* Stream.decodeText(child.all).pipe(
      Stream.splitLines,
      Stream.filter((line) => line.startsWith(readyPrefix)),
      Stream.runHead
    )
    if (Option.isNone(firstLine)) return yield* Effect.die(new Error("Relay exited before readiness"))
    if (!firstLine.value.startsWith(readyPrefix)) {
      return yield* Effect.die(new Error(`Relay emitted unexpected readiness output: ${firstLine.value}`))
    }
    const url = firstLine.value.slice(readyPrefix.length)
    return {
      env: { VITE_RELAY_PORT: new URL(url).port },
      close: () => runtime.runPromise(Scope.close(scope, Exit.void))
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Scope.close(scope, Exit.failCause(cause)).pipe(Effect.andThen(Effect.failCause(cause)))
    )
  )
  return runtime.runPromise(program)
}

export const { expect, test } = makeViteTest({
  configFile: fileURLToPath(new URL("./vite.config.ts", import.meta.url)),
  mode: "development",
  dependency: startRelay
})
