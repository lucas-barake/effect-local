import { OpfsWorker } from "@effect/sql-sqlite-wasm"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

self.addEventListener("message", (event) => {
  const { databasePort } = event.data as {
    readonly databasePort: MessagePort
  }
  databasePort.start()
  void navigator.locks.request("effect-local-tasks-opfs", async () => {
    await Effect.runPromise(OpfsWorker.run({ port: databasePort, dbName: "effect-local-tasks.sqlite" }))
  })
}, { once: true })
