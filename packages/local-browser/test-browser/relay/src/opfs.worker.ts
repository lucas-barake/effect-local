import { OpfsWorker } from "@effect/sql-sqlite-wasm"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

self.addEventListener("message", (event) => {
  const { databasePort, dbName } = event.data as {
    readonly databasePort: MessagePort
    readonly dbName: string
  }
  databasePort.start()
  void navigator.locks.request(`effect-local-relay-${dbName}`, async () => {
    await Effect.runPromise(OpfsWorker.run({ port: databasePort, dbName }))
  })
}, { once: true })
