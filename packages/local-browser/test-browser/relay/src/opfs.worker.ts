import { OpfsWorker } from "@effect/sql-sqlite-wasm"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

self.addEventListener("message", (
  event: MessageEvent<{
    readonly databasePort: MessagePort
    readonly dbName: string
  }>
) => {
  const { databasePort, dbName } = event.data
  databasePort.start()
  void navigator.locks.request(
    `effect-local-relay-${dbName}`,
    () => Effect.runPromise(OpfsWorker.run({ port: databasePort, dbName }))
  )
}, { once: true })
