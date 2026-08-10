/// <reference lib="webworker" />
import { OpfsWorker } from "@effect/sql-sqlite-wasm"
import { Effect, Schema } from "effect"

declare const self: DedicatedWorkerGlobalScope

const diagnostics = new BroadcastChannel("effect-local-stage0-diagnostics")
diagnostics.postMessage("OPFS worker loaded")

self.addEventListener("message", (event) => {
  Effect.runFork(
    Effect.gen(function*() {
      const port = yield* Schema.decodeUnknownEffect(Schema.instanceOf(MessagePort))(event.data)
      port.start()
      yield* OpfsWorker.run({
        port,
        dbName: "effect-local-stage0.sqlite"
      })
    }).pipe(
      Effect.tapCause((cause) => Effect.sync(() => diagnostics.postMessage(`OPFS failure: ${String(cause)}`))),
      Effect.ensuring(Effect.sync(() => diagnostics.close()))
    )
  )
}, { once: true })
