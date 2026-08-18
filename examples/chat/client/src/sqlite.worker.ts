/**
 * Dedicated OPFS SQLite worker. The main thread spawns it with the user id as
 * the worker `name` (each profile owns its own OPFS database and Web Lock),
 * then transfers one end of a MessageChannel for the SqliteClient protocol.
 */
import * as OpfsWorker from "@effect/sql-sqlite-wasm/OpfsWorker"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

globalThis.addEventListener(
  "message",
  (event) => {
    const port = event.ports[0]
    if (port === undefined) {
      globalThis.reportError("sqlite.worker.ts expected a transferred MessagePort")
      return
    }
    let dbName = "effect-local-chat-anonymous"
    if (self.name.length > 0) dbName = `effect-local-chat-${self.name}`
    // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Worker bootstrap is a genuine non-Effect entry point; the runner starts the OPFS worker protocol.
    void Effect.runPromiseExit(OpfsWorker.run({ port, dbName })).then((exit) => {
      if (exit._tag === "Failure") globalThis.reportError(exit.cause)
    })
  },
  { once: true }
)
