/**
 * Dedicated OPFS SQLite worker. The main thread spawns it with the user id as
 * the worker `name` (each profile owns its own OPFS database and Web Lock) and
 * speaks the SqliteClient protocol over the worker's own message port.
 */
import * as OpfsWorker from "@effect/sql-sqlite-wasm/OpfsWorker"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

let dbName = "effect-local-chat-anonymous"
if (self.name.length > 0) dbName = `effect-local-chat-${self.name}`

// oxlint-disable-next-line effect-local/noManualEffectBoundary -- Worker bootstrap is a genuine non-Effect entry point; the runner starts the OPFS worker protocol.
void Effect.runPromiseExit(OpfsWorker.run({ port: self, dbName })).then((exit) => {
  if (exit._tag === "Failure") globalThis.reportError(exit.cause)
})
