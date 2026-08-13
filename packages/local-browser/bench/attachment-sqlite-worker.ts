/// <reference lib="webworker" />
import * as OpfsWorker from "@effect/sql-sqlite-wasm/OpfsWorker"
import * as Effect from "effect/Effect"

// oxlint-disable-next-line effect-local/noManualEffectBoundary -- A module worker is the non Effect host that owns this long lived worker program.
Effect.runPromise(OpfsWorker.run({
  port: self,
  dbName: "effect-local-attachment-byte-path.sqlite"
})).catch((error) => {
  self.postMessage(["startup-error", String(error), undefined])
})
