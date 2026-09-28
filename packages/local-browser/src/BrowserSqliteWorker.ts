import * as OpfsWorker from "@effect/sql-sqlite-wasm/OpfsWorker"
import * as Effect from "effect/Effect"
import * as platform from "./internal/platform.js"

const databaseLockName = (dbName: string): string => `@lucas-barake/effect-local-browser:sqlite:${dbName}`

export const run = (options: OpfsWorker.OpfsWorkerConfig) =>
  platform.WebLocks.use((locks) => locks.acquire(databaseLockName(options.dbName))).pipe(
    Effect.andThen(OpfsWorker.run(options)),
    Effect.scoped,
    Effect.provide(platform.layerWebLocksNavigator)
  )
