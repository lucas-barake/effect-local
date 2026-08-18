import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { makeServerLayer } from "./server.js"

const config = Config.all({
  port: Config.number("CHAT_PORT").pipe(Config.withDefault(4100)),
  databaseFile: Config.string("CHAT_DB").pipe(Config.withDefault("chat.db"))
})

const main = Effect.gen(function*() {
  const { databaseFile, port } = yield* config
  // Layer.launch never returns, so build explicitly, announce readiness once
  // the server is actually bound, then park forever.
  yield* Layer.build(makeServerLayer({ port, databaseFile }))
  yield* Effect.logInfo("chat sync server ready").pipe(Effect.annotateLogs({ port, databaseFile }))
  return yield* Effect.never
})

NodeRuntime.runMain(Effect.scoped(main))
