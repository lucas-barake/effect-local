import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

export class DatabasePort extends Context.Service<DatabasePort, MessagePort>()(
  "@lucas-barake/effect-local-browser/DatabasePort"
) {}

export const layer = DatabasePort.pipe(
  Effect.map((port) => SqliteClient.layer({ worker: Effect.succeed(port) })),
  Layer.unwrap
)

export const layerMessagePort = (port: MessagePort) => layer.pipe(Layer.provide(Layer.succeed(DatabasePort, port)))
