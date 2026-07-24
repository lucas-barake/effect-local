import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

export class DatabasePort extends Context.Service<DatabasePort, MessagePort>()(
  "@lucas-barake/effect-local-browser/DatabasePort"
) {}

const normalizeQueryParameters = (message: unknown): unknown => {
  if (
    !Array.isArray(message) ||
    typeof message[0] !== "number" ||
    typeof message[1] !== "string" ||
    !Array.isArray(message[2])
  ) {
    return message
  }
  let changed = false
  const parameters = message[2].map((value) => {
    if (typeof value !== "boolean") return value
    changed = true
    return value ? 1 : 0
  })
  return changed ? [message[0], message[1], parameters] : message
}

// wa-sqlite 0.1.2 satisfies the driver's peer range but cannot bind booleans.
const compatiblePort = (port: MessagePort): MessagePort =>
  new Proxy(port, {
    get(target, property) {
      if (property === "postMessage") {
        return (
          message: unknown,
          transferOrOptions?: StructuredSerializeOptions | Array<Transferable>
        ): void => {
          const normalized = normalizeQueryParameters(message)
          if (transferOrOptions === undefined) {
            target.postMessage(normalized)
          } else if (Array.isArray(transferOrOptions)) {
            target.postMessage(normalized, transferOrOptions)
          } else {
            target.postMessage(normalized, transferOrOptions)
          }
        }
      }
      const value = Reflect.get(target, property, target)
      return typeof value === "function" ? value.bind(target) : value
    }
  })

export const layer = DatabasePort.pipe(
  Effect.map((port) => SqliteClient.layer({ worker: Effect.succeed(compatiblePort(port)) })),
  Layer.unwrap
)

export const layerMessagePort = (port: MessagePort) => layer.pipe(Layer.provide(Layer.succeed(DatabasePort, port)))
