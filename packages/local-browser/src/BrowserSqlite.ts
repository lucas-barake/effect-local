import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type { LazyArg } from "effect/Function"
import * as EffectLayer from "effect/Layer"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"

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
    if (value) return 1
    return 0
  })
  if (changed) return [message[0], message[1], parameters]
  return message
}

// wa-sqlite 0.1.2 satisfies the driver's peer range but cannot bind booleans.
const compatiblePort = <T extends Pick<MessagePort, "postMessage">,>(port: T): T =>
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
      if (typeof value === "function") return value.bind(target)
      return value
    }
  })

const makeLayer = Effect.fnUntraced(
  function*() {
    const port = yield* DatabasePort
    return SqliteClient.layer({
      // A MessagePort from `new MessageChannel()` only dispatches queued messages once started.
      worker: Effect.sync(() => {
        port.start()
        return compatiblePort(port)
      })
    })
  },
  EffectLayer.unwrap
)

export const layer = makeLayer()

export const layerMessagePort = (port: MessagePort) =>
  makeLayer().pipe(EffectLayer.provide(EffectLayer.succeed(DatabasePort, port)))

const isReady = (data: unknown) => Array.isArray(data) && data[0] === "ready"

const readyMessage = (): MessageEvent => new MessageEvent("message", { data: ["ready", undefined, undefined] })

const spawnReady = (spawn: LazyArg<Worker>) =>
  Effect.callback<Worker, SqlError>((resume) => {
    const worker = spawn()
    const onMessage = (event: MessageEvent<unknown>) => {
      if (!isReady(event.data)) return
      detach()
      const ready = replayingReady(worker)
      resume(Effect.succeed(ready))
    }
    const onError = (event: ErrorEvent) => {
      detach()
      worker.terminate()
      const error = new SqlError({
        reason: classifySqliteError(event.error ?? event.message, {
          message: "The SQLite worker failed before it was ready",
          operation: "worker"
        })
      })
      resume(Effect.fail(error))
    }
    const detach = () => {
      worker.removeEventListener("message", onMessage)
      worker.removeEventListener("error", onError)
    }
    worker.addEventListener("message", onMessage)
    worker.addEventListener("error", onError)
    return Effect.sync(() => {
      detach()
      worker.terminate()
    })
  })

const replayingReady = (worker: Worker): Worker => {
  let replayed = false
  return new Proxy(worker, {
    get(target, property) {
      if (property === "addEventListener") {
        return (
          type: string,
          listener: EventListenerOrEventListenerObject | null,
          options?: AddEventListenerOptions | boolean
        ): void => {
          if (listener === null) return
          target.addEventListener(type, listener, options)
          if (type !== "message" || replayed) return
          replayed = true
          if (typeof listener === "function") listener(readyMessage())
          else listener.handleEvent(readyMessage())
        }
      }
      const value = Reflect.get(target, property, target)
      if (typeof value === "function") return value.bind(target)
      return value
    }
  })
}

// Terminating on release also serves SqliteClient's restart path: a worker
// "error" event re-acquires the worker, so the replacement is a fresh spawn.
export const layerWorker = (spawn: LazyArg<Worker>) =>
  EffectLayer.effectContext(Effect.gen(function*() {
    let first: Worker | undefined = yield* spawnReady(spawn)
    yield* Effect.addFinalizer(() => Effect.sync(() => first?.terminate()))
    const worker = Effect.acquireRelease(
      Effect.suspend(() => {
        const ready = first
        first = undefined
        if (ready !== undefined) return Effect.succeed(ready)
        return spawnReady(spawn).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
      }).pipe(Effect.map(compatiblePort)),
      (spawned) => Effect.sync(() => spawned.terminate())
    )
    return yield* EffectLayer.build(SqliteClient.layer({ worker }))
  }))
