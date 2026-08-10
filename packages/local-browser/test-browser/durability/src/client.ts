import { BrowserWorker } from "@effect/platform-browser"
import { Context, Deferred, Effect, Layer, ManagedRuntime, Stream } from "effect"
import { RpcClient } from "effect/unstable/rpc"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import { PageApi } from "./schema.ts"

class PageClient extends Context.Service<
  PageClient,
  RpcClient.FromGroup<typeof PageApi, RpcClientError>
>()("browser-spike/PageClient") {}

const engineWorkers = new Set<{
  readonly bridge: ReturnType<typeof makeDatabaseBridge>
  readonly database: Worker
  readonly shared: SharedWorker
}>()

const SharedWorker = globalThis.SharedWorker
const Worker = globalThis.Worker

const deferred = () => {
  const cell = Deferred.makeUnsafe<void>()
  return {
    promise: Effect.runPromise(Deferred.await(cell)),
    resolve: () => Effect.runSync(Deferred.succeed(cell, undefined))
  }
}

const makeDatabaseBridge = () => {
  const shared = new MessageChannel()
  const worker = new MessageChannel()
  let gate: {
    readonly request: ReturnType<typeof deferred>
    readonly response: ReturnType<typeof deferred>
    requestObserved: boolean
    held?: MessageEvent
  } | undefined
  const forward = (target: MessagePort, event: MessageEvent) => {
    target.postMessage(event.data, [...event.ports])
  }
  shared.port1.addEventListener("message", (event) => {
    if (gate !== undefined && !gate.requestObserved) {
      gate.requestObserved = true
      gate.request.resolve()
    }
    forward(worker.port2, event)
  })
  worker.port2.addEventListener("message", (event) => {
    if (gate !== undefined && gate.requestObserved && gate.held === undefined) {
      gate.held = event
      gate.response.resolve()
      return
    }
    forward(shared.port1, event)
  })
  shared.port1.start()
  worker.port2.start()
  return {
    sharedPort: shared.port2,
    workerPort: worker.port1,
    arm: () => {
      if (gate !== undefined) Effect.runSync(Effect.die(new Error("a database response gate is already armed")))
      gate = { request: deferred(), response: deferred(), requestObserved: false }
    },
    waitForRequest: () => {
      if (gate === undefined) return Effect.runPromise(Effect.die(new Error("no database response gate is armed")))
      return gate.request.promise
    },
    waitForResponse: () => {
      if (gate === undefined) return Effect.runPromise(Effect.die(new Error("no database response gate is armed")))
      return gate.response.promise
    },
    release: () => {
      const currentGate = gate
      if (currentGate?.held === undefined) {
        Effect.runSync(Effect.die(new Error("no database response is held")))
        return
      }
      forward(shared.port1, currentGate.held)
      gate = undefined
    },
    releaseIfHeld: () => {
      if (gate?.held !== undefined) forward(shared.port1, gate.held)
      gate = undefined
    },
    close: () => {
      if (gate !== undefined) {
        Effect.runSync(Effect.die(new Error("database bridge closed while a response gate was armed")))
      }
      shared.port1.close()
      worker.port2.close()
    }
  }
}

let activeDatabaseBridge: ReturnType<typeof makeDatabaseBridge> | undefined

const PageClientLive = Layer.effect(PageClient)(RpcClient.make(PageApi)).pipe(
  Layer.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 16 })),
  Layer.provide(BrowserWorker.layer((id) => {
    const shared = new SharedWorker(
      new URL("./engine.shared-worker.ts", import.meta.url),
      { name: `effect-local-stage0-${id}-${globalThis.crypto.randomUUID()}`, type: "module" }
    )
    const database = new Worker(new URL("./opfs.worker.ts", import.meta.url), {
      name: "stage0-opfs",
      type: "module"
    })
    const bridge = makeDatabaseBridge()
    const rpcChannel = new MessageChannel()
    shared.addEventListener(
      "error",
      (event) =>
        void Effect.runPromise(
          Effect.logError(
            `Shared engine failed: ${event.message} ${String(event.error)} ${event.filename}:${event.lineno}`
          )
        )
    )
    database.addEventListener(
      "error",
      (event) =>
        void Effect.runPromise(
          Effect.logError(
            `OPFS worker failed: ${event.message} ${String(event.error)} ${event.filename}:${event.lineno}`
          )
        )
    )
    database.postMessage(bridge.workerPort, [bridge.workerPort])
    shared.port.postMessage({
      databasePort: bridge.sharedPort,
      rpcPort: rpcChannel.port1
    }, [bridge.sharedPort, rpcChannel.port1])
    shared.port.start()
    activeDatabaseBridge = bridge
    engineWorkers.add({ bridge, database, shared })
    return rpcChannel.port2
  }))
)

const runtime = ManagedRuntime.make(PageClientLive)

const run = <A, E,>(effect: Effect.Effect<A, E, PageClient>) => runtime.runPromise(effect)

export const client = {
  commit: (input: { readonly commandId: string; readonly documentId: string; readonly value: string }) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.CommitDocument(input))),
  rollback: (input: { readonly commandId: string; readonly documentId: string; readonly value: string }) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.RollbackDocument(input))),
  inspect: (commandId: string) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.InspectCommand({ commandId }))),
  inspectRollback: (input: { readonly commandId: string; readonly documentId: string }) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.InspectRollback(input))),
  cleanupRollback: (input: { readonly commandId: string; readonly documentId: string }) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.CleanupRollback(input))),
  stressDatabase: (iterations: number) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.StressDatabase({ iterations }))),
  startWorkflow: (id: string) => run(Effect.flatMap(PageClient, (pageClient) => pageClient.StartWorkflow({ id }))),
  inspectWorkflow: (id: string, executionId: string) =>
    run(Effect.flatMap(PageClient, (pageClient) => pageClient.InspectWorkflow({ executionId, id }))),
  heartbeat: (count: number) =>
    run(Effect.flatMap(PageClient, (pageClient) =>
      Stream.runCollect(pageClient.Heartbeat({ count })).pipe(
        Effect.map((chunk) => Array.from(chunk))
      ))),
  armNextDatabaseResponse: () => {
    const bridge = activeDatabaseBridge
    if (bridge === undefined) {
      Effect.runSync(Effect.die(new Error("database bridge is not ready")))
      return
    }
    bridge.arm()
  },
  waitForDatabaseRequest: () => {
    const bridge = activeDatabaseBridge
    if (bridge === undefined) return Effect.runPromise(Effect.die(new Error("database bridge is not ready")))
    return bridge.waitForRequest()
  },
  waitForDatabaseResponse: () => {
    const bridge = activeDatabaseBridge
    if (bridge === undefined) return Effect.runPromise(Effect.die(new Error("database bridge is not ready")))
    return bridge.waitForResponse()
  },
  releaseDatabaseResponse: () => {
    const bridge = activeDatabaseBridge
    if (bridge === undefined) {
      Effect.runSync(Effect.die(new Error("database bridge is not ready")))
      return
    }
    bridge.release()
  },
  dispose: () => {
    activeDatabaseBridge?.releaseIfHeld()
    return runtime.dispose().then(() => {
      for (const workers of engineWorkers) {
        workers.bridge.close()
        workers.database.terminate()
        workers.shared.port.close()
      }
      engineWorkers.clear()
      activeDatabaseBridge = undefined
    })
  }
}
