import { BrowserWorker } from "@effect/platform-browser"
import { Context, Data, Deferred, Effect, Layer, ManagedRuntime, Stream } from "effect"
import { RpcClient } from "effect/unstable/rpc"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import { PageApi } from "./schema.ts"

class PageClient extends Context.Service<
  PageClient,
  RpcClient.FromGroup<typeof PageApi, RpcClientError>
>()("browser-spike/PageClient") {}

class DatabaseBridgeError extends Data.TaggedError("DatabaseBridgeError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

const engineWorkers = new Set<{
  readonly bridge: ReturnType<typeof makeDatabaseBridge>
  readonly database: Worker
  readonly shared: SharedWorker
}>()

const deferred = () => {
  const cell = Deferred.makeUnsafe<void>()
  return {
    await: Deferred.await(cell),
    resolve: () => {
      Effect.runFork(Deferred.succeed(cell, undefined))
    }
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
  const forward = (target: MessagePort, event: MessageEvent) =>
    Effect.try({
      try: () => target.postMessage(event.data, [...event.ports]),
      catch: (cause) => new DatabaseBridgeError({ cause, message: "Failed to forward database message" })
    })
  const reportForwardError = (error: DatabaseBridgeError) => Effect.logError(`${error.message}: ${String(error.cause)}`)
  shared.port1.addEventListener("message", (event) => {
    if (gate !== undefined && !gate.requestObserved) {
      gate.requestObserved = true
      gate.request.resolve()
    }
    Effect.runFork(forward(worker.port2, event).pipe(Effect.tapError(reportForwardError)))
  })
  worker.port2.addEventListener("message", (event) => {
    if (gate !== undefined && gate.requestObserved && gate.held === undefined) {
      gate.held = event
      gate.response.resolve()
      return
    }
    Effect.runFork(forward(shared.port1, event).pipe(Effect.tapError(reportForwardError)))
  })
  shared.port1.start()
  worker.port2.start()
  return {
    sharedPort: shared.port2,
    workerPort: worker.port1,
    arm: () => {
      if (gate !== undefined) {
        return Effect.fail(new DatabaseBridgeError({ message: "A database response gate is already armed" }))
      }
      gate = { request: deferred(), response: deferred(), requestObserved: false }
      return Effect.void
    },
    waitForRequest: () => {
      if (gate === undefined) {
        return Effect.fail(new DatabaseBridgeError({ message: "No database response gate is armed" }))
      }
      return gate.request.await
    },
    waitForResponse: () => {
      if (gate === undefined) {
        return Effect.fail(new DatabaseBridgeError({ message: "No database response gate is armed" }))
      }
      return gate.response.await
    },
    release: () => {
      const currentGate = gate
      if (currentGate?.held === undefined) {
        return Effect.fail(new DatabaseBridgeError({ message: "No database response is held" }))
      }
      return forward(shared.port1, currentGate.held).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            gate = undefined
          })
        )
      )
    },
    releaseIfHeld: () => {
      const currentGate = gate
      gate = undefined
      if (currentGate?.held === undefined) return Effect.void
      return forward(shared.port1, currentGate.held).pipe(Effect.tapError(reportForwardError))
    },
    close: () => {
      if (gate !== undefined) {
        return Effect.fail(
          new DatabaseBridgeError({ message: "Database bridge closed while a response gate was armed" })
        )
      }
      return Effect.try({
        try: () => {
          shared.port1.close()
          worker.port2.close()
        },
        catch: (cause) => new DatabaseBridgeError({ cause, message: "Failed to close database bridge" })
      })
    }
  }
}

let activeDatabaseBridge: ReturnType<typeof makeDatabaseBridge> | undefined

const PageClientLive = Layer.effect(PageClient)(RpcClient.make(PageApi)).pipe(
  Layer.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 16 })),
  Layer.provide(BrowserWorker.layer((id) => {
    // BrowserWorker accepts an already-created endpoint and has no constructor abstraction.
    // oxlint-disable-next-line effect/noGlobals -- the browser constructor is the platform boundary required by BrowserWorker.layer.
    const shared = new SharedWorker(
      new URL("./engine.shared-worker.ts", import.meta.url),
      { name: `effect-local-stage0-${id}`, type: "module" }
    )
    // oxlint-disable-next-line effect/noGlobals -- the browser constructor is the platform boundary required by BrowserWorker.layer.
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

const runDatabase = <A,>(effect: Effect.Effect<A, DatabaseBridgeError>) => Effect.runPromise(effect)

const withDatabaseBridge = <A,>(
  operation: (bridge: ReturnType<typeof makeDatabaseBridge>) => Effect.Effect<A, DatabaseBridgeError>
) =>
  Effect.suspend(() => {
    const bridge = activeDatabaseBridge
    if (bridge === undefined) {
      return Effect.fail(new DatabaseBridgeError({ message: "Database bridge is not ready" }))
    }
    return operation(bridge)
  })

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
  armNextDatabaseResponse: () => runDatabase(withDatabaseBridge((bridge) => bridge.arm())),
  waitForDatabaseRequest: () => runDatabase(withDatabaseBridge((bridge) => bridge.waitForRequest())),
  waitForDatabaseResponse: () => runDatabase(withDatabaseBridge((bridge) => bridge.waitForResponse())),
  releaseDatabaseResponse: () => runDatabase(withDatabaseBridge((bridge) => bridge.release())),
  dispose: () =>
    Effect.runPromise(
      Effect.ensuring(
        runtime.disposeEffect,
        Effect.gen(function*() {
          for (const workers of engineWorkers) {
            yield* workers.bridge.releaseIfHeld().pipe(Effect.catchCause((cause) => Effect.logError(String(cause))))
            yield* workers.bridge.close().pipe(Effect.catchCause((cause) => Effect.logError(String(cause))))
            yield* Effect.sync(() => {
              workers.database.terminate()
              workers.shared.port.close()
            })
          }
          engineWorkers.clear()
          activeDatabaseBridge = undefined
        })
      )
    )
}
