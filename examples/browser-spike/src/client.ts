import { BrowserWorker } from "@effect/platform-browser"
import { Context, Effect, Layer, ManagedRuntime, Stream } from "effect"
import { RpcClient } from "effect/unstable/rpc"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import { PageApi } from "./schema.ts"

class PageClient extends Context.Service<
  PageClient,
  RpcClient.FromGroup<typeof PageApi, RpcClientError>
>()("browser-spike/PageClient") {}

const engineWorkers = new Set<{
  readonly database: Worker
  readonly shared: SharedWorker
}>()

const PageClientLive = Layer.effect(PageClient)(RpcClient.make(PageApi)).pipe(
  Layer.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 16 })),
  Layer.provide(BrowserWorker.layer(() => {
    const shared = new SharedWorker(
      new URL("./engine.shared-worker.ts", import.meta.url),
      { name: `effect-local-stage0-${crypto.randomUUID()}`, type: "module" }
    )
    const database = new Worker(new URL("./opfs.worker.ts", import.meta.url), {
      name: "stage0-opfs",
      type: "module"
    })
    const databaseChannel = new MessageChannel()
    const rpcChannel = new MessageChannel()
    shared.addEventListener("error", (event) => console.error("Shared engine failed", event.message))
    database.addEventListener("error", (event) => console.error("OPFS worker failed", event.message))
    database.postMessage(databaseChannel.port1, [databaseChannel.port1])
    shared.port.postMessage({
      databasePort: databaseChannel.port2,
      rpcPort: rpcChannel.port1
    }, [databaseChannel.port2, rpcChannel.port1])
    shared.port.start()
    engineWorkers.add({ database, shared })
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
  heartbeat: (count: number, intervalMs: number) =>
    run(Effect.flatMap(PageClient, (pageClient) =>
      Stream.runCollect(pageClient.Heartbeat({ count, intervalMs })).pipe(
        Effect.map((chunk) => Array.from(chunk))
      ))),
  dispose: async () => {
    await runtime.dispose()
    for (const workers of engineWorkers) {
      workers.database.terminate()
      workers.shared.port.close()
    }
    engineWorkers.clear()
  }
}

export type Stage0Client = typeof client
