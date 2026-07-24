import { BrowserCrypto, BrowserWorker } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { definition, ListTasks, RenameTask, SetTaskCompleted, TaskDocument, TaskList } from "./domain.ts"

declare global {
  interface Window {
    __effectLocalOwnerError?: string
    __effectLocalOwnerInfo?: {
      readonly ownerId: string
      readonly provider: boolean
      readonly replicaId: string
      readonly writerGeneration: number
    }
  }
}

const workers = new Set<{
  database: Worker | undefined
  provisioning: {
    readonly database: Worker
    readonly nonce: string
  } | undefined
  readonly replica: SharedWorker
}>()
let disposed = false

const WorkerLive = BrowserWorker.layer(() => {
  if (disposed) {
    throw new Error("Cannot start a replica worker after page teardown")
  }
  const replica = new SharedWorker(new URL("./replica.shared-worker.ts", import.meta.url), {
    name: "effect-local-tasks",
    type: "module"
  })
  const rpcChannel = new MessageChannel()
  const connection: {
    database: Worker | undefined
    provisioning: {
      readonly database: Worker
      readonly nonce: string
    } | undefined
    readonly replica: SharedWorker
  } = { database: undefined, provisioning: undefined, replica }
  workers.add(connection)
  let failed = false
  const fail = (error: Error) => {
    if (failed || disposed) return
    failed = true
    window.__effectLocalOwnerError = error.message
    rpcChannel.port2.dispatchEvent(new MessageEvent("message", { data: [0] as const }))
    rpcChannel.port2.dispatchEvent(new ErrorEvent("error", { error, message: error.message }))
    rpcChannel.port2.close()
    replica.port.close()
    connection.provisioning?.database.terminate()
    if (connection.database !== connection.provisioning?.database) connection.database?.terminate()
    connection.database = undefined
    connection.provisioning = undefined
    workers.delete(connection)
  }
  replica.port.addEventListener("message", (event) => {
    if (disposed) return
    const message = event.data as
      | ({ readonly _tag: "Attached" } & NonNullable<Window["__effectLocalOwnerInfo"]>)
      | {
        readonly _tag: "OwnerError"
        readonly message: string
        readonly reason?: {
          readonly _tag: "RuntimeLoadFailure"
          readonly message: string
          readonly name: string
        }
      }
      | { readonly _tag: "Ping"; readonly nonce: string }
      | { readonly _tag: "ProvisionAccepted"; readonly nonce: string }
      | { readonly _tag: "ProvisionRejected"; readonly nonce: string }
      | { readonly _tag: "Provision"; readonly nonce: string }
    if (message._tag === "Ping") {
      replica.port.postMessage({ _tag: "Alive", nonce: message.nonce })
      return
    }
    if (message._tag === "Provision") {
      const database = new Worker(new URL("./opfs.worker.ts", import.meta.url), {
        name: "effect-local-tasks-opfs",
        type: "module"
      })
      const databaseChannel = new MessageChannel()
      database.postMessage({
        databasePort: databaseChannel.port1
      }, [databaseChannel.port1])
      connection.database = database
      connection.provisioning = {
        database,
        nonce: message.nonce
      }
      replica.port.postMessage({
        _tag: "Provision",
        databasePort: databaseChannel.port2,
        nonce: message.nonce
      }, [databaseChannel.port2])
      return
    }
    if (message._tag === "ProvisionAccepted") {
      if (connection.provisioning?.nonce !== message.nonce) return
      connection.provisioning = undefined
      return
    }
    if (message._tag === "ProvisionRejected") {
      if (connection.provisioning?.nonce !== message.nonce) return
      connection.provisioning.database.terminate()
      connection.database = undefined
      connection.provisioning = undefined
      return
    }
    if (message._tag === "OwnerError") {
      fail(new Error(message.message, { cause: message.reason }))
      return
    }
    if (!message.provider && connection.database !== undefined) {
      connection.database.terminate()
      connection.database = undefined
      connection.provisioning = undefined
    }
    window.__effectLocalOwnerInfo = message
  })
  replica.addEventListener("error", (event) => {
    const error = event.error instanceof Error ? event.error : new Error(event.message)
    fail(error)
  })
  replica.port.postMessage({
    _tag: "Attach",
    rpcPort: rpcChannel.port1
  }, [rpcChannel.port1])
  replica.port.start()
  return rpcChannel.port2
})

const ReplicaLive = Layer.merge(
  BrowserReplica.layerWithReactivity(definition).pipe(Layer.provide(Layer.merge(WorkerLive, BrowserCrypto.layer))),
  BrowserCrypto.layer
).pipe(Layer.provideMerge(Reactivity.layer))
const replicaRuntime = ManagedRuntime.make(ReplicaLive)
const runtime = Atom.context({ memoMap: replicaRuntime.memoMap })(
  Layer.effectContext(replicaRuntime.contextEffect)
)

export const tasks = ReplicaAtom.queryFamily(runtime, ListTasks)
export const task = ReplicaAtom.documentFamily(runtime, TaskDocument)

export const createTask = runtime.fn<{ readonly title: string }>()(
  ({ title }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const now = Date.now()
      return yield* replica.create(TaskDocument, {
        commandId: yield* Identity.makeCommandId,
        value: { title, completed: false, createdAt: now, updatedAt: now }
      }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
    }),
  { concurrent: true, reactivityKeys: [TaskList.name] }
)

export const renameTask = runtime.fn<{
  readonly documentId: Identity.DocumentId
  readonly title: string
}>()(
  ({ documentId, title }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      return yield* replica.mutate(RenameTask, {
        commandId: yield* Identity.makeCommandId,
        documentId,
        payload: { title }
      }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
    }),
  { concurrent: true, reactivityKeys: [TaskList.name] }
)

export const setTaskCompleted = runtime.fn<{
  readonly completed: boolean
  readonly documentId: Identity.DocumentId
}>()(
  ({ completed, documentId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      return yield* replica.mutate(SetTaskCompleted, {
        commandId: yield* Identity.makeCommandId,
        documentId,
        payload: { completed }
      }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
    }),
  { concurrent: true, reactivityKeys: [TaskList.name] }
)

export const deleteTask = runtime.fn<{ readonly documentId: Identity.DocumentId }>()(
  ({ documentId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      return yield* replica.delete(TaskDocument, {
        commandId: yield* Identity.makeCommandId,
        documentId
      }).pipe(Effect.flatMap(CommandOutcome.committedOrFail))
    }),
  { concurrent: true, reactivityKeys: [TaskList.name] }
)

export const connectionStatus = ReplicaAtom.status(runtime)

export const exportBackup = runtime.fn<void>()(
  () => Replica.Replica.use((replica) => Stream.mkUint8Array(replica.exportBackup({ maxBytes: 32 * 1024 * 1024 }))),
  { concurrent: false }
)

export const restoreBackup = runtime.fn<Uint8Array>()(
  (bytes) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      yield* replica.restoreBackup({
        source: Stream.make(bytes),
        mode: "replace",
        maxBytes: 32 * 1024 * 1024,
        expectedDefinitionHash: definition.hash,
        installationId: yield* Identity.makeBackupInstallationId
      })
    }),
  { concurrent: false, reactivityKeys: [TaskList.name] }
)

export const dispose = () => {
  disposed = true
  const disposing = replicaRuntime.dispose()
  for (const worker of workers) {
    worker.database?.terminate()
    worker.replica.port.close()
  }
  workers.clear()
  return disposing
}
