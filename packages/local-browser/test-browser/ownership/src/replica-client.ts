import { BrowserCrypto } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
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

const OwnerInfo = Schema.Struct({
  replicaId: Schema.String,
  writerGeneration: Identity.WriterGeneration
})

const OwnershipLive = OwnershipCoordinator.layerTab({
  name: "effect-local-tasks",
  sharedWorker: () =>
    new SharedWorker(new URL("./replica.shared-worker.ts", import.meta.url), {
      name: "effect-local-tasks",
      type: "module"
    }),
  databaseWorker: () =>
    new Worker(new URL("./opfs.worker.ts", import.meta.url), {
      name: "effect-local-tasks-opfs",
      type: "module"
    }),
  infoSchema: OwnerInfo,
  onAttached: (attached) => {
    window.__effectLocalOwnerInfo = {
      ownerId: attached.ownerId,
      provider: attached.provider,
      replicaId: attached.info.replicaId,
      writerGeneration: attached.info.writerGeneration
    }
  },
  onOwnerError: (message) => {
    window.__effectLocalOwnerError = message
  }
})

const ReplicaLive = Layer.merge(
  BrowserReplica.layerWithReactivity(definition).pipe(Layer.provide(Layer.merge(OwnershipLive, BrowserCrypto.layer))),
  BrowserCrypto.layer
)
// The shared `Atom.runtime` factory supplies `Reactivity` and builds every Layer under the
// default memo map, so these services are shared with any other atom runtime in the app.
export const runtime = Atom.runtime(ReplicaLive)

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
      })
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
      })
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
      })
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
      })
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
