/**
 * Each user must open its own OPFS database: `runDatabaseWorker`'s `name` is the only thing that
 * derives the database file and its Web Lock, so the user id travels here as a worker URL query
 * parameter. Two users sharing one name would contend for one lock and the second engine would
 * never start.
 */
const pending: Array<MessageEvent<unknown>> = []
declare const self: DedicatedWorkerGlobalScope

const bufferMessage = (event: MessageEvent<unknown>) => {
  pending.push(event)
}

globalThis.addEventListener("message", bufferMessage)

const user = self.name

void import("@lucas-barake/effect-local-browser/OwnershipCoordinator").then(
  (OwnershipCoordinator) => {
    globalThis.removeEventListener("message", bufferMessage)
    const initialMessage = pending.shift()
    for (const event of pending.splice(0)) {
      for (const port of event.ports) port.close()
    }
    if (user.length === 0) throw new Error("opfs.worker.ts requires the user id as its worker name")
    OwnershipCoordinator.runDatabaseWorker({ name: `chat-${user}` }, initialMessage)
  },
  (cause) => {
    globalThis.removeEventListener("message", bufferMessage)
    for (const event of pending.splice(0)) {
      for (const port of event.ports) port.close()
    }
    globalThis.reportError(cause)
    globalThis.close()
  }
)
