/**
 * Synchronous connect buffering: ports that arrive while the coordinator module is still loading
 * are held and replayed, and a load failure is answered on every buffered port instead of leaving
 * tabs waiting forever.
 */
const pending: Array<MessagePort> = []
let coordinatorImportFailure: { readonly message: string; readonly name: string } | undefined
const bufferConnection = (event: Event) => {
  if (!("ports" in event)) return
  const ports: unknown = event.ports
  if (typeof ports !== "object" || ports === null) return
  const port = Reflect.get(ports, 0)
  if (!(port instanceof MessagePort)) return
  if (coordinatorImportFailure === undefined) {
    pending.push(port)
    return
  }
  try {
    port.postMessage({
      _tag: "OwnerError",
      message: coordinatorImportFailure.message,
      reason: { _tag: "RuntimeLoadFailure", ...coordinatorImportFailure }
    })
  } catch {
  } finally {
    port.close()
  }
}

globalThis.addEventListener("connect", bufferConnection)
void import("@lucas-barake/effect-local-browser/OwnershipCoordinator").then(
  (OwnershipCoordinator) => {
    globalThis.removeEventListener("connect", bufferConnection)
    OwnershipCoordinator.runSharedWorker(
      () => import("./replica.shared-worker-runtime.ts").then((module) => module.options),
      pending.splice(0)
    )
  },
  (cause) => {
    coordinatorImportFailure = cause instanceof Error
      ? { message: cause.message, name: cause.name }
      : { message: String(cause), name: "UnknownError" }
    for (const port of pending.splice(0)) {
      try {
        port.postMessage({
          _tag: "OwnerError",
          message: coordinatorImportFailure.message,
          reason: { _tag: "RuntimeLoadFailure", ...coordinatorImportFailure }
        })
      } catch {
      } finally {
        port.close()
      }
    }
    globalThis.reportError(cause)
  }
)
