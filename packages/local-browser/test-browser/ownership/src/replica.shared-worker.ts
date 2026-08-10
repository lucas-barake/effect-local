import * as Effect from "effect/Effect"

const pending: Array<MessagePort> = []
let coordinatorImportFailure: { readonly message: string; readonly name: string } | undefined

const postOwnerError = (port: MessagePort, failure: { readonly message: string; readonly name: string }) => {
  Effect.runSync(
    Effect.try({
      try: () => {
        port.postMessage({
          _tag: "OwnerError",
          message: failure.message,
          reason: { _tag: "RuntimeLoadFailure", ...failure }
        })
      },
      catch: () => undefined
    }).pipe(Effect.ensuring(Effect.sync(() => port.close())))
  )
}

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
  postOwnerError(port, coordinatorImportFailure)
}

globalThis.addEventListener("connect", bufferConnection)
const loadOwnershipCoordinator = import("@lucas-barake/effect-local-browser/OwnershipCoordinator")
const loadRuntime = () => import("./replica.shared-worker-runtime.ts")

void loadOwnershipCoordinator.then(
  (OwnershipCoordinator) => {
    globalThis.removeEventListener("connect", bufferConnection)
    OwnershipCoordinator.runSharedWorker(
      () => loadRuntime().then((module) => module.options),
      pending.splice(0)
    )
  },
  (cause) => {
    if (cause instanceof Error) {
      coordinatorImportFailure = { message: cause.message, name: cause.name }
    } else {
      coordinatorImportFailure = { message: String(cause), name: "UnknownError" }
    }
    for (const port of pending.splice(0)) {
      postOwnerError(port, coordinatorImportFailure)
    }
    globalThis.reportError(cause)
  }
)
