import * as Effect from "effect/Effect"

const pending: Array<MessageEvent<unknown>> = []
const bufferMessage = (event: MessageEvent<unknown>) => {
  pending.push(event)
}

const nativeError = () => {
  // The browser Worker error surface requires a native Error instance for this test failure.
  // oxlint-disable-next-line effect/noNewError
  return new Error("effect-local test coordinator import failure")
}

globalThis.addEventListener("message", bufferMessage)
const workerUrl = new URL(globalThis.location.href)
const loadOwnershipCoordinator = () => import("@lucas-barake/effect-local-browser/OwnershipCoordinator")
const coordinator = Effect.runPromise(Effect.gen(function*() {
  if (
    workerUrl.searchParams.has("effectLocalTestHoldImport") ||
    workerUrl.searchParams.has("effectLocalTestRejectImport")
  ) {
    yield* Effect.callback<void>((resume) => {
      const release = (event: MessageEvent<unknown>) => {
        if (event.data !== "effectLocalTestReleaseImport") return
        resume(Effect.void)
      }
      globalThis.addEventListener("message", release)
      return Effect.sync(() => globalThis.removeEventListener("message", release))
    })
  }
  if (workerUrl.searchParams.has("effectLocalTestRejectImport")) {
    return yield* Effect.fail(nativeError())
  }
  return yield* Effect.tryPromise(() => loadOwnershipCoordinator())
}))

void coordinator.then(
  (OwnershipCoordinator) => {
    globalThis.removeEventListener("message", bufferMessage)
    const initialMessage = pending.shift()
    for (const event of pending.splice(0)) {
      for (const port of event.ports) port.close()
    }
    OwnershipCoordinator.runDatabaseWorker({ name: "effect-local-tasks" }, initialMessage)
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
