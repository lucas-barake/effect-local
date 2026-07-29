const pending: Array<MessageEvent<unknown>> = []
const bufferMessage = (event: MessageEvent<unknown>) => {
  pending.push(event)
}

globalThis.addEventListener("message", bufferMessage)
const workerUrl = new URL(globalThis.location.href)
const coordinator = (
    workerUrl.searchParams.has("effectLocalTestHoldImport") ||
    workerUrl.searchParams.has("effectLocalTestRejectImport")
  )
  ? new Promise<void>((resolve) => {
    const release = (event: MessageEvent<unknown>) => {
      if (event.data !== "effectLocalTestReleaseImport") return
      globalThis.removeEventListener("message", release)
      resolve()
    }
    globalThis.addEventListener("message", release)
  }).then(() => {
    if (workerUrl.searchParams.has("effectLocalTestRejectImport")) {
      throw new Error("effect-local test coordinator import failure")
    }
    return import("@lucas-barake/effect-local-browser/OwnershipCoordinator")
  })
  : import("@lucas-barake/effect-local-browser/OwnershipCoordinator")

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
