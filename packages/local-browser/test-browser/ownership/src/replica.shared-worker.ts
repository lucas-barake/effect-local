declare const self: SharedWorkerGlobalScope

self.addEventListener("connect", (event) => {
  const controlPort = event.ports[0]
  void runtime.then(({ connect }) => connect(controlPort)).catch((cause) => {
    const error = cause instanceof Error
      ? { message: cause.message, name: cause.name }
      : { message: String(cause), name: "UnknownError" }
    self.reportError(cause)
    controlPort.postMessage({
      _tag: "OwnerError",
      message: error.message,
      reason: { _tag: "RuntimeLoadFailure", ...error }
    })
    controlPort.close()
  })
})

const runtime = import("./replica.shared-worker-runtime.ts").then(async (module) => {
  try {
    const { layerMessagePort } = await import("@effect/platform-browser/BrowserWorkerRunner")
    module.initialize(layerMessagePort)
    return module
  } finally {
    Reflect.set(self, "onconnect", null)
  }
})
