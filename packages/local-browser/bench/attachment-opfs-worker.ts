/// <reference lib="webworker" />
/* oxlint-disable effect/noAsyncFunction, effect/noTryCatch -- This worker intentionally benchmarks the native OPFS API without an Effect adapter. */

interface Request {
  readonly id: number
  readonly operation: "write" | "read" | "remove"
  readonly name: string
  readonly bytes?: Uint8Array
}

const directory = navigator.storage.getDirectory().then((root) =>
  root.getDirectoryHandle("effect-local-attachment-bench-files", { create: true })
)

self.addEventListener("message", async (event: MessageEvent<Request>) => {
  const request = event.data
  try {
    const parent = await directory
    if (request.operation === "remove") {
      await parent.removeEntry(request.name)
      self.postMessage({ id: request.id })
      return
    }
    const file = await parent.getFileHandle(request.name, { create: true })
    const handle = await file.createSyncAccessHandle()
    try {
      if (request.operation === "write") {
        const bytes = request.bytes!
        handle.truncate(0)
        const written = handle.write(bytes, { at: 0 })
        handle.truncate(written)
        handle.flush()
        self.postMessage({ id: request.id, written })
        return
      }
      const size = handle.getSize()
      const bytes = new Uint8Array(size)
      const read = handle.read(bytes, { at: 0 })
      self.postMessage({ id: request.id, bytes, read }, [bytes.buffer])
    } finally {
      handle.close()
    }
  } catch (error) {
    self.postMessage({ id: request.id, error: String(error) })
  }
})
