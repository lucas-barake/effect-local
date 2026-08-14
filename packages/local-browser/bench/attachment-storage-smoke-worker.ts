/// <reference lib="webworker" />
/* oxlint-disable effect/noAsyncFunction -- The dedicated worker reports the production smoke result to its browser host. */
import { BrowserCrypto } from "@effect/platform-browser"
import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as BrowserAttachmentStorage from "../src/BrowserAttachmentStorage.js"
import * as BrowserAttachmentWorker from "../src/BrowserAttachmentWorker.js"

const program = Effect.gen(function*() {
  const channel = new MessageChannel()
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      channel.port1.close()
      channel.port2.close()
    })
  )
  yield* BrowserAttachmentWorker.serveMessagePort(channel.port1, {
    directory: "effect-local-attachment-storage-smoke",
    maximumBytes: 8 * 1_024,
    maximumPendingRequests: 4
  }).pipe(Effect.forkScoped({ startImmediately: true }))
  const context = yield* Layer.build(
    BrowserAttachmentStorage.layerMessagePort(channel.port2, {
      maximumBytes: 8 * 1_024,
      readChunkBytes: 257,
      maximumPendingRequests: 4,
      cleanupRequestTimeout: "1 second"
    }).pipe(Layer.provide(BrowserCrypto.layer))
  )
  const storage = Context.get(context, AttachmentStorage.AttachmentStorage)
  const input = Array.from({ length: 4_096 }, (_, index) => Uint8Array.of(index % 251))
  const staged = yield* storage.stage(Stream.fromIterable(input))
  yield* storage.verify(staged.key, staged.reference)
  const output = yield* storage.read(staged.key, staged.reference).pipe(Stream.runCollect)
  const bytes = Uint8Array.from(output.flatMap((chunk) => Array.from(chunk)))
  if (bytes.length !== input.length) {
    return yield* Effect.die(new Error(`length mismatch: ${bytes.length} !== ${input.length}`))
  }
  for (let index = 0; index < bytes.length; index++) {
    if (bytes[index] !== index % 251) return yield* Effect.die(new Error(`byte mismatch at ${index}`))
  }
  yield* storage.remove(staged.key)
  return bytes.length
}).pipe(Effect.scoped)

self.addEventListener("message", () => {
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The dedicated worker message is the Effect runtime entrypoint.
  Effect.runPromise(program).then(
    (bytes) => self.postMessage({ _tag: "Complete", bytes }),
    (error) => self.postMessage({ _tag: "Failed", error: String(error) })
  )
})
