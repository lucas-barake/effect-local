/* oxlint-disable effect/noGlobals -- This browser smoke page owns its native Worker and reports results through page state. */
const status = document.querySelector<HTMLParagraphElement>("#status")!
const results = document.querySelector<HTMLPreElement>("#results")!
const worker = new Worker(new URL("./attachment-storage-smoke-worker.ts", import.meta.url), { type: "module" })

declare global {
  interface Window {
    __attachmentStorageSmoke?: {
      readonly status: "running" | "complete" | "failed"
      readonly bytes?: number
      readonly error?: string
    }
  }
}

worker.addEventListener("message", (
  event: MessageEvent<
    | { readonly _tag: "Complete"; readonly bytes: number }
    | { readonly _tag: "Failed"; readonly error: string }
  >
) => {
  if (event.data._tag === "Complete") {
    window.__attachmentStorageSmoke = { status: "complete", bytes: event.data.bytes }
    status.textContent = "Complete"
  } else {
    window.__attachmentStorageSmoke = { status: "failed", error: event.data.error }
    status.textContent = "Failed"
  }
  results.textContent = JSON.stringify(window.__attachmentStorageSmoke)
  worker.terminate()
})
worker.addEventListener("error", (event) => {
  window.__attachmentStorageSmoke = { status: "failed", error: event.message }
  status.textContent = "Failed"
  results.textContent = JSON.stringify(window.__attachmentStorageSmoke)
  worker.terminate()
})

window.__attachmentStorageSmoke = { status: "running" }
worker.postMessage(undefined)
