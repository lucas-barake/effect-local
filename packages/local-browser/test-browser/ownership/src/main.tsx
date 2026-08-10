import { RegistryProvider } from "@effect/atom-react"
import { BrowserKeyValueStore, BrowserRuntime } from "@effect/platform-browser"
import * as Effect from "effect/Effect"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"
import { type ComponentType, StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "./style.css"

const loadApp = () => import("./app.tsx")

const render = (App: ComponentType) => {
  const root = createRoot(document.querySelector("#root")!)
  root.render(
    <StrictMode>
      <RegistryProvider>
        <App />
      </RegistryProvider>
    </StrictMode>
  )
  // Worker teardown on pagehide is owned by the library: the ownership tab layer detaches
  // synchronously and the replica client closes its session.
  window.addEventListener("pagehide", () => {
    root.unmount()
  }, { once: true })
}

const start = Effect.gen(function*() {
  if ("serviceWorker" in navigator) {
    const startedControlled = navigator.serviceWorker.controller !== null
    yield* Effect.tryPromise(() => navigator.serviceWorker.register("/service-worker.js"))
    yield* Effect.tryPromise(() => navigator.serviceWorker.ready)
    if (!startedControlled) {
      const keyValueStore = yield* KeyValueStore.KeyValueStore
      const controlled = yield* keyValueStore.get("effect-local-shell-controlled")
      if (controlled === undefined) {
        yield* keyValueStore.set("effect-local-shell-controlled", "true")
        window.location.reload()
        return
      }
    }
  }

  const { App } = yield* Effect.tryPromise(() => loadApp())
  render(App)
}).pipe(Effect.provide(BrowserKeyValueStore.layerSessionStorage))

BrowserRuntime.runMain(start)
