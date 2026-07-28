import { RegistryProvider } from "@effect/atom-react"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "./style.css"

const start = async () => {
  if ("serviceWorker" in navigator) {
    const startedControlled = navigator.serviceWorker.controller !== null
    await navigator.serviceWorker.register("/service-worker.js")
    await navigator.serviceWorker.ready
    if (!startedControlled && sessionStorage.getItem("effect-local-shell-controlled") === null) {
      sessionStorage.setItem("effect-local-shell-controlled", "true")
      window.location.reload()
      return
    }
  }

  const { App } = await import("./app.tsx")
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

void start()
