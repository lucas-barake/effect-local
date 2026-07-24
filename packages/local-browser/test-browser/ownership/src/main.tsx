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

  const [{ App }, { dispose }] = await Promise.all([
    import("./app.tsx"),
    import("./replica-client.ts")
  ])
  const root = createRoot(document.querySelector("#root")!)
  root.render(
    <StrictMode>
      <RegistryProvider>
        <App />
      </RegistryProvider>
    </StrictMode>
  )
  window.addEventListener("pagehide", () => {
    void dispose()
    root.unmount()
  }, { once: true })
}

void start()
