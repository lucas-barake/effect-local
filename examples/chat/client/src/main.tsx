import { RegistryProvider, useAtomValue } from "@effect/atom-react"
import { createRoot } from "react-dom/client"
import { App } from "./app.js"
import { Login } from "./login.js"
import { sessionAtom } from "./replica.js"
import "./style.css"

const Root = () => {
  const session = useAtomValue(sessionAtom)
  return session === undefined ? <Login /> : <App session={session} />
}

const container = document.getElementById("root")
if (container === null) {
  globalThis.reportError("chat: #root element missing")
} else {
  createRoot(container).render(
    <RegistryProvider>
      <Root />
    </RegistryProvider>
  )
}
