import { RegistryProvider, useAtomValue } from "@effect/atom-react"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { createRoot } from "react-dom/client"
import { App } from "./app.js"
import { Login } from "./login.js"
import { sessionAtom } from "./replica.js"
import "./style.css"

const Root = () => {
  const session = useAtomValue(sessionAtom)
  // Nothing renders until the stored session has been read: a login screen
  // that flashes before an existing session resumes would be a lie.
  if (AsyncResult.isInitial(session)) return null
  if (AsyncResult.isSuccess(session) && session.value !== null) return <App session={session.value} />
  return <Login />
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
