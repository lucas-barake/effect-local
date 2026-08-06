import { RegistryProvider } from "@effect/atom-react"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { users } from "./shared/identities.ts"
import "./style.css"

const params = new URL(window.location.href).searchParams
const selected = params.get("user")

const UserPicker = () => (
  <div className="picker">
    <h1>Effect Local Chat</h1>
    <p>
      Pick who you want to be. A browser profile is one device, so to chat between two users open the other one in a
      separate profile, an incognito window, or another browser.
    </p>
    <div className="picker-grid">
      {users.map((user) => (
        <a key={user.id} className="picker-card" href={`?user=${user.id}`}>
          <span className="avatar" style={{ backgroundColor: user.color }}>
            {user.displayName[0]}
          </span>
          <span>{user.displayName}</span>
        </a>
      ))}
    </div>
  </div>
)

const ActiveUserWarning = ({ activeUser }: { readonly activeUser: string }) => (
  <div className="picker">
    <h1>{activeUser} is still active here</h1>
    <p>
      This browser profile is one device, and its database is still held by{" "}
      {activeUser}'s open tabs. Close them first, or open this user in a separate profile or incognito window to chat
      between two users.
    </p>
    <div className="picker-grid">
      <a className="picker-card" href="?">
        <span>Back to user picker</span>
      </a>
    </div>
  </div>
)

/**
 * Every worker of this origin shares one OPFS access-handle pool, so a second user's engine cannot
 * open its database while another user's is running: it hangs and is never retried. The engine's
 * OPFS worker holds a `chat-<user>-opfs` Web Lock, but only from the moment it boots, which is long
 * after the page decides whether it may run. So a tab takes its own `chat-<user>-tab` lock in
 * shared mode (shared, so this user's other tabs still get in) and holds it for the tab's lifetime.
 */
const profileLockPattern = /^chat-(.+)-(?:tab|opfs)$/

const otherUserHolding = async (user: string) => {
  const locks = await navigator.locks.query()
  for (const lock of locks.held ?? []) {
    const holder = typeof lock.name === "string" ? profileLockPattern.exec(lock.name)?.[1] : undefined
    if (holder !== undefined && holder !== user) return holder
  }
  return undefined
}

/** Resolves once this tab holds the claim; the browser drops it when the tab goes away. */
const holdClaim = (user: string) =>
  new Promise<void>((granted, rejected) => {
    navigator.locks
      .request(`chat-${user}-tab`, { mode: "shared" }, () => {
        granted()
        return new Promise<never>(() => {})
      })
      .catch(rejected)
  })

/**
 * Looking and claiming must be one step. Otherwise two tabs of different users opened together
 * both read an empty registry, both start, and one of them ends up with a replica whose database
 * can never open.
 */
const claimProfile = (user: string) =>
  navigator.locks.request("chat-profile-claim", async () => {
    const other = await otherUserHolding(user)
    if (other !== undefined) return other
    await holdClaim(user)
    return undefined
  })

const root = createRoot(document.querySelector("#root")!)

const start = async () => {
  if (selected === null || !users.some((user) => user.id === selected)) {
    root.render(
      <StrictMode>
        <UserPicker />
      </StrictMode>
    )
    return
  }
  const active = await claimProfile(selected)
  if (active !== undefined) {
    root.render(
      <StrictMode>
        <ActiveUserWarning activeUser={active} />
      </StrictMode>
    )
    return
  }
  // The replica client derives its identity from the URL at module load, so it is only imported
  // once a valid user is selected.
  const { App } = await import("./app.tsx")
  root.render(
    <StrictMode>
      <RegistryProvider>
        <App />
      </RegistryProvider>
    </StrictMode>
  )
}

void start()
