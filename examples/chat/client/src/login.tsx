import { type UserId, users } from "@effect-local/example-chat-shared/domain"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Cause from "effect/Cause"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { useState } from "react"
import { loginAtom } from "./replica.js"

/**
 * Login screen. Renders instantly from the hard-coded roster - the only
 * network call is the credential exchange itself, and its failure renders
 * inline under the form. No spinners.
 */
export const Login = () => {
  const [userId, setUserId] = useState<UserId>(users[0].id)
  const [password, setPassword] = useState("")
  const login = useAtomSet(loginAtom)
  const result = useAtomValue(loginAtom)
  const error = AsyncResult.isFailure(result) ? Cause.pretty(result.cause) : undefined

  const submit = (event: { preventDefault(): void }) => {
    event.preventDefault()
    login({ username: userId, password })
  }

  return (
    <div className="login-screen">
      <div className="login-card">
        <h1 className="login-title">Effect Chat</h1>
        <p className="login-subtitle">Local-first chat demo — pick a user and sign in.</p>
        <div className="login-users">
          {users.map((user) => (
            <button
              key={user.id}
              type="button"
              className={user.id === userId ? "login-user login-user-active" : "login-user"}
              onClick={() => setUserId(user.id)}
            >
              <span className="avatar" style={{ backgroundColor: user.color }}>{user.name[0]}</span>
              <span>{user.name}</span>
            </button>
          ))}
        </div>
        <form onSubmit={submit} className="login-form">
          <input
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder={`Password for ${userId} (hint: ${userId}123)`}
            className="login-password"
          />
          <button type="submit" className="login-submit">Sign in</button>
        </form>
        {error !== undefined && <p className="login-error">{error}</p>}
      </div>
    </div>
  )
}
