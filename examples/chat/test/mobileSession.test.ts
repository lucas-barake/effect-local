import { users } from "@effect-local/example-chat-shared/domain"
import { sessionKey, StoredSession } from "@effect-local/example-chat-shared/session"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import * as AtomRegistry from "effect/reactivity/AtomRegistry"
import * as Schema from "effect/Schema"
import { logoutAtom, sessionAtom } from "../mobile/src/runtime.js"
import { keychain } from "./fixtures/expoSecureStore.js"

const alice = users[0]
const aliceSession = { token: "chat-token-alice", userId: alice.id, name: alice.name, color: alice.color }

const storeAliceSession = Schema.encodeEffect(Schema.fromJsonString(StoredSession))(aliceSession).pipe(
  Effect.map((encoded) => {
    keychain.entries.set(sessionKey, encoded)
  })
)

const logOut = Effect.fnUntraced(function*() {
  const registry = AtomRegistry.make()
  const unmountSession = registry.mount(sessionAtom)
  yield* AtomRegistry.getResult(registry, sessionAtom)
  const unmountLogout = registry.mount(logoutAtom)
  registry.set(logoutAtom, undefined)
  const logout = yield* Effect.exit(AtomRegistry.getResult(registry, logoutAtom))
  const shown = AsyncResult.value(registry.get(sessionAtom))
  unmountLogout()
  unmountSession()
  const relaunched = yield* AtomRegistry.getResult(AtomRegistry.make(), sessionAtom)
  return { logout, shown, relaunched }
})

describe("mobile session", () => {
  it.effect(
    "stays signed in when logout cannot clear the stored session",
    Effect.fnUntraced(function*() {
      yield* storeAliceSession
      keychain.failWrites = true
      const { logout, shown, relaunched } = yield* logOut()
      keychain.failWrites = false
      assert.isTrue(Exit.isFailure(logout))
      assert.deepStrictEqual(relaunched, aliceSession)
      assert.deepStrictEqual(shown, Option.some(aliceSession))
    })
  )

  it.effect(
    "signs out durably so a relaunch lands on the login screen",
    Effect.fnUntraced(function*() {
      yield* storeAliceSession
      const { logout, shown, relaunched } = yield* logOut()
      assert.isTrue(Exit.isSuccess(logout))
      assert.isNull(relaunched)
      assert.deepStrictEqual(shown, Option.some(null))
    })
  )
})
