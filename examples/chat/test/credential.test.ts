import { layerSessionCredential } from "@effect-local/example-chat-shared/credential"
import { assert, describe, it } from "@effect/vitest"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as Effect from "effect/Effect"
import * as Atom from "effect/reactivity/Atom"
import * as AtomRegistry from "effect/reactivity/AtomRegistry"
import * as Redacted from "effect/Redacted"

describe("layerSessionCredential", () => {
  it.effect(
    "serves each session its own token while both runtimes share one registry",
    Effect.fnUntraced(function*() {
      const registry = AtomRegistry.make()
      const acquire = Authentication.CredentialProvider.use((provider) => provider.acquire)
      const alice = Atom.runtime(layerSessionCredential("token-alice")).atom(acquire)
      const bob = Atom.runtime(layerSessionCredential("token-bob")).atom(acquire)
      const unmountAlice = registry.mount(alice)
      const unmountBob = registry.mount(bob)
      const aliceCredential = yield* AtomRegistry.getResult(registry, alice)
      const bobCredential = yield* AtomRegistry.getResult(registry, bob)
      unmountAlice()
      unmountBob()
      assert.strictEqual(Redacted.value(aliceCredential.bearer), "token-alice")
      assert.strictEqual(Redacted.value(bobCredential.bearer), "token-bob")
    })
  )
})
