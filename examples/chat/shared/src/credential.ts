import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as SubscriptionRef from "effect/SubscriptionRef"

export class SessionCredential extends Context.Service<
  SessionCredential,
  SubscriptionRef.SubscriptionRef<Authentication.Credential>
>()("@effect-local/example-chat/SessionCredential") {}

const layerCredentialProvider = Layer.effect(
  Authentication.CredentialProvider,
  SessionCredential.use((credential) => Effect.succeed(Authentication.makeCredentialProvider(credential)))
)

export const layerSessionCredential = (token: string) => {
  const credential = SubscriptionRef.make<Authentication.Credential>({ generation: 0, bearer: Redacted.make(token) })
  return layerCredentialProvider.pipe(Layer.provideMerge(Layer.effect(SessionCredential, credential)))
}

export const renewCredential = (token: string) =>
  SessionCredential.use((credential) =>
    SubscriptionRef.update(credential, (current) => {
      if (Redacted.value(current.bearer) === token) return current
      return { generation: current.generation + 1, bearer: Redacted.make(token) }
    })
  )
