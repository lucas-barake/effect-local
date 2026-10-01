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

export const layerSessionCredential = (token: string) =>
  Layer.unwrap(
    SubscriptionRef.make<Authentication.Credential>({ generation: 0, bearer: Redacted.make(token) }).pipe(
      Effect.map((credential) =>
        Layer.merge(Layer.succeed(SessionCredential, credential), Authentication.layerCredentialProvider(credential))
      )
    )
  )

export const renewCredential = (token: string) =>
  SessionCredential.use((credential) =>
    SubscriptionRef.update(credential, (current) => {
      if (Redacted.value(current.bearer) === token) return current
      return { generation: current.generation + 1, bearer: Redacted.make(token) }
    })
  )
