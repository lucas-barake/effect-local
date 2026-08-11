import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Headers from "effect/unstable/http/Headers"
import * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"

export class AuthenticationFailure extends Schema.TaggedErrorClass<AuthenticationFailure>(
  "@lucas-barake/effect-local-rpc/AuthenticationFailure"
)("AuthenticationFailure", {}) {}

export class Principal extends Context.Service<Principal, typeof Schema.Json.Type>()(
  "@lucas-barake/effect-local-rpc/Principal"
) {}

export class Credentials extends Context.Service<Credentials, Redacted.Redacted>()(
  "@lucas-barake/effect-local-rpc/Credentials"
) {}

export class Authenticator extends Context.Service<Authenticator, {
  readonly authenticate: (
    credential: Redacted.Redacted
  ) => Effect.Effect<typeof Schema.Json.Type, AuthenticationFailure>
}>()("@lucas-barake/effect-local-rpc/Authenticator") {}

export class Authentication extends RpcMiddleware.Service<Authentication, {
  provides: Principal
}>()("@lucas-barake/effect-local-rpc/Authentication", {
  error: AuthenticationFailure,
  requiredForClient: true
}) {}

export const layerServer: Layer.Layer<Authentication, never, Authenticator> = Layer.effect(
  Authentication,
  Effect.gen(function*() {
    const authenticator = yield* Authenticator
    return Authentication.of((effect, options) =>
      Effect.gen(function*() {
        const authorization = options.headers.authorization
        if (authorization === undefined || !authorization.startsWith("Bearer ")) {
          return yield* new AuthenticationFailure()
        }
        const credential = Redacted.make(authorization.slice("Bearer ".length))
        const principal = yield* authenticator.authenticate(credential)
        return yield* effect.pipe(Effect.provideService(Principal, principal))
      })
    )
  })
)

export const layerClient = RpcMiddleware.layerClient(
  Authentication,
  Credentials.pipe(Effect.map((credential) => ({ next, request }) =>
    next({
      ...request,
      headers: Headers.set(request.headers, "authorization", `Bearer ${Redacted.value(credential)}`)
    })
  ))
)
