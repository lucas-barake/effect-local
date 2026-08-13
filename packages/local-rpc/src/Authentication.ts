import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Headers from "effect/unstable/http/Headers"
import * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"

export class Principal extends Context.Service<Principal, typeof Schema.Json.Type>()(
  "@lucas-barake/effect-local-rpc/Principal"
) {}

export interface Credential {
  readonly generation: number
  readonly bearer: Redacted.Redacted
}

export class CredentialProvider extends Context.Service<CredentialProvider, {
  readonly acquire: Effect.Effect<Credential>
  readonly awaitChange: (rejectedGeneration: number) => Effect.Effect<Credential>
}>()("@lucas-barake/effect-local-rpc/CredentialProvider") {}

export const AuthenticationError = Schema.Union([
  ReplicaError.CredentialRejected,
  ReplicaError.AuthenticatorUnavailable
])
export type AuthenticationError = typeof AuthenticationError.Type

export class Authenticator extends Context.Service<Authenticator, {
  readonly authenticate: (
    credential: Redacted.Redacted
  ) => Effect.Effect<typeof Schema.Json.Type, AuthenticationError>
}>()("@lucas-barake/effect-local-rpc/Authenticator") {}

export class Authentication extends RpcMiddleware.Service<Authentication, {
  provides: Principal
  clientError: ReplicaError.InvalidConfiguration
}>()("@lucas-barake/effect-local-rpc/Authentication", {
  error: AuthenticationError,
  requiredForClient: true
}) {}

const credentialGenerationHeader = "x-effect-local-credential-generation"

const CredentialGenerationHeader = Schema.isInt().pipe((isInt) =>
  Schema.isGreaterThanOrEqualTo(0).pipe((isNonNegative) => Schema.NumberFromString.check(isInt, isNonNegative))
)

export const layerServer: Layer.Layer<Authentication, never, Authenticator> = Effect.gen(function*() {
  const authenticator = yield* Authenticator
  return Authentication.of((effect, options) =>
    Effect.gen(function*() {
      const authorization = options.headers.authorization
      const credentialGeneration = yield* CredentialGenerationHeader.pipe(
        Schema.UndefinedOr,
        Schema.decodeUnknownEffect
      )(options.headers[credentialGenerationHeader]).pipe(Effect.orElseSucceed(() => undefined))
      if (authorization === undefined || !authorization.startsWith("Bearer ")) {
        if (credentialGeneration === undefined) return yield* new ReplicaError.CredentialRejected()
        return yield* new ReplicaError.CredentialRejected({ credentialGeneration })
      }
      const credential = pipe(authorization.slice("Bearer ".length), Redacted.make)
      const principal = yield* authenticator.authenticate(credential).pipe(
        Effect.catchTag(
          "CredentialRejected",
          () => {
            if (credentialGeneration === undefined) return Effect.fail(new ReplicaError.CredentialRejected())
            return Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration }))
          }
        )
      )
      return yield* effect.pipe(Effect.provideService(Principal, principal))
    })
  )
}).pipe(Layer.effect(Authentication))

const layerClientMiddleware = CredentialProvider.pipe(
  Effect.map((provider) =>
    (({ next, request }) =>
      Effect.gen(function*() {
        const credential = yield* provider.acquire
        if (
          !Number.isSafeInteger(credential.generation) || credential.generation < 0
        ) {
          return yield* new ReplicaError.InvalidConfiguration({
            option: "credentialProvider.generation",
            message: "credential generation must be a nonnegative safe integer"
          })
        }
        const headers = Headers.set(
          Headers.set(
            request.headers,
            "authorization",
            `Bearer ${Redacted.value(credential.bearer)}`
          ),
          credentialGenerationHeader,
          String(credential.generation)
        )
        return yield* next({
          ...request,
          headers
        })
      })) satisfies RpcMiddleware.RpcMiddlewareClient<AuthenticationError, ReplicaError.InvalidConfiguration, never>
  ),
  (middleware) => RpcMiddleware.layerClient(Authentication, middleware)
)

export const layerClient = Layer.effect(CredentialProvider, CredentialProvider).pipe(
  Layer.merge(layerClientMiddleware)
)
