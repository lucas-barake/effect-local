import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
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

const CredentialGenerationHeader = Schema.NumberFromString.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0)
)

export const LayerServer: Layer.Layer<Authentication, never, Authenticator> = Layer.effect(
  Authentication,
  Effect.gen(function*() {
    const authenticator = yield* Authenticator
    return Authentication.of(
      Effect.fnUntraced(function*(effect, options) {
        const authorization = options.headers.authorization
        const credentialGeneration = yield* Schema.decodeUnknownEffect(
          Schema.UndefinedOr(CredentialGenerationHeader)
        )(options.headers[credentialGenerationHeader]).pipe(Effect.orElseSucceed(() => undefined))
        if (authorization === undefined || !authorization.startsWith("Bearer ")) {
          if (credentialGeneration === undefined) return yield* new ReplicaError.CredentialRejected()
          return yield* new ReplicaError.CredentialRejected({ credentialGeneration })
        }
        const credential = Redacted.make(authorization.slice("Bearer ".length))
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
  })
)

const LayerClientMiddleware = RpcMiddleware.layerClient(
  Authentication,
  Effect.gen(function*() {
    const provider = yield* CredentialProvider
    return Effect.fnUntraced(function*({ next, request }) {
      const credential = yield* provider.acquire
      if (
        !Number.isSafeInteger(credential.generation) || credential.generation < 0
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "credentialProvider.generation",
          message: "credential generation must be a nonnegative safe integer"
        })
      }
      return yield* next({
        ...request,
        headers: Headers.set(
          Headers.set(
            request.headers,
            "authorization",
            `Bearer ${Redacted.value(credential.bearer)}`
          ),
          credentialGenerationHeader,
          String(credential.generation)
        )
      })
    }) satisfies RpcMiddleware.RpcMiddlewareClient<AuthenticationError, ReplicaError.InvalidConfiguration, never>
  })
)

export const LayerClient = Layer.merge(
  LayerClientMiddleware,
  Layer.effect(CredentialProvider, CredentialProvider)
)
