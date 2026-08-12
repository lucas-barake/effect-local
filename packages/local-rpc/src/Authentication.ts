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

export class CredentialLifecycle extends Context.Service<CredentialLifecycle, {
  readonly awaitChange: (rejectedGeneration: number) => Effect.Effect<void>
}>()("@lucas-barake/effect-local-rpc/CredentialLifecycle") {}

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

const requestCredentialGeneration = (headers: Headers.Headers) => {
  const raw = headers[credentialGenerationHeader]
  if (raw === undefined || raw.trim() === "") return undefined
  const generation = Number(raw)
  if (!Number.isSafeInteger(generation) || generation < 0) return undefined
  return generation
}

const credentialRejected = (credentialGeneration: number | undefined) => {
  if (credentialGeneration === undefined) return new ReplicaError.CredentialRejected()
  return new ReplicaError.CredentialRejected({ credentialGeneration })
}

export const layerServer: Layer.Layer<Authentication, never, Authenticator> = Layer.effect(
  Authentication,
  Effect.gen(function*() {
    const authenticator = yield* Authenticator
    return Authentication.of((effect, options) =>
      Effect.gen(function*() {
        const authorization = options.headers.authorization
        const credentialGeneration = requestCredentialGeneration(options.headers)
        if (authorization === undefined || !authorization.startsWith("Bearer ")) {
          return yield* credentialRejected(credentialGeneration)
        }
        const credential = Redacted.make(authorization.slice("Bearer ".length))
        const principal = yield* authenticator.authenticate(credential).pipe(
          Effect.catchTag(
            "CredentialRejected",
            () => Effect.fail(credentialRejected(credentialGeneration))
          )
        )
        return yield* effect.pipe(Effect.provideService(Principal, principal))
      })
    )
  })
)

const layerClientMiddleware = RpcMiddleware.layerClient(
  Authentication,
  CredentialProvider.pipe(Effect.map((provider) =>
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
      })) satisfies RpcMiddleware.RpcMiddlewareClient<AuthenticationError, ReplicaError.InvalidConfiguration, never>
  ))
)

const layerCredentialLifecycle = Layer.effect(
  CredentialLifecycle,
  CredentialProvider.pipe(Effect.map((provider) =>
    CredentialLifecycle.of({
      awaitChange: (rejectedGeneration) => provider.awaitChange(rejectedGeneration).pipe(Effect.asVoid)
    })
  ))
)

export const layerClient = Layer.merge(layerClientMiddleware, layerCredentialLifecycle)
