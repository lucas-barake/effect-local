import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Base64Url from "effect/encoding/Base64Url"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { invalidConfiguration } from "./internal/errors.js"
import * as Hmac from "./internal/hmac.js"

export const PrincipalAssertion = Schema.NonEmptyString.pipe(
  Schema.brand("@lucas-barake/effect-local-rpc/PrincipalAssertion")
)
export type PrincipalAssertion = typeof PrincipalAssertion.Type

export interface IssuerService {
  readonly issue: (
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<PrincipalAssertion, ReplicaError.ReplicaError>
}

export class Issuer extends Context.Service<Issuer, IssuerService>()(
  "@lucas-barake/effect-local-rpc/PrincipalAssertion/Issuer"
) {}

export interface VerifierService {
  readonly verify: (
    assertion: PrincipalAssertion
  ) => Effect.Effect<typeof Schema.Json.Type, ReplicaError.ReplicaError>
}

export class Verifier extends Context.Service<Verifier, VerifierService>()(
  "@lucas-barake/effect-local-rpc/PrincipalAssertion/Verifier"
) {}

export const layerIssuer = (
  issue: IssuerService["issue"]
): Layer.Layer<Issuer> => Layer.succeed(Issuer, Issuer.of({ issue }))

export const layerVerifier = (
  verify: VerifierService["verify"]
): Layer.Layer<Verifier> => Layer.succeed(Verifier, Verifier.of({ verify }))

const JsonAssertion = Schema.fromJsonString(Schema.Json)

/**
 * Carries the principal as its JSON encoding with no signature. Only for
 * deployments where the issuing facade and the verifying entities share one
 * trusted process; a networked cluster must sign assertions instead.
 */
export const layerJson: Layer.Layer<Issuer | Verifier> = Layer.merge(
  layerIssuer((principal) =>
    Schema.encodeUnknownEffect(JsonAssertion)(principal).pipe(
      Effect.map((assertion) => PrincipalAssertion.make(assertion)),
      Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "could not issue principal assertion" }))
    )
  ),
  layerVerifier((assertion) =>
    Schema.decodeUnknownEffect(JsonAssertion)(assertion).pipe(
      Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" }))
    )
  )
)

export interface HmacOptions {
  readonly secret: Redacted.Redacted
  readonly timeToLive?: Duration.Input | undefined
}

const minimumSecretBytes = 32

const HmacClaims = Schema.fromJsonString(Schema.Struct({
  principal: Schema.Json,
  expiresAtMillis: Schema.Number
}))

export const layerHmac = (
  options: HmacOptions
): Layer.Layer<Issuer | Verifier, ReplicaError.InvalidConfiguration, Crypto.Crypto> =>
  Layer.effectContext(Effect.gen(function*() {
    const crypto = yield* Crypto.Crypto
    const encoder = new TextEncoder()
    const key = encoder.encode(Redacted.value(options.secret))
    if (key.length < minimumSecretBytes) {
      return yield* invalidConfiguration("secret", `secret must be at least ${minimumSecretBytes} bytes`)
    }
    const timeToLiveMillis = Duration.toMillis(options.timeToLive ?? Duration.minutes(1))
    const invalid = new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" })
    const sign = (payload: string) =>
      Hmac.sha256(crypto, key, encoder.encode(payload)).pipe(
        Effect.catchTag("PlatformError", () => Effect.fail(new ReplicaError.AuthenticatorUnavailable()))
      )
    const issue = Effect.fnUntraced(function*(principal: typeof Schema.Json.Type) {
      const expiresAtMillis = (yield* Clock.currentTimeMillis) + timeToLiveMillis
      const claims = yield* Schema.encodeEffect(HmacClaims)({ principal, expiresAtMillis }).pipe(
        Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "could not issue principal assertion" }))
      )
      const payload = Base64Url.encode(claims)
      const signature = Base64Url.encode(yield* sign(payload))
      return PrincipalAssertion.make(`${payload}.${signature}`)
    })
    const verify = Effect.fnUntraced(function*(assertion: PrincipalAssertion) {
      const parts = assertion.split(".")
      if (parts.length !== 2) return yield* invalid
      const [payload, encodedSignature] = parts
      const signature = Base64Url.decode(encodedSignature)
      if (Result.isFailure(signature)) return yield* invalid
      if (!Hmac.constantTimeEqual(signature.success, yield* sign(payload))) return yield* invalid
      const claimsJson = Base64Url.decodeString(payload)
      if (Result.isFailure(claimsJson)) return yield* invalid
      const claims = yield* Schema.decodeUnknownEffect(HmacClaims)(claimsJson.success).pipe(
        Effect.mapError(() => invalid)
      )
      if (claims.expiresAtMillis < (yield* Clock.currentTimeMillis)) {
        return yield* new ReplicaError.AuthorizationDenied({ reason: "expired principal assertion" })
      }
      return claims.principal
    })
    return Context.make(Issuer, Issuer.of({ issue })).pipe(Context.add(Verifier, Verifier.of({ verify })))
  }))
