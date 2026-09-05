import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

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

const jsonAssertion = Schema.fromJsonString(Schema.Json)

/**
 * Carries the principal as its JSON encoding with no signature. Only for
 * deployments where the issuing facade and the verifying entities share one
 * trusted process; a networked cluster must sign assertions instead.
 */
export const layerJson: Layer.Layer<Issuer | Verifier> = Layer.merge(
  layerIssuer((principal) =>
    Schema.encodeUnknownEffect(jsonAssertion)(principal).pipe(
      Effect.map((assertion) => PrincipalAssertion.make(assertion)),
      Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "could not issue principal assertion" }))
    )
  ),
  layerVerifier((assertion) =>
    Schema.decodeUnknownEffect(jsonAssertion)(assertion).pipe(
      Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" }))
    )
  )
)
