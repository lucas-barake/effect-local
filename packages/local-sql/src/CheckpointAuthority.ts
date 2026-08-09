import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as WriterProvenance from "./internal/writerProvenance.js"

export const manifestPurpose = "@lucas-barake/effect-local-sql/CheckpointManifest/v1" as const
export const transitionPurpose = "@lucas-barake/effect-local-sql/LineageTransition/v1" as const

export const maximumAuthorizationTokenBytes = WriterProvenance.maximumAuthorizationTokenBytes
export const AuthorizationToken = WriterProvenance.AuthorizationToken
export type AuthorizationToken = typeof AuthorizationToken.Type

const CheckpointHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))

export const ManifestBase = Schema.TaggedUnion({
  Bootstrap: {},
  Heads: { baseHeads: Conflict.Heads }
})
export type ManifestBase = typeof ManifestBase.Type

export const ManifestClaims = Schema.Struct({
  purpose: Schema.Literal(manifestPurpose),
  documentId: Identity.DocumentId,
  lineage: Identity.DocumentLineage,
  checkpointHash: CheckpointHash,
  heads: Conflict.Heads,
  base: ManifestBase,
  schemaVersion: WriterProvenance.WriterSchemaVersion,
  writerDefinitionHash: WriterProvenance.WriterDefinitionHash
})
export type ManifestClaims = typeof ManifestClaims.Type

export const TransitionClaims = Schema.Struct({
  purpose: Schema.Literal(transitionPurpose),
  documentId: Identity.DocumentId,
  priorLineage: Identity.DocumentLineage,
  priorCheckpointHash: CheckpointHash,
  priorHeads: Conflict.Heads,
  resultingLineage: Identity.DocumentLineage,
  anchorCheckpointHash: CheckpointHash,
  resultingHeads: Conflict.Heads,
  schemaVersion: WriterProvenance.WriterSchemaVersion,
  writerDefinitionHash: WriterProvenance.WriterDefinitionHash
})
export type TransitionClaims = typeof TransitionClaims.Type

export interface Implementation {
  readonly signManifest: (
    claims: ManifestClaims
  ) => Effect.Effect<Option.Option<AuthorizationToken>, ReplicaError.ReplicaError>
  readonly verifyManifest: (
    claims: ManifestClaims,
    token: AuthorizationToken
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly signTransition: (
    claims: TransitionClaims
  ) => Effect.Effect<Option.Option<AuthorizationToken>, ReplicaError.ReplicaError>
  readonly verifyTransition: (
    claims: TransitionClaims,
    token: AuthorizationToken
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export class CheckpointAuthority extends Context.Service<CheckpointAuthority, Implementation>()(
  "@lucas-barake/effect-local-sql/CheckpointAuthority"
) {}

export const layer = (implementation: Implementation): Layer.Layer<CheckpointAuthority> =>
  Layer.succeed(CheckpointAuthority)({
    signManifest: (claims) =>
      Schema.decodeUnknownEffect(ManifestClaims)(claims).pipe(
        Effect.catchTag("SchemaError", () =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Checkpoint manifest claims are malformed"
            })
          })),
        Effect.flatMap(implementation.signManifest),
        Effect.flatMap((token) =>
          Option.isNone(token)
            ? Effect.succeed(token)
            : Schema.decodeUnknownEffect(AuthorizationToken)(token.value).pipe(
              Effect.asSome,
              Effect.catchTag("SchemaError", () =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId: claims.documentId,
                    reason: "Checkpoint manifest authorization token is malformed"
                  })
                }))
            )
        )
      ),
    verifyManifest: (claims, token) =>
      Effect.all([
        Schema.decodeUnknownEffect(ManifestClaims)(claims),
        Schema.decodeUnknownEffect(AuthorizationToken)(token)
      ]).pipe(
        Effect.catchTag("SchemaError", () =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Checkpoint manifest authorization is malformed"
            })
          })),
        Effect.flatMap(([canonicalClaims, boundedToken]) =>
          implementation.verifyManifest(canonicalClaims, boundedToken)
        )
      ),
    signTransition: (claims) =>
      Schema.decodeUnknownEffect(TransitionClaims)(claims).pipe(
        Effect.catchTag("SchemaError", () =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Lineage transition claims are malformed"
            })
          })),
        Effect.flatMap(implementation.signTransition),
        Effect.flatMap((token) =>
          Option.isNone(token)
            ? Effect.succeed(token)
            : Schema.decodeUnknownEffect(AuthorizationToken)(token.value).pipe(
              Effect.asSome,
              Effect.catchTag("SchemaError", () =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId: claims.documentId,
                    reason: "Lineage transition authorization token is malformed"
                  })
                }))
            )
        )
      ),
    verifyTransition: (claims, token) =>
      Effect.all([
        Schema.decodeUnknownEffect(TransitionClaims)(claims),
        Schema.decodeUnknownEffect(AuthorizationToken)(token)
      ]).pipe(
        Effect.catchTag("SchemaError", () =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CheckpointRejected({
              documentId: claims.documentId,
              reason: "Lineage transition authorization is malformed"
            })
          })),
        Effect.flatMap(([canonicalClaims, boundedToken]) =>
          implementation.verifyTransition(canonicalClaims, boundedToken)
        )
      )
  })

export const rejectAll = CheckpointAuthority.of({
  signManifest: () => Effect.succeed(Option.none()),
  verifyManifest: (claims) =>
    Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.CheckpointRejected({
          documentId: claims.documentId,
          reason: "Checkpoint manifest authorization is not configured"
        })
      })
    ),
  signTransition: () => Effect.succeed(Option.none()),
  verifyTransition: (claims) =>
    Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.CheckpointRejected({
          documentId: claims.documentId,
          reason: "Lineage transition authorization is not configured"
        })
      })
    )
})

export const layerRejectAll = Layer.succeed(CheckpointAuthority, rejectAll)
