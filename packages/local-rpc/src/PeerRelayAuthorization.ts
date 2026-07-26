import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { validate, validateRequest } from "./internal/peerAuthorization.js"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerRpcError from "./PeerRpcError.js"

export const Direction = Schema.Literals(["Send", "Receive"])
export type Direction = typeof Direction.Type

export const RemotePeer = Schema.Struct({
  subjectId: Schema.NonEmptyString,
  peerId: Identity.PeerId
})
export type RemotePeer = typeof RemotePeer.Type

export interface Request {
  readonly direction: Direction
  readonly principal: PeerAuthentication.PeerPrincipal
  readonly remote: RemotePeer
  readonly documents: ReadonlyArray<{
    readonly documentType: string
    readonly documentId: PeerSession.SelectedDocument["documentId"]
  }>
}

export interface Result {
  readonly remote: PeerAuthentication.PeerPrincipal
  readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
  readonly validUntil: number
  readonly invalidated: Effect.Effect<void>
}

export type Authorize = (
  request: Request
) => Effect.Effect<Result, PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable>

export class PeerRelayAuthorization extends Context.Service<PeerRelayAuthorization, {
  readonly authorize: Authorize
}>()("@lucas-barake/effect-local-rpc/PeerRelayAuthorization") {}

const accessDeniedOnSchemaError = <A, E, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchIf(
      (error): error is E & { readonly _tag: "SchemaError" } =>
        typeof error === "object" && error !== null && "_tag" in error && error._tag === "SchemaError",
      () => new PeerRpcError.AccessDenied()
    )
  )

const validateRequestShape = (request: Request) =>
  accessDeniedOnSchemaError(
    Effect.all([
      Direction.makeEffect(request.direction),
      PeerAuthentication.PeerPrincipal.makeEffect(request.principal),
      RemotePeer.makeEffect(request.remote)
    ])
  ).pipe(
    Effect.flatMap(() => validateRequest(request.documents)),
    Effect.flatMap((requested) =>
      new Set(request.documents.map((document) => document.documentId)).size ===
          request.documents.length
        ? Effect.succeed(requested)
        : Effect.fail(new PeerRpcError.AccessDenied())
    )
  )

const validateResult = (request: Request, result: Result) =>
  accessDeniedOnSchemaError(PeerAuthentication.PeerPrincipal.makeEffect(result.remote)).pipe(
    Effect.flatMap((remote) =>
      remote.tenantId === request.principal.tenantId &&
        remote.subjectId === request.remote.subjectId &&
        remote.peerId === request.remote.peerId
        ? validate(request, result)
        : Effect.fail(new PeerRpcError.AccessDenied())
    ),
    Effect.as(result)
  )

export const layer = (authorize: Authorize) =>
  Layer.succeed(PeerRelayAuthorization)({
    authorize: (request) =>
      validateRequestShape(request).pipe(
        Effect.flatMap(() => authorize(request)),
        Effect.flatMap((result) => validateResult(request, result))
      )
  })
