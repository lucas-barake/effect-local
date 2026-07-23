import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { validate, validateRequest } from "./internal/peerAuthorization.js"
import type * as PeerAuthentication from "./PeerAuthentication.js"
import type * as PeerRpcError from "./PeerRpcError.js"

export class PeerAuthorization extends Context.Service<PeerAuthorization, {
  readonly authorize: (request: {
    readonly principal: PeerAuthentication.PeerPrincipal
    readonly documents: ReadonlyArray<{
      readonly documentType: string
      readonly documentId: PeerSession.SelectedDocument["documentId"]
    }>
  }) => Effect.Effect<{
    readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
    readonly validUntil: number
    readonly invalidated: Effect.Effect<void>
  }, PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable>
}>()("@lucas-barake/effect-local-rpc/PeerAuthorization") {}

export const layer = (
  authorize: (request: {
    readonly principal: PeerAuthentication.PeerPrincipal
    readonly documents: ReadonlyArray<{
      readonly documentType: string
      readonly documentId: PeerSession.SelectedDocument["documentId"]
    }>
  }) => Effect.Effect<{
    readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
    readonly validUntil: number
    readonly invalidated: Effect.Effect<void>
  }, PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable>
) =>
  Layer.succeed(PeerAuthorization)({
    authorize: (request) =>
      validateRequest(request.documents).pipe(
        Effect.flatMap(() => authorize(request)),
        Effect.flatMap((result) => validate(request, result))
      )
  })
