import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import type * as PeerAuthentication from "../PeerAuthentication.js"
import * as PeerRpcError from "../PeerRpcError.js"

const key = (documentType: string, documentId: PeerSession.SelectedDocument["documentId"]) =>
  JSON.stringify([documentType, documentId])

export const validateRequest = (
  documents: ReadonlyArray<{
    readonly documentType: string
    readonly documentId: PeerSession.SelectedDocument["documentId"]
  }>
) =>
  Effect.gen(function*() {
    const requested = new Set(documents.map((entry) => key(entry.documentType, entry.documentId)))
    if (requested.size !== documents.length) return yield* new PeerRpcError.AccessDenied()
    return requested
  })

export const validate = (
  request: {
    readonly principal: PeerAuthentication.PeerPrincipal
    readonly documents: ReadonlyArray<{
      readonly documentType: string
      readonly documentId: PeerSession.SelectedDocument["documentId"]
    }>
  },
  result: {
    readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
    readonly validUntil: number
    readonly invalidated: Effect.Effect<void>
  }
) =>
  Effect.gen(function*() {
    const requested = yield* validateRequest(request.documents)
    const selected = new Set(result.documents.map((entry) => key(entry.document.name, entry.documentId)))
    if (
      selected.size !== result.documents.length ||
      selected.size !== requested.size ||
      [...selected].some((entry) => !requested.has(entry))
    ) {
      return yield* new PeerRpcError.AccessDenied()
    }

    const now = yield* Clock.currentTimeMillis
    if (!Number.isFinite(result.validUntil) || result.validUntil <= now) {
      return yield* new PeerRpcError.AccessDenied()
    }
    return result
  })
