import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Clock from "effect/Clock"
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

const RequestedDocument = Schema.Struct({
  documentType: Schema.NonEmptyString,
  documentId: Identity.DocumentId
})
const RequestedDocuments = Schema.Array(RequestedDocument).check(Schema.isMinLength(1))

export const unsafeUnboundedAutomerge3DecodeRisk = "Automerge3.3.2DecodeIsNotAllocationBounded"

export const UnsafeUnboundedAutomerge3DecodeRequest = Schema.Struct({
  risk: Schema.Literal(unsafeUnboundedAutomerge3DecodeRisk),
  direction: Direction,
  principal: PeerAuthentication.PeerPrincipal,
  remote: RemotePeer,
  documents: RequestedDocuments
})
export type UnsafeUnboundedAutomerge3DecodeRequest = typeof UnsafeUnboundedAutomerge3DecodeRequest.Type

const Invalidation = Schema.declare<Effect.Effect<void>>(
  (value): value is Effect.Effect<void> => Effect.isEffect(value),
  { expected: "Effect<void> invalidation" }
)

export const UnsafeUnboundedAutomerge3DecodeGrant = Schema.TaggedStruct(
  "UnsafeUnboundedAutomerge3DecodeGrant",
  {
    risk: Schema.Literal(unsafeUnboundedAutomerge3DecodeRisk),
    principal: PeerAuthentication.PeerPrincipal,
    remote: PeerAuthentication.PeerPrincipal,
    direction: Direction,
    documents: RequestedDocuments,
    validUntil: Schema.Number.check(Schema.isFinite()),
    invalidated: Invalidation
  }
)
export type UnsafeUnboundedAutomerge3DecodeGrant = typeof UnsafeUnboundedAutomerge3DecodeGrant.Type

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

export type AuthorizeUnsafeUnboundedAutomerge3Decode = (
  request: UnsafeUnboundedAutomerge3DecodeRequest
) => Effect.Effect<
  UnsafeUnboundedAutomerge3DecodeGrant,
  PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable
>

export class PeerRelayAuthorization extends Context.Service<PeerRelayAuthorization, {
  readonly authorize: Authorize
  readonly authorizeUnsafeUnboundedAutomerge3Decode: AuthorizeUnsafeUnboundedAutomerge3Decode
}>()("@lucas-barake/effect-local-rpc/PeerRelayAuthorization") {}

const validateRequestShape = (request: Request) =>
  Effect.all([
    Direction.makeEffect(request.direction),
    PeerAuthentication.PeerPrincipal.makeEffect(request.principal),
    RemotePeer.makeEffect(request.remote)
  ]).pipe(
    Effect.catchTag("SchemaError", () => new PeerRpcError.AccessDenied()),
    Effect.flatMap(() => validateRequest(request.documents)),
    Effect.flatMap((requested) => {
      if (new Set(request.documents.map((document) => document.documentId)).size === request.documents.length) {
        return Effect.succeed(requested)
      }
      return Effect.fail(new PeerRpcError.AccessDenied())
    })
  )

const validateResult = (request: Request, result: Result) =>
  PeerAuthentication.PeerPrincipal.makeEffect(result.remote).pipe(
    Effect.catchTag("SchemaError", () => new PeerRpcError.AccessDenied()),
    Effect.flatMap((remote) => {
      if (
        remote.tenantId === request.principal.tenantId &&
        remote.subjectId === request.remote.subjectId &&
        remote.peerId === request.remote.peerId
      ) return validate(request, result)
      return Effect.fail(new PeerRpcError.AccessDenied())
    }),
    Effect.as(result)
  )

const DocumentKeyJson = Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String]))
const encodeDocumentKey = Schema.encodeSync(DocumentKeyJson)

const documentKey = (
  document: UnsafeUnboundedAutomerge3DecodeRequest["documents"][number]
) =>
  encodeDocumentKey([
    document.documentType,
    document.documentId
  ])

const validateUnsafeDocuments = (
  documents: UnsafeUnboundedAutomerge3DecodeRequest["documents"]
) => {
  const keys = new Set(documents.map(documentKey))
  const documentIds = new Set(documents.map((document) => document.documentId))
  if (keys.size === documents.length && documentIds.size === documents.length) return Effect.succeed(keys)
  return Effect.fail(new PeerRpcError.AccessDenied())
}

const canonicalDocuments = (
  documents: UnsafeUnboundedAutomerge3DecodeRequest["documents"]
) =>
  documents.toSorted((left, right) => {
    const leftKey = documentKey(left)
    const rightKey = documentKey(right)
    if (leftKey < rightKey) return -1
    if (leftKey > rightKey) return 1
    return 0
  })

const validateUnsafeRequest = (request: UnsafeUnboundedAutomerge3DecodeRequest) =>
  UnsafeUnboundedAutomerge3DecodeRequest.makeEffect(request).pipe(
    Effect.catchTag("SchemaError", () => new PeerRpcError.AccessDenied()),
    Effect.flatMap((validated) => validateUnsafeDocuments(validated.documents).pipe(Effect.as(validated)))
  )

const validateUnsafeGrant = (
  request: UnsafeUnboundedAutomerge3DecodeRequest,
  result: UnsafeUnboundedAutomerge3DecodeGrant
) =>
  Schema.decodeUnknownEffect(UnsafeUnboundedAutomerge3DecodeGrant)(result).pipe(
    Effect.catchTag("SchemaError", () => new PeerRpcError.AccessDenied()),
    Effect.flatMap((grant) =>
      Effect.gen(function*() {
        const requested = yield* validateUnsafeDocuments(request.documents)
        const granted = yield* validateUnsafeDocuments(grant.documents)
        const now = yield* Clock.currentTimeMillis
        if (
          grant.principal.tenantId !== request.principal.tenantId ||
          grant.principal.subjectId !== request.principal.subjectId ||
          grant.principal.peerId !== request.principal.peerId ||
          grant.remote.tenantId !== request.principal.tenantId ||
          grant.remote.subjectId !== request.remote.subjectId ||
          grant.remote.peerId !== request.remote.peerId ||
          grant.direction !== request.direction ||
          granted.size !== requested.size ||
          [...granted].some((entry) => !requested.has(entry)) ||
          grant.validUntil <= now
        ) {
          return yield* new PeerRpcError.AccessDenied()
        }
        return {
          ...grant,
          documents: canonicalDocuments(grant.documents)
        }
      })
    )
  )

export const denyUnsafeUnboundedAutomerge3Decode: AuthorizeUnsafeUnboundedAutomerge3Decode = () =>
  Effect.fail(new PeerRpcError.AccessDenied())

export const layer = (
  authorize: Authorize,
  authorizeUnsafeUnboundedAutomerge3Decode: AuthorizeUnsafeUnboundedAutomerge3Decode
) =>
  Layer.succeed(PeerRelayAuthorization)({
    authorize: (request) =>
      validateRequestShape(request).pipe(
        Effect.flatMap(() => authorize(request)),
        Effect.flatMap((result) => validateResult(request, result))
      ),
    authorizeUnsafeUnboundedAutomerge3Decode: (request) =>
      validateUnsafeRequest(request).pipe(
        Effect.flatMap((validated) => authorizeUnsafeUnboundedAutomerge3Decode(validated)),
        Effect.flatMap((grant) => validateUnsafeGrant(request, grant))
      )
  })
