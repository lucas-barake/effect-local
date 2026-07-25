import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Rpc from "effect/unstable/rpc/Rpc"
import { make as makeClient } from "effect/unstable/rpc/RpcClient"
import type * as RpcClient_ from "effect/unstable/rpc/RpcClient"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerRpcError from "./PeerRpcError.js"

export const protocolVersion = 2

export const RequestedDocument = Schema.Struct({
  documentType: Schema.NonEmptyString,
  documentId: Identity.DocumentId
})
export type RequestedDocument = typeof RequestedDocument.Type

export const Opened = Schema.TaggedStruct("Opened", {
  protocolVersion: Schema.Literal(protocolVersion),
  sessionId: Identity.SessionId,
  peerId: Identity.PeerId,
  // `lineageAware` is `optionalKey` and never `Schema.Boolean` alone: this struct decodes every
  // `Opened` frame, including one from a peer built before lineage existed, and a required key
  // would make that frame fail to decode instead of reading as "not lineage aware".
  capabilities: Schema.Struct({
    storeAndForward: Schema.Literal(false),
    lineageAware: Schema.optionalKey(Schema.Boolean)
  })
})
export type Opened = typeof Opened.Type

export const Message = Schema.TaggedStruct("Message", {
  payload: Schema.Uint8Array
})
export type Message = typeof Message.Type

export const OpenEvent = Schema.Union([Opened, Message])
export type OpenEvent = typeof OpenEvent.Type

const Credential = Schema.optionalKey(Schema.RedactedFromValue(Schema.String))

export const DefinitionHash = Schema.String.check(Schema.isPattern(/^def_[0-9a-f]{16}$/))

const OpenPayload = Schema.Struct({
  protocolVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expectedPeerId: Identity.PeerId,
  definitionHash: Schema.optionalKey(DefinitionHash),
  documents: Schema.Array(RequestedDocument),
  // What the opening client claims about itself, and the only thing that tells the server whether
  // this peer compares document lineage before it merges. `optionalKey` at both levels and never a
  // required key: a client built before lineage sends neither, and a required key would make its
  // `Open` fail to decode instead of reading as "not lineage aware".
  //
  // `protocolVersion` was deliberately not bumped for lineage, so an older build passes the version
  // check and this advertisement is the only thing that distinguishes it. Absent is the fail closed
  // answer: such a client unions whatever it is given, so a rewritten document sent to it comes
  // straight back carrying the history the rewrite discarded.
  capabilities: Schema.optionalKey(Schema.Struct({
    lineageAware: Schema.optionalKey(Schema.Boolean)
  })),
  credential: Credential
}).check(
  Schema.makeFilter(
    (request) => request.protocolVersion !== protocolVersion || request.definitionHash !== undefined,
    { expected: `definitionHash for protocol version ${protocolVersion}` }
  )
)

export class OpenRpc extends Rpc.make("Open", {
  payload: OpenPayload,
  success: OpenEvent,
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect,
  stream: true
}) {}

export class PushRpc extends Rpc.make("Push", {
  payload: {
    sessionId: Identity.SessionId,
    payload: Schema.Uint8Array,
    credential: Credential
  },
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect
}) {}

export class Rpcs extends RpcGroup.make(OpenRpc, PushRpc).middleware(PeerAuthentication.PeerAuthentication) {}

export interface RpcClient extends RpcClient_.FromGroup<typeof Rpcs, RpcClientError> {}

export const makeRpcClient: Effect.Effect<
  RpcClient,
  never,
  RpcClient_.Protocol | RpcMiddleware.ForClient<PeerAuthentication.PeerAuthentication> | Scope.Scope
> = makeClient(Rpcs)
