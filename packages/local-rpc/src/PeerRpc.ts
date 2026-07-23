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

export const protocolVersion = 1

export const RequestedDocument = Schema.Struct({
  documentType: Schema.NonEmptyString,
  documentId: Identity.DocumentId
})
export type RequestedDocument = typeof RequestedDocument.Type

export const Opened = Schema.TaggedStruct("Opened", {
  protocolVersion: Schema.Literal(protocolVersion),
  sessionId: Identity.SessionId,
  peerId: Identity.PeerId,
  capabilities: Schema.Struct({ storeAndForward: Schema.Literal(false) })
})
export type Opened = typeof Opened.Type

export const Message = Schema.TaggedStruct("Message", {
  payload: Schema.Uint8Array
})
export type Message = typeof Message.Type

export const OpenEvent = Schema.Union([Opened, Message])
export type OpenEvent = typeof OpenEvent.Type

const Credential = Schema.optionalKey(Schema.RedactedFromValue(Schema.String))

export class OpenRpc extends Rpc.make("Open", {
  payload: {
    protocolVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    expectedPeerId: Identity.PeerId,
    documents: Schema.Array(RequestedDocument),
    credential: Credential
  },
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
