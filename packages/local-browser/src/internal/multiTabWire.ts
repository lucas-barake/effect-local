import * as Schema from "effect/Schema"

/**
 * Tabs from different deployments of the same app can share one origin. The
 * version rides on the envelope, outside the frame union, so a mismatched peer
 * is detected by a decode that always survives instead of a defect the peer
 * cannot discriminate on.
 */
export const protocolVersion = 1

export const TabId = Schema.String.pipe(
  Schema.brand("@lucas-barake/effect-local-browser/TabId")
)
export type TabId = typeof TabId.Type

export const Epoch = Schema.Int.pipe(
  Schema.brand("@lucas-barake/effect-local-browser/Epoch")
)
export type Epoch = typeof Epoch.Type

export const Elected = Schema.TaggedStruct("Elected", {
  epoch: Epoch,
  leaderId: TabId
})
export type Elected = typeof Elected.Type

export const Ready = Schema.TaggedStruct("Ready", {
  epoch: Epoch,
  leaderId: TabId,
  fingerprint: Schema.String
})
export type Ready = typeof Ready.Type

export const ProbeLeader = Schema.TaggedStruct("ProbeLeader", {
  tabId: TabId
})
export type ProbeLeader = typeof ProbeLeader.Type

export const ClientHello = Schema.TaggedStruct("ClientHello", {
  epoch: Epoch,
  tabId: TabId
})
export type ClientHello = typeof ClientHello.Type

export const ClientHeartbeat = Schema.TaggedStruct("ClientHeartbeat", {
  epoch: Epoch,
  tabId: TabId
})
export type ClientHeartbeat = typeof ClientHeartbeat.Type

export const ClientBye = Schema.TaggedStruct("ClientBye", {
  tabId: TabId
})
export type ClientBye = typeof ClientBye.Type

export const RpcRequest = Schema.TaggedStruct("RpcRequest", {
  epoch: Epoch,
  from: TabId,
  message: Schema.Unknown
})
export type RpcRequest = typeof RpcRequest.Type

export const RpcResponse = Schema.TaggedStruct("RpcResponse", {
  epoch: Epoch,
  to: TabId,
  message: Schema.Unknown
})
export type RpcResponse = typeof RpcResponse.Type

export const Invalidation = Schema.TaggedStruct("Invalidation", {
  epoch: Epoch,
  seq: Schema.Int,
  keys: Schema.Array(Schema.String)
})
export type Invalidation = typeof Invalidation.Type

export const WireFrame = Schema.Union([
  Elected,
  Ready,
  ProbeLeader,
  ClientHello,
  ClientHeartbeat,
  ClientBye,
  RpcRequest,
  RpcResponse,
  Invalidation
])
export type WireFrame = typeof WireFrame.Type

export const Envelope = Schema.Struct({
  v: Schema.Int,
  frame: WireFrame
})
export type Envelope = typeof Envelope.Type

export const VersionProbe = Schema.Struct({
  v: Schema.Int
})
