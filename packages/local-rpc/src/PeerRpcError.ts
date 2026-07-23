import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import type * as Rpc from "effect/unstable/rpc/Rpc"

export class AuthenticationFailure extends Schema.TaggedErrorClass<AuthenticationFailure>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/AuthenticationFailure"
)("AuthenticationFailure", {}) {}

export class AccessDenied extends Schema.TaggedErrorClass<AccessDenied>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/AccessDenied"
)("AccessDenied", {}) {}

export class UnsupportedVersion extends Schema.TaggedErrorClass<UnsupportedVersion>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/UnsupportedVersion"
)("UnsupportedVersion", {}) {}

export class PeerMismatch extends Schema.TaggedErrorClass<PeerMismatch>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/PeerMismatch"
)("PeerMismatch", {}) {}

export class DefinitionMismatch extends Schema.TaggedErrorClass<DefinitionMismatch>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/DefinitionMismatch"
)("DefinitionMismatch", {}) {}

export class InvalidRequest extends Schema.TaggedErrorClass<InvalidRequest>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/InvalidRequest"
)("InvalidRequest", {}) {}

export class RequestLimitExceeded extends Schema.TaggedErrorClass<RequestLimitExceeded>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/RequestLimitExceeded"
)("RequestLimitExceeded", {}) {}

export class RequestCapacityExceeded extends Schema.TaggedErrorClass<RequestCapacityExceeded>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/RequestCapacityExceeded"
)("RequestCapacityExceeded", {}) {}

export class SessionUnavailable extends Schema.TaggedErrorClass<SessionUnavailable>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/SessionUnavailable"
)("SessionUnavailable", {}) {}

export class SessionOverloaded extends Schema.TaggedErrorClass<SessionOverloaded>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/SessionOverloaded"
)("SessionOverloaded", {}) {}

export class ServerUnavailable extends Schema.TaggedErrorClass<ServerUnavailable>(
  "@lucas-barake/effect-local-rpc/PeerRpcError/ServerUnavailable"
)("ServerUnavailable", {}) {}

export const PeerRpcError = Schema.Union([
  AuthenticationFailure,
  AccessDenied,
  UnsupportedVersion,
  PeerMismatch,
  DefinitionMismatch,
  InvalidRequest,
  RequestLimitExceeded,
  RequestCapacityExceeded,
  SessionUnavailable,
  SessionOverloaded,
  ServerUnavailable
])
export type PeerRpcError = typeof PeerRpcError.Type

export const Defect: Rpc.DefectSchema = Schema.Unknown.pipe(
  Schema.encodeTo(Schema.Struct({ _tag: Schema.Literal("InternalError") }), {
    decode: SchemaGetter.transform(() => undefined),
    encode: SchemaGetter.transform(() => ({ _tag: "InternalError" as const }))
  })
)
