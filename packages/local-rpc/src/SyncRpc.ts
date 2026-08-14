import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import type * as Scope from "effect/Scope"
import * as Rpc from "effect/unstable/rpc/Rpc"
import {
  type FromGroup as RpcClientFromGroup,
  make as makeClient,
  type Protocol as RpcClientProtocol
} from "effect/unstable/rpc/RpcClient"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as Authentication from "./Authentication.js"
import { invalidConfiguration } from "./internal/errors.js"

export const maximumFrameBytes = Protocol.maximumRpcFrameBytes

const RemoteDefect = Schema.Struct({
  _tag: Schema.Literal("RemoteDefect")
}).pipe(Schema.decodeTo(Schema.Unknown, {
  decode: SchemaGetter.transform(() => ({ _tag: "RemoteDefect" })),
  encode: SchemaGetter.transform(() => ({ _tag: "RemoteDefect" as const }))
}))

const opaqueDefect = { _tag: "RemoteDefect" } as const
const opaqueCause = { name: "Error", message: "Remote internal error" } as const
const JsonString = Schema.fromJsonString(Schema.Unknown)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- RpcSerialization.Parser decode is synchronous and must signal invalid input by throwing.
const decodeJsonString = Schema.decodeUnknownSync(JsonString)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- RpcSerialization.Parser encode is synchronous and must signal invalid output by throwing.
const encodeJsonString = Schema.encodeSync(JsonString)

const sanitizeReason = (reason: unknown): unknown => {
  if (reason === null || typeof reason !== "object" || Array.isArray(reason)) return reason
  if (!Predicate.hasProperty(reason, "_tag")) return reason
  if (reason._tag === "Die") return { ...reason, defect: opaqueDefect }
  if (reason._tag !== "Fail" || !Predicate.hasProperty(reason, "error")) return reason
  const error = reason.error
  if (error === null || typeof error !== "object" || Array.isArray(error)) return reason
  if (!Predicate.hasProperty(error, "cause")) return reason
  return { ...reason, error: { ...error, cause: opaqueCause } }
}

const sanitizeResponse = (response: unknown): unknown => {
  if (Array.isArray(response)) return response.map(sanitizeResponse)
  if (response === null || typeof response !== "object") return response
  if (!Predicate.hasProperty(response, "_tag")) return response
  if (response._tag === "Defect") return { ...response, defect: opaqueDefect }
  if (response._tag !== "Exit" || !Predicate.hasProperty(response, "exit")) return response
  const exit = response.exit
  if (exit === null || typeof exit !== "object" || Array.isArray(exit)) return response
  if (!Predicate.hasProperty(exit, "_tag") || exit._tag !== "Failure") return response
  if (!Predicate.hasProperty(exit, "cause") || !Array.isArray(exit.cause)) return response
  return {
    ...response,
    exit: { ...exit, cause: exit.cause.map(sanitizeReason) }
  }
}

export const layerJson = (options?: {
  readonly maximumFrameBytes?: number
}): Layer.Layer<RpcSerialization.RpcSerialization, ReplicaError.InvalidConfiguration> =>
  Layer.effect(
    RpcSerialization.RpcSerialization,
    Effect.gen(function*() {
      const limit = options?.maximumFrameBytes ?? maximumFrameBytes
      if (!Number.isSafeInteger(limit) || limit <= 0) {
        return yield* invalidConfiguration(
          "maximumFrameBytes",
          "maximumFrameBytes must be a positive safe integer"
        )
      }
      return RpcSerialization.RpcSerialization.of({
        contentType: "application/json",
        includesFraming: false,
        makeUnsafe: () => {
          const decoder = new TextDecoder()
          const encoder = new TextEncoder()
          return {
            decode: (data) => {
              let encoded: string
              let bytes: number
              if (typeof data === "string") {
                encoded = data
                bytes = encoder.encode(data).byteLength
              } else {
                encoded = decoder.decode(data)
                bytes = data.byteLength
              }
              if (bytes > limit) {
                // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- RpcSerialization.Parser is synchronous and signals parsing failures by throwing.
                throw new RangeError(`RPC frame exceeds ${limit} bytes`)
              }
              const decoded = decodeJsonString(encoded)
              if (Array.isArray(decoded)) return decoded
              return [decoded]
            },
            encode: (response) => {
              const encoded = encodeJsonString(sanitizeResponse(response))
              if (encoder.encode(encoded).byteLength > limit) {
                // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- RpcSerialization.Parser is synchronous and signals encoding failures by throwing.
                throw new RangeError(`RPC frame exceeds ${limit} bytes`)
              }
              return encoded
            }
          }
        }
      })
    })
  )

export class Submit extends Rpc.make("Submit", {
  payload: Protocol.VersionedSubmitRequest.fields,
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Discard extends Rpc.make("Discard", {
  payload: Protocol.VersionedDiscardRequest.fields,
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Negotiate extends Rpc.make("Negotiate", {
  payload: Protocol.NegotiateRequest.fields,
  success: Protocol.NegotiatedProtocol,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Pull extends Rpc.make("Pull", {
  payload: Protocol.VersionedPullRequest.fields,
  success: Protocol.PullResult,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Bootstrap extends Rpc.make("Bootstrap", {
  payload: Protocol.VersionedBootstrapRequest.fields,
  success: Protocol.BootstrapPage,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Watch extends Rpc.make("Watch", {
  payload: Protocol.VersionedWatchRequest.fields,
  success: Protocol.Wake,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect,
  stream: true
}) {}

export class JoinEphemeral extends Rpc.make("JoinEphemeral", {
  payload: Protocol.VersionedEphemeralJoinRequest.fields,
  success: Protocol.EphemeralJoinMessage,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect,
  stream: true
}) {}

export class PublishEphemeral extends Rpc.make("PublishEphemeral", {
  payload: Protocol.VersionedEphemeralPublishRequest.fields,
  success: Schema.Null,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class HeartbeatEphemeral extends Rpc.make("HeartbeatEphemeral", {
  payload: Protocol.VersionedEphemeralHeartbeatRequest.fields,
  success: Schema.Null,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export const Rpcs = RpcGroup.make(
  Negotiate,
  Submit,
  Discard,
  Pull,
  Bootstrap,
  Watch,
  JoinEphemeral,
  PublishEphemeral,
  HeartbeatEphemeral
).middleware(
  Authentication.Authentication
)

export interface Client extends RpcClientFromGroup<typeof Rpcs, RpcClientError> {}

export const makeRpcClient: Effect.Effect<
  Client,
  never,
  RpcClientProtocol | RpcMiddleware.ForClient<Authentication.Authentication> | Scope.Scope
> = makeClient(Rpcs)
