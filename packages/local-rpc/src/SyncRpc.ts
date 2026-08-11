import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import type * as Scope from "effect/Scope"
import * as Rpc from "effect/unstable/rpc/Rpc"
import { make as makeClient } from "effect/unstable/rpc/RpcClient"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as Authentication from "./Authentication.js"

export const maximumFrameBytes = Protocol.maximumBatchBytes + 64 * 1024

const RemoteDefect = Schema.Struct({
  _tag: Schema.Literal("RemoteDefect")
}).pipe(Schema.decodeTo(Schema.Unknown, {
  decode: SchemaGetter.transform(() => ({ _tag: "RemoteDefect" })),
  encode: SchemaGetter.transform(() => ({ _tag: "RemoteDefect" as const }))
}))

const opaqueDefect = { _tag: "RemoteDefect" } as const
const opaqueCause = { name: "Error", message: "Remote internal error" } as const

const sanitizeReason = (reason: unknown): unknown => {
  if (reason === null || typeof reason !== "object" || Array.isArray(reason)) return reason
  const record = reason as Record<string, unknown>
  if (record._tag === "Die") return { ...record, defect: opaqueDefect }
  if (record._tag !== "Fail") return reason
  const error = record.error
  if (error === null || typeof error !== "object" || Array.isArray(error) || !("cause" in error)) return reason
  return { ...record, error: { ...(error as Record<string, unknown>), cause: opaqueCause } }
}

const sanitizeResponse = (response: unknown): unknown => {
  if (Array.isArray(response)) return response.map(sanitizeResponse)
  if (response === null || typeof response !== "object") return response
  const record = response as Record<string, unknown>
  if (record._tag === "Defect") return { ...record, defect: opaqueDefect }
  if (record._tag !== "Exit") return response
  const exit = record.exit
  if (exit === null || typeof exit !== "object" || Array.isArray(exit)) return response
  const encodedExit = exit as Record<string, unknown>
  if (encodedExit._tag !== "Failure" || !Array.isArray(encodedExit.cause)) return response
  return {
    ...record,
    exit: { ...encodedExit, cause: encodedExit.cause.map(sanitizeReason) }
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
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumFrameBytes",
          message: "maximumFrameBytes must be a positive safe integer"
        })
      }
      return RpcSerialization.RpcSerialization.of({
        contentType: "application/json",
        includesFraming: false,
        makeUnsafe: () => {
          const decoder = new TextDecoder()
          const encoder = new TextEncoder()
          return {
            decode: (data) => {
              const bytes = typeof data === "string" ? encoder.encode(data).byteLength : data.byteLength
              if (bytes > limit) throw new RangeError(`RPC frame exceeds ${limit} bytes`)
              const decoded = JSON.parse(typeof data === "string" ? data : decoder.decode(data))
              return Array.isArray(decoded) ? decoded : [decoded]
            },
            encode: (response) => {
              const encoded = JSON.stringify(sanitizeResponse(response))
              if (encoder.encode(encoded).byteLength > limit) {
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
  payload: Protocol.SubmitRequest.fields,
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Pull extends Rpc.make("Pull", {
  payload: Protocol.PullRequest.fields,
  success: Protocol.PullResult,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Bootstrap extends Rpc.make("Bootstrap", {
  payload: Protocol.BootstrapRequest.fields,
  success: Protocol.BootstrapPage,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class Watch extends Rpc.make("Watch", {
  payload: Protocol.WatchRequest.fields,
  success: Protocol.Wake,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect,
  stream: true
}) {}

export class PublishPresence extends Rpc.make("PublishPresence", {
  payload: Protocol.PresenceUpdate.fields,
  success: Schema.Null,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect
}) {}

export class WatchPresence extends Rpc.make("WatchPresence", {
  payload: { spaceId: Protocol.PullRequest.fields.spaceId },
  success: Protocol.PresenceUpdate,
  error: ReplicaError.ReplicaError,
  defect: RemoteDefect,
  stream: true
}) {}

export const Rpcs = RpcGroup.make(Submit, Pull, Bootstrap, Watch, PublishPresence, WatchPresence).middleware(
  Authentication.Authentication
)

export interface Client extends RpcClient.FromGroup<typeof Rpcs, RpcClientError> {}

export const makeRpcClient: Effect.Effect<
  Client,
  never,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication> | Scope.Scope
> = makeClient(Rpcs)
