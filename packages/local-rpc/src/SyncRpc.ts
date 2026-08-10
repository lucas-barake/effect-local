import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Rpc from "effect/unstable/rpc/Rpc"
import { make as makeClient } from "effect/unstable/rpc/RpcClient"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import * as Authentication from "./Authentication.js"

export class Submit extends Rpc.make("Submit", {
  payload: Protocol.MutationEnvelope.fields,
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError,
  defect: Schema.Defect()
}) {}

export class Pull extends Rpc.make("Pull", {
  payload: Protocol.PullRequest.fields,
  success: Protocol.PullPage,
  error: ReplicaError.ReplicaError,
  defect: Schema.Defect()
}) {}

export class Watch extends Rpc.make("Watch", {
  payload: { spaceId: Protocol.PullRequest.fields.spaceId },
  success: Protocol.Wake,
  error: ReplicaError.ReplicaError,
  defect: Schema.Defect(),
  stream: true
}) {}

export class PublishPresence extends Rpc.make("PublishPresence", {
  payload: Protocol.PresenceUpdate.fields,
  success: Schema.Null,
  error: ReplicaError.ReplicaError,
  defect: Schema.Defect()
}) {}

export class WatchPresence extends Rpc.make("WatchPresence", {
  payload: { spaceId: Protocol.PullRequest.fields.spaceId },
  success: Protocol.PresenceUpdate,
  error: ReplicaError.ReplicaError,
  defect: Schema.Defect(),
  stream: true
}) {}

export const Rpcs = RpcGroup.make(Submit, Pull, Watch, PublishPresence, WatchPresence).middleware(
  Authentication.Authentication
)

export interface Client extends RpcClient.FromGroup<typeof Rpcs, RpcClientError> {}

export const makeRpcClient: Effect.Effect<
  Client,
  never,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication> | Scope.Scope
> = makeClient(Rpcs)
