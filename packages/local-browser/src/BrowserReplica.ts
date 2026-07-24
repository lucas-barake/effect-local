import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Layer from "effect/Layer"
import { RpcClient } from "effect/unstable/rpc"
import * as ReplicaAtom from "./ReplicaAtom.js"
import * as ReplicaClient from "./ReplicaClient.js"

type WorkerOptions = Parameters<typeof RpcClient.layerProtocolWorker>[0]

export const layerWith = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  timeouts?: ReplicaClient.TimeoutOptions
) =>
  Layer.effect(
    Replica.Replica,
    ReplicaClient.ReplicaClient
  ).pipe(
    Layer.provide(ReplicaClient.layer(definition, timeouts)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layer = (definition: ReplicaDefinition.Any, timeouts?: ReplicaClient.TimeoutOptions) =>
  layerWith(definition, { size: 1, concurrency: 32 }, timeouts)

export const layerWithReactivityOptions = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  timeouts?: ReplicaClient.TimeoutOptions
) =>
  Layer.merge(
    Layer.effect(Replica.Replica, ReplicaClient.ReplicaClient),
    ReplicaAtom.layerReactivity
  ).pipe(
    Layer.provide(ReplicaClient.layer(definition, timeouts)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layerWithReactivity = (definition: ReplicaDefinition.Any, timeouts?: ReplicaClient.TimeoutOptions) =>
  layerWithReactivityOptions(definition, { size: 1, concurrency: 32 }, timeouts)
