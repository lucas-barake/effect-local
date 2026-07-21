import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Layer from "effect/Layer"
import { RpcClient } from "effect/unstable/rpc"
import * as ReplicaClient from "./ReplicaClient.js"

export const layer = (definition: ReplicaDefinition.Any) =>
  Layer.effect(
    Replica.Replica,
    ReplicaClient.ReplicaClient
  ).pipe(
    Layer.provideMerge(ReplicaClient.layer(definition)),
    Layer.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 32 }))
  )
