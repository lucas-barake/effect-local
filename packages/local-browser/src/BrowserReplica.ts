import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { RpcClient } from "effect/unstable/rpc"
import * as ReplicaAtom from "./ReplicaAtom.js"
import * as ReplicaClient from "./ReplicaClient.js"

type WorkerOptions = Parameters<typeof RpcClient.layerProtocolWorker>[0]

const clientServices = Layer.effectContext(
  ReplicaClient.ReplicaClient.pipe(
    Effect.map((client) =>
      Context.make(Replica.Replica, client).pipe(
        Context.add(PeerConnectionStatus.PeerConnectionStatus, client.peerConnectionStatus)
      )
    )
  )
)

export const layerWith = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  clientOptions?: ReplicaClient.Options
) =>
  clientServices.pipe(
    Layer.provide(ReplicaClient.layer(definition, clientOptions)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layer = (definition: ReplicaDefinition.Any, clientOptions?: ReplicaClient.Options) =>
  layerWith(definition, { size: 1, concurrency: 32 }, clientOptions)

export const layerWithReactivityOptions = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  clientOptions?: ReplicaClient.Options
) =>
  Layer.merge(
    clientServices,
    ReplicaAtom.layerReactivity
  ).pipe(
    Layer.provide(ReplicaClient.layer(definition, clientOptions)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layerWithReactivity = (definition: ReplicaDefinition.Any, clientOptions?: ReplicaClient.Options) =>
  layerWithReactivityOptions(definition, { size: 1, concurrency: 32 }, clientOptions)
