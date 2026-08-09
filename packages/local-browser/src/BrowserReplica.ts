import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { RpcClient } from "effect/unstable/rpc"
import * as ReplicaAtom from "./ReplicaAtom.js"
import * as ReplicaClient from "./ReplicaClient.js"

type WorkerOptions = Parameters<typeof RpcClient.layerProtocolWorker>[0]

/**
 * `Layer.fresh`, because this republishes whichever `ReplicaClient` its own graph provides, and
 * Layer memoization is by reference. Two replica graphs under one memo map - one `Atom.runtime`
 * memo map is shared app wide - would otherwise both resolve to the first graph's build, and the
 * second replica's `Replica` and `PeerConnectionStatus` would silently answer for the first one.
 */
const clientServices = Layer.mergeAll(
  Layer.effect(Replica.Replica, ReplicaClient.ReplicaClient),
  Layer.effect(
    PeerConnectionStatus.PeerConnectionStatus,
    ReplicaClient.ReplicaClient.pipe(Effect.map((client) => client.peerConnectionStatus))
  ),
  Layer.effect(
    RelayConnectionStatus.RelayConnectionStatus,
    ReplicaClient.ReplicaClient.pipe(Effect.map((client) => client.relayConnectionStatus))
  )
).pipe(Layer.fresh)

export const layerWith = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  clientOptions?: ReplicaClient.Options
) =>
  clientServices.pipe(
    Layer.provideMerge(ReplicaClient.layer(definition, clientOptions)),
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
    Layer.provideMerge(ReplicaClient.layer(definition, clientOptions)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layerWithReactivity = (definition: ReplicaDefinition.Any, clientOptions?: ReplicaClient.Options) =>
  layerWithReactivityOptions(definition, { size: 1, concurrency: 32 }, clientOptions)
