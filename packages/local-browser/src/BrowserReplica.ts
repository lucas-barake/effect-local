import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Transient from "@lucas-barake/effect-local/Transient"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { RpcClient } from "effect/unstable/rpc"
import * as ReplicaAtom from "./ReplicaAtom.js"
import * as ReplicaClient from "./ReplicaClient.js"

type WorkerOptions = Parameters<typeof RpcClient.layerProtocolWorker>[0]

/**
 * The republished services must build per graph, not once. They re-export whichever
 * `ReplicaClient` their own graph provides, and Layer memoization is by object identity under the
 * app wide `Atom.runtime` memo map, so lifting them out of this function would make a second
 * replica silently resolve to the first one's client.
 */
const clientServices = (definition: ReplicaDefinition.Any) =>
  Layer.merge(
    Layer.effectContext(
      ReplicaClient.ReplicaClient.pipe(
        Effect.map((client) =>
          Context.make(Replica.Replica, client).pipe(
            Context.add(PeerConnectionStatus.PeerConnectionStatus, client.peerConnectionStatus),
            Context.add(RelayConnectionStatus.RelayConnectionStatus, client.relayConnectionStatus)
          )
        )
      )
    ),
    Transient.layer(definition.transients)
  )

export const layerWith = (
  definition: ReplicaDefinition.Any,
  options: WorkerOptions,
  clientOptions?: ReplicaClient.Options
) =>
  clientServices(definition).pipe(
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
    clientServices(definition),
    ReplicaAtom.layerReactivity
  ).pipe(
    Layer.provideMerge(ReplicaClient.layer(definition, clientOptions)),
    Layer.provide(RpcClient.layerProtocolWorker(options))
  )

export const layerWithReactivity = (definition: ReplicaDefinition.Any, clientOptions?: ReplicaClient.Options) =>
  layerWithReactivityOptions(definition, { size: 1, concurrency: 32 }, clientOptions)
