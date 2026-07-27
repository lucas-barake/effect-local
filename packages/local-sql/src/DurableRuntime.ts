import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as DocumentEntity from "./DocumentEntity.js"
import * as ClusterStorage from "./internal/clusterStorage.js"
import * as PeerSync from "./PeerSync.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaWorkflow from "./ReplicaWorkflow.js"

const layerWithPeerSync = <A, E, R, EPeer, RPeer,>(
  definition: ReplicaDefinition.Any,
  workflowRegistrations: Layer.Layer<A, E, R>,
  peerSync: Layer.Layer<PeerSync.PeerSync, EPeer, RPeer>
) =>
  Layer.unwrap(Effect.gen(function*() {
    yield* ReplicaBootstrap.ReplicaBootstrap
    const sql = yield* SqlClient.SqlClient
    const cluster = ClusterWorkflowEngine.layer.pipe(
      Layer.provideMerge(ClusterStorage.layer),
      Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))
    )
    const workflows = Layer.mergeAll(
      ReplicaWorkflow.layerRegistration(definition),
      ReplicaWorkflow.layerRuntime,
      ReplicaWorkflow.layerHistoryRewriteRegistration(definition),
      ReplicaWorkflow.layerHistoryRewriteRuntime,
      workflowRegistrations
    )
    // `PeerSync` is provided to both branches, not just the entity. A workflow that changes what a
    // document's history is has to be able to reach the in-memory sync state keyed on that document,
    // and it is reachable only from whatever branch the layer is provided into. The selected
    // `peerSync` layer requires no cluster service, so hoisting it above `cluster` introduces no
    // cycle and preserves the relay-specific implementation.
    return Layer.merge(DocumentEntity.layer(definition), workflows)
      .pipe(
        Layer.provideMerge(peerSync),
        Layer.provideMerge(cluster)
      )
  }))

export const layerWith = <A, E, R,>(
  definition: ReplicaDefinition.Any,
  workflowRegistrations: Layer.Layer<A, E, R>
) => layerWithPeerSync(definition, workflowRegistrations, PeerSync.layer)

export const layer = (definition: ReplicaDefinition.Any) => layerWith(definition, Layer.empty)

export const layerRelay = (definition: ReplicaDefinition.Any) =>
  layerWithPeerSync(definition, Layer.empty, PeerSync.layerRelay)
