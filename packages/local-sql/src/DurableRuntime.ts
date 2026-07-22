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

export const layerWith = <A, E, R,>(
  definition: ReplicaDefinition.Any,
  workflowRegistrations: Layer.Layer<A, E, R>
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
      workflowRegistrations
    )
    return Layer.merge(DocumentEntity.layer(definition).pipe(Layer.provideMerge(PeerSync.layer)), workflows)
      .pipe(
        Layer.provideMerge(cluster)
      )
  }))

export const layer = (definition: ReplicaDefinition.Any) => layerWith(definition, Layer.empty)
