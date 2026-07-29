import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import { definition, DomainLive, limits, TaskListSql } from "./domain.js"

const DatabaseLive = SqliteClient.layer({ filename: "tasks.sqlite" })

const DependenciesLive = Layer.mergeAll(
  DatabaseLive,
  NodeCrypto.layer,
  DomainLive.pipe(Layer.provide(DatabaseLive)),
  ReplicaLimits.layer(limits)
)

export const EngineLive = SqlReplica.layerWithBindings(definition, {
  projections: [TaskListSql]
}).pipe(Layer.provideMerge(DependenciesLive))
