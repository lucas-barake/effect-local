import type * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import type * as QueryExecutor from "@lucas-barake/effect-local-sql/QueryExecutor"
import type * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Crypto from "effect/Crypto"
import * as Layer from "effect/Layer"
import type * as Reactivity from "effect/reactivity/Reactivity"
import type { SqlError } from "effect/sql/SqlError"
import * as ExpoCrypto from "./ExpoCrypto.js"
import * as ExpoSqliteClient from "./ExpoSqliteClient.js"

export interface Options<D extends Definition.Any,> extends SqlReplica.Options<D> {
  readonly database: ExpoSqliteClient.ExpoSqliteClientConfig
}

export const layer = <D extends Definition.Any,>(
  options: Options<D>
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity | Crypto.Crypto,
  ReplicaError.ReplicaError | SqlError,
  Reactivity.Reactivity | MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D> | SyncEngine.SyncEngine
> => {
  const { database, ...replica } = options
  return SqlReplica.layer(replica).pipe(
    Layer.provide(ExpoSqliteClient.layer(database)),
    Layer.provideMerge(ExpoCrypto.layer)
  )
}
