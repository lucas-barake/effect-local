import type * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
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
import type * as Socket from "effect/socket/Socket"
import type { SqlError } from "effect/sql/SqlError"
import * as ExpoCrypto from "./ExpoCrypto.js"
import * as ExpoSqliteClient from "./ExpoSqliteClient.js"
import * as ReactNativeSocket from "./ReactNativeSocket.js"

export interface Options<D extends Definition.Any, ES extends { readonly _tag: string },>
  extends SqlReplica.Options<D>
{
  readonly database: ExpoSqliteClient.ExpoSqliteClientConfig
  readonly layerSync: Layer.Layer<
    SyncEngine.SyncEngine | EphemeralClient.EphemeralClient,
    ES,
    Socket.WebSocketConstructor
  >
}

export const layer = <D extends Definition.Any, ES extends { readonly _tag: string },>(
  options: Options<D, ES>
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient | Crypto.Crypto,
  ReplicaError.ReplicaError | SqlError | ES,
  Reactivity.Reactivity | MutationRuntime.Handlers<D> | QueryExecutor.Handlers<D>
> => {
  const { database, layerSync, ...replica } = options
  return SqlReplica.layer(replica).pipe(
    Layer.provide(ExpoSqliteClient.layer(database)),
    Layer.provideMerge(layerSync.pipe(Layer.provide(ReactNativeSocket.layerWebSocketConstructor))),
    Layer.provideMerge(ExpoCrypto.layer)
  )
}
