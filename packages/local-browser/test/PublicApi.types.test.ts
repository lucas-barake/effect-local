import type * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import type * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Crypto from "effect/Crypto"
import type * as Layer from "effect/Layer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import type * as Worker from "effect/unstable/workers/Worker"
import type * as WorkerError from "effect/unstable/workers/WorkerError"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as BrowserSqlite from "../src/BrowserSqlite.js"
import type * as ReplicaClient from "../src/ReplicaClient.js"
import { definition } from "./fixtures.js"

describe("public browser API types", () => {
  it("keeps worker creation as a layer requirement", () => {
    const layer: Layer.Layer<
      Replica.Replica | PeerConnectionStatus.PeerConnectionStatus,
      ReplicaError.ReplicaError | WorkerError.WorkerError,
      Crypto.Crypto | Worker.WorkerPlatform | Worker.Spawner
    > = BrowserReplica.layer(definition)
    assert.isDefined(layer)
  })

  it("exposes client options through every browser replica constructor", () => {
    const timeouts = {
      sessionTimeout: "20 seconds",
      operationTimeout: "2 minutes"
    } satisfies ReplicaClient.Options
    const worker = { size: 2, concurrency: 8 }
    assert.isDefined(BrowserReplica.layer(definition, timeouts))
    assert.isDefined(BrowserReplica.layerWith(definition, worker, timeouts))
    assert.isDefined(BrowserReplica.layerWithReactivity(definition, timeouts))
    assert.isDefined(BrowserReplica.layerWithReactivityOptions(definition, worker, timeouts))
  })

  it("accepts a page provisioned database port", () => {
    const layer: Layer.Layer<
      SqliteClient.SqliteClient | SqlClient.SqlClient,
      SqlError.SqlError,
      BrowserSqlite.DatabasePort
    > = BrowserSqlite.layer
    const provisioned: Layer.Layer<
      SqliteClient.SqliteClient | SqlClient.SqlClient,
      SqlError.SqlError
    > = BrowserSqlite.layerMessagePort(new MessageChannel().port1)
    assert.isDefined(layer)
    assert.isDefined(provisioned)
  })
})
