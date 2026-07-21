import type * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Layer from "effect/Layer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import type * as Worker from "effect/unstable/workers/Worker"
import type * as WorkerError from "effect/unstable/workers/WorkerError"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as BrowserSqlite from "../src/BrowserSqlite.js"
import { definition } from "./fixtures.js"

describe("public browser API types", () => {
  it("keeps worker creation as a layer requirement", () => {
    const layer: Layer.Layer<
      Replica.Replica,
      ReplicaError.ReplicaError | WorkerError.WorkerError,
      Worker.WorkerPlatform | Worker.Spawner
    > = BrowserReplica.layer(definition)
    assert.isDefined(layer)
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
    > = BrowserSqlite.layerPort(new MessageChannel().port1)
    assert.isDefined(layer)
    assert.isDefined(provisioned)
  })
})
