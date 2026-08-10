import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { MysqlClient } from "@effect/sql-mysql2"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { describe, it } from "@effect/vitest"
import { MySqlContainer, type StartedMySqlContainer } from "@testcontainers/mysql"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"
import { ContainerError, PgContainer } from "./PgContainer.js"
import { relayInboxStoreContract } from "./RelayInboxStoreContract.js"

/**
 * The store contract against every dialect the relay claims to support.
 *
 * Multi-dialect parity is a decision, not an accident: the previous relay store supported all three
 * and dropping one here would be a silent regression. It is also the only way this suite can see a
 * whole class of defect — the PostgreSQL driver hands back `BIGINT`, `COUNT` and `SUM` as strings,
 * and MySQL compares identity columns case-insensitively without a binary collation. Both were real
 * bugs in this store, and both are invisible to SQLite.
 */

class MysqlContainer extends Context.Service<MysqlContainer, StartedMySqlContainer>()(
  "@lucas-barake/effect-local-rpc/test/MysqlContainer"
) {
  static readonly layer = Layer.effect(this)(
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () => new MySqlContainer("mysql:lts").start(),
        catch: (cause) => new ContainerError({ cause })
      }),
      (container) => Effect.promise(() => container.stop())
    )
  )

  static readonly layerClient = Layer.unwrap(
    Effect.gen(function*() {
      const container = yield* MysqlContainer
      return MysqlClient.layer({ url: Redacted.make(container.getConnectionUri()) })
    })
  ).pipe(Layer.provide(this.layer))
}

// A file rather than `:memory:`, so the store is exercised against a real page cache and a real
// journal instead of the in-process shortcut.
const SqliteLayer = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  return SqliteClient.layer({ filename: `${directory}/relay-inbox.sqlite` })
}).pipe(Layer.unwrap, Layer.provide(NodeFileSystem.layer))

// Merged out rather than kept private: observing payload erasure means reading the row.
const storeFor = (client: Layer.Layer<SqlClient.SqlClient, unknown>) =>
  SqlRelayInboxStore.layer.pipe(
    Layer.provideMerge(client),
    Layer.provide(NodeCrypto.layer),
    Layer.orDie
  )

describe("RelayInboxStore", () => {
  for (
    const dialect of [
      { name: "sqlite", client: SqliteLayer, timeout: 60_000 },
      { name: "postgres", client: PgContainer.layerClient, timeout: 180_000 },
      { name: "mysql", client: MysqlContainer.layerClient, timeout: 240_000 }
    ]
  ) {
    it.layer(storeFor(dialect.client), {
      timeout: dialect.timeout
    })(dialect.name, (testApi) => {
      for (const check of relayInboxStoreContract) {
        testApi.effect(check.name, () => check.run)
      }
    })
  }
})
