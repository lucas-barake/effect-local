import { assert, describe, it } from "@effect/vitest"
import type * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as Crypto from "effect/Crypto"
import type * as Layer from "effect/Layer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as ExpoSqlite from "../src/ExpoSqlite.js"
import * as ReactNativeReplica from "../src/ReactNativeReplica.js"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import { definition, LabelsSql } from "./fixtures.js"

describe("public react-native API types", () => {
  it("keeps storage, crypto, and limits as requirements of the direct replica", () => {
    type Requirements = ReturnType<typeof ReactNativeReplica.layer> extends Layer.Layer<infer _Out, infer _E, infer R> ?
      R :
      never
    type Expected = Crypto.Crypto | ReplicaLimits.ReplicaLimits | SqlClient.SqlClient
    // Assignable only while all three remain members of the requirement union.
    const check: Requirements = undefined as unknown as Expected
    assert.isUndefined(check)
    assert.isDefined(ReactNativeReplica.layer(definition, { projections: [LabelsSql] }))
  })

  it("leaves RelayConnectionStatus open on the relay topology", () => {
    type RelayRequirements = ReturnType<typeof ReactNativeReplica.layerRelay> extends
      Layer.Layer<infer _Out, infer _E, infer Requirements> ? Requirements : never

    const relay: RelayRequirements = undefined as unknown as RelayConnectionStatus.RelayConnectionStatus
    assert.isUndefined(relay)
    assert.isDefined(ReactNativeReplica.layerRelay(definition, { projections: [LabelsSql] }))
  })

  it("exposes the driver through both the specific and the generic SqlClient tag", () => {
    const layer: Layer.Layer<ExpoSqlite.ExpoSqliteClient | SqlClient.SqlClient, SqlError.SqlError> = ExpoSqlite.layer({
      databaseName: ":memory:"
    })
    assert.isDefined(layer)
  })

  it("re-exports the shared atom factories", () => {
    assert.isDefined(ReplicaAtom.queryFamily)
    assert.isDefined(ReplicaAtom.documentFamily)
    assert.isDefined(ReplicaAtom.mutation)
    assert.isDefined(ReplicaAtom.status)
    assert.isDefined(ReplicaAtom.relayConnectionStatus)
  })
})
