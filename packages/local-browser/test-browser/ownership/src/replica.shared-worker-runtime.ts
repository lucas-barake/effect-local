import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import type * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as SessionManager from "@lucas-barake/effect-local-browser/SessionManager"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { definition, DomainLive, limits, TaskListSql } from "./domain.ts"

const MetadataRow = Schema.Struct({
  replica_id: Schema.String,
  writer_generation: Identity.WriterGeneration
})

const OwnerInfo = Schema.Struct({
  replicaId: Schema.String,
  writerGeneration: Identity.WriterGeneration
})

const workerUrl = new URL(globalThis.location.href)
const timerGateToken = workerUrl.searchParams.get("effectLocalTestTimerGate")
let timerGate: BroadcastChannel | undefined
if (timerGateToken !== null) {
  timerGate = new BroadcastChannel(`effect-local-ownership-timer-${timerGateToken}`)
}
let timerGateArmed = workerUrl.searchParams.has("effectLocalTestTimerGateArmed")
let releaseProvisionDeadline: (() => void) | undefined
timerGate?.addEventListener("message", (event) => {
  if (event.data?._tag === "Arm" && releaseProvisionDeadline === undefined) {
    timerGateArmed = true
    timerGate.postMessage({ _tag: "Armed" })
  }
  if (event.data?._tag !== "Release" || releaseProvisionDeadline === undefined) return
  const release = releaseProvisionDeadline
  releaseProvisionDeadline = undefined
  timerGate.postMessage({ _tag: "Released" })
  release()
})

let provisionDeadline: OwnershipCoordinator.SharedWorkerOptions<unknown, unknown, unknown>["provisionDeadline"]
if (timerGate !== undefined) {
  const gate = timerGate
  provisionDeadline = (timeout) =>
    Effect.suspend(() => {
      if (!timerGateArmed) return Effect.sleep(timeout)
      timerGateArmed = false
      return Effect.callback<void>((resume) => {
        const release = () => resume(Effect.void)
        releaseProvisionDeadline = release
        gate.postMessage({ _tag: "Held" })
        return Effect.sync(() => {
          if (releaseProvisionDeadline === release) releaseProvisionDeadline = undefined
        })
      })
    })
}

const makeEngine = (databasePort: MessagePort) => {
  const DatabaseLive = BrowserSqlite.layerMessagePort(databasePort)
  const DependenciesLive = Layer.mergeAll(
    DatabaseLive,
    BrowserCrypto.layer,
    DomainLive.pipe(Layer.provide(DatabaseLive)),
    ReplicaLimits.layer(limits)
  )
  const EngineLive = Layer.merge(
    SqlReplica.layerWithBindings(definition, { projections: [TaskListSql] }),
    SessionManager.layer
  ).pipe(Layer.provideMerge(DependenciesLive))
  return ManagedRuntime.make(EngineLive)
}

export const options: OwnershipCoordinator.SharedWorkerOptions<unknown, typeof OwnerInfo.Type, unknown> = {
  name: "effect-local-tasks",
  definition,
  provisionTimeout: "2 seconds",
  provisionDeadline,
  engine: makeEngine,
  info: {
    schema: OwnerInfo,
    make: Effect.flatMap(SqlClient.SqlClient, (sql) =>
      SqlSchema.findOne({
        Request: Schema.Void,
        Result: MetadataRow,
        execute: () => sql`SELECT replica_id, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      })(undefined).pipe(
        Effect.map((row) => ({ replicaId: row.replica_id, writerGeneration: row.writer_generation }))
      ))
  }
}
