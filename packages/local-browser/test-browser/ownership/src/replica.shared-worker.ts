import { BrowserCrypto, BrowserWorkerRunner } from "@effect/platform-browser"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as ReplicaOwner from "@lucas-barake/effect-local-browser/ReplicaOwner"
import * as SessionManager from "@lucas-barake/effect-local-browser/SessionManager"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { definition, DomainLive, limits, TaskListSql } from "./domain.ts"

declare const self: SharedWorkerGlobalScope

const makeEngine = (databasePort: MessagePort) => {
  databasePort.start()
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
  return {
    ownerId: crypto.randomUUID(),
    runtime: ManagedRuntime.make(EngineLive)
  }
}

let engine: ReturnType<typeof makeEngine> | undefined
let provisioner: {
  readonly nonce: string
  readonly port: MessagePort
  readonly timeout: ReturnType<typeof setTimeout>
} | undefined
let resetting = false
const pending = new Map<MessagePort, MessagePort>()
const connections = new Map<MessagePort, {
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly rpcPort: MessagePort
}>()
let verification: {
  readonly engine: ReturnType<typeof makeEngine>
  readonly token: symbol
} | undefined

const requestProvision = () => {
  if (engine !== undefined || provisioner !== undefined || resetting) return
  const next = pending.keys().next().value
  if (next === undefined) return
  const nonce = crypto.randomUUID()
  provisioner = {
    nonce,
    port: next,
    timeout: setTimeout(() => {
      if (provisioner?.nonce !== nonce) return
      provisioner = undefined
      pending.delete(next)
      next.postMessage({ _tag: "ProvisionRejected", nonce })
      next.close()
      requestProvision()
    }, 2000)
  }
  next.postMessage({ _tag: "Provision", nonce })
}

const resetEngine = () => {
  const currentEngine = engine
  if (currentEngine === undefined || resetting) return
  resetting = true
  engine = undefined
  verification = undefined
  for (const controlPort of connections.keys()) {
    controlPort.postMessage({ _tag: "Reattach" })
  }
  Effect.runFork(
    currentEngine.runtime.disposeEffect.pipe(
      Effect.timeout("1 second"),
      Effect.catchTag("TimeoutError", () => Effect.void),
      Effect.ensuring(Effect.sync(() => {
        resetting = false
        requestProvision()
      }))
    )
  )
}

const verifyProvider = () => {
  const currentEngine = engine
  if (currentEngine === undefined || verification !== undefined) return
  const token = Symbol()
  verification = { engine: currentEngine, token }
  currentEngine.runtime.runFork(
    Effect.flatMap(SqlClient.SqlClient, (sql) => sql`SELECT 1`).pipe(Effect.timeout("2 seconds"))
  ).addObserver((exit) => {
    if (verification?.engine !== currentEngine || verification.token !== token) return
    verification = undefined
    if (exit._tag === "Success") {
      for (const [controlPort, rpcPort] of pending) serve(controlPort, rpcPort, false)
    } else {
      resetEngine()
    }
  })
}

const detach = (controlPort: MessagePort) => {
  pending.delete(controlPort)
  const connection = connections.get(controlPort)
  if (connection !== undefined) {
    connections.delete(controlPort)
    Effect.runFork(Fiber.interrupt(connection.fiber))
    connection.rpcPort.close()
  }
  controlPort.close()
}

const serve = (controlPort: MessagePort, rpcPort: MessagePort, provider: boolean) => {
  const currentEngine = engine
  if (currentEngine === undefined) return
  pending.delete(controlPort)
  rpcPort.start()

  const OwnerLive = ReplicaOwner.layerWorker(definition).pipe(
    Layer.provide(BrowserWorkerRunner.layerMessagePort(rpcPort))
  )

  const fiber = currentEngine.runtime.runFork(
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{
        readonly replicaId: string
        readonly writerGeneration: number
      }>`SELECT replica_id AS replicaId, writer_generation AS writerGeneration
        FROM effect_local_metadata WHERE singleton = 1`
      const metadata = rows[0]
      if (metadata === undefined) return yield* Effect.die(new Error("Replica metadata was not initialized"))
      controlPort.postMessage({ _tag: "Attached", ownerId: currentEngine.ownerId, provider, ...metadata })
      return yield* Layer.launch(OwnerLive)
    }).pipe(
      Effect.tapCause((cause) =>
        Effect.sync(() => controlPort.postMessage({ _tag: "OwnerError", message: String(cause) }))
      ),
      Effect.ensuring(Effect.sync(() => {
        rpcPort.close()
        if (connections.get(controlPort)?.rpcPort === rpcPort) connections.delete(controlPort)
      }))
    )
  )
  connections.set(controlPort, { fiber, rpcPort })
}

self.addEventListener("connect", (event) => {
  const controlPort = event.ports[0]
  controlPort.addEventListener("message", (message) => {
    const request = message.data as
      | { readonly _tag: "Attach"; readonly rpcPort: MessagePort }
      | { readonly _tag: "Detach" }
      | {
        readonly _tag: "Provision"
        readonly databasePort: MessagePort
        readonly nonce: string
      }
      | { readonly _tag: "Sessions"; readonly nonce: string }
    if (request._tag === "Sessions") {
      const currentEngine = engine
      if (currentEngine === undefined) {
        controlPort.postMessage({ _tag: "Sessions", nonce: request.nonce, count: 0 })
        return
      }
      currentEngine.runtime.runFork(
        Effect.flatMap(SessionManager.SessionManager, (manager) => manager.activeCount)
      ).addObserver((exit) => {
        controlPort.postMessage({
          _tag: "Sessions",
          nonce: request.nonce,
          count: exit._tag === "Success" ? exit.value : 0
        })
      })
      return
    }
    if (request._tag === "Detach") {
      detach(controlPort)
      return
    }
    if (request._tag === "Attach") {
      pending.set(controlPort, request.rpcPort)
      if (engine !== undefined) verifyProvider()
      else requestProvision()
      return
    }
    if (
      provisioner === undefined || controlPort !== provisioner.port || request.nonce !== provisioner.nonce ||
      engine !== undefined
    ) {
      controlPort.postMessage({ _tag: "ProvisionRejected", nonce: request.nonce })
      request.databasePort.close()
      return
    }
    clearTimeout(provisioner.timeout)
    provisioner = undefined
    engine = makeEngine(request.databasePort)
    controlPort.postMessage({ _tag: "ProvisionAccepted", nonce: request.nonce })
    for (const [pendingControl, rpcPort] of pending) {
      serve(pendingControl, rpcPort, pendingControl === controlPort)
    }
  })
  controlPort.start()
})
