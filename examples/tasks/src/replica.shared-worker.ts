import { BrowserCrypto, BrowserWorkerRunner } from "@effect/platform-browser"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as ReplicaOwner from "@lucas-barake/effect-local-browser/ReplicaOwner"
import * as SessionManager from "@lucas-barake/effect-local-browser/SessionManager"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { definition, DomainLive, limits, TaskListSql } from "./domain.ts"

declare const self: SharedWorkerGlobalScope

const makeEngine = (databasePort: MessagePort, providerPort: MessagePort) => {
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
    providerPort,
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
let verification: {
  readonly engine: ReturnType<typeof makeEngine>
  readonly nonce: string
  readonly timeout: ReturnType<typeof setTimeout>
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
  if (verification !== undefined) clearTimeout(verification.timeout)
  verification = undefined
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
  const nonce = crypto.randomUUID()
  verification = {
    engine: currentEngine,
    nonce,
    timeout: setTimeout(() => {
      if (verification?.engine === currentEngine && verification.nonce === nonce) resetEngine()
    }, 2000)
  }
  currentEngine.providerPort.postMessage({ _tag: "Ping", nonce })
}

const onLiveness = (response: { readonly _tag: "Alive"; readonly nonce: string }) => {
  const current = verification
  if (current === undefined || response.nonce !== current.nonce) return
  clearTimeout(current.timeout)
  verification = undefined
  for (const [controlPort, rpcPort] of pending) {
    serve(controlPort, rpcPort, false)
  }
}

const serve = (controlPort: MessagePort, rpcPort: MessagePort, provider: boolean) => {
  const currentEngine = engine
  if (currentEngine === undefined) return
  pending.delete(controlPort)
  rpcPort.start()

  const OwnerLive = ReplicaOwner.layerWorker(definition).pipe(
    Layer.provide(BrowserWorkerRunner.layerMessagePort(rpcPort))
  )

  currentEngine.runtime.runFork(
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
        controlPort.close()
      }))
    )
  )
}

self.addEventListener("connect", (event) => {
  const controlPort = event.ports[0]
  controlPort.addEventListener("message", (message) => {
    const request = message.data as
      | { readonly _tag: "Alive"; readonly nonce: string }
      | { readonly _tag: "Attach"; readonly rpcPort: MessagePort }
      | {
        readonly _tag: "Provision"
        readonly databasePort: MessagePort
        readonly nonce: string
      }
    if (request._tag === "Alive") {
      onLiveness(request)
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
    engine = makeEngine(request.databasePort, controlPort)
    controlPort.postMessage({ _tag: "ProvisionAccepted", nonce: request.nonce })
    for (const [pendingControl, rpcPort] of pending) {
      serve(pendingControl, rpcPort, pendingControl === controlPort)
    }
  })
  controlPort.start()
})
