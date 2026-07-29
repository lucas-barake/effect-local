import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import * as OpfsWorker from "@effect/sql-sqlite-wasm/OpfsWorker"
import type * as CommandDeliveryPublisher from "@lucas-barake/effect-local-sql/CommandDeliveryPublisher"
import type * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import type * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import type * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Worker from "effect/unstable/workers/Worker"
import * as OwnershipProtocol from "./internal/ownershipProtocol.js"
import * as ReplicaOwner from "./ReplicaOwner.js"
import * as ReplicaRpc from "./ReplicaRpc.js"
import type * as SessionManager from "./SessionManager.js"

/**
 * The services the coordinator must find in the consumer supplied engine runtime. `SqlClient` is
 * required so liveness is proven by a round trip through the actual database worker, never by
 * pinging the provisioning tab. A runtime providing more than these services still satisfies the
 * contract: `ManagedRuntime` is contravariant in its services.
 */
export type EngineServices =
  | Replica.Replica
  | SessionManager.SessionManager
  | CommitPublisher.CommitPublisher
  | PeerConnectionStatus.PeerConnectionStatus
  | RelayConnectionStatus.RelayConnectionStatus
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | Crypto.Crypto
  | SqlClient.SqlClient

export interface SharedWorkerOptions<E, A, E2,> {
  /**
   * Stable identity of this replica owner topology, used in log annotations. The database worker
   * started through `runDatabaseWorker` derives its OPFS database name and Web Lock name from the
   * same string.
   */
  readonly name: string
  readonly definition: ReplicaDefinition.Any
  /**
   * Creates the owner engine over the database port transferred by the provisioning tab. The
   * coordinator proves the engine with a database round trip before accepting the provider, serves
   * tabs against it, and disposes it on takeover.
   */
  readonly engine: (databasePort: MessagePort) => ManagedRuntime.ManagedRuntime<EngineServices, E>
  /**
   * Optional consumer metadata computed once per engine epoch inside the engine runtime and
   * delivered to every tab in the `Attached` frame. Encoding failures fail the engine start.
   */
  readonly info?: {
    readonly schema: Schema.Codec<A, unknown>
    readonly make: Effect.Effect<A, E2, EngineServices>
  } | undefined
  readonly provisionTimeout?: Duration.Input | undefined
  readonly engineStartTimeout?: Duration.Input | undefined
  readonly engineDisposeTimeout?: Duration.Input | undefined
  readonly healthCheck?: {
    readonly interval?: Duration.Input | undefined
    readonly timeout?: Duration.Input | undefined
  } | undefined
}

export class OwnershipCoordinator extends Context.Service<OwnershipCoordinator, {
  /**
   * Wires one SharedWorker control port into the coordinator. Every frame arriving on the port is
   * Schema decoded before it is routed; undecodable frames are answered with an `OwnerError`
   * frame, never trusted.
   */
  readonly attach: (controlPort: MessagePort) => Effect.Effect<void>
}>()(
  "@lucas-barake/effect-local-browser/OwnershipCoordinator"
) {}

interface Client {
  readonly controlPort: MessagePort
  rpcPort: MessagePort | undefined
  served: boolean
}

interface EngineSnapshot {
  readonly runtime: ManagedRuntime.ManagedRuntime<EngineServices, unknown>
  readonly scope: Scope.Closeable
  readonly ownerId: string
  readonly info: unknown
}

interface LiveEngine extends EngineSnapshot {
  readonly epoch: number
  readonly provider: MessagePort
}

interface Provisioning {
  readonly nonce: OwnershipProtocol.ProvisionNonce
  readonly candidate: MessagePort
}

interface Starting {
  readonly epoch: number
  readonly candidate: MessagePort
  readonly fiber: Fiber.Fiber<boolean>
}

interface CoordinatorState {
  readonly clients: Map<MessagePort, Client>
  engine: Option.Option<LiveEngine>
  provisioning: Option.Option<Provisioning>
  starting: Option.Option<Starting>
  disposing: boolean
  epoch: number
  readonly tried: Set<MessagePort>
}

type CoordinatorEvent =
  | {
    readonly _tag: "Frame"
    readonly controlPort: MessagePort
    readonly frame: OwnershipProtocol.PageToOwnerFrame
  }
  | { readonly _tag: "ProvisionExpired"; readonly nonce: OwnershipProtocol.ProvisionNonce }
  | {
    readonly _tag: "EngineStarted"
    readonly epoch: number
    readonly nonce: OwnershipProtocol.ProvisionNonce
    readonly candidate: MessagePort
    readonly exit: Exit.Exit<EngineSnapshot, unknown>
  }
  | { readonly _tag: "HealthProbe"; readonly epoch: number }
  | { readonly _tag: "HealthFailed"; readonly epoch: number }
  | {
    readonly _tag: "AttachVerified"
    readonly controlPort: MessagePort
    readonly epoch: number
    readonly ok: boolean
  }
  | { readonly _tag: "EngineDisposed"; readonly epoch: number }

const HealthRow = Schema.Struct({ ok: Schema.Int })

const healthProbe = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) =>
    SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: HealthRow,
      execute: () => sql`SELECT 1 AS ok`
    })(undefined)
)

export const layerSharedWorker = <E, A = unknown, E2 = never,>(
  options: SharedWorkerOptions<E, A, E2>
): Layer.Layer<OwnershipCoordinator, never, Crypto.Crypto> =>
  Layer.effect(
    OwnershipCoordinator,
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      const layerScope = yield* Effect.scope
      const provisionTimeoutMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.provisionTimeout ?? "2 seconds")
      )
      const engineStartTimeoutMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.engineStartTimeout ?? "30 seconds")
      )
      const engineDisposeTimeoutMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.engineDisposeTimeout ?? "1 second")
      )
      const healthIntervalMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.healthCheck?.interval ?? "2 seconds")
      )
      const healthTimeoutMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.healthCheck?.timeout ?? "5 seconds")
      )

      const events = yield* Queue.unbounded<CoordinatorEvent>()
      const state = yield* Ref.make<CoordinatorState>({
        clients: new Map(),
        engine: Option.none(),
        provisioning: Option.none(),
        starting: Option.none(),
        disposing: false,
        epoch: 0,
        tried: new Set()
      })

      const post = (port: MessagePort, frame: OwnershipProtocol.OwnerToPageFrame) =>
        Effect.sync(() => port.postMessage(Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)(frame)))

      const postOwnerError = (port: MessagePort, message: string, reason?: unknown) =>
        post(port, { _tag: "OwnerError", message, reason })

      const serve = (client: Client, engine: LiveEngine): Effect.Effect<void> => {
        const rpcPort = client.rpcPort
        if (rpcPort === undefined) return Effect.void
        client.served = true
        const ownerLayer = ReplicaOwner.layerWorker(options.definition).pipe(
          Layer.provide(BrowserWorkerRunner.layerMessagePort(rpcPort))
        )
        const server = Effect.scopedWith((scope) =>
          Layer.build(ownerLayer).pipe(
            Scope.provide(scope),
            Effect.andThen(
              post(client.controlPort, {
                _tag: "Attached",
                ownerId: engine.ownerId,
                provider: client.controlPort === engine.provider,
                info: engine.info
              })
            ),
            Effect.andThen(Effect.never)
          )
        )
        return engine.runtime.contextEffect.pipe(
          Effect.flatMap((context) => server.pipe(Effect.provide(context))),
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : postOwnerError(client.controlPort, Cause.pretty(cause))
          ),
          Effect.ensuring(Effect.sync(() => rpcPort.close())),
          Effect.forkIn(engine.scope),
          Effect.asVoid
        )
      }

      const provideRuntime = <A2, E2_,>(
        runtime: ManagedRuntime.ManagedRuntime<EngineServices, unknown>,
        effect: Effect.Effect<A2, E2_, EngineServices>
      ): Effect.Effect<A2, unknown, never> =>
        Effect.flatMap(runtime.contextEffect, (context) => Effect.provide(effect, context))

      const startEngine = (
        epoch: number,
        nonce: OwnershipProtocol.ProvisionNonce,
        candidate: MessagePort,
        databasePort: MessagePort
      ) => {
        const start = Effect.gen(function*() {
          const runtime = yield* Effect.sync(() => options.engine(databasePort))
          const engineScope = yield* Scope.make()
          const started = yield* Effect.exit(
            Effect.gen(function*() {
              yield* provideRuntime(runtime, healthProbe).pipe(
                Effect.timeoutOrElse({
                  duration: engineStartTimeoutMillis,
                  orElse: () => Effect.fail(new Error("replica engine start timed out"))
                })
              )
              let info: unknown
              if (options.info !== undefined) {
                const value = yield* provideRuntime(runtime, options.info.make)
                info = yield* Schema.encodeEffect(options.info.schema)(value)
              }
              const ownerId = yield* Effect.orDie(crypto.randomUUIDv4)
              const snapshot: EngineSnapshot = { runtime, scope: engineScope, ownerId, info }
              return snapshot
            })
          )
          if (Exit.isFailure(started)) {
            yield* Scope.close(engineScope, started).pipe(Effect.ignore)
            yield* runtime.disposeEffect.pipe(Effect.timeout(engineDisposeTimeoutMillis), Effect.ignore)
            yield* Effect.sync(() => databasePort.close())
          }
          return started
        })
        return start.pipe(
          Effect.flatMap((exit) => Queue.offer(events, { _tag: "EngineStarted", epoch, nonce, candidate, exit })),
          Effect.forkIn(layerScope)
        )
      }

      const kickProvisioning: Effect.Effect<void> = Effect.suspend(() => {
        const current = Ref.getUnsafe(state)
        if (
          Option.isSome(current.engine) || Option.isSome(current.provisioning) || Option.isSome(current.starting) ||
          current.disposing
        ) {
          return Effect.void
        }
        let candidate: Client | undefined
        for (const client of current.clients.values()) {
          if (client.rpcPort !== undefined && !current.tried.has(client.controlPort)) {
            candidate = client
            break
          }
        }
        if (candidate === undefined) return Effect.void
        const controlPort = candidate.controlPort
        return Effect.orDie(crypto.randomUUIDv4).pipe(
          Effect.flatMap((uuid) => {
            const nonce = OwnershipProtocol.ProvisionNonce.make(uuid)
            current.provisioning = Option.some({ nonce, candidate: controlPort })
            return post(controlPort, { _tag: "Provision", nonce }).pipe(
              Effect.andThen(
                Effect.sleep(provisionTimeoutMillis).pipe(
                  Effect.andThen(Queue.offer(events, { _tag: "ProvisionExpired", nonce })),
                  Effect.forkIn(layerScope)
                )
              )
            )
          }),
          Effect.asVoid
        )
      })

      const scheduleHealthChecks = (epoch: number): Effect.Effect<void> =>
        Effect.suspend(() => {
          const current = Ref.getUnsafe(state)
          if (Option.isNone(current.engine) || current.engine.value.epoch !== epoch) return Effect.void
          return Queue.offer(events, { _tag: "HealthProbe", epoch }).pipe(
            Effect.andThen(Effect.sleep(healthIntervalMillis)),
            Effect.andThen(scheduleHealthChecks(epoch))
          )
        })

      const disposeEngine = (engine: EngineSnapshot, exit: Exit.Exit<unknown, unknown>) =>
        Scope.close(engine.scope, exit).pipe(
          Effect.andThen(engine.runtime.disposeEffect),
          Effect.timeout(engineDisposeTimeoutMillis),
          Effect.ignore
        )

      const resetEngine = (reason: string): Effect.Effect<void> =>
        Effect.suspend(() => {
          const current = Ref.getUnsafe(state)
          if (Option.isNone(current.engine) || current.disposing) return Effect.void
          const engine = current.engine.value
          current.engine = Option.none()
          current.disposing = true
          const reattach: Array<Effect.Effect<void>> = []
          for (const client of current.clients.values()) {
            // Only a tab whose server just died is told to re-attach. A tab still waiting for
            // its first serve has nothing to re-establish; the next engine serves it as is.
            if (client.served && client.rpcPort !== undefined) {
              reattach.push(post(client.controlPort, { _tag: "Reattach", ownerId: engine.ownerId }))
            }
            client.served = false
          }
          return Effect.logInfo("replica owner engine reset").pipe(
            Effect.annotateLogs({ reason, epoch: engine.epoch, ownerId: engine.ownerId }),
            Effect.andThen(Effect.forEach(reattach, (effect) => effect, { discard: true })),
            Effect.andThen(
              disposeEngine(engine, Exit.void).pipe(
                Effect.andThen(Queue.offer(events, { _tag: "EngineDisposed", epoch: engine.epoch })),
                Effect.forkIn(layerScope)
              )
            ),
            Effect.asVoid
          )
        })

      const handle = (event: CoordinatorEvent): Effect.Effect<void> =>
        Effect.suspend(() => {
          const current = Ref.getUnsafe(state)
          switch (event._tag) {
            case "Frame": {
              const frame = event.frame
              switch (frame._tag) {
                case "Attach": {
                  if (frame.protocolVersion !== OwnershipProtocol.protocolVersion) {
                    return postOwnerError(
                      event.controlPort,
                      `ownership protocol version ${OwnershipProtocol.protocolVersion} required, observed ${frame.protocolVersion}`
                    )
                  }
                  let client = current.clients.get(event.controlPort)
                  if (client === undefined) {
                    client = { controlPort: event.controlPort, rpcPort: undefined, served: false }
                    current.clients.set(event.controlPort, client)
                  }
                  client.rpcPort = frame.rpcPort
                  client.served = false
                  // A fresh attach is a fresh signal: a tab dropped or passed over in an earlier
                  // provisioning round becomes eligible to provide again.
                  current.tried.delete(event.controlPort)
                  if (Option.isNone(current.engine)) {
                    return kickProvisioning
                  }
                  // A tab is never attached to an unverified engine: the database worker is
                  // probed through the engine itself before this tab is served, so a dead
                  // database behind a live control channel cannot hand out a stale owner.
                  const engine = current.engine.value
                  return provideRuntime(engine.runtime, healthProbe).pipe(
                    Effect.timeout(healthTimeoutMillis),
                    Effect.flatMap((result) =>
                      Queue.offer(events, {
                        _tag: "AttachVerified",
                        controlPort: event.controlPort,
                        epoch: engine.epoch,
                        ok: Option.isSome(result)
                      })
                    ),
                    Effect.catchCause((cause) =>
                      Cause.hasInterruptsOnly(cause)
                        ? Effect.failCause(cause)
                        : Queue.offer(events, {
                          _tag: "AttachVerified",
                          controlPort: event.controlPort,
                          epoch: engine.epoch,
                          ok: false
                        })
                    ),
                    Effect.forkIn(layerScope),
                    Effect.asVoid
                  )
                }
                case "Detach": {
                  const client = current.clients.get(event.controlPort)
                  if (client === undefined) return Effect.void
                  current.clients.delete(event.controlPort)
                  const wasProvider = Option.isSome(current.engine) &&
                    current.engine.value.provider === event.controlPort
                  const wasCandidate = Option.isSome(current.provisioning) &&
                    current.provisioning.value.candidate === event.controlPort
                  const starting = Option.getOrUndefined(current.starting)
                  const wasStartingCandidate = starting?.candidate === event.controlPort
                  if (wasCandidate) current.provisioning = Option.none()
                  if (wasProvider) {
                    return resetEngine("the database provider detached")
                  }
                  if (wasStartingCandidate && starting !== undefined) {
                    current.starting = Option.none()
                    current.epoch += 1
                    return Fiber.interrupt(starting.fiber).pipe(
                      Effect.forkIn(layerScope),
                      Effect.andThen(kickProvisioning),
                      Effect.asVoid
                    )
                  }
                  return wasCandidate ? kickProvisioning : Effect.void
                }
                case "Provision": {
                  if (
                    Option.isNone(current.provisioning) ||
                    current.provisioning.value.nonce !== frame.nonce ||
                    current.provisioning.value.candidate !== event.controlPort ||
                    Option.isSome(current.engine)
                  ) {
                    return post(event.controlPort, { _tag: "ProvisionRejected", nonce: frame.nonce })
                  }
                  const nonce = current.provisioning.value.nonce
                  current.provisioning = Option.none()
                  current.epoch += 1
                  const epoch = current.epoch
                  return startEngine(epoch, nonce, event.controlPort, frame.databasePort).pipe(
                    Effect.tap((fiber) =>
                      Effect.sync(() => {
                        current.starting = Option.some({ epoch, candidate: event.controlPort, fiber })
                      })
                    ),
                    Effect.asVoid
                  )
                }
              }
            }
            case "ProvisionExpired": {
              if (
                Option.isNone(current.provisioning) || current.provisioning.value.nonce !== event.nonce
              ) {
                return Effect.void
              }
              const candidate = current.provisioning.value.candidate
              current.provisioning = Option.none()
              current.tried.add(candidate)
              // A candidate that cannot provision when asked is dropped as a client. Its rpc
              // retry will post a fresh Attach when it recovers, which re-registers it.
              current.clients.delete(candidate)
              return post(candidate, { _tag: "ProvisionRejected", nonce: event.nonce }).pipe(
                Effect.andThen(kickProvisioning)
              )
            }
            case "EngineStarted": {
              const isCurrentStart = Option.isSome(current.starting) &&
                current.starting.value.epoch === event.epoch &&
                current.starting.value.candidate === event.candidate
              if (!isCurrentStart || event.epoch !== current.epoch || Option.isSome(current.engine)) {
                if (Exit.isSuccess(event.exit)) {
                  return disposeEngine(event.exit.value, Exit.void).pipe(
                    Effect.forkIn(layerScope),
                    Effect.asVoid
                  )
                }
                return Effect.void
              }
              current.starting = Option.none()
              if (Exit.isFailure(event.exit)) {
                current.tried.add(event.candidate)
                // Same drop as a provisioning timeout: the candidate could not produce a healthy
                // engine, so it is removed and may re-register with a fresh Attach.
                current.clients.delete(event.candidate)
                return Effect.logWarning("replica engine start failed").pipe(
                  Effect.annotateLogs({ cause: Cause.pretty(event.exit.cause) }),
                  Effect.andThen(post(event.candidate, { _tag: "ProvisionRejected", nonce: event.nonce })),
                  Effect.andThen(kickProvisioning)
                )
              }
              if (!current.clients.has(event.candidate)) {
                return disposeEngine(event.exit.value, Exit.void).pipe(
                  Effect.andThen(kickProvisioning),
                  Effect.forkIn(layerScope),
                  Effect.asVoid
                )
              }
              const engine: LiveEngine = {
                ...event.exit.value,
                epoch: event.epoch,
                provider: event.candidate
              }
              current.engine = Option.some(engine)
              current.tried.clear()
              const serves: Array<Effect.Effect<void>> = []
              for (const client of current.clients.values()) {
                if (client.rpcPort !== undefined) serves.push(serve(client, engine))
              }
              return post(event.candidate, { _tag: "ProvisionAccepted", nonce: event.nonce }).pipe(
                Effect.andThen(Effect.forEach(serves, (serveEffect) => serveEffect, { discard: true })),
                Effect.andThen(
                  Effect.sleep(healthIntervalMillis).pipe(
                    Effect.andThen(scheduleHealthChecks(engine.epoch)),
                    Effect.forkIn(layerScope)
                  )
                ),
                Effect.asVoid
              )
            }
            case "HealthProbe": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              const engine = current.engine.value
              return provideRuntime(engine.runtime, healthProbe).pipe(
                Effect.timeout(healthTimeoutMillis),
                Effect.flatMap((result) =>
                  Option.isSome(result)
                    ? Effect.void
                    : Queue.offer(events, { _tag: "HealthFailed", epoch: engine.epoch })
                ),
                // A failed or defecting database round trip means the database worker is gone or
                // broken; both are takeover conditions. Interruption passes through untouched.
                Effect.catchCause((cause) =>
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.failCause(cause)
                    : Queue.offer(events, { _tag: "HealthFailed", epoch: engine.epoch })
                ),
                Effect.forkIn(layerScope),
                Effect.asVoid
              )
            }
            case "HealthFailed": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              return resetEngine("the database worker health check failed")
            }
            case "AttachVerified": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              if (!event.ok) {
                return resetEngine("the database worker health check failed")
              }
              const client = current.clients.get(event.controlPort)
              if (client === undefined || client.rpcPort === undefined) return Effect.void
              return serve(client, current.engine.value)
            }
            case "EngineDisposed": {
              if (event.epoch !== current.epoch) return Effect.void
              current.disposing = false
              current.tried.clear()
              return kickProvisioning
            }
          }
        })

      yield* Queue.take(events).pipe(
        Effect.flatMap(handle),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.logError("ownership coordinator event failed", cause)
        ),
        Effect.forever,
        Effect.forkIn(layerScope)
      )

      return {
        attach: (controlPort) =>
          Effect.sync(() => {
            controlPort.addEventListener("message", (event: MessageEvent<unknown>) => {
              let frame: OwnershipProtocol.PageToOwnerFrame
              try {
                frame = Schema.decodeUnknownSync(OwnershipProtocol.PageToOwnerFrame)(event.data)
              } catch (cause) {
                try {
                  controlPort.postMessage(
                    Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)({
                      _tag: "OwnerError",
                      message: `undecodable ownership frame: ${String(cause)}`.slice(0, 512)
                    })
                  )
                } catch {
                  // The peer is gone; nothing left to notify.
                }
                return
              }
              Queue.offerUnsafe(events, { _tag: "Frame", controlPort, frame })
            })
            controlPort.start()
          })
      } satisfies OwnershipCoordinator["Service"]
    }).pipe(
      Effect.annotateLogs({ module: "OwnershipCoordinator", replica: options.name })
    )
  )

const firstConnectPort = (event: Event): MessagePort | undefined => {
  if (!("ports" in event)) return undefined
  const ports: unknown = event.ports
  if (typeof ports !== "object" || ports === null) return undefined
  const first: unknown = Reflect.get(ports, 0)
  return ReplicaRpc.isMessagePort(first) ? first : undefined
}

/**
 * SharedWorker entry point. The `connect` listener is registered synchronously and every port is
 * buffered until the coordinator is ready, so tabs connecting while the options module is still
 * loading are attached in order once it arrives. When the load fails, every buffered and future
 * connection is answered with an `OwnerError` frame whose reason is tagged `RuntimeLoadFailure`.
 */
export const runSharedWorker = <E, A = unknown, E2 = never,>(
  load: () => Promise<SharedWorkerOptions<E, A, E2>>
): void => {
  const pending: Array<MessagePort> = []
  let coordinator: OwnershipCoordinator["Service"] | undefined
  let startupFailure: { readonly message: string; readonly name: string } | undefined
  const flush = () => {
    if (coordinator !== undefined) {
      const runtime = coordinatorRuntime
      if (runtime === undefined) return
      for (const controlPort of pending.splice(0)) {
        runtime.runFork(coordinator.attach(controlPort))
      }
      return
    }
    if (startupFailure !== undefined) {
      const failure = startupFailure
      for (const controlPort of pending.splice(0)) {
        try {
          controlPort.postMessage(
            Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)({
              _tag: "OwnerError",
              message: failure.message,
              reason: { _tag: "RuntimeLoadFailure", message: failure.message, name: failure.name }
            })
          )
        } catch {
          // The peer is gone; nothing left to notify.
        }
        controlPort.close()
      }
    }
  }
  let coordinatorRuntime: ManagedRuntime.ManagedRuntime<OwnershipCoordinator, unknown> | undefined
  const describe = (cause: unknown) =>
    cause instanceof Error
      ? { message: cause.message, name: cause.name }
      : { message: String(cause), name: "UnknownError" }
  globalThis.addEventListener("connect", (event) => {
    const controlPort = firstConnectPort(event)
    if (controlPort === undefined) return
    pending.push(controlPort)
    flush()
  })
  void load().then(
    (options) => {
      const runtime = ManagedRuntime.make(
        layerSharedWorker(options).pipe(Layer.provide(BrowserCrypto.layer))
      )
      coordinatorRuntime = runtime
      runtime.runPromise(OwnershipCoordinator).then(
        (service) => {
          coordinator = service
          flush()
        },
        (cause) => {
          startupFailure = describe(cause)
          globalThis.reportError(cause)
          flush()
        }
      )
    },
    (cause) => {
      startupFailure = describe(cause)
      globalThis.reportError(cause)
      flush()
    }
  )
}

export interface TabOptions<A = unknown,> {
  /**
   * Must match the `name` given to `layerSharedWorker` / `runSharedWorker` and to
   * `runDatabaseWorker`.
   */
  readonly name: string
  readonly sharedWorker: () => SharedWorker
  readonly databaseWorker: () => globalThis.Worker
  /**
   * Decodes the consumer metadata carried by `Attached` frames. Must be the same schema the
   * SharedWorker side encodes with. When omitted, the metadata is delivered unexamined as
   * `unknown`.
   */
  readonly infoSchema?: Schema.Codec<A, unknown> | undefined
  onAttached?(attached: {
    readonly ownerId: string
    readonly provider: boolean
    readonly info: A
  }): void
  onOwnerError?(message: string, reason?: unknown): void
}

/**
 * Drop in replacement for `BrowserWorker.layer` that wires the tab into the ownership topology.
 * Each spawned RPC worker is a fresh `MessageChannel` registered with the coordinator through an
 * `Attach` frame. When ownership moves, the coordinator posts `Reattach`, the current RPC ports
 * are failed, and Effect's worker protocol reconnects through a new attach handshake. An
 * `OwnerError` frame or a crashed coordinator worker tears the whole connection down, so the next
 * spawn re-establishes it from a clean SharedWorker connection instead of retaining a poisoned
 * one. `pagehide` detaches synchronously so a closing tab releases its provider candidacy and its
 * control channel immediately.
 */
export const layerTab = <A = unknown,>(
  options: TabOptions<A>
): Layer.Layer<Worker.WorkerPlatform | Worker.Spawner> => layerTabImpl(options)

const layerTabImpl = (
  options: TabOptions<unknown>
): Layer.Layer<Worker.WorkerPlatform | Worker.Spawner> => {
  const spawner = Layer.effect(
    Worker.Spawner,
    Effect.gen(function*() {
      interface Connection {
        readonly worker: SharedWorker
        readonly controlPort: MessagePort
        readonly rpcPorts: Map<number, MessagePort>
        readonly onMessage: (event: MessageEvent<unknown>) => void
        readonly onWorkerError: (event: Event) => void
        provisioning:
          | { readonly nonce: OwnershipProtocol.ProvisionNonce; readonly worker: globalThis.Worker }
          | undefined
        databaseWorker: globalThis.Worker | undefined
      }
      let connection: Connection | undefined
      let closed = false

      const failRpcPorts = (current: Connection, message: string) => {
        for (const port of current.rpcPorts.values()) {
          // The ready frame comes first on purpose: a run still waiting for its initial ready
          // latch never observes an error event, so it must be released into the failure rather
          // than left hanging on the latch with its requests buffered.
          port.dispatchEvent(new MessageEvent("message", { data: [0] }))
          port.dispatchEvent(new ErrorEvent("error", { message }))
        }
      }

      const postFrame = (current: Connection, frame: OwnershipProtocol.PageToOwnerFrame) => {
        current.controlPort.postMessage(
          Schema.encodeSync(OwnershipProtocol.PageToOwnerFrame)(frame),
          [...OwnershipProtocol.transferOf(frame)]
        )
      }

      const teardown = (current: Connection, message: string | undefined) => {
        if (connection !== current) return
        connection = undefined
        // The rpc ports are failed before anything is closed: events dispatched on a closed port
        // never reach the worker protocol's listeners, and the connection must already read as
        // cleared when the protocol reacts and reconnects.
        if (message !== undefined) failRpcPorts(current, message)
        current.controlPort.removeEventListener("message", current.onMessage)
        current.worker.removeEventListener("error", current.onWorkerError)
        try {
          postFrame(current, { _tag: "Detach" })
        } catch {
          // The owner is gone; nothing left to notify.
        }
        current.controlPort.close()
        current.provisioning?.worker.terminate()
        current.provisioning = undefined
        current.databaseWorker?.terminate()
        current.databaseWorker = undefined
        for (const port of current.rpcPorts.values()) port.close()
        current.rpcPorts.clear()
      }

      const handleFrame = (current: Connection, frame: OwnershipProtocol.OwnerToPageFrame) => {
        switch (frame._tag) {
          case "Attached": {
            let info: unknown
            try {
              info = options.infoSchema !== undefined
                ? Schema.decodeUnknownSync(options.infoSchema)(frame.info)
                : frame.info
            } catch (cause) {
              options.onOwnerError?.(`undecodable ownership info: ${String(cause)}`.slice(0, 512))
              return
            }
            if (!frame.provider && current.provisioning === undefined && current.databaseWorker !== undefined) {
              // The current engine is provided by another tab, so any database worker this tab
              // still runs belongs to a dead engine epoch.
              current.databaseWorker.terminate()
              current.databaseWorker = undefined
            }
            options.onAttached?.({ ownerId: frame.ownerId, provider: frame.provider, info })
            return
          }
          case "Provision": {
            if (current.provisioning !== undefined) return
            // A new provision request means the coordinator reset the owner: the previous epoch's
            // database worker must go before its replacement starts, or both race the same OPFS
            // handle.
            current.databaseWorker?.terminate()
            current.databaseWorker = undefined
            let database: globalThis.Worker
            try {
              database = options.databaseWorker()
            } catch (cause) {
              options.onOwnerError?.(`replica database worker failed to start: ${String(cause)}`.slice(0, 512))
              return
            }
            const channel = new MessageChannel()
            database.postMessage(
              Schema.encodeSync(OwnershipProtocol.DatabaseWorkerStart)({
                _tag: "DatabaseWorkerStart",
                databasePort: channel.port1
              }),
              [channel.port1]
            )
            current.provisioning = { nonce: frame.nonce, worker: database }
            current.databaseWorker = database
            postFrame(current, { _tag: "Provision", nonce: frame.nonce, databasePort: channel.port2 })
            return
          }
          case "ProvisionAccepted": {
            if (current.provisioning?.nonce === frame.nonce) current.provisioning = undefined
            return
          }
          case "ProvisionRejected": {
            if (current.provisioning?.nonce === frame.nonce) {
              current.provisioning.worker.terminate()
              current.provisioning = undefined
              current.databaseWorker = undefined
            }
            return
          }
          case "Reattach": {
            failRpcPorts(current, `replica ownership moved from ${frame.ownerId}`)
            return
          }
          case "OwnerError": {
            options.onOwnerError?.(frame.message, frame.reason)
            teardown(current, frame.message)
            return
          }
        }
      }

      const establish = (): Connection => {
        const worker = options.sharedWorker()
        const current: Connection = {
          worker,
          controlPort: worker.port,
          rpcPorts: new Map(),
          provisioning: undefined,
          databaseWorker: undefined,
          onMessage: (event: MessageEvent<unknown>) => {
            let frame: OwnershipProtocol.OwnerToPageFrame
            try {
              frame = Schema.decodeUnknownSync(OwnershipProtocol.OwnerToPageFrame)(event.data)
            } catch (cause) {
              options.onOwnerError?.(`undecodable ownership frame: ${String(cause)}`.slice(0, 512))
              return
            }
            handleFrame(current, frame)
          },
          onWorkerError: (event: Event) => {
            if (connection !== current) return
            const message = event instanceof ErrorEvent ? event.message : "replica owner worker failed"
            options.onOwnerError?.(message)
            teardown(current, message)
          }
        }
        current.controlPort.addEventListener("message", current.onMessage)
        current.controlPort.start()
        worker.addEventListener("error", current.onWorkerError)
        connection = current
        return current
      }

      const onPageHide = () => {
        closed = true
        const current = connection
        if (current !== undefined) teardown(current, undefined)
      }

      const listeningToPageHide = typeof globalThis.addEventListener === "function"
      if (listeningToPageHide) globalThis.addEventListener("pagehide", onPageHide)

      yield* Effect.acquireRelease(
        Effect.void,
        () =>
          Effect.sync(() => {
            closed = true
            if (listeningToPageHide) globalThis.removeEventListener("pagehide", onPageHide)
            const current = connection
            if (current !== undefined) teardown(current, undefined)
          })
      )

      return (id: number): MessagePort => {
        if (closed) throw new Error("the ownership tab connection is closed")
        const current = connection ?? establish()
        current.rpcPorts.get(id)?.close()
        const channel = new MessageChannel()
        current.rpcPorts.set(id, channel.port2)
        postFrame(current, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: channel.port1
        })
        return channel.port2
      }
    })
  )
  return Layer.merge(BrowserWorker.layerPlatform, spawner)
}

const withDatabaseLock = <E,>(
  name: string,
  effect: Effect.Effect<void, E>
): Effect.Effect<void, E> => {
  if (typeof navigator !== "undefined" && "locks" in navigator) {
    return Effect.callback<void, E>((resume, signal) => {
      navigator.locks.request(name, { signal }, () =>
        new Promise<void>((resolve) => {
          resume(Effect.onExit(effect, () => {
            resolve()
            return Effect.void
          }))
        })).catch((defect) => resume(Effect.die(defect)))
    })
  }
  return effect
}

export interface DatabaseWorkerOptions {
  /**
   * Must match the `name` given to `layerSharedWorker` / `runSharedWorker` and `layerTab`.
   */
  readonly name: string
  readonly dbName?: string | undefined
  readonly lockName?: string | undefined
}

/**
 * Entry point of the durable database worker. The first message must be a Schema decodable
 * `DatabaseWorkerStart` frame carrying the database port. The worker then holds a Web Lock for the
 * database lifetime and runs the SQLite OPFS loop, so a replacement provider waits for the lock
 * instead of opening OPFS concurrently with a slow prior owner.
 */
export const runDatabaseWorker = (options: DatabaseWorkerOptions): void => {
  const dbName = options.dbName ?? `${options.name}.sqlite`
  const lockName = options.lockName ?? `${options.name}-opfs`
  globalThis.addEventListener("message", (event: MessageEvent<unknown>) => {
    let frame: OwnershipProtocol.DatabaseWorkerStart
    try {
      frame = Schema.decodeUnknownSync(OwnershipProtocol.DatabaseWorkerStart)(event.data)
    } catch (cause) {
      globalThis.reportError(cause)
      return
    }
    frame.databasePort.start()
    Effect.runFork(
      withDatabaseLock(lockName, OpfsWorker.run({ port: frame.databasePort, dbName })).pipe(
        Effect.tapCause(Effect.logError)
      )
    )
  }, { once: true })
}
