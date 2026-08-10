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
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Pull from "effect/Pull"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
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
  readonly provisionDeadline?: ((timeout: Duration.Duration) => Effect.Effect<void>) | undefined
  readonly engineStartTimeout?: Duration.Input | undefined
  readonly engineDisposeTimeout?: Duration.Input | undefined
  /**
   * Liveness policy for the database worker behind the engine. A probe is one `SELECT 1` round
   * trip through the engine's own `SqlClient` and therefore queues behind the engine's regular
   * SQL work: under a heavy write burst, such as a relay backlog drain, a probe completes late
   * even though the worker is perfectly healthy. `interval` paces probes, with at most one in
   * flight at a time. A probe outstanding past `timeout` logs a warning. The engine is reset
   * only when no round trip has completed within `deadline`, or when a probe fails outright,
   * which means the database worker is gone or broken rather than busy.
   */
  readonly healthCheck?: {
    readonly interval?: Duration.Input | undefined
    readonly timeout?: Duration.Input | undefined
    readonly deadline?: Duration.Input | undefined
  } | undefined
  /**
   * Governs re-provisioning after engine failures. The `schedule` shapes one run per failure
   * streak: every consecutive engine failure takes one step, the step's delay gates the next
   * provisioning attempt, and a passing periodic health probe ends the streak so the next failure
   * starts a fresh run. A provider tab detaching is not a failure and never consults the schedule,
   * so tab-close takeover stays immediate. When the schedule completes, provisioning stops, every
   * tab is posted an `OwnerError` frame with an `EngineProvisionExhausted` reason, and the next
   * tab attach starts a new run. The default schedule retries forever: an immediate first retry,
   * then jittered exponential delays from 500ms capped at 30 seconds. Independently, once
   * `failureBudget` (default 5) consecutive failures accumulate, every further failure is surfaced
   * to tabs as an `OwnerError` frame with an `EngineProvisionFailing` reason while retrying
   * continues, so the app can react without the coordinator giving up.
   */
  readonly provisionRetry?: {
    readonly schedule?: Schedule.Schedule<unknown, void> | undefined
    readonly failureBudget?: number | undefined
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

const runSyncSafe = <A,>(thunk: () => A, fallback: A): A => {
  const exit = Effect.runSyncExit(Effect.try({ try: thunk, catch: () => fallback }))
  if (Exit.isSuccess(exit)) return exit.value
  return fallback
}

const safeCall = (thunk: () => void): void => {
  runSyncSafe(() => {
    thunk()
  }, undefined)
}

const safePostMessage = (
  port: MessagePort,
  value: unknown,
  transfer?: ReadonlyArray<Transferable>
): void => {
  safeCall(() => {
    if (transfer === undefined) port.postMessage(value)
    else port.postMessage(value, [...transfer])
  })
}

const DatabaseActivityFrame = Schema.Union([
  Schema.Tuple([Schema.Int, Schema.Unknown, Schema.Unknown]),
  Schema.Tuple([Schema.Literal("update_hook"), Schema.String, Schema.Number])
])
const isDatabaseActivityFrame = Schema.is(DatabaseActivityFrame)

interface EngineSnapshot {
  readonly runtime: ManagedRuntime.ManagedRuntime<EngineServices, unknown>
  readonly scope: Scope.Closeable
  readonly ownerId: string
  readonly info: unknown
  readonly databaseReplies: { count: number }
  readonly detachDatabaseObserver: () => void
}

interface EngineHealth {
  lastRoundTripAt: number
  lastProbeTickAt: number
  probeStartedAt: number | undefined
  warned: boolean
  observedReplies: number
}

interface LiveEngine extends EngineSnapshot {
  readonly epoch: number
  readonly provider: MessagePort
  readonly health: EngineHealth
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
  failures: number
  retryStep: ((input: void) => Pull.Pull<unknown, never, unknown>) | undefined
  retryGeneration: number
  backingOff: boolean
  exhausted: boolean
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
  | { readonly _tag: "HealthProbeSettled"; readonly epoch: number }
  | { readonly _tag: "HealthFailed"; readonly epoch: number }
  | {
    readonly _tag: "AttachVerified"
    readonly controlPort: MessagePort
    readonly epoch: number
    readonly ok: boolean
  }
  | { readonly _tag: "EngineDisposed"; readonly epoch: number }
  | { readonly _tag: "BackoffElapsed"; readonly generation: number }
  | { readonly _tag: "RetryExhausted"; readonly generation: number }

const defaultRetrySchedule: Schedule.Schedule<unknown, void> = Schedule.andThen(
  Schedule.recurs(1),
  Schedule.jittered(
    Schedule.modifyDelay(
      Schedule.exponential("500 millis"),
      ({ duration }) => Effect.succeed(Math.min(Duration.toMillis(duration), 30_000))
    )
  )
)

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
      const provisionDeadline = options.provisionDeadline ?? Effect.sleep
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
      const healthDeadlineMillis = Duration.toMillis(
        Duration.fromInputUnsafe(options.healthCheck?.deadline ?? "30 seconds")
      )
      const retrySchedule = options.provisionRetry?.schedule ?? defaultRetrySchedule
      const failureBudget = options.provisionRetry?.failureBudget ?? 5

      const events = yield* Queue.unbounded<CoordinatorEvent>()
      const state = yield* Ref.make<CoordinatorState>({
        clients: new Map(),
        engine: Option.none(),
        provisioning: Option.none(),
        starting: Option.none(),
        disposing: false,
        epoch: 0,
        tried: new Set(),
        failures: 0,
        retryStep: undefined,
        retryGeneration: 0,
        backingOff: false,
        exhausted: false
      })

      const post = (port: MessagePort, frame: OwnershipProtocol.OwnerToPageFrame) =>
        Schema.encodeUnknownEffect(OwnershipProtocol.OwnerToPageFrame)(frame).pipe(
          Effect.flatMap((encoded) =>
            Effect.try({
              try: () => port.postMessage(encoded),
              catch: () => undefined
            })
          ),
          Effect.catch(() => Effect.void)
        )

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
          Effect.tapCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) return Effect.void
            return postOwnerError(client.controlPort, Cause.pretty(cause))
          }),
          Effect.ensuring(
            Effect.try({
              try: () => rpcPort.close(),
              catch: () => undefined
            }).pipe(Effect.catch(() => Effect.void))
          ),
          Effect.forkIn(engine.scope),
          Effect.asVoid
        )
      }

      const provideRuntime = <A2, E2_,>(
        runtime: ManagedRuntime.ManagedRuntime<EngineServices, unknown>,
        effect: Effect.Effect<A2, E2_, EngineServices>
      ): Effect.Effect<A2, unknown> =>
        Effect.flatMap(runtime.contextEffect, (context) => Effect.provide(effect, context))

      const startEngine = (
        epoch: number,
        nonce: OwnershipProtocol.ProvisionNonce,
        candidate: MessagePort,
        databasePort: MessagePort
      ) => {
        const start = Effect.gen(function*() {
          // Database replies prove liveness while the connection permit blocks the probe. The
          // driver remains responsible for starting the port.
          const databaseReplies = { count: 0 }
          const runtime = yield* Effect.sync(() => options.engine(databasePort))
          const engineScope = yield* Scope.make()
          const onDatabaseReply = (event: MessageEvent<unknown>) => {
            if (isDatabaseActivityFrame(event.data)) databaseReplies.count++
          }
          databasePort.addEventListener("message", onDatabaseReply)
          const detachDatabaseObserver = () => databasePort.removeEventListener("message", onDatabaseReply)
          // A candidate that detaches mid start has this fiber interrupted, and the engine it
          // already built is reachable from nowhere else: it never became an `EngineStarted` event,
          // so `disposeEngine` can never see it.
          const discardEngine = (exit: Exit.Exit<unknown, unknown>) =>
            Scope.close(engineScope, exit).pipe(
              Effect.ignore,
              Effect.andThen(
                runtime.disposeEffect.pipe(Effect.timeout(engineDisposeTimeoutMillis), Effect.ignore)
              ),
              Effect.andThen(
                Effect.try({ try: detachDatabaseObserver, catch: () => undefined }).pipe(
                  Effect.catch(() => Effect.void)
                )
              ),
              Effect.andThen(
                Effect.try({ try: () => databasePort.close(), catch: () => undefined }).pipe(
                  Effect.catch(() => Effect.void)
                )
              )
            )
          const started = yield* Effect.exit(
            Effect.gen(function*() {
              yield* provideRuntime(runtime, healthProbe).pipe(
                Effect.timeoutOrElse({
                  duration: engineStartTimeoutMillis,
                  orElse: () => Effect.die(new Error("replica engine start timed out"))
                })
              )
              let info: unknown
              if (options.info !== undefined) {
                const value = yield* provideRuntime(runtime, options.info.make)
                info = yield* Schema.encodeEffect(options.info.schema)(value)
              }
              const ownerId = yield* Effect.orDie(crypto.randomUUIDv4)
              const snapshot: EngineSnapshot = {
                runtime,
                scope: engineScope,
                ownerId,
                info,
                databaseReplies,
                detachDatabaseObserver
              }
              return snapshot
            })
          ).pipe(Effect.onInterrupt(() => discardEngine(Exit.interrupt())))
          if (Exit.isFailure(started)) {
            yield* discardEngine(started)
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
          current.disposing || current.backingOff || current.exhausted
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
                Effect.suspend(() => provisionDeadline(Duration.millis(provisionTimeoutMillis))).pipe(
                  Effect.andThen(Queue.offer(events, { _tag: "ProvisionExpired", nonce })),
                  Effect.forkIn(layerScope)
                )
              )
            )
          }),
          Effect.asVoid
        )
      })

      // One schedule run spans a failure streak, so consecutive failures step the same run and
      // its delays grow; the step is discarded when a health probe passes, which starts the next
      // streak on a fresh run.
      const kickAfterBackoff: Effect.Effect<void> = Effect.suspend(() => {
        const current = Ref.getUnsafe(state)
        if (current.failures === 0) return kickProvisioning
        if (current.exhausted) return Effect.void
        let acquireStep: Effect.Effect<
          (input: void) => Pull.Pull<unknown, never, unknown>,
          unknown
        >
        if (current.retryStep !== undefined) {
          acquireStep = Effect.succeed(current.retryStep)
        } else {
          acquireStep = Schedule.toStepWithSleep(retrySchedule).pipe(
            Effect.tap((made) =>
              Effect.sync(() => {
                current.retryStep = made
              })
            )
          )
        }
        current.retryGeneration += 1
        const generation = current.retryGeneration
        current.backingOff = true
        return acquireStep.pipe(
          Effect.flatMap((step) =>
            Pull.matchEffect(step(undefined), {
              onSuccess: () => Queue.offer(events, { _tag: "BackoffElapsed", generation }),
              onDone: () => Queue.offer(events, { _tag: "RetryExhausted", generation }),
              onFailure: (cause) =>
                (() => {
                  if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
                  return Effect.logError("replica engine retry schedule failed", cause).pipe(
                    Effect.andThen(Queue.offer(events, { _tag: "RetryExhausted", generation }))
                  )
                })()
            })
          ),
          Effect.forkIn(layerScope),
          Effect.asVoid
        )
      })

      const noteEngineFailure = (candidate?: MessagePort): Effect.Effect<void> =>
        Effect.suspend(() => {
          const current = Ref.getUnsafe(state)
          current.failures += 1
          if (current.failures < failureBudget) return Effect.void
          const message =
            `the replica engine failed ${current.failures} consecutive times and keeps retrying with backoff`
          const reason = { _tag: "EngineProvisionFailing", failures: current.failures }
          const posts: Array<Effect.Effect<void>> = []
          if (candidate !== undefined) posts.push(postOwnerError(candidate, message, reason))
          for (const client of current.clients.values()) {
            if (client.controlPort !== candidate) posts.push(postOwnerError(client.controlPort, message, reason))
          }
          return Effect.forEach(posts, (effect) => effect, { discard: true })
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
          Effect.ignore,
          Effect.ensuring(
            Effect.try({ try: engine.detachDatabaseObserver, catch: () => undefined }).pipe(
              Effect.catch(() => Effect.void)
            )
          )
        )

      const resetEngine = (reason: string, mode?: { readonly failure: boolean }): Effect.Effect<void> =>
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
              // Reattach carries the reset cause because closing the RPC port loses it.
              if (mode?.failure === true) {
                reattach.push(post(client.controlPort, {
                  _tag: "Reattach",
                  ownerId: engine.ownerId,
                  reason
                }))
              } else {
                reattach.push(post(client.controlPort, {
                  _tag: "Reattach",
                  ownerId: engine.ownerId
                }))
              }
            }
            client.served = false
          }
          return Effect.logInfo("replica owner engine reset").pipe(
            Effect.annotateLogs({ reason, epoch: engine.epoch, ownerId: engine.ownerId }),
            Effect.andThen(Effect.forEach(reattach, (effect) => effect, { discard: true })),
            Effect.andThen(Effect.suspend(() => {
              if (mode?.failure === true) return noteEngineFailure()
              return Effect.void
            })),
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
                  if (current.exhausted) {
                    // An attach after exhaustion means the app reloaded or opened a new tab, so
                    // the retry schedule starts a new run.
                    current.exhausted = false
                    current.retryStep = undefined
                  }
                  if (Option.isNone(current.engine)) {
                    return kickProvisioning
                  }
                  // A tab is never attached to an unverified engine: the database worker is
                  // probed through the engine itself before this tab is served, so a dead
                  // database behind a live control channel cannot hand out a stale owner. The
                  // verification has no timeout of its own: a busy engine serves the tab late,
                  // and a dead one is reset by the periodic round-trip deadline.
                  const engine = current.engine.value
                  return provideRuntime(engine.runtime, healthProbe).pipe(
                    Effect.flatMap((result) =>
                      Queue.offer(events, {
                        _tag: "AttachVerified",
                        controlPort: event.controlPort,
                        epoch: engine.epoch,
                        ok: Option.isSome(result)
                      })
                    ),
                    Effect.catchCause((cause) => {
                      if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
                      return Queue.offer(events, {
                        _tag: "AttachVerified",
                        controlPort: event.controlPort,
                        epoch: engine.epoch,
                        ok: false
                      })
                    }),
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
                  if (wasCandidate) current.provisioning = Option.none()
                  if (wasProvider) {
                    return resetEngine("the database provider detached")
                  }
                  if (starting?.candidate === event.controlPort) {
                    current.starting = Option.none()
                    current.epoch += 1
                    return Fiber.interrupt(starting.fiber).pipe(
                      Effect.forkIn(layerScope),
                      Effect.andThen(kickProvisioning),
                      Effect.asVoid
                    )
                  }
                  if (wasCandidate) return kickProvisioning
                  return Effect.void
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
                  Effect.andThen(noteEngineFailure(event.candidate)),
                  Effect.andThen(kickAfterBackoff)
                )
              }
              if (!current.clients.has(event.candidate)) {
                return disposeEngine(event.exit.value, Exit.void).pipe(
                  Effect.andThen(kickProvisioning),
                  Effect.forkIn(layerScope),
                  Effect.asVoid
                )
              }
              const snapshot = event.exit.value
              return Effect.flatMap(Clock.currentTimeMillis, (now) => {
                const engine: LiveEngine = {
                  ...snapshot,
                  epoch: event.epoch,
                  provider: event.candidate,
                  // The start itself just proved a round trip, so the deadline counts from here.
                  health: {
                    lastRoundTripAt: now,
                    lastProbeTickAt: now,
                    probeStartedAt: undefined,
                    warned: false,
                    observedReplies: snapshot.databaseReplies.count
                  }
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
              })
            }
            case "HealthProbe": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              const engine = current.engine.value
              return Effect.flatMap(Clock.currentTimeMillis, (now) => {
                const health = engine.health
                // The deadline below reads the wall clock, but this handler runs on the same
                // dispatcher the browser throttles when every client of the worker is hidden —
                // in a worker each task hop is a setTimeout. When the gap since the previous
                // probe tick swallows the whole deadline, the coordinator itself was frozen
                // while the clock ran on, and that silence is evidence about the environment,
                // not the engine. Re-baseline and let a fresh round trip judge the engine.
                const sinceLastTick = now - health.lastProbeTickAt
                health.lastProbeTickAt = now
                if (sinceLastTick >= healthDeadlineMillis) {
                  health.lastRoundTripAt = now
                  if (health.probeStartedAt !== undefined) {
                    health.probeStartedAt = now
                    health.warned = false
                  }
                }
                // Database replies prove liveness while a long transaction blocks the queued probe.
                const replies = engine.databaseReplies.count
                if (replies !== health.observedReplies) {
                  health.observedReplies = replies
                  health.lastRoundTripAt = now
                }
                if (health.probeStartedAt !== undefined) {
                  // Probe age warns about backlog. Only complete database silence triggers takeover.
                  if (now - health.lastRoundTripAt >= healthDeadlineMillis) {
                    return Queue.offer(events, { _tag: "HealthFailed", epoch: engine.epoch })
                  }
                  if (!health.warned && now - health.probeStartedAt >= healthTimeoutMillis) {
                    health.warned = true
                    return Effect.logWarning("replica database round trip is delayed").pipe(
                      Effect.annotateLogs({
                        epoch: engine.epoch,
                        ownerId: engine.ownerId,
                        delayedMillis: now - health.probeStartedAt
                      })
                    )
                  }
                  return Effect.void
                }
                health.probeStartedAt = now
                health.warned = false
                return provideRuntime(engine.runtime, healthProbe).pipe(
                  Effect.flatMap((result) => {
                    if (Option.isSome(result)) {
                      return Queue.offer(events, { _tag: "HealthProbeSettled", epoch: engine.epoch })
                    }
                    return Queue.offer(events, { _tag: "HealthFailed", epoch: engine.epoch })
                  }),
                  // A failed or defecting database round trip means the database worker is gone or
                  // broken; both are takeover conditions. Interruption passes through untouched.
                  Effect.catchCause((cause) => {
                    if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
                    return Queue.offer(events, { _tag: "HealthFailed", epoch: engine.epoch })
                  }),
                  Effect.forkIn(layerScope),
                  Effect.asVoid
                )
              })
            }
            case "HealthProbeSettled": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              const engine = current.engine.value
              return Effect.flatMap(Clock.currentTimeMillis, (now) =>
                Effect.sync(() => {
                  engine.health.lastRoundTripAt = now
                  engine.health.probeStartedAt = undefined
                  // A settled periodic probe is the healthy signal that ends the failure streak.
                  // The start round trip must not: an engine that crashes right after starting
                  // would defeat the backoff.
                  current.failures = 0
                  current.retryStep = undefined
                }))
            }
            case "HealthFailed": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              return resetEngine("the database worker health check failed", { failure: true })
            }
            case "AttachVerified": {
              if (Option.isNone(current.engine) || current.engine.value.epoch !== event.epoch) {
                return Effect.void
              }
              if (!event.ok) {
                return resetEngine("the database worker health check failed", { failure: true })
              }
              const client = current.clients.get(event.controlPort)
              if (client === undefined || client.rpcPort === undefined) return Effect.void
              return serve(client, current.engine.value)
            }
            case "EngineDisposed": {
              if (event.epoch !== current.epoch) return Effect.void
              current.disposing = false
              current.tried.clear()
              return kickAfterBackoff
            }
            case "BackoffElapsed": {
              if (event.generation !== current.retryGeneration || !current.backingOff) return Effect.void
              current.backingOff = false
              return kickProvisioning
            }
            case "RetryExhausted": {
              if (event.generation !== current.retryGeneration || !current.backingOff) return Effect.void
              current.backingOff = false
              current.exhausted = true
              const message =
                `the replica engine failed ${current.failures} consecutive times and the retry schedule is exhausted; provisioning is stopped until a tab attaches again`
              const reason = { _tag: "EngineProvisionExhausted", failures: current.failures }
              const posts: Array<Effect.Effect<void>> = []
              for (const client of current.clients.values()) {
                posts.push(postOwnerError(client.controlPort, message, reason))
              }
              return Effect.logWarning("replica engine provisioning exhausted").pipe(
                Effect.annotateLogs({ failures: current.failures }),
                Effect.andThen(Effect.forEach(posts, (effect) => effect, { discard: true }))
              )
            }
          }
          return Effect.void
        })

      yield* Queue.take(events).pipe(
        Effect.flatMap(handle),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
          return Effect.logError("ownership coordinator event failed", cause)
        }),
        Effect.forever,
        Effect.forkIn(layerScope)
      )

      return {
        attach: (controlPort) =>
          Effect.sync(() => {
            controlPort.addEventListener("message", (event: MessageEvent<unknown>) => {
              let frame: OwnershipProtocol.PageToOwnerFrame
              const decoded = Schema.decodeUnknownExit(OwnershipProtocol.PageToOwnerFrame)(event.data)
              if (Exit.isFailure(decoded)) {
                const response = Schema.encodeUnknownExit(OwnershipProtocol.OwnerToPageFrame)({
                  _tag: "OwnerError",
                  message: `undecodable ownership frame: ${String(decoded.cause)}`.slice(0, 512)
                })
                if (Exit.isSuccess(response)) safePostMessage(controlPort, response.value)
                return
              }
              frame = decoded.value
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
  const first: unknown = runSyncSafe(() => Reflect.get(ports, 0), undefined)
  if (ReplicaRpc.isMessagePort(first)) return first
  return undefined
}

/**
 * SharedWorker entry point. The `connect` listener is registered synchronously and every port is
 * buffered until the coordinator is ready, so tabs connecting while the options module is still
 * loading are attached in order once it arrives. When the load fails, every buffered and future
 * connection is answered with an `OwnerError` frame whose reason is tagged `RuntimeLoadFailure`.
 * Entries that dynamically import this module can pass ports captured before that import as
 * `initialPorts`.
 */
export const runSharedWorker = <E, A = unknown, E2 = never,>(
  load: () => Promise<SharedWorkerOptions<E, A, E2>>,
  initialPorts: ReadonlyArray<MessagePort> = []
): void => {
  const pending = [...initialPorts]
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
        const response = Schema.encodeUnknownExit(OwnershipProtocol.OwnerToPageFrame)({
          _tag: "OwnerError",
          message: failure.message,
          reason: { _tag: "RuntimeLoadFailure", message: failure.message, name: failure.name }
        })
        if (Exit.isSuccess(response)) safePostMessage(controlPort, response.value)
        safeCall(() => controlPort.close())
      }
    }
  }
  let coordinatorRuntime: ManagedRuntime.ManagedRuntime<OwnershipCoordinator, unknown> | undefined
  const describe = (cause: unknown) => {
    if (cause instanceof Error) return { message: cause.message, name: cause.name }
    return { message: String(cause), name: "UnknownError" }
  }
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
  options: TabOptions
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
          safeCall(() => port.dispatchEvent(new MessageEvent("message", { data: [0] })))
          safeCall(() => port.dispatchEvent(new ErrorEvent("error", { message })))
        }
      }

      const postFrame = (current: Connection, frame: OwnershipProtocol.PageToOwnerFrame) => {
        runSyncSafe(() => {
          const encoded = Schema.encodeSync(OwnershipProtocol.PageToOwnerFrame)(frame)
          safePostMessage(current.controlPort, encoded, OwnershipProtocol.transferOf(frame))
        }, undefined)
      }

      const teardown = (current: Connection, message: string | undefined) => {
        if (connection !== current) return
        connection = undefined
        // The rpc ports are failed before anything is closed: events dispatched on a closed port
        // never reach the worker protocol's listeners, and the connection must already read as
        // cleared when the protocol reacts and reconnects.
        if (message !== undefined) failRpcPorts(current, message)
        safeCall(() => current.controlPort.removeEventListener("message", current.onMessage))
        safeCall(() => current.worker.removeEventListener("error", current.onWorkerError))
        postFrame(current, { _tag: "Detach" })
        safeCall(() => current.controlPort.close())
        safeCall(() => current.provisioning?.worker.terminate())
        current.provisioning = undefined
        safeCall(() => current.databaseWorker?.terminate())
        current.databaseWorker = undefined
        for (const port of current.rpcPorts.values()) safeCall(() => port.close())
        current.rpcPorts.clear()
      }

      const handleFrame = (current: Connection, frame: OwnershipProtocol.OwnerToPageFrame) => {
        switch (frame._tag) {
          case "Attached": {
            let info: unknown
            if (options.infoSchema === undefined) {
              info = frame.info
            } else {
              const decoded = Schema.decodeUnknownExit(options.infoSchema)(frame.info)
              if (Exit.isFailure(decoded)) {
                options.onOwnerError?.(`undecodable ownership info: ${String(decoded.cause)}`.slice(0, 512))
                return
              }
              info = decoded.value
            }
            if (!frame.provider && current.provisioning === undefined && current.databaseWorker !== undefined) {
              // The current engine is provided by another tab, so any database worker this tab
              // still runs belongs to a dead engine epoch.
              safeCall(() => current.databaseWorker?.terminate())
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
            safeCall(() => current.databaseWorker?.terminate())
            current.databaseWorker = undefined
            const databaseExit = Effect.runSyncExit(Effect.try({
              try: options.databaseWorker,
              catch: (cause) => cause
            }))
            if (Exit.isFailure(databaseExit)) {
              options.onOwnerError?.(
                `replica database worker failed to start: ${String(databaseExit.cause)}`.slice(0, 512)
              )
              return
            }
            const database = databaseExit.value
            const channel = new MessageChannel()
            const posted = runSyncSafe(() => {
              const encoded = Schema.encodeSync(OwnershipProtocol.DatabaseWorkerStart)({
                _tag: "DatabaseWorkerStart",
                databasePort: channel.port1
              })
              database.postMessage(encoded, [channel.port1])
              return true
            }, false)
            if (!posted) {
              safeCall(() => channel.port1.close())
              safeCall(() => channel.port2.close())
              safeCall(() => database.terminate())
              options.onOwnerError?.("replica database worker failed to receive its start frame")
              return
            }
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
              const worker = current.provisioning.worker
              current.provisioning = undefined
              current.databaseWorker = undefined
              safeCall(() => worker.terminate())
              failRpcPorts(current, "replica provisioning was rejected")
            }
            return
          }
          case "Reattach": {
            // Fail stale RPC ports before reporting so a consumer callback cannot block recovery.
            failRpcPorts(current, `replica ownership moved from ${frame.ownerId}`)
            if (frame.reason !== undefined) {
              options.onOwnerError?.(`the replica owner engine was reset: ${frame.reason}`, {
                _tag: "EngineReset",
                reason: frame.reason
              })
            }
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
            const decoded = Schema.decodeUnknownExit(OwnershipProtocol.OwnerToPageFrame)(event.data)
            if (Exit.isFailure(decoded)) {
              options.onOwnerError?.(`undecodable ownership frame: ${String(decoded.cause)}`.slice(0, 512))
              return
            }
            handleFrame(current, decoded.value)
          },
          onWorkerError: (event: Event) => {
            if (connection !== current) return
            let message = "replica owner worker failed"
            if (event instanceof ErrorEvent) message = event.message
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
        if (closed) return Effect.runSync(Effect.die(new Error("the ownership tab connection is closed")))
        const current = connection ?? establish()
        safeCall(() => current.rpcPorts.get(id)?.close())
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
        Effect.runPromise(
          Effect.onExit(effect, () => {
            resume(Effect.void)
            return Effect.void
          })
        )).catch((defect) => resume(Effect.die(defect)))
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
 * instead of opening OPFS concurrently with a slow prior owner. An entry that dynamically imports
 * this module can pass the message captured before that import as `initialMessage`.
 */
export const runDatabaseWorker = (
  options: DatabaseWorkerOptions,
  initialMessage?: MessageEvent<unknown>
): void => {
  const dbName = options.dbName ?? `${options.name}.sqlite`
  const lockName = options.lockName ?? `${options.name}-opfs`
  const start = (event: MessageEvent<unknown>) => {
    const decoded = Schema.decodeUnknownExit(OwnershipProtocol.DatabaseWorkerStart)(event.data)
    if (Exit.isFailure(decoded)) {
      for (const port of event.ports) safeCall(() => port.close())
      globalThis.reportError(decoded.cause)
      return
    }
    const frame = decoded.value
    frame.databasePort.start()
    Effect.runFork(
      withDatabaseLock(lockName, OpfsWorker.run({ port: frame.databasePort, dbName })).pipe(
        Effect.tapCause(Effect.logError)
      )
    )
  }
  if (initialMessage !== undefined) {
    start(initialMessage)
    return
  }
  globalThis.addEventListener("message", start, { once: true })
}
