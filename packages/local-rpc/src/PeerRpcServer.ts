import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as PeerRpcObservability from "./internal/peerRpcObservability.js"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerRelayAuthorization from "./PeerRelayAuthorization.js"
import * as PeerRelayIngress from "./PeerRelayIngress.js"
import * as PeerRelayLimits from "./PeerRelayLimits.js"
import * as PeerRelayStore from "./PeerRelayStore.js"
import * as PeerRpc from "./PeerRpc.js"
import * as PeerRpcError from "./PeerRpcError.js"

type RelaySqlLane = "Admission" | "Terminal" | "Delivery" | "Maintenance"

interface RelaySqlTask {
  state: "Queued" | "Acquired" | "Cancelled" | "Completed"
  readonly cancel: Deferred.Deferred<void>
  readonly cancelResult: () => void
  readonly execute: Effect.Effect<void>
}

interface RelaySqlScheduler {
  readonly submit: <A, E,>(
    lane: RelaySqlLane,
    effect: Effect.Effect<A, E>
  ) => Effect.Effect<A, E | PeerRpcError.RequestCapacityExceeded>
  readonly shutdown: Effect.Effect<void>
}

const drainRelayQueue = <A, E,>(
  queue: Queue.Dequeue<A, E>
): Effect.Effect<Array<A>> =>
  Effect.gen(function*() {
    const items: Array<A> = []
    while (true) {
      const item = yield* Queue.poll(queue)
      if (Option.isNone(item)) return items
      items.push(item.value)
    }
  })

const makeRelaySqlScheduler = (
  limits: PeerRelayLimits.Values,
  runtimeScope: Scope.Scope,
  onFatal: (cause: Cause.Cause<unknown>) => Effect.Effect<void>
): Effect.Effect<RelaySqlScheduler> =>
  Effect.gen(function*() {
    const capacities: Record<RelaySqlLane, number> = {
      Admission: limits.sqlAdmissionQueueCapacity,
      Terminal: limits.sqlTerminalQueueCapacity,
      Delivery: limits.sqlDeliveryQueueCapacity,
      Maintenance: limits.sqlMaintenanceQueueCapacity
    }
    const queues = {
      Admission: yield* Queue.dropping<RelaySqlTask, Cause.Done>(capacities.Admission),
      Terminal: yield* Queue.dropping<RelaySqlTask, Cause.Done>(capacities.Terminal),
      Delivery: yield* Queue.dropping<RelaySqlTask, Cause.Done>(capacities.Delivery),
      Maintenance: yield* Queue.dropping<RelaySqlTask, Cause.Done>(capacities.Maintenance)
    } satisfies Record<RelaySqlLane, Queue.Queue<RelaySqlTask, Cause.Done>>
    const lock = yield* Semaphore.make(1)
    const globalPermits = yield* Semaphore.make(limits.maxInFlightSqlTransactions)
    const laneConcurrency = {
      Admission: limits.maxInFlightSqlAdmission,
      Terminal: limits.maxInFlightSqlTerminal,
      Delivery: limits.maxInFlightSqlDelivery,
      Maintenance: limits.maxInFlightSqlMaintenance
    } satisfies Record<RelaySqlLane, number>
    const activeTasks = new Set<RelaySqlTask>()
    let accepting = true

    const cancelTask = (task: RelaySqlTask) =>
      lock.withPermit(Effect.sync(() => {
        if (task.state === "Completed" || task.state === "Cancelled") return
        task.state = "Cancelled"
        Deferred.doneUnsafe(task.cancel, Effect.void)
        task.cancelResult()
        activeTasks.delete(task)
      }))

    const submit: RelaySqlScheduler["submit"] = (lane, effect) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function*() {
          const result = yield* Deferred.make<Effect.Success<typeof effect>, Effect.Error<typeof effect>>()
          const cancel = yield* Deferred.make<void>()
          const task: RelaySqlTask = {
            state: "Queued",
            cancel,
            cancelResult: () => {
              Deferred.doneUnsafe(result, Effect.interrupt)
            },
            execute: Effect.uninterruptibleMask((restoreTask) =>
              lock.withPermit(Effect.sync(() => {
                if (task.state !== "Queued") return false
                task.state = "Acquired"
                return true
              })).pipe(
                Effect.flatMap((acquired) => {
                  if (!acquired) return Effect.void
                  const cancelled = Deferred.await(cancel).pipe(Effect.flatMap(() => Effect.interrupt))
                  return Effect.raceFirst(restoreTask(effect), cancelled).pipe(
                    Effect.exit,
                    Effect.flatMap((exit) => Deferred.done(result, exit)),
                    Effect.ensuring(lock.withPermit(Effect.sync(() => {
                      task.state = "Completed"
                      activeTasks.delete(task)
                    })))
                  )
                })
              )
            )
          }
          const offered = yield* lock.withPermit(
            Effect.suspend(() => {
              if (!accepting) return Effect.succeed(false)
              return Queue.offer(queues[lane], task).pipe(
                Effect.tap((offered) =>
                  Effect.sync(() => {
                    if (offered) activeTasks.add(task)
                  })
                )
              )
            })
          )
          if (!offered) return yield* new PeerRpcError.RequestCapacityExceeded()
          return yield* restore(Deferred.await(result)).pipe(
            Effect.onInterrupt(() => cancelTask(task))
          )
        })
      )

    const workerFibers: Array<Fiber.Fiber<unknown, unknown>> = []
    for (const lane of Object.keys(queues) as Array<RelaySqlLane>) {
      const worker = Effect.forever(
        Queue.take(queues[lane]).pipe(
          Effect.flatMap((task) => task.execute.pipe(globalPermits.withPermits(1)))
        )
      )
      for (let index = 0; index < laneConcurrency[lane]; index++) {
        const fiber = yield* Effect.forkIn(worker, runtimeScope)
        workerFibers.push(fiber)
        yield* Effect.forkIn(
          Fiber.await(fiber).pipe(
            Effect.flatMap((exit) =>
              lock.withPermit(Effect.sync(() => accepting)).pipe(
                Effect.flatMap((wasAccepting) =>
                  wasAccepting && Exit.isFailure(exit)
                    ? onFatal(exit.cause)
                    : Effect.void
                )
              )
            )
          ),
          runtimeScope
        )
      }
    }

    const shutdown = Effect.uninterruptible(
      lock.withPermit(Effect.sync(() => {
        accepting = false
        return [...activeTasks]
      })).pipe(
        Effect.flatMap((tasks) => Effect.forEach(tasks, cancelTask, { discard: true })),
        Effect.andThen(Effect.forEach(
          Object.values(queues),
          (queue) => Queue.shutdown(queue),
          { discard: true }
        )),
        Effect.andThen(Effect.forEach(workerFibers, Fiber.interrupt, { discard: true }))
      )
    ).pipe(Effect.ignore)

    return { submit, shutdown }
  })

interface RelaySubjectState {
  tokens: number
  updatedAt: number
  lastUsedAt: number
  inFlight: number
}

interface RelayAdmissionLane {
  readonly run: <A, E, R,>(
    subjectId: string,
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | PeerRpcError.RequestCapacityExceeded, R>
  readonly clear: Effect.Effect<void>
}

const makeRelayAdmissionLane = (options: {
  readonly maximumInFlight: number
  readonly maximumInFlightPerSubject: number
  readonly ratePerSecond: number
  readonly burst: number
  readonly maximumSubjects: number
  readonly idleRetentionMillis: number
}): Effect.Effect<RelayAdmissionLane> =>
  Effect.gen(function*() {
    const lock = yield* Semaphore.make(1)
    const permits = yield* Semaphore.make(options.maximumInFlight)
    const subjects = new Map<string, RelaySubjectState>()
    const inactive = new Map<string, RelaySubjectState>()

    const removeExpired = (now: number) => {
      while (inactive.size > 0) {
        const oldest = inactive.entries().next().value!
        if (now - oldest[1].lastUsedAt < options.idleRetentionMillis) return
        inactive.delete(oldest[0])
        subjects.delete(oldest[0])
      }
    }

    const admit = (subjectId: string, now: number) =>
      lock.withPermit(Effect.sync(() => {
        removeExpired(now)
        let state = subjects.get(subjectId)
        if (state?.inFlight === 0) inactive.delete(subjectId)
        if (state === undefined) {
          while (subjects.size >= options.maximumSubjects) {
            const evictable = inactive.entries().next().value
            if (evictable === undefined) return false
            inactive.delete(evictable[0])
            subjects.delete(evictable[0])
          }
          state = {
            tokens: options.burst,
            updatedAt: now,
            lastUsedAt: now,
            inFlight: 0
          }
          subjects.set(subjectId, state)
        }
        const effectiveNow = Math.max(now, state.updatedAt, state.lastUsedAt)
        state.tokens = Math.min(
          options.burst,
          state.tokens + ((effectiveNow - state.updatedAt) / 1_000) * options.ratePerSecond
        )
        state.updatedAt = effectiveNow
        state.lastUsedAt = effectiveNow
        if (state.inFlight >= options.maximumInFlightPerSubject || state.tokens < 1) {
          if (state.inFlight === 0) inactive.set(subjectId, state)
          return false
        }
        state.tokens -= 1
        state.inFlight += 1
        return true
      }))

    const release = (subjectId: string) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          lock.withPermit(Effect.sync(() => {
            const state = subjects.get(subjectId)
            if (state === undefined) return
            state.inFlight -= 1
            state.lastUsedAt = Math.max(now, state.lastUsedAt)
            if (state.inFlight === 0) {
              inactive.delete(subjectId)
              inactive.set(subjectId, state)
            }
          }))
        )
      )

    const run: RelayAdmissionLane["run"] = (subjectId, effect) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function*() {
          const admittedAt = yield* Clock.currentTimeMillis
          if (!(yield* admit(subjectId, admittedAt))) {
            return yield* new PeerRpcError.RequestCapacityExceeded()
          }
          return yield* restore(
            effect.pipe(
              permits.withPermitsIfAvailable(1),
              Effect.flatMap(Effect.fromOption(() => new PeerRpcError.RequestCapacityExceeded()))
            )
          ).pipe(Effect.ensuring(release(subjectId)))
        })
      )

    return {
      run,
      clear: lock.withPermit(Effect.sync(() => {
        subjects.clear()
        inactive.clear()
      }))
    }
  })

export interface PeerRpcServerUsage {
  readonly accepting: boolean
  readonly sessions: number
  readonly subjects: number
  readonly activeClaims: number
  readonly queuedChannels: number
}

export class PeerRpcServerRuntime extends Context.Service<PeerRpcServerRuntime, {
  readonly health: Effect.Effect<void, PeerRpcError.ServerUnavailable>
  readonly owner: Effect.Effect<never, unknown>
  readonly shutdown: Effect.Effect<void>
  readonly usage: Effect.Effect<PeerRpcServerUsage>
}>()("@lucas-barake/effect-local-rpc/PeerRpcServerRuntime") {}

class PeerRpcFatalSignal extends Context.Service<PeerRpcFatalSignal, {
  readonly signal: (cause: Cause.Cause<unknown>) => Effect.Effect<void>
}>()("@lucas-barake/effect-local-rpc/PeerRpcFatalSignal") {}

interface RelayWorkSelector {
  readonly recipient: {
    readonly tenantId: string
    readonly subjectId: string
    readonly peerId: Identity.PeerId
  }
  readonly sender: {
    readonly subjectId: string
    readonly peerId: Identity.PeerId
  }
}

interface RelayOutboundItem {
  readonly event: PeerRpc.StoredMessage
  readonly reservation: PeerRelayIngress.Reservation
  transferred: boolean
}

interface RelayEntry {
  readonly sessionId: Identity.SessionId
  readonly generation: number
  readonly principal: PeerAuthentication.PeerPrincipal
  readonly senderReplicaIncarnation: Identity.ReplicaIncarnation
  readonly remote: PeerRelayAuthorization.RemotePeer
  readonly documents: ReadonlyArray<PeerRpc.RequestedDocument>
  readonly receiptRetentionMillis: number
  readonly senderRetryHorizonMillis: number
  readonly outbound: Queue.Queue<RelayOutboundItem, PeerRpcError.PeerRpcError | Cause.Done>
  readonly claims: Map<Identity.RelayMessageId, PeerRelayStore.ClaimedMessage>
  readonly revoked: Deferred.Deferred<void>
  readonly monitorFibers: Array<Fiber.Fiber<void, unknown>>
  watcher: Fiber.Fiber<void, unknown> | undefined
  active: boolean
}

type RelayWorkOwner =
  | {
    readonly _tag: "Worker"
    readonly lane: "New" | "Retry"
    pending: boolean
  }
  | {
    readonly _tag: "Entry"
    readonly generation: number
    pending: boolean
  }
  | {
    readonly _tag: "Terminal"
  }

const relaySelectorKey = (selector: RelayWorkSelector) =>
  JSON.stringify([
    selector.recipient.tenantId,
    selector.recipient.subjectId,
    selector.recipient.peerId,
    selector.sender.subjectId,
    selector.sender.peerId
  ])

const relayEntryKey = (
  principal: PeerAuthentication.PeerPrincipal,
  remote: PeerRelayAuthorization.RemotePeer
) =>
  JSON.stringify([
    principal.tenantId,
    principal.subjectId,
    principal.peerId,
    remote.subjectId,
    remote.peerId
  ])

const relayIncarnationKey = (
  principal: PeerAuthentication.PeerPrincipal,
  incarnation: Identity.ReplicaIncarnation,
  remote: PeerRelayAuthorization.RemotePeer
) =>
  JSON.stringify([
    principal.tenantId,
    principal.subjectId,
    principal.peerId,
    incarnation,
    remote.subjectId,
    remote.peerId
  ])

const relaySelectorForEntry = (entry: RelayEntry): RelayWorkSelector => ({
  recipient: entry.principal,
  sender: entry.remote
})

const sameRelayPrincipal = (
  left: PeerAuthentication.PeerPrincipal,
  right: PeerAuthentication.PeerPrincipal
) =>
  left.tenantId === right.tenantId &&
  left.subjectId === right.subjectId &&
  left.peerId === right.peerId

const relayStoreFailure = (error: PeerRelayStore.StoreError): PeerRpcError.PeerRpcError => {
  if (error._tag !== "ReplicaError") {
    return new PeerRpcError.ServerUnavailable()
  }
  switch (error.reason._tag) {
    case "QuotaExceeded":
      return new PeerRpcError.RequestCapacityExceeded()
    case "ProtocolMismatch":
      return new PeerRpcError.InvalidRequest()
    default:
      return new PeerRpcError.ServerUnavailable()
  }
}

const RelaySyncEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

const decodeRelayEnvelope = (payload: Uint8Array) =>
  Schema.decodeUnknownEffect(RelaySyncEnvelopeJson)(new TextDecoder().decode(payload)).pipe(
    Effect.mapError(() => new PeerRpcError.InvalidRequest())
  )

export const layerHandlers = (
  options: {
    readonly tenantId: string
    readonly peerId: Identity.PeerId
  }
) =>
  Layer.effectContext(Effect.gen(function*() {
    const serverScope = yield* Scope.Scope
    const runtimeScope = yield* Scope.fork(serverScope, "sequential")
    const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
    const store = yield* PeerRelayStore.PeerRelayStore
    const limits = yield* PeerRelayLimits.PeerRelayLimits
    const ingress = yield* PeerRelayIngress.PeerRelayIngress
    yield* Crypto.Crypto
    const lock = yield* Semaphore.make(1)
    const fatal = yield* Deferred.make<never, unknown>()
    const shutdownStarted = yield* Deferred.make<void>()
    const shutdownFinished = yield* Deferred.make<void>()
    const sessions = new Map<Identity.SessionId, RelayEntry>()
    const endpoints = new Map<string, Identity.SessionId>()
    const incarnations = new Map<string, Identity.SessionId>()
    const subjectSessions = new Map<string, number>()
    const workOwners = new Map<string, RelayWorkOwner>()
    const selectors = new Map<string, RelayWorkSelector>()
    const newWork = yield* Queue.dropping<RelayWorkSelector, Cause.Done>(limits.newWorkQueueCapacity)
    const retryWork = yield* Queue.dropping<RelayWorkSelector, Cause.Done>(limits.retryQueueCapacity)
    const newWorkLock = yield* Semaphore.make(1)
    const retryWorkLock = yield* Semaphore.make(1)
    const workWake = yield* Queue.dropping<void, Cause.Done>(
      limits.newWorkQueueCapacity + limits.retryQueueCapacity
    )
    let accepting = true
    let generation = 0
    let compensationCursor = 0

    const signalFatal = (cause: Cause.Cause<unknown>) => Deferred.done(fatal, Exit.failCause(cause)).pipe(Effect.asVoid)

    const sql = yield* makeRelaySqlScheduler(limits, runtimeScope, signalFatal)
    const openLane = yield* makeRelayAdmissionLane({
      maximumInFlight: limits.maxInFlightOpen,
      maximumInFlightPerSubject: limits.maxInFlightOpenPerSubject,
      ratePerSecond: limits.openRatePerSecond,
      burst: limits.openBurst,
      maximumSubjects: limits.maxRetainedRateLimitedSubjects,
      idleRetentionMillis: limits.rateLimitIdleRetentionMillis
    })
    const pushLane = yield* makeRelayAdmissionLane({
      maximumInFlight: limits.maxInFlightPush,
      maximumInFlightPerSubject: limits.maxInFlightPushPerSubject,
      ratePerSecond: limits.admissionRatePerSecond,
      burst: limits.admissionBurst,
      maximumSubjects: limits.maxRetainedRateLimitedSubjects,
      idleRetentionMillis: limits.rateLimitIdleRetentionMillis
    })
    const terminalLane = yield* makeRelayAdmissionLane({
      maximumInFlight: limits.maxInFlightTerminalResponses,
      maximumInFlightPerSubject: limits.maxInFlightTerminalResponsesPerSubject,
      ratePerSecond: limits.terminalResponseRatePerSecond,
      burst: limits.terminalResponseBurst,
      maximumSubjects: limits.maxRetainedTerminalResponseSubjects,
      idleRetentionMillis: limits.terminalResponseSubjectIdleRetentionMillis
    })

    const storeEffect = <A,>(effect: Effect.Effect<A, PeerRelayStore.StoreError>) =>
      effect.pipe(Effect.mapError(relayStoreFailure))

    const activeClaimCount = () => [...sessions.values()].reduce((sum, entry) => sum + entry.claims.size, 0)

    const refreshRelayPending = sql.submit(
      "Maintenance",
      storeEffect(store.usage())
    ).pipe(
      Effect.flatMap((usage) => PeerRpcObservability.setRelayPending(usage.activeCount, usage.activeBytes))
    )

    yield* refreshRelayPending
    yield* PeerRpcObservability.setRelayActiveClaims(0)
    yield* PeerRpcObservability.setRelayWorkers(0)
    yield* PeerRpcObservability.setRelayReadyQueueItems("New", 0)
    yield* PeerRpcObservability.setRelayReadyQueueItems("Retry", 0)

    const releaseClaim = (
      entry: RelayEntry,
      claim: PeerRelayStore.ClaimedMessage
    ) =>
      sql.submit(
        "Terminal",
        storeEffect(store.release({
          channel: claim.channel,
          relayMessageId: claim.relayMessageId,
          claimToken: claim.claimToken,
          sessionGeneration: entry.generation
        }))
      ).pipe(
        Effect.timeoutOrElse({
          duration: limits.shutdownReleaseTimeoutMillis,
          orElse: () => Effect.succeed(undefined)
        }),
        Effect.ignore
      )

    const detachEntry = (
      entry: RelayEntry,
      fromWatcher: boolean
    ): Effect.Effect<void> =>
      Effect.uninterruptible(Effect.gen(function*() {
        yield* Deferred.succeed(entry.revoked, undefined)
        const cleanup = yield* lock.withPermit(Effect.gen(function*() {
          if (!entry.active) return undefined
          entry.active = false
          sessions.delete(entry.sessionId)
          const endpoint = relayEntryKey(entry.principal, entry.remote)
          if (endpoints.get(endpoint) === entry.sessionId) endpoints.delete(endpoint)
          const incarnation = relayIncarnationKey(
            entry.principal,
            entry.senderReplicaIncarnation,
            entry.remote
          )
          if (incarnations.get(incarnation) === entry.sessionId) incarnations.delete(incarnation)
          const current = subjectSessions.get(entry.principal.subjectId) ?? 0
          if (current <= 1) subjectSessions.delete(entry.principal.subjectId)
          else subjectSessions.set(entry.principal.subjectId, current - 1)
          const selector = relaySelectorForEntry(entry)
          const key = relaySelectorKey(selector)
          const owner = workOwners.get(key)
          if (owner?._tag === "Entry" && owner.generation === entry.generation) {
            workOwners.delete(key)
          }
          if (endpoints.get(endpoint) === undefined) {
            selectors.delete(key)
          }
          const claims = [...entry.claims.values()]
          entry.claims.clear()
          yield* PeerRpcObservability.setRelayActiveClaims(activeClaimCount())
          return { claims }
        }))
        if (cleanup === undefined) return
        const buffered = yield* drainRelayQueue(entry.outbound)
        yield* Effect.forEach(buffered, (item) => item.reservation.release, {
          concurrency: 1,
          discard: true
        })
        yield* Queue.fail(entry.outbound, new PeerRpcError.SessionUnavailable())
        yield* Queue.shutdown(entry.outbound)
        yield* Effect.forEach(
          cleanup.claims,
          (claim) => releaseClaim(entry, claim),
          { concurrency: limits.shutdownReleaseConcurrency, discard: true }
        )
        const watcherFibers = fromWatcher || entry.watcher === undefined
          ? entry.monitorFibers
          : [...entry.monitorFibers, entry.watcher]
        yield* Effect.forEach(
          watcherFibers,
          (fiber) => Effect.forkIn(Fiber.interrupt(fiber), runtimeScope),
          { discard: true }
        )
      }))

    const notify = (
      selector: RelayWorkSelector,
      lane: "New" | "Retry"
    ): Effect.Effect<void> =>
      Effect.gen(function*() {
        const key = relaySelectorKey(selector)
        const shouldOffer = yield* lock.withPermit(Effect.sync(() => {
          if (!accepting) return false
          if (
            endpoints.get(relayEntryKey(selector.recipient, selector.sender)) === undefined
          ) {
            return false
          }
          selectors.set(key, selector)
          const current = workOwners.get(key)
          if (current === undefined || current._tag === "Terminal") {
            workOwners.set(key, { _tag: "Worker", lane, pending: false })
            return true
          }
          current.pending = true
          return false
        }))
        if (!shouldOffer) return
        const queue = lane === "New" ? newWork : retryWork
        const queueLock = lane === "New" ? newWorkLock : retryWorkLock
        const offered = yield* queueLock.withPermit(Effect.gen(function*() {
          const offered = yield* Queue.offer(queue, selector)
          if (offered) {
            const size = yield* Queue.size(queue)
            yield* PeerRpcObservability.setRelayReadyQueueItems(lane, size)
          }
          return offered
        }))
        if (!offered) {
          yield* lock.withPermit(Effect.sync(() => {
            const current = workOwners.get(key)
            if (current?._tag === "Worker" && current.lane === lane) {
              workOwners.delete(key)
            }
          }))
          return
        }
        yield* Queue.offer(workWake, undefined)
      })

    const ensureCurrentEntry = (entry: RelayEntry) =>
      lock.withPermit(Effect.gen(function*() {
        const revoked = yield* Deferred.poll(entry.revoked)
        if (
          Option.isSome(revoked) ||
          !entry.active ||
          sessions.get(entry.sessionId) !== entry ||
          endpoints.get(relayEntryKey(entry.principal, entry.remote)) !== entry.sessionId ||
          incarnations.get(
              relayIncarnationKey(
                entry.principal,
                entry.senderReplicaIncarnation,
                entry.remote
              )
            ) !== entry.sessionId
        ) {
          return yield* new PeerRpcError.SessionUnavailable()
        }
        return entry
      }))

    const freshEntry = (
      sessionId: Identity.SessionId,
      authenticated: Context.Service.Shape<typeof PeerAuthentication.AuthenticatedPeer>
    ) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        if (authenticated.validUntil <= now) {
          return yield* new PeerRpcError.SessionUnavailable()
        }
        const entry = yield* lock.withPermit(Effect.sync(() => sessions.get(sessionId)))
        if (
          entry === undefined ||
          !sameRelayPrincipal(entry.principal, authenticated.principal)
        ) {
          return yield* new PeerRpcError.SessionUnavailable()
        }
        return yield* ensureCurrentEntry(entry)
      })

    const raceRevocation = <A, E, R,>(
      entry: RelayEntry,
      effect: Effect.Effect<A, E, R>
    ) =>
      Effect.raceFirst(
        effect,
        Deferred.await(entry.revoked).pipe(
          Effect.andThen(Effect.fail(new PeerRpcError.SessionUnavailable()))
        )
      )

    const authorizeEntry = (
      entry: RelayEntry,
      direction: PeerRelayAuthorization.Direction,
      documents: ReadonlyArray<PeerRpc.RequestedDocument>
    ) =>
      authorization.authorize({
        direction,
        principal: entry.principal,
        remote: entry.remote,
        documents
      })

    const withRelayGrantAdmission = <A, E, R,>(
      entry: RelayEntry,
      grants: ReadonlyArray<{
        readonly validUntil: number
        readonly invalidated: Effect.Effect<unknown, unknown>
      }>,
      effect: Effect.Effect<A, E, R>
    ) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const validUntil = Math.min(...grants.map((grant) => grant.validUntil))
        if (validUntil <= now) {
          return yield* new PeerRpcError.AccessDenied()
        }
        const admissionGate = yield* Semaphore.make(1)
        let admitted = false
        let invalidated = false
        const invalidate = admissionGate.withPermit(
          Effect.sync(() => {
            if (!admitted) invalidated = true
          })
        )
        const enter = admissionGate.withPermit(
          Effect.gen(function*() {
            const currentTime = yield* Clock.currentTimeMillis
            if (invalidated || validUntil <= currentTime) return false
            admitted = true
            return true
          })
        ).pipe(
          Effect.flatMap((entered) =>
            entered
              ? Effect.void
              : Effect.fail(new PeerRpcError.AccessDenied())
          )
        )
        return yield* Effect.acquireUseRelease(
          Effect.forEach(
            [
              ...grants.map((grant) => grant.invalidated),
              Deferred.await(entry.revoked),
              Effect.sleep(Math.max(0, validUntil - now))
            ],
            (monitor) =>
              monitor.pipe(
                Effect.exit,
                Effect.andThen(invalidate),
                Effect.forkIn(runtimeScope)
              )
          ),
          () =>
            Effect.gen(function*() {
              yield* Effect.yieldNow
              yield* ensureCurrentEntry(entry)
              yield* enter
              return yield* effect
            }),
          (fibers) =>
            Effect.forEach(
              fibers,
              (fiber) => Effect.forkIn(Fiber.interrupt(fiber), runtimeScope),
              { discard: true }
            )
        )
      })

    const withRelayAuthorizationGrants = <A, E, R,>(
      entry: RelayEntry,
      direction: PeerRelayAuthorization.Direction,
      normalGrant: PeerRelayAuthorization.Result,
      documents: PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest["documents"],
      effect: Effect.Effect<A, E, R>
    ) =>
      Effect.gen(function*() {
        const grant = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode({
          risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
          direction,
          principal: entry.principal,
          remote: entry.remote,
          documents
        })
        return yield* withRelayGrantAdmission(
          entry,
          [normalGrant, grant],
          effect
        )
      })

    const validateClaimPayload = (
      claim: PeerRelayStore.ClaimedMessage,
      payload: Uint8Array
    ) =>
      Effect.gen(function*() {
        if (payload.byteLength !== claim.payloadBytes) {
          return yield* new PeerRpcError.ServerUnavailable()
        }
        const envelope = yield* decodeRelayEnvelope(payload)
        if (
          claim.relayPeerId !== options.peerId ||
          envelope.connectionEpoch !== claim.senderConnectionEpoch ||
          envelope.sequence !== claim.senderSequence ||
          envelope.messageHash !== claim.messageHash ||
          claim.documentIds.length !== 1 ||
          envelope.documentId !== claim.documentIds[0]
        ) {
          return yield* new PeerRpcError.ServerUnavailable()
        }
        const digest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
          domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
          version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
          expectedLocal: {
            tenantId: claim.channel.tenantId,
            subjectId: claim.channel.senderSubjectId,
            peerId: claim.channel.senderPeerId
          },
          remote: {
            tenantId: claim.channel.tenantId,
            subjectId: claim.channel.recipientSubjectId,
            peerId: claim.channel.recipientPeerId
          },
          relayPeerId: claim.relayPeerId,
          relayMessageId: claim.relayMessageId,
          protocolVersion: PeerRpc.protocolVersion,
          payloadVersion: claim.payloadVersion,
          senderReplicaIncarnation: claim.channel.senderReplicaIncarnation,
          senderConnectionEpoch: claim.senderConnectionEpoch,
          senderSequence: claim.senderSequence,
          document: {
            documentId: envelope.documentId,
            documentType: envelope.documentType
          },
          lineage: envelope.lineage,
          writerProvenance: envelope.writerProvenance,
          messageHash: envelope.messageHash,
          payload
        }).pipe(Effect.mapError(relayStoreFailure))
        if (digest !== claim.outerEnvelopeDigest) {
          return yield* new PeerRpcError.ServerUnavailable()
        }
        return envelope
      })

    const abandonClaim = (
      entry: RelayEntry,
      claim: PeerRelayStore.ClaimedMessage
    ) =>
      lock.withPermit(Effect.gen(function*() {
        if (entry.claims.get(claim.relayMessageId) === claim) {
          entry.claims.delete(claim.relayMessageId)
          const key = relaySelectorKey(relaySelectorForEntry(entry))
          const owner = workOwners.get(key)
          if (
            owner?._tag === "Worker" ||
            owner?._tag === "Entry" && owner.generation === entry.generation
          ) {
            workOwners.delete(key)
          }
          yield* PeerRpcObservability.setRelayActiveClaims(activeClaimCount())
          return true
        }
        return false
      })).pipe(
        Effect.flatMap((removed) => removed ? releaseClaim(entry, claim) : Effect.void)
      )

    const deliver = (selector: RelayWorkSelector) =>
      Effect.gen(function*() {
        const key = relaySelectorKey(selector)
        const entry = yield* lock.withPermit(Effect.sync(() => {
          const sessionId = endpoints.get(relayEntryKey(selector.recipient, selector.sender))
          return sessionId === undefined ? undefined : sessions.get(sessionId)
        }))
        if (entry === undefined || !entry.active) {
          yield* lock.withPermit(Effect.sync(() => {
            workOwners.delete(key)
          }))
          return
        }
        const allowed = yield* authorizeEntry(entry, "Receive", entry.documents)
        const authorizedDocumentIds = allowed.documents.map((document) => document.documentId)
        const unsafeDocuments = allowed.documents.map((document) => ({
          documentType: document.document.name,
          documentId: document.documentId
        }))
        let ownedClaim: PeerRelayStore.ClaimedMessage | undefined
        yield* withRelayAuthorizationGrants(
          entry,
          "Receive",
          allowed,
          unsafeDocuments,
          Effect.gen(function*() {
            type ClaimAcquisition = {
              readonly claimed: PeerRelayStore.ClaimResult
              readonly claim: PeerRelayStore.ClaimedMessage | undefined
              readonly recorded: boolean
            }
            const acquisition = yield* Effect.uninterruptibleMask((restore) =>
              restore(sql.submit(
                "Delivery",
                storeEffect(store.claim({
                  recipient: selector.recipient,
                  sender: selector.sender,
                  sessionGeneration: entry.generation,
                  authorizedDocumentIds
                }))
              )).pipe(
                Effect.flatMap((claimed) => {
                  if (Option.isNone(claimed.message)) {
                    return Effect.succeed({
                      claimed,
                      claim: undefined,
                      recorded: true
                    } as ClaimAcquisition)
                  }
                  const claim = claimed.message.value
                  return lock.withPermit(Effect.gen(function*() {
                    if (
                      sessions.get(entry.sessionId) !== entry ||
                      !entry.active ||
                      endpoints.get(relayEntryKey(entry.principal, entry.remote)) !== entry.sessionId
                    ) {
                      return { claimed, claim, recorded: false } as ClaimAcquisition
                    }
                    entry.claims.set(claim.relayMessageId, claim)
                    yield* PeerRpcObservability.setRelayActiveClaims(activeClaimCount())
                    return { claimed, claim, recorded: true } as ClaimAcquisition
                  }))
                })
              )
            )
            if (acquisition.claim === undefined) {
              const pending = yield* lock.withPermit(Effect.sync(() => {
                const current = workOwners.get(key)
                workOwners.delete(key)
                return current?._tag === "Worker" && current.pending
              }))
              if (pending) yield* notify(selector, acquisition.claimed.lane)
              return
            }
            const claim = acquisition.claim
            if (!acquisition.recorded) {
              yield* releaseClaim(entry, claim)
              return
            }
            ownedClaim = claim
            let pendingReservation: PeerRelayIngress.Reservation | undefined
            yield* Effect.gen(function*() {
              const reservation = yield* ingress.reserveOutbound(claim.payloadBytes).pipe(
                Effect.onError(() => abandonClaim(entry, claim))
              )
              pendingReservation = reservation
              const payload = yield* sql.submit(
                "Delivery",
                storeEffect(store.loadClaimedPayload({
                  channel: claim.channel,
                  rowId: claim.rowId,
                  relayMessageId: claim.relayMessageId,
                  claimToken: claim.claimToken,
                  sessionGeneration: entry.generation,
                  payloadBytes: claim.payloadBytes
                }))
              ).pipe(
                Effect.catchTag(
                  "InvalidRequest",
                  () => Effect.fail(new PeerRpcError.SessionUnavailable())
                ),
                Effect.onError(() => Effect.andThen(reservation.release, abandonClaim(entry, claim)))
              )
              const envelope = yield* validateClaimPayload(claim, payload).pipe(
                Effect.onError(() => Effect.andThen(reservation.release, abandonClaim(entry, claim)))
              )
              const authorized = allowed.documents.some((document) =>
                document.documentId === envelope.documentId &&
                document.document.name === envelope.documentType
              )
              if (!authorized) {
                yield* Effect.uninterruptible(
                  Effect.sync(() => {
                    pendingReservation = undefined
                  }).pipe(
                    Effect.andThen(reservation.release),
                    Effect.ensuring(abandonClaim(entry, claim))
                  )
                )
                return
              }
              const event = PeerRpc.StoredMessage.make({
                _tag: "StoredMessage",
                relayMessageId: claim.relayMessageId,
                claimToken: claim.claimToken,
                relayPeerId: claim.relayPeerId,
                sender: {
                  tenantId: claim.channel.tenantId,
                  subjectId: claim.channel.senderSubjectId,
                  peerId: claim.channel.senderPeerId,
                  replicaIncarnation: claim.channel.senderReplicaIncarnation,
                  connectionEpoch: claim.senderConnectionEpoch,
                  sequence: claim.senderSequence
                },
                recipient: selector.recipient,
                payloadVersion: 1,
                document: {
                  documentType: envelope.documentType,
                  documentId: envelope.documentId
                },
                writerProvenance: envelope.writerProvenance,
                messageHash: envelope.messageHash,
                outerEnvelopeDigest: claim.outerEnvelopeDigest,
                payload
              })
              const transferred = yield* lock.withPermit(Effect.sync(() => {
                const currentSession = sessions.get(entry.sessionId)
                const owner = workOwners.get(key)
                if (
                  currentSession !== entry ||
                  !entry.active ||
                  owner?._tag !== "Worker"
                ) {
                  return false
                }
                workOwners.set(key, {
                  _tag: "Entry",
                  generation: entry.generation,
                  pending: owner.pending
                })
                return true
              }))
              if (!transferred) {
                yield* reservation.release
                yield* abandonClaim(entry, claim)
                return
              }
              const offered = yield* Queue.offer(entry.outbound, {
                event,
                reservation,
                transferred: false
              })
              if (!offered) {
                yield* lock.withPermit(Effect.sync(() => {
                  const owner = workOwners.get(key)
                  if (owner?._tag === "Entry" && owner.generation === entry.generation) {
                    workOwners.delete(key)
                  }
                }))
                yield* reservation.release
                yield* abandonClaim(entry, claim)
                return
              }
              pendingReservation = undefined
              ownedClaim = undefined
              yield* PeerRpcObservability.recordRelayOutcome({
                operation: "RelayClaim",
                direction: "Receive",
                result: "Delivered",
                facts: {
                  bytes: payload.byteLength,
                  items: 1,
                  version: claim.payloadVersion
                }
              })
            }).pipe(
              Effect.onInterrupt(() =>
                pendingReservation === undefined
                  ? Effect.void
                  : Effect.andThen(
                    pendingReservation.release,
                    abandonClaim(entry, claim)
                  )
              )
            )
            ownedClaim = undefined
          }).pipe(
            Effect.onExitIf(
              Exit.isFailure,
              () => ownedClaim === undefined ? Effect.void : abandonClaim(entry, ownedClaim)
            )
          )
        )
      }).pipe(
        Effect.catchTags({
          AccessDenied: () =>
            lock.withPermit(Effect.sync(() => {
              workOwners.delete(relaySelectorKey(selector))
            })),
          RequestCapacityExceeded: () =>
            lock.withPermit(Effect.sync(() => {
              workOwners.delete(relaySelectorKey(selector))
            })),
          SessionUnavailable: () =>
            lock.withPermit(Effect.sync(() => {
              workOwners.delete(relaySelectorKey(selector))
            }))
        }),
        Effect.catchCause((cause) => signalFatal(cause))
      )

    const workOrder = [
      ...Array.from({ length: limits.newWorkWeight }, () => "New" as const),
      ...Array.from({ length: limits.retryWorkWeight }, () => "Retry" as const)
    ]
    const makeWorker = Effect.suspend(() => {
      let cursor = 0
      const take = (): Effect.Effect<RelayWorkSelector, Cause.Done> =>
        Effect.gen(function*() {
          for (let offset = 0; offset < workOrder.length; offset++) {
            const index = (cursor + offset) % workOrder.length
            const lane = workOrder[index]!
            const queue = lane === "New" ? newWork : retryWork
            const queueLock = lane === "New" ? newWorkLock : retryWorkLock
            const item = yield* queueLock.withPermit(Effect.gen(function*() {
              const item = yield* Queue.poll(queue)
              if (Option.isSome(item)) {
                const size = yield* Queue.size(queue)
                yield* PeerRpcObservability.setRelayReadyQueueItems(lane, size)
              }
              return item
            }))
            if (Option.isSome(item)) {
              cursor = (index + 1) % workOrder.length
              return item.value
            }
          }
          yield* Queue.take(workWake)
          return yield* take()
        })
      return Effect.forever(take().pipe(Effect.flatMap(deliver)))
    })

    const workerFibers = new Set<Fiber.Fiber<void, Cause.Done>>()
    let workerCount = 0
    for (let index = 0; index < limits.relayWorkerConcurrency; index++) {
      const countedWorker = Effect.acquireUseRelease(
        lock.withPermit(Effect.gen(function*() {
          workerCount += 1
          yield* PeerRpcObservability.setRelayWorkers(workerCount)
        })),
        () => makeWorker,
        () =>
          lock.withPermit(Effect.gen(function*() {
            workerCount -= 1
            yield* PeerRpcObservability.setRelayWorkers(workerCount)
          }))
      )
      const fiber = yield* Effect.forkIn(countedWorker, runtimeScope).pipe(
        Effect.tap((fiber) =>
          lock.withPermit(Effect.sync(() => {
            workerFibers.add(fiber)
          }))
        ),
        Effect.uninterruptible
      )
      yield* Effect.forkIn(
        Fiber.await(fiber).pipe(
          Effect.flatMap((exit) =>
            lock.withPermit(Effect.sync(() => accepting)).pipe(
              Effect.flatMap((wasAccepting) =>
                wasAccepting && Exit.isFailure(exit)
                  ? signalFatal(exit.cause)
                  : Effect.void
              )
            )
          )
        ),
        runtimeScope
      )
    }

    const compensationFiber = yield* Effect.forkIn(
      Effect.forever(
        Effect.sleep(limits.compensationIntervalMillis).pipe(
          Effect.andThen(lock.withPermit(Effect.sync(() => {
            const active = [...selectors.values()]
            if (active.length === 0) return []
            const batch: Array<RelayWorkSelector> = []
            for (
              let index = 0;
              index < Math.min(limits.compensationBatchSize, active.length);
              index++
            ) {
              batch.push(active[(compensationCursor + index) % active.length]!)
            }
            compensationCursor = (compensationCursor + batch.length) % active.length
            return batch
          }))),
          Effect.flatMap((batch) =>
            Effect.forEach(batch, (selector) => notify(selector, "Retry"), {
              discard: true
            })
          )
        )
      ),
      runtimeScope
    )

    type MaintenanceStage = {
      cursor: number | undefined
      readonly batchSize: number
      readonly run: (
        request: PeerRelayStore.MaintenanceRequest
      ) => Effect.Effect<PeerRelayStore.MaintenanceResult, PeerRelayStore.StoreError>
    }
    const maintenanceStages: Array<MaintenanceStage> = [
      { cursor: undefined, batchSize: limits.claimRecoveryBatchSize, run: store.recover },
      { cursor: undefined, batchSize: limits.expiryBatchSize, run: store.expire },
      { cursor: undefined, batchSize: limits.integrityBatchSize, run: store.repair },
      { cursor: undefined, batchSize: limits.reconciliationBatchSize, run: store.reconcile },
      { cursor: undefined, batchSize: limits.terminalCollectionBatchSize, run: store.collect }
    ]
    const maintenanceFiber = yield* Effect.forkIn(
      Effect.forever(
        Effect.sleep(limits.maintenanceIntervalMillis).pipe(
          Effect.andThen(Effect.forEach(maintenanceStages, (stage) =>
            sql.submit(
              "Maintenance",
              storeEffect(stage.run({
                ...(stage.cursor === undefined ? {} : { cursor: stage.cursor }),
                batchSize: stage.batchSize
              }))
            ).pipe(
              Effect.tap((result) =>
                Effect.sync(() => {
                  stage.cursor = result.hasMore ? result.cursor : undefined
                })
              )
            ), { discard: true })),
          Effect.andThen(refreshRelayPending)
        )
      ).pipe(Effect.catchCause(signalFatal)),
      runtimeScope
    )

    const open = (
      request: typeof PeerRpc.OpenRpc.payloadSchema.Type
    ) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        return yield* openLane.run(
          authenticated.principal.subjectId,
          Effect.gen(function*() {
            const principal = authenticated.principal
            const now = yield* Clock.currentTimeMillis
            if (authenticated.validUntil <= now) {
              return yield* new PeerRpcError.AuthenticationFailure()
            }
            if (request.protocolVersion !== PeerRpc.protocolVersion) {
              return yield* new PeerRpcError.UnsupportedVersion()
            }
            if (
              request.expectedRelayPeerId !== options.peerId ||
              request.expectedLocal.tenantId !== principal.tenantId ||
              request.expectedLocal.subjectId !== principal.subjectId ||
              request.expectedLocal.peerId !== principal.peerId
            ) {
              return yield* new PeerRpcError.PeerMismatch()
            }
            if (principal.tenantId !== options.tenantId) {
              return yield* new PeerRpcError.AccessDenied()
            }
            if (
              request.senderRetryHorizonMillis > limits.maximumSenderRetryHorizonMillis ||
              request.receiptRetentionMillis > limits.maximumReceiptRetentionMillis ||
              request.receiptRetentionMillis <
                Math.max(limits.messageTtlMillis, request.senderRetryHorizonMillis) +
                  limits.minimumTerminalRetentionMillis
            ) {
              return yield* new PeerRpcError.InvalidRequest()
            }
            const send = yield* authorization.authorize({
              direction: "Send",
              principal,
              remote: request.remote,
              documents: request.documents
            })
            const receive = yield* authorization.authorize({
              direction: "Receive",
              principal,
              remote: request.remote,
              documents: request.documents
            })
            const authorizationCompletedAt = yield* Clock.currentTimeMillis
            if (authenticated.validUntil <= authorizationCompletedAt) {
              return yield* new PeerRpcError.AuthenticationFailure()
            }
            if (
              send.validUntil <= authorizationCompletedAt ||
              receive.validUntil <= authorizationCompletedAt
            ) {
              return yield* new PeerRpcError.AccessDenied()
            }
            return yield* Effect.uninterruptibleMask((restore) =>
              Effect.gen(function*() {
                const outbound = yield* Queue.dropping<
                  RelayOutboundItem,
                  PeerRpcError.PeerRpcError | Cause.Done
                >(1)
                const sessionId = yield* Identity.makeSessionId.pipe(
                  Effect.mapError(() => new PeerRpcError.ServerUnavailable())
                )
                const revoked = yield* Deferred.make<void>()
                const entry = yield* lock.withPermit(Effect.gen(function*() {
                  if (!accepting) return yield* new PeerRpcError.ServerUnavailable()
                  const current = subjectSessions.get(principal.subjectId) ?? 0
                  const endpoint = relayEntryKey(principal, request.remote)
                  const replacedId = endpoints.get(endpoint)
                  const replaced = replacedId === undefined ? undefined : sessions.get(replacedId)
                  if (replaced === undefined && current >= limits.maxSessionsPerSubject) {
                    return yield* new PeerRpcError.RequestCapacityExceeded()
                  }
                  if (replaced === undefined && endpoints.size >= limits.maxActiveChannels) {
                    return yield* new PeerRpcError.RequestCapacityExceeded()
                  }
                  const entry: RelayEntry = {
                    sessionId,
                    generation: generation++,
                    principal,
                    senderReplicaIncarnation: request.senderReplicaIncarnation,
                    remote: request.remote,
                    documents: request.documents,
                    receiptRetentionMillis: request.receiptRetentionMillis,
                    senderRetryHorizonMillis: request.senderRetryHorizonMillis,
                    outbound,
                    claims: new Map(),
                    revoked,
                    monitorFibers: [],
                    watcher: undefined,
                    active: true
                  }
                  sessions.set(sessionId, entry)
                  endpoints.set(endpoint, sessionId)
                  incarnations.set(
                    relayIncarnationKey(principal, request.senderReplicaIncarnation, request.remote),
                    sessionId
                  )
                  subjectSessions.set(principal.subjectId, current + 1)
                  selectors.set(
                    relaySelectorKey(relaySelectorForEntry(entry)),
                    relaySelectorForEntry(entry)
                  )
                  return { entry, replaced }
                }))
                const start = restore(Effect.gen(function*() {
                  const monitorStartedAt = yield* Clock.currentTimeMillis
                  if (authenticated.validUntil <= monitorStartedAt) {
                    return yield* new PeerRpcError.AuthenticationFailure()
                  }
                  if (
                    send.validUntil <= monitorStartedAt ||
                    receive.validUntil <= monitorStartedAt
                  ) {
                    return yield* new PeerRpcError.AccessDenied()
                  }
                  const validUntil = Math.min(
                    authenticated.validUntil,
                    send.validUntil,
                    receive.validUntil
                  )
                  const monitors = [
                    authenticated.invalidated,
                    send.invalidated,
                    receive.invalidated,
                    Effect.sleep(Math.max(0, validUntil - monitorStartedAt))
                  ]
                  for (const monitor of monitors) {
                    yield* monitor.pipe(
                      Effect.exit,
                      Effect.andThen(Deferred.succeed(entry.entry.revoked, undefined)),
                      Effect.asVoid,
                      Effect.forkIn(runtimeScope),
                      Effect.tap((fiber) =>
                        lock.withPermit(Effect.sync(() => {
                          if (
                            entry.entry.active &&
                            sessions.get(entry.entry.sessionId) === entry.entry
                          ) {
                            entry.entry.monitorFibers.push(fiber)
                            return true
                          }
                          return false
                        })).pipe(
                          Effect.flatMap((owned) =>
                            owned
                              ? Effect.void
                              : Effect.forkIn(Fiber.interrupt(fiber), runtimeScope).pipe(
                                Effect.asVoid
                              )
                          )
                        )
                      ),
                      Effect.uninterruptible
                    )
                  }
                  yield* Deferred.await(entry.entry.revoked).pipe(
                    Effect.andThen(detachEntry(entry.entry, true)),
                    Effect.forkIn(runtimeScope),
                    Effect.tap((fiber) =>
                      Effect.sync(() => {
                        entry.entry.watcher = fiber
                      })
                    ),
                    Effect.uninterruptible
                  )
                  yield* notify(relaySelectorForEntry(entry.entry), "Retry")
                  const opened = PeerRpc.Opened.make({
                    _tag: "Opened",
                    protocolVersion: PeerRpc.protocolVersion,
                    sessionId,
                    remotePeerId: entry.entry.remote.peerId,
                    authenticatedLocal: principal
                  })
                  const deliveries = Stream.fromQueue(entry.entry.outbound).pipe(
                    Stream.mapEffect((item) =>
                      raceRevocation(
                        entry.entry,
                        item.reservation.transferToCurrentRequest
                      ).pipe(
                        Effect.tap(() =>
                          Effect.sync(() => {
                            item.transferred = true
                          })
                        ),
                        Effect.andThen(ensureCurrentEntry(entry.entry)),
                        Effect.as(item.event),
                        Effect.onError(() => item.reservation.release)
                      )
                    )
                  )
                  return Stream.concat(Stream.make(opened), deliveries).pipe(
                    Stream.ensuring(detachEntry(entry.entry, false))
                  )
                }))
                return yield* (
                  entry.replaced === undefined
                    ? start
                    : detachEntry(entry.replaced, false).pipe(Effect.andThen(start))
                ).pipe(
                  Effect.onExitIf(
                    Exit.isFailure,
                    () => detachEntry(entry.entry, false)
                  )
                )
              })
            )
          })
        )
      })

    const push = (request: typeof PeerRpc.PushRpc.payloadSchema.Type) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        return yield* pushLane.run(
          authenticated.principal.subjectId,
          Effect.gen(function*() {
            const entry = yield* freshEntry(request.sessionId, authenticated)
            const envelope = yield* decodeRelayEnvelope(request.payload)
            const document = entry.documents.find((candidate) =>
              candidate.documentId === envelope.documentId &&
              candidate.documentType === envelope.documentType
            )
            if (document === undefined || !/^[0-9a-f]{64}$/.test(envelope.messageHash)) {
              return yield* new PeerRpcError.InvalidRequest()
            }
            const messageHash = yield* Canonical.digest(envelope.message).pipe(
              Effect.mapError(relayStoreFailure)
            )
            if (messageHash !== envelope.messageHash) {
              return yield* new PeerRpcError.InvalidRequest()
            }
            const send = yield* authorizeEntry(entry, "Send", [document])
            const sendDocument = send.documents.find((candidate) =>
              candidate.documentId === document.documentId &&
              candidate.document.name === document.documentType
            )
            if (sendDocument === undefined) {
              return yield* new PeerRpcError.AccessDenied()
            }
            yield* ensureCurrentEntry(entry)
            const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
              domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
              version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
              expectedLocal: entry.principal,
              remote: {
                tenantId: entry.principal.tenantId,
                subjectId: entry.remote.subjectId,
                peerId: entry.remote.peerId
              },
              relayPeerId: options.peerId,
              relayMessageId: request.relayMessageId,
              protocolVersion: PeerRpc.protocolVersion,
              payloadVersion: 1,
              senderReplicaIncarnation: entry.senderReplicaIncarnation,
              senderConnectionEpoch: envelope.connectionEpoch,
              senderSequence: envelope.sequence,
              document: {
                documentId: envelope.documentId,
                documentType: envelope.documentType
              },
              lineage: envelope.lineage,
              writerProvenance: envelope.writerProvenance,
              messageHash: envelope.messageHash,
              payload: request.payload
            }).pipe(Effect.mapError(relayStoreFailure))
            const channel: PeerRelayStore.ChannelKey = {
              tenantId: entry.principal.tenantId,
              senderSubjectId: entry.principal.subjectId,
              senderPeerId: entry.principal.peerId,
              senderReplicaIncarnation: entry.senderReplicaIncarnation,
              recipientSubjectId: entry.remote.subjectId,
              recipientPeerId: entry.remote.peerId
            }
            const result = yield* withRelayAuthorizationGrants(
              entry,
              "Send",
              send,
              [{
                documentType: sendDocument.document.name,
                documentId: sendDocument.documentId
              }],
              sql.submit(
                "Admission",
                storeEffect(store.admit({
                  channel,
                  relayMessageId: request.relayMessageId,
                  relayPeerId: options.peerId,
                  documentIds: [envelope.documentId],
                  senderConnectionEpoch: envelope.connectionEpoch,
                  senderSequence: envelope.sequence,
                  payloadVersion: 1,
                  messageHash: envelope.messageHash,
                  outerEnvelopeDigest,
                  payload: request.payload,
                  messageTtlMillis: limits.messageTtlMillis,
                  senderRetryHorizonMillis: entry.senderRetryHorizonMillis,
                  minimumTerminalRetentionMillis: limits.minimumTerminalRetentionMillis
                }))
              )
            )
            if (result.ready) {
              yield* notify({
                recipient: {
                  tenantId: channel.tenantId,
                  subjectId: channel.recipientSubjectId,
                  peerId: channel.recipientPeerId
                },
                sender: {
                  subjectId: channel.senderSubjectId,
                  peerId: channel.senderPeerId
                }
              }, "New")
            }
          })
        )
      })

    const terminal = (
      request:
        | typeof PeerRpc.AcknowledgeRpc.payloadSchema.Type
        | typeof PeerRpc.RejectRpc.payloadSchema.Type,
      reason: PeerRpc.RejectReason | undefined
    ) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        return yield* terminalLane.run(
          authenticated.principal.subjectId,
          Effect.gen(function*() {
            const entry = yield* freshEntry(request.sessionId, authenticated)
            const claim = yield* lock.withPermit(Effect.sync(() => entry.claims.get(request.relayMessageId)))
            if (
              claim === undefined ||
              claim.claimToken !== request.claimToken ||
              claim.messageHash !== request.messageHash
            ) {
              return yield* new PeerRpcError.SessionUnavailable()
            }
            const document = entry.documents.filter((candidate) => claim.documentIds.includes(candidate.documentId))
            const receive = yield* authorizeEntry(entry, "Receive", document)
            const transition = yield* withRelayGrantAdmission(
              entry,
              [receive],
              sql.submit(
                "Terminal",
                storeEffect(
                  reason === undefined
                    ? store.acknowledge({
                      channel: claim.channel,
                      relayMessageId: claim.relayMessageId,
                      claimToken: claim.claimToken,
                      messageHash: claim.messageHash,
                      sessionGeneration: entry.generation,
                      recipient: entry.principal
                    })
                    : store.reject({
                      channel: claim.channel,
                      relayMessageId: claim.relayMessageId,
                      claimToken: claim.claimToken,
                      messageHash: claim.messageHash,
                      sessionGeneration: entry.generation,
                      recipient: entry.principal,
                      reason
                    })
                )
              )
            )
            const selector = relaySelectorForEntry(entry)
            const shouldNotify = yield* lock.withPermit(Effect.gen(function*() {
              entry.claims.delete(claim.relayMessageId)
              const key = relaySelectorKey(selector)
              const owner = workOwners.get(key)
              if (owner?._tag === "Entry" && owner.generation === entry.generation) {
                workOwners.delete(key)
              }
              yield* PeerRpcObservability.setRelayActiveClaims(activeClaimCount())
              return transition.ready || owner?._tag === "Entry" && owner.pending
            }))
            if (transition.status === "Stale") {
              return yield* new PeerRpcError.SessionUnavailable()
            }
            if (shouldNotify) yield* notify(selector, transition.lane)
          })
        )
      })

    const shutdown = Effect.uninterruptibleMask(() =>
      Effect.gen(function*() {
        const first = yield* Deferred.succeed(shutdownStarted, undefined)
        if (!first) return yield* Deferred.await(shutdownFinished)
        yield* lock.withPermit(Effect.sync(() => {
          accepting = false
        }))
        yield* Fiber.interrupt(compensationFiber)
        yield* Fiber.interrupt(maintenanceFiber)
        const activeWorkers = yield* lock.withPermit(Effect.sync(() => [...workerFibers]))
        yield* Effect.forEach(activeWorkers, Fiber.interrupt, { discard: true })
        yield* lock.withPermit(Effect.gen(function*() {
          workerFibers.clear()
          yield* PeerRpcObservability.setRelayWorkers(workerCount)
        }))
        yield* newWorkLock.withPermit(
          Queue.shutdown(newWork).pipe(
            Effect.andThen(Queue.size(newWork)),
            Effect.flatMap((size) => PeerRpcObservability.setRelayReadyQueueItems("New", size))
          )
        )
        yield* retryWorkLock.withPermit(
          Queue.shutdown(retryWork).pipe(
            Effect.andThen(Queue.size(retryWork)),
            Effect.flatMap((size) => PeerRpcObservability.setRelayReadyQueueItems("Retry", size))
          )
        )
        yield* Queue.shutdown(workWake)
        const active = yield* lock.withPermit(Effect.sync(() => [...sessions.values()]))
        yield* Effect.forEach(active, (entry) => detachEntry(entry, false), {
          concurrency: limits.shutdownReleaseConcurrency,
          discard: true
        })
        yield* lock.withPermit(Effect.sync(() => {
          workOwners.clear()
          selectors.clear()
        }))
        yield* PeerRpcObservability.setRelayActiveClaims(0)
        yield* sql.shutdown
        yield* openLane.clear
        yield* pushLane.clear
        yield* terminalLane.clear
        yield* Scope.close(runtimeScope, Exit.void)
        yield* Deferred.succeed(shutdownFinished, undefined)
      })
    )

    yield* Effect.addFinalizer(() => shutdown)
    yield* Effect.forkIn(
      Deferred.await(fatal).pipe(
        Effect.ensuring(shutdown)
      ),
      serverScope
    )

    const runtime = PeerRpcServerRuntime.of({
      health: Deferred.poll(fatal).pipe(
        Effect.flatMap((exit) =>
          Option.isNone(exit)
            ? Effect.void
            : Effect.fail(new PeerRpcError.ServerUnavailable())
        )
      ),
      owner: Deferred.await(fatal),
      shutdown,
      usage: lock.withPermit(Effect.sync(() => ({
        accepting,
        sessions: sessions.size,
        subjects: subjectSessions.size,
        activeClaims: [...sessions.values()].reduce((sum, entry) => sum + entry.claims.size, 0),
        queuedChannels: workOwners.size
      })))
    })
    const handlerContext = yield* PeerRpc.Rpcs.toHandlers(PeerRpc.Rpcs.of({
      Open: (request) => Stream.unwrap(open(request)),
      Push: push,
      Acknowledge: (request) => terminal(request, undefined),
      Reject: (request) => terminal(request, request.reason)
    }))
    return Context.add(handlerContext, PeerRpcServerRuntime, runtime).pipe(
      Context.add(PeerRpcFatalSignal, PeerRpcFatalSignal.of({ signal: signalFatal }))
    )
  }))

export const layerServer = Layer.effectDiscard(Effect.gen(function*() {
  const scope = yield* Scope.Scope
  const runtime = yield* PeerRpcServerRuntime
  const fatalSignal = yield* PeerRpcFatalSignal
  const ingress = yield* PeerRelayIngress.PeerRelayIngress
  let stopping = false
  const serverFiber = yield* Effect.forkIn(
    RpcServer.make(PeerRpc.Rpcs, { disableFatalDefects: true }),
    scope
  )
  const ingressFiber = yield* Effect.forkIn(ingress.await, scope)
  const stop = Effect.uninterruptible(
    Effect.sync(() => {
      stopping = true
    }).pipe(
      Effect.andThen(runtime.shutdown),
      Effect.andThen(Fiber.interrupt(serverFiber)),
      Effect.andThen(Fiber.interrupt(ingressFiber))
    )
  )
  const observe = <A, E,>(fiber: Fiber.Fiber<A, E>) =>
    Fiber.await(fiber).pipe(
      Effect.flatMap((exit) =>
        stopping || Exit.isSuccess(exit)
          ? Effect.void
          : fatalSignal.signal(exit.cause)
      )
    )
  yield* Effect.forkIn(observe(serverFiber), scope)
  yield* Effect.forkIn(observe(ingressFiber), scope)
  yield* Effect.forkIn(
    runtime.owner.pipe(
      Effect.catchCause(() => stop)
    ),
    scope
  )
  yield* Effect.addFinalizer(() => stop)
}))
