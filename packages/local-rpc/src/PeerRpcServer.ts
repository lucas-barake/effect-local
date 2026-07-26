import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
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
import type * as SocketServer from "effect/unstable/socket/SocketServer"
import * as PeerAuthorizationValidation from "./internal/peerAuthorization.js"
import * as PeerRpcObservability from "./internal/peerRpcObservability.js"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerAuthorization from "./PeerAuthorization.js"
import * as PeerRelayAuthorization from "./PeerRelayAuthorization.js"
import * as PeerRelayIngress from "./PeerRelayIngress.js"
import * as PeerRelayLimits from "./PeerRelayLimits.js"
import * as PeerRelayRpc from "./PeerRelayRpc.js"
import * as PeerRelayStore from "./PeerRelayStore.js"
import * as PeerRpc from "./PeerRpc.js"
import * as PeerRpcError from "./PeerRpcError.js"
import * as PeerRpcLimits from "./PeerRpcLimits.js"

interface InboundItem {
  readonly id: number
  readonly payload: Uint8Array
}

interface OutboundItem {
  readonly id: number
  readonly payload: Uint8Array
}

interface Entry {
  readonly tenantId: string
  readonly subjectId: string
  readonly peerId: Identity.PeerId
  readonly sessionId: Identity.SessionId
  /**
   * What the client advertised on its own `Open`, never anything inferred from the protocol version.
   *
   * `PeerRpc.protocolVersion` was not bumped when lineage was added, so a client running an older
   * build passes the version check and would be inferred lineage aware. It would then be sent a
   * rewritten document, union it, and reply with the history the rewrite discarded. Absent means
   * not lineage aware.
   */
  readonly lineageAware: boolean
  readonly validUntil: number
  readonly scope: Scope.Closeable
  readonly inbound: Queue.Queue<InboundItem, ReplicaError.ReplicaError | Cause.Done>
  readonly outbound: Queue.Queue<OutboundItem, PeerRpcError.PeerRpcError | Cause.Done>
  readonly outboundPermits: Semaphore.Semaphore
  readonly inboundConsumerStarted: Deferred.Deferred<void>
  readonly closed: Deferred.Deferred<void>
  readonly terminal: Deferred.Deferred<never, PeerRpcError.PeerRpcError>
  readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
  readonly selectedIds: ReadonlySet<Identity.DocumentId>
  readonly dirty: Set<Identity.DocumentId>
  readonly inboundReservations: Map<number, number>
  readonly outboundReservations: Map<number, number>
  outboundWaiter: ByteCapacityWaiter | undefined
  active: boolean
  cleanupStarted: boolean
  queued: boolean
  inboundBytes: number
  session: PeerSession.PeerSession | undefined
  watcher: Fiber.Fiber<void, unknown> | undefined
  requestFiber: Fiber.Fiber<unknown, unknown> | undefined
}

interface SubjectState {
  openTokens: number
  pushTokens: number
  openUpdatedAt: number
  pushUpdatedAt: number
  lastUsedAt: number
  openInFlight: number
  pushInFlight: number
  activeSessions: number
}

interface Registry {
  accepting: boolean
  bufferedBytes: number
  readonly sessions: Map<Identity.SessionId, Entry>
  readonly cleanups: Map<Identity.SessionId, Cleanup>
  readonly peers: Map<string, Identity.SessionId>
  readonly documents: Map<Identity.DocumentId, Set<Identity.SessionId>>
  readonly subjects: Map<string, SubjectState>
  readonly outboundWaiters: Map<number, ByteCapacityWaiter>
}

interface ByteCapacityWaiter {
  readonly id: number
  readonly bytes: number
  readonly ready: Deferred.Deferred<boolean>
  readonly active: () => boolean
  readonly entry: Entry
  state: "Waiting" | "Reserved" | "Registered" | "Cancelled"
}

interface Cleanup {
  readonly entry: Entry
  readonly activeSession: boolean
  readonly inboundItems: number
  readonly outboundItems: number
  readonly outboundBytes: number
  readonly watcher: Fiber.Fiber<void, unknown> | undefined
  readonly requestFiber: Fiber.Fiber<unknown, unknown> | undefined
  readonly completed: Deferred.Deferred<void>
  readonly error: PeerRpcError.PeerRpcError | undefined
  readonly fromWatcher: boolean
  readonly interruptRequest: boolean
  started: boolean
}

const replicaFailure = () =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({ cause: new Error("RPC peer session unavailable") })
  })

const sessionFailure = (cause: Cause.Cause<ReplicaError.ReplicaError>) =>
  Cause.findErrorOption(cause).pipe(
    Option.match({
      onNone: () => new PeerRpcError.ServerUnavailable(),
      onSome: (error) => {
        // A rewritten document lineage is a peer visible protocol outcome, not a server fault, and
        // the peer must be able to stop retrying against a history it can no longer reach.
        if (error.reason._tag === "DocumentLineageChanged") {
          return new PeerRpcError.DocumentLineageChanged()
        }
        return error.reason._tag === "StorageUnavailable" && Cause.isTimeoutError(error.reason.cause)
          ? new PeerRpcError.SessionOverloaded()
          : new PeerRpcError.ServerUnavailable()
      }
    })
  )

const authorizationFailure = (
  cause: Cause.Cause<PeerRpcError.AccessDenied | PeerRpcError.ServerUnavailable>
) => {
  if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
  if (cause.reasons.length === 1 && Cause.isFailReason(cause.reasons[0])) {
    const error = cause.reasons[0].error
    if (error._tag === "AccessDenied" || error._tag === "ServerUnavailable") {
      return Effect.fail(error)
    }
  }
  return Effect.fail(new PeerRpcError.ServerUnavailable())
}

export const layerHandlers = (
  options: {
    readonly tenantId: string
    readonly peerId: Identity.PeerId
    readonly definition: ReplicaDefinition.Any
  }
) =>
  PeerRpc.Rpcs.toLayer(Effect.gen(function*() {
    const serverScope = yield* Scope.Scope
    const runtimeScope = yield* Scope.fork(serverScope, "parallel")
    const publisher = yield* CommitPublisher.CommitPublisher
    const limits = yield* PeerRpcLimits.PeerRpcLimits
    const replicaLimits = yield* ReplicaLimits.ReplicaLimits
    const authorization = yield* PeerAuthorization.PeerAuthorization
    const subscription = yield* publisher.subscribe
    const lock = yield* Semaphore.make(1)
    const openPermits = yield* Semaphore.make(limits.maxInFlightOpen)
    const pushPermits = yield* Semaphore.make(limits.maxInFlightPush)
    const bufferedBytes = yield* Semaphore.make(limits.maxBufferedBytes)
    const readySessions = yield* Queue.bounded<Identity.SessionId, Cause.Done>(replicaLimits.maxSessions)
    const registry: Registry = {
      accepting: true,
      bufferedBytes: 0,
      sessions: new Map(),
      cleanups: new Map(),
      peers: new Map(),
      documents: new Map(),
      subjects: new Map(),
      outboundWaiters: new Map()
    }
    const inactiveSubjects = new Map<string, SubjectState>()
    let reservationId = 0
    let outboundWaiterId = 0

    const peerKey = (tenantId: string, peerId: Identity.PeerId) => `${tenantId}:${peerId}`

    const retainInactiveSubject = (subjectId: string, subject: SubjectState) => {
      if (subject.activeSessions !== 0 || subject.openInFlight !== 0 || subject.pushInFlight !== 0) return
      inactiveSubjects.delete(subjectId)
      inactiveSubjects.set(subjectId, subject)
    }

    const drainByteCapacity = () => {
      while (registry.outboundWaiters.size > 0) {
        const waiter = registry.outboundWaiters.values().next().value!
        if (!registry.accepting || !waiter.active()) {
          registry.outboundWaiters.delete(waiter.id)
          waiter.state = "Cancelled"
          if (waiter.entry.outboundWaiter === waiter) waiter.entry.outboundWaiter = undefined
          Deferred.doneUnsafe(waiter.ready, Effect.succeed(false))
          continue
        }
        if (registry.bufferedBytes + waiter.bytes > limits.maxBufferedBytes) return
        registry.outboundWaiters.delete(waiter.id)
        registry.bufferedBytes += waiter.bytes
        waiter.state = "Reserved"
        Deferred.doneUnsafe(waiter.ready, Effect.succeed(true))
      }
    }

    const cancelByteCapacityWaiter = (waiter: ByteCapacityWaiter): Effect.Effect<void> =>
      lock.withPermit(Effect.sync(() => {
        if (waiter.state === "Cancelled" || waiter.state === "Registered") return
        if (waiter.state === "Waiting") registry.outboundWaiters.delete(waiter.id)
        else registry.bufferedBytes -= waiter.bytes
        waiter.state = "Cancelled"
        if (waiter.entry.outboundWaiter === waiter) waiter.entry.outboundWaiter = undefined
        Deferred.doneUnsafe(waiter.ready, Effect.succeed(false))
        drainByteCapacity()
      }))

    const reserveByteCapacity = (
      active: () => boolean,
      entry: Entry,
      bytes: number,
      interruptible: <A,>(effect: Effect.Effect<A>) => Effect.Effect<A>
    ) =>
      lock.withPermit(Effect.sync(() => {
        if (!active()) return false as const
        if (registry.outboundWaiters.size === 0 && registry.bufferedBytes + bytes <= limits.maxBufferedBytes) {
          registry.bufferedBytes += bytes
          return true as const
        }
        if (registry.outboundWaiters.size >= replicaLimits.maxSessions) {
          return false as const
        }
        const waiter: ByteCapacityWaiter = {
          id: outboundWaiterId++,
          bytes,
          ready: Deferred.makeUnsafe(),
          active,
          entry,
          state: "Waiting"
        }
        registry.outboundWaiters.set(waiter.id, waiter)
        entry.outboundWaiter = waiter
        return waiter
      })).pipe(
        Effect.flatMap((result): Effect.Effect<boolean | ByteCapacityWaiter> => {
          if (typeof result === "boolean") return Effect.succeed(result)
          return interruptible(Deferred.await(result.ready)).pipe(
            Effect.onInterrupt(() => cancelByteCapacityWaiter(result)),
            Effect.map((granted) => granted ? result : false)
          )
        })
      )

    const releaseByteCapacity = (bytes: number) =>
      lock.withPermit(Effect.sync(() => {
        registry.bufferedBytes -= bytes
        drainByteCapacity()
      }))

    const releaseReservations = (cleanup: Cleanup) =>
      cleanup.outboundBytes === 0
        ? Effect.void
        : Effect.all([
          bufferedBytes.release(cleanup.outboundBytes),
          cleanup.entry.outboundPermits.release(cleanup.outboundBytes)
        ], { discard: true })

    const detach = (
      entry: Entry,
      error: PeerRpcError.PeerRpcError | undefined,
      fromWatcher: boolean,
      interruptRequest: boolean
    ): Cleanup | undefined => {
      if (entry.cleanupStarted) return undefined
      const activeSession = entry.active
      if (error !== undefined) Deferred.doneUnsafe(entry.terminal, Effect.fail(error))
      Deferred.doneUnsafe(entry.closed, Effect.void)
      entry.cleanupStarted = true
      entry.active = false
      if (registry.sessions.get(entry.sessionId) === entry) {
        registry.sessions.delete(entry.sessionId)
        if (registry.peers.get(peerKey(entry.tenantId, entry.peerId)) === entry.sessionId) {
          registry.peers.delete(peerKey(entry.tenantId, entry.peerId))
        }
        for (const documentId of entry.selectedIds) {
          const sessions = registry.documents.get(documentId)
          sessions?.delete(entry.sessionId)
          if (sessions?.size === 0) registry.documents.delete(documentId)
        }
        const subject = registry.subjects.get(entry.subjectId)
        if (subject !== undefined) {
          subject.activeSessions -= 1
          retainInactiveSubject(entry.subjectId, subject)
        }
      }
      const inboundItems = entry.inboundReservations.size
      const inboundBytes = [...entry.inboundReservations.values()].reduce((total, bytes) => total + bytes, 0)
      const outboundItems = entry.outboundReservations.size
      const outboundBytes = [...entry.outboundReservations.values()].reduce((total, bytes) => total + bytes, 0)
      entry.inboundReservations.clear()
      entry.outboundReservations.clear()
      entry.inboundBytes = 0
      registry.bufferedBytes -= inboundBytes + outboundBytes
      const outboundWaiter = entry.outboundWaiter
      if (outboundWaiter !== undefined && outboundWaiter.state !== "Registered") {
        if (outboundWaiter.state === "Waiting") registry.outboundWaiters.delete(outboundWaiter.id)
        else if (outboundWaiter.state === "Reserved") registry.bufferedBytes -= outboundWaiter.bytes
        outboundWaiter.state = "Cancelled"
        entry.outboundWaiter = undefined
        Deferred.doneUnsafe(outboundWaiter.ready, Effect.succeed(false))
      }
      drainByteCapacity()
      const cleanup = {
        entry,
        activeSession,
        inboundItems,
        outboundItems,
        outboundBytes,
        watcher: entry.watcher,
        requestFiber: entry.requestFiber,
        completed: Deferred.makeUnsafe<void>(),
        error,
        fromWatcher,
        interruptRequest,
        started: false
      }
      registry.cleanups.set(entry.sessionId, cleanup)
      return cleanup
    }

    const finishCleanup = (cleanup: Cleanup) =>
      Effect.uninterruptible(
        lock.withPermit(Effect.sync(() => {
          if (cleanup.started) return false
          cleanup.started = true
          return true
        })).pipe(
          Effect.flatMap((owner) =>
            owner
              ? Effect.gen(function*() {
                if (cleanup.activeSession) yield* PeerRpcObservability.modifyActiveSessions(-1)
                yield* PeerRpcObservability.modifyQueueItems("Inbound", -cleanup.inboundItems)
                yield* PeerRpcObservability.modifyQueueItems("Outbound", -cleanup.outboundItems)
                if (cleanup.interruptRequest && cleanup.requestFiber !== undefined) {
                  yield* Effect.sync(() => cleanup.requestFiber?.interruptUnsafe(cleanup.requestFiber.id))
                }
                if (cleanup.error !== undefined) {
                  yield* Queue.fail(cleanup.entry.inbound, replicaFailure())
                  yield* Queue.fail(cleanup.entry.outbound, cleanup.error)
                }
                yield* Queue.shutdown(cleanup.entry.inbound)
                yield* Queue.shutdown(cleanup.entry.outbound)
                yield* releaseReservations(cleanup)
                if (!cleanup.fromWatcher && cleanup.watcher !== undefined) yield* Fiber.interrupt(cleanup.watcher)
                yield* Scope.close(
                  cleanup.entry.scope,
                  cleanup.error === undefined ? Exit.void : Exit.fail(cleanup.error)
                )
              }).pipe(
                Effect.ensuring(lock.withPermit(Effect.sync(() => {
                  if (registry.cleanups.get(cleanup.entry.sessionId) === cleanup) {
                    registry.cleanups.delete(cleanup.entry.sessionId)
                  }
                  Deferred.doneUnsafe(cleanup.completed, Effect.void)
                })))
              )
              : Deferred.await(cleanup.completed)
          )
        )
      )

    const finishCleanups = (cleanups: ReadonlyArray<Cleanup>) =>
      Effect.forEach(cleanups, finishCleanup, {
        concurrency: limits.shutdownCleanupConcurrency,
        discard: true
      })

    const revoke = (
      entry: Entry,
      error: PeerRpcError.PeerRpcError | undefined,
      fromWatcher = false,
      interruptRequest = error !== undefined
    ) =>
      lock.withPermit(Effect.sync(() => detach(entry, error, fromWatcher, interruptRequest))).pipe(
        Effect.flatMap((cleanup) => cleanup === undefined ? Effect.void : finishCleanup(cleanup))
      )

    const stopAll = (error: PeerRpcError.PeerRpcError, shutdown: boolean) =>
      lock.withPermit(Effect.sync(() => {
        registry.accepting = false
        const entries = Array.from(registry.sessions.values())
        let shutdownCloses = 0
        for (const entry of entries) {
          const cleanup = detach(entry, error, false, true)
          if (cleanup?.activeSession === true) shutdownCloses += 1
        }
        const cleanups = [...registry.cleanups.values()]
        return { cleanups, shutdownCloses }
      })).pipe(
        Effect.flatMap(({ cleanups, shutdownCloses }) =>
          (shutdown && shutdownCloses > 0
            ? PeerRpcObservability.record("Server", "ShutdownClosed", shutdownCloses)
            : Effect.void).pipe(
              Effect.andThen(Effect.forEach(
                cleanups,
                (cleanup) =>
                  cleanup.interruptRequest && cleanup.requestFiber !== undefined
                    ? Fiber.interrupt(cleanup.requestFiber)
                    : Effect.void,
                { concurrency: limits.shutdownCleanupConcurrency, discard: true }
              )),
              Effect.andThen(finishCleanups(cleanups))
            )
        )
      )

    const subjectState = (subjectId: string, now: number) => {
      const oldest = inactiveSubjects.entries().next().value
      if (oldest !== undefined) {
        if (now - oldest[1].lastUsedAt >= limits.rateLimitIdleRetention) {
          inactiveSubjects.delete(oldest[0])
          registry.subjects.delete(oldest[0])
        }
      }
      let subject = registry.subjects.get(subjectId)
      if (
        subject !== undefined && subject.activeSessions === 0 && subject.openInFlight === 0 &&
        subject.pushInFlight === 0
      ) {
        inactiveSubjects.delete(subjectId)
        if (now - subject.lastUsedAt >= limits.rateLimitIdleRetention) {
          registry.subjects.delete(subjectId)
          subject = undefined
        }
      }
      if (subject !== undefined) return subject
      if (registry.subjects.size >= limits.maxRetainedRateLimitedSubjects) {
        const evictable = inactiveSubjects.keys().next().value
        if (evictable === undefined) return undefined
        inactiveSubjects.delete(evictable)
        registry.subjects.delete(evictable)
      }
      subject = {
        openTokens: limits.openBurst,
        pushTokens: limits.pushBurst,
        openUpdatedAt: now,
        pushUpdatedAt: now,
        lastUsedAt: now,
        openInFlight: 0,
        pushInFlight: 0,
        activeSessions: 0
      }
      registry.subjects.set(subjectId, subject)
      return subject
    }

    const acquireSubject = (subjectId: string, operation: "Open" | "Push", now: number) =>
      lock.withPermit(Effect.sync(() => {
        if (!registry.accepting) return "Unavailable" as const
        const subject = subjectState(subjectId, now)
        if (subject === undefined) return "Capacity" as const
        const storedUpdatedAt = operation === "Open" ? subject.openUpdatedAt : subject.pushUpdatedAt
        const effectiveNow = Math.max(now, subject.openUpdatedAt, subject.pushUpdatedAt)
        const elapsed = effectiveNow - storedUpdatedAt
        if (operation === "Open") {
          subject.openTokens = Math.min(
            limits.openBurst,
            subject.openTokens + elapsed / 1_000 * limits.openRatePerSecond
          )
          subject.openUpdatedAt = effectiveNow
          if (subject.openTokens < 1 || subject.openInFlight >= limits.maxInFlightOpenPerSubject) {
            retainInactiveSubject(subjectId, subject)
            return "Capacity" as const
          }
          subject.openTokens -= 1
          subject.openInFlight += 1
        } else {
          subject.pushTokens = Math.min(
            limits.pushBurst,
            subject.pushTokens + elapsed / 1_000 * limits.pushRatePerSecond
          )
          subject.pushUpdatedAt = effectiveNow
          if (subject.pushTokens < 1 || subject.pushInFlight >= limits.maxInFlightPushPerSubject) {
            retainInactiveSubject(subjectId, subject)
            return "Capacity" as const
          }
          subject.pushTokens -= 1
          subject.pushInFlight += 1
        }
        subject.lastUsedAt = effectiveNow
        return "Acquired" as const
      }))

    const releaseSubject = (subjectId: string, operation: "Open" | "Push") =>
      lock.withPermit(Effect.sync(() => {
        const subject = registry.subjects.get(subjectId)
        if (subject === undefined) return
        if (operation === "Open") subject.openInFlight -= 1
        else subject.pushInFlight -= 1
        retainInactiveSubject(subjectId, subject)
      }))

    const admitted = <A, E, R,>(
      subjectId: string,
      operation: "Open" | "Push",
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<
      A,
      E | PeerRpcError.RequestCapacityExceeded | PeerRpcError.ServerUnavailable,
      R
    > =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) => acquireSubject(subjectId, operation, now)),
        Effect.flatMap((admission) =>
          Effect.gen(function*() {
            if (admission === "Unavailable") return yield* new PeerRpcError.ServerUnavailable()
            if (admission === "Capacity") return yield* new PeerRpcError.RequestCapacityExceeded()
            return yield* (operation === "Open" ? openPermits : pushPermits).withPermitsIfAvailable(1)(effect).pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.fail(new PeerRpcError.RequestCapacityExceeded()),
                  onSome: Effect.succeed
                })
              ),
              Effect.ensuring(releaseSubject(subjectId, operation))
            )
          })
        )
      )

    const releaseInbound = (entry: Entry, id: number) =>
      lock.withPermit(Effect.gen(function*() {
        const bytes = yield* Effect.sync(() => {
          const bytes = entry.inboundReservations.get(id)
          if (bytes === undefined) return undefined
          entry.inboundReservations.delete(id)
          entry.inboundBytes -= bytes
          registry.bufferedBytes -= bytes
          drainByteCapacity()
          return bytes
        })
        if (bytes !== undefined) yield* PeerRpcObservability.modifyQueueItems("Inbound", -1)
      }))

    const releaseOutbound = (entry: Entry, id: number) =>
      lock.withPermit(Effect.gen(function*() {
        const bytes = yield* Effect.sync(() => {
          const bytes = entry.outboundReservations.get(id)
          if (bytes === undefined) return undefined
          entry.outboundReservations.delete(id)
          registry.bufferedBytes -= bytes
          drainByteCapacity()
          return bytes
        })
        if (bytes !== undefined) yield* PeerRpcObservability.modifyQueueItems("Outbound", -1)
        return bytes
      })).pipe(
        Effect.flatMap((bytes) =>
          bytes === undefined
            ? Effect.void
            : Effect.all([
              bufferedBytes.release(bytes),
              entry.outboundPermits.release(bytes)
            ], { discard: true })
        )
      )

    const sessionTransport = (entry: Entry) => {
      let currentInbound: number | undefined
      let started = false
      const closeRequested = Deferred.makeUnsafe<never>()
      const receive = Stream.fromPull(Effect.succeed(
        Effect.gen(function*() {
          if (currentInbound !== undefined) {
            yield* releaseInbound(entry, currentInbound)
            currentInbound = undefined
          }
          if (!started) {
            started = true
            yield* Deferred.succeed(entry.inboundConsumerStarted, undefined)
          }
          const item = yield* Effect.raceFirst(Deferred.await(closeRequested), Queue.take(entry.inbound))
          currentInbound = item.id
          return Arr.of(item.payload)
        })
      )).pipe(
        Stream.ensuring(
          Effect.suspend(() => currentInbound === undefined ? Effect.void : releaseInbound(entry, currentInbound))
        )
      )
      const send = (payload: Uint8Array) => {
        const bytes = payload.byteLength
        if (
          bytes > PeerSession.maximumSyncEnvelopeBytes(
            replicaLimits.maxSyncMessageBytes,
            replicaLimits.maxSyncChangesPerMessage
          )
        ) {
          return PeerRpcObservability.record("Outbound", "Overloaded", 1).pipe(
            Effect.andThen(Effect.fail(replicaFailure()))
          )
        }
        const releaseUntracked = releaseByteCapacity(bytes)
        const reserve = Effect.uninterruptibleMask((restore) => {
          return Effect.gen(function*() {
            yield* restore(entry.outboundPermits.take(bytes))
            const capacityReserved = yield* reserveByteCapacity(
              () => entry.active,
              entry,
              bytes,
              (effect) => restore(effect)
            ).pipe(
              Effect.onInterrupt(() => entry.outboundPermits.release(bytes).pipe(Effect.asVoid))
            )
            if (!capacityReserved) {
              yield* entry.outboundPermits.release(bytes)
              yield* PeerRpcObservability.record("Outbound", "Overloaded", 1)
              return yield* replicaFailure()
            }
            yield* bufferedBytes.take(bytes)
            const id = reservationId++
            const registration = yield* lock.withPermit(Effect.gen(function*() {
              if (!entry.active || registry.sessions.get(entry.sessionId) !== entry) {
                return { registered: false, cleanup: undefined }
              }
              if ((yield* Clock.currentTimeMillis) >= entry.validUntil) {
                return {
                  registered: false,
                  cleanup: detach(entry, new PeerRpcError.SessionUnavailable(), false, true)
                }
              }
              const registered = yield* Effect.sync(() => {
                if (typeof capacityReserved === "object") {
                  if (capacityReserved.state !== "Reserved") return false
                  capacityReserved.state = "Registered"
                  if (entry.outboundWaiter === capacityReserved) entry.outboundWaiter = undefined
                }
                entry.outboundReservations.set(id, bytes)
                return true
              })
              if (registered) yield* PeerRpcObservability.modifyQueueItems("Outbound", 1)
              return { registered, cleanup: undefined }
            }))
            if (!registration.registered) {
              yield* bufferedBytes.release(bytes)
              yield* entry.outboundPermits.release(bytes)
              if (typeof capacityReserved !== "object" || capacityReserved.state === "Reserved") {
                yield* releaseUntracked
              }
              if (registration.cleanup !== undefined) {
                yield* finishCleanup(registration.cleanup).pipe(Effect.forkIn(runtimeScope))
              }
              return yield* replicaFailure()
            }
            const offered = yield* restore(Queue.offer(entry.outbound, { id, payload })).pipe(
              Effect.onInterrupt(() => releaseOutbound(entry, id))
            )
            if (!offered) {
              yield* releaseOutbound(entry, id)
              yield* PeerRpcObservability.record("Outbound", "Overloaded", 1)
              return yield* replicaFailure()
            }
            yield* PeerRpcObservability.recordBytes("Outbound", bytes)
          })
        })
        return reserve
      }
      return PeerTransport.PeerTransport.of({
        // Both `capabilities` here describe the REMOTE peer, which is what the server side
        // `PeerSession` reads before it lets `PeerSync.generate` emit a rewritten document. The
        // value is read off the client's own `Open` advertisement and is never inferred from the
        // shared protocol version, which lineage did not bump and which therefore cannot tell this
        // build apart from one that would union a rewritten document.
        capabilities: { storeAndForward: false, lineageAware: entry.lineageAware },
        connect: (connectOptions) =>
          connectOptions.peerId !== entry.peerId
            ? Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProtocolMismatch({
                  expected: entry.peerId,
                  observed: connectOptions.peerId
                })
              })
            )
            : Effect.succeed({
              peerId: entry.peerId,
              capabilities: { storeAndForward: false, lineageAware: entry.lineageAware },
              receive,
              send,
              close: Deferred.interrupt(closeRequested).pipe(Effect.asVoid)
            })
      })
    }

    const register = (entry: Entry) =>
      lock.withPermit(Effect.sync(() => {
        if (!registry.accepting) return { _tag: "Unavailable" as const }
        if (entry.cleanupStarted) return { _tag: "Revoked" as const }
        const previousId = registry.peers.get(peerKey(entry.tenantId, entry.peerId))
        const previous = previousId === undefined ? undefined : registry.sessions.get(previousId)
        const subject = registry.subjects.get(entry.subjectId)
        if (subject === undefined) return { _tag: "Capacity" as const }
        const effectiveSessions = registry.sessions.size - (previous === undefined ? 0 : 1)
        const effectiveSubjectSessions = subject.activeSessions -
          (previous?.subjectId === entry.subjectId ? 1 : 0)
        if (
          effectiveSessions >= replicaLimits.maxSessions ||
          effectiveSubjectSessions >= limits.maxSessionsPerSubject
        ) {
          return { _tag: "Overloaded" as const }
        }
        const previousCleanup = previous === undefined
          ? undefined
          : detach(previous, new PeerRpcError.SessionUnavailable(), false, true)
        entry.active = true
        registry.sessions.set(entry.sessionId, entry)
        registry.peers.set(peerKey(entry.tenantId, entry.peerId), entry.sessionId)
        subject.activeSessions += 1
        inactiveSubjects.delete(entry.subjectId)
        for (const documentId of entry.selectedIds) {
          const sessions = registry.documents.get(documentId) ?? new Set()
          sessions.add(entry.sessionId)
          registry.documents.set(documentId, sessions)
        }
        return { _tag: "Registered" as const, previousCleanup }
      })).pipe(
        Effect.tap((result) =>
          result._tag !== "Registered"
            ? Effect.void
            : PeerRpcObservability.modifyActiveSessions(1).pipe(
              Effect.andThen(
                result.previousCleanup === undefined
                  ? Effect.void
                  : PeerRpcObservability.record("Open", "Replaced", 1)
              )
            )
        )
      )

    const markReady = (entry: Entry, session: PeerSession.PeerSession) =>
      lock.withPermit(Effect.sync(() => {
        if (!entry.active || registry.sessions.get(entry.sessionId) !== entry) return false
        entry.session = session
        if (entry.dirty.size === 0 || entry.queued) return false
        entry.queued = true
        return true
      })).pipe(
        Effect.flatMap((enqueue) =>
          enqueue ? Queue.offer(readySessions, entry.sessionId).pipe(Effect.asVoid) : Effect.void
        )
      )

    const revokeForSessionFailure = (entry: Entry, cause: Cause.Cause<ReplicaError.ReplicaError>) => {
      const error = sessionFailure(cause)
      return (error._tag === "SessionOverloaded"
        ? PeerRpcObservability.record("Outbound", "Overloaded", 1)
        : Effect.void).pipe(
          Effect.andThen(revoke(entry, error, false, true))
        )
    }

    const responseStream = (entry: Entry) => {
      let opened = false
      let currentOutbound: number | undefined
      const checkDelivery = lock.withPermit(Effect.gen(function*() {
        if (!entry.active || registry.sessions.get(entry.sessionId) !== entry) {
          return { _tag: "Inactive" as const }
        }
        if ((yield* Clock.currentTimeMillis) >= entry.validUntil) {
          return {
            _tag: "Expired" as const,
            cleanup: detach(entry, new PeerRpcError.SessionUnavailable(), false, false)
          }
        }
        return { _tag: "Active" as const }
      }))
      const requireDelivery = Effect.gen(function*() {
        const delivery = yield* checkDelivery
        if (delivery._tag === "Active") return
        if (delivery._tag === "Expired" && delivery.cleanup !== undefined) {
          yield* finishCleanup(delivery.cleanup)
        }
        return yield* Deferred.await(entry.terminal)
      })
      const pull = Effect.withFiber((fiber) =>
        lock.withPermit(Effect.sync(() => {
          if (!entry.cleanupStarted && entry.requestFiber === undefined) entry.requestFiber = fiber
        })).pipe(
          Effect.as(
            Effect.gen(function*() {
              if (currentOutbound !== undefined) {
                yield* releaseOutbound(entry, currentOutbound)
                currentOutbound = undefined
              }
              if (!opened) {
                yield* Effect.raceFirst(
                  Deferred.await(entry.terminal),
                  Deferred.await(entry.inboundConsumerStarted)
                )
                return yield* Effect.uninterruptible(Effect.gen(function*() {
                  yield* requireDelivery
                  opened = true
                  return Arr.of<PeerRpc.OpenEvent>(PeerRpc.Opened.make({
                    _tag: "Opened",
                    protocolVersion: PeerRpc.protocolVersion,
                    sessionId: entry.sessionId,
                    peerId: options.peerId,
                    // The only place the flag reaches a remote peer. Without it the client reads an
                    // absent capability, and its own `generate` refuses to emit any rewritten
                    // document toward this server.
                    capabilities: { storeAndForward: false, lineageAware: true }
                  }))
                }))
              }
              const item = yield* Effect.raceFirst(
                Deferred.await(entry.terminal),
                Queue.take(entry.outbound)
              )
              return yield* Effect.uninterruptible(Effect.gen(function*() {
                const delivery = yield* checkDelivery
                if (delivery._tag === "Active") {
                  currentOutbound = item.id
                  return Arr.of<PeerRpc.OpenEvent>(PeerRpc.Message.make({ _tag: "Message", payload: item.payload }))
                }
                yield* releaseOutbound(entry, item.id)
                if (delivery._tag === "Expired" && delivery.cleanup !== undefined) {
                  yield* finishCleanup(delivery.cleanup)
                }
                return yield* Deferred.await(entry.terminal)
              }))
            }).pipe(
              Effect.catchCauseIf(
                (cause) => cause.reasons.every(Cause.isInterruptReason),
                (cause) =>
                  Deferred.poll(entry.terminal).pipe(
                    Effect.flatMap(Option.match({
                      onNone: () => Effect.failCause(cause),
                      onSome: (terminal) => terminal
                    }))
                  )
              )
            )
          )
        )
      )
      return Stream.fromPull(pull).pipe(
        Stream.ensuring(
          Effect.suspend(() =>
            (currentOutbound === undefined ? Effect.void : releaseOutbound(entry, currentOutbound)).pipe(
              Effect.andThen(revoke(entry, undefined, false, false))
            )
          )
        )
      )
    }

    const openUnobserved = (request: typeof PeerRpc.OpenRpc.payloadSchema.Type) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        const now = yield* Clock.currentTimeMillis
        if (!Number.isFinite(authenticated.validUntil) || authenticated.validUntil <= now) {
          return yield* new PeerRpcError.AuthenticationFailure()
        }
        if (authenticated.principal.tenantId !== options.tenantId) return yield* new PeerRpcError.AccessDenied()
        if (request.protocolVersion !== PeerRpc.protocolVersion) return yield* new PeerRpcError.UnsupportedVersion()
        if (request.expectedPeerId !== options.peerId) return yield* new PeerRpcError.PeerMismatch()
        if (request.definitionHash === undefined) return yield* new PeerRpcError.InvalidRequest()
        if (request.definitionHash !== options.definition.hash) return yield* new PeerRpcError.DefinitionMismatch()
        if (request.documents.length === 0) return yield* new PeerRpcError.InvalidRequest()
        const requested = new Set(request.documents.map((entry) => `${entry.documentType}:${entry.documentId}`))
        if (requested.size !== request.documents.length) return yield* new PeerRpcError.InvalidRequest()
        const requestedDocumentIds = new Set(request.documents.map((entry) => entry.documentId))
        if (requestedDocumentIds.size !== request.documents.length) return yield* new PeerRpcError.InvalidRequest()
        if (request.documents.length > replicaLimits.maxStreamsPerSession) {
          return yield* new PeerRpcError.RequestLimitExceeded()
        }

        return yield* admitted(
          authenticated.principal.subjectId,
          "Open",
          Effect.gen(function*() {
            const authorizationRequest = {
              principal: authenticated.principal,
              documents: request.documents
            }
            const authorized = yield* authorization.authorize(authorizationRequest).pipe(
              Effect.flatMap((result) => PeerAuthorizationValidation.validate(authorizationRequest, result)),
              Effect.catchCause(authorizationFailure)
            )
            if (
              authorized.documents.some((entry) =>
                options.definition.documents.byName.get(entry.document.name) !== entry.document
              )
            ) {
              return yield* new PeerRpcError.AccessDenied()
            }
            const sessionId = yield* Identity.makeSessionId.pipe(
              Effect.catchTag("PlatformError", () => Effect.fail(new PeerRpcError.ServerUnavailable()))
            )
            const now = yield* Clock.currentTimeMillis
            const entry: Entry = yield* Effect.uninterruptible(Effect.gen(function*() {
              const scope = yield* Scope.fork(runtimeScope, "parallel")
              return {
                tenantId: options.tenantId,
                subjectId: authenticated.principal.subjectId,
                peerId: authenticated.principal.peerId,
                sessionId,
                // Read, not inferred. An omitted advertisement is exactly the older client this
                // must stay false for.
                lineageAware: request.capabilities?.lineageAware === true,
                validUntil: Math.min(
                  authenticated.validUntil,
                  authorized.validUntil,
                  now + limits.maximumReauthorizationInterval
                ),
                scope,
                inbound: yield* Queue.dropping<InboundItem, ReplicaError.ReplicaError | Cause.Done>(
                  limits.inboundItemCapacity
                ),
                outbound: yield* Queue.bounded<OutboundItem, PeerRpcError.PeerRpcError | Cause.Done>(
                  limits.outboundItemCapacity
                ),
                outboundPermits: yield* Semaphore.make(limits.maxOutboundBufferedBytesPerSession),
                inboundConsumerStarted: yield* Deferred.make<void>(),
                closed: yield* Deferred.make<void>(),
                terminal: yield* Deferred.make<never, PeerRpcError.PeerRpcError>(),
                documents: authorized.documents,
                selectedIds: new Set(authorized.documents.map((document) => document.documentId)),
                dirty: new Set<Identity.DocumentId>(),
                inboundReservations: new Map<number, number>(),
                outboundReservations: new Map<number, number>(),
                outboundWaiter: undefined,
                active: false,
                cleanupStarted: false,
                queued: false,
                inboundBytes: 0,
                session: undefined,
                watcher: undefined,
                requestFiber: undefined
              }
            }))
            return yield* Effect.gen(function*() {
              // Per-branch so revocation is not gated on the race awaiting the losing monitors' finalizers.
              const revokeOnInvalidation = revoke(entry, new PeerRpcError.SessionUnavailable(), true, true)
              const leaseWatcher = Effect.raceAllFirst([
                authenticated.invalidated,
                authorized.invalidated,
                Effect.sleep(Duration.millis(Math.max(0, authenticated.validUntil - now))),
                Effect.sleep(Duration.millis(Math.max(0, authorized.validUntil - now))),
                Effect.sleep(Duration.millis(limits.maximumReauthorizationInterval))
              ].map((trigger) => trigger.pipe(Effect.ensuring(revokeOnInvalidation))))
              entry.watcher = yield* Effect.forkIn(leaseWatcher, runtimeScope, { startImmediately: true })
              const registered = yield* register(entry)
              if (registered._tag !== "Registered") {
                const error = registered._tag === "Overloaded"
                  ? new PeerRpcError.SessionOverloaded()
                  : registered._tag === "Revoked"
                  ? new PeerRpcError.SessionUnavailable()
                  : registered._tag === "Unavailable"
                  ? new PeerRpcError.ServerUnavailable()
                  : new PeerRpcError.RequestCapacityExceeded()
                yield* revoke(entry, error, false, false)
                return yield* error
              }
              if (registered.previousCleanup !== undefined) {
                yield* finishCleanup(registered.previousCleanup)
              }
              yield* PeerSession.makeSupervised({ peerId: entry.peerId, documents: entry.documents }).pipe(
                Effect.provideService(PeerTransport.PeerTransport, sessionTransport(entry)),
                Effect.provideService(Scope.Scope, entry.scope),
                Effect.tap((session) =>
                  markReady(entry, session).pipe(
                    Effect.andThen(
                      Effect.raceFirst(
                        session.awaitDisconnect.pipe(
                          Effect.matchEffect({
                            onFailure: (error) => revokeForSessionFailure(entry, Cause.fail(error)),
                            onSuccess: () => Effect.void
                          })
                        ),
                        Deferred.await(entry.closed)
                      ).pipe(
                        Effect.forkIn(runtimeScope, { startImmediately: true }),
                        Effect.asVoid
                      )
                    )
                  )
                ),
                Effect.onError((cause) =>
                  entry.cleanupStarted
                    ? Effect.void
                    : revokeForSessionFailure(entry, cause).pipe(
                      Effect.forkIn(runtimeScope),
                      Effect.asVoid
                    )
                ),
                Effect.forkIn(entry.scope, { startImmediately: true })
              )
              return responseStream(entry)
            }).pipe(
              Effect.onExitIf(Exit.isFailure, () => revoke(entry, new PeerRpcError.ServerUnavailable(), false, false))
            )
          })
        )
      })

    const pushUnobserved = (request: typeof PeerRpc.PushRpc.payloadSchema.Type) =>
      Effect.gen(function*() {
        const authenticated = yield* PeerAuthentication.AuthenticatedPeer
        if (
          request.payload.byteLength > PeerSession.maximumSyncEnvelopeBytes(
            replicaLimits.maxSyncMessageBytes,
            replicaLimits.maxSyncChangesPerMessage
          )
        ) {
          return yield* new PeerRpcError.RequestLimitExceeded()
        }
        return yield* admitted(
          authenticated.principal.subjectId,
          "Push",
          Effect.gen(function*() {
            const error = new PeerRpcError.SessionOverloaded()
            const expired = new PeerRpcError.SessionUnavailable()
            const outcome = yield* lock.withPermit(Effect.gen(function*() {
              const now = yield* Clock.currentTimeMillis
              if (!Number.isFinite(authenticated.validUntil) || now >= authenticated.validUntil) {
                return { _tag: "AuthenticationExpired" as const }
              }
              if (!registry.accepting) return { _tag: "Unavailable" as const }
              const entry = registry.sessions.get(request.sessionId)
              if (
                entry === undefined || !entry.active || entry.tenantId !== authenticated.principal.tenantId ||
                entry.subjectId !== authenticated.principal.subjectId || entry.peerId !== authenticated.principal.peerId
              ) {
                return { _tag: "Unavailable" as const }
              }
              if (now >= entry.validUntil) {
                return { _tag: "Expired" as const, cleanup: detach(entry, expired, false, true) }
              }
              const bytes = request.payload.byteLength
              if (
                entry.inboundReservations.size >= limits.inboundItemCapacity ||
                entry.inboundBytes + bytes > limits.maxInboundBufferedBytesPerSession ||
                registry.bufferedBytes + bytes > limits.maxBufferedBytes
              ) {
                return { _tag: "Overloaded" as const, cleanup: detach(entry, error, false, true) }
              }
              const id = reservationId++
              entry.inboundReservations.set(id, bytes)
              entry.inboundBytes += bytes
              registry.bufferedBytes += bytes
              yield* PeerRpcObservability.modifyQueueItems("Inbound", 1)
              const offered = yield* Queue.offer(entry.inbound, { id, payload: request.payload })
              if (!offered) {
                return { _tag: "Overloaded" as const, cleanup: detach(entry, error, false, true) }
              }
              yield* PeerRpcObservability.recordBytes("Inbound", bytes)
              return { _tag: "Accepted" as const }
            }))
            if (outcome._tag === "Accepted") return
            if (outcome._tag === "AuthenticationExpired") {
              return yield* new PeerRpcError.AuthenticationFailure()
            }
            if (outcome._tag === "Unavailable") return yield* new PeerRpcError.SessionUnavailable()
            if (outcome._tag === "Expired") {
              if (outcome.cleanup !== undefined) yield* finishCleanup(outcome.cleanup)
              return yield* expired
            }
            if (outcome.cleanup !== undefined) {
              yield* finishCleanup(outcome.cleanup)
            }
            return yield* error
          })
        )
      })

    const handlerResult = (exit: Exit.Exit<unknown, PeerRpcError.PeerRpcError>) => {
      const error = PeerRpcObservability.failure(exit)
      switch (error?._tag) {
        case "AuthenticationFailure":
          return "AuthenticationDenied" as const
        case "AccessDenied":
          return "AuthorizationDenied" as const
        case "UnsupportedVersion":
        case "PeerMismatch":
        case "DefinitionMismatch":
        case "InvalidRequest":
        case "RequestLimitExceeded":
        // Peer caused, so it must not read as a server fault in effect_local_rpc_boundary_total.
        case "DocumentLineageChanged":
          return "ProtocolRejected" as const
        case "RequestCapacityExceeded":
          return "CapacityRejected" as const
        case "SessionOverloaded":
          return "Overloaded" as const
        case "SessionUnavailable":
        case "ServerUnavailable":
          return "Failure" as const
        case undefined:
          return Exit.isSuccess(exit) ? "Success" as const : "Failure" as const
      }
    }

    const open = (request: typeof PeerRpc.OpenRpc.payloadSchema.Type) =>
      PeerRpcObservability.observe({
        effect: PeerRpcObservability.recordSelectedDocuments(request.documents.length).pipe(
          Effect.andThen(openUnobserved(request))
        ),
        operation: "Open",
        spanName: "effect_local_rpc.server.open",
        attributes: { "rpc.selected_documents": request.documents.length },
        result: handlerResult
      })

    const push = (request: typeof PeerRpc.PushRpc.payloadSchema.Type) =>
      PeerRpcObservability.observe({
        effect: pushUnobserved(request),
        operation: "Push",
        spanName: "effect_local_rpc.server.push",
        attributes: { "rpc.payload_bytes": request.payload.byteLength },
        result: handlerResult
      })

    yield* Stream.runForEach(subscription.events, (event) =>
      lock.withPermit(Effect.sync(() => {
        const entries = event._tag === "FullRefreshRequired"
          ? [...registry.sessions.values()]
          : [...(registry.documents.get(event.documentId) ?? [])].flatMap((sessionId) => {
            const entry = registry.sessions.get(sessionId)
            return entry === undefined ? [] : [entry]
          })
        const enqueue: Array<Identity.SessionId> = []
        for (const entry of entries) {
          if (!entry.active) continue
          if (event._tag === "FullRefreshRequired") {
            for (const documentId of entry.selectedIds) entry.dirty.add(documentId)
          } else {
            entry.dirty.add(event.documentId)
          }
          if (entry.session !== undefined && !entry.queued) {
            entry.queued = true
            enqueue.push(entry.sessionId)
          }
        }
        return enqueue
      })).pipe(
        Effect.flatMap((entries) => Queue.offerAll(readySessions, entries)),
        Effect.asVoid
      )).pipe(
        Effect.andThen(stopAll(new PeerRpcError.ServerUnavailable(), false)),
        Effect.catchCause(() => stopAll(new PeerRpcError.ServerUnavailable(), false)),
        Effect.forkIn(runtimeScope, { startImmediately: true })
      )

    for (let index = 0; index < limits.commitFlushConcurrency; index++) {
      yield* Effect.gen(function*() {
        while (true) {
          const sessionId = yield* Queue.take(readySessions)
          const work = yield* lock.withPermit(Effect.gen(function*() {
            const entry = registry.sessions.get(sessionId)
            if (entry === undefined || !entry.active || entry.session === undefined) return undefined
            if ((yield* Clock.currentTimeMillis) >= entry.validUntil) {
              return {
                _tag: "Expired" as const,
                cleanup: detach(entry, new PeerRpcError.SessionUnavailable(), false, true)
              }
            }
            const dirty = [...entry.dirty]
            entry.dirty.clear()
            return { _tag: "Ready" as const, entry, session: entry.session, dirty }
          }))
          if (work === undefined) continue
          if (work._tag === "Expired") {
            if (work.cleanup !== undefined) yield* finishCleanup(work.cleanup)
            continue
          }
          const exit = yield* Effect.forEach(work.dirty, work.session.markDirty, { discard: true }).pipe(
            Effect.andThen(work.session.flush),
            Effect.exit
          )
          if (Exit.isFailure(exit)) {
            yield* revokeForSessionFailure(work.entry, exit.cause)
            continue
          }
          const requeue = yield* lock.withPermit(Effect.sync(() => {
            if (!work.entry.active) return false
            work.entry.queued = false
            if (work.entry.dirty.size === 0) return false
            work.entry.queued = true
            return true
          }))
          if (requeue) yield* Queue.offer(readySessions, work.entry.sessionId)
        }
      }).pipe(Effect.forkIn(runtimeScope, { startImmediately: true }))
    }

    yield* Effect.addFinalizer(() =>
      stopAll(new PeerRpcError.ServerUnavailable(), true).pipe(
        Effect.andThen(Queue.shutdown(readySessions)),
        Effect.asVoid
      )
    )

    return PeerRpc.Rpcs.of({
      Open: (request) => Stream.unwrap(open(request)),
      Push: push
    })
  }))

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

export interface PeerRelayServerUsage {
  readonly accepting: boolean
  readonly sessions: number
  readonly subjects: number
  readonly activeClaims: number
  readonly queuedChannels: number
}

export class PeerRelayServerRuntime extends Context.Service<PeerRelayServerRuntime, {
  readonly health: Effect.Effect<void, PeerRpcError.ServerUnavailable>
  readonly owner: Effect.Effect<never, unknown>
  readonly shutdown: Effect.Effect<void>
  readonly usage: Effect.Effect<PeerRelayServerUsage>
}>()("@lucas-barake/effect-local-rpc/PeerRelayServerRuntime") {}

class PeerRelayFatalSignal extends Context.Service<PeerRelayFatalSignal, {
  readonly signal: (cause: Cause.Cause<unknown>) => Effect.Effect<void>
}>()("@lucas-barake/effect-local-rpc/PeerRelayFatalSignal") {}

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
  readonly event: PeerRelayRpc.StoredMessage
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

export const layerRelayHandlers = (
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
          protocolVersion: PeerRelayRpc.protocolVersion,
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
              const event = PeerRelayRpc.StoredMessage.make({
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
      request: typeof PeerRelayRpc.OpenRelayRpc.payloadSchema.Type
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
            if (request.version !== PeerRelayRpc.protocolVersion) {
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
                return yield* restore(Effect.gen(function*() {
                  if (entry.replaced !== undefined) yield* detachEntry(entry.replaced, false)
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
                  const opened = PeerRelayRpc.RelayOpened.make({
                    _tag: "RelayOpened",
                    version: PeerRelayRpc.protocolVersion,
                    sessionId,
                    remotePeerId: entry.entry.remote.peerId,
                    authenticatedLocal: principal,
                    capabilities: { storeAndForward: true }
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
                })).pipe(
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

    const push = (request: typeof PeerRelayRpc.PushRelayRpc.payloadSchema.Type) =>
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
              protocolVersion: PeerRelayRpc.protocolVersion,
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
        | typeof PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema.Type
        | typeof PeerRelayRpc.RejectRelayRpc.payloadSchema.Type,
      reason: PeerRelayRpc.RejectReason | undefined
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

    const runtime = PeerRelayServerRuntime.of({
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
    const handlerContext = yield* PeerRelayRpc.Rpcs.toHandlers(PeerRelayRpc.Rpcs.of({
      OpenRelay: (request) => Stream.unwrap(open(request)),
      PushRelay: push,
      AcknowledgeRelay: (request) => terminal(request, undefined),
      RejectRelay: (request) => terminal(request, request.reason)
    }))
    return Context.add(handlerContext, PeerRelayServerRuntime, runtime).pipe(
      Context.add(PeerRelayFatalSignal, PeerRelayFatalSignal.of({ signal: signalFatal }))
    )
  }))

export const layerRelayServer = Layer.effectDiscard(Effect.gen(function*() {
  const scope = yield* Scope.Scope
  const runtime = yield* PeerRelayServerRuntime
  const fatalSignal = yield* PeerRelayFatalSignal
  const ingress = yield* PeerRelayIngress.PeerRelayIngress
  let stopping = false
  const serverFiber = yield* Effect.forkIn(
    RpcServer.make(PeerRelayRpc.Rpcs, { disableFatalDefects: true }),
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

export const layerStoreAndForwardDeployment = <
  DirectOut,
  DirectError,
  DirectRequirements,
  RelaySocketError,
  RelaySocketRequirements,
>(options: {
  readonly directDeployment: Layer.Layer<DirectOut, DirectError, DirectRequirements>
  readonly relaySocketLayer: Layer.Layer<
    SocketServer.SocketServer,
    RelaySocketError,
    RelaySocketRequirements
  >
  readonly relay: {
    readonly tenantId: string
    readonly peerId: Identity.PeerId
  }
}) => {
  const ingress = PeerRelayIngress.layerProtocolSocketServer(options.relaySocketLayer)
  const handlers = layerRelayHandlers(options.relay)
  const relay = layerRelayServer.pipe(
    Layer.provideMerge(handlers),
    Layer.provideMerge(ingress)
  )
  return Layer.merge(options.directDeployment, relay)
}
