import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as PeerAuthorizationValidation from "./internal/peerAuthorization.js"
import * as PeerRpcObservability from "./internal/peerRpcObservability.js"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerAuthorization from "./PeerAuthorization.js"
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
      onSome: (error) =>
        error.reason._tag === "StorageUnavailable" && Cause.isTimeoutError(error.reason.cause)
          ? new PeerRpcError.SessionOverloaded()
          : new PeerRpcError.ServerUnavailable()
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
        const elapsed = Math.max(0, now - (operation === "Open" ? subject.openUpdatedAt : subject.pushUpdatedAt))
        if (operation === "Open") {
          subject.openTokens = Math.min(
            limits.openBurst,
            subject.openTokens + elapsed / 1_000 * limits.openRatePerSecond
          )
          subject.openUpdatedAt = now
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
          subject.pushUpdatedAt = now
          if (subject.pushTokens < 1 || subject.pushInFlight >= limits.maxInFlightPushPerSubject) {
            retainInactiveSubject(subjectId, subject)
            return "Capacity" as const
          }
          subject.pushTokens -= 1
          subject.pushInFlight += 1
        }
        subject.lastUsedAt = now
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
          const item = yield* Queue.take(entry.inbound)
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
        if (bytes > PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes)) {
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
        capabilities: { storeAndForward: false },
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
              capabilities: { storeAndForward: false },
              receive,
              send,
              close: Effect.void
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
                    capabilities: { storeAndForward: false }
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
        if (request.definitionHash !== options.definition.hash) return yield* new PeerRpcError.DefinitionMismatch()
        if (request.documents.length === 0) return yield* new PeerRpcError.InvalidRequest()
        const requested = new Set(request.documents.map((entry) => `${entry.documentType}:${entry.documentId}`))
        if (requested.size !== request.documents.length) return yield* new PeerRpcError.InvalidRequest()
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
              const leaseWatcher = Effect.raceAllFirst([
                authenticated.invalidated,
                authorized.invalidated,
                Effect.sleep(Duration.millis(Math.max(0, authenticated.validUntil - now))),
                Effect.sleep(Duration.millis(Math.max(0, authorized.validUntil - now))),
                Effect.sleep(Duration.millis(limits.maximumReauthorizationInterval))
              ]).pipe(
                Effect.ensuring(revoke(entry, new PeerRpcError.SessionUnavailable(), true, true))
              )
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
        if (request.payload.byteLength > PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes)) {
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
