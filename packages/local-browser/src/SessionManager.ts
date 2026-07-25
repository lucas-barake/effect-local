import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"

export const leaseDurationMillis = 60_000

interface SessionEntry {
  readonly token: symbol
  readonly clientId: number
  readonly expiresAt: number
  readonly activeRestores: number
  readonly inFlight: Semaphore.Semaphore
  readonly streams: Semaphore.Semaphore
  readonly expired: Deferred.Deferred<never, ReplicaError.ReplicaError>
}

interface SessionState {
  readonly sessions: ReadonlyMap<Identity.SessionId, SessionEntry>
  readonly activeRestores: number
}

export interface RestoreLease {
  readonly expired: Deferred.Deferred<never, ReplicaError.ReplicaError>
  readonly release: Effect.Effect<void>
}

export class SessionManager extends Context.Service<SessionManager, {
  readonly maxChunkBytes: number
  readonly maxActiveRestores: number
  readonly effectiveRestoreCapacity: number
  readonly maxRestoresPerSession: number
  readonly maxRestoreMillis: number
  readonly maxRestorePullMillis: number
  readonly maxRestoreCoalesceMillis: number
  readonly maxRestoreErrorBytes: number
  readonly open: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly renew: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly close: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly contains: (sessionId: Identity.SessionId) => Effect.Effect<boolean>
  readonly activeCount: Effect.Effect<number>
  readonly acquireRestore: (
    sessionId: Identity.SessionId,
    clientId: number
  ) => Effect.Effect<RestoreLease, ReplicaError.ReplicaError>
  readonly activeRestoreCount: Effect.Effect<number>
  readonly activeRestoreCountForSession: (sessionId: Identity.SessionId) => Effect.Effect<number>
  readonly run: <A, E, R,>(
    sessionId: Identity.SessionId,
    clientId: number,
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | ReplicaError.ReplicaError, R>
  readonly stream: <A, E, R,>(
    sessionId: Identity.SessionId,
    clientId: number,
    stream: Stream.Stream<A, E, R>
  ) => Stream.Stream<A, E | ReplicaError.ReplicaError, R>
}>()(
  "@lucas-barake/effect-local-browser/SessionManager"
) {}

export const layer = Layer.effect(
  SessionManager,
  Effect.gen(function*() {
    const limits = yield* ReplicaLimits.ReplicaLimits
    const restoreCapacity = Math.min(
      limits.maxActiveRestores,
      limits.maxSessions * limits.maxRestoresPerSession
    )
    const state = yield* Ref.make<SessionState>({
      sessions: new Map(),
      activeRestores: 0
    })
    const queued = yield* Ref.make(0)

    const expire = Effect.fnUntraced(function*(now: number) {
      const expired = yield* Ref.modify(state, (current) => {
        const live = new Map<Identity.SessionId, SessionEntry>()
        const expired: Array<readonly [Identity.SessionId, SessionEntry]> = []
        for (const entry of current.sessions) {
          if (entry[1].expiresAt <= now) expired.push(entry)
          else live.set(...entry)
        }
        return [expired, { ...current, sessions: live }]
      })
      yield* Effect.forEach(
        expired,
        ([sessionId, entry]) =>
          Deferred.fail(
            entry.expired,
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
          ),
        { discard: true }
      )
    })

    yield* Effect.sleep(leaseDurationMillis / 4).pipe(
      Effect.andThen(Clock.currentTimeMillis),
      Effect.flatMap(expire),
      Effect.forever,
      Effect.forkScoped
    )

    const active = Effect.fnUntraced(function*(sessionId: Identity.SessionId) {
      const now = yield* Clock.currentTimeMillis
      const [result, expired] = yield* Ref.modify(state, (current): readonly [
        readonly [Result.Result<SessionEntry, ReplicaError.ReplicaError>, SessionEntry | undefined],
        SessionState
      ] => {
        const entry = current.sessions.get(sessionId)
        if (entry === undefined) {
          return [[
            Result.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProtocolMismatch({
                  expected: "active session",
                  observed: sessionId
                })
              })
            ),
            undefined
          ], current]
        }
        if (entry.expiresAt > now) return [[Result.succeed(entry), undefined], current]
        const next = new Map(current.sessions)
        next.delete(sessionId)
        return [[
          Result.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
          ),
          entry
        ], { ...current, sessions: next }]
      })
      if (expired !== undefined) {
        yield* Deferred.fail(
          expired.expired,
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "active session",
              observed: sessionId
            })
          })
        )
      }
      return yield* Effect.fromResult(result)
    })

    const owned = (sessionId: Identity.SessionId, clientId: number) =>
      active(sessionId).pipe(
        Effect.filterOrFail(
          (entry) => entry.clientId === clientId,
          () =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
        )
      )

    const validate = (sessionId: Identity.SessionId, clientId: number, token: symbol) =>
      owned(sessionId, clientId).pipe(
        Effect.filterOrFail(
          (entry) => entry.token === token,
          () =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
        )
      )

    const acquireQueued = Effect.gen(function*() {
      const admitted = yield* Ref.modify(
        queued,
        (current) => current >= limits.maxQueuedRpc ? [false, current] as const : [true, current + 1] as const
      )
      if (!admitted) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.QuotaExceeded({
            resource: "queued RPCs",
            limit: limits.maxQueuedRpc
          })
        })
      }
    })
    const releaseQueued = Ref.update(queued, (current) => current - 1)

    const acquireRestore: SessionManager["Service"]["acquireRestore"] = (sessionId, clientId) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const released = yield* Ref.make(false)
        const [result, expired] = yield* Ref.modify(state, (current): readonly [
          readonly [
            Result.Result<Pick<SessionEntry, "expired" | "token">, ReplicaError.ReplicaError>,
            SessionEntry | undefined
          ],
          SessionState
        ] => {
          const entry = current.sessions.get(sessionId)
          if (entry === undefined || entry.expiresAt <= now) {
            const next = new Map(current.sessions)
            next.delete(sessionId)
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "active session",
                    observed: sessionId
                  })
                })
              ),
              entry
            ], { ...current, sessions: next }]
          }
          if (entry.clientId !== clientId) {
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "active session",
                    observed: sessionId
                  })
                })
              ),
              undefined
            ], current]
          }
          if (current.activeRestores >= restoreCapacity) {
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.QuotaExceeded({
                    resource: "active restores",
                    limit: restoreCapacity
                  })
                })
              ),
              undefined
            ], current]
          }
          if (entry.activeRestores >= limits.maxRestoresPerSession) {
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.QuotaExceeded({
                    resource: "active restores per session",
                    limit: limits.maxRestoresPerSession
                  })
                })
              ),
              undefined
            ], current]
          }
          const next = new Map(current.sessions)
          next.set(sessionId, {
            ...entry,
            activeRestores: entry.activeRestores + 1
          })
          return [[
            Result.succeed({ expired: entry.expired, token: entry.token }),
            undefined
          ], {
            sessions: next,
            activeRestores: current.activeRestores + 1
          }]
        })
        if (expired !== undefined) {
          yield* Deferred.fail(
            expired.expired,
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
          )
        }
        const reservation = yield* Effect.fromResult(result)
        const release = Effect.uninterruptible(
          Ref.modify(released, (current) => current ? [false, true] : [true, true]).pipe(
            Effect.flatMap((owned) => {
              if (!owned) return Effect.void
              return Ref.update(state, (current) => {
                const entry = current.sessions.get(sessionId)
                if (entry === undefined || entry.token !== reservation.token) {
                  return {
                    ...current,
                    activeRestores: current.activeRestores - 1
                  }
                }
                const next = new Map(current.sessions)
                next.set(sessionId, {
                  ...entry,
                  activeRestores: entry.activeRestores - 1
                })
                return {
                  sessions: next,
                  activeRestores: current.activeRestores - 1
                }
              })
            })
          )
        )
        return {
          expired: reservation.expired,
          release
        }
      }).pipe(Effect.uninterruptible)

    const run: SessionManager["Service"]["run"] = (sessionId, clientId, effect) =>
      Effect.acquireUseRelease(
        acquireQueued,
        () =>
          Effect.gen(function*() {
            const entry = yield* owned(sessionId, clientId)
            return yield* Effect.raceFirst(
              entry.inFlight.withPermit(validate(sessionId, clientId, entry.token).pipe(Effect.andThen(effect))),
              Deferred.await(entry.expired)
            )
          }),
        () => releaseQueued
      )

    const stream: SessionManager["Service"]["stream"] = (sessionId, clientId, source) =>
      Effect.gen(function*() {
        yield* Effect.acquireRelease(acquireQueued, () => releaseQueued)
        const entry = yield* owned(sessionId, clientId)
        return yield* Effect.raceFirst(
          Effect.gen(function*() {
            yield* Effect.acquireRelease(entry.streams.take(1), () => entry.streams.release(1))
            yield* Effect.acquireRelease(entry.inFlight.take(1), () => entry.inFlight.release(1))
            yield* validate(sessionId, clientId, entry.token)
            return source.pipe(Stream.interruptWhen(Deferred.await(entry.expired)))
          }),
          Deferred.await(entry.expired)
        )
      }).pipe(Stream.unwrap, Stream.scoped)

    return {
      maxChunkBytes: limits.maxChunkBytes,
      maxActiveRestores: limits.maxActiveRestores,
      effectiveRestoreCapacity: restoreCapacity,
      maxRestoresPerSession: limits.maxRestoresPerSession,
      maxRestoreMillis: limits.maxRestoreMillis,
      maxRestorePullMillis: limits.maxRestorePullMillis,
      maxRestoreCoalesceMillis: limits.maxRestoreCoalesceMillis,
      maxRestoreErrorBytes: limits.maxRestoreErrorBytes,
      open: Effect.fnUntraced(function*(sessionId, clientId) {
        const now = yield* Clock.currentTimeMillis
        const inFlight = yield* Semaphore.make(limits.maxInFlightPerSession)
        const streams = yield* Semaphore.make(limits.maxStreamsPerSession)
        const expired = yield* Deferred.make<never, ReplicaError.ReplicaError>()
        const [result, expiredEntries] = yield* Ref.modify(state, (current): readonly [
          readonly [
            Result.Result<void, ReplicaError.ReplicaError>,
            ReadonlyArray<readonly [Identity.SessionId, SessionEntry]>
          ],
          SessionState
        ] => {
          const live = new Map<Identity.SessionId, SessionEntry>()
          const expiredEntries: Array<readonly [Identity.SessionId, SessionEntry]> = []
          for (const entry of current.sessions) {
            if (entry[1].expiresAt <= now) expiredEntries.push(entry)
            else live.set(...entry)
          }
          const existing = live.get(sessionId)
          if (existing !== undefined) {
            if (existing.clientId !== clientId) {
              return [[
                Result.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "active session",
                      observed: sessionId
                    })
                  })
                ),
                expiredEntries
              ], { ...current, sessions: live }]
            }
            live.set(sessionId, { ...existing, expiresAt: now + leaseDurationMillis })
            return [[Result.void, expiredEntries], { ...current, sessions: live }]
          }
          if (live.size >= limits.maxSessions) {
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.QuotaExceeded({
                    resource: "sessions",
                    limit: limits.maxSessions
                  })
                })
              ),
              expiredEntries
            ], { ...current, sessions: live }]
          }
          live.set(sessionId, {
            token: Symbol(),
            clientId,
            expiresAt: now + leaseDurationMillis,
            activeRestores: 0,
            inFlight,
            streams,
            expired
          })
          return [[Result.void, expiredEntries], { ...current, sessions: live }]
        })
        yield* Effect.forEach(
          expiredEntries,
          ([expiredSessionId, entry]) =>
            Deferred.fail(
              entry.expired,
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProtocolMismatch({
                  expected: "active session",
                  observed: expiredSessionId
                })
              })
            ),
          { discard: true }
        )
        return yield* Effect.fromResult(result)
      }),
      renew: Effect.fnUntraced(function*(sessionId, clientId) {
        const now = yield* Clock.currentTimeMillis
        const [result, expired] = yield* Ref.modify(state, (current): readonly [
          readonly [Result.Result<void, ReplicaError.ReplicaError>, SessionEntry | undefined],
          SessionState
        ] => {
          const entry = current.sessions.get(sessionId)
          if (entry === undefined || entry.expiresAt <= now) {
            const next = new Map(current.sessions)
            next.delete(sessionId)
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "active session",
                    observed: sessionId
                  })
                })
              ),
              entry
            ], { ...current, sessions: next }]
          }
          if (entry.clientId !== clientId) {
            return [[
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "active session",
                    observed: sessionId
                  })
                })
              ),
              undefined
            ], current]
          }
          const next = new Map(current.sessions)
          next.set(sessionId, { ...entry, expiresAt: now + leaseDurationMillis })
          return [[Result.void, undefined], { ...current, sessions: next }]
        })
        if (expired !== undefined) {
          yield* Deferred.fail(
            expired.expired,
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
          )
        }
        return yield* Effect.fromResult(result)
      }),
      close: Effect.fnUntraced(function*(sessionId, clientId) {
        const result = yield* Ref.modify(state, (current): readonly [
          Result.Result<SessionEntry | undefined, ReplicaError.ReplicaError>,
          SessionState
        ] => {
          const entry = current.sessions.get(sessionId)
          if (entry === undefined) return [Result.succeed(undefined), current]
          if (entry.clientId !== clientId) {
            return [
              Result.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "active session",
                    observed: sessionId
                  })
                })
              ),
              current
            ]
          }
          const next = new Map(current.sessions)
          next.delete(sessionId)
          return [Result.succeed(entry), { ...current, sessions: next }]
        })
        const entry = yield* Effect.fromResult(result)
        if (entry !== undefined) {
          yield* Deferred.fail(
            entry.expired,
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: "active session",
                observed: sessionId
              })
            })
          )
        }
      }),
      contains: (sessionId) => Effect.isSuccess(active(sessionId)),
      activeCount: Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        yield* expire(now)
        return (yield* Ref.get(state)).sessions.size
      }),
      acquireRestore,
      activeRestoreCount: Ref.get(state).pipe(Effect.map((current) => current.activeRestores)),
      activeRestoreCountForSession: (sessionId) =>
        Ref.get(state).pipe(Effect.map((current) => current.sessions.get(sessionId)?.activeRestores ?? 0)),
      run,
      stream
    }
  })
)
