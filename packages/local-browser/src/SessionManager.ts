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
  readonly inFlight: Semaphore.Semaphore
  readonly streams: Semaphore.Semaphore
  readonly expired: Deferred.Deferred<never, ReplicaError.ReplicaError>
}

export class SessionManager extends Context.Service<SessionManager, {
  readonly open: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly renew: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly close: (sessionId: Identity.SessionId, clientId: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly contains: (sessionId: Identity.SessionId) => Effect.Effect<boolean>
  readonly activeCount: Effect.Effect<number>
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
    const sessions = yield* Ref.make<ReadonlyMap<Identity.SessionId, SessionEntry>>(new Map())
    const queued = yield* Ref.make(0)

    const expire = Effect.fnUntraced(function*(now: number) {
      const expired = yield* Ref.modify(sessions, (current) => {
        const live = new Map<Identity.SessionId, SessionEntry>()
        const expired: Array<readonly [Identity.SessionId, SessionEntry]> = []
        for (const entry of current) {
          if (entry[1].expiresAt <= now) expired.push(entry)
          else live.set(...entry)
        }
        return [expired, live]
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
      const [result, expired] = yield* Ref.modify(sessions, (current): readonly [
        readonly [Result.Result<SessionEntry, ReplicaError.ReplicaError>, SessionEntry | undefined],
        ReadonlyMap<Identity.SessionId, SessionEntry>
      ] => {
        const entry = current.get(sessionId)
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
        const next = new Map(current)
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
        ], next]
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
      open: Effect.fnUntraced(function*(sessionId, clientId) {
        const now = yield* Clock.currentTimeMillis
        const inFlight = yield* Semaphore.make(limits.maxInFlightPerSession)
        const streams = yield* Semaphore.make(limits.maxStreamsPerSession)
        const expired = yield* Deferred.make<never, ReplicaError.ReplicaError>()
        const [result, expiredEntries] = yield* Ref.modify(sessions, (current): readonly [
          readonly [
            Result.Result<void, ReplicaError.ReplicaError>,
            ReadonlyArray<readonly [Identity.SessionId, SessionEntry]>
          ],
          ReadonlyMap<Identity.SessionId, SessionEntry>
        ] => {
          const live = new Map<Identity.SessionId, SessionEntry>()
          const expiredEntries: Array<readonly [Identity.SessionId, SessionEntry]> = []
          for (const entry of current) {
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
              ], live]
            }
            live.set(sessionId, { ...existing, expiresAt: now + leaseDurationMillis })
            return [[Result.void, expiredEntries], live]
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
            ], live]
          }
          live.set(sessionId, {
            token: Symbol(),
            clientId,
            expiresAt: now + leaseDurationMillis,
            inFlight,
            streams,
            expired
          })
          return [[Result.void, expiredEntries], live]
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
        const [result, expired] = yield* Ref.modify(sessions, (current): readonly [
          readonly [Result.Result<void, ReplicaError.ReplicaError>, SessionEntry | undefined],
          ReadonlyMap<Identity.SessionId, SessionEntry>
        ] => {
          const entry = current.get(sessionId)
          if (entry === undefined || entry.expiresAt <= now) {
            const next = new Map(current)
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
            ], next]
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
          const next = new Map(current)
          next.set(sessionId, { ...entry, expiresAt: now + leaseDurationMillis })
          return [[Result.void, undefined], next]
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
        const result = yield* Ref.modify(sessions, (current): readonly [
          Result.Result<SessionEntry | undefined, ReplicaError.ReplicaError>,
          ReadonlyMap<Identity.SessionId, SessionEntry>
        ] => {
          const entry = current.get(sessionId)
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
          const next = new Map(current)
          next.delete(sessionId)
          return [Result.succeed(entry), next]
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
        return (yield* Ref.get(sessions)).size
      }),
      run,
      stream
    }
  })
)
