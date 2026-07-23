import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type { PeerPrincipal } from "./internal/peerPrincipal.js"
import * as PeerRpcObservability from "./internal/peerRpcObservability.js"
import * as PeerAuthenticator from "./PeerAuthenticator.js"
import * as PeerCredentials from "./PeerCredentials.js"
import * as PeerRpcError from "./PeerRpcError.js"
import * as PeerRpcLimits from "./PeerRpcLimits.js"

export { PeerPrincipal } from "./internal/peerPrincipal.js"

const AuthenticationError = Schema.Union([
  PeerRpcError.AuthenticationFailure,
  PeerRpcError.RequestCapacityExceeded
])

export class AuthenticatedPeer extends Context.Service<AuthenticatedPeer, {
  readonly principal: PeerPrincipal
  readonly validUntil: number
  readonly invalidated: Effect.Effect<void>
}>()("@lucas-barake/effect-local-rpc/AuthenticatedPeer") {}

export class PeerAuthentication extends RpcMiddleware.Service<PeerAuthentication, {
  provides: AuthenticatedPeer
}>()("@lucas-barake/effect-local-rpc/PeerAuthentication", {
  error: AuthenticationError,
  requiredForClient: true
}) {}

export const layerServer = Layer.effect(
  PeerAuthentication,
  Effect.gen(function*() {
    const authenticator = yield* PeerAuthenticator.PeerAuthenticator
    const limits = yield* PeerRpcLimits.PeerRpcLimits
    const verifierPermits = yield* Semaphore.make(limits.maxInFlightAuthentication)
    const rateStateLock = yield* Semaphore.make(1)
    const rateState = new Map<number, {
      tokens: number
      updatedAt: number
      lastUsedAt: number
      inFlight: number
    }>()
    const inactiveRateState = new Map<number, {
      tokens: number
      updatedAt: number
      lastUsedAt: number
      inFlight: number
    }>()

    const expireOldestInactive = (now: number) => {
      const oldest = inactiveRateState.entries().next().value
      if (oldest === undefined) return
      if (now - oldest[1].lastUsedAt < limits.rateLimitIdleRetention) return
      inactiveRateState.delete(oldest[0])
      rateState.delete(oldest[0])
    }

    const retainInactive = (clientId: number, entry: {
      tokens: number
      updatedAt: number
      lastUsedAt: number
      inFlight: number
    }) => {
      inactiveRateState.delete(clientId)
      inactiveRateState.set(clientId, entry)
    }

    const admit = (clientId: number, now: number) =>
      rateStateLock.withPermit(Effect.sync(() => {
        expireOldestInactive(now)
        let entry = rateState.get(clientId)
        if (entry?.inFlight === 0) {
          inactiveRateState.delete(clientId)
          if (now - entry.lastUsedAt >= limits.rateLimitIdleRetention) {
            rateState.delete(clientId)
            entry = undefined
          }
        }
        if (entry === undefined) {
          if (rateState.size >= limits.maxRetainedRateLimitedConnections) {
            const evictable = inactiveRateState.keys().next().value
            if (evictable === undefined) return false
            inactiveRateState.delete(evictable)
            rateState.delete(evictable)
          }
          entry = {
            tokens: limits.authenticationBurst,
            updatedAt: now,
            lastUsedAt: now,
            inFlight: 0
          }
          rateState.set(clientId, entry)
        }

        const effectiveNow = Math.max(now, entry.updatedAt, entry.lastUsedAt)
        entry.tokens = Math.min(
          limits.authenticationBurst,
          entry.tokens + (Math.max(0, effectiveNow - entry.updatedAt) / 1_000) * limits.authenticationRatePerSecond
        )
        entry.updatedAt = effectiveNow
        entry.lastUsedAt = effectiveNow
        if (entry.tokens < 1) {
          if (entry.inFlight === 0) retainInactive(clientId, entry)
          return false
        }
        entry.tokens -= 1
        entry.inFlight += 1
        return true
      }))

    const release = (clientId: number, now: number) =>
      rateStateLock.withPermit(Effect.sync(() => {
        const entry = rateState.get(clientId)
        if (entry === undefined) return
        entry.inFlight -= 1
        entry.lastUsedAt = Math.max(now, entry.lastUsedAt)
        if (entry.inFlight === 0) retainInactive(clientId, entry)
      }))

    return PeerAuthentication.of((effect, options) =>
      PeerRpcObservability.observe({
        effect: Effect.gen(function*() {
          if (typeof options.payload !== "object" || options.payload === null || !("credential" in options.payload)) {
            return yield* new PeerRpcError.AuthenticationFailure()
          }
          if (!Redacted.isRedacted(options.payload.credential)) {
            return yield* new PeerRpcError.AuthenticationFailure()
          }
          const credential = options.payload.credential as Redacted.Redacted<string>

          const admittedAt = yield* Clock.currentTimeMillis
          if (!(yield* admit(options.client.id, admittedAt))) {
            return yield* new PeerRpcError.RequestCapacityExceeded()
          }

          const verified = yield* verifierPermits.withPermitsIfAvailable(1)(
            authenticator.authenticate(credential).pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.fail(new PeerRpcError.AuthenticationFailure())
              )
            )
          ).pipe(
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.fail(new PeerRpcError.RequestCapacityExceeded()),
                onSome: Effect.succeed
              })
            ),
            Effect.ensuring(Clock.currentTimeMillis.pipe(Effect.flatMap((now) => release(options.client.id, now))))
          )
          const now = yield* Clock.currentTimeMillis
          if (!Number.isFinite(verified.validUntil) || verified.validUntil <= now) {
            return yield* new PeerRpcError.AuthenticationFailure()
          }
          return verified
        }),
        operation: "Authentication",
        spanName: "effect_local_rpc.authentication",
        attributes: {},
        result: (exit) => {
          const error = PeerRpcObservability.failure(exit)
          return error instanceof PeerRpcError.AuthenticationFailure
            ? "AuthenticationDenied"
            : error instanceof PeerRpcError.RequestCapacityExceeded
            ? "CapacityRejected"
            : Exit.isSuccess(exit)
            ? "Success"
            : "Failure"
        }
      }).pipe(Effect.flatMap((verified) => Effect.provideService(effect, AuthenticatedPeer, verified)))
    )
  })
)

export const layerClient = RpcMiddleware.layerClient(
  PeerAuthentication,
  PeerCredentials.PeerCredentials.pipe(
    Effect.map((credentials) => ({ next, request }) =>
      credentials.get.pipe(
        Effect.flatMap((credential) =>
          next(
            {
              ...request,
              payload: {
                ...(typeof request.payload === "object" && request.payload !== null ? request.payload : {}),
                credential
              }
            } as typeof request
          )
        )
      )
    )
  )
)
