import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Tracer from "effect/Tracer"
import * as Headers from "effect/unstable/http/Headers"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"

const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const principal = PeerAuthentication.PeerPrincipal.make({ tenantId: "tenant", subjectId: "subject", peerId })
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")

const invoke = (
  middleware: PeerAuthentication.PeerAuthentication["Service"],
  payload: unknown,
  clientId: number,
  headers = Headers.empty,
  rpc: typeof PeerRpc.OpenRpc | typeof PeerRpc.PushRpc = PeerRpc.PushRpc
) =>
  middleware(PeerAuthentication.AuthenticatedPeer as never, {
    client: new Rpc.ServerClient(clientId),
    requestId: RpcMessage.RequestId(clientId),
    rpc,
    payload,
    headers
  }) as unknown as Effect.Effect<
    PeerAuthentication.AuthenticatedPeer["Service"],
    PeerRpcError.AuthenticationFailure | PeerRpcError.RequestCapacityExceeded,
    Scope.Scope
  >

const authenticated = {
  principal,
  validUntil: Number.MAX_SAFE_INTEGER,
  invalidated: Effect.void
}

describe("PeerAuthentication", () => {
  it.effect("supports constant credentials", () =>
    Effect.gen(function*() {
      const authenticatedWith = yield* Deferred.make<Redacted.Redacted<string>>()
      const handlers = PeerRpc.Rpcs.toLayer(PeerRpc.Rpcs.of({
        Open: () => Stream.empty,
        Push: () => Effect.void
      }))
      const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
        Effect.provide(handlers),
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: (value) => Deferred.succeed(authenticatedWith, value).pipe(Effect.as(authenticated))
        }),
        Effect.provide(PeerAuthentication.layerClient),
        Effect.provideService(PeerCredentials.PeerCredentials, {
          get: Effect.succeed(Redacted.make("constant"))
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
      )

      yield* client.Push({ sessionId, payload: Uint8Array.of(1) })
      assert.strictEqual(Redacted.value(yield* Deferred.await(authenticatedWith)), "constant")
    }))

  it.effect("resolves credential dependencies while constructing the service layer", () => {
    const CredentialValue = Context.Service<string>("test/PeerAuthentication/CredentialValue")
    const CredentialsLive = Layer.effect(
      PeerCredentials.PeerCredentials,
      CredentialValue.pipe(
        Effect.map((value) => ({ get: Effect.succeed(Redacted.make(value)) }))
      )
    ).pipe(Layer.provide(Layer.succeed(CredentialValue, "context")))

    return Effect.gen(function*() {
      const credentials = yield* PeerCredentials.PeerCredentials
      assert.strictEqual(Redacted.value(yield* credentials.get), "context")
    }).pipe(Effect.provide(CredentialsLive))
  })

  it.effect("provides the principal and rotates overwritten client credentials per request", () =>
    Effect.gen(function*() {
      const credentials: Array<string> = []
      let credential = "first"
      const handlers = PeerRpc.Rpcs.toLayer(PeerRpc.Rpcs.of({
        Open: () =>
          Stream.fromEffect(PeerAuthentication.AuthenticatedPeer).pipe(
            Stream.map((authenticated) => {
              assert.deepStrictEqual(authenticated.principal, principal)
              return PeerRpc.Opened.make({
                _tag: "Opened",
                protocolVersion: PeerRpc.protocolVersion,
                sessionId,
                peerId,
                capabilities: { storeAndForward: false }
              })
            })
          ),
        Push: () => PeerAuthentication.AuthenticatedPeer.pipe(Effect.asVoid)
      }))
      const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
        Effect.provide(handlers),
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provide(
          Layer.succeed(PeerAuthenticator.PeerAuthenticator, {
            authenticate: (value) => {
              credentials.push(Redacted.value(value))
              return Effect.succeed(authenticated)
            }
          })
        ),
        Effect.provide(PeerAuthentication.layerClient),
        Effect.provide(
          Layer.succeed(PeerCredentials.PeerCredentials, {
            get: Effect.sync(() => Redacted.make(credential))
          })
        ),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
      )

      yield* client.Open({
        protocolVersion: PeerRpc.protocolVersion,
        expectedPeerId: peerId,
        definitionHash: "def_test",
        documents: [],
        credential: Redacted.make("caller")
      }).pipe(Stream.runDrain)
      credential = "second"
      yield* client.Push({ sessionId, payload: Uint8Array.of(1) })
      assert.deepStrictEqual(credentials, ["first", "second"])
    }))

  it.effect("awaits asynchronous credentials before invoking the authenticator", () =>
    Effect.gen(function*() {
      const requested = yield* Deferred.make<void>()
      const credential = yield* Deferred.make<Redacted.Redacted<string>>()
      const authenticatedWith = yield* Deferred.make<Redacted.Redacted<string>>()
      const handlers = PeerRpc.Rpcs.toLayer(PeerRpc.Rpcs.of({
        Open: () => Stream.empty,
        Push: () => Effect.void
      }))
      const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
        Effect.provide(handlers),
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: (value) => Deferred.succeed(authenticatedWith, value).pipe(Effect.as(authenticated))
        }),
        Effect.provide(PeerAuthentication.layerClient),
        Effect.provideService(PeerCredentials.PeerCredentials, {
          get: Deferred.succeed(requested, undefined).pipe(Effect.andThen(Deferred.await(credential)))
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
      )

      const request = yield* client.Push({ sessionId, payload: Uint8Array.of(1) }).pipe(Effect.forkChild)
      yield* Deferred.await(requested)
      assert.isTrue(Option.isNone(yield* Deferred.poll(authenticatedWith)))
      yield* Deferred.succeed(credential, Redacted.make("asynchronous"))
      yield* Fiber.join(request)
      assert.strictEqual(Redacted.value(yield* Deferred.await(authenticatedWith)), "asynchronous")
    }))

  it.effect("rejects raw missing and header only credentials", () =>
    Effect.gen(function*() {
      const middleware = yield* PeerAuthentication.PeerAuthentication
      const missingOpen = yield* invoke(middleware, {}, 1, Headers.empty, PeerRpc.OpenRpc).pipe(Effect.flip)
      const missingPush = yield* invoke(middleware, {}, 1).pipe(Effect.flip)
      const headers = Headers.fromInput({ authorization: "Bearer secret", cookie: "token=secret" })
      const headerOnly = yield* invoke(middleware, {}, 1, headers).pipe(Effect.flip)
      assert.instanceOf(missingOpen, PeerRpcError.AuthenticationFailure)
      assert.instanceOf(missingPush, PeerRpcError.AuthenticationFailure)
      assert.instanceOf(headerOnly, PeerRpcError.AuthenticationFailure)
    }).pipe(
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: () => Effect.succeed(authenticated)
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
    ))

  it.effect("rejects verifier concurrency immediately", () =>
    Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: () =>
            Effect.gen(function*() {
              yield* Deferred.succeed(started, undefined)
              yield* Deferred.await(release)
              return authenticated
            })
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
          ...PeerRpcLimits.defaults,
          maxInFlightAuthentication: 1
        })
      )
      const first = yield* invoke(middleware, { credential: Redacted.make("first") }, 1).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const rejected = yield* invoke(middleware, { credential: Redacted.make("second") }, 2).pipe(Effect.flip)
      assert.instanceOf(rejected, PeerRpcError.RequestCapacityExceeded)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(first)
    }))

  it.effect("does not restore rate tokens when concurrent requests acquire the state lock out of timestamp order", () =>
    Effect.gen(function*() {
      const firstTimeRead = yield* Deferred.make<void>()
      const releaseFirstTimeRead = yield* Deferred.make<void>()
      const authenticatedOnce = yield* Deferred.make<void>()
      let currentTime = 0
      let timeReads = 0
      const clock = {
        currentTimeMillisUnsafe: () => currentTime,
        currentTimeMillis: Effect.suspend(() => {
          timeReads += 1
          return timeReads === 1
            ? Deferred.succeed(firstTimeRead, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFirstTimeRead)),
              Effect.as(0)
            )
            : Effect.succeed(currentTime)
        }),
        currentTimeNanosUnsafe: () => BigInt(currentTime) * 1_000_000n,
        currentTimeNanos: Effect.sync(() => BigInt(currentTime) * 1_000_000n),
        sleep: () => Effect.never
      } satisfies Clock.Clock
      const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: () => Deferred.succeed(authenticatedOnce, undefined).pipe(Effect.as(authenticated))
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
          ...PeerRpcLimits.defaults,
          authenticationRatePerSecond: 1,
          authenticationBurst: 1
        })
      )
      const invokeWithClock = (credential: string) =>
        invoke(middleware, { credential: Redacted.make(credential) }, 1).pipe(
          Effect.provideService(Clock.Clock, clock)
        )

      const earlier = yield* invokeWithClock("earlier").pipe(
        Effect.result,
        Effect.forkChild
      )
      yield* Deferred.await(firstTimeRead)
      currentTime = 1_000
      yield* invokeWithClock("later")
      yield* Deferred.await(authenticatedOnce)
      yield* Deferred.succeed(releaseFirstTimeRead, undefined)
      yield* Fiber.join(earlier)

      const third = yield* invokeWithClock("third").pipe(Effect.result)
      assert.isTrue(Result.isFailure(third))
      if (Result.isFailure(third)) assert.instanceOf(third.failure, PeerRpcError.RequestCapacityExceeded)
    }))

  it.effect("preserves authentication interruption", () =>
    Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
      )
      const fiber = yield* invoke(middleware, { credential: Redacted.make("credential") }, 1).pipe(Effect.forkChild)

      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      const exit = yield* Fiber.await(fiber)

      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterruptsOnly(exit.cause))
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Authentication", "Success"))).count,
        0
      )
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Authentication", "Failure"))).count,
        1
      )
    }).pipe(Effect.provideService(Metric.MetricRegistry, new Map())))

  it.effect("bounds connection rate state and evicts an idle bucket", () =>
    Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: (credential) =>
            Redacted.value(credential) === "first"
              ? Effect.gen(function*() {
                yield* Deferred.succeed(started, undefined)
                yield* Deferred.await(release)
                return authenticated
              })
              : Effect.succeed(authenticated)
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
          ...PeerRpcLimits.defaults,
          authenticationBurst: 1,
          maxRetainedRateLimitedConnections: 1
        })
      )
      const first = yield* invoke(middleware, { credential: Redacted.make("first") }, 1).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const allBusy = yield* invoke(middleware, { credential: Redacted.make("second") }, 2).pipe(Effect.flip)
      assert.instanceOf(allBusy, PeerRpcError.RequestCapacityExceeded)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(first)
      yield* invoke(middleware, { credential: Redacted.make("second") }, 2)
    }))

  it.effect("retains touched connections while evicting the oldest inactive connection", () =>
    Effect.gen(function*() {
      const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: () => Effect.succeed(authenticated)
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
          ...PeerRpcLimits.defaults,
          authenticationRatePerSecond: 1,
          authenticationBurst: 1,
          maxRetainedRateLimitedConnections: 3
        })
      )
      for (const clientId of [1, 2, 3]) {
        yield* invoke(middleware, { credential: Redacted.make(String(clientId)) }, clientId)
      }
      assert.instanceOf(
        yield* invoke(middleware, { credential: Redacted.make("touch") }, 2).pipe(Effect.flip),
        PeerRpcError.RequestCapacityExceeded
      )
      yield* invoke(middleware, { credential: Redacted.make("fourth") }, 4)
      yield* invoke(middleware, { credential: Redacted.make("evicted-oldest") }, 1)
      assert.instanceOf(
        yield* invoke(middleware, { credential: Redacted.make("still-hot") }, 2).pipe(Effect.flip),
        PeerRpcError.RequestCapacityExceeded
      )
      yield* invoke(middleware, { credential: Redacted.make("evicted-next") }, 3)
    }))

  for (
    const [description, elapsed, expected] of [
      ["retains an exhausted inactive connection just before idle expiry", 999, "Capacity"],
      ["refreshes an exhausted inactive connection exactly at idle expiry", 1_000, "Authenticated"]
    ] as const
  ) {
    it.effect(description, () =>
      Effect.gen(function*() {
        const middleware = yield* PeerAuthentication.PeerAuthentication.pipe(
          Effect.provide(PeerAuthentication.layerServer),
          Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
            authenticate: () => Effect.succeed(authenticated)
          }),
          Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
            ...PeerRpcLimits.defaults,
            authenticationRatePerSecond: Number.MIN_VALUE,
            authenticationBurst: 1,
            rateLimitIdleRetention: 1_000,
            maxRetainedRateLimitedConnections: 8
          })
        )
        yield* invoke(middleware, { credential: Redacted.make("first") }, 1)
        yield* TestClock.adjust(elapsed)
        const result = yield* Effect.result(invoke(middleware, { credential: Redacted.make("second") }, 1))
        assert.strictEqual(
          Result.isSuccess(result)
            ? "Authenticated"
            : result.failure._tag === "RequestCapacityExceeded"
            ? "Capacity"
            : "Failure",
          expected
        )
      }))
  }

  it.effect("isolates authentication rate tokens by connection", () =>
    Effect.gen(function*() {
      const middleware = yield* PeerAuthentication.PeerAuthentication
      yield* invoke(middleware, { credential: Redacted.make("first") }, 1)
      const flooding = yield* invoke(middleware, { credential: Redacted.make("first") }, 1).pipe(Effect.flip)
      yield* invoke(middleware, { credential: Redacted.make("second") }, 2)
      assert.instanceOf(flooding, PeerRpcError.RequestCapacityExceeded)
    }).pipe(
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: () => Effect.succeed(authenticated)
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, {
        ...PeerRpcLimits.defaults,
        authenticationBurst: 1
      })
    ))

  it.effect("rejects an expired authentication lease", () =>
    Effect.gen(function*() {
      const middleware = yield* PeerAuthentication.PeerAuthentication
      const error = yield* invoke(middleware, { credential: Redacted.make("credential") }, 1).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.AuthenticationFailure)
    }).pipe(
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: () => Effect.succeed({ ...authenticated, validUntil: 0 })
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
    ))

  it.effect("records authentication denial without credential identity or verifier defects", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    return Effect.gen(function*() {
      const middleware = yield* PeerAuthentication.PeerAuthentication
      const error = yield* invoke(
        middleware,
        { credential: Redacted.make("credential-forbidden-value") },
        1
      ).pipe(
        Effect.provideService(Metric.CurrentMetricAttributes, { "tenant.id": "metric-forbidden-value" }),
        Effect.flip
      )
      assert.instanceOf(error, PeerRpcError.AuthenticationFailure)
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Authentication", "Attempt"))).count,
        1
      )
      assert.strictEqual(
        (yield* Metric.value(PeerRpcObservability.boundary("Authentication", "AuthenticationDenied"))).count,
        1
      )
      const span = spans.find((span) => span.name === "effect_local_rpc.authentication")
      assert.isDefined(span)
      assert.strictEqual(span!.status._tag, "Ended")
      if (span!.status._tag === "Ended") assert.isTrue(Exit.isSuccess(span!.status.exit))
      assert.deepStrictEqual(span!.events, [])
      const telemetry = JSON.stringify([...span!.attributes]) +
        (span!.status._tag === "Ended" && Exit.isFailure(span!.status.exit)
          ? Cause.pretty(span!.status.exit.cause)
          : span!.status._tag) +
        (yield* Metric.dump)
      for (
        const forbidden of [
          "credential-forbidden-value",
          "verifier-forbidden-value",
          "verifier-log-forbidden-value",
          "metric-forbidden-value",
          "tenant",
          "subject",
          peerId
        ]
      ) {
        assert.notInclude(telemetry, forbidden)
      }
    }).pipe(
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: () =>
          Effect.logDebug("verifier-log-forbidden-value").pipe(
            Effect.andThen(Effect.die(new Error("verifier-forbidden-value")))
          )
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults),
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })

  for (const validUntil of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    it.effect(`rejects nonfinite authentication lease ${String(validUntil)}`, () =>
      Effect.gen(function*() {
        const middleware = yield* PeerAuthentication.PeerAuthentication
        const error = yield* invoke(middleware, { credential: Redacted.make("credential") }, 1).pipe(Effect.flip)
        assert.instanceOf(error, PeerRpcError.AuthenticationFailure)
      }).pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: () => Effect.succeed({ ...authenticated, validUntil })
        }),
        Effect.provideService(PeerRpcLimits.PeerRpcLimits, PeerRpcLimits.defaults)
      ))
  }
})
