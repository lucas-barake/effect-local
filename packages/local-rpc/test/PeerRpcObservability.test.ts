import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Metric from "effect/Metric"
import * as References from "effect/References"
import * as Tracer from "effect/Tracer"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"

describe("PeerRpcObservability", () => {
  it.effect("does not let telemetry Clock defects replace the business Cause", () =>
    Effect.gen(function*() {
      const clock = yield* Clock.Clock
      const original = Cause.fail({ _tag: "OriginalFailure" })
      const defectiveClock = (defectAtRead: number): Clock.Clock => {
        let reads = 0
        const currentTimeNanosUnsafe = () => {
          reads += 1
          if (reads === defectAtRead) throw new Error("telemetry clock defect")
          return clock.currentTimeNanosUnsafe()
        }
        return {
          currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
          currentTimeMillis: clock.currentTimeMillis,
          currentTimeNanosUnsafe,
          currentTimeNanos: Effect.sync(currentTimeNanosUnsafe),
          monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
          monotonicTimeNanos: clock.monotonicTimeNanos,
          sleep: (duration) => clock.sleep(duration)
        }
      }
      let observeRuns = 0
      let relayRuns = 0

      const observeExit = yield* PeerRpcObservability.observe({
        effect: Effect.sync(() => {
          observeRuns += 1
        }).pipe(Effect.andThen(Effect.failCause(original))),
        operation: "Open",
        spanName: "effect_local_rpc.server.open",
        attributes: {},
        result: () => "Failure"
      }).pipe(
        Effect.provideService(Clock.Clock, defectiveClock(2)),
        Effect.exit
      )
      const relayExit = yield* PeerRpcObservability.observeRelay({
        effect: Effect.sync(() => {
          relayRuns += 1
        }).pipe(Effect.andThen(Effect.failCause(original))),
        operation: "RelayClaim",
        direction: "Receive",
        facts: () => ({ items: 0 }),
        result: () => "Failure"
      }).pipe(
        Effect.provideService(Clock.Clock, defectiveClock(3)),
        Effect.exit
      )

      assert.strictEqual(observeRuns, 1)
      assert.strictEqual(relayRuns, 1)
      for (const exit of [observeExit, relayExit]) {
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.strictEqual(exit.cause, original)
      }
    }))

  it.effect("does not let telemetry defects replace a captured business Cause", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    const original = Cause.fail({ _tag: "OriginalFailure" })
    const throwTelemetry = () => {
      throw new Error("telemetry defect")
    }

    return Effect.gen(function*() {
      const exits = [
        yield* PeerRpcObservability.observe({
          effect: Effect.failCause(original),
          operation: "Open",
          spanName: "effect_local_rpc.server.open",
          attributes: {},
          result: throwTelemetry
        }).pipe(Effect.exit),
        yield* PeerRpcObservability.observeRelay({
          effect: Effect.failCause(original),
          operation: "RelayClaim",
          direction: "Receive",
          facts: () => ({ items: 0 }),
          result: throwTelemetry
        }).pipe(Effect.exit),
        yield* PeerRpcObservability.observeRelay({
          effect: Effect.failCause(original),
          operation: "RelayClaim",
          direction: "Receive",
          facts: throwTelemetry,
          result: () => "Failure"
        }).pipe(Effect.exit)
      ]
      for (const exit of exits) {
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.strictEqual(exit.cause, original)
      }
      assert.strictEqual(spans.length, exits.length)
      for (const span of spans) {
        assert.strictEqual(span.status._tag, "Ended")
        if (span.status._tag === "Ended") {
          assert.isTrue(Exit.isSuccess(span.status.exit))
          if (Exit.isSuccess(span.status.exit)) {
            assert.isUndefined(span.status.exit.value)
          }
        }
      }
    }).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })

  it.effect("does not let tracer setup or finalization replace the business Cause", () => {
    const original = Cause.fail({ _tag: "OriginalFailure" })
    let createRan = false
    const createTracer = Tracer.make({
      span: () => {
        throw new Error("span create defect")
      }
    })
    let endRan = false
    const endTracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        span.end = () => {
          throw new Error("span end defect")
        }
        return span
      }
    })

    return Effect.gen(function*() {
      const createExit = yield* PeerRpcObservability.observe({
        effect: Effect.sync(() => {
          createRan = true
        }).pipe(Effect.andThen(Effect.failCause(original))),
        operation: "Open",
        spanName: "effect_local_rpc.server.open",
        attributes: {},
        result: () => "Failure"
      }).pipe(
        Effect.provideService(Tracer.Tracer, createTracer),
        Effect.exit
      )
      assert.isTrue(createRan)
      assert.isTrue(Exit.isFailure(createExit))
      if (Exit.isFailure(createExit)) assert.strictEqual(createExit.cause, original)

      const endExit = yield* PeerRpcObservability.observeRelay({
        effect: Effect.sync(() => {
          endRan = true
        }).pipe(Effect.andThen(Effect.failCause(original))),
        operation: "RelayRelease",
        direction: "Receive",
        facts: () => ({ items: 1 }),
        result: () => "Failure"
      }).pipe(
        Effect.provideService(Tracer.Tracer, endTracer),
        Effect.exit
      )
      assert.isTrue(endRan)
      assert.isTrue(Exit.isFailure(endExit))
      if (Exit.isFailure(endExit)) assert.strictEqual(endExit.cause, original)
    }).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map())
    )
  })

  it.effect("keeps relay spans and metrics safe while preserving every original Cause", () => {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      }
    })
    const forbidden = "tenant-subject-peer-session-document-relay-token-payload"
    const ambientLink: Tracer.SpanLink = {
      span: Tracer.externalSpan({
        spanId: "forbidden-span-id",
        traceId: "forbidden-trace-id"
      }),
      attributes: { forbidden }
    }
    const causes = [
      Cause.fail({ _tag: "ExpectedFailure", forbidden }),
      Cause.die(new Error(forbidden)),
      Cause.interrupt(123)
    ] as const
    const metricValue = <Input, State,>(
      metric: Metric.Metric<Input, State>
    ) =>
      Metric.value(metric).pipe(
        Effect.provideService(Metric.CurrentMetricAttributes, {})
      )

    return Effect.gen(function*() {
      for (const cause of causes) {
        const exit = yield* PeerRpcObservability.observeRelay({
          effect: Effect.failCause(cause),
          operation: "RelayAdmit",
          direction: "Send",
          facts: () => ({
            bytes: 4,
            items: 1,
            attempt: 1,
            version: 1
          }),
          result: () => "Failure"
        }).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
        if (Exit.isFailure(exit)) assert.strictEqual(exit.cause, cause)
      }
      yield* PeerRpcObservability.recordRelayOutcome({
        operation: "RelayAcknowledge",
        direction: "Receive",
        result: "Acknowledged",
        facts: {
          bytes: 4,
          items: 1,
          version: 1,
          latencyMillis: 25
        }
      })
      yield* PeerRpcObservability.recordRelayOutcome({
        operation: "RelayMaintenance",
        direction: "Receive",
        result: "Expired",
        facts: { items: 2 },
        stage: "Expire"
      })
      for (const version of [1_000_001, 1_000_002]) {
        yield* PeerRpcObservability.recordRelayOutcome({
          operation: "RelayAdmit",
          direction: "Send",
          result: "Success",
          facts: { bytes: 4, version }
        })
      }

      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayOutcomes("RelayAdmit", "Send", "Attempt")
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayOutcomes("RelayAdmit", "Send", "Failure")
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayBytes("RelayAdmit", "Send", "Failure", 1)
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayItems("RelayAdmit", "Send", "Failure")
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayAttempts("RelayAdmit", "Send", "Failure")
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayDurationMillis("RelayAdmit", "Send", "Failure")
        )).count,
        causes.length
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayLatencyMillis(
            "RelayAcknowledge",
            "Receive",
            "Acknowledged"
          )
        )).count,
        1
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayOutcomes(
            "RelayMaintenance",
            "Receive",
            "Expired",
            "Expire"
          )
        )).count,
        1
      )
      assert.strictEqual(
        (yield* metricValue(
          PeerRpcObservability.relayBytes(
            "RelayAdmit",
            "Send",
            "Success",
            1_000_001
          )
        )).count,
        2
      )

      assert.strictEqual(spans.length, causes.length)
      for (const span of spans) {
        assert.strictEqual(span.name, "effect_local_rpc.relay.admit")
        assert.strictEqual(span.status._tag, "Ended")
        if (span.status._tag === "Ended") {
          assert.isTrue(Exit.isSuccess(span.status.exit))
          if (Exit.isSuccess(span.status.exit)) {
            assert.isUndefined(span.status.exit.value)
          }
        }
        assert.deepStrictEqual(
          [...span.attributes],
          [
            ["rpc.operation", "RelayAdmit"],
            ["rpc.direction", "Send"],
            ["rpc.result", "Failure"],
            ["rpc.bytes", 4],
            ["rpc.items", 1],
            ["rpc.attempt", 1],
            ["rpc.version", "1"],
            ["rpc.duration_millis", 0]
          ]
        )
        assert.deepStrictEqual(span.links, [])
        assert.deepStrictEqual(span.events, [])
      }

      assert.notInclude(
        JSON.stringify(spans.map((span) => ({
          attributes: [...span.attributes],
          status: span.status._tag
        }))) + (yield* Metric.dump),
        forbidden
      )
    }).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(Metric.CurrentMetricAttributes, { forbidden }),
      Effect.provideService(References.TracerSpanAnnotations, { forbidden }),
      Effect.provideService(References.TracerSpanLinks, [ambientLink]),
      Effect.provideService(Tracer.Tracer, tracer)
    )
  })
})
