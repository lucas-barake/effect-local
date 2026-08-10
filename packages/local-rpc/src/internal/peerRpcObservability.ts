import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as References from "effect/References"
import * as Tracer from "effect/Tracer"

/**
 * Spans and metrics for the peer RPC boundary.
 *
 * Trimmed with the single-process relay. The gauges it used to publish — active sessions, queue
 * depth, pending items and bytes, active claims, worker count, ready-queue depth, per-scope quota
 * rejections — were all written by `PeerRpcServer` and `PeerRelayIngress`, and nothing writes them
 * now: durable inbox depth belongs to the entity's store, and worker and queue accounting belongs
 * to the cluster. They were removed rather than left declared, because a gauge that is always zero
 * reads to an operator as "there is no backlog" rather than as "nobody is measuring".
 *
 * What remains is what the surviving code actually drives: the authentication boundary counter, and
 * the relay outcome, byte, item, attempt, duration and latency families that `observeRelay` records
 * from the client transport.
 */

export type Operation =
  | "Authentication"
  | "Open"
  | "Push"
  | "AdapterOpen"
  | "AdapterPush"
  | "Inbound"
  | "Outbound"
  | "Server"
  | "RelayAdmit"
  | "RelayClaim"
  | "RelayAcknowledge"
  | "RelayRelease"
  | "RelayMaintenance"
  | "AdapterAcknowledge"

export type Result =
  | "Attempt"
  | "Success"
  | "AuthenticationDenied"
  | "AuthorizationDenied"
  | "ProtocolRejected"
  | "CapacityRejected"
  | "Overloaded"
  | "Failure"
  | "Replaced"
  | "ShutdownClosed"
  | "Accepted"
  | "Duplicate"
  | "Claimed"
  | "Empty"
  | "Delivered"
  | "Acknowledged"
  | "Released"
  | "Expired"
  | "DeadLettered"
  | "Stale"
  | "Unavailable"

export type RelayResult = Extract<
  Result,
  | "Attempt"
  | "Success"
  | "AuthorizationDenied"
  | "ProtocolRejected"
  | "CapacityRejected"
  | "Failure"
  | "Accepted"
  | "Duplicate"
  | "Claimed"
  | "Empty"
  | "Delivered"
  | "Acknowledged"
  | "Released"
  | "Expired"
  | "DeadLettered"
  | "Stale"
  | "Unavailable"
>

export type RelayOperation = Extract<
  Operation,
  | "RelayAdmit"
  | "RelayClaim"
  | "RelayAcknowledge"
  | "RelayRelease"
  | "RelayMaintenance"
  | "AdapterAcknowledge"
>

export type RelayDirection = "Send" | "Receive"

export type RelayMaintenanceStage =
  | "Recover"
  | "Expire"
  | "Repair"
  | "Reconcile"
  | "Collect"
  | "Usage"

export interface RelayFacts {
  readonly bytes?: number
  readonly items?: number
  readonly attempt?: number
  readonly version?: number
  readonly latencyMillis?: number
}

const relayVersion = (version: number): "1" | "3" | "Unsupported" => {
  if (version === 1) return "1"
  if (version === 3) return "3"
  return "Unsupported"
}

const relaySpanName = (operation: RelayOperation): string => {
  switch (operation) {
    case "RelayAdmit":
      return "effect_local_rpc.relay.admit"
    case "RelayClaim":
      return "effect_local_rpc.relay.claim"
    case "RelayAcknowledge":
      return "effect_local_rpc.relay.acknowledge"
    case "RelayRelease":
      return "effect_local_rpc.relay.release"
    case "RelayMaintenance":
      return "effect_local_rpc.relay.maintenance"
    case "AdapterAcknowledge":
      return "effect_local_rpc.adapter.relay_acknowledge"
  }
  return "effect_local_rpc.unknown"
}

const relayAttributes = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) => {
  const attributes: {
    operation: RelayOperation
    direction: RelayDirection
    result: RelayResult
    stage?: RelayMaintenanceStage
  } = {
    operation,
    direction,
    result
  }
  if (stage !== undefined) attributes.stage = stage
  return attributes
}

export const relayOutcomes = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) =>
  Metric.counter("effect_local_rpc_relay_outcome_total", {
    incremental: true,
    attributes: relayAttributes(operation, direction, result, stage)
  })

export const relayBytes = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  version: number,
  stage?: RelayMaintenanceStage
) =>
  Metric.histogram("effect_local_rpc_relay_bytes", {
    attributes: {
      ...relayAttributes(operation, direction, result, stage),
      version: relayVersion(version)
    },
    boundaries: [0, 64, 256, 1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576]
  })

export const relayItems = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) =>
  Metric.histogram("effect_local_rpc_relay_items", {
    attributes: relayAttributes(operation, direction, result, stage),
    boundaries: [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024]
  })

export const relayAttempts = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) =>
  Metric.histogram("effect_local_rpc_relay_attempt", {
    attributes: relayAttributes(operation, direction, result, stage),
    boundaries: [0, 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1_024]
  })

export const relayDurationMillis = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) =>
  Metric.histogram("effect_local_rpc_relay_duration_millis", {
    attributes: relayAttributes(operation, direction, result, stage),
    boundaries: [0, 1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000, 30_000, 300_000]
  })

export const relayLatencyMillis = (
  operation: "RelayAcknowledge" | "AdapterAcknowledge",
  direction: RelayDirection,
  result: RelayResult
) =>
  Metric.histogram("effect_local_rpc_relay_latency_millis", {
    attributes: relayAttributes(operation, direction, result),
    boundaries: [0, 1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000, 30_000, 300_000, 3_600_000, 86_400_000]
  })

export const boundary = (operation: Operation, result: Result) =>
  Metric.counter("effect_local_rpc_boundary_total", {
    incremental: true,
    attributes: { operation, result }
  })

const record = (operation: Operation, result: Result, amount: number) =>
  Metric.update(boundary(operation, result), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

const withoutMetricAttributes = <A, E, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Metric.CurrentMetricAttributes, {}))

const bestEffort = <A, E, R,>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.ignoreCause)

const safe = <A,>(thunk: () => A, fallback: A): A =>
  Effect.runSync(
    Effect.try({ try: thunk, catch: () => fallback }).pipe(
      Effect.orElseSucceed(() => fallback)
    )
  )

const safeSpan = (span: Tracer.Span): Tracer.Span => {
  let status = span.status
  return {
    _tag: "Span",
    name: span.name,
    spanId: span.spanId,
    traceId: span.traceId,
    parent: span.parent,
    annotations: span.annotations,
    get status() {
      return status
    },
    attributes: span.attributes,
    links: span.links,
    sampled: span.sampled,
    kind: span.kind,
    end(endTime, exit) {
      if (status._tag === "Ended") return
      status = {
        _tag: "Ended",
        startTime: status.startTime,
        endTime,
        exit
      }
      safe(() => span.end(endTime, exit), undefined)
    },
    attribute(key, value) {
      safe(() => span.attribute(key, value), undefined)
    },
    event(name, startTime, attributes) {
      safe(() => span.event(name, startTime, attributes), undefined)
    },
    addLinks(links) {
      safe(() => span.addLinks(links), undefined)
    }
  }
}

const safeTracer = (tracer: Tracer.Tracer): Tracer.Tracer =>
  Tracer.make({
    span: (options) => {
      return safe(() => safeSpan(tracer.span(options)), new Tracer.NativeSpan(options))
    }
  })

const safeClock = (clock: Clock.Clock): Clock.Clock => {
  let lastMillis = 0
  let lastNanos = BigInt(0)
  let lastMonotonicNanos = BigInt(0)
  const currentTimeMillisUnsafe = () => {
    const current = safe(() => clock.currentTimeMillisUnsafe(), lastMillis)
    lastMillis = current
    return current
  }
  const currentTimeNanosUnsafe = () => {
    const current = safe(() => clock.currentTimeNanosUnsafe(), lastNanos)
    lastNanos = current
    return current
  }
  const monotonicTimeNanosUnsafe = () => {
    const current = safe(() => clock.monotonicTimeNanosUnsafe(), lastMonotonicNanos)
    lastMonotonicNanos = current
    return current
  }
  return {
    currentTimeMillisUnsafe,
    currentTimeMillis: Effect.sync(currentTimeMillisUnsafe),
    currentTimeNanosUnsafe,
    currentTimeNanos: Effect.sync(currentTimeNanosUnsafe),
    monotonicTimeNanosUnsafe,
    monotonicTimeNanos: Effect.sync(monotonicTimeNanosUnsafe),
    sleep: (duration) => clock.sleep(duration)
  }
}

const useSafeSpan = <A, E, R,>(
  name: string,
  attributes: Readonly<Record<string, string | number | boolean>>,
  clock: Clock.Clock,
  evaluate: (span: Tracer.Span) => Effect.Effect<A, E, R>
) =>
  Effect.flatMap(Tracer.Tracer, (tracer) =>
    Effect.useSpan(
      name,
      { attributes },
      evaluate
    ).pipe(
      Effect.provideService(Tracer.Tracer, safeTracer(tracer)),
      Effect.provideService(Clock.Clock, safeClock(clock)),
      Effect.provideService(References.TracerSpanAnnotations, {}),
      Effect.provideService(References.TracerSpanLinks, [])
    ))

export const recordRelayOutcome = (options: {
  readonly operation: RelayOperation
  readonly direction: RelayDirection
  readonly result: RelayResult
  readonly facts?: RelayFacts
  readonly durationMillis?: number
  readonly stage?: RelayMaintenanceStage
}) => {
  const facts = options.facts ?? {}
  const updates: Array<Effect.Effect<void>> = [
    Metric.update(
      relayOutcomes(
        options.operation,
        options.direction,
        options.result,
        options.stage
      ),
      1
    )
  ]
  if (facts.bytes !== undefined && facts.version !== undefined) {
    updates.push(Metric.update(
      relayBytes(
        options.operation,
        options.direction,
        options.result,
        facts.version,
        options.stage
      ),
      facts.bytes
    ))
  }
  if (facts.items !== undefined) {
    updates.push(Metric.update(
      relayItems(
        options.operation,
        options.direction,
        options.result,
        options.stage
      ),
      facts.items
    ))
  }
  if (facts.attempt !== undefined) {
    updates.push(Metric.update(
      relayAttempts(
        options.operation,
        options.direction,
        options.result,
        options.stage
      ),
      facts.attempt
    ))
  }
  if (
    facts.latencyMillis !== undefined &&
    (options.operation === "RelayAcknowledge" ||
      options.operation === "AdapterAcknowledge")
  ) {
    updates.push(Metric.update(
      relayLatencyMillis(
        options.operation,
        options.direction,
        options.result
      ),
      facts.latencyMillis
    ))
  }
  if (options.durationMillis !== undefined) {
    updates.push(Metric.update(
      relayDurationMillis(
        options.operation,
        options.direction,
        options.result,
        options.stage
      ),
      options.durationMillis
    ))
  }
  return Effect.all(updates, { discard: true }).pipe(withoutMetricAttributes)
}

export const observe = <A, E, R,>(options: {
  readonly effect: Effect.Effect<A, E, R>
  readonly operation: Operation
  readonly spanName: string
  readonly attributes: Readonly<Record<string, string | number | boolean>>
  readonly result: (exit: Exit.Exit<A, E>) => Result
}) =>
  Effect.suspend(() => {
    let captured: Exit.Exit<A, E> | undefined
    return Effect.uninterruptibleMask((restore) =>
      Effect.flatMap(Clock.Clock, (clock) =>
        useSafeSpan(
          options.spanName,
          {
            "rpc.operation": options.operation,
            ...options.attributes
          },
          clock,
          (span) =>
            Effect.suspend(() => record(options.operation, "Attempt", 1)).pipe(
              bestEffort,
              Effect.andThen(
                restore(options.effect).pipe(
                  Effect.provideService(Clock.Clock, clock),
                  Effect.exit
                )
              ),
              Effect.tap((exit) =>
                Effect.sync(() => {
                  captured = exit
                })
              ),
              Effect.tap((exit) =>
                Effect.sync(() => options.result(exit)).pipe(
                  // Only a defect thrown by the caller's own classifier is recovered — the wrapped
                  // effect is `Effect.sync`, so nothing else can arrive here — and it is logged
                  // rather than swallowed. Recovering the whole `Cause` would relabel every
                  // observed operation as a failure with nothing anywhere saying why, and would
                  // silently absorb interruption alongside it.
                  Effect.catchDefect((defect) =>
                    Effect.logWarning("Peer rpc observability classifier died", defect).pipe(
                      Effect.as<Result>("Failure")
                    )
                  ),
                  Effect.flatMap((result) =>
                    Effect.sync(() => span.attribute("rpc.result", result)).pipe(
                      Effect.andThen(record(options.operation, result, 1))
                    )
                  ),
                  bestEffort
                )
              ),
              Effect.asVoid
            )
        ).pipe(
          Effect.exit,
          Effect.flatMap(() => {
            if (captured === undefined) return restore(options.effect).pipe(Effect.provideService(Clock.Clock, clock))
            return captured
          })
        ))
    )
  })

export const observeRelay = <A, E, R,>(options: {
  readonly effect: Effect.Effect<A, E, R>
  readonly operation: RelayOperation
  readonly direction: RelayDirection
  readonly stage?: RelayMaintenanceStage
  readonly facts: (exit: Exit.Exit<A, E>) => RelayFacts
  readonly result: (exit: Exit.Exit<A, E>) => RelayResult
}) =>
  Effect.suspend(() => {
    let captured: Exit.Exit<A, E> | undefined
    return Effect.uninterruptibleMask((restore) =>
      Effect.flatMap(Clock.Clock, (clock) =>
        useSafeSpan(
          relaySpanName(options.operation),
          {
            "rpc.operation": options.operation,
            "rpc.direction": options.direction
          },
          clock,
          (span) =>
            Effect.suspend(() => {
              const attempt: {
                operation: RelayOperation
                direction: RelayDirection
                result: RelayResult
                stage?: RelayMaintenanceStage
              } = {
                operation: options.operation,
                direction: options.direction,
                result: "Attempt"
              }
              if (options.stage !== undefined) attempt.stage = options.stage
              return recordRelayOutcome(attempt)
            }).pipe(
              bestEffort,
              Effect.andThen(
                restore(options.effect).pipe(
                  Effect.provideService(Clock.Clock, clock),
                  Effect.exit,
                  Effect.tap((exit) =>
                    Effect.sync(() => {
                      captured = exit
                    })
                  ),
                  Effect.timed
                )
              ),
              Effect.tap(([duration, exit]) => {
                const durationMillis = Duration.toMillis(duration)
                return Effect.gen(function*() {
                  // As above: a defect in the caller's classifier is recovered and reported;
                  // interruption is left alone.
                  const result = yield* Effect.sync(() => options.result(exit)).pipe(
                    Effect.catchDefect((defect) =>
                      Effect.logWarning("Relay observability classifier died", defect).pipe(
                        Effect.as<RelayResult>("Failure")
                      )
                    )
                  )
                  const facts = yield* Effect.sync(() => options.facts(exit)).pipe(
                    Effect.catchDefect((defect) =>
                      Effect.logWarning("Relay observability facts died", defect).pipe(
                        Effect.as<RelayFacts>({})
                      )
                    )
                  )
                  yield* Effect.sync(() => {
                    span.attribute("rpc.result", result)
                    if (facts.bytes !== undefined) span.attribute("rpc.bytes", facts.bytes)
                    if (facts.items !== undefined) span.attribute("rpc.items", facts.items)
                    if (facts.attempt !== undefined) span.attribute("rpc.attempt", facts.attempt)
                    if (facts.version !== undefined) {
                      span.attribute("rpc.version", relayVersion(facts.version))
                    }
                    if (facts.latencyMillis !== undefined) {
                      span.attribute("rpc.latency_millis", facts.latencyMillis)
                    }
                    span.attribute("rpc.duration_millis", durationMillis)
                  })
                  const outcome: {
                    operation: RelayOperation
                    direction: RelayDirection
                    result: RelayResult
                    facts: RelayFacts
                    durationMillis: number
                    stage?: RelayMaintenanceStage
                  } = {
                    operation: options.operation,
                    direction: options.direction,
                    result,
                    facts,
                    durationMillis
                  }
                  if (options.stage !== undefined) outcome.stage = options.stage
                  yield* recordRelayOutcome(outcome)
                }).pipe(
                  bestEffort
                )
              }),
              Effect.asVoid
            )
        ).pipe(
          Effect.exit,
          Effect.flatMap(() => {
            if (captured === undefined) return restore(options.effect).pipe(Effect.provideService(Clock.Clock, clock))
            return captured
          })
        ))
    )
  })

export const failure = <E,>(exit: Exit.Exit<unknown, E>): E | undefined =>
  Exit.findErrorOption(exit).pipe(Option.getOrUndefined)
