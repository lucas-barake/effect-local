import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as References from "effect/References"
import * as Tracer from "effect/Tracer"

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

export type RelayQuotaDomain =
  | "Payload"
  | "SenderPeer"
  | "RecipientPeer"
  | "RecipientSubject"
  | "Tenant"
  | "Shard"

export interface RelayFacts {
  readonly bytes?: number
  readonly items?: number
  readonly attempt?: number
  readonly version?: number
  readonly latencyMillis?: number
}

const relayVersion = (version: number): "1" | "3" | "Unsupported" =>
  version === 1 ? "1" : version === 3 ? "3" : "Unsupported"

const relaySpanName = (operation: RelayOperation) => {
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
}

const relayAttributes = (
  operation: RelayOperation,
  direction: RelayDirection,
  result: RelayResult,
  stage?: RelayMaintenanceStage
) => ({
  operation,
  direction,
  result,
  ...(stage === undefined ? {} : { stage })
})

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

export const relayQuotaRejections = (domain: RelayQuotaDomain) =>
  Metric.counter("effect_local_rpc_relay_quota_rejection_total", {
    incremental: true,
    attributes: { domain }
  })

export const relayPendingItems = () => Metric.gauge("effect_local_rpc_relay_pending_items")

export const relayPendingBytes = () => Metric.gauge("effect_local_rpc_relay_pending_bytes")

export const relayActiveClaims = () => Metric.gauge("effect_local_rpc_relay_active_claims")

export const relayWorkers = () => Metric.gauge("effect_local_rpc_relay_workers")

export const relayReadyQueueItems = (lane: "New" | "Retry") =>
  Metric.gauge("effect_local_rpc_relay_ready_queue_items", {
    attributes: { lane }
  })

export const boundary = (operation: Operation, result: Result) =>
  Metric.counter("effect_local_rpc_boundary_total", {
    incremental: true,
    attributes: { operation, result }
  })

export const activeSessions = () => Metric.gauge("effect_local_rpc_active_sessions")

export const queueItems = (operation: "Inbound" | "Outbound") =>
  Metric.gauge("effect_local_rpc_queue_items", { attributes: { operation } })

export const bytes = (operation: "Inbound" | "Outbound") =>
  Metric.histogram("effect_local_rpc_message_bytes", {
    attributes: { operation },
    boundaries: [0, 64, 256, 1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576]
  })

export const selectedDocuments = () =>
  Metric.histogram("effect_local_rpc_selected_documents", {
    attributes: { operation: "Open" },
    boundaries: [0, 1, 2, 4, 8, 16, 32, 64, 128]
  })

export const record = (operation: Operation, result: Result, amount: number) =>
  Metric.update(boundary(operation, result), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

export const modifyActiveSessions = (amount: number) =>
  Metric.modify(activeSessions(), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

export const modifyQueueItems = (operation: "Inbound" | "Outbound", amount: number) =>
  Metric.modify(queueItems(operation), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

export const recordBytes = (operation: "Inbound" | "Outbound", amount: number) =>
  Metric.update(bytes(operation), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

export const recordSelectedDocuments = (amount: number) =>
  Metric.update(selectedDocuments(), amount).pipe(
    Effect.provideService(Metric.CurrentMetricAttributes, {})
  )

const withoutMetricAttributes = <A, E, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Metric.CurrentMetricAttributes, {}))

const bestEffort = <A, E, R,>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.ignoreCause)

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
      try {
        span.end(endTime, exit)
      } catch {
        // Telemetry must never replace the business outcome.
      }
    },
    attribute(key, value) {
      try {
        span.attribute(key, value)
      } catch {
        // Telemetry must never replace the business outcome.
      }
    },
    event(name, startTime, attributes) {
      try {
        span.event(name, startTime, attributes)
      } catch {
        // Telemetry must never replace the business outcome.
      }
    },
    addLinks(links) {
      try {
        span.addLinks(links)
      } catch {
        // Telemetry must never replace the business outcome.
      }
    }
  }
}

const safeTracer = (tracer: Tracer.Tracer): Tracer.Tracer =>
  Tracer.make({
    span: (options) => {
      try {
        return safeSpan(tracer.span(options))
      } catch {
        return new Tracer.NativeSpan(options)
      }
    }
  })

const safeClock = (clock: Clock.Clock): Clock.Clock => {
  let lastMillis = 0
  let lastNanos = BigInt(0)
  const currentTimeMillisUnsafe = () => {
    try {
      return lastMillis = clock.currentTimeMillisUnsafe()
    } catch {
      return lastMillis
    }
  }
  const currentTimeNanosUnsafe = () => {
    try {
      return lastNanos = clock.currentTimeNanosUnsafe()
    } catch {
      return lastNanos
    }
  }
  return {
    currentTimeMillisUnsafe,
    currentTimeMillis: Effect.sync(currentTimeMillisUnsafe),
    currentTimeNanosUnsafe,
    currentTimeNanos: Effect.sync(currentTimeNanosUnsafe),
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

export const recordRelayQuotaRejection = (domain: RelayQuotaDomain) =>
  Metric.update(relayQuotaRejections(domain), 1).pipe(withoutMetricAttributes)

export const setRelayPending = (items: number, amountBytes: number) =>
  Effect.all([
    Metric.update(relayPendingItems(), items),
    Metric.update(relayPendingBytes(), amountBytes)
  ], { discard: true }).pipe(withoutMetricAttributes)

export const setRelayActiveClaims = (amount: number) =>
  Metric.update(relayActiveClaims(), amount).pipe(withoutMetricAttributes)

export const setRelayWorkers = (amount: number) => Metric.update(relayWorkers(), amount).pipe(withoutMetricAttributes)

export const setRelayReadyQueueItems = (
  lane: "New" | "Retry",
  amount: number
) => Metric.update(relayReadyQueueItems(lane), amount).pipe(withoutMetricAttributes)

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
                  Effect.catchCause(() => Effect.succeed("Failure" as const)),
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
          Effect.flatMap(() =>
            captured === undefined
              ? restore(options.effect).pipe(Effect.provideService(Clock.Clock, clock))
              : captured
          )
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
            Effect.suspend(() =>
              recordRelayOutcome({
                operation: options.operation,
                direction: options.direction,
                result: "Attempt",
                ...(options.stage === undefined ? {} : { stage: options.stage })
              })
            ).pipe(
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
                  const result = yield* Effect.sync(() => options.result(exit)).pipe(
                    Effect.catchCause(() => Effect.succeed("Failure" as const))
                  )
                  const facts = yield* Effect.sync(() => options.facts(exit)).pipe(
                    Effect.catchCause(() => Effect.succeed<RelayFacts>({}))
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
                  yield* recordRelayOutcome({
                    operation: options.operation,
                    direction: options.direction,
                    result,
                    facts,
                    durationMillis,
                    ...(options.stage === undefined ? {} : { stage: options.stage })
                  })
                }).pipe(
                  bestEffort
                )
              }),
              Effect.asVoid
            )
        ).pipe(
          Effect.exit,
          Effect.flatMap(() =>
            captured === undefined
              ? restore(options.effect).pipe(Effect.provideService(Clock.Clock, clock))
              : captured
          )
        ))
    )
  })

export const failure = <E,>(exit: Exit.Exit<unknown, E>): E | undefined =>
  Exit.findErrorOption(exit).pipe(Option.getOrUndefined)
