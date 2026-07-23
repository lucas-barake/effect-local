import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"

export type Operation =
  | "Authentication"
  | "Open"
  | "Push"
  | "AdapterOpen"
  | "AdapterPush"
  | "Inbound"
  | "Outbound"
  | "Server"

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

export const observe = <A, E, R,>(options: {
  readonly effect: Effect.Effect<A, E, R>
  readonly operation: Operation
  readonly spanName: string
  readonly attributes: Readonly<Record<string, string | number | boolean>>
  readonly result: (exit: Exit.Exit<A, E>) => Result
}) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.useSpan(
      options.spanName,
      {
        attributes: {
          "rpc.operation": options.operation,
          ...options.attributes
        }
      },
      (span) =>
        record(options.operation, "Attempt", 1).pipe(
          Effect.andThen(restore(options.effect).pipe(Effect.exit)),
          Effect.tap((exit) => {
            const result = options.result(exit)
            return Effect.sync(() => span.attribute("rpc.result", result)).pipe(
              Effect.andThen(record(options.operation, result, 1))
            )
          })
        )
    ).pipe(Effect.flatten)
  )

export const failure = <E,>(exit: Exit.Exit<unknown, E>): E | undefined =>
  Exit.findErrorOption(exit).pipe(Option.getOrUndefined)
