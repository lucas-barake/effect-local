import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Metric from "effect/Metric"

export const make = (limits: { readonly history: number; readonly receipts: number }) => {
  const admission = Metric.counter("effect_local_server_admission", { incremental: true })
  const rejection = Metric.counter("effect_local_server_rejection", { incremental: true })
  const historyDepth = Metric.gauge("effect_local_server_history_depth")
  const historyLimit = Metric.gauge("effect_local_server_history_limit")
  const receiptDepth = Metric.gauge("effect_local_server_receipt_depth")
  const receiptLimit = Metric.gauge("effect_local_server_receipt_limit")
  const syncWatchers = Metric.gauge("effect_local_server_sync_watcher_count")
  const wakeFanout = Metric.timer("effect_local_server_wake_fanout_duration")
  const maintenance = Metric.counter("effect_local_server_maintenance", { incremental: true })
  const pruned = Metric.counter("effect_local_server_pruned", { incremental: true })

  return {
    initializeDepths: (history: number, receipts: number) =>
      Effect.all([
        Metric.update(historyDepth, history),
        Metric.update(historyLimit, limits.history),
        Metric.update(receiptDepth, receipts),
        Metric.update(receiptLimit, limits.receipts)
      ], { discard: true }),
    recordAdmission: (outcome: "accepted" | "rejected" | "expired" | "failed") =>
      Metric.update(Metric.withAttributes(admission, { outcome }), 1),
    recordRejection: (rejectionClass: string) =>
      Metric.update(Metric.withAttributes(rejection, { class: rejectionClass }), 1),
    changeDepths: (history: number, receipts: number) =>
      Effect.all([
        Metric.modify(historyDepth, history),
        Metric.modify(receiptDepth, receipts)
      ], { discard: true }),
    changeWatchers: (delta: number) => Metric.modify(syncWatchers, delta),
    recordWakeFanout: (elapsedNanos: bigint) => Metric.update(wakeFanout, Duration.nanos(elapsedNanos)),
    recordMaintenance: (outcome: "completed" | "failed") =>
      Metric.update(Metric.withAttributes(maintenance, { outcome }), 1),
    recordPruned: (resource: "history" | "receipt", count: number) => {
      if (count === 0) return Effect.void
      return Metric.update(Metric.withAttributes(pruned, { resource }), count)
    }
  }
}
