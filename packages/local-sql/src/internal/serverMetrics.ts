import * as Cause from "effect/Cause"
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
  const update = (operation: string, effect: Effect.Effect<void>) =>
    effect.pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
        return Effect.logWarning("Server metric update failed", cause).pipe(
          Effect.annotateLogs({ operation })
        )
      })
    )

  return {
    initializeDepths: (history: number, receipts: number) =>
      Effect.all([
        Metric.update(historyDepth, history),
        Metric.update(historyLimit, limits.history),
        Metric.update(receiptDepth, receipts),
        Metric.update(receiptLimit, limits.receipts)
      ], { discard: true }).pipe((effect) => update("initialize_depths", effect)),
    recordAdmission: (outcome: "accepted" | "rejected" | "expired" | "failed") =>
      Metric.withAttributes(admission, { outcome }).pipe(
        Metric.update(1),
        (effect) => update("record_admission", effect)
      ),
    recordRejection: (rejectionClass: string) =>
      Metric.withAttributes(rejection, { class: rejectionClass }).pipe(
        Metric.update(1),
        (effect) => update("record_rejection", effect)
      ),
    changeDepths: (history: number, receipts: number) =>
      Effect.all([
        Metric.modify(historyDepth, history),
        Metric.modify(receiptDepth, receipts)
      ], { discard: true }).pipe((effect) => update("change_depths", effect)),
    changeWatchers: (delta: number) =>
      Metric.modify(syncWatchers, delta).pipe((effect) => update("change_watchers", effect)),
    recordWakeFanout: (elapsedNanos: bigint) =>
      Duration.nanos(elapsedNanos).pipe(
        (duration) => Metric.update(wakeFanout, duration),
        (effect) => update("record_wake_fanout", effect)
      ),
    recordMaintenance: (outcome: "completed" | "failed") =>
      Metric.withAttributes(maintenance, { outcome }).pipe(
        Metric.update(1),
        (effect) => update("record_maintenance", effect)
      ),
    recordPruned: (resource: "history" | "receipt", count: number) => {
      if (count === 0) return Effect.void
      return Metric.withAttributes(pruned, { resource }).pipe(
        Metric.update(count),
        (effect) => update("record_pruned", effect)
      )
    }
  }
}
