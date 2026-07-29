import * as Automerge from "@automerge/automerge"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"

export interface HistoryCounters {
  readonly changes: number
  readonly operations: number
  readonly bytes: number
}

export interface NullableHistoryCounters {
  readonly changes: number | null
  readonly operations: number | null
  readonly bytes: number | null
}

export const measure = (
  changes: ReadonlyArray<{ readonly bytes: Uint8Array }>
): HistoryCounters => {
  let operations = 0
  let bytes = 0
  for (const change of changes) {
    operations += Automerge.decodeChange(change.bytes).ops.length
    bytes += change.bytes.byteLength
  }
  return { changes: changes.length, operations, bytes }
}

export const measureDecoded = (
  changes: ReadonlyArray<{ readonly bytes: Uint8Array; readonly operations: number }>
): HistoryCounters => ({
  changes: changes.length,
  operations: changes.reduce((total, change) => total + change.operations, 0),
  bytes: changes.reduce((total, change) => total + change.bytes.byteLength, 0)
})

const quotaExceeded = (resource: string, limit: number) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.QuotaExceeded({ resource, limit })
  })

export const check = (
  counters: HistoryCounters,
  limits: ReplicaLimits.Values
): Effect.Effect<HistoryCounters, ReplicaError.ReplicaError> => {
  if (counters.changes > limits.maxConflictSourceChanges) {
    return Effect.fail(quotaExceeded("conflict source changes", limits.maxConflictSourceChanges))
  }
  if (counters.operations > limits.maxConflictSourceOperations) {
    return Effect.fail(quotaExceeded("conflict source operations", limits.maxConflictSourceOperations))
  }
  if (counters.bytes > limits.maxConflictSourceBytes) {
    return Effect.fail(quotaExceeded("conflict source bytes", limits.maxConflictSourceBytes))
  }
  return Effect.succeed(counters)
}

export const add = (
  current: NullableHistoryCounters,
  delta: HistoryCounters,
  limits: ReplicaLimits.Values
): Effect.Effect<HistoryCounters, ReplicaError.ReplicaError> => {
  if (current.changes === null || current.operations === null || current.bytes === null) {
    return Effect.fail(quotaExceeded("unmeasured conflict source history", limits.maxConflictSourceChanges))
  }
  return check({
    changes: current.changes + delta.changes,
    operations: current.operations + delta.operations,
    bytes: current.bytes + delta.bytes
  }, limits)
}
