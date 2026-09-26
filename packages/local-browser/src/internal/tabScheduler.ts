import * as Effect from "effect/Effect"
import * as Scheduler from "effect/Scheduler"
import type * as Scope from "effect/Scope"

export const make: Effect.Effect<Scheduler.Scheduler, never, Scope.Scope> = Effect.gen(function*() {
  const fallback = new Scheduler.MixedScheduler("async")
  const channel = new MessageChannel()
  const pending: Array<() => void> = []
  let closed = false
  channel.port1.addEventListener("message", () => {
    const task = pending.shift()
    if (task !== undefined) task()
  })
  channel.port1.start()
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true
      channel.port1.close()
      channel.port2.close()
      for (const task of pending.splice(0)) fallback.setImmediate(task)
    })
  )
  const post = (task: () => void): () => void => {
    if (closed) return fallback.setImmediate(task)
    let cancelled = false
    pending.push(() => {
      if (!cancelled) task()
    })
    channel.port2.postMessage(undefined)
    return () => {
      cancelled = true
    }
  }
  return new Scheduler.MixedScheduler("async", post)
})
