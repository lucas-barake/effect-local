import * as Effect from "effect/Effect"
import * as Scheduler from "effect/Scheduler"
import type * as Scope from "effect/Scope"

interface Task {
  readonly run: () => void
  cancelled: boolean
  next: Task | undefined
}

export const make: Effect.Effect<Scheduler.MixedScheduler, never, Scope.Scope> = Effect.gen(function*() {
  const fallback = new Scheduler.MixedScheduler("async")
  const channel = new MessageChannel()
  let head: Task | undefined
  let tail: Task | undefined
  let closed = false

  const take = () => {
    const task = head
    if (task === undefined) return undefined
    head = task.next
    if (head === undefined) tail = undefined
    task.next = undefined
    return task
  }

  channel.port1.addEventListener("message", () => {
    const task = take()
    if (task !== undefined && !task.cancelled) task.run()
  })
  channel.port1.start()
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true
      channel.port1.close()
      channel.port2.close()
      let task = take()
      while (task !== undefined) {
        const pending = task
        fallback.setImmediate(() => {
          if (!pending.cancelled) pending.run()
        })
        task = take()
      }
    })
  )
  const post = (run: () => void): () => void => {
    if (closed) return fallback.setImmediate(run)
    const task: Task = { run, cancelled: false, next: undefined }
    if (tail === undefined) head = task
    else tail.next = task
    tail = task
    channel.port2.postMessage(undefined)
    return () => {
      task.cancelled = true
    }
  }
  return new Scheduler.MixedScheduler("async", post)
})
