import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import { ListTasks, RenameTask, SetTaskCompleted, TaskDocument } from "./domain.js"
import { EngineLive } from "./node.js"

const program = Effect.gen(function*() {
  const replica = yield* Replica.Replica

  const now = yield* Clock.currentTimeMillis
  const documentId = yield* replica.create(TaskDocument, {
    commandId: yield* Identity.makeCommandId,
    value: { title: "Write the README", completed: false, createdAt: now, updatedAt: now }
  })

  // The mutation's declared domain error arrives unwrapped and is caught by its own tag.
  const renamed = yield* replica.mutate(RenameTask, {
    commandId: yield* Identity.makeCommandId,
    documentId,
    payload: { title: "   " }
  }).pipe(
    Effect.catchTag("TitleEmpty", () => Effect.succeed("rejected"))
  )

  yield* replica.mutate(SetTaskCompleted, {
    commandId: yield* Identity.makeCommandId,
    documentId,
    payload: { completed: true }
  })

  const snapshot = yield* replica.get(TaskDocument, documentId)
  const tasks = yield* replica.query(ListTasks, { search: "readme" })

  // `CommandOutcomeUnknown` is never retried blind: look the command id up instead.
  const commandId = yield* Identity.makeCommandId
  const settled = yield* replica.mutate(RenameTask, {
    commandId,
    documentId,
    payload: { title: "Ship it" }
  }).pipe(
    Effect.catchReason(
      "ReplicaError",
      "CommandOutcomeUnknown",
      (reason) =>
        replica.lookupMutation(RenameTask, reason.commandId).pipe(
          Effect.flatMap(CommandOutcome.committedOrFail)
        )
    )
  )

  const outcome = yield* replica.lookupMutation(RenameTask, commandId)
  const described = CommandOutcome.match(outcome, {
    onRejected: ({ error }) => `Rejected with ${error._tag}`,
    onCommitted: () => "Committed locally",
    onUnknown: () => "Outcome unknown"
  })

  return { renamed, snapshot, tasks, settled, described }
})

export const main = program.pipe(
  Effect.provide(EngineLive),
  Effect.scoped
)
