import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import { EngineLive, InMemoryTestLive, ListTasks, RenameTask, SetCompleted, Task, TaskList } from "./domain.ts"

const program = Effect.gen(function*() {
  const replica = yield* Replica.Replica
  const createCommandId = Identity.makeCommandId()
  const created = yield* replica.create(Task, {
    commandId: createCommandId,
    value: { title: "Write documentation", completed: false, labels: ["docs"] }
  })
  const documentId = yield* CommandOutcome.committedOrFail(created)
  const createReceipt = yield* replica.lookupCreate(Task, createCommandId)
  const initial = yield* replica.get(Task, documentId)
  const projected = yield* Projection.evaluate(TaskList, initial)

  const renameCommandId = Identity.makeCommandId()
  const renamed = yield* replica.mutate(RenameTask, {
    commandId: renameCommandId,
    documentId,
    payload: "Publish API tour"
  })
  const renameReceipt = yield* replica.lookupMutation(RenameTask, renameCommandId)
  const rejectedCommandId = Identity.makeCommandId()
  const rejected = yield* replica.mutate(RenameTask, {
    commandId: rejectedCommandId,
    documentId,
    payload: "   "
  })
  const rejectedReceipt = yield* replica.lookupMutation(RenameTask, rejectedCommandId)

  yield* replica.mutate(SetCompleted, {
    commandId: Identity.makeCommandId(),
    documentId,
    payload: true
  })
  const queried = yield* replica.query(ListTasks, { state: "done" })
  yield* replica.flush
  const status = Option.getOrThrow(yield* replica.status.pipe(Stream.runHead))

  const deleteCommandId = Identity.makeCommandId()
  const deleted = yield* replica.delete(Task, { commandId: deleteCommandId, documentId })
  const deleteReceipt = yield* replica.lookupDelete(Task, deleteCommandId)
  const tombstone = yield* replica.get(Task, documentId)

  yield* Effect.log({
    definitionProjection: projected,
    createReceipt,
    renamed,
    renameReceipt,
    rejected: CommandOutcome.match(rejected, {
      onRejected: ({ error }) => error,
      onCommitted: () => "unexpected commit",
      onUnknown: () => "outcome unknown"
    }),
    rejectedReceipt,
    queried,
    status,
    deleted,
    deleteReceipt,
    tombstone: tombstone.tombstone
  })
})

const testLayerProgram = Effect.gen(function*() {
  const replica = yield* Replica.Replica
  const outcome = yield* replica.create(Task, {
    commandId: Identity.makeCommandId(),
    value: { title: "TestReplica", completed: false, labels: [] }
  })
  const documentId = yield* CommandOutcome.committedOrFail(outcome)
  const snapshot = yield* replica.get(Task, documentId)
  yield* Effect.log({ testReplica: snapshot.value })
})

await Effect.runPromise(program.pipe(Effect.provide(EngineLive)))
await Effect.runPromise(testLayerProgram.pipe(Effect.provide(InMemoryTestLive)))
