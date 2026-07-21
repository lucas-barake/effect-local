import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as Compaction from "@lucas-barake/effect-local-sql/Compaction"
import * as Recovery from "@lucas-barake/effect-local-sql/Recovery"
import * as ReplicaWorkflow from "@lucas-barake/effect-local-sql/ReplicaWorkflow"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Workflow from "effect/unstable/workflow/Workflow"
import assert from "node:assert/strict"

const Note = Document.make("Note", {
  schema: Schema.Struct({ body: Schema.String }),
  version: 1
})

const documentId = Identity.makeDocumentId()
const checkpoint: Compaction.PreparedCheckpoint = {
  bytes: new TextEncoder().encode("checkpoint"),
  checkpointHash: "checkpoint-hash",
  checksum: "checksum",
  commitSequence: Identity.CommitSequence.make(1),
  documentId,
  documentType: Note.name,
  heads: ["head-1"]
}

const WorkflowLive = Layer.effect(
  ReplicaWorkflow.WorkflowRuntime,
  Effect.gen(function*() {
    const completed = yield* Ref.make(new Set<string>())
    return ReplicaWorkflow.WorkflowRuntime.of({
      execute: (operationId) =>
        Effect.succeed({
          executionId: `execution:${operationId}`,
          operationId,
          replicaIncarnation: Identity.ReplicaIncarnation.make(0)
        }),
      poll: (execution) =>
        Ref.get(completed).pipe(
          Effect.map((executions) =>
            executions.has(execution.executionId)
              ? Option.some(new Workflow.Complete({ exit: Exit.void }))
              : Option.none()
          )
        ),
      resume: (execution) => Ref.update(completed, (executions) => new Set(executions).add(execution.executionId))
    })
  })
)

const CompactionLive = Layer.succeed(
  Compaction.Compaction,
  Compaction.Compaction.of({
    prepare: () => Effect.succeed(checkpoint),
    publish: () => Effect.succeed(true),
    compact: () => Effect.succeed({ checkpoint, published: true }),
    prune: () => Effect.succeed(4)
  })
)

const RecoveryLive = Layer.succeed(
  Recovery.Recovery,
  Recovery.Recovery.of({
    recover: () => Effect.die("recover is not exercised by this service contract example"),
    exportRaw: () => Effect.succeed({ document: null, checkpoints: [], changes: [] })
  })
)

const CommitPublisherLive = Layer.effect(
  CommitPublisher.CommitPublisher,
  Effect.gen(function*() {
    const events = yield* Effect.acquireRelease(
      PubSub.unbounded<CommitPublisher.CommitEvent>(),
      PubSub.shutdown
    )
    const refreshGeneration = yield* Ref.make(0)
    return CommitPublisher.CommitPublisher.of({
      publishPending: PubSub.publish(events, {
        _tag: "Commit",
        commitSequence: Identity.CommitSequence.make(1),
        documentId,
        keys: ["Notes"],
        refreshGeneration: 0
      }).pipe(Effect.as(1)),
      invalidate: () =>
        Ref.updateAndGet(refreshGeneration, (generation) => generation + 1).pipe(
          Effect.flatMap((generation) =>
            PubSub.publish(events, { _tag: "FullRefreshRequired", refreshGeneration: generation })
          ),
          Effect.asVoid
        ),
      subscribe: Effect.gen(function*() {
        const subscription = yield* PubSub.subscribe(events)
        return {
          watermark: Identity.CommitSequence.make(0),
          refreshGeneration: yield* Ref.get(refreshGeneration),
          events: Stream.fromSubscription(subscription)
        }
      })
    })
  })
)

const program = Effect.scoped(Effect.gen(function*() {
  const workflow = yield* ReplicaWorkflow.WorkflowRuntime
  const compaction = yield* Compaction.Compaction
  const recovery = yield* Recovery.Recovery
  const commits = yield* CommitPublisher.CommitPublisher

  const execution = yield* workflow.execute(ReplicaWorkflow.OperationId.make("compact-notes"))
  assert.equal(Option.isNone(yield* workflow.poll(execution)), true)
  yield* workflow.resume(execution)
  const result = yield* workflow.poll(execution)
  assert.equal(Option.isSome(result), true)
  if (Option.isSome(result)) assert.equal(result.value._tag, "Complete")

  assert.deepEqual(yield* compaction.prepare(Note, documentId), checkpoint)
  assert.equal(yield* compaction.publish(checkpoint), true)
  assert.deepEqual(yield* compaction.compact(Note, documentId), { checkpoint, published: true })
  assert.equal(yield* compaction.prune(documentId), 4)
  assert.deepEqual(yield* recovery.exportRaw(documentId), { document: null, checkpoints: [], changes: [] })

  const subscription = yield* commits.subscribe
  const commit = yield* subscription.events.pipe(Stream.runHead, Effect.forkChild)
  assert.equal(yield* commits.publishPending, 1)
  assert.equal(Option.getOrThrow(yield* Fiber.join(commit))._tag, "Commit")

  const refresh = yield* subscription.events.pipe(Stream.runHead, Effect.forkChild)
  yield* commits.invalidate(["Notes"])
  assert.equal(Option.getOrThrow(yield* Fiber.join(refresh))._tag, "FullRefreshRequired")

  yield* Effect.log("workflow", execution, result)
  yield* Effect.log("checkpoint", checkpoint)
  yield* Effect.log("commit subscription", subscription.watermark, subscription.refreshGeneration)
}))

Effect.runPromise(program.pipe(
  Effect.provide(Layer.mergeAll(WorkflowLive, CompactionLive, RecoveryLive, CommitPublisherLive))
))
