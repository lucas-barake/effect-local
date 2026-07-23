import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

const OutboxRow = Schema.Struct({
  commit_sequence: Identity.CommitSequence,
  document_id: Identity.DocumentId,
  invalidation_keys: Schema.fromJsonString(Schema.Array(Schema.String))
})

const WatermarkRow = Schema.Struct({ watermark: Identity.CommitSequence })

export type CommitEvent =
  | {
    readonly _tag: "Commit"
    readonly commitSequence: Identity.CommitSequence
    readonly documentId: Identity.DocumentId
    readonly keys: ReadonlyArray<string>
    readonly refreshGeneration: number
  }
  | { readonly _tag: "FullRefreshRequired"; readonly refreshGeneration: number }

export interface CommitSubscription {
  readonly watermark: Identity.CommitSequence
  readonly refreshGeneration: number
  readonly events: Stream.Stream<CommitEvent>
}

export class CommitPublisher extends Context.Service<CommitPublisher, {
  readonly publishPending: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly invalidate: (keys: ReadonlyArray<unknown>) => Effect.Effect<void>
  readonly subscribe: Effect.Effect<CommitSubscription, ReplicaError.ReplicaError, Scope.Scope>
}>()("@lucas-barake/effect-local-sql/CommitPublisher") {}

export const layer: Layer.Layer<CommitPublisher, never, Reactivity.Reactivity | SqlClient.SqlClient> = Layer.effect(
  CommitPublisher,
  Effect.gen(function*() {
    const reactivity = yield* Reactivity.Reactivity
    const sql = yield* SqlClient.SqlClient
    const lock = yield* Semaphore.make(1)
    const events = yield* Effect.acquireRelease(PubSub.sliding<CommitEvent>(256), PubSub.shutdown)
    const refreshGeneration = yield* Ref.make(0)
    const findPending = SqlSchema.findAll({
      Request: Schema.Void,
      Result: OutboxRow,
      execute: () =>
        sql`SELECT commit_sequence, document_id, invalidation_keys
          FROM effect_local_commit_outbox WHERE published = 0 ORDER BY commit_sequence`
    })
    const findWatermark = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: WatermarkRow,
      execute: () =>
        sql`SELECT COALESCE(MAX(commit_sequence), 0) AS watermark
          FROM effect_local_commit_outbox WHERE published = 1`
    })
    const invalidate = (keys: ReadonlyArray<unknown>) =>
      lock.withPermit(
        reactivity.invalidate(keys).pipe(
          Effect.andThen(Ref.updateAndGet(refreshGeneration, (generation) => generation + 1)),
          Effect.flatMap((refreshGeneration) =>
            PubSub.publish(events, { _tag: "FullRefreshRequired", refreshGeneration })
          ),
          Effect.asVoid,
          Effect.uninterruptible
        )
      )
    const publishPending = lock.withPermit(Effect.gen(function*() {
      const rows = yield* findPending(undefined)
      for (const row of rows) {
        yield* reactivity.invalidate(row.invalidation_keys)
        yield* PubSub.publish(events, {
          _tag: "Commit",
          commitSequence: row.commit_sequence,
          documentId: row.document_id,
          keys: row.invalidation_keys,
          refreshGeneration: yield* Ref.get(refreshGeneration)
        })
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_commit_outbox SET published = 1
              WHERE commit_sequence = ${row.commit_sequence}`
          yield* sql`DELETE FROM effect_local_commit_outbox
              WHERE published = 1 AND commit_sequence < ${row.commit_sequence}`
        }))
      }
      return rows.length
    })).pipe(
      Effect.catchTags({
        SqlError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause
              })
            })
          ),
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause
              })
            })
          )
      })
    )
    const subscribe = lock.withPermit(Effect.gen(function*() {
      const subscription = yield* PubSub.subscribe(events)
      const generation = yield* Ref.get(refreshGeneration)
      const watermark = yield* findWatermark(undefined).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause
                })
              })
            ),
          SchemaError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
            )
        })
      )
      const initialWatermark = Option.match(watermark, {
        onNone: () => Identity.CommitSequence.make(0),
        onSome: (row) => row.watermark
      })
      return {
        watermark: initialWatermark,
        refreshGeneration: generation,
        events: Stream.fromSubscription(subscription).pipe(
          Stream.mapAccum<readonly [number, Identity.CommitSequence], CommitEvent, CommitEvent>(
            () => [generation, initialWatermark],
            ([observedGeneration, observedSequence], event) => {
              if (event._tag === "FullRefreshRequired") {
                return [[Math.max(observedGeneration, event.refreshGeneration), observedSequence], [event]]
              }
              const refreshRequired = event.refreshGeneration > observedGeneration ||
                event.commitSequence > observedSequence + 1
              const state = [
                Math.max(observedGeneration, event.refreshGeneration),
                Identity.CommitSequence.make(Math.max(observedSequence, event.commitSequence))
              ] as const
              return refreshRequired
                ? [state, [{
                  _tag: "FullRefreshRequired",
                  refreshGeneration: event.refreshGeneration
                }, event]]
                : [state, [event]]
            }
          )
        )
      }
    }))
    yield* publishPending.pipe(
      Effect.catchTag("ReplicaError", () => Effect.void),
      Effect.repeat(Schedule.spaced("1 second")),
      Effect.forkScoped({ startImmediately: true })
    )
    return CommitPublisher.of({ invalidate, publishPending, subscribe })
  })
)
