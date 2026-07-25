import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Projection from "@lucas-barake/effect-local/Projection"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Arr from "effect/Array"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaGate from "./ReplicaGate.js"

const PendingRow = Schema.Struct({ count: Schema.Int })
const BlockedRow = Schema.Struct({ document_type: Schema.String })
const BlockedProjectionRow = Schema.Struct({ projection_name: Schema.String })
const GenerationRow = Schema.Struct({ writer_generation: Identity.WriterGeneration })

export interface Options {
  readonly sampleInterval: Duration.Input
}

export const defaultOptions: Options = {
  sampleInterval: "1 second"
}

/**
 * One in-flight restore. Ingest and installation are reported as separate, sequential conditions because
 * they are genuinely different: during ingest the replica still serves reads and writes, while the
 * installation holds the gate's exclusive claim and blocks both.
 */
export interface Restore {
  readonly progress: (processedBytes: number) => Effect.Effect<void>
  readonly installing: Effect.Effect<void, never, Scope.Scope>
}

interface RestoreState {
  readonly processedBytes: number
  readonly installing: boolean
}

interface Conditions {
  readonly restores: ReadonlyMap<number, RestoreState>
  readonly pendingCommands: number
  readonly blocked: Option.Option<{ readonly projection: string; readonly reason: string }>
  readonly degraded: Option.Option<string>
  readonly failure: Option.Option<string>
  readonly fenced: boolean
  readonly samplerStopped: boolean
}

const initial: Conditions = {
  restores: new Map(),
  pendingCommands: 0,
  blocked: Option.none(),
  degraded: Option.none(),
  failure: Option.none(),
  fenced: false,
  samplerStopped: false
}

/**
 * Total, and ordered so that a restore stays visible while it runs: a restore is the remedy for a failed
 * replica, so reporting `Failed` over it would hide the recovery a consumer is waiting on.
 */
const derive = (conditions: Conditions): ReplicaStatus.ReplicaStatus => {
  let ingesting: number | undefined
  let installing = false
  for (const restore of conditions.restores.values()) {
    if (restore.installing) installing = true
    else ingesting = Math.max(ingesting ?? 0, restore.processedBytes)
  }
  // An install blocks writes even when a concurrent restore is still ingesting, so it must not be masked
  // by the other restore's `Restoring`, which promises a replica that still accepts writes.
  if (!installing && ingesting !== undefined) return { _tag: "Restoring", processedBytes: ingesting }
  if (installing) return { _tag: "ReadOnly", reason: "A backup restore is installing" }
  if (conditions.samplerStopped) return { _tag: "Failed", message: "Replica health sampling stopped" }
  if (Option.isSome(conditions.failure)) return { _tag: "Failed", message: conditions.failure.value }
  if (conditions.fenced) return { _tag: "ReadOnly", reason: "Another writer generation owns this replica" }
  if (Option.isSome(conditions.blocked)) {
    return {
      _tag: "ProjectionBlocked",
      projection: conditions.blocked.value.projection,
      reason: conditions.blocked.value.reason
    }
  }
  if (Option.isSome(conditions.degraded)) return { _tag: "Degraded", reason: conditions.degraded.value }
  return { _tag: "Ready", pendingCommands: conditions.pendingCommands }
}

export class ReplicaHealth extends Context.Service<ReplicaHealth, {
  readonly status: Stream.Stream<ReplicaStatus.ReplicaStatus>
  readonly sample: Effect.Effect<void>
  readonly restoring: Effect.Effect<Restore, never, Scope.Scope>
}>()("@lucas-barake/effect-local-sql/ReplicaHealth") {}

export const layer = (definition: ReplicaDefinition.Any, options: Options): Layer.Layer<
  ReplicaHealth,
  never,
  ReplicaBootstrap.ReplicaBootstrap | ReplicaGate.ReplicaGate | SqlClient.SqlClient
> =>
  Layer.effect(
    ReplicaHealth,
    Effect.gen(function*() {
      yield* ReplicaBootstrap.ReplicaBootstrap
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient

      const conditions = yield* Ref.make(initial)
      const published = yield* Ref.make(derive(initial))
      // Acquired before the sampler is forked so reverse-order finalization interrupts the sampler first
      // and only then shuts the PubSub down, ending every live subscriber with a graceful stream end.
      const statuses = yield* Effect.acquireRelease(
        PubSub.sliding<ReplicaStatus.ReplicaStatus>({ capacity: 1, replay: 1 }),
        PubSub.shutdown
      )
      // Seeds the replay window. Without it the first subscriber would see nothing until the first change,
      // and a first sample equal to the initial value would dedup away and leave it waiting indefinitely.
      yield* PubSub.publish(statuses, derive(initial))

      // Every writer serialises here. `commit` is a read-modify-write over `conditions`, then over
      // `published`, then the publish that has to observe the same order, and the runtime may preempt a
      // fiber between any two operations once it has spent its yield budget. The forked sampler, each
      // concurrent restore's registration, progress and teardown, and every `status` subscriber all write,
      // so without the lock one writer can resurrect a restore another one just deleted and strand the
      // replica in `Restoring` forever.
      const writes = yield* Semaphore.make(1)
      const samples = yield* Semaphore.make(1)

      const commit = (update: (current: Conditions) => Conditions) =>
        writes.withPermit(Effect.gen(function*() {
          const next = update(yield* Ref.get(conditions))
          yield* Ref.set(conditions, next)
          const status = derive(next)
          if (Equal.equals(yield* Ref.get(published), status)) return
          yield* Ref.set(published, status)
          yield* PubSub.publish(statuses, status)
        }))

      const countPending = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: PendingRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_commit_outbox WHERE published = 0`
      })
      // Distinct types, not the first blocked row: a document type carrying no projection is still blocked
      // durably, and picking one row would let such a type shadow a type whose projection has to be
      // reported. The result is bounded by the number of stored document types.
      const findBlockedDocumentTypes = SqlSchema.findAll({
        Request: Schema.Void,
        Result: BlockedRow,
        execute: () =>
          sql`SELECT DISTINCT document_type FROM effect_local_documents
            WHERE projection_status != 'Ready' ORDER BY document_type`
      })
      // The other two conditions `QueryExecutor` refuses a query on. Reading only blocked documents would
      // report `Ready` on a replica whose every query already fails, which is worse than the constant it
      // replaced.
      const findBlockedProjection = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: BlockedProjectionRow,
        execute: () =>
          sql`SELECT projection_name FROM effect_local_projection_registry WHERE status != 'Ready'
            UNION
            SELECT projection_name FROM effect_local_document_projections WHERE status != 'Ready'
            ORDER BY projection_name LIMIT 1`
      })
      const findWriterGeneration = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: GenerationRow,
        execute: () => sql`SELECT writer_generation FROM effect_local_metadata WHERE singleton = 1`
      })

      const isDeclared = (projectionName: string): boolean =>
        definition.projections.some((projection: Projection.Any) => projection.name === projectionName)

      const projectionFor = (documentType: string): Option.Option<string> =>
        Arr.findFirst(
          definition.projections,
          (projection: Projection.Any) =>
            projection.document.name === documentType ? Option.some(projection.name) : Option.none()
        )

      // `ReplicaError.message` is only the reason tag, so the underlying driver or schema message is
      // pulled out here: "no such table: effect_local_commit_outbox" is actionable, "StorageUnavailable"
      // is not.
      // Walks the cause chain because the outermost message is often generic: a `SqlError` says only
      // "Failed to execute statement" while the driver error underneath it names the missing table.
      const detailOf = (cause: unknown): string => {
        const messages: Array<string> = []
        let current: unknown = cause
        while (Predicate.isNotNullish(current) && messages.length < 3) {
          if (
            Predicate.hasProperty(current, "message") && Predicate.isString(current.message) &&
            current.message.length > 0 && !messages.includes(current.message)
          ) {
            messages.push(current.message)
          }
          current = Predicate.hasProperty(current, "cause") ? current.cause : undefined
        }
        return messages.length > 0 ? messages.join(": ") : String(cause)
      }

      const sample = samples.withPermit(
        Effect.gen(function*() {
          const pending = yield* countPending(undefined)
          const blockedDocumentTypes = yield* findBlockedDocumentTypes(undefined)
          const blockedProjection = yield* findBlockedProjection(undefined)
          const generation = yield* findWriterGeneration(undefined)
          // Read in this order on purpose. `ReplicaGate.claim` sets its owner, bumps `writer_generation`
          // inside its transaction, republishes the matching permit, and only then clears the owner, and the
          // sampler holds nothing that keeps it out of that window. Observing no owner therefore proves the
          // permit read next is at least as new as the generation read above, and observing an owner means
          // any disagreement belongs to a claim this process is running rather than to a foreign writer.
          const claiming = yield* gate.claiming
          const permit = yield* gate.current
          // A missing metadata singleton means the replica's identity is gone, which the rest of the package
          // already treats as fatal. It is reported level-triggered rather than latched so a repair clears it.
          if (Option.isNone(generation)) {
            return yield* commit((current) => ({
              ...current,
              failure: Option.some("Replica metadata is missing")
            }))
          }
          const blocked = Arr.findFirst(blockedDocumentTypes, (row) =>
            Option.map(projectionFor(row.document_type), (projection) => ({
              projection,
              reason: `A ${row.document_type} document is not ready for projection`
            }))).pipe(
              Option.orElse(() =>
                Option.filter(blockedProjection, (row) =>
                  isDeclared(row.projection_name)).pipe(
                    Option.map((row) => ({
                      projection: row.projection_name,
                      reason: `The ${row.projection_name} projection is not ready`
                    }))
                  )
              )
            )
          yield* commit((current) => ({
            ...current,
            pendingCommands: Option.match(pending, {
              onNone: () =>
                0,
              onSome: (row) => row.count
            }),
            blocked,
            degraded: Option.none(),
            failure: Option.none(),
            // Only a generation ahead of the permit is a fence. `writer_generation` only ever increases, so a
            // durable value behind the permit cannot mean a foreign writer: it is this sampler's own read skew
            // over a local claim that committed and republished its permit after the generation was read.
            fenced: !claiming && generation.value.writer_generation > permit.writerGeneration
          }))
        }).pipe(
          // `SqlError | SchemaError` is the entire failure channel of the reads above, so handling both
          // closes it and the status stream itself can never fail, which is what keeps subscribers from
          // having to resubscribe. `catchCause` would also have swallowed defects and interruption.
          Effect.catchTags({
            SchemaError: (cause) =>
              commit((current) => ({
                ...current,
                failure: Option.some(`Replica storage is corrupt: ${detailOf(cause)}`)
              })),
            // A busy or locked database is transient, so it degrades rather than failing, and the previously
            // sampled fields are deliberately left in place instead of being reset to a guess.
            SqlError: (cause) =>
              cause.isRetryable
                ? commit((current) => ({
                  ...current,
                  degraded: Option.some(`Replica storage is unavailable: ${detailOf(cause)}`)
                }))
                : commit((current) => ({
                  ...current,
                  degraded: Option.none(),
                  failure: Option.some(`Replica storage failed: ${detailOf(cause)}`)
                }))
          }),
          Effect.withSpan("ReplicaHealth.sample")
        )
      )

      const editRestores = (edit: (restores: Map<number, RestoreState>) => void) =>
        commit((current) => {
          const restores = new Map(current.restores)
          edit(restores)
          return { ...current, restores }
        })

      // An edit that finds no entry is a no-op: the restore has already been retired, and reinstating it
      // would strand the replica in `Restoring`.
      const editRestore = (token: number, edit: (state: RestoreState) => RestoreState) =>
        editRestores((restores) => {
          const existing = restores.get(token)
          if (existing !== undefined) restores.set(token, edit(existing))
        })

      const nextToken = yield* Ref.make(0)
      const restoring = Effect.acquireRelease(
        Ref.getAndUpdate(nextToken, (token) => token + 1).pipe(
          Effect.tap((token) =>
            editRestores((restores) => restores.set(token, { processedBytes: 0, installing: false }))
          )
        ),
        (token) => editRestores((restores) => restores.delete(token))
      ).pipe(
        Effect.map((token): Restore => ({
          progress: (processedBytes) => editRestore(token, (state) => ({ ...state, processedBytes })),
          installing: Effect.acquireRelease(
            editRestore(token, (state) => ({ ...state, installing: true })),
            // Installation is the last phase, so ending it retires the restore rather than dropping it
            // back to the ingest it already finished. Scopes finalize inner-first, so reverting the flag
            // here would re-publish `Restoring` between the install ending and the outer scope removing
            // the token, walking the status backwards on every restore.
            () => editRestores((restores) => restores.delete(token))
          )
        }))
      )

      yield* sample.pipe(
        Effect.repeat(Schedule.spaced(options.sampleInterval)),
        Effect.onExit((exit) =>
          Exit.hasInterrupts(exit)
            ? Effect.void
            : commit((current) => ({ ...current, samplerStopped: true }))
        ),
        Effect.forkScoped({ startImmediately: true })
      )

      return {
        // Sampling on subscribe means a consumer never starts from a stale value, and it is what makes the
        // stream observable without waiting for a poll tick. Racing shutdown keeps a caller-owned stream
        // from holding its session permits when scope closure lands before the PubSub subscription exists.
        status: Stream.fromPubSub(statuses).pipe(
          Stream.onStart(Effect.raceFirst(sample, PubSub.awaitShutdown(statuses)))
        ),
        sample,
        restoring
      }
    })
  )
