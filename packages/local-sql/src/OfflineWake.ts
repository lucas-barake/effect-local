import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import type * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Configuration from "./internal/configuration.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

const NonNegativeInt = pipe(Schema.isGreaterThanOrEqualTo(0), (check) => Schema.Int.check(check))
const PositiveInt = pipe(Schema.isGreaterThan(0), (check) => Schema.Int.check(check))
type HookRejection = Schema.JsonObject & { readonly _tag: string }

export interface Delivery {
  readonly wakeId: Identity.WakeId
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}

export interface Options<R = never,> {
  readonly recipients: (input: {
    readonly spaceId: Identity.SpaceId
  }) => Effect.Effect<ReadonlyArray<Identity.ClientId>, HookRejection, R>
  readonly deliver: (wake: Delivery) => Effect.Effect<void, HookRejection, R>
  readonly coalescingWindow: Duration.Input
  readonly pollInterval: Duration.Input
  readonly retryDelay: Duration.Input
  readonly maximumRetryDelay: Duration.Input
  readonly claimLeaseDuration: Duration.Input
  readonly hookTimeout: Duration.Input
  readonly presenceLeaseDuration: Duration.Input
  readonly presenceHeartbeatInterval: Duration.Input
  readonly claimBatchSize: number
  readonly maximumConcurrentRecipientResolutions: number
  readonly maximumConcurrentDeliveries: number
  readonly maximumRecipientsPerSpace: number
}

export interface Service {
  readonly enqueue: (
    spaceId: Identity.SpaceId,
    sequence: Identity.ServerSequence
  ) => Effect.Effect<void, ReplicaError.StorageUnavailable>
  readonly notify: Effect.Effect<void>
  readonly registerWatch: (
    spaceId: Identity.SpaceId,
    clientId: Identity.ClientId
  ) => Effect.Effect<void, ReplicaError.StorageUnavailable, Scope.Scope>
}

const disabled: Service = {
  enqueue: () => Effect.void,
  notify: Effect.void,
  registerWatch: () => Effect.void
}

const SpaceRow = Schema.Struct({
  space_id: Identity.SpaceId,
  high_water_sequence: Identity.ServerSequence,
  expanded_sequence: Identity.ServerSequence,
  membership_generation: NonNegativeInt,
  attempt_count: NonNegativeInt,
  next_attempt_at: NonNegativeInt,
  claim_token: Schema.NullOr(Schema.String),
  claimed_until: Schema.NullOr(NonNegativeInt)
})

const ClientRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  wake_id: Identity.WakeId,
  high_water_sequence: Identity.ServerSequence,
  notified_sequence: Identity.ServerSequence,
  membership_generation: PositiveInt,
  attempt_count: NonNegativeInt,
  next_attempt_at: NonNegativeInt,
  claim_token: Schema.NullOr(Schema.String),
  claimed_until: Schema.NullOr(NonNegativeInt)
})

const WakeAckRow = Schema.Struct({ wake_ack_sequence: Identity.ServerSequence })
const WatcherRow = Schema.Struct({ watcher_id: Schema.String })

const positiveInt = (option: string, value: number) => {
  if (Number.isSafeInteger(value) && value > 0) return Effect.void
  return Effect.fail(
    new ReplicaError.InvalidConfiguration({
      option,
      message: `${option} must be a positive safe integer`
    })
  )
}

const randomToken = (crypto: Crypto.Crypto) =>
  crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
  )

export const make = <R,>(
  options: Options<R> | undefined,
  context: Context.Context<R>
): Effect.Effect<
  Service,
  ReplicaError.InvalidConfiguration | ReplicaError.StorageUnavailable,
  SqlClient.SqlClient | Crypto.Crypto | Scope.Scope
> => {
  if (options === undefined) return Effect.succeed(disabled)
  return Effect.gen(function*() {
    yield* positiveInt("offlineWake.claimBatchSize", options.claimBatchSize)
    yield* positiveInt(
      "offlineWake.maximumConcurrentRecipientResolutions",
      options.maximumConcurrentRecipientResolutions
    )
    yield* positiveInt("offlineWake.maximumConcurrentDeliveries", options.maximumConcurrentDeliveries)
    yield* positiveInt("offlineWake.maximumRecipientsPerSpace", options.maximumRecipientsPerSpace)
    const coalescingWindowMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.coalescingWindow",
      options.coalescingWindow
    )
    const pollIntervalMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.pollInterval",
      options.pollInterval
    )
    const claimLeaseMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.claimLeaseDuration",
      options.claimLeaseDuration
    )
    const hookTimeoutMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.hookTimeout",
      options.hookTimeout
    )
    const presenceLeaseMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.presenceLeaseDuration",
      options.presenceLeaseDuration
    )
    const presenceHeartbeatMillis = yield* Configuration.positiveFiniteDurationMillis(
      "offlineWake.presenceHeartbeatInterval",
      options.presenceHeartbeatInterval
    )
    const presenceRegistrationRetryMillis = Math.min(pollIntervalMillis, 100)
    const retryTiming = yield* Configuration.retryTiming(options, "offlineWake.")
    if (hookTimeoutMillis >= claimLeaseMillis) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "offlineWake.hookTimeout",
        message: "offlineWake.hookTimeout must be less than offlineWake.claimLeaseDuration"
      })
    }
    if (presenceHeartbeatMillis >= presenceLeaseMillis) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "offlineWake.presenceHeartbeatInterval",
        message: "offlineWake.presenceHeartbeatInterval must be less than offlineWake.presenceLeaseDuration"
      })
    }

    const sql = yield* SqlClient.SqlClient
    const crypto = yield* Crypto.Crypto
    const prompt = yield* Queue.dropping<void>(1).pipe(
      (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
    )
    const notify = Queue.offer(prompt, undefined).pipe(Effect.asVoid)

    const dueSpaces = SqlSchema.findAll({
      Request: Schema.Struct({ now: NonNegativeInt, limit: PositiveInt }),
      Result: SpaceRow,
      execute: ({ now, limit }) =>
        sql`SELECT space_id, high_water_sequence, expanded_sequence, membership_generation,
          attempt_count, next_attempt_at, claim_token, claimed_until
        FROM effect_local_server_offline_wake_spaces
        WHERE high_water_sequence > expanded_sequence AND next_attempt_at <= ${now}
          AND (claim_token IS NULL OR claimed_until <= ${now})
        ORDER BY next_attempt_at, space_id LIMIT ${limit}`
    })
    const claimSpace = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Identity.SpaceId,
        now: NonNegativeInt,
        token: Schema.String,
        claimedUntil: NonNegativeInt
      }),
      Result: SpaceRow,
      execute: ({ spaceId, now, token, claimedUntil }) =>
        sql`UPDATE effect_local_server_offline_wake_spaces
        SET claim_token = ${token}, claimed_until = ${claimedUntil}
        WHERE space_id = ${spaceId} AND high_water_sequence > expanded_sequence
          AND next_attempt_at <= ${now} AND (claim_token IS NULL OR claimed_until <= ${now})
        RETURNING space_id, high_water_sequence, expanded_sequence, membership_generation,
          attempt_count, next_attempt_at, claim_token, claimed_until`
    })
    const lockSpaceClaim = SqlSchema.findOneOption({
      Request: Schema.Struct({ spaceId: Identity.SpaceId, token: Schema.String }),
      Result: SpaceRow,
      execute: ({ spaceId, token }) =>
        sql`UPDATE effect_local_server_offline_wake_spaces SET claim_token = claim_token
        WHERE space_id = ${spaceId} AND claim_token = ${token}
        RETURNING space_id, high_water_sequence, expanded_sequence, membership_generation,
          attempt_count, next_attempt_at, claim_token, claimed_until`
    })
    const findClient = SqlSchema.findOneOption({
      Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
      Result: ClientRow,
      execute: ({ spaceId, clientId }) =>
        sql`SELECT space_id, client_id, wake_id, high_water_sequence, notified_sequence,
          membership_generation, attempt_count, next_attempt_at, claim_token, claimed_until
        FROM effect_local_server_offline_wakes
        WHERE space_id = ${spaceId} AND client_id = ${clientId}`
    })
    const findWakeAck = SqlSchema.findOneOption({
      Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
      Result: WakeAckRow,
      execute: ({ spaceId, clientId }) =>
        sql`SELECT wake_ack_sequence FROM effect_local_server_replication_views
        WHERE space_id = ${spaceId} AND client_id = ${clientId}`
    })

    const releaseSpaceClaim = (row: typeof SpaceRow.Type) =>
      sql`UPDATE effect_local_server_offline_wake_spaces SET claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND claim_token = ${row.claim_token}`.pipe(
        Effect.asVoid,
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )
    const failSpaceClaim = (row: typeof SpaceRow.Type, now: number) =>
      sql`UPDATE effect_local_server_offline_wake_spaces SET
        attempt_count = attempt_count + 1,
        next_attempt_at = ${now + Configuration.retryMillis(retryTiming, row.attempt_count + 1)},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND claim_token = ${row.claim_token}`.pipe(
        Effect.asVoid,
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )

    const expand = (row: typeof SpaceRow.Type) =>
      Effect.gen(function*() {
        const recipientsExit = yield* options.recipients({ spaceId: row.space_id }).pipe(
          Effect.provide(context),
          Effect.flatMap((value) => Schema.decodeUnknownEffect(Schema.Array(Identity.ClientId))(value)),
          Effect.timeoutOption(hookTimeoutMillis),
          Effect.exit
        )
        const now = yield* Clock.currentTimeMillis
        if (recipientsExit._tag === "Failure") {
          if (Cause.hasInterrupts(recipientsExit.cause)) {
            yield* releaseSpaceClaim(row)
            yield* Effect.failCause(recipientsExit.cause)
          }
          yield* failSpaceClaim(row, now)
          yield* Effect.logWarning("Offline wake recipient resolution failed").pipe(
            Effect.annotateLogs({
              "space.id": row.space_id,
              "failure.kind": Cause.findErrorOption(recipientsExit.cause).pipe(Option.match({
                onNone: () => "defect",
                onSome: () => "failure"
              }))
            })
          )
          return
        }
        if (Option.isNone(recipientsExit.value)) {
          yield* failSpaceClaim(row, now)
          yield* Effect.logWarning("Offline wake recipient resolution timed out").pipe(
            Effect.annotateLogs({ "space.id": row.space_id })
          )
          return
        }
        const recipients = [...new Set(recipientsExit.value.value)].toSorted()
        if (recipients.length > options.maximumRecipientsPerSpace) {
          yield* failSpaceClaim(row, now)
          yield* Effect.logWarning("Offline wake recipient capacity exceeded").pipe(
            Effect.annotateLogs({
              "space.id": row.space_id,
              recipients: recipients.length,
              limit: options.maximumRecipientsPerSpace
            })
          )
          return
        }
        const generation = row.membership_generation + 1
        yield* Effect.gen(function*() {
          const locked = yield* lockSpaceClaim({ spaceId: row.space_id, token: row.claim_token! })
          if (Option.isNone(locked)) return yield* Effect.void
          for (const clientId of recipients) {
            const acknowledged = yield* findWakeAck({ spaceId: row.space_id, clientId }).pipe(
              Effect.map(Option.map((value) => value.wake_ack_sequence)),
              Effect.map(Option.getOrElse(() => Identity.ServerSequence.make(0)))
            )
            if (acknowledged >= row.high_water_sequence) {
              yield* sql`DELETE FROM effect_local_server_offline_wakes
                WHERE space_id = ${row.space_id} AND client_id = ${clientId}`
              continue
            }
            const existing = yield* findClient({ spaceId: row.space_id, clientId })
            let wakeId: Identity.WakeId
            let nextAttemptAt = now
            if (Option.isSome(existing) && existing.value.notified_sequence < existing.value.high_water_sequence) {
              wakeId = existing.value.wake_id
              nextAttemptAt = existing.value.next_attempt_at
            } else {
              wakeId = yield* Identity.makeWakeId.pipe(
                Effect.provideService(Crypto.Crypto, crypto),
                Effect.mapError(StorageUnavailable.make)
              )
            }
            yield* sql`INSERT INTO effect_local_server_offline_wakes
              (space_id, client_id, wake_id, high_water_sequence, notified_sequence,
                membership_generation, attempt_count, next_attempt_at)
              VALUES (${row.space_id}, ${clientId}, ${wakeId}, ${row.high_water_sequence}, 0,
                ${generation}, 0, ${nextAttemptAt})
              ON CONFLICT (space_id, client_id) DO UPDATE SET
                wake_id = ${wakeId},
                high_water_sequence = MAX(effect_local_server_offline_wakes.high_water_sequence,
                  excluded.high_water_sequence),
                membership_generation = excluded.membership_generation,
                next_attempt_at = ${nextAttemptAt}`
          }
          yield* sql`DELETE FROM effect_local_server_offline_wakes
            WHERE space_id = ${row.space_id} AND membership_generation < ${generation}`
          yield* sql`UPDATE effect_local_server_offline_wake_spaces SET
            expanded_sequence = MAX(expanded_sequence, ${row.high_water_sequence}),
            membership_generation = ${generation}, attempt_count = 0,
            next_attempt_at = CASE WHEN high_water_sequence > ${row.high_water_sequence}
              THEN ${now + coalescingWindowMillis} ELSE 0 END,
            claim_token = NULL, claimed_until = NULL
            WHERE space_id = ${row.space_id} AND claim_token = ${row.claim_token}`
          return yield* Effect.void
        }).pipe(
          (effect) => sql.withTransaction(effect),
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
      })

    const dueClients = SqlSchema.findAll({
      Request: Schema.Struct({ now: NonNegativeInt, limit: PositiveInt }),
      Result: ClientRow,
      execute: ({ now, limit }) =>
        sql`SELECT wake.space_id, wake.client_id, wake.wake_id, wake.high_water_sequence,
          wake.notified_sequence, wake.membership_generation, wake.attempt_count,
          wake.next_attempt_at, wake.claim_token, wake.claimed_until
        FROM effect_local_server_offline_wakes AS wake
        WHERE wake.high_water_sequence > wake.notified_sequence AND wake.next_attempt_at <= ${now}
          AND (wake.claim_token IS NULL OR wake.claimed_until <= ${now})
          AND NOT EXISTS (SELECT 1 FROM effect_local_server_watch_presence AS presence
            WHERE presence.space_id = wake.space_id AND presence.client_id = wake.client_id
              AND presence.expires_at > ${now})
        ORDER BY wake.next_attempt_at, wake.space_id, wake.client_id LIMIT ${limit}`
    })
    const claimClient = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Identity.SpaceId,
        clientId: Identity.ClientId,
        now: NonNegativeInt,
        token: Schema.String,
        claimedUntil: NonNegativeInt
      }),
      Result: ClientRow,
      execute: ({ spaceId, clientId, now, token, claimedUntil }) =>
        sql`UPDATE effect_local_server_offline_wakes SET claim_token = ${token}, claimed_until = ${claimedUntil}
        WHERE space_id = ${spaceId} AND client_id = ${clientId}
          AND high_water_sequence > notified_sequence AND next_attempt_at <= ${now}
          AND (claim_token IS NULL OR claimed_until <= ${now})
          AND NOT EXISTS (SELECT 1 FROM effect_local_server_watch_presence AS presence
            WHERE presence.space_id = ${spaceId} AND presence.client_id = ${clientId}
              AND presence.expires_at > ${now})
        RETURNING space_id, client_id, wake_id, high_water_sequence, notified_sequence,
          membership_generation, attempt_count, next_attempt_at, claim_token, claimed_until`
    })
    const activePresence = SqlSchema.findOne({
      Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId, now: NonNegativeInt }),
      Result: Schema.Struct({ count: NonNegativeInt }),
      execute: ({ spaceId, clientId, now }) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_server_watch_presence
        WHERE space_id = ${spaceId} AND client_id = ${clientId} AND expires_at > ${now}`
    })
    const insertPresence = SqlSchema.findOneOption({
      Request: Schema.Struct({
        spaceId: Identity.SpaceId,
        clientId: Identity.ClientId,
        watcherId: Schema.String,
        now: NonNegativeInt,
        expiresAt: NonNegativeInt
      }),
      Result: WatcherRow,
      execute: ({ spaceId, clientId, watcherId, now, expiresAt }) =>
        sql`INSERT INTO effect_local_server_watch_presence
          (space_id, client_id, watcher_id, expires_at)
        SELECT ${spaceId}, ${clientId}, ${watcherId}, ${expiresAt}
        WHERE NOT EXISTS (SELECT 1 FROM effect_local_server_offline_wakes AS wake
          WHERE wake.space_id = ${spaceId} AND wake.client_id = ${clientId}
            AND wake.claim_token IS NOT NULL AND wake.claimed_until > ${now})
        RETURNING watcher_id`
    })

    const releaseClientClaim = (row: typeof ClientRow.Type, nextAttemptAt: number) =>
      sql`UPDATE effect_local_server_offline_wakes SET next_attempt_at = ${nextAttemptAt},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
          AND claim_token = ${row.claim_token}`.pipe(
        Effect.asVoid,
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )
    const failClientClaim = (row: typeof ClientRow.Type, now: number) =>
      sql`UPDATE effect_local_server_offline_wakes SET attempt_count = attempt_count + 1,
        next_attempt_at = ${now + Configuration.retryMillis(retryTiming, row.attempt_count + 1)},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
          AND claim_token = ${row.claim_token}`.pipe(
        Effect.asVoid,
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )

    const deliver = (row: typeof ClientRow.Type) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const presence = yield* activePresence({ spaceId: row.space_id, clientId: row.client_id, now }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (presence.count > 0) {
          yield* releaseClientClaim(row, now + pollIntervalMillis)
          return
        }
        const delivered = yield* options.deliver({
          wakeId: row.wake_id,
          spaceId: row.space_id,
          clientId: row.client_id
        }).pipe(
          Effect.provide(context),
          Effect.timeoutOption(hookTimeoutMillis),
          Effect.exit
        )
        const completedAt = yield* Clock.currentTimeMillis
        if (delivered._tag === "Failure") {
          if (Cause.hasInterrupts(delivered.cause)) {
            yield* releaseClientClaim(row, completedAt)
            yield* Effect.failCause(delivered.cause)
          }
          yield* failClientClaim(row, completedAt)
          yield* Effect.logWarning("Offline wake delivery failed").pipe(
            Effect.annotateLogs({
              "wake.id": row.wake_id,
              "space.id": row.space_id,
              "client.id": row.client_id,
              "failure.kind": Cause.findErrorOption(delivered.cause).pipe(Option.match({
                onNone: () => "defect",
                onSome: () => "failure"
              }))
            })
          )
          return
        }
        if (Option.isNone(delivered.value)) {
          yield* failClientClaim(row, completedAt)
          yield* Effect.logWarning("Offline wake delivery timed out").pipe(
            Effect.annotateLogs({
              "wake.id": row.wake_id,
              "space.id": row.space_id,
              "client.id": row.client_id
            })
          )
          return
        }
        const nextWakeId = yield* Identity.makeWakeId.pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError(StorageUnavailable.make)
        )
        yield* sql`UPDATE effect_local_server_offline_wakes SET
          notified_sequence = MAX(notified_sequence, ${row.high_water_sequence}),
          wake_id = CASE WHEN high_water_sequence > ${row.high_water_sequence} THEN ${nextWakeId} ELSE wake_id END,
          attempt_count = 0,
          next_attempt_at = CASE WHEN high_water_sequence > ${row.high_water_sequence}
            THEN ${completedAt + coalescingWindowMillis} ELSE 0 END,
          claim_token = NULL, claimed_until = NULL
          WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
            AND claim_token = ${row.claim_token}`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.asVoid
        )
      }).pipe(Effect.withSpan("OfflineWake.deliver", {
        attributes: {
          "wake.id": row.wake_id,
          "space.id": row.space_id,
          "client.id": row.client_id
        }
      }))

    const run = Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis
      yield* sql`DELETE FROM effect_local_server_watch_presence WHERE expires_at <= ${now}`.pipe(
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )
      const spaces = yield* dueSpaces({ now, limit: options.claimBatchSize }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      yield* Effect.forEach(spaces, (candidate) =>
        Effect.gen(function*() {
          const token = yield* randomToken(crypto)
          const claimed = yield* claimSpace({
            spaceId: candidate.space_id,
            now,
            token,
            claimedUntil: now + claimLeaseMillis
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(claimed)) {
            yield* expand(claimed.value).pipe(Effect.ensuring(
              releaseSpaceClaim(claimed.value).pipe(
                Effect.catch((error) => Effect.logWarning("Offline wake space claim cleanup failed", error))
              )
            ))
          }
        }), {
        concurrency: options.maximumConcurrentRecipientResolutions,
        discard: true
      })
      const clients = yield* dueClients({ now, limit: options.claimBatchSize }).pipe(
        Effect.mapError(StorageUnavailable.make)
      )
      yield* Effect.forEach(clients, (candidate) =>
        Effect.gen(function*() {
          const token = yield* randomToken(crypto)
          const claimed = yield* claimClient({
            spaceId: candidate.space_id,
            clientId: candidate.client_id,
            now,
            token,
            claimedUntil: now + claimLeaseMillis
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(claimed)) {
            yield* deliver(claimed.value).pipe(Effect.ensuring(
              Clock.currentTimeMillis.pipe(
                Effect.flatMap((releasedAt) => releaseClientClaim(claimed.value, releasedAt)),
                Effect.catch((error) => Effect.logWarning("Offline wake client claim cleanup failed", error))
              )
            ))
          }
        }), {
        concurrency: options.maximumConcurrentDeliveries,
        discard: true
      })
    }).pipe(Effect.withSpan("OfflineWake.run"))

    const enqueue = (spaceId: Identity.SpaceId, sequence: Identity.ServerSequence) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          sql`INSERT INTO effect_local_server_offline_wake_spaces
            (space_id, high_water_sequence, expanded_sequence, membership_generation,
              attempt_count, next_attempt_at)
            VALUES (${spaceId}, ${sequence}, 0, 0, 0, ${now + coalescingWindowMillis})
            ON CONFLICT (space_id) DO UPDATE SET
              high_water_sequence = MAX(effect_local_server_offline_wake_spaces.high_water_sequence,
                excluded.high_water_sequence),
              next_attempt_at = CASE
                WHEN effect_local_server_offline_wake_spaces.expanded_sequence >=
                  effect_local_server_offline_wake_spaces.high_water_sequence
                THEN excluded.next_attempt_at
                ELSE effect_local_server_offline_wake_spaces.next_attempt_at END`
        ),
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
        Effect.asVoid
      )

    const registerWatch = (spaceId: Identity.SpaceId, clientId: Identity.ClientId) =>
      Effect.gen(function*() {
        const watcherId = yield* randomToken(crypto)
        let registered = false
        while (!registered) {
          const now = yield* Clock.currentTimeMillis
          const inserted = yield* insertPresence({
            spaceId,
            clientId,
            watcherId,
            now,
            expiresAt: now + presenceLeaseMillis
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(inserted)) {
            registered = true
          } else {
            yield* Effect.sleep(presenceRegistrationRetryMillis)
          }
        }
        yield* Effect.addFinalizer(() =>
          sql`DELETE FROM effect_local_server_watch_presence
            WHERE space_id = ${spaceId} AND client_id = ${clientId} AND watcher_id = ${watcherId}`.pipe(
            Effect.andThen(notify),
            Effect.catchCause((cause) => Effect.logWarning("Offline wake presence cleanup failed", cause)),
            Effect.asVoid
          )
        )
        yield* Effect.sleep(presenceHeartbeatMillis).pipe(
          Effect.andThen(Clock.currentTimeMillis),
          Effect.flatMap((heartbeatAt) =>
            sql`UPDATE effect_local_server_watch_presence SET expires_at = ${heartbeatAt + presenceLeaseMillis}
              WHERE space_id = ${spaceId} AND client_id = ${clientId} AND watcher_id = ${watcherId}`
          ),
          Effect.catchCause((cause) => {
            if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
            return Effect.logWarning("Offline wake presence heartbeat failed", cause)
          }),
          Effect.forever,
          Effect.forkScoped
        )
      })

    const prompted = Queue.take(prompt)
    const polled = Effect.sleep(pollIntervalMillis)
    yield* Effect.raceFirst(prompted, polled).pipe(
      Effect.andThen(run),
      Effect.catch(() => Effect.logError("Offline wake dispatch failed")),
      Effect.forever,
      Effect.forkScoped
    )

    return { enqueue, notify, registerWatch } satisfies Service
  })
}
