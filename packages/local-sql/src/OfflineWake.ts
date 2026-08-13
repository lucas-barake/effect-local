import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import type * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
type HookRejection = Schema.JsonObject & { readonly _tag: string }

export interface Delivery {
  readonly wakeId: Identity.WakeId
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}

export type DeliveryOutcome = "Delivered" | "NotRecipient"
const DeliveryOutcome = Schema.Literals(["Delivered", "NotRecipient"])

export interface Options<R = never,> {
  readonly recipients: (input: {
    readonly spaceId: Identity.SpaceId
  }) => Effect.Effect<ReadonlyArray<Identity.ClientId>, HookRejection, R>
  readonly deliver: (wake: Delivery) => Effect.Effect<DeliveryOutcome, HookRejection, R>
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
  /** The caller's Watch scope owns the durable presence lease and its finalizer. */
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

const WatcherRow = Schema.Struct({ watcher_id: Schema.String })
const RuntimeRow = Schema.Struct({ runtime_id: Schema.String })
const CountRow = Schema.Struct({ count: NonNegativeInt })

const randomToken = (crypto: Crypto.Crypto) =>
  crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
  )

export const make = Effect.fnUntraced(function*<R,>(
  options: Options<R> | undefined,
  context: Context.Context<R>
): Effect.fn.Return<
  Service,
  ReplicaError.InvalidConfiguration | ReplicaError.StorageUnavailable,
  SqlClient.SqlClient | Crypto.Crypto | Scope.Scope
> {
  if (options === undefined) return disabled
  yield* Configuration.positiveSafeInteger("offlineWake.claimBatchSize", options.claimBatchSize)
  yield* Configuration.positiveSafeInteger(
    "offlineWake.maximumConcurrentRecipientResolutions",
    options.maximumConcurrentRecipientResolutions
  )
  yield* Configuration.positiveSafeInteger(
    "offlineWake.maximumConcurrentDeliveries",
    options.maximumConcurrentDeliveries
  )
  yield* Configuration.positiveSafeInteger(
    "offlineWake.maximumRecipientsPerSpace",
    options.maximumRecipientsPerSpace
  )
  const coalescingWindowMillis = yield* Configuration.positiveIntegerDurationMillis(
    "offlineWake.coalescingWindow",
    options.coalescingWindow
  )
  const pollIntervalMillis = yield* Configuration.positiveIntegerDurationMillis(
    "offlineWake.pollInterval",
    options.pollInterval
  )
  const claimLeaseMillis = yield* Configuration.positiveIntegerDurationMillis(
    "offlineWake.claimLeaseDuration",
    options.claimLeaseDuration
  )
  const hookTimeoutMillis = yield* Configuration.positiveIntegerDurationMillis(
    "offlineWake.hookTimeout",
    options.hookTimeout
  )
  const presenceLeaseMillis = yield* Configuration.positiveIntegerDurationMillis(
    "offlineWake.presenceLeaseDuration",
    options.presenceLeaseDuration
  )
  const presenceHeartbeatMillis = yield* Configuration.positiveIntegerDurationMillis(
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
  const runtimeId = yield* randomToken(crypto)
  const presences = new Map<string, {
    readonly spaceId: Identity.SpaceId
    readonly clientId: Identity.ClientId
  }>()
  const presenceGate = yield* Semaphore.make(1)
  let presenceDirty = false
  const prompt = yield* Queue.dropping<void>(1).pipe(
    (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
  )
  const notify = Queue.offer(prompt, undefined).pipe(Effect.asVoid)
  const runtimeStartedAt = yield* Clock.currentTimeMillis
  yield* Effect.acquireRelease(
    sql`INSERT INTO effect_local_server_watch_runtimes (runtime_id, expires_at)
      VALUES (${runtimeId}, ${runtimeStartedAt + presenceLeaseMillis})`.pipe(
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
      Effect.as(runtimeId)
    ),
    () =>
      presenceGate.withPermit(
        sql`DELETE FROM effect_local_server_watch_runtimes WHERE runtime_id = ${runtimeId}`
      ).pipe(
        Effect.catchTag("SqlError", (cause) => Effect.logWarning("Offline wake runtime cleanup failed", cause)),
        Effect.asVoid
      )
  )

  const claimSpaces = SqlSchema.findAll({
    Request: Schema.Struct({
      now: NonNegativeInt,
      token: Schema.String,
      claimedUntil: NonNegativeInt,
      limit: PositiveInt
    }),
    Result: SpaceRow,
    execute: ({ now, token, claimedUntil, limit }) =>
      sql`UPDATE effect_local_server_offline_wake_spaces
        SET claim_token = ${token}, claimed_until = ${claimedUntil}
        WHERE rowid IN (SELECT rowid FROM effect_local_server_offline_wake_spaces
          WHERE high_water_sequence > expanded_sequence AND next_attempt_at <= ${now}
            AND (claim_token IS NULL OR claimed_until <= ${now})
          ORDER BY next_attempt_at, space_id LIMIT ${limit})
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
  const releaseSpaceClaim = (row: typeof SpaceRow.Type) =>
    sql`UPDATE effect_local_server_offline_wake_spaces SET claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND claim_token = ${row.claim_token}`.pipe(
      Effect.asVoid,
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
  const failSpaceClaim = (row: typeof SpaceRow.Type, now: number) => {
    const retryDelay = Configuration.retryMillis(retryTiming, row.attempt_count + 1)
    const retryDelayMillis = Math.ceil(retryDelay)
    return sql`UPDATE effect_local_server_offline_wake_spaces SET
        attempt_count = attempt_count + 1, next_attempt_at = ${now + retryDelayMillis},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND claim_token = ${row.claim_token}`.pipe(
      Effect.asVoid,
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
  }

  const expand = Effect.fn("OfflineWake.expand")(function*(row: typeof SpaceRow.Type) {
    yield* Effect.annotateCurrentSpan({ "space.id": row.space_id })
    const recipientsExit = yield* options.recipients({ spaceId: row.space_id }).pipe(
      Effect.provide(context),
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
    const candidates = recipientsExit.value.value
    if (!Array.isArray(candidates)) {
      yield* failSpaceClaim(row, now)
      yield* Effect.logWarning("Offline wake recipients hook returned a non-array value").pipe(
        Effect.annotateLogs({ "space.id": row.space_id })
      )
      return
    }
    if (candidates.length > options.maximumRecipientsPerSpace) {
      yield* failSpaceClaim(row, now)
      yield* Effect.logWarning("Offline wake recipient capacity exceeded").pipe(
        Effect.annotateLogs({
          "space.id": row.space_id,
          recipients: candidates.length,
          limit: options.maximumRecipientsPerSpace
        })
      )
      return
    }
    const uniqueRecipients = new Set<Identity.ClientId>()
    for (const candidate of candidates) {
      const decoded = yield* Schema.decodeUnknownEffect(Identity.ClientId)(candidate).pipe(Effect.exit)
      if (decoded._tag === "Failure") {
        yield* failSpaceClaim(row, now)
        yield* Effect.logWarning("Offline wake recipients hook returned an invalid client ID").pipe(
          Effect.annotateLogs({ "space.id": row.space_id })
        )
        return
      }
      uniqueRecipients.add(decoded.value)
    }
    const recipients = [...uniqueRecipients].toSorted()
    const pending = yield* Effect.forEach(recipients, (clientId) =>
      Identity.makeWakeId.pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError(StorageUnavailable.make),
        Effect.map((wakeId) => ({ clientId, wakeId }))
      ))
    const pendingJson = yield* Codec.stringify(pending).pipe(Effect.mapError(StorageUnavailable.make))
    const generation = row.membership_generation + 1
    yield* sql.withTransaction(Effect.gen(function*() {
      const locked = yield* lockSpaceClaim({ spaceId: row.space_id, token: row.claim_token! })
      if (Option.isNone(locked)) return yield* Effect.void
      yield* sql`INSERT INTO effect_local_server_offline_wakes
            (space_id, client_id, wake_id, high_water_sequence, notified_sequence,
              membership_generation, attempt_count, next_attempt_at)
            SELECT ${row.space_id}, json_extract(candidate.value, '$.clientId'),
              json_extract(candidate.value, '$.wakeId'), ${row.high_water_sequence}, 0,
              ${generation}, 0, ${now}
            FROM json_each(${pendingJson}) AS candidate
            WHERE COALESCE((SELECT wake_ack_sequence
              FROM effect_local_server_replication_views AS acknowledged
              WHERE acknowledged.space_id = ${row.space_id}
                AND acknowledged.client_id = json_extract(candidate.value, '$.clientId')), 0)
              < ${row.high_water_sequence}
            ON CONFLICT (space_id, client_id) DO UPDATE SET
              wake_id = CASE
                WHEN effect_local_server_offline_wakes.notified_sequence <
                  effect_local_server_offline_wakes.high_water_sequence
                THEN effect_local_server_offline_wakes.wake_id ELSE excluded.wake_id END,
              high_water_sequence = MAX(effect_local_server_offline_wakes.high_water_sequence,
                excluded.high_water_sequence),
              membership_generation = excluded.membership_generation,
              next_attempt_at = CASE
                WHEN effect_local_server_offline_wakes.notified_sequence <
                  effect_local_server_offline_wakes.high_water_sequence
                THEN effect_local_server_offline_wakes.next_attempt_at ELSE excluded.next_attempt_at END`
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
    })).pipe(
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
  })

  const claimClients = SqlSchema.findAll({
    Request: Schema.Struct({
      now: NonNegativeInt,
      token: Schema.String,
      claimedUntil: NonNegativeInt,
      limit: PositiveInt
    }),
    Result: ClientRow,
    execute: ({ now, token, claimedUntil, limit }) =>
      sql`UPDATE effect_local_server_offline_wakes SET claim_token = ${token}, claimed_until = ${claimedUntil}
        WHERE rowid IN (SELECT wake.rowid FROM effect_local_server_offline_wakes AS wake
          WHERE wake.high_water_sequence > wake.notified_sequence AND wake.next_attempt_at <= ${now}
            AND (wake.claim_token IS NULL OR wake.claimed_until <= ${now})
            AND NOT EXISTS (SELECT 1 FROM effect_local_server_watch_presence AS presence
              INNER JOIN effect_local_server_watch_runtimes AS runtime
                ON runtime.runtime_id = presence.runtime_id
              WHERE presence.space_id = wake.space_id AND presence.client_id = wake.client_id
                AND runtime.expires_at > ${now})
          ORDER BY wake.next_attempt_at, wake.space_id, wake.client_id LIMIT ${limit})
        RETURNING space_id, client_id, wake_id, high_water_sequence, notified_sequence,
          membership_generation, attempt_count, next_attempt_at, claim_token, claimed_until`
  })
  const activePresence = SqlSchema.findOne({
    Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId, now: NonNegativeInt }),
    Result: Schema.Struct({ count: NonNegativeInt }),
    execute: ({ spaceId, clientId, now }) =>
      sql`SELECT COUNT(*) AS count FROM effect_local_server_watch_presence AS presence
        INNER JOIN effect_local_server_watch_runtimes AS runtime ON runtime.runtime_id = presence.runtime_id
        WHERE presence.space_id = ${spaceId} AND presence.client_id = ${clientId}
          AND runtime.expires_at > ${now}`
  })
  const insertPresence = SqlSchema.findOneOption({
    Request: Schema.Struct({
      spaceId: Identity.SpaceId,
      clientId: Identity.ClientId,
      watcherId: Schema.String,
      databaseRuntimeId: Schema.String,
      now: NonNegativeInt
    }),
    Result: WatcherRow,
    execute: ({ spaceId, clientId, watcherId, databaseRuntimeId, now }) =>
      sql`INSERT INTO effect_local_server_watch_presence
          (space_id, client_id, watcher_id, runtime_id)
        SELECT ${spaceId}, ${clientId}, ${watcherId}, ${databaseRuntimeId}
        WHERE NOT EXISTS (SELECT 1 FROM effect_local_server_offline_wakes AS wake
          WHERE wake.space_id = ${spaceId} AND wake.client_id = ${clientId}
            AND wake.claim_token IS NOT NULL AND wake.claimed_until > ${now})
        RETURNING watcher_id`
  })
  const heartbeatRuntime = SqlSchema.findOneOption({
    Request: Schema.Struct({ databaseRuntimeId: Schema.String, expiresAt: NonNegativeInt }),
    Result: RuntimeRow,
    execute: ({ databaseRuntimeId, expiresAt }) =>
      sql`UPDATE effect_local_server_watch_runtimes SET expires_at = ${expiresAt}
        WHERE runtime_id = ${databaseRuntimeId} RETURNING runtime_id`
  })

  const releaseClientClaim = (row: typeof ClientRow.Type, nextAttemptAt: number) =>
    sql`UPDATE effect_local_server_offline_wakes SET next_attempt_at = ${nextAttemptAt},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
          AND claim_token = ${row.claim_token}`.pipe(
      Effect.asVoid,
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
  const failClientClaim = (row: typeof ClientRow.Type, now: number) => {
    const retryDelay = Configuration.retryMillis(retryTiming, row.attempt_count + 1)
    const retryDelayMillis = Math.ceil(retryDelay)
    return sql`UPDATE effect_local_server_offline_wakes SET attempt_count = attempt_count + 1,
        next_attempt_at = ${now + retryDelayMillis},
        claim_token = NULL, claimed_until = NULL
        WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
          AND claim_token = ${row.claim_token}`.pipe(
      Effect.asVoid,
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
  }

  const deliver = Effect.fn("OfflineWake.deliver")(function*(row: typeof ClientRow.Type) {
    yield* Effect.annotateCurrentSpan({
      "wake.id": row.wake_id,
      "space.id": row.space_id,
      "client.id": row.client_id
    })
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
      Effect.flatMap((value) => Schema.decodeUnknownEffect(DeliveryOutcome)(value)),
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
    if (delivered.value.value === "NotRecipient") {
      const nextWakeId = yield* Identity.makeWakeId.pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError(StorageUnavailable.make)
      )
      yield* sql.withTransaction(Effect.gen(function*() {
        yield* sql`UPDATE effect_local_server_offline_wakes SET
          wake_id = ${nextWakeId}, attempt_count = 0,
          next_attempt_at = ${completedAt + coalescingWindowMillis},
          claim_token = NULL, claimed_until = NULL
          WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
            AND claim_token = ${row.claim_token}
            AND high_water_sequence > ${row.high_water_sequence}`
        yield* sql`DELETE FROM effect_local_server_offline_wakes
          WHERE space_id = ${row.space_id} AND client_id = ${row.client_id}
            AND claim_token = ${row.claim_token}
            AND high_water_sequence <= ${row.high_water_sequence}`
      })).pipe(
        Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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
  })

  const run = Effect.gen(function*() {
    const now = yield* Clock.currentTimeMillis
    yield* sql`DELETE FROM effect_local_server_watch_runtimes WHERE rowid IN (
      SELECT rowid FROM effect_local_server_watch_runtimes
      WHERE expires_at <= ${now} ORDER BY expires_at, runtime_id LIMIT ${options.claimBatchSize}
    )`.pipe(
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
    yield* sql`DELETE FROM effect_local_server_watch_presence WHERE rowid IN (
      SELECT presence.rowid FROM effect_local_server_watch_presence AS presence
      LEFT JOIN effect_local_server_watch_runtimes AS runtime ON runtime.runtime_id = presence.runtime_id
      WHERE runtime.runtime_id IS NULL LIMIT ${options.claimBatchSize}
    )`.pipe(
      Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
    )
    let spaceClaims = 0
    while (spaceClaims < options.claimBatchSize) {
      const claimNow = yield* Clock.currentTimeMillis
      const token = yield* randomToken(crypto)
      const limit = Math.min(
        options.maximumConcurrentRecipientResolutions,
        options.claimBatchSize - spaceClaims
      )
      const spaces = yield* claimSpaces({
        now: claimNow,
        token,
        claimedUntil: claimNow + claimLeaseMillis,
        limit
      }).pipe(Effect.mapError(StorageUnavailable.make))
      if (spaces.length === 0) break
      spaceClaims += spaces.length
      const orderedSpaces = spaces.toSorted((left, right) => left.space_id.localeCompare(right.space_id))
      yield* Effect.forEach(orderedSpaces, (claimed) =>
        expand(claimed).pipe(Effect.ensuring(
          releaseSpaceClaim(claimed).pipe(
            Effect.catch((failure) => Effect.logWarning("Offline wake space claim cleanup failed", failure))
          )
        )), {
        concurrency: options.maximumConcurrentRecipientResolutions,
        discard: true
      })
    }
    let clientClaims = 0
    while (clientClaims < options.claimBatchSize) {
      const claimNow = yield* Clock.currentTimeMillis
      const token = yield* randomToken(crypto)
      const limit = Math.min(options.maximumConcurrentDeliveries, options.claimBatchSize - clientClaims)
      const clients = yield* claimClients({
        now: claimNow,
        token,
        claimedUntil: claimNow + claimLeaseMillis,
        limit
      }).pipe(Effect.mapError(StorageUnavailable.make))
      if (clients.length === 0) break
      clientClaims += clients.length
      const orderedClients = clients.toSorted((left, right) => {
        const spaceOrder = left.space_id.localeCompare(right.space_id)
        if (spaceOrder !== 0) return spaceOrder
        return left.client_id.localeCompare(right.client_id)
      })
      yield* Effect.forEach(orderedClients, (claimed) =>
        deliver(claimed).pipe(Effect.ensuring(
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((releasedAt) => releaseClientClaim(claimed, releasedAt)),
            Effect.catch((failure) => Effect.logWarning("Offline wake client claim cleanup failed", failure))
          )
        )), {
        concurrency: options.maximumConcurrentDeliveries,
        discard: true
      })
    }
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

  const registerWatch = Effect.fn("OfflineWake.registerWatch")(function*(
    spaceId: Identity.SpaceId,
    clientId: Identity.ClientId
  ) {
    yield* Effect.annotateCurrentSpan({ "space.id": spaceId, "client.id": clientId })
    const watcherId = yield* randomToken(crypto)
    const register = Effect.gen(function*() {
      let registered = false
      while (!registered) {
        const now = yield* Clock.currentTimeMillis
        const inserted = yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`INSERT INTO effect_local_server_watch_runtimes (runtime_id, expires_at)
                VALUES (${runtimeId}, ${now + presenceLeaseMillis})
                ON CONFLICT (runtime_id) DO UPDATE SET expires_at = excluded.expires_at`
          return yield* insertPresence({
            spaceId,
            clientId,
            watcherId,
            databaseRuntimeId: runtimeId,
            now
          })
        })).pipe(Effect.mapError(StorageUnavailable.make))
        if (Option.isSome(inserted)) {
          registered = true
        } else {
          yield* Effect.sleep(presenceRegistrationRetryMillis).pipe(Effect.interruptible)
        }
      }
      presences.set(watcherId, { spaceId, clientId })
      return watcherId
    })
    yield* Effect.acquireRelease(
      presenceGate.withPermit(register),
      () =>
        presenceGate.withPermit(Effect.gen(function*() {
          presences.delete(watcherId)
          yield* sql`DELETE FROM effect_local_server_watch_presence
            WHERE space_id = ${spaceId} AND client_id = ${clientId} AND watcher_id = ${watcherId}`.pipe(
            Effect.catchTag("SqlError", (cause) => {
              presenceDirty = true
              return Effect.logWarning("Offline wake presence cleanup failed", cause)
            })
          )
          yield* notify
        }))
    )
  })

  const countPresenceDifferences = SqlSchema.findOne({
    Request: Schema.Struct({ databaseRuntimeId: Schema.String, encoded: Schema.String }),
    Result: CountRow,
    execute: ({ databaseRuntimeId, encoded }) =>
      sql`SELECT
        (SELECT COUNT(*) FROM json_each(${encoded}) AS local_presence
          WHERE NOT EXISTS (SELECT 1 FROM effect_local_server_watch_presence AS durable
            WHERE durable.runtime_id = ${databaseRuntimeId}
              AND durable.watcher_id = json_extract(local_presence.value, '$.watcherId')))
        + (SELECT COUNT(*) FROM effect_local_server_watch_presence AS durable
          WHERE durable.runtime_id = ${databaseRuntimeId}
            AND NOT EXISTS (SELECT 1 FROM json_each(${encoded}) AS local_presence
              WHERE json_extract(local_presence.value, '$.watcherId') = durable.watcher_id)) AS count`
  })

  const heartbeat = Effect.fnUntraced(function*(heartbeatAt: number) {
    const updated = yield* heartbeatRuntime({
      databaseRuntimeId: runtimeId,
      expiresAt: heartbeatAt + presenceLeaseMillis
    })
    if (Option.isSome(updated) && !presenceDirty) return
    presenceDirty = true
    yield* presenceGate.withPermit(Effect.gen(function*() {
      const encoded = yield* Codec.stringify([...presences].map(([watcherId, presence]) => ({
        watcherId,
        spaceId: presence.spaceId,
        clientId: presence.clientId
      }))).pipe(Effect.mapError(StorageUnavailable.make))
      yield* sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO effect_local_server_watch_runtimes (runtime_id, expires_at)
            VALUES (${runtimeId}, ${heartbeatAt + presenceLeaseMillis})
            ON CONFLICT (runtime_id) DO UPDATE SET expires_at = excluded.expires_at`
        yield* sql`INSERT INTO effect_local_server_watch_presence
            (space_id, client_id, watcher_id, runtime_id)
            SELECT json_extract(value, '$.spaceId'), json_extract(value, '$.clientId'),
              json_extract(value, '$.watcherId'), ${runtimeId}
            FROM json_each(${encoded}) AS local_presence
            WHERE NOT EXISTS (SELECT 1 FROM effect_local_server_offline_wakes AS wake
              WHERE wake.space_id = json_extract(local_presence.value, '$.spaceId')
                AND wake.client_id = json_extract(local_presence.value, '$.clientId')
                AND wake.claim_token IS NOT NULL AND wake.claimed_until > ${heartbeatAt})
            ON CONFLICT (space_id, client_id, watcher_id) DO UPDATE SET
              runtime_id = excluded.runtime_id`
        yield* sql`DELETE FROM effect_local_server_watch_presence
          WHERE runtime_id = ${runtimeId}
            AND watcher_id NOT IN (SELECT json_extract(value, '$.watcherId') FROM json_each(${encoded}))`
      }))
      const differences = yield* countPresenceDifferences({ databaseRuntimeId: runtimeId, encoded })
      presenceDirty = differences.count > 0
    }))
  })

  yield* Effect.sleep(presenceHeartbeatMillis).pipe(
    Effect.andThen(Clock.currentTimeMillis),
    Effect.flatMap(heartbeat),
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
      return Effect.logWarning("Offline wake presence heartbeat failed", cause)
    }),
    Effect.forever,
    Effect.forkScoped
  )

  const prompted = Queue.take(prompt)
  const polled = Effect.sleep(pollIntervalMillis)
  yield* Effect.raceFirst(prompted, polled).pipe(
    Effect.andThen(run),
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
      return Effect.logError("Offline wake dispatch failed", cause)
    }),
    Effect.forever,
    Effect.forkScoped
  )

  return { enqueue, notify, registerWatch } satisfies Service
})
