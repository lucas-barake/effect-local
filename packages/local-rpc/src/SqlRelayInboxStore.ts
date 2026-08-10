import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "./internal/relayInboxMigrations.js"
import * as RelayInboxStore from "./RelayInboxStore.js"

/**
 * `RelayInboxStore` over a generic `SqlClient`, so the operator chooses the database.
 *
 * Every guard is expressed as a predicate inside the write that depends on it. The owning entity is
 * the sole writer for an inbox, but its rpc lane and its forked dispatcher fibers issue statements
 * concurrently, so a read followed by a separate write is still a race within one process.
 */

const EnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(RelayInboxStore.InboxEnvelope)
)

/**
 * PostgreSQL returns `BIGINT`, `COUNT(*)` and `SUM(...)` as strings, and no default type parser is
 * installed, so a bare `Schema.Number` would fail to decode every row on that dialect.
 */
const DatabaseInt = Schema.Union([Schema.Int, Schema.NumberFromString]).check(Schema.isInt())

/** Rows are decoded, never asserted: the database is an external boundary like any other. */
const PendingRow = Schema.Struct({
  relay_message_id: Schema.String,
  tenant_id: Schema.String,
  sender_subject_id: Schema.String,
  sender_peer_id: Schema.String,
  sender_replica_incarnation: DatabaseInt,
  sender_connection_epoch: Schema.String,
  sender_sequence: DatabaseInt,
  deliveries: DatabaseInt,
  envelope: Schema.String,
  message_hash: Schema.String,
  outer_envelope_digest: Schema.String
})

const ExistingRow = Schema.Struct({
  state: Schema.String,
  terminal_at: Schema.NullOr(DatabaseInt),
  outer_envelope_digest: Schema.String,
  message_hash: Schema.String,
  // Read so a revive can be charged the bytes it restores to pending, not just the row count.
  byte_size: DatabaseInt
})

const CountRow = Schema.Struct({
  pending_count: DatabaseInt,
  pending_bytes: DatabaseInt
})

const RetainedRow = Schema.Struct({ retained_count: DatabaseInt })

const AbandonedRow = Schema.Struct({
  // Decoded with its domain schema rather than asserted: the database is an external boundary, and
  // a branded id that never passed its own pattern check is not the identity it claims to be.
  relay_message_id: Identity.RelayMessageId,
  state: Schema.String,
  deliveries: DatabaseInt,
  terminal_at: Schema.NullOr(DatabaseInt)
})

const ChargeRow = Schema.Struct({
  state: Schema.String,
  deliveries: DatabaseInt,
  charged_at: DatabaseInt
})

const ProgressRow = Schema.Struct({ progressed: DatabaseInt })

const IdRow = Schema.Struct({ inbox_key: Schema.String, relay_message_id: Schema.String })

/**
 * A channel's identity as a fixed width digest.
 *
 * Ordering is indexed on this rather than on the raw channel columns because a composite index over
 * them would exceed MySQL's key length limit once the subject is sized for real input.
 */
const channelDigest = (channel: RelayInboxStore.ChannelKey) =>
  Canonical.digest({
    domain: "effect-local/relay-inbox-channel",
    tenantId: channel.tenantId,
    senderSubjectId: channel.senderSubjectId,
    senderPeerId: channel.senderPeerId,
    senderReplicaIncarnation: channel.senderReplicaIncarnation,
    senderConnectionEpoch: channel.senderConnectionEpoch
  })

const table = Migrations.tableName

export const make = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  // Captured once so the service's own methods carry no requirements of their own.
  const crypto = yield* Crypto.Crypto

  yield* Migrations.run.pipe(
    // Discriminated rather than collapsed. A `MigrationError` is a permanent lineage fault — a
    // duplicate id, an unloadable module — and reporting it as unavailability would have every
    // caller above retry a schema that will never build.
    Effect.catchTag(
      "SqlError",
      (cause) => Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))
    ),
    Effect.catchTag("MigrationError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.MigrationFailed({ migration: Migrations.tableName, cause })
        })
      ))
  )

  const decodeEnvelope = Schema.decodeUnknownEffect(EnvelopeJson)
  const encodeEnvelope = Schema.encodeEffect(EnvelopeJson)

  const findExisting = SqlSchema.findOneOption({
    Request: Schema.Struct({ inboxKey: Schema.String, relayMessageId: Schema.String }),
    Result: ExistingRow,
    execute: (request) =>
      sql`
        SELECT state, terminal_at, outer_envelope_digest, message_hash, byte_size FROM ${sql(table)}
        WHERE inbox_key = ${request.inboxKey} AND relay_message_id = ${request.relayMessageId}
      `
  })

  const findUsage = SqlSchema.findOne({
    Request: Schema.String,
    Result: CountRow,
    execute: (inboxKey) =>
      sql`
        SELECT COUNT(*) AS pending_count, COALESCE(SUM(byte_size), 0) AS pending_bytes
        FROM ${sql(table)} WHERE inbox_key = ${inboxKey} AND state = 'Pending'
      `
  })

  // SQLite spells the two argument maximum `MAX`; PostgreSQL and MySQL spell it `GREATEST`.
  const greatestFn = sql.onDialectOrElse({
    orElse: () => "MAX",
    pg: () => "GREATEST",
    mysql: () => "GREATEST"
  })

  const admit = (request: RelayInboxStore.AdmissionRequest) =>
    sql.withTransaction(Effect.gen(function*() {
      // The channel is redundant with the envelope's own sender fields, so a disagreement means
      // the message would be filed under a fabricated ordering stream — the unique index would
      // guard nothing and per channel ordering would silently break.
      const sender = request.envelope.sender
      if (
        request.channel.tenantId !== sender.tenantId ||
        request.channel.senderSubjectId !== sender.subjectId ||
        request.channel.senderPeerId !== sender.peerId ||
        request.channel.senderReplicaIncarnation !== sender.replicaIncarnation ||
        request.channel.senderConnectionEpoch !== sender.connectionEpoch
      ) {
        return { _tag: "Conflict" } as const
      }

      const existing = yield* findExisting({
        inboxKey: request.inboxKey,
        relayMessageId: request.envelope.relayMessageId
      })

      if (Option.isSome(existing)) {
        const row = existing.value
        // Conflicts are detected on the outer envelope digest, which binds sender, recipient,
        // relay, sequence, epoch, document, lineage and provenance. The message hash covers only
        // the inner payload, and both peers key their own deduplication on the digest.
        if (row.outer_envelope_digest !== request.envelope.outerEnvelopeDigest) {
          return { _tag: "Conflict" } as const
        }
        const state = yield* Schema.decodeUnknownEffect(RelayInboxStore.InboxState)(row.state).pipe(
          Effect.mapError(() =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: `unknown inbox state ${row.state}` })
            })
          )
        )
        if (state !== "DeadLettered" && state !== "Expired") {
          return { _tag: "Duplicate", state } as const
        }
        // The sender still holds custody and is replaying, which is better evidence than this
        // inbox's earlier decision to give up. Revived as a fresh attempt so the message is not
        // lost the moment the sender drops it from its outbox.
        //
        // A revive creates a `Pending` row, so it is charged against the same caps as a first
        // admission; otherwise replaying abandoned identities would restore unbounded pending work.
        const revivedUsage = yield* findUsage(request.inboxKey)
        if (
          revivedUsage.pending_count + 1 > request.quota.maxPendingMessages ||
          revivedUsage.pending_bytes + row.byte_size > request.quota.maxPendingBytes
        ) {
          return { _tag: "QuotaExceeded" } as const
        }
        yield* sql`
          UPDATE ${sql(table)}
          SET state = 'Pending', deliveries = 0, terminal_at = NULL,
              expires_at = ${request.now + request.messageTtlMillis},
              deduplicate_until = ${sql.literal(greatestFn)}(
                deduplicate_until,
                ${request.now + Math.max(request.messageTtlMillis, request.senderRetryHorizonMillis)}
              )
          WHERE inbox_key = ${request.inboxKey}
            AND relay_message_id = ${request.envelope.relayMessageId}
        `
        return { _tag: "Admitted" } as const
      }

      const usage = yield* findUsage(request.inboxKey)
      const encoded = yield* encodeEnvelope(request.envelope).pipe(
        Effect.mapError(() =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({ cause: "relay inbox envelope failed to encode" })
          })
        )
      )
      const byteSize = encoded.length
      if (
        usage.pending_count + 1 > request.quota.maxPendingMessages ||
        usage.pending_bytes + byteSize > request.quota.maxPendingBytes
      ) {
        return { _tag: "QuotaExceeded" } as const
      }

      const channelKey = yield* channelDigest(request.channel).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      // The identity outlives the window in which the sender may still replay it. Derived from the
      // horizon the sender negotiated rather than a fixed server value, because a shorter one would
      // let a replay arrive after the record was collected and be applied a second time.
      const deduplicateUntil = request.now +
        Math.max(request.messageTtlMillis, request.senderRetryHorizonMillis)

      yield* sql`
        INSERT INTO ${sql(table)} (
          inbox_key, relay_message_id, channel_key, tenant_id, sender_subject_id, sender_peer_id,
          sender_replica_incarnation, sender_connection_epoch, sender_sequence, state, deliveries,
          envelope, message_hash, outer_envelope_digest, byte_size, created_at, expires_at,
          deduplicate_until, terminal_at
        ) VALUES (
          ${request.inboxKey}, ${request.envelope.relayMessageId}, ${channelKey},
          ${request.channel.tenantId}, ${request.channel.senderSubjectId},
          ${request.channel.senderPeerId}, ${request.channel.senderReplicaIncarnation},
          ${request.channel.senderConnectionEpoch}, ${request.envelope.sender.sequence},
          'Pending', 0, ${encoded}, ${request.envelope.messageHash},
          ${request.envelope.outerEnvelopeDigest}, ${byteSize}, ${request.now},
          ${request.now + request.messageTtlMillis}, ${deduplicateUntil}, NULL
        )
      `
      return { _tag: "Admitted" } as const
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })
        )),
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        )),
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        ))
    )

  // Each sender's most recently started channel goes first. A sender allocates a fresh connection
  // epoch per session, so a crash looping sender leaves one channel per lapsed epoch — and under
  // age order alone that stale trail occupies every delivery slot while the epoch the sender is
  // connected on right now waits behind it until the stale heads dead letter or expire. Recency is
  // the channel's first pending admission, because the epochs themselves are opaque and carry no
  // order.
  //
  // Superseded channels are deprioritized, never dropped. Priority alone would starve them the
  // other way: a live channel that is never empty never leaves a slot idle, and a superseded
  // message would sit `Pending` until expiry destroyed the last copy — the sender only replays its
  // current epoch's outbox. So a head that has burned half its TTL is promoted ahead of the live
  // channel: deprioritization is bounded at half the message's lifetime and the other half is its
  // delivery window.
  //
  // That promotion is capped, because it is a pre-emption and pre-empting every slot at once is
  // indistinguishable from being deaf. Aged heads all cross their half-life together — one per
  // lapsed epoch, so a long-lived sender accumulates them faster than a jammed recipient settles
  // them — and once more of them existed than the caller had slots they filled the window between
  // themselves, evicting the only channel the sender still replays. The recipient then received
  // nothing at all, for hours, on a session that stayed healthy the whole time.
  //
  // Aged heads therefore take the window minus one slot per live channel, so every sender still
  // connected keeps a route in. The floor is one: at a single slot there is nothing to reserve and
  // aging keeps its old, absolute precedence — a lone aged head pre-empts, settles, and hands the
  // slot straight back. Reserving per live channel rather than one slot overall matters as soon as
  // an inbox has more than one sender: a single reservation is taken by whichever live head is
  // oldest, and a live channel stuck behind it is starved exactly as before.
  const findHeads = SqlSchema.findAll({
    Request: Schema.Struct({
      inboxKey: Schema.String,
      limit: Schema.Int,
      now: Schema.Number
    }),
    Result: PendingRow,
    execute: (request) =>
      sql`
        SELECT relay_message_id, tenant_id, sender_subject_id, sender_peer_id,
               sender_replica_incarnation, sender_connection_epoch, sender_sequence,
               deliveries, envelope, message_hash, outer_envelope_digest
        FROM (
          SELECT ranked.*,
            ROW_NUMBER() OVER (
              PARTITION BY CASE WHEN created_at + expires_at <= ${request.now * 2} THEN 1 ELSE 0 END
              ORDER BY created_at ASC, channel_key ASC
            ) AS aged_rank,
            SUM(CASE WHEN channel_recency = 1 THEN 1 ELSE 0 END) OVER () AS live_channels
          FROM (
            SELECT heads.*, ROW_NUMBER() OVER (
              PARTITION BY tenant_id, sender_subject_id, sender_peer_id, sender_replica_incarnation
              ORDER BY channel_started_at DESC, channel_key DESC
            ) AS channel_recency
            FROM (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY channel_key ORDER BY sender_sequence ASC
                ) AS rn,
                MIN(created_at) OVER (PARTITION BY channel_key) AS channel_started_at
              FROM ${sql(table)}
              WHERE inbox_key = ${request.inboxKey} AND state = 'Pending'
            ) heads
            WHERE rn = 1
          ) ranked
        ) prioritized
        ORDER BY CASE
            WHEN created_at + expires_at <= ${request.now * 2}
              AND aged_rank <= CASE
                WHEN ${sql.literal(String(request.limit))} - live_channels < 1 THEN 1
                ELSE ${sql.literal(String(request.limit))} - live_channels
              END THEN 0
            WHEN channel_recency = 1 THEN 1
            WHEN created_at + expires_at <= ${request.now * 2} THEN 2
            ELSE 3
          END,
          created_at ASC, channel_key ASC
        LIMIT ${sql.literal(String(request.limit))}
      `
  })

  const pendingHeads = (inboxKey: string, options: { readonly limit: number; readonly now: number }) =>
    findHeads({ inboxKey, limit: options.limit, now: options.now }).pipe(
      // A `SchemaError` here is a row this store cannot decode, which is corruption rather than
      // downtime. Collapsing the two would have callers retry a row that will never decode.
      Effect.catchTag(
        "SqlError",
        (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))
      ),
      Effect.catchTag(
        "SchemaError",
        (cause) => Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) }))
      ),
      Effect.flatMap(Effect.forEach((row) =>
        Effect.gen(function*() {
          const envelope = yield* decodeEnvelope(row.envelope).pipe(
            Effect.mapError(() =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause: `undecodable envelope for ${row.relay_message_id}` })
              })
            )
          )
          // The redundant columns exist to be checked. A row whose envelope disagrees with the
          // identity and ordering it was filed under cannot be replayed safely.
          if (envelope.messageHash !== row.message_hash) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: `message hash mismatch for ${row.relay_message_id}` })
            })
          }
          if (envelope.relayMessageId !== row.relay_message_id) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: `envelope identity disagrees with key column for ${row.relay_message_id}`
              })
            })
          }
          if (envelope.outerEnvelopeDigest !== row.outer_envelope_digest) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: `outer envelope digest mismatch for ${row.relay_message_id}`
              })
            })
          }
          if (envelope.sender.sequence !== row.sender_sequence) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: `sequence mismatch for ${row.relay_message_id}` })
            })
          }
          const channel = yield* Schema.decodeUnknownEffect(RelayInboxStore.ChannelKey)({
            tenantId: row.tenant_id,
            senderSubjectId: row.sender_subject_id,
            senderPeerId: row.sender_peer_id,
            senderReplicaIncarnation: row.sender_replica_incarnation,
            senderConnectionEpoch: row.sender_connection_epoch
          }).pipe(
            Effect.mapError(() =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause: `invalid channel for ${row.relay_message_id}` })
              })
            )
          )
          // The five channel columns are redundant metadata exactly like the four above, and they
          // are what everything downstream orders and routes by: the SQL head-selection key, the
          // entity's `channelId`, and its busy and withheld channel bookkeeping. A row whose
          // columns disagree with its own envelope would be replayed under an ordering stream that
          // contradicts its content. `admit` enforces this at write time; a row that reaches here
          // violating it did not come from `admit`.
          if (
            channel.tenantId !== envelope.sender.tenantId ||
            channel.senderSubjectId !== envelope.sender.subjectId ||
            channel.senderPeerId !== envelope.sender.peerId ||
            channel.senderReplicaIncarnation !== envelope.sender.replicaIncarnation ||
            channel.senderConnectionEpoch !== envelope.sender.connectionEpoch
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: `channel columns disagree with the envelope sender for ${row.relay_message_id}`
              })
            })
          }
          return {
            relayMessageId: envelope.relayMessageId,
            channel,
            senderSequence: row.sender_sequence,
            deliveries: row.deliveries,
            envelope
          } satisfies RelayInboxStore.PendingMessage
        })
      ))
    )

  const findCharge = SqlSchema.findOneOption({
    Request: Schema.Struct({ inboxKey: Schema.String, relayMessageId: Schema.String }),
    Result: ChargeRow,
    execute: (request) =>
      sql`
        SELECT state, deliveries, charged_at FROM ${sql(table)}
        WHERE inbox_key = ${request.inboxKey} AND relay_message_id = ${request.relayMessageId}
      `
  })

  /**
   * Whether any message in the inbox settled after the given charge time. A settlement proves the
   * recipient was alive and draining at that moment, so a message still failing to settle across
   * it is failing for its own reasons rather than because sessions keep dying.
   */
  const findProgress = SqlSchema.findOne({
    Request: Schema.Struct({ inboxKey: Schema.String, since: Schema.Int }),
    Result: ProgressRow,
    execute: (request) =>
      sql`
        SELECT COUNT(*) AS progressed FROM ${sql(table)}
        WHERE inbox_key = ${request.inboxKey}
          AND state IN ('Acknowledged', 'Rejected')
          AND terminal_at > ${request.since}
      `
  })

  const recordDelivery = (
    inboxKey: string,
    relayMessageId: string,
    options: { readonly maxDeliveries: number; readonly now: number }
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      const before = yield* findCharge({ inboxKey, relayMessageId })
      if (Option.isNone(before)) {
        return { _tag: "Recorded", deliveries: 0 } as const
      }
      // Charged only for rows that are still `Pending`, so a message settled concurrently by its
      // recipient cannot be charged for a delivery it no longer needs.
      if (before.value.state !== "Pending") {
        return { _tag: "Recorded", deliveries: before.value.deliveries } as const
      }
      // The budget counts consecutive transmissions with zero settled progress in the inbox, not
      // lifetime transmissions. Session churn that still drains something restarts every other
      // message's streak, so only a recipient that settles nothing across the whole budget — a
      // poisoned message or a peer that never comes back — loses custody to dead letter.
      const progressed = yield* findProgress({ inboxKey, since: before.value.charged_at })
      const deliveries = progressed.progressed > 0 ? 1 : before.value.deliveries + 1
      // Strictly greater: the attempt that spends the last of the budget is still a real delivery
      // the recipient can settle, so dead lettering at equality would waste it.
      if (deliveries > options.maxDeliveries) {
        yield* sql`
          UPDATE ${sql(table)}
          SET deliveries = ${deliveries}, state = 'DeadLettered', terminal_at = ${options.now}
          WHERE inbox_key = ${inboxKey} AND relay_message_id = ${relayMessageId}
            AND state = 'Pending'
        `
        return { _tag: "DeadLettered", deliveries } as const
      }
      yield* sql`
        UPDATE ${sql(table)} SET deliveries = ${deliveries}, charged_at = ${options.now}
        WHERE inbox_key = ${inboxKey} AND relay_message_id = ${relayMessageId}
          AND state = 'Pending'
      `
      return { _tag: "Recorded", deliveries } as const
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })
        )),
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        )),
      // A COUNT aggregate that returns no row at all is corruption, not downtime; retrying it
      // would spin against a database that will never answer differently.
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        ))
    )

  const deadLetterExhausted = (
    inboxKey: string,
    relayMessageId: string,
    options: { readonly maxDeliveries: number; readonly now: number }
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      // Conditional on the stored row rather than the caller's read: a concurrent settlement may
      // have restarted the streak since the caller polled, and that row keeps its refund.
      const before = yield* findCharge({ inboxKey, relayMessageId })
      if (
        Option.isNone(before) ||
        before.value.state !== "Pending" ||
        before.value.deliveries < options.maxDeliveries
      ) {
        return false
      }
      // The settlement that landed after this row's last charge is the streak restart the next
      // charge would apply; destroying custody on the stale count would take back the refund.
      const progressed = yield* findProgress({ inboxKey, since: before.value.charged_at })
      if (progressed.progressed > 0) {
        yield* sql`
          UPDATE ${sql(table)} SET deliveries = 0
          WHERE inbox_key = ${inboxKey} AND relay_message_id = ${relayMessageId}
            AND state = 'Pending'
        `
        return false
      }
      yield* sql`
        UPDATE ${sql(table)}
        SET state = 'DeadLettered', terminal_at = ${options.now}
        WHERE inbox_key = ${inboxKey} AND relay_message_id = ${relayMessageId}
          AND state = 'Pending' AND deliveries >= ${options.maxDeliveries}
      `
      const after = yield* findExisting({ inboxKey, relayMessageId })
      return Option.isSome(after) &&
        after.value.state === "DeadLettered" &&
        after.value.terminal_at === options.now
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })
        )),
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        )),
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        ))
    )

  const settle = (
    inboxKey: string,
    relayMessageId: string,
    options: {
      readonly outcome: RelayInboxStore.TerminalOutcome
      readonly messageHash: string
      readonly now: number
      readonly terminalRetentionMillis: number
    }
  ) =>
    sql.withTransaction(Effect.gen(function*() {
      const current = yield* findExisting({ inboxKey, relayMessageId })
      if (Option.isNone(current)) return "NotPending" as const
      if (current.value.state !== "Pending") return "NotPending" as const
      // Checked against the stored row rather than the delivering fiber's memory, which is lost on
      // runner death and on defect restart.
      if (current.value.message_hash !== options.messageHash) return "HashMismatch" as const

      const horizon = options.now + options.terminalRetentionMillis
      // The horizon only ever grows. Shrinking it would reopen a deduplication window a sender may
      // still be replaying into.
      const greatest = greatestFn
      // Only `Acknowledged` and `Rejected` reach this statement and neither is ever revived, so this is
      // the one place a payload may be cleared. `DeadLettered` and `Expired` are replayed by `admit`.
      // What a replay is matched against - the hash, the digest, the horizon - is not the payload.
      yield* sql`
        UPDATE ${sql(table)}
        SET state = ${options.outcome}, terminal_at = ${options.now}, envelope = '',
            deduplicate_until = ${sql.literal(greatest)}(deduplicate_until, ${horizon})
        WHERE inbox_key = ${inboxKey} AND relay_message_id = ${relayMessageId}
          AND state = 'Pending' AND message_hash = ${options.messageHash}
      `
      // Re-read rather than trusting the earlier one. The read above takes no lock at the default
      // isolation of PostgreSQL and MySQL, so a sweeper could have moved the row out of `Pending`
      // in between; reporting success then would tell the recipient an acknowledgement is durable
      // when it never applied.
      const after = yield* findExisting({ inboxKey, relayMessageId })
      if (Option.isNone(after)) return "NotPending" as const
      return after.value.state === options.outcome && after.value.terminal_at === options.now
        ? "Settled" as const
        : "NotPending" as const
    })).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })
        )),
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })
        ))
    )

  const findRetained = SqlSchema.findOne({
    Request: Schema.String,
    Result: RetainedRow,
    execute: (inboxKey) =>
      sql`
        SELECT COUNT(*) AS retained_count FROM ${sql(table)}
        WHERE inbox_key = ${inboxKey} AND state <> 'Pending'
      `
  })

  const usage = (inboxKey: string) =>
    Effect.all([findUsage(inboxKey), findRetained(inboxKey)]).pipe(
      // A `SchemaError` here is a row this store cannot decode, which is corruption rather than
      // downtime. Collapsing the two would have callers retry a row that will never decode. An
      // aggregate returning no row at all is likewise not something a retry fixes.
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))),
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) }))),
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) }))),
      Effect.map(([pending, retained]) => ({
        pendingCount: pending.pending_count,
        pendingBytes: pending.pending_bytes,
        retainedCount: retained.retained_count
      }))
    )

  const findAbandoned = SqlSchema.findAll({
    Request: Schema.Struct({ inboxKey: Schema.String, limit: Schema.Int }),
    Result: AbandonedRow,
    execute: (request) =>
      sql`
        SELECT relay_message_id, state, deliveries, terminal_at FROM ${sql(table)}
        WHERE inbox_key = ${request.inboxKey} AND state IN ('DeadLettered', 'Expired')
        ORDER BY terminal_at DESC
        LIMIT ${sql.literal(String(request.limit))}
      `
  })

  const abandoned = (inboxKey: string, options: { readonly limit: number }) =>
    findAbandoned({ inboxKey, limit: options.limit }).pipe(
      // A `SchemaError` here is a row this store cannot decode, which is corruption rather than
      // downtime. Collapsing the two would have callers retry a row that will never decode.
      Effect.catchTag(
        "SqlError",
        (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))
      ),
      Effect.catchTag(
        "SchemaError",
        (cause) => Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) }))
      ),
      Effect.flatMap(Effect.forEach((row) =>
        Schema.decodeUnknownEffect(RelayInboxStore.InboxState)(row.state).pipe(
          Effect.mapError(() =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: `unknown inbox state ${row.state}` })
            })
          ),
          Effect.map((state) => ({
            relayMessageId: row.relay_message_id,
            state,
            deliveries: row.deliveries,
            terminalAt: row.terminal_at ?? 0
          }))
        )
      ))
    )

  const selectExpired = SqlSchema.findAll({
    Request: Schema.Struct({ now: Schema.Number, limit: Schema.Int }),
    Result: IdRow,
    execute: (request) =>
      sql`
        SELECT inbox_key, relay_message_id FROM ${sql(table)}
        WHERE state = 'Pending' AND expires_at <= ${request.now}
        LIMIT ${sql.literal(String(request.limit))}
      `
  })

  // `LIMIT` is not portable inside `UPDATE`/`DELETE`, so the batch is chosen by key first.
  const expire = (
    options: {
      readonly now: number
      readonly limit: number
      readonly terminalRetentionMillis: number
    }
  ) =>
    Effect.gen(function*() {
      const rows = yield* selectExpired({ now: options.now, limit: options.limit }).pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))),
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })))
      )
      if (rows.length === 0) {
        return 0
      }
      // Re-applies every predicate from the selection. Filtering on `relay_message_id` alone would
      // reach rows in other inboxes, because the key is `(inbox_key, relay_message_id)` and the
      // same identity can legitimately be addressed to more than one device.
      yield* sql`
        UPDATE ${sql(table)}
        SET state = 'Expired', terminal_at = ${options.now},
            deduplicate_until = ${sql.literal(greatestFn)}(
              deduplicate_until, ${options.now + options.terminalRetentionMillis}
            )
        WHERE state = 'Pending' AND expires_at <= ${options.now}
          AND ${
        sql.or(rows.map((row) =>
          sql`(inbox_key = ${row.inbox_key} AND relay_message_id = ${row.relay_message_id})`
        ))
      }
      `.pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })))
      )
      yield* Effect.logWarning("Relay inbox expired undelivered messages").pipe(
        Effect.annotateLogs({ expired: rows.length })
      )
      return rows.length
    })

  const selectCollectable = SqlSchema.findAll({
    Request: Schema.Struct({ now: Schema.Number, limit: Schema.Int }),
    Result: IdRow,
    execute: (request) =>
      sql`
        SELECT inbox_key, relay_message_id FROM ${sql(table)}
        WHERE state <> 'Pending' AND deduplicate_until <= ${request.now}
        LIMIT ${sql.literal(String(request.limit))}
      `
  })

  const collect = (options: { readonly now: number; readonly limit: number }) =>
    Effect.gen(function*() {
      const rows = yield* selectCollectable({ now: options.now, limit: options.limit }).pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) }))),
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageCorrupt({ cause }) })))
      )
      if (rows.length === 0) {
        return 0
      }
      yield* sql`
        DELETE FROM ${sql(table)}
        WHERE state <> 'Pending' AND deduplicate_until <= ${options.now}
          AND ${
        sql.or(rows.map((row) =>
          sql`(inbox_key = ${row.inbox_key} AND relay_message_id = ${row.relay_message_id})`
        ))
      }
      `.pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(new ReplicaError.ReplicaError({ reason: new ReplicaError.StorageUnavailable({ cause }) })))
      )
      return rows.length
    })

  // Spanned at the boundary rather than inside each body, so every database round trip is visible
  // with a stable name. `inbox_key` is a digest and safe to record; the envelope and payload are
  // never attributes.
  return RelayInboxStore.RelayInboxStore.of({
    admit: (request) =>
      admit(request).pipe(
        Effect.withSpan("SqlRelayInboxStore.admit", {
          attributes: { inbox_key: request.inboxKey, relay_message_id: request.envelope.relayMessageId }
        })
      ),
    pendingHeads: (inboxKey, options) =>
      pendingHeads(inboxKey, options).pipe(
        Effect.withSpan("SqlRelayInboxStore.pendingHeads", {
          attributes: { inbox_key: inboxKey, limit: options.limit }
        })
      ),
    recordDelivery: (inboxKey, relayMessageId, options) =>
      recordDelivery(inboxKey, relayMessageId, options).pipe(
        Effect.withSpan("SqlRelayInboxStore.recordDelivery", {
          attributes: { inbox_key: inboxKey, relay_message_id: relayMessageId, max_deliveries: options.maxDeliveries }
        })
      ),
    settle: (inboxKey, relayMessageId, options) =>
      settle(inboxKey, relayMessageId, options).pipe(
        Effect.withSpan("SqlRelayInboxStore.settle", {
          attributes: { inbox_key: inboxKey, relay_message_id: relayMessageId, outcome: options.outcome }
        })
      ),
    deadLetterExhausted: (inboxKey, relayMessageId, options) =>
      deadLetterExhausted(inboxKey, relayMessageId, options).pipe(
        Effect.withSpan("SqlRelayInboxStore.deadLetterExhausted", {
          attributes: { inbox_key: inboxKey, relay_message_id: relayMessageId, max_deliveries: options.maxDeliveries }
        })
      ),
    usage: (inboxKey) =>
      usage(inboxKey).pipe(
        Effect.withSpan("SqlRelayInboxStore.usage", { attributes: { inbox_key: inboxKey } })
      ),
    abandoned: (inboxKey, options) =>
      abandoned(inboxKey, options).pipe(
        Effect.withSpan("SqlRelayInboxStore.abandoned", {
          attributes: { inbox_key: inboxKey, limit: options.limit }
        })
      ),
    expire: (options) =>
      expire(options).pipe(
        Effect.withSpan("SqlRelayInboxStore.expire", { attributes: { limit: options.limit } })
      ),
    collect: (options) =>
      collect(options).pipe(
        Effect.withSpan("SqlRelayInboxStore.collect", { attributes: { limit: options.limit } })
      )
  })
}) satisfies Effect.Effect<
  RelayInboxStore.RelayInboxStore["Service"],
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto
>

export const layer: Layer.Layer<
  RelayInboxStore.RelayInboxStore,
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto
> = Layer.effect(RelayInboxStore.RelayInboxStore)(make)
