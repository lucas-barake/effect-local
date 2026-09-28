import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as OfflineWakeRuntime from "../src/internal/offlineWake.js"
import * as Rows from "../src/internal/rows.js"
import * as SqlTransaction from "../src/internal/transaction.js"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import type * as OfflineWake from "../src/OfflineWake.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { postgresDatabaseUrl } from "./fixtures/ServerDatabase.js"
import { awaitLockWaiters, gateStatements, lockWaiters, type Pause, type Phase } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000901")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000901")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000902")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000901")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const envelope = Effect.fnUntraced(function*(id: string, localSequence: number) {
  const identity = {
    spaceId,
    clientId: writerId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: Domain.PutTodo.name,
    payload: Domain.todo(id),
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: Domain.definition.schemaIdentity,
    mutationVersion: Domain.PutTodo.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

const submitTodo = (store: ServerStore.Service, id: string, localSequence: number) =>
  envelope(id, localSequence).pipe(Effect.provide(NodeCrypto.layer), Effect.flatMap(store.submit))

const pullRequest = Protocol.PullRequest.make({
  spaceId,
  clientId: readerId,
  schema: Domain.definition.schemaIdentity,
  scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
  cursor: null,
  limit: 100
})

const buildStore = (
  sql: SqlClient.SqlClient,
  authorizeRead: ServerStore.Options<SqlClient.SqlClient>["authorizeRead"] = () => Effect.void
) =>
  ServerStore.layer({
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provide(NodeCrypto.layer),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )

const HeadRow = Schema.Struct({ next_server_sequence: Rows.integer(Schema.Int) })

const offlineOptions = (
  recipients: OfflineWake.Options["recipients"],
  deliver: OfflineWake.Options["deliver"]
): OfflineWake.Options => ({
  recipients,
  deliver,
  coalescingWindow: "1 millis",
  pollInterval: "1 hour",
  retryDelay: "1 second",
  maximumRetryDelay: "1 minute",
  claimLeaseDuration: "30 seconds",
  hookTimeout: "10 seconds",
  presenceLeaseDuration: "30 seconds",
  presenceHeartbeatInterval: "20 seconds",
  claimBatchSize: 8,
  maximumConcurrentRecipientResolutions: 1,
  maximumConcurrentDeliveries: 1,
  maximumRecipientsPerSpace: 8
})

const makeRuntime = (sql: SqlClient.SqlClient, options: OfflineWake.Options, scope: Scope.Scope) =>
  Crypto.Crypto.pipe(
    Effect.flatMap((crypto) =>
      OfflineWakeRuntime.make(options, Context.empty()).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(Scope.Scope, scope)
      )
    )
  )

const afterPause = (pauses: Queue.Queue<Pause>) =>
  Queue.take(pauses).pipe(Effect.repeat({ until: (pause) => pause.phase === "after" }))

const releaseAfterPauses = Effect.forEach(
  (gate: { readonly pauses: Queue.Queue<Pause> }) =>
    afterPause(gate.pauses).pipe(Effect.tap((pause) => Deferred.succeed(pause.release, undefined))),
  { concurrency: "unbounded" }
)

const afterWhen = (matches: (statement: string) => boolean) => (statement: string): ReadonlyArray<Phase> => {
  if (matches(statement)) return ["after"]
  return []
}

const outcomeTag = <A, E extends { readonly _tag: string },>(outcome: Result.Result<A, E>) => {
  if (Result.isSuccess(outcome)) return "Built"
  return outcome.failure._tag
}

const provideReactivity = Effect.provide(Reactivity.layer)
const provideReactivityAndCrypto = Effect.provide([Reactivity.layer, NodeCrypto.layer])

const settleClaims = Effect.fnUntraced(function*(
  observer: SqlClient.SqlClient,
  gates: ReadonlyArray<{ readonly pauses: Queue.Queue<Pause> }>
) {
  while (true) {
    const paused = (yield* Effect.forEach(gates, (gate) => Queue.size(gate.pauses))).reduce((a, b) => a + b, 0)
    if (paused + (yield* lockWaiters(observer)) >= gates.length) return
  }
})

describe("postgres server concurrency", () => {
  it.effect(
    "a pull reads one server head while a concurrent submit waits for the space",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const observer = yield* PgClient.makeClient({ url })
        const pool = yield* PgClient.make({ url, maxConnections: 6 })
        const inside = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const heads = yield* Deferred.make<readonly [number, number]>()
        const readHead = SqlSchema.findOne({
          Request: Schema.Void,
          Result: HeadRow,
          execute: () => pool`SELECT next_server_sequence FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
        })(undefined).pipe(Effect.map((row) => row.next_server_sequence))
        const probeHeads = Effect.fnUntraced(
          function*() {
            const before = yield* readHead
            yield* Deferred.succeed(inside, undefined)
            yield* Deferred.await(release)
            const after = yield* readHead
            yield* Deferred.succeed(heads, [before, after] as const)
          },
          Effect.catchTags({
            SqlError: (error) => Effect.die(error),
            SchemaError: (error) => Effect.die(error),
            NoSuchElementError: (error) => Effect.die(error)
          })
        )
        const store = yield* buildStore(pool, (input) => {
          if (input._tag !== "Entity" || input.principal !== "gated") return Effect.void
          return probeHeads()
        })
        assert.strictEqual((yield* submitTodo(store, "gate", 1))._tag, "Accepted")

        const pull = yield* store.pullAuthorized(pullRequest, "gated").pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(inside)
        const late = yield* submitTodo(store, "late", 2).pipe(Effect.forkChild({ startImmediately: true }))
        yield* lockWaiters(observer).pipe(
          Effect.repeat({ until: (waiters) => waiters > 0 || late.pollUnsafe() !== undefined })
        )
        yield* Deferred.succeed(release, undefined)

        const pulled = yield* Fiber.join(pull)
        const [before, after] = yield* Deferred.await(heads)
        assert.strictEqual(after, before)
        if (!("_tag" in pulled)) assert.fail("expected a bootstrap manifest")
        assert.strictEqual(pulled.manifest.sequence, 1)
        assert.strictEqual((yield* Fiber.join(late))._tag, "Accepted")
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "snapshot preparation reads one consistent head while admission continues",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const pool = yield* PgClient.make({ url, maxConnections: 6 })
        const gate = yield* gateStatements(pool, (statement) => {
          if (
            statement.includes("FROM effect_local_server_entities WHERE space_id = ?") &&
            statement.includes("ORDER BY model, entity_key LIMIT")
          ) return ["before"]
          return []
        })
        const store = yield* buildStore(gate.sql)
        assert.strictEqual((yield* submitTodo(store, "first", 1))._tag, "Accepted")

        const maintained = yield* store.maintain(spaceId).pipe(
          Effect.result,
          Effect.forkChild({ startImmediately: true })
        )
        const paused = yield* Queue.take(gate.pauses)
        assert.strictEqual((yield* submitTodo(store, "second", 2))._tag, "Accepted")
        yield* Deferred.succeed(paused.release, undefined)
        yield* Queue.take(gate.pauses).pipe(
          Effect.flatMap((pause) => Deferred.succeed(pause.release, undefined)),
          Effect.forever,
          Effect.forkChild
        )

        const result = yield* Fiber.join(maintained)
        if (Result.isFailure(result)) assert.fail(result.failure._tag)
        yield* store.maintain(spaceId)
        const snapshot = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ snapshot_sequence: Rows.integer(Schema.Int) }),
          execute: () => pool`SELECT snapshot_sequence FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
        })(undefined)
        assert.strictEqual(snapshot.snapshot_sequence, 2)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "offline wake space claims are exclusive across runtimes",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const observer = yield* PgClient.makeClient({ url })
        const blocker = yield* PgClient.makeClient({ url })
        yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, observer))
        const recipients = yield* Ref.make(0)
        const options = offlineOptions(
          () => Ref.update(recipients, (count) => count + 1).pipe(Effect.as([])),
          () => Effect.succeed("Delivered" as const)
        )
        const isClaim = (statement: string) =>
          statement.includes("UPDATE effect_local_server_offline_wake_spaces") &&
          statement.includes("SET claim_token = ?")
        const scope = yield* Effect.scope
        const gates = yield* Effect.forEach([0, 1], () =>
          PgClient.make({ url, maxConnections: 3 }).pipe(
            Effect.flatMap((pool) => gateStatements(pool, afterWhen(isClaim)))
          ))
        const runtimes = yield* Effect.forEach(gates, (gate) => makeRuntime(gate.sql, options, scope))
        yield* runtimes[0].enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* TestClock.adjust("1 millis")

        yield* blocker`BEGIN`
        yield* blocker`SELECT space_id FROM effect_local_server_offline_wake_spaces
          WHERE space_id = ${spaceId} FOR UPDATE`
        yield* Effect.forEach(runtimes, (runtime) => runtime.notify, { discard: true })
        yield* settleClaims(observer, gates)
        yield* blocker`COMMIT`
        const contended = yield* releaseAfterPauses(gates)
        assert.isAtMost(contended.reduce((total, pause) => total + pause.rows.length, 0), 1)

        yield* Effect.forEach(runtimes, (runtime) => runtime.notify, { discard: true })
        const uncontended = yield* releaseAfterPauses(gates)
        const claims = [...contended, ...uncontended].reduce((total, pause) => total + pause.rows.length, 0)
        assert.strictEqual(claims, 1)
      },
      Effect.scoped,
      provideReactivityAndCrypto
    )
  )

  it.effect(
    "offline wake client claims are exclusive across runtimes",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const observer = yield* PgClient.makeClient({ url })
        const blocker = yield* PgClient.makeClient({ url })
        yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, observer))
        yield* observer`INSERT INTO effect_local_server_offline_wakes
          (space_id, client_id, wake_id, high_water_sequence, notified_sequence, membership_generation,
            attempt_count, next_attempt_at)
          VALUES (${spaceId}, ${readerId}, 'wak_00000000-0000-4000-8000-000000000901', 1, 0, 1, 0, 0)`
        const options = offlineOptions(
          () => Effect.succeed([]),
          () => Effect.succeed("Delivered" as const)
        )
        const isClaim = (statement: string) =>
          statement.includes("UPDATE effect_local_server_offline_wakes SET claim_token = ?")
        const scope = yield* Effect.scope
        const gates = yield* Effect.forEach([0, 1], () =>
          PgClient.make({ url, maxConnections: 3 }).pipe(
            Effect.flatMap((pool) => gateStatements(pool, afterWhen(isClaim)))
          ))
        const runtimes = yield* Effect.forEach(gates, (gate) => makeRuntime(gate.sql, options, scope))

        yield* blocker`BEGIN`
        yield* blocker`SELECT client_id FROM effect_local_server_offline_wakes
          WHERE space_id = ${spaceId} AND client_id = ${readerId} FOR UPDATE`
        yield* Effect.forEach(runtimes, (runtime) => runtime.notify, { discard: true })
        yield* settleClaims(observer, gates)
        yield* blocker`COMMIT`
        const claimed = yield* releaseAfterPauses(gates)
        assert.strictEqual(claimed.reduce((total, pause) => total + pause.rows.length, 0), 1)
      },
      Effect.scoped,
      provideReactivityAndCrypto
    )
  )

  it.effect(
    "retries a server transaction chosen as a deadlock victim",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const pool = yield* PgClient.make({ url, maxConnections: 4 })
        yield* pool`CREATE TABLE deadlock_probe (id TEXT PRIMARY KEY, value INTEGER NOT NULL)`
        yield* pool`INSERT INTO deadlock_probe (id, value) VALUES ('a', 0), ('b', 0)`
        const attempts = yield* Ref.make(0)
        const holding = yield* Effect.forEach([0, 1], () => Deferred.make<void>())
        const crossing = (
          first: string,
          second: string,
          held: Deferred.Deferred<void>,
          other: Deferred.Deferred<void>
        ) =>
          SqlTransaction.withServerTransaction(
            pool,
            Effect.gen(function*() {
              const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1)
              yield* pool`UPDATE deadlock_probe SET value = value + 1 WHERE id = ${first}`
              if (attempt <= 2) {
                yield* Deferred.succeed(held, undefined)
                yield* Deferred.await(other)
              }
              yield* pool`UPDATE deadlock_probe SET value = value + 1 WHERE id = ${second}`
            })
          )
        const outcomes = yield* Effect.all([
          crossing("a", "b", holding[0], holding[1]),
          crossing("b", "a", holding[1], holding[0])
        ], { concurrency: 2 })
        assert.strictEqual(outcomes.length, 2)
        assert.strictEqual(yield* Ref.get(attempts), 3)
        const totals = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Schema.Struct({ id: Schema.String, value: Schema.Number }),
          execute: () => pool`SELECT id, value FROM deadlock_probe ORDER BY id`
        })(undefined)
        assert.deepStrictEqual(totals, [{ id: "a", value: 2 }, { id: "b", value: 2 }])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "concurrent runners create server index tables once",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const observer = yield* PgClient.makeClient({ url })
        const blocker = yield* PgClient.makeClient({ url })
        yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, observer))
        yield* blocker`BEGIN`
        yield* blocker`LOCK TABLE pg_catalog.pg_class IN SHARE MODE`
        const builds = yield* Effect.forEach([0, 1], () =>
          PgClient.make({ url, maxConnections: 3 }).pipe(
            Effect.flatMap((pool) => buildStore(pool).pipe(Effect.result, Effect.forkChild({ startImmediately: true })))
          ))
        yield* awaitLockWaiters(observer, 2)
        yield* blocker`COMMIT`
        const outcomes = yield* Effect.forEach(builds, Fiber.join)
        assert.deepStrictEqual(
          outcomes.map(outcomeTag),
          ["Built", "Built"]
        )
      },
      Effect.scoped,
      provideReactivity
    )
  )

  const presenceRace = Effect.fnUntraced(function*(blockWakeRow: boolean) {
    const { url } = yield* postgresDatabaseUrl
    const observer = yield* PgClient.makeClient({ url })
    const blocker = yield* PgClient.makeClient({ url })
    yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, observer))
    yield* observer`INSERT INTO effect_local_server_offline_wakes
      (space_id, client_id, wake_id, high_water_sequence, notified_sequence, membership_generation,
        attempt_count, next_attempt_at)
      VALUES (${spaceId}, ${readerId}, 'wak_00000000-0000-4000-8000-000000000902', 1, 0, 1, 0, 0)`
    const options = offlineOptions(
      () => Effect.succeed([]),
      () => Effect.never
    )
    const scope = yield* Effect.scope
    const claiming = yield* PgClient.make({ url, maxConnections: 3 }).pipe(
      Effect.flatMap((pool) =>
        gateStatements(pool, (statement) => {
          if (statement.includes("UPDATE effect_local_server_offline_wakes SET claim_token = ?")) return ["after"]
          return []
        })
      )
    )
    const watching = yield* PgClient.make({ url, maxConnections: 3 }).pipe(
      Effect.flatMap((pool) =>
        gateStatements(pool, (statement) => {
          if (
            statement.includes("INSERT INTO effect_local_server_watch_presence") &&
            statement.includes("WHERE NOT EXISTS")
          ) return ["after"]
          return []
        })
      )
    )
    const claimingRuntime = yield* makeRuntime(claiming.sql, options, scope)
    const watchingRuntime = yield* makeRuntime(watching.sql, options, scope)

    if (blockWakeRow) {
      yield* blocker`BEGIN`
      yield* blocker`SELECT client_id FROM effect_local_server_offline_wakes
        WHERE space_id = ${spaceId} AND client_id = ${readerId} FOR UPDATE`
    }
    yield* claimingRuntime.notify
    if (!blockWakeRow) {
      yield* Queue.take(claiming.pauses).pipe(Effect.flatMap((pause) => Queue.offer(claiming.pauses, pause)))
    }
    const registration = yield* watchingRuntime.registerWatch(spaceId, readerId).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.forkChild({ startImmediately: true })
    )
    yield* settleClaims(observer, [claiming, watching])
    if (blockWakeRow) yield* blocker`COMMIT`
    const [claim, presence] = yield* releaseAfterPauses([claiming, watching])
    assert.isFalse(
      claim.rows.length > 0 && presence.rows.length > 0,
      "a delivery claim and a watch presence for the same client both committed"
    )
    yield* Fiber.interrupt(registration)
  })

  it.effect(
    "a watch registration waits for an in-flight delivery claim of the same client",
    () => presenceRace(false).pipe(Effect.scoped, provideReactivityAndCrypto)
  )

  it.effect(
    "a delivery claim blocked behind a row lock does not commit alongside a new watch presence",
    () => presenceRace(true).pipe(Effect.scoped, provideReactivityAndCrypto)
  )
})
