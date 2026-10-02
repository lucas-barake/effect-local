import { NodeCrypto, NodeServices } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import * as Stream from "effect/Stream"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import { type ManualDatabase, manualDatabases } from "./fixtures/ManualMigration.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000c01")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000c01")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const NoteSchema = Schema.Struct({ id: Schema.String, rank: Schema.Number })
const rankIndex = {
  version: 1,
  partition: [],
  sort: [{
    name: "rank",
    affinity: "real",
    schema: Schema.Number,
    extract: (note: typeof NoteSchema.Type) => note.rank
  }]
} as const

const rankedNote = (name: string) =>
  Model.make(name, { version: 1, key: Schema.String, schema: NoteSchema, indexes: { byRank: rankIndex } })
const RankedNote = rankedNote("Note")
const PlainNote = Model.make("Note", { version: 1, key: Schema.String, schema: NoteSchema })
const PutNote = Mutation.make("PutNote", { version: 1, payload: NoteSchema, success: NoteSchema })

const definitionOf = (model: Model.Any) => Definition.make({ version: 1, models: [model], mutations: [PutNote] })
const ranked = definitionOf(RankedNote)
const plain = definitionOf(PlainNote)

const layerHandlers = PutNote.toLayer(({ payload, transaction }) =>
  transaction.set(RankedNote, payload.id, payload).pipe(Effect.as(payload))
)

const layerStore = (
  database: ManualDatabase,
  definition: Definition.Any,
  mode: "apply" | "verify"
) =>
  ServerStore.layerTrusted({
    definition,
    readAuthorizationRefreshInterval: "30 seconds",
    maximumWatchersPerSpace: 1_024,
    maximumConcurrentReadAuthorizations: 64,
    maximumPendingReadAuthorizations: 4_096,
    readAuthorizationCacheCapacity: 4_096,
    retainedHistoryEntries: 256,
    maximumHistoryEntries: 10_000,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    maximumSnapshotEntities: 10_000,
    maximumSnapshotBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
    pruneBatchSize: 1_000,
    retainedSnapshots: 2,
    maintenanceConcurrency: 1,
    maintenanceSpaceBatchSize: 128,
    migration: { ...migration, mode }
  }).pipe(
    Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(database.layer())
  )

const boot = (database: ManualDatabase, definition: Definition.Any, mode: "apply" | "verify") =>
  layerStore(database, definition, mode).pipe(Layer.build, Effect.scoped, Effect.exit)

const failureOf = <A,>(exit: Exit.Exit<A, { readonly _tag: string; readonly catalog?: string }>) => {
  if (Exit.isSuccess(exit)) return "Success"
  const error = Option.getOrUndefined(Exit.findErrorOption(exit))
  if (error === undefined) return "Defect"
  if (error.catalog === undefined) return error._tag
  return `${error._tag}(${error.catalog})`
}

const render = (database: ManualDatabase, definition: Definition.Any) =>
  Migrations.renderServer({ definition }).pipe(Effect.provide(database.layer()))

const scriptOf = Effect.fnUntraced(function*(database: ManualDatabase, definition: Definition.Any) {
  const script = yield* render(database, definition)
  if (Option.isNone(script)) return assert.fail("expected a pending script")
  return script.value
})

const renderAndApply = Effect.fnUntraced(function*(database: ManualDatabase, definition: Definition.Any) {
  const script = yield* scriptOf(database, definition)
  yield* database.apply(script)
  return script
})

const introspection = {
  sqlite: [
    `SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`
  ],
  pg: [
    `SELECT table_name, column_name, data_type, collation_name, is_nullable, column_default
      FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, column_name`,
    `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname`,
    `SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint
      WHERE connamespace = 'public'::regnamespace ORDER BY conname`,
    `SELECT tgname, pg_get_triggerdef(oid) AS definition FROM pg_trigger WHERE NOT tgisinternal ORDER BY tgname`,
    `SELECT proname, pg_get_functiondef(oid) AS definition FROM pg_proc
      WHERE pronamespace = 'public'::regnamespace ORDER BY proname`,
    `SELECT viewname, definition FROM pg_views WHERE schemaname = 'public' ORDER BY viewname`
  ]
} as const

const bookkeeping = [
  `SELECT id, name, checksum FROM effect_local_server_migrations ORDER BY id`,
  `SELECT model, index_name, descriptor_hash, table_name, scan_index_name
    FROM effect_local_server_index_catalog ORDER BY descriptor_hash`,
  `SELECT space_id, schema_generation, descriptor_hash, built FROM effect_local_server_index_state
    ORDER BY space_id, schema_generation, descriptor_hash`
]

const schemaOf = (database: ManualDatabase) =>
  SqlClient.SqlClient.use((sql) =>
    Effect.forEach(introspection[database.dialect], (statement) => sql.unsafe(statement))
  ).pipe(Effect.provide(database.layer()))

const bookkeepingOf = (database: ManualDatabase) =>
  SqlClient.SqlClient.use((sql) => Effect.forEach(bookkeeping, (statement) => sql.unsafe(statement))).pipe(
    Effect.provide(database.layer())
  )

const stateOf = (database: ManualDatabase) => Effect.all([schemaOf(database), bookkeepingOf(database)])

const indexTablesOf = (database: ManualDatabase) =>
  schemaOf(database).pipe(
    Effect.map((results) =>
      results.flat().flatMap((row) =>
        Object.values(row).filter((value) =>
          typeof value === "string" && /^effect_local_srvidx_[0-9a-f]{16}$/.test(value)
        )
      )
    ),
    Effect.map((names) => [...new Set(names)].toSorted((left, right) => left.localeCompare(right)))
  )

const layerClientDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerReplica = (database: ManualDatabase) => {
  const layerSync = Effect.gen(function*() {
    const store = yield* ServerStore.ServerStore
    return SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      transportGeneration: Effect.succeed(0),
      waitForTransportChange: () => Effect.never,
      submitBatch: (request) => store.admitBatch(request, null),
      discard: (request) => store.discard(request, null),
      pull: store.pull,
      bootstrap: store.bootstrap,
      watch: store.watch
    })
  }).pipe(Layer.effect(SyncEngine.SyncEngine), Layer.provide(layerStore(database, ranked, "verify")))
  return SqlReplica.layer({ definition: ranked, clientId, initialSpaces: [spaceId], retryDelay: "10 millis" }).pipe(
    Layer.provide(layerSync),
    Layer.provide(layerHandlers),
    Layer.provide(layerClientDatabase),
    Layer.provide(Reactivity.layer)
  )
}

const provideServices = Effect.provide([Reactivity.layer, NodeServices.layer])

describe.each(manualDatabases)("server migrations applied by hand on $dialect", ({ make }) => {
  it.effect(
    "a database built only from the rendered script serves in verify mode",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        yield* renderAndApply(database, ranked)
        const settled = yield* Effect.gen(function*() {
          const space = yield* Replica.Replica.use((replica) => replica.space(spaceId))
          yield* space.mutate(PutNote, { id: "note-1", rank: 1 })
          return yield* space.settlements({ from: 0 }).pipe(Stream.runHead)
        }).pipe(Effect.provide(layerReplica(database)))
        assert.strictEqual(Option.getOrUndefined(settled)?.settlement.receipt._tag, "Accepted")
        const stored = yield* SqlClient.SqlClient.use((sql) =>
          sql`SELECT entity_key FROM effect_local_server_entities WHERE model = 'Note'`
        ).pipe(Effect.provide(database.layer()))
        assert.deepStrictEqual(stored, [{ entity_key: "\"note-1\"" }])
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "the rendered script produces the same schema and bookkeeping as automatic migration",
    Effect.fnUntraced(
      function*() {
        const manual = yield* make
        const automatic = yield* make
        yield* renderAndApply(manual, ranked)
        assert.strictEqual(failureOf(yield* boot(automatic, ranked, "apply")), "Success")
        assert.deepStrictEqual(yield* schemaOf(manual), yield* schemaOf(automatic))
        assert.deepStrictEqual(yield* bookkeepingOf(manual), yield* bookkeepingOf(automatic))
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "verify refuses an empty database and leaves it untouched",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        const before = yield* schemaOf(database)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "StorageMigrationPending(Server)")
        assert.deepStrictEqual(yield* schemaOf(database), before)
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "verify refuses a database without the index tables its definition needs",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        assert.strictEqual(failureOf(yield* boot(database, plain, "apply")), "Success")
        const before = yield* stateOf(database)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "StorageMigrationPending(ServerIndex)")
        assert.deepStrictEqual(yield* stateOf(database), before)
        assert.isTrue(Option.isSome(yield* render(database, ranked)))
        assert.deepStrictEqual(yield* stateOf(database), before)
        yield* renderAndApply(database, ranked)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "verify rejects a migration ledger whose checksum differs from the catalog",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        yield* renderAndApply(database, ranked)
        yield* SqlClient.SqlClient.use((sql) =>
          sql`UPDATE effect_local_server_migrations SET checksum = '0000000000000000' WHERE id = 1`
        ).pipe(Effect.provide(database.layer()))
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "StorageMigrationMismatch(Server)")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "verify refuses index tables the definition no longer declares until the rendered script drops them",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        assert.strictEqual(failureOf(yield* boot(database, ranked, "apply")), "Success")
        assert.strictEqual((yield* indexTablesOf(database)).length, 1)
        const before = yield* stateOf(database)
        assert.strictEqual(failureOf(yield* boot(database, plain, "verify")), "StorageMigrationPending(ServerIndex)")
        assert.deepStrictEqual(yield* stateOf(database), before)
        yield* renderAndApply(database, plain)
        assert.strictEqual(failureOf(yield* boot(database, plain, "verify")), "Success")
        assert.deepStrictEqual(yield* indexTablesOf(database), [])
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "a rendered script applied twice is rejected",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        const script = yield* renderAndApply(database, ranked)
        assert.strictEqual(failureOf(yield* database.apply(script).pipe(Effect.exit)), "ScriptRejected")
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "an index script replayed after a later deploy changed the indexes again is rejected",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        assert.strictEqual(failureOf(yield* boot(database, ranked, "apply")), "Success")
        const dropIndexes = yield* renderAndApply(database, plain)
        yield* renderAndApply(database, ranked)
        const before = yield* stateOf(database)
        assert.strictEqual(failureOf(yield* database.apply(dropIndexes).pipe(Effect.exit)), "ScriptRejected")
        assert.deepStrictEqual(yield* stateOf(database), before)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "an index script rendered before another script changed the indexes is rejected",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        assert.strictEqual(failureOf(yield* boot(database, ranked, "apply")), "Success")
        const first = yield* scriptOf(database, plain)
        const stale = yield* scriptOf(database, plain)
        yield* database.apply(first)
        yield* renderAndApply(database, ranked)
        const before = yield* stateOf(database)
        assert.strictEqual(failureOf(yield* database.apply(stale).pipe(Effect.exit)), "ScriptRejected")
        assert.deepStrictEqual(yield* stateOf(database), before)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "an index script rendered before automatic migration changed the indexes is rejected",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        assert.strictEqual(failureOf(yield* boot(database, ranked, "apply")), "Success")
        const stale = yield* scriptOf(database, plain)
        assert.strictEqual(failureOf(yield* boot(database, plain, "apply")), "Success")
        assert.strictEqual(failureOf(yield* boot(database, ranked, "apply")), "Success")
        const before = yield* stateOf(database)
        assert.strictEqual(failureOf(yield* database.apply(stale).pipe(Effect.exit)), "ScriptRejected")
        assert.deepStrictEqual(yield* stateOf(database), before)
        assert.strictEqual(failureOf(yield* boot(database, ranked, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "a rendered script that fails partway leaves the database unchanged",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        const script = yield* scriptOf(database, ranked)
        yield* SqlClient.SqlClient.use((sql) => sql`CREATE TABLE effect_local_server_index_catalog (squatter TEXT)`)
          .pipe(Effect.provide(database.layer()))
        const before = yield* schemaOf(database)
        assert.strictEqual(failureOf(yield* database.apply(script).pipe(Effect.exit)), "ScriptRejected")
        assert.deepStrictEqual(yield* schemaOf(database), before)
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "rendering a current database returns none and rendering never writes",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        const before = yield* schemaOf(database)
        assert.isTrue(Option.isSome(yield* render(database, ranked)))
        assert.deepStrictEqual(yield* schemaOf(database), before)
        yield* renderAndApply(database, ranked)
        assert.isTrue(Option.isNone(yield* render(database, ranked)))
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "verify rejects retry options it could not use",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        yield* renderAndApply(database, ranked)
        const outcome = yield* Migrations.server({ mode: "verify", maximumAttempts: 0 }).pipe(
          Effect.provide(database.layer()),
          Effect.exit
        )
        assert.strictEqual(failureOf(outcome), "InvalidConfiguration")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "rendering rejects a model name SQL text cannot carry",
    Effect.fnUntraced(
      function*() {
        const database = yield* make
        const definition = definitionOf(rankedNote("Note\u0000"))
        const outcome = yield* render(database, definition).pipe(Effect.exit)
        assert.strictEqual(failureOf(outcome), "InvalidConfiguration")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )
})

describe("server migrations applied by hand through psql", () => {
  it.effect(
    "keep non ASCII names intact when the whole script is sent as one message in another client encoding",
    Effect.fnUntraced(
      function*() {
        const database = yield* manualDatabases[1].make
        const definition = definitionOf(rankedNote("No'té\\ \u{1F600}"))
        const script = yield* scriptOf(database, definition)
        yield* database.applyAsOneMessage(script, ["PGCLIENTENCODING=LATIN1"])
        assert.strictEqual(failureOf(yield* boot(database, definition, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "keep backslashes intact when the whole script is sent as one message without standard conforming strings",
    Effect.fnUntraced(
      function*() {
        const database = yield* manualDatabases[1].make
        const definition = definitionOf(rankedNote("Back\\slash\\n"))
        const script = yield* scriptOf(database, definition)
        yield* database.applyAsOneMessage(script, ["PGOPTIONS=-c standard_conforming_strings=off"])
        assert.strictEqual(failureOf(yield* boot(database, definition, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )

  it.effect(
    "keep non ASCII names intact when the session uses another client encoding",
    Effect.fnUntraced(
      function*() {
        const database = yield* manualDatabases[1].make
        const definition = definitionOf(rankedNote("Noté"))
        const script = yield* scriptOf(database, definition)
        yield* database.apply(`SET client_encoding = 'LATIN1';\n${script}`)
        assert.strictEqual(failureOf(yield* boot(database, definition, "verify")), "Success")
      },
      Effect.scoped,
      provideServices
    ),
    60_000
  )
})
