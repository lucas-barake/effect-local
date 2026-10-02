import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import * as Stream from "effect/Stream"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000e01")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000e01")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000e02")

const PairKey = Schema.Record(Schema.String, Schema.String)
const Pair = Model.make("Pair", {
  version: 1,
  key: PairKey,
  schema: Schema.Struct({ label: Schema.String })
})
const PutPair = Mutation.make("PutPair", { version: 1, payload: { key: PairKey, label: Schema.String } })
const definition = Definition.make({ version: 1, models: [Pair], mutations: [PutPair] })
const layerHandlers = PutPair.toLayer(({ payload, transaction }) =>
  transaction.set(Pair, payload.key, { label: payload.label })
)
const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))

const forward = { a: "x", b: "y" }
const reversed = { b: "y", a: "x" }

const layerDatabase = () =>
  Layer.mergeAll(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)

const layerServer = ServerStore.layerTrusted({
  definition,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(Layer.provide(layerRuntime), Layer.provideMerge(layerDatabase()))

const replicaSpace = (server: ServerStore.Service, clientId: Identity.ClientId) =>
  SqlReplica.layer({ definition, clientId, initialSpaces: [spaceId], retryDelay: "10 millis" }).pipe(
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, {
      waitForCredentialChange: () => Effect.never,
      transportGeneration: Effect.succeed(0),
      waitForTransportChange: () => Effect.never,
      submitBatch: (request) => server.admitBatch(request, null),
      discard: (request) => server.discard(request, null),
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })),
    Layer.provide(layerHandlers),
    Layer.provide(layerDatabase()),
    Layer.provide(Reactivity.layer),
    Layer.build,
    Effect.map(Context.get(Replica.Replica)),
    Effect.flatMap((replica) => replica.space(spaceId))
  )

const settled = (space: Replica.Space, count: number) =>
  space.settlements({ from: 0 }).pipe(Stream.take(count), Stream.runDrain)

const labelOf = (space: Replica.Space, key: typeof PairKey.Type) =>
  space.get(Pair, key).pipe(Effect.map(Option.map((value) => value.label)))

describe("object entity keys", () => {
  it.live(
    "treat the same object key written in either property order as one entity everywhere",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const context = yield* Layer.build(layerServer)
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const writer = yield* replicaSpace(server, writerId)

        yield* writer.mutate(PutPair, { key: forward, label: "first" })
        yield* writer.mutate(PutPair, { key: reversed, label: "second" })
        assert.deepStrictEqual(yield* labelOf(writer, forward), Option.some("second"))
        yield* settled(writer, 2)

        const stored = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_entities WHERE model = 'Pair'`
        assert.strictEqual(stored[0]?.count, 1)

        const reader = yield* replicaSpace(server, readerId)
        const seen = yield* Effect.all([labelOf(reader, forward), labelOf(reader, reversed)]).pipe(
          Effect.repeat({ until: ([a, b]) => Option.isSome(a) && Option.isSome(b) })
        )
        assert.deepStrictEqual(seen, [Option.some("second"), Option.some("second")])

        yield* writer.mutate(PutPair, { key: forward, label: "third" })
        yield* settled(writer, 3)
        const updated = yield* Effect.all([labelOf(reader, forward), labelOf(reader, reversed)]).pipe(
          Effect.repeat({
            until: ([a, b]) => Option.getOrUndefined(a) === "third" && Option.getOrUndefined(b) === "third"
          })
        )
        assert.deepStrictEqual(updated, [Option.some("third"), Option.some("third")])
      })),
    30_000
  )
})
