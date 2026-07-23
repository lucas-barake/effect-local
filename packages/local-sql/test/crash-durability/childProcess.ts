import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as DocumentStore from "../../src/DocumentStore.js"
import * as InternalAutomerge from "../../src/internal/automerge.js"
import { AckLineJson, acknowledgedTitle, ChildMode, ReadLineJson, storeLayer, Task } from "./fixture.js"

const [modeArg, dbPath, documentIdArg] = process.argv.slice(2)
const mode = Schema.decodeUnknownSync(ChildMode)(modeArg)

const journalMode = SqlClient.SqlClient.pipe(
  Effect.flatMap((sql) =>
    SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ journal_mode: Schema.String }),
      execute: () => sql`PRAGMA journal_mode`
    })(undefined)
  ),
  Effect.map((row) => row.journal_mode),
  Effect.orDie
)

const write = Effect.gen(function*() {
  const store = yield* DocumentStore.DocumentStore
  const documentId = yield* Identity.makeDocumentId
  const stored = yield* store.create(Task, documentId, { title: acknowledgedTitle })
  InternalAutomerge.free(stored.automerge)
  const line = Schema.encodeSync(AckLineJson)({
    kind: "ack",
    journalMode: yield* journalMode,
    documentId,
    title: acknowledgedTitle
  })
  yield* Effect.sync(() => process.stdout.write(line + "\n"))
  // Hold the process open with a referenced timer so the WAL is never
  // checkpointed by a clean shutdown; the parent ends it with SIGKILL.
  yield* Effect.callback<never>(() => {
    const timer = setInterval(() => {}, 2_147_483_647)
    return Effect.sync(() => clearInterval(timer))
  })
})

const read = Effect.gen(function*() {
  const store = yield* DocumentStore.DocumentStore
  const documentId = Schema.decodeUnknownSync(Identity.DocumentId)(documentIdArg)
  const result = yield* store.load(Task, documentId).pipe(
    Effect.map((stored) => {
      InternalAutomerge.free(stored.automerge)
      return { kind: "read" as const, found: true, title: stored.snapshot.value.title }
    }),
    Effect.catchReason(
      "ReplicaError",
      "DocumentNotFound",
      () => Effect.succeed({ kind: "read" as const, found: false, title: null })
    )
  )
  const line = Schema.encodeSync(ReadLineJson)(result)
  yield* Effect.sync(() => process.stdout.write(line + "\n"))
})

const program = (mode === "write" ? write : read).pipe(
  Effect.scoped,
  Effect.provide(storeLayer(dbPath))
)

Effect.runPromiseExit(program).then((exit) => {
  if (Exit.isFailure(exit)) {
    process.stderr.write(Cause.pretty(exit.cause) + "\n")
    process.exit(1)
  }
  process.exit(0)
})
