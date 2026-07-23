import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as DocumentStore from "../../src/DocumentStore.js"
import * as ReplicaBootstrap from "../../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../../src/ReplicaGate.js"

export const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

const definition = ReplicaDefinition.make({
  name: "tasks",
  documents: DocumentSet.make(Task),
  mutations: [],
  projections: [],
  queries: []
})

export const acknowledgedTitle = "survives-sigkill"

/**
 * On-disk SQLite with WAL enabled (the driver runs `PRAGMA journal_mode = WAL`
 * whenever `disableWAL` is not `true`), composing the same DocumentStore stack
 * production uses so a write acknowledged here is acknowledged the same way a
 * consumer would observe it.
 */
export const storeLayer = (filename: string) => {
  const database = Layer.merge(SqliteClient.layer({ filename }), NodeCrypto.layer)
  const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
  const base = Layer.merge(database, bootstrap)
  const gate = ReplicaGate.layer.pipe(Layer.provide(base))
  const store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(base, gate)))
  return Layer.merge(base, store)
}

export const ChildMode = Schema.Literals(["write", "read"])
export type ChildMode = typeof ChildMode.Type

const AckLine = Schema.Struct({
  kind: Schema.Literal("ack"),
  journalMode: Schema.String,
  documentId: Identity.DocumentId,
  title: Schema.String
})

const ReadLine = Schema.Struct({
  kind: Schema.Literal("read"),
  found: Schema.Boolean,
  title: Schema.NullOr(Schema.String)
})

export const AckLineJson = Schema.fromJsonString(AckLine)
export const ReadLineJson = Schema.fromJsonString(ReadLine)
