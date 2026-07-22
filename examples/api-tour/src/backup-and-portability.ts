import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { definition, EngineLive, RenameTask, Task } from "./domain.ts"

const program = Effect.scoped(Effect.gen(function*() {
  const sourceContext = yield* Layer.build(EngineLive)
  const targetContext = yield* Layer.build(EngineLive)
  const source = Context.get(sourceContext, Replica.Replica)
  const target = Context.get(targetContext, Replica.Replica)
  const created = yield* source.create(Task, {
    commandId: (yield* Identity.makeCommandId),
    value: { title: "Portable", completed: false, labels: ["backup"] }
  })
  const sourceDocumentId = yield* CommandOutcome.committedOrFail(created)

  const portable = yield* source.exportDocument(Task, sourceDocumentId)
  const decoded = yield* Document.decode(Task, sourceDocumentId, portable.value)
  const encoded = yield* Document.encode(Task, sourceDocumentId, decoded)
  const imported = yield* source.importDocument(Task, {
    commandId: (yield* Identity.makeCommandId),
    value: { ...portable, value: encoded }
  })
  const importedDocumentId = yield* CommandOutcome.committedOrFail(imported)
  const importedSnapshot = yield* source.get(Task, importedDocumentId)

  const archive = yield* source.exportBackup({ maxBytes: TestReplica.defaultLimits.maxBackupBytes }).pipe(
    Stream.runCollect
  )
  yield* target.restoreBackup({
    source: Stream.fromIterable(archive),
    mode: "clone",
    maxBytes: TestReplica.defaultLimits.maxBackupBytes,
    expectedDefinitionHash: definition.hash
  })

  yield* source.mutate(RenameTask, {
    commandId: (yield* Identity.makeCommandId),
    documentId: sourceDocumentId,
    payload: "Changed after backup"
  })
  yield* source.restoreBackup({
    source: Stream.fromIterable(archive),
    mode: "replace",
    maxBytes: TestReplica.defaultLimits.maxBackupBytes,
    expectedDefinitionHash: definition.hash
  })

  const restored = yield* source.get(Task, sourceDocumentId)
  const cloned = yield* target.get(Task, sourceDocumentId)
  yield* Effect.log({
    archiveChunks: archive.length,
    restored: restored.value,
    cloned: cloned.value,
    importedDocumentId,
    imported: importedSnapshot.value
  })
}))

await Effect.runPromise(program.pipe(Effect.provide(NodeCrypto.layer)))
import { NodeCrypto } from "@effect/platform-node"
