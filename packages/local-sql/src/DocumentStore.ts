import type * as Automerge from "@automerge/automerge"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Snapshot from "@lucas-barake/effect-local/Snapshot"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as InternalAutomerge from "./internal/automerge.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface Stored<D extends Document.Any,> {
  readonly automerge: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
  readonly encoded: D["schema"]["Encoded"]
  readonly snapshot: Snapshot.FromDocument<D>
  readonly materializedHeads: ReadonlyArray<string>
  readonly acceptedHeads: ReadonlyArray<string>
  readonly commitSequence: Identity.CommitSequence
}

export class DocumentStore extends Context.Service<DocumentStore, {
  readonly create: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    value: D["schema"]["Type"]
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
  readonly load: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
  readonly stage: <D extends Document.Any,>(
    stored: Stored<D>,
    change: (draft: Mutation.Draft<D>) => void
  ) => Effect.Effect<Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>, unknown>
  readonly tombstone: <D extends Document.Any,>(
    stored: Stored<D>
  ) => Effect.Effect<Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>, unknown>
  readonly persist: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    durable: Stored<D>,
    staged: Automerge.Doc<InternalAutomerge.Root<D["schema"]["Encoded"]>>
  ) => Effect.Effect<Stored<D>, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/DocumentStore") {}

export const layer: Layer.Layer<
  DocumentStore,
  never,
  SqlClient.SqlClient | ReplicaGate.ReplicaGate
> = Layer.effect(
  DocumentStore,
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const gate = yield* ReplicaGate.ReplicaGate
    const recovery = yield* Recovery.make

    const nextSequence = sql<{ readonly commit_sequence: number }>`
      UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
      WHERE singleton = 1 RETURNING commit_sequence
    `.pipe(Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.die(new Error("Replica metadata was not initialized"))
        : Effect.succeed(Identity.CommitSequence.make(rows[0].commit_sequence))
    ))

    return DocumentStore.of({
      create: (document, documentId, value) =>
        sql.withTransaction(Effect.gen(function*() {
          const encoded = yield* Document.encode(document, documentId, value)
          if (!Document.isAutomergeValue(encoded)) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "DocumentDecodeError",
                documentId,
                cause: { _tag: "SchemaCause", message: "Encoded value is not Automerge compatible", path: [] }
              }
            })
          }
          const epoch = yield* gate.current
          const actor = InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, documentId)
          const automerge = yield* Effect.try({
            try: () => InternalAutomerge.initialize(encoded, actor),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "AutomergeCause", message: String(cause) }
                }
              })
          })
          const heads = InternalAutomerge.heads(automerge)
          const sequence = yield* nextSequence
          yield* sql`INSERT INTO effect_local_documents (
            document_id, document_type, schema_version, observed_versions,
            materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
          ) VALUES (
            ${documentId}, ${document.name}, ${document.version}, ${JSON.stringify([document.version])},
            ${JSON.stringify(heads)}, ${JSON.stringify(heads)}, 0, 'Ready', NULL
          )`
          for (const change of InternalAutomerge.changesSince(automerge, [])) {
            yield* sql`INSERT INTO effect_local_changes (
              change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
              actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
            ) VALUES (
              ${change.hash}, ${documentId}, ${document.name}, ${document.version}, 'local',
              ${change.actor}, ${change.sequence}, ${JSON.stringify(change.dependencies)}, ${change.bytes}, 1,
              NULL, ${new Date().toISOString()}, ${sequence}
            )`
          }
          yield* sql`INSERT INTO effect_local_commit_outbox (
            commit_sequence, document_id, invalidation_keys, published
          ) VALUES (${sequence}, ${documentId}, ${JSON.stringify([document.name])}, 0)`
          return yield* recovery.recover(document, documentId).pipe(
            Effect.ensuring(Effect.sync(() => InternalAutomerge.free(automerge)))
          )
        })).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            ))
        ),
      load: recovery.recover,
      stage: (stored, change) =>
        gate.current.pipe(Effect.flatMap((epoch) =>
          Effect.try({
            try: () =>
              InternalAutomerge.stage(
                stored.automerge,
                InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, stored.snapshot.documentId),
                change
              ),
            catch: (cause) => cause
          })
        )),
      tombstone: (stored) =>
        gate.current.pipe(Effect.flatMap((epoch) =>
          Effect.try({
            try: () =>
              InternalAutomerge.stageTombstone(
                stored.automerge,
                InternalAutomerge.actorId(epoch.replicaId, epoch.writerGeneration, stored.snapshot.documentId)
              ),
            catch: (cause) => cause
          })
        )),
      persist: (document, documentId, durable, staged) =>
        sql.withTransaction(Effect.gen(function*() {
          const changes = InternalAutomerge.changesSince(staged, durable.materializedHeads)
          if (changes.length === 0) return durable
          const encoded = InternalAutomerge.value(staged)
          yield* Document.decode(document, documentId, encoded)
          if (!Document.isAutomergeValue(encoded)) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "DocumentDecodeError",
                documentId,
                cause: { _tag: "SchemaCause", message: "Encoded value is not Automerge compatible", path: [] }
              }
            })
          }
          const heads = InternalAutomerge.heads(staged)
          const sequence = yield* nextSequence
          for (const change of changes) {
            yield* sql`INSERT INTO effect_local_changes (
              change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
              actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
            ) VALUES (
              ${change.hash}, ${documentId}, ${document.name}, ${document.version}, 'local',
              ${change.actor}, ${change.sequence}, ${JSON.stringify(change.dependencies)}, ${change.bytes}, 1,
              NULL, ${new Date().toISOString()}, ${sequence}
            )`
          }
          yield* sql`UPDATE effect_local_documents SET
            schema_version = ${document.version},
            observed_versions = ${JSON.stringify([document.version])},
            materialized_heads = ${JSON.stringify(heads)},
            accepted_heads = ${JSON.stringify(heads)}
            , tombstone = ${InternalAutomerge.tombstone(staged) ? 1 : 0}
            WHERE document_id = ${documentId}`
          yield* sql`INSERT INTO effect_local_commit_outbox (
            commit_sequence, document_id, invalidation_keys, published
          ) VALUES (${sequence}, ${documentId}, ${JSON.stringify([document.name])}, 0)`
          return yield* recovery.recover(document, documentId)
        })).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            ))
        )
    })
  })
)
